package cdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// runReplicationStream establishes replication connection and consumes logical messages.
func (c *Consumer) runReplicationStream(ctx context.Context) error {
	connConfig, err := pgconn.ParseConfig(c.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}
	connConfig.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("failed to connect in replication mode: %w", err)
	}
	defer conn.Close(ctx)

	c.logger.Info().
		Str("db", connConfig.Database).
		Str("user", connConfig.User).
		Msg("Connected to PostgreSQL in replication mode")

	if err := c.ensureReplicationSlot(ctx, conn); err != nil {
		return fmt.Errorf("failed to ensure replication slot: %w", err)
	}

	startLSN, err := c.resolveStartLSN(ctx)
	if err != nil {
		return err
	}

	pluginArguments := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", c.config.Publication),
	}

	if err := pglogrepl.StartReplication(ctx, conn, string(c.config.SlotName), startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArguments,
	}); err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	c.logger.Info().
		Str("slot", string(c.config.SlotName)).
		Str("publication", c.config.Publication).
		Str("start_lsn", startLSN.String()).
		Str("plugin_args", strings.Join(pluginArguments, ", ")).
		Msg("Replication started")

	if err := c.sendStatusUpdate(ctx, conn, startLSN); err != nil {
		c.logger.Warn().
			Err(err).
			Str("lsn", startLSN.String()).
			Msg("Failed to send initial standby status update")
	}

	return c.processMessages(ctx, conn, startLSN)
}

func (c *Consumer) resolveStartLSN(ctx context.Context) (pglogrepl.LSN, error) {
	if c.checkpointStore != nil {
		checkpointLSN, err := c.checkpointStore.LoadCheckpoint(ctx, c.config.SlotName)
		if err != nil {
			return 0, fmt.Errorf("failed to load checkpoint LSN: %w", err)
		}
		if checkpointLSN != "" {
			lsn, parseErr := pglogrepl.ParseLSN(string(checkpointLSN))
			if parseErr != nil {
				return 0, fmt.Errorf("failed to parse checkpoint LSN %q: %w", checkpointLSN, parseErr)
			}
			c.logger.Info().
				Str("slot", string(c.config.SlotName)).
				Str("lsn", string(checkpointLSN)).
				Msg("Resuming from checkpoint LSN")
			return lsn, nil
		}
	}

	if strings.TrimSpace(string(c.config.StartFromLSN)) != "" {
		lsn, err := pglogrepl.ParseLSN(strings.TrimSpace(string(c.config.StartFromLSN)))
		if err != nil {
			return 0, fmt.Errorf("failed to parse StartFromLSN %q: %w", c.config.StartFromLSN, err)
		}
		c.logger.Info().
			Str("slot", string(c.config.SlotName)).
			Str("start_from_lsn", string(c.config.StartFromLSN)).
			Msg("Using configured StartFromLSN as replication start position")
		return lsn, nil
	}

	return 0, nil
}

// ensureReplicationSlot creates logical slot if it does not already exist.
func (c *Consumer) ensureReplicationSlot(ctx context.Context, conn *pgconn.PgConn) error {
	c.logger.Info().
		Str("slot", string(c.config.SlotName)).
		Msg("Ensuring replication slot exists")

	_, err := pglogrepl.CreateReplicationSlot(ctx, conn, string(c.config.SlotName), "pgoutput", pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" {
			c.logger.Info().
				Str("slot", string(c.config.SlotName)).
				Msg("Replication slot already exists")
			return nil
		}
		if strings.Contains(err.Error(), "already exists") {
			c.logger.Info().
				Str("slot", string(c.config.SlotName)).
				Msg("Replication slot already exists")
			return nil
		}
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	c.logger.Info().
		Str("slot", string(c.config.SlotName)).
		Msg("Replication slot created")
	return nil
}

// processMessages reads replication messages, assembles tx batches, and dispatches commits.
func (c *Consumer) processMessages(ctx context.Context, conn *pgconn.PgConn, startLSN pglogrepl.LSN) error {
	statusTicker := time.NewTicker(c.statusInterval())
	defer statusTicker.Stop()

	var currentBatch *TxBatch
	relations := make(map[uint32]*pglogrepl.RelationMessageV2)
	typeMap := pgtype.NewMap()
	clientXLogPos := startLSN

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return nil
		case <-statusTicker.C:
			if err := c.sendStatusUpdate(ctx, conn, clientXLogPos); err != nil {
				c.logger.Warn().Err(err).Msg("Failed to send standby status update")
			}
		default:
		}

		receiveCtx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("failed to receive message: %w", err)
		}

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				if err := c.sendStatusUpdate(ctx, conn, clientXLogPos); err != nil {
					c.logger.Warn().Err(err).Msg("Failed to send requested status update")
				}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse XLogData: %w", err)
			}

			logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
			if err != nil {
				return fmt.Errorf("failed to parse logical message: %w", err)
			}

			switch lmsg := logicalMsg.(type) {
			case *pglogrepl.RelationMessageV2:
				relations[lmsg.RelationID] = lmsg
			case *pglogrepl.BeginMessage:
				currentBatch = &TxBatch{
					LSN:    LSN(xld.WALStart.String()),
					Events: []TxEvent{},
				}
			case *pglogrepl.InsertMessageV2:
				if currentBatch != nil {
					event := c.decodeInsert(lmsg, relations, typeMap, xld.WALStart)
					if c.applyBackpressure(ctx, len(currentBatch.Events)) {
						currentBatch.Events = append(currentBatch.Events, event)
					}
				}
			case *pglogrepl.UpdateMessageV2:
				if currentBatch != nil {
					event := c.decodeUpdate(lmsg, relations, typeMap, xld.WALStart)
					if c.applyBackpressure(ctx, len(currentBatch.Events)) {
						currentBatch.Events = append(currentBatch.Events, event)
					}
				}
			case *pglogrepl.DeleteMessageV2:
				if currentBatch != nil {
					event := c.decodeDelete(lmsg, relations, typeMap, xld.WALStart)
					if c.applyBackpressure(ctx, len(currentBatch.Events)) {
						currentBatch.Events = append(currentBatch.Events, event)
					}
				}
			case *pglogrepl.CommitMessage:
				if currentBatch == nil {
					currentBatch = &TxBatch{
						LSN:    LSN(lmsg.CommitLSN.String()),
						Events: []TxEvent{},
					}
				} else {
					currentBatch.LSN = LSN(lmsg.CommitLSN.String())
					for i := range currentBatch.Events {
						currentBatch.Events[i].CommitTime = lmsg.CommitTime
						currentBatch.Events[i].CommitLSN = currentBatch.LSN
					}
				}

				if err := c.dispatcher.DispatchWithCheckpoint(ctx, *currentBatch, c.config.SlotName, c.checkpointStore, c.handler); err != nil {
					c.logger.Error().
						Err(err).
						Str("lsn", string(currentBatch.LSN)).
						Msg("Event handler failed")
					return fmt.Errorf("event handler failed for LSN %s: %w", currentBatch.LSN, err)
				}

				c.logger.Debug().
					Str("lsn", string(currentBatch.LSN)).
					Int("events", len(currentBatch.Events)).
					Msg("Batch processed (commit)")

				clientXLogPos = lmsg.CommitLSN
				currentBatch = nil
			case *pglogrepl.TypeMessageV2,
				*pglogrepl.OriginMessage,
				*pglogrepl.TruncateMessageV2,
				*pglogrepl.StreamStartMessageV2,
				*pglogrepl.StreamStopMessageV2,
				*pglogrepl.StreamCommitMessageV2,
				*pglogrepl.StreamAbortMessageV2:
				// ignored by design
			}
		}
	}
}

func (c *Consumer) statusInterval() time.Duration {
	if c.config.StatusInterval <= 0 {
		return defaultStatusInterval
	}
	return c.config.StatusInterval
}
