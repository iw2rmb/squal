package cdc

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// sendStatusUpdate emits standby status feedback to PostgreSQL.
func (c *Consumer) sendStatusUpdate(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	}

	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, status); err != nil {
		return fmt.Errorf("failed to send standby status: %w", err)
	}

	c.logger.Debug().
		Str("lsn", lsn.String()).
		Msg("Sent standby status update")

	return nil
}
