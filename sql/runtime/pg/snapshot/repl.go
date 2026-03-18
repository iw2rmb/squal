package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/iw2rmb/squall/sql/runtime/pg/cdc"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// OpenReplication opens a PostgreSQL replication connection.
func OpenReplication(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	if dsn == "" {
		return nil, ErrEmptyDSN
	}

	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open replication connection: %w", err)
	}
	return conn, nil
}

// CreateTempSlotExportSnapshot creates a temporary logical slot with exported snapshot.
func CreateTempSlotExportSnapshot(ctx context.Context, conn *pgconn.PgConn, plugin, prefix string) (string, string, cdc.LSN, error) {
	if conn == nil {
		return "", "", "", ErrNilConnection
	}
	if plugin == "" {
		return "", "", "", ErrEmptyPlugin
	}
	if prefix == "" {
		return "", "", "", ErrEmptyPrefix
	}

	slotName := fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())

	result, err := pglogrepl.CreateReplicationSlot(
		ctx,
		conn,
		slotName,
		plugin,
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      true,
			SnapshotAction: "EXPORT_SNAPSHOT",
			Mode:           pglogrepl.LogicalReplication,
		},
	)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to create replication slot: %w", err)
	}

	return result.SlotName, result.SnapshotName, cdc.LSN(result.ConsistentPoint), nil
}

// DropSlot drops an existing replication slot.
func DropSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) error {
	if conn == nil {
		return ErrNilConnection
	}
	if slotName == "" {
		return ErrEmptySlotName
	}

	if err := pglogrepl.DropReplicationSlot(ctx, conn, slotName, pglogrepl.DropReplicationSlotOptions{}); err != nil {
		return fmt.Errorf("failed to drop replication slot: %w", err)
	}
	return nil
}
