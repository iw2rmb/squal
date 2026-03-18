package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/iw2rmb/squall/sql/runtime/pg/cdc"
)

// Export is a replication-exported PostgreSQL snapshot descriptor.
type Export struct {
	SnapshotName  string
	ConsistentLSN cdc.LSN
}

// ExportWithSlot exports a consistent snapshot through a temporary replication slot.
//
// The returned cleanup function closes the replication connection and best-effort drops
// the temporary slot. Slot drop errors are intentionally ignored because temporary slots
// are released when the replication connection closes.
func ExportWithSlot(ctx context.Context, dsn, plugin, prefix string, timeout time.Duration) (Export, func() error, error) {
	if dsn == "" {
		return Export{}, nil, ErrEmptyDSN
	}
	if plugin == "" {
		return Export{}, nil, ErrEmptyPlugin
	}
	if prefix == "" {
		return Export{}, nil, ErrEmptyPrefix
	}

	opCtx, cancel := context.WithTimeout(ctx, timeout)

	conn, err := OpenReplication(opCtx, dsn)
	if err != nil {
		cancel()
		return Export{}, nil, fmt.Errorf("failed to open replication connection: %w", err)
	}

	slotName, snapshotName, consistentLSN, err := CreateTempSlotExportSnapshot(opCtx, conn, plugin, prefix)
	if err != nil {
		_ = conn.Close(opCtx)
		cancel()
		return Export{}, nil, fmt.Errorf("failed to create temporary slot with snapshot: %w", err)
	}

	cleanup := func() error {
		defer cancel()
		defer conn.Close(context.Background())
		_ = DropSlot(context.Background(), conn, slotName)
		return nil
	}

	return Export{
		SnapshotName:  snapshotName,
		ConsistentLSN: consistentLSN,
	}, cleanup, nil
}

// Import imports an exported snapshot into an open SQL transaction.
func Import(ctx context.Context, tx *sql.Tx, snapshotName string) error {
	if tx == nil {
		return ErrNilTransaction
	}
	if snapshotName == "" {
		return ErrEmptySnapshotName
	}

	quoted := "'" + strings.ReplaceAll(snapshotName, "'", "''") + "'"
	if _, err := tx.ExecContext(ctx, "SET TRANSACTION SNAPSHOT "+quoted); err != nil {
		return fmt.Errorf("failed to set transaction snapshot: %w", err)
	}
	return nil
}
