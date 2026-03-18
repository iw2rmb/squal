package cdc

import (
	"context"
	"database/sql"
	"fmt"
)

const checkpointTableName = "mill_cdc_checkpoint"

// CheckpointManager persists checkpoint state and can optionally ack downstream.
type CheckpointManager struct {
	db     *sql.DB
	logger Logger
	acker  Acker
}

// NewCheckpointManager constructs a checkpoint manager over a SQL DB.
func NewCheckpointManager(db *sql.DB, logger Logger) *CheckpointManager {
	return &CheckpointManager{
		db:     db,
		logger: logger,
	}
}

// WithAcker attaches optional replication acknowledgement behavior.
func (m *CheckpointManager) WithAcker(acker Acker) *CheckpointManager {
	m.acker = acker
	return m
}

// SaveCheckpoint persists slot checkpoint.
func (m *CheckpointManager) SaveCheckpoint(ctx context.Context, slotName SlotName, lsn LSN) error {
	return SaveLSN(ctx, m.db, slotName, lsn)
}

// LoadCheckpoint reads the last persisted checkpoint for a slot.
func (m *CheckpointManager) LoadCheckpoint(ctx context.Context, slotName SlotName) (LSN, error) {
	return LoadLSN(ctx, m.db, slotName)
}

// AckLSN sends replication ack through configured Acker (best-effort no-op when nil).
func (m *CheckpointManager) AckLSN(ctx context.Context, lsn LSN) error {
	if m.acker == nil {
		return nil
	}
	return m.acker.AckLSN(ctx, lsn)
}

// Acker sends replication acknowledgements after checkpoint save.
type Acker interface {
	AckLSN(ctx context.Context, lsn LSN) error
}

// EnsureCheckpointTable creates the checkpoint table and a monitoring index.
func EnsureCheckpointTable(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrNilDatabase
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS mill_cdc_checkpoint (
			slot_name TEXT PRIMARY KEY,
			last_lsn TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`

	if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	_, _ = db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_mill_cdc_checkpoint_updated_at ON mill_cdc_checkpoint(updated_at)`)
	return nil
}

// LoadLSN loads the last persisted checkpoint for a slot.
func LoadLSN(ctx context.Context, db *sql.DB, slotName SlotName) (LSN, error) {
	if db == nil {
		return "", ErrNilDatabase
	}
	if slotName == "" {
		return "", fmt.Errorf("slot name cannot be empty")
	}

	var lsn LSN
	query := fmt.Sprintf("SELECT last_lsn FROM %s WHERE slot_name = $1", checkpointTableName)
	err := db.QueryRowContext(ctx, query, string(slotName)).Scan(&lsn)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to load LSN for slot %s: %w", slotName, err)
	}
	return lsn, nil
}

// SaveLSN persists slot checkpoint with monotonicity enforcement.
func SaveLSN(ctx context.Context, db *sql.DB, slotName SlotName, lsn LSN) error {
	if db == nil {
		return ErrNilDatabase
	}
	if slotName == "" {
		return fmt.Errorf("slot name cannot be empty")
	}
	if lsn == "" {
		return fmt.Errorf("LSN cannot be empty")
	}

	current, err := LoadLSN(ctx, db, slotName)
	if err != nil {
		return err
	}

	if current != "" {
		cmp, cmpErr := CompareLSN(lsn, current)
		if cmpErr != nil {
			return fmt.Errorf("invalid LSN comparison: %w", cmpErr)
		}
		if cmp < 0 {
			return fmt.Errorf("LSN %s is not greater than or equal to current checkpoint for slot %s (monotonicity check failed)", lsn, slotName)
		}
	}

	upsertSQL := fmt.Sprintf(`
		INSERT INTO %s (slot_name, last_lsn, updated_at)
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (slot_name) DO UPDATE
		SET last_lsn = EXCLUDED.last_lsn,
			updated_at = CURRENT_TIMESTAMP
	`, checkpointTableName)

	if _, err := db.ExecContext(ctx, upsertSQL, string(slotName), string(lsn)); err != nil {
		return fmt.Errorf("failed to save LSN %s for slot %s: %w", lsn, slotName, err)
	}

	return nil
}
