//go:build integration
// +build integration

package snapshot

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestExportWithSlotIntegration(t *testing.T) {
	dsn := testPostgresDSN()
	if dsn == "" {
		t.Skip("SQUALL_TEST_POSTGRES_DSN not set, skipping integration test")
	}

	replDSN := dsn
	if !strings.Contains(replDSN, "replication=") {
		replDSN += "&replication=database"
	}

	ctx := context.Background()

	export, cleanup, err := ExportWithSlot(ctx, replDSN, "pgoutput", "squal_snap_", 10*time.Second)
	if err != nil {
		t.Fatalf("ExportWithSlot() error = %v", err)
	}
	defer func() {
		if cleanup != nil {
			_ = cleanup()
		}
	}()

	if export.SnapshotName == "" {
		t.Fatal("snapshot name is empty")
	}
	if export.ConsistentLSN == "" {
		t.Fatal("consistent LSN is empty")
	}
	if !strings.Contains(string(export.ConsistentLSN), "/") {
		t.Fatalf("consistent LSN %q is not in PostgreSQL format", export.ConsistentLSN)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		t.Fatalf("db.BeginTx() error = %v", err)
	}
	defer tx.Rollback()

	if err := Import(ctx, tx, export.SnapshotName); err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	var one int
	if err := tx.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", one)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit() error = %v", err)
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup() error = %v", err)
	}
	cleanup = nil
}

func TestExportWithSlotSlotLifecycle(t *testing.T) {
	dsn := testPostgresDSN()
	if dsn == "" {
		t.Skip("SQUALL_TEST_POSTGRES_DSN not set, skipping integration test")
	}

	replDSN := dsn
	if !strings.Contains(replDSN, "replication=") {
		replDSN += "&replication=database"
	}

	ctx := context.Background()
	export, cleanup, err := ExportWithSlot(ctx, replDSN, "pgoutput", "squal_lifecycle_", 10*time.Second)
	if err != nil {
		t.Fatalf("ExportWithSlot() error = %v", err)
	}
	defer func() {
		if cleanup != nil {
			_ = cleanup()
		}
	}()

	if export.SnapshotName == "" || export.ConsistentLSN == "" {
		t.Fatalf("invalid export: %+v", export)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name LIKE 'squal_lifecycle_%'").Scan(&count); err != nil {
		t.Fatalf("failed to query replication slots: %v", err)
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup() error = %v", err)
	}
	cleanup = nil

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name LIKE 'squal_lifecycle_%'").Scan(&count); err != nil {
		t.Fatalf("failed to query replication slots after cleanup: %v", err)
	}
	if count != 0 {
		t.Fatalf("found %d slots with lifecycle prefix after cleanup, want 0", count)
	}
}

func testPostgresDSN() string {
	if dsn := os.Getenv("SQUALL_TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return os.Getenv("MILL_TEST_POSTGRES_DSN")
}
