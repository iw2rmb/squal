package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestExportWithSlotValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dsn         string
		plugin      string
		prefix      string
		wantErr     bool
		errContains string
	}{
		{
			name:        "empty dsn",
			dsn:         "",
			plugin:      "pgoutput",
			prefix:      "squal_snap_",
			wantErr:     true,
			errContains: "dsn is empty",
		},
		{
			name:        "empty plugin",
			dsn:         "postgres://localhost/test?replication=database",
			plugin:      "",
			prefix:      "squal_snap_",
			wantErr:     true,
			errContains: "plugin name is empty",
		},
		{
			name:        "empty prefix",
			dsn:         "postgres://localhost/test?replication=database",
			plugin:      "pgoutput",
			prefix:      "",
			wantErr:     true,
			errContains: "slot prefix is empty",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			export, cleanup, err := ExportWithSlot(context.Background(), tt.dsn, tt.plugin, tt.prefix, 5*time.Second)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error = %v, want substring %q", err, tt.errContains)
				}
				if cleanup != nil {
					t.Fatal("cleanup must be nil on error")
				}
				if export.SnapshotName != "" {
					t.Fatalf("snapshot name = %q, want empty", export.SnapshotName)
				}
				if export.ConsistentLSN != "" {
					t.Fatalf("consistent lsn = %q, want empty", export.ConsistentLSN)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestExportWithSlotFailedExportHasNilCleanup(t *testing.T) {
	t.Parallel()

	_, cleanup, err := ExportWithSlot(context.Background(), "", "pgoutput", "squal_snap_", 5*time.Second)
	if err == nil {
		t.Fatal("expected error")
	}
	if cleanup != nil {
		t.Fatal("cleanup must be nil on failed export")
	}
}

func TestImport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snapshotName string
		setupMock    func(mock sqlmock.Sqlmock)
		nilTx        bool
		wantErr      bool
		errContains  string
	}{
		{
			name:         "nil transaction",
			snapshotName: "00000003-00000001-1",
			nilTx:        true,
			wantErr:      true,
			errContains:  "transaction is nil",
		},
		{
			name:         "empty snapshot name",
			snapshotName: "",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
			},
			wantErr:     true,
			errContains: "snapshot name is empty",
		},
		{
			name:         "successful import",
			snapshotName: "00000003-00000001-1",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("SET TRANSACTION SNAPSHOT '00000003-00000001-1'").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:         "snapshot name is safely escaped",
			snapshotName: "snap'quoted",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("SET TRANSACTION SNAPSHOT 'snap''quoted'").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:         "set transaction snapshot fails",
			snapshotName: "00000003-00000001-1",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("SET TRANSACTION SNAPSHOT '00000003-00000001-1'").
					WillReturnError(errors.New("snapshot not found"))
			},
			wantErr:     true,
			errContains: "failed to set transaction snapshot",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var tx *sql.Tx
			var mock sqlmock.Sqlmock

			if !tt.nilTx {
				db, m, err := sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New() error: %v", err)
				}
				defer db.Close()
				mock = m

				if tt.setupMock != nil {
					tt.setupMock(mock)
				}

				tx, err = db.Begin()
				if err != nil {
					t.Fatalf("db.Begin() error: %v", err)
				}
				defer tx.Rollback()
			}

			if mock != nil {
				defer func() {
					if err := mock.ExpectationsWereMet(); err != nil {
						t.Fatalf("unmet sqlmock expectations: %v", err)
					}
				}()
			}

			err := Import(context.Background(), tx, tt.snapshotName)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error = %v, want substring %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
