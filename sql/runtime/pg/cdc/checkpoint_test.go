package cdc

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestEnsureCheckpointTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupMock   func(mock sqlmock.Sqlmock)
		callCount   int
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil database",
			callCount:   1,
			wantErr:     true,
			errContains: ErrNilDatabase.Error(),
		},
		{
			name:      "creates table and index",
			callCount: 1,
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS mill_cdc_checkpoint").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE INDEX IF NOT EXISTS idx_mill_cdc_checkpoint_updated_at").WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:      "idempotent on repeated calls",
			callCount: 2,
			setupMock: func(mock sqlmock.Sqlmock) {
				for i := 0; i < 2; i++ {
					mock.ExpectExec("CREATE TABLE IF NOT EXISTS mill_cdc_checkpoint").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("CREATE INDEX IF NOT EXISTS idx_mill_cdc_checkpoint_updated_at").WillReturnResult(sqlmock.NewResult(0, 0))
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db *sql.DB
			var mock sqlmock.Sqlmock
			if tt.setupMock != nil {
				var err error
				db, mock, err = sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				defer db.Close()
				tt.setupMock(mock)
			}

			for i := 0; i < tt.callCount; i++ {
				err := EnsureCheckpointTable(context.Background(), db)
				if tt.wantErr {
					if err == nil {
						t.Fatalf("expected error, got nil")
					}
					if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
						t.Fatalf("error %q does not contain %q", err.Error(), tt.errContains)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			if mock != nil {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

func TestLoadLSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		slotName    SlotName
		setupMock   func(mock sqlmock.Sqlmock)
		wantLSN     LSN
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil database",
			slotName:    "slot_a",
			wantErr:     true,
			errContains: ErrNilDatabase.Error(),
		},
		{
			name:        "empty slot name",
			slotName:    "",
			wantErr:     true,
			errContains: "slot name cannot be empty",
			setupMock: func(mock sqlmock.Sqlmock) {
			},
		},
		{
			name:     "no checkpoint row",
			slotName: "slot_a",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT last_lsn FROM mill_cdc_checkpoint WHERE slot_name = \\$1").
					WithArgs("slot_a").
					WillReturnRows(sqlmock.NewRows([]string{"last_lsn"}))
			},
			wantLSN: "",
		},
		{
			name:     "checkpoint row exists",
			slotName: "slot_a",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT last_lsn FROM mill_cdc_checkpoint WHERE slot_name = \\$1").
					WithArgs("slot_a").
					WillReturnRows(sqlmock.NewRows([]string{"last_lsn"}).AddRow("0/100"))
			},
			wantLSN: "0/100",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db *sql.DB
			var mock sqlmock.Sqlmock
			if tt.setupMock != nil {
				var err error
				db, mock, err = sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				defer db.Close()
				tt.setupMock(mock)
			}

			lsn, err := LoadLSN(context.Background(), db, tt.slotName)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if lsn != tt.wantLSN {
				t.Fatalf("lsn=%q want=%q", lsn, tt.wantLSN)
			}
			if mock != nil {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

func TestSaveLSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		slotName    SlotName
		lsn         LSN
		setupMock   func(mock sqlmock.Sqlmock)
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil database",
			slotName:    "slot_a",
			lsn:         "0/100",
			wantErr:     true,
			errContains: ErrNilDatabase.Error(),
		},
		{
			name:        "empty slot name",
			slotName:    "",
			lsn:         "0/100",
			wantErr:     true,
			errContains: "slot name cannot be empty",
			setupMock: func(mock sqlmock.Sqlmock) {
			},
		},
		{
			name:        "empty lsn",
			slotName:    "slot_a",
			lsn:         "",
			wantErr:     true,
			errContains: "LSN cannot be empty",
			setupMock: func(mock sqlmock.Sqlmock) {
			},
		},
		{
			name:     "first save",
			slotName: "slot_a",
			lsn:      "0/100",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT last_lsn FROM mill_cdc_checkpoint WHERE slot_name = \\$1").
					WithArgs("slot_a").
					WillReturnRows(sqlmock.NewRows([]string{"last_lsn"}))
				mock.ExpectExec("INSERT INTO mill_cdc_checkpoint").
					WithArgs("slot_a", "0/100").
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			name:     "monotonic advance",
			slotName: "slot_a",
			lsn:      "0/200",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT last_lsn FROM mill_cdc_checkpoint WHERE slot_name = \\$1").
					WithArgs("slot_a").
					WillReturnRows(sqlmock.NewRows([]string{"last_lsn"}).AddRow("0/100"))
				mock.ExpectExec("INSERT INTO mill_cdc_checkpoint").
					WithArgs("slot_a", "0/200").
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			name:     "same lsn is idempotent",
			slotName: "slot_a",
			lsn:      "0/200",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT last_lsn FROM mill_cdc_checkpoint WHERE slot_name = \\$1").
					WithArgs("slot_a").
					WillReturnRows(sqlmock.NewRows([]string{"last_lsn"}).AddRow("0/200"))
				mock.ExpectExec("INSERT INTO mill_cdc_checkpoint").
					WithArgs("slot_a", "0/200").
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			name:        "lower lsn rejected",
			slotName:    "slot_a",
			lsn:         "0/100",
			wantErr:     true,
			errContains: "monotonicity check failed",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT last_lsn FROM mill_cdc_checkpoint WHERE slot_name = \\$1").
					WithArgs("slot_a").
					WillReturnRows(sqlmock.NewRows([]string{"last_lsn"}).AddRow("0/200"))
			},
		},
		{
			name:        "invalid existing lsn",
			slotName:    "slot_a",
			lsn:         "0/300",
			wantErr:     true,
			errContains: "invalid LSN comparison",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT last_lsn FROM mill_cdc_checkpoint WHERE slot_name = \\$1").
					WithArgs("slot_a").
					WillReturnRows(sqlmock.NewRows([]string{"last_lsn"}).AddRow("not-an-lsn"))
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db *sql.DB
			var mock sqlmock.Sqlmock
			if tt.setupMock != nil {
				var err error
				db, mock, err = sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				defer db.Close()
				tt.setupMock(mock)
			}

			err := SaveLSN(context.Background(), db, tt.slotName, tt.lsn)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.errContains)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if mock != nil {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

type checkpointAcker struct {
	calls []LSN
	err   error
}

func (a *checkpointAcker) AckLSN(_ context.Context, lsn LSN) error {
	a.calls = append(a.calls, lsn)
	return a.err
}

func TestCheckpointManager(t *testing.T) {
	t.Parallel()

	t.Run("SaveCheckpoint delegates to SaveLSN", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery("SELECT last_lsn FROM mill_cdc_checkpoint WHERE slot_name = \\$1").
			WithArgs("slot_a").
			WillReturnRows(sqlmock.NewRows([]string{"last_lsn"}))
		mock.ExpectExec("INSERT INTO mill_cdc_checkpoint").
			WithArgs("slot_a", "0/100").
			WillReturnResult(sqlmock.NewResult(1, 1))

		mgr := NewCheckpointManager(db, nil)
		if err := mgr.SaveCheckpoint(context.Background(), "slot_a", "0/100"); err != nil {
			t.Fatalf("SaveCheckpoint error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("expectations: %v", err)
		}
	})

	t.Run("AckLSN with nil acker is no-op", func(t *testing.T) {
		t.Parallel()
		mgr := NewCheckpointManager(nil, nil)
		if err := mgr.AckLSN(context.Background(), "0/100"); err != nil {
			t.Fatalf("AckLSN error: %v", err)
		}
	})

	t.Run("AckLSN delegates to configured acker", func(t *testing.T) {
		t.Parallel()
		acker := &checkpointAcker{}
		mgr := NewCheckpointManager(nil, nil).WithAcker(acker)
		if err := mgr.AckLSN(context.Background(), "0/200"); err != nil {
			t.Fatalf("AckLSN error: %v", err)
		}
		if len(acker.calls) != 1 || acker.calls[0] != "0/200" {
			t.Fatalf("acker calls=%v", acker.calls)
		}
	})

	t.Run("AckLSN surfaces acker error", func(t *testing.T) {
		t.Parallel()
		acker := &checkpointAcker{err: errors.New("ack failed")}
		mgr := NewCheckpointManager(nil, nil).WithAcker(acker)
		err := mgr.AckLSN(context.Background(), "0/200")
		if err == nil || !strings.Contains(err.Error(), "ack failed") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
