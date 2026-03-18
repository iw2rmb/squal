package cdc

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type probeMetricsRecorder struct {
	probeResults  []bool
	ensureResults []string
}

func (m *probeMetricsRecorder) RecordProbe(success bool) {
	m.probeResults = append(m.probeResults, success)
}

func (m *probeMetricsRecorder) RecordEnsure(result string) {
	m.ensureResults = append(m.ensureResults, result)
}

func (m *probeMetricsRecorder) RecordEnsureWithLatency(string, float64) {}

type probeLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *probeLogger) Info() LogEvent  { return &probeLogEvent{logger: l} }
func (l *probeLogger) Warn() LogEvent  { return &probeLogEvent{logger: l} }
func (l *probeLogger) Error() LogEvent { return &probeLogEvent{logger: l} }
func (l *probeLogger) Debug() LogEvent { return &probeLogEvent{logger: l} }

type probeLogEvent struct {
	logger *probeLogger
}

func (e *probeLogEvent) Str(string, string) LogEvent { return e }
func (e *probeLogEvent) Int(string, int) LogEvent    { return e }
func (e *probeLogEvent) Err(error) LogEvent          { return e }
func (e *probeLogEvent) Msg(msg string) {
	e.logger.mu.Lock()
	e.logger.msgs = append(e.logger.msgs, msg)
	e.logger.mu.Unlock()
}

func (l *probeLogger) Events() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.msgs))
	copy(out, l.msgs)
	return out
}

func TestProbe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupDB     func(t *testing.T) (*sql.DB, sqlmock.Sqlmock)
		wantErr     bool
		errContains string
		checkType   func(t *testing.T, err error)
		wantMetrics []bool
	}{
		{
			name:        "nil database",
			wantErr:     true,
			errContains: ErrNilDatabase.Error(),
			wantMetrics: []bool{false},
		},
		{
			name: "successful probe",
			setupDB: func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
				t.Helper()
				db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				mock.ExpectPing().WillReturnError(nil)
				mock.ExpectQuery("SHOW server_version_num").WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow("140000"))
				mock.ExpectQuery("SHOW wal_level").WillReturnRows(sqlmock.NewRows([]string{"wal_level"}).AddRow("logical"))
				mock.ExpectQuery("SHOW max_wal_senders").WillReturnRows(sqlmock.NewRows([]string{"max_wal_senders"}).AddRow("10"))
				mock.ExpectQuery("SHOW max_replication_slots").WillReturnRows(sqlmock.NewRows([]string{"max_replication_slots"}).AddRow("10"))
				mock.ExpectQuery("SELECT has_database_privilege").WillReturnRows(sqlmock.NewRows([]string{"has_database_privilege"}).AddRow(true))
				return db, mock
			},
			wantMetrics: []bool{true},
		},
		{
			name: "unsupported server version",
			setupDB: func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
				t.Helper()
				db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				mock.ExpectPing().WillReturnError(nil)
				mock.ExpectQuery("SHOW server_version_num").WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow("90600"))
				return db, mock
			},
			wantErr: true,
			checkType: func(t *testing.T, err error) {
				t.Helper()
				var typed *ErrUnsupportedVersion
				if !errors.As(err, &typed) {
					t.Fatalf("expected *ErrUnsupportedVersion, got %T", err)
				}
			},
			wantMetrics: []bool{false},
		},
		{
			name: "wal level requires restart",
			setupDB: func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
				t.Helper()
				db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				mock.ExpectPing().WillReturnError(nil)
				mock.ExpectQuery("SHOW server_version_num").WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow("140000"))
				mock.ExpectQuery("SHOW wal_level").WillReturnRows(sqlmock.NewRows([]string{"wal_level"}).AddRow("replica"))
				return db, mock
			},
			wantErr: true,
			checkType: func(t *testing.T, err error) {
				t.Helper()
				var typed *ErrPostgresNeedsRestart
				if !errors.As(err, &typed) {
					t.Fatalf("expected *ErrPostgresNeedsRestart, got %T", err)
				}
				if typed.Param != "wal_level" || typed.Have != "replica" || typed.Want != "logical" {
					t.Fatalf("unexpected fields: %+v", typed)
				}
			},
			wantMetrics: []bool{false},
		},
		{
			name: "insufficient privileges",
			setupDB: func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
				t.Helper()
				db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				mock.ExpectPing().WillReturnError(nil)
				mock.ExpectQuery("SHOW server_version_num").WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow("140000"))
				mock.ExpectQuery("SHOW wal_level").WillReturnRows(sqlmock.NewRows([]string{"wal_level"}).AddRow("logical"))
				mock.ExpectQuery("SHOW max_wal_senders").WillReturnRows(sqlmock.NewRows([]string{"max_wal_senders"}).AddRow("10"))
				mock.ExpectQuery("SHOW max_replication_slots").WillReturnRows(sqlmock.NewRows([]string{"max_replication_slots"}).AddRow("10"))
				mock.ExpectQuery("SELECT has_database_privilege").WillReturnRows(sqlmock.NewRows([]string{"has_database_privilege"}).AddRow(false))
				return db, mock
			},
			wantErr: true,
			checkType: func(t *testing.T, err error) {
				t.Helper()
				var typed *ErrInsufficientPrivileges
				if !errors.As(err, &typed) {
					t.Fatalf("expected *ErrInsufficientPrivileges, got %T", err)
				}
			},
			wantMetrics: []bool{false},
		},
		{
			name: "ping failure",
			setupDB: func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
				t.Helper()
				db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				mock.ExpectPing().WillReturnError(errors.New("connection refused"))
				return db, mock
			},
			wantErr:     true,
			errContains: "connection refused",
			wantMetrics: []bool{false},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db *sql.DB
			var mock sqlmock.Sqlmock
			if tt.setupDB != nil {
				db, mock = tt.setupDB(t)
				defer db.Close()
			}

			metrics := &probeMetricsRecorder{}
			log := &probeLogger{}
			err := Probe(context.Background(), db, log, metrics)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				if tt.checkType != nil {
					tt.checkType(t, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(metrics.probeResults) != len(tt.wantMetrics) {
				t.Fatalf("metrics count=%d want=%d", len(metrics.probeResults), len(tt.wantMetrics))
			}
			for i, want := range tt.wantMetrics {
				if metrics.probeResults[i] != want {
					t.Fatalf("metric[%d]=%v want=%v", i, metrics.probeResults[i], want)
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

func TestProbe_AllowsNilLogger(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectPing().WillReturnError(nil)
	mock.ExpectQuery("SHOW server_version_num").WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow("140000"))
	mock.ExpectQuery("SHOW wal_level").WillReturnRows(sqlmock.NewRows([]string{"wal_level"}).AddRow("logical"))
	mock.ExpectQuery("SHOW max_wal_senders").WillReturnRows(sqlmock.NewRows([]string{"max_wal_senders"}).AddRow("10"))
	mock.ExpectQuery("SHOW max_replication_slots").WillReturnRows(sqlmock.NewRows([]string{"max_replication_slots"}).AddRow("10"))
	mock.ExpectQuery("SELECT has_database_privilege").WillReturnRows(sqlmock.NewRows([]string{"has_database_privilege"}).AddRow(true))

	if err := Probe(context.Background(), db, nil, &probeMetricsRecorder{}); err != nil {
		t.Fatalf("Probe with nil logger failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
