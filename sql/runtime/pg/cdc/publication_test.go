package cdc

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type publicationMetricsRecorder struct {
	probeResults       []bool
	ensureResults      []string
	ensureLatencyCalls []string
}

func (m *publicationMetricsRecorder) RecordProbe(success bool) {
	m.probeResults = append(m.probeResults, success)
}

func (m *publicationMetricsRecorder) RecordEnsure(result string) {
	m.ensureResults = append(m.ensureResults, result)
}

func (m *publicationMetricsRecorder) RecordEnsureWithLatency(result string, _ float64) {
	m.ensureLatencyCalls = append(m.ensureLatencyCalls, result)
}

func TestPubExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pubName     string
		setupMock   func(mock sqlmock.Sqlmock)
		wantExists  bool
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil database",
			pubName:     "any",
			wantErr:     true,
			errContains: ErrNilDatabase.Error(),
		},
		{
			name:    "publication exists",
			pubName: "my_publication",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"exists"}).AddRow(1)
				mock.ExpectQuery("SELECT 1 FROM pg_publication WHERE pubname = \\$1").
					WithArgs("my_publication").
					WillReturnRows(rows)
			},
			wantExists: true,
		},
		{
			name:    "publication does not exist",
			pubName: "missing_publication",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"exists"})
				mock.ExpectQuery("SELECT 1 FROM pg_publication WHERE pubname = \\$1").
					WithArgs("missing_publication").
					WillReturnRows(rows)
			},
		},
		{
			name:    "query error",
			pubName: "broken_publication",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT 1 FROM pg_publication WHERE pubname = \\$1").
					WithArgs("broken_publication").
					WillReturnError(errors.New("connection lost"))
			},
			wantErr:     true,
			errContains: "failed to check if publication exists",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db database
			if tt.setupMock != nil {
				mockDB, mock, err := sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				defer mockDB.Close()
				tt.setupMock(mock)
				db = database{db: mockDB, mock: mock}
			}

			exists, err := pubExists(context.Background(), db.db, tt.pubName)
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
			if exists != tt.wantExists {
				t.Fatalf("exists=%v want=%v", exists, tt.wantExists)
			}
			if db.mock != nil {
				if err := db.mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

func TestCreatePublication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		useNilDB    bool
		pubName     string
		tables      []string
		ops         []string
		setupMock   func(mock sqlmock.Sqlmock)
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil database",
			useNilDB:    true,
			pubName:     "test_pub",
			tables:      []string{"users"},
			ops:         []string{"insert"},
			wantErr:     true,
			errContains: ErrNilDatabase.Error(),
		},
		{
			name:    "stable ordering and deterministic ddl",
			pubName: "ordered_pub",
			tables:  []string{"zebra", "apple", "banana", "banana"},
			ops:     []string{"update", "delete", "insert"},
			setupMock: func(mock sqlmock.Sqlmock) {
				wantSQL := "CREATE PUBLICATION ordered_pub FOR TABLE apple,banana,zebra WITH (publish = 'delete,insert,update')"
				mock.ExpectExec(regexp.QuoteMeta(wantSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:        "invalid publication name",
			pubName:     "bad;name",
			tables:      []string{"public.users"},
			ops:         []string{"insert"},
			wantErr:     true,
			errContains: "invalid publication name",
		},
		{
			name:        "invalid table identifier",
			pubName:     "safe_pub",
			tables:      []string{"public.users;DROP"},
			ops:         []string{"insert"},
			wantErr:     true,
			errContains: "invalid table identifier",
		},
		{
			name:        "empty table list",
			pubName:     "empty_tables",
			tables:      []string{},
			ops:         []string{"insert"},
			wantErr:     true,
			errContains: "tables cannot be empty",
		},
		{
			name:        "empty ops list",
			pubName:     "empty_ops",
			tables:      []string{"users"},
			ops:         []string{},
			wantErr:     true,
			errContains: "operations cannot be empty",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db database
			if !tt.useNilDB {
				mockDB, mock, err := sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				defer mockDB.Close()
				if tt.setupMock != nil {
					tt.setupMock(mock)
				}
				db = database{db: mockDB, mock: mock}
			}

			err := createPublication(context.Background(), db.db, tt.pubName, tt.tables, tt.ops)
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
			if db.mock != nil {
				if err := db.mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

func TestListPublicationTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pubName     string
		setupMock   func(mock sqlmock.Sqlmock)
		wantTables  []string
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil database",
			pubName:     "test_pub",
			wantErr:     true,
			errContains: ErrNilDatabase.Error(),
		},
		{
			name:    "sorted schema-qualified tables",
			pubName: "test_pub",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
					AddRow("public", "users").
					AddRow("analytics", "events").
					AddRow("public", "orders")
				mock.ExpectQuery("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = \\$1").
					WithArgs("test_pub").
					WillReturnRows(rows)
			},
			wantTables: []string{"analytics.events", "public.orders", "public.users"},
		},
		{
			name:    "query error",
			pubName: "test_pub",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = \\$1").
					WithArgs("test_pub").
					WillReturnError(errors.New("query failed"))
			},
			wantErr:     true,
			errContains: "failed to list publication tables",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db database
			if tt.setupMock != nil {
				mockDB, mock, err := sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				defer mockDB.Close()
				tt.setupMock(mock)
				db = database{db: mockDB, mock: mock}
			}

			tables, err := ListPublicationTables(context.Background(), db.db, tt.pubName)
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
			if !reflect.DeepEqual(tables, tt.wantTables) {
				t.Fatalf("tables=%v want=%v", tables, tt.wantTables)
			}
			if db.mock != nil {
				if err := db.mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

func TestAddTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		useNilDB    bool
		pubName     string
		tables      []string
		setupMock   func(mock sqlmock.Sqlmock)
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil database",
			useNilDB:    true,
			pubName:     "test_pub",
			tables:      []string{"public.users"},
			wantErr:     true,
			errContains: ErrNilDatabase.Error(),
		},
		{
			name:    "deterministic and deduped add table",
			pubName: "test_pub",
			tables:  []string{"public.users", "analytics.events", "public.users"},
			setupMock: func(mock sqlmock.Sqlmock) {
				wantSQL := "ALTER PUBLICATION test_pub ADD TABLE analytics.events,public.users"
				mock.ExpectExec(regexp.QuoteMeta(wantSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:    "empty table list is no-op",
			pubName: "test_pub",
			tables:  []string{},
		},
		{
			name:        "invalid publication name",
			pubName:     "bad;name",
			tables:      []string{"public.users"},
			wantErr:     true,
			errContains: "invalid publication name",
		},
		{
			name:        "invalid table identifier",
			pubName:     "test_pub",
			tables:      []string{"public.users;DROP"},
			wantErr:     true,
			errContains: "invalid table identifier",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db database
			if !tt.useNilDB {
				mockDB, mock, err := sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				defer mockDB.Close()
				if tt.setupMock != nil {
					tt.setupMock(mock)
				}
				db = database{db: mockDB, mock: mock}
			}

			err := addTables(context.Background(), db.db, tt.pubName, tt.tables)
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
			if db.mock != nil {
				if err := db.mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

func TestEnsurePublication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		pubName           string
		tables            []string
		ops               []string
		setupMock         func(mock sqlmock.Sqlmock)
		wantErr           bool
		errContains       string
		wantMetricResults []string
	}{
		{
			name:              "nil database",
			pubName:           "test_pub",
			tables:            []string{"public.users"},
			ops:               []string{"insert"},
			wantErr:           true,
			errContains:       ErrNilDatabase.Error(),
			wantMetricResults: []string{"error"},
		},
		{
			name:    "creates publication when missing",
			pubName: "test_pub",
			tables:  []string{"public.users", "public.orders"},
			ops:     []string{"update", "insert"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT 1 FROM pg_publication WHERE pubname = \\$1").
					WithArgs("test_pub").
					WillReturnRows(sqlmock.NewRows([]string{"exists"}))
				wantSQL := "CREATE PUBLICATION test_pub FOR TABLE public.orders,public.users WITH (publish = 'insert,update')"
				mock.ExpectExec(regexp.QuoteMeta(wantSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantMetricResults: []string{"created"},
		},
		{
			name:    "reconciles missing tables",
			pubName: "test_pub",
			tables:  []string{"public.users", "public.orders"},
			ops:     []string{"insert"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT 1 FROM pg_publication WHERE pubname = \\$1").
					WithArgs("test_pub").
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
				mock.ExpectQuery("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = \\$1").
					WithArgs("test_pub").
					WillReturnRows(sqlmock.NewRows([]string{"schemaname", "tablename"}).AddRow("public", "users"))
				wantSQL := "ALTER PUBLICATION test_pub ADD TABLE public.orders"
				mock.ExpectExec(regexp.QuoteMeta(wantSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantMetricResults: []string{"reconciled"},
		},
		{
			name:    "unchanged when no missing tables",
			pubName: "test_pub",
			tables:  []string{"public.users"},
			ops:     []string{"insert"},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT 1 FROM pg_publication WHERE pubname = \\$1").
					WithArgs("test_pub").
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
				mock.ExpectQuery("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = \\$1").
					WithArgs("test_pub").
					WillReturnRows(sqlmock.NewRows([]string{"schemaname", "tablename"}).AddRow("public", "users"))
			},
			wantMetricResults: []string{"unchanged"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var db database
			if tt.setupMock != nil {
				mockDB, mock, err := sqlmock.New()
				if err != nil {
					t.Fatalf("sqlmock.New: %v", err)
				}
				defer mockDB.Close()
				tt.setupMock(mock)
				db = database{db: mockDB, mock: mock}
			}

			metrics := &publicationMetricsRecorder{}
			err := EnsurePublication(context.Background(), db.db, tt.pubName, tt.tables, tt.ops, metrics)
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

			if !reflect.DeepEqual(metrics.ensureLatencyCalls, tt.wantMetricResults) {
				t.Fatalf("metric results=%v want=%v", metrics.ensureLatencyCalls, tt.wantMetricResults)
			}
			if db.mock != nil {
				if err := db.mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("expectations: %v", err)
				}
			}
		})
	}
}

func TestQualifyTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		tableName     string
		defaultSchema string
		want          string
	}{
		{name: "empty table", tableName: "", defaultSchema: "public", want: ""},
		{name: "already qualified", tableName: "analytics.events", defaultSchema: "public", want: "analytics.events"},
		{name: "default schema fallback", tableName: "users", defaultSchema: "", want: "public.users"},
		{name: "custom schema", tableName: "users", defaultSchema: "app", want: "app.users"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := QualifyTableName(tt.tableName, tt.defaultSchema)
			if got != tt.want {
				t.Fatalf("got=%q want=%q", got, tt.want)
			}
		})
	}
}

type database struct {
	db   *sql.DB
	mock sqlmock.Sqlmock
}
