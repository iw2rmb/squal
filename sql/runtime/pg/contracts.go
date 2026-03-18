package pg

import (
	"context"

	"github.com/iw2rmb/squall/parser"
	"github.com/iw2rmb/squall/sql/runtime/pg/cdc"
)

// DatabaseProvider defines database runtime integration consumed by host applications.
type DatabaseProvider interface {
	Name() string
	Parser() parser.Parser
	CDC() CDCSource
	Exec() QueryRunner
	Schema() SchemaIntrospector
	Caps() Capabilities
	IMV() StrategyHints
}

// CDCSource streams committed transaction batches.
type CDCSource interface {
	Start(ctx context.Context) (<-chan cdc.TxBatch, error)
	Stop(ctx context.Context) error
}

// QueryRunner executes SQL and returns encoded row payloads.
type QueryRunner interface {
	QueryJSON(ctx context.Context, sql string, args ...any) ([]byte, error)
}

// SchemaIntrospector reads schema metadata needed by runtime orchestration.
type SchemaIntrospector interface {
	PrimaryKeys(ctx context.Context, table string) ([]string, error)
}

// Capabilities describes database-level feature support.
type Capabilities struct {
	SupportsCDC             bool
	SupportsIncremental     bool
	SupportsWindowFunctions bool
}

// StrategyHints provides runtime strategy preferences for IMV orchestration.
type StrategyHints struct {
	PreferDelta                 bool
	SupportsBucketedAggregation bool
}
