package parser

import (
	"time"

	"github.com/iw2rmb/squall/core"
)

// QueryMetadata contains extracted information from a SQL query.
type QueryMetadata struct {
	Tables       []string          `json:"tables"`
	Columns      []string          `json:"columns"`
	Operations   []string          `json:"operations"`   // SELECT, INSERT, UPDATE, DELETE
	Aggregations []string          `json:"aggregations"` // COUNT, SUM, AVG, etc.
	Filters      map[string]string `json:"filters"`      // column -> condition
	GroupBy      []string          `json:"group_by"`
	OrderBy      []string          `json:"order_by"`
	HasSubquery  bool              `json:"has_subquery"`

	// Enhanced metadata for dependency detection
	SelectColumns   []ColumnRef       `json:"select_columns"`   // Columns in SELECT clause
	WhereConditions []FilterCondition `json:"where_conditions"` // Parsed WHERE conditions
	JoinConditions  []JoinCondition   `json:"join_conditions"`  // JOIN relationships
	Subqueries      []SubqueryInfo    `json:"subqueries"`       // Nested subqueries
	IsAggregate     bool              `json:"is_aggregate"`     // Has aggregation functions
	Limit           int               `json:"limit"`            // LIMIT value if present
	Offset          int               `json:"offset"`           // OFFSET value if present

	// Advanced operation metadata
	HasDistinct            bool     `json:"has_distinct"`              // Has DISTINCT operation
	DistinctColumns        []string `json:"distinct_columns"`          // Columns with DISTINCT
	HasWindowFunctions     bool     `json:"has_window_functions"`      // Has window functions
	WindowFunctions        []string `json:"window_functions"`          // Window function names
	WindowPartitions       []string `json:"window_partitions"`         // PARTITION BY columns
	WindowOrderBy          []string `json:"window_order_by"`           // Window ORDER BY columns
	HasCTEs                bool     `json:"has_ctes"`                  // Has CTEs
	CTENames               []string `json:"cte_names"`                 // CTE names
	IsRecursiveCTE         bool     `json:"is_recursive_cte"`          // Has RECURSIVE CTE
	HasDatabaseSpecificOps bool     `json:"has_database_specific_ops"` // Has DB-specific operations
	DatabaseType           string   `json:"database_type"`             // postgresql, mysql, etc.
	DatabaseOperations     []string `json:"database_operations"`       // DB-specific operation names

	// Time bucket metadata for scoped validation
	HasTimeBucket bool        `json:"has_time_bucket"` // Has date_trunc or similar time bucketing
	BucketInfo    *BucketInfo `json:"bucket_info"`     // Time bucket details if present

	// Sliding window metadata for automatic expiry
	SlidingWindow *SlidingWindowInfo `json:"sliding_window,omitempty"` // Sliding window details if present
}

// BucketInfo describes time bucketing in GROUP BY for scoped validation.
type BucketInfo struct {
	Function    string `json:"function"`     // e.g., "date_trunc"
	Interval    string `json:"interval"`     // e.g., "hour", "day"
	Column      string `json:"column"`       // timestamp column name
	ColumnTable string `json:"column_table"` // table qualifier if present
}

// AggCase represents a parsed aggregate with a CASE expression.
// It is a normalized, portable shape used by strategies.
type AggCase struct {
	Func       string         `json:"func"`  // "SUM" or "COUNT"
	Alias      string         `json:"alias"` // target alias when known
	Conditions []AggCondition `json:"conditions"`
	ThenColumn string         `json:"then_column,omitempty"`
	ThenConst  *float64       `json:"then_const,omitempty"`
	ElseConst  *float64       `json:"else_const,omitempty"`
}

// AggCondition models a minimal predicate supported in CASE parsing.
// Only AND-conjunctions of eq/in are supported.
type AggCondition struct {
	Kind   string        `json:"kind"` // "eq" or "in"
	Column ColumnRef     `json:"column"`
	Value  interface{}   `json:"value,omitempty"`
	Values []interface{} `json:"values,omitempty"`
}

// AggTerm represents one aggregate term in an arithmetic expression.
// Sign is +1 or -1; Func is SUM/COUNT. Either Case is set, or Column is set for simple SUM(column).
type AggTerm struct {
	Sign   int      `json:"sign"`
	Func   string   `json:"func"`
	Case   *AggCase `json:"case,omitempty"`
	Column string   `json:"column,omitempty"`
}

// AggComposition represents an aliased expression composed of aggregate terms.
type AggComposition struct {
	Alias string    `json:"alias"`
	Terms []AggTerm `json:"terms"`
}

// ColumnRef represents a column reference with table context.
type ColumnRef struct {
	Table  string `json:"table,omitempty"`
	Column string `json:"column"`
	Alias  string `json:"alias,omitempty"`
	IsAgg  bool   `json:"is_agg"` // Is aggregation function
}

// FilterCondition represents a WHERE clause condition.
type FilterCondition struct {
	Column   ColumnRef      `json:"column"`
	Operator core.CompareOp `json:"operator"` // =, !=, <, >, LIKE, IN, etc.
	Value    interface{}    `json:"value"`
	IsParam  bool           `json:"is_param"` // Is parameterized value
}

// JoinCondition represents a JOIN relationship.
type JoinCondition struct {
	Type        core.JoinType `json:"type"` // INNER, LEFT, RIGHT, FULL
	LeftTable   string        `json:"left_table"`
	RightTable  string        `json:"right_table"`
	LeftColumn  string        `json:"left_column"`
	RightColumn string        `json:"right_column"`
	LeftAlias   string        `json:"left_alias,omitempty"`
	RightAlias  string        `json:"right_alias,omitempty"`
	OnExpr      string        `json:"on_expr,omitempty"`
}

// SubqueryInfo represents information about a subquery.
type SubqueryInfo struct {
	Alias    string         `json:"alias"`
	SQL      string         `json:"sql"`
	Metadata *QueryMetadata `json:"metadata"`
}

// Aggregate represents a parsed aggregate function (COUNT, SUM, AVG, MIN, MAX).
// It captures the function name, optional alias, argument column (or "*" for COUNT(*)),
// and whether DISTINCT is applied.
type Aggregate struct {
	Func     string `json:"func"`     // "COUNT", "SUM", "AVG", "MIN", "MAX"
	Alias    string `json:"alias"`    // target alias when known
	Column   string `json:"column"`   // column name or "*" for COUNT(*)
	Distinct bool   `json:"distinct"` // true if DISTINCT is applied
	Table    string `json:"table"`    // table qualifier if present
}

// DistinctSpec represents DISTINCT usage in a query.
// It distinguishes between SELECT DISTINCT and COUNT(DISTINCT).
type DistinctSpec struct {
	// HasDistinct is true when SELECT DISTINCT is present.
	HasDistinct bool `json:"has_distinct"`
	// Columns contains the columns specified in SELECT DISTINCT.
	// For "SELECT DISTINCT col1, col2", Columns = ["col1", "col2"].
	// For "SELECT DISTINCT *", Columns = ["*"].
	// Empty when HasDistinct is false.
	Columns []string `json:"columns"`
	// HasCountDistinct is true when one or more COUNT(DISTINCT col) aggregates are present.
	HasCountDistinct bool `json:"has_count_distinct"`
	// CountColumns contains the columns used in COUNT(DISTINCT col) aggregates.
	// For "COUNT(DISTINCT email)", CountColumns = ["email"].
	// Multiple COUNT(DISTINCT) calls result in multiple entries.
	CountColumns []string `json:"count_columns"`
}

// GroupItem represents a single GROUP BY item in a query.
// It captures the expression kind and resolves positional references to aliases/columns.
type GroupItem struct {
	// Kind describes the type of GROUP BY expression.
	// Supported values: "column", "alias", "positional", "function", "expression"
	Kind string `json:"kind"`

	// Column is the bare column name when Kind is "column".
	// For table-qualified columns (t.col), this contains only the column name.
	Column string `json:"column,omitempty"`

	// Table is the table qualifier when present (e.g., "t" in "t.col").
	Table string `json:"table,omitempty"`

	// Alias is the target alias when Kind is "alias" or when a positional
	// reference resolves to an aliased SELECT target.
	Alias string `json:"alias,omitempty"`

	// Position is the 1-based index when Kind is "positional" (GROUP BY 1).
	// Zero when not positional.
	Position int `json:"position,omitempty"`

	// RawExpr is the raw expression string for complex expressions
	// when Kind is "function" or "expression".
	RawExpr string `json:"raw_expr,omitempty"`
}

// TemporalOps represents temporal operations detected in a SQL query.
// It captures NOW() usage, date_trunc() usage, and time-based WHERE ranges.
type TemporalOps struct {
	// HasNow is true when NOW() or CURRENT_TIMESTAMP is present.
	HasNow bool `json:"has_now"`

	// HasDateTrunc is true when DATE_TRUNC() function is present.
	HasDateTrunc bool `json:"has_date_trunc"`

	// WhereRanges contains time-based filter ranges extracted from WHERE clause.
	// Examples: created_at > NOW() - INTERVAL '1 day', timestamp BETWEEN '2024-01-01' AND '2024-12-31'
	WhereRanges []TimeRange `json:"where_ranges"`
}

// TimeRange represents a time-based filter range in a WHERE clause.
type TimeRange struct {
	// Column is the timestamp column name.
	Column string `json:"column"`

	// Table is the table qualifier if present.
	Table string `json:"table,omitempty"`

	// Kind describes the type of time range.
	// Supported values: "now_minus_interval", "between", "greater_than", "less_than"
	Kind string `json:"kind"`

	// Interval is the interval string for NOW() - INTERVAL expressions (e.g., "1 day", "7 days").
	Interval string `json:"interval,omitempty"`

	// StartTime is the start time for BETWEEN expressions (ISO 8601 format preferred).
	StartTime string `json:"start_time,omitempty"`

	// EndTime is the end time for BETWEEN expressions (ISO 8601 format preferred).
	EndTime string `json:"end_time,omitempty"`

	// Operator is the comparison operator (>, <, >=, <=, =).
	Operator string `json:"operator,omitempty"`
}

// JSONPath represents a JSON path extraction in a query.
// It captures column, path components, operator type, and result type.
type JSONPath struct {
	// Table is the table name containing the JSON column.
	Table string `json:"table,omitempty"`

	// Column is the JSON column name (e.g., "data", "profile").
	Column string `json:"column"`

	// Path is the ordered list of path components (e.g., ["settings", "theme"]).
	// For array access, indices are represented as strings (e.g., ["items", "0", "price"]).
	Path []string `json:"path"`

	// Operator is the JSON operator used (->, ->>, #>, #>>).
	Operator string `json:"operator"`

	// IsText is true when the result is text (true for ->> and #>>).
	IsText bool `json:"is_text"`
}

// SlidingWindowInfo represents a sliding window query pattern detected in a WHERE clause.
// A sliding window query filters rows based on a timestamp column compared against a
// relative time expression (for example: ts > NOW() - INTERVAL '1 hour'). This is distinct
// from time-bucketed GROUP BY queries which partition data into fixed buckets.
//
// Example sliding window patterns:
//   - ts > NOW() - INTERVAL '1 hour'                  // rows from last hour
//   - created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days' // rows from last week
//   - updated_at > NOW() - INTERVAL '5 minutes'       // rows from last 5 minutes
//
// This metadata enables the expiry mechanism to automatically remove rows from incremental
// state when they age out of the sliding time window.
type SlidingWindowInfo struct {
	// Enabled is true when a sliding window pattern is detected.
	Enabled bool `json:"enabled"`

	// Column is the timestamp column name used in the time filter (e.g., "ts", "created_at").
	Column string `json:"column"`

	// Table is the table qualifier if present (e.g., "transactions" in "transactions.ts").
	Table string `json:"table,omitempty"`

	// Operator is the comparison operator used (">", ">=", "<", "<=").
	// For sliding windows that look backward in time (e.g., ts > NOW() - INTERVAL '1 hour'),
	// the operator is typically ">" or ">=".
	Operator string `json:"operator"`

	// Interval is the parsed duration of the sliding window (e.g., 1h, 7d).
	// It is derived from the SQL INTERVAL expression using ParseInterval.
	Interval time.Duration `json:"interval"`

	// ReferenceSQL is the raw SQL expression that defines the time boundary
	// (e.g., "now() - interval '1 hour'", "CURRENT_TIMESTAMP - INTERVAL '7 days'").
	// This is preserved for debugging and diagnostics.
	ReferenceSQL string `json:"reference_sql"`
}
