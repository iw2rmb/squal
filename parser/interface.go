package parser

// Parser defines the interface for SQL parsing implementations.
type Parser interface {
	// ExtractMetadata parses a SQL query and extracts metadata
	ExtractMetadata(sql string) (*QueryMetadata, error)

	// NormalizeQuery normalizes a SQL query for consistent caching
	NormalizeQuery(sql string) (string, error)

	// GenerateFingerprint generates a unique fingerprint for a query
	GenerateFingerprint(sql string) (string, error)

	// ExtractTables extracts table names from a SQL query
	ExtractTables(sql string) ([]string, error)

	// ExtractCaseAggregates returns CASE-in-aggregate expressions found in SELECT targets.
	// Implementations should return an empty slice when none are present or unsupported.
	// Supported minimum set: SUM(CASE WHEN ... THEN <col|const> ELSE <const> END),
	// COUNT(CASE WHEN ... THEN 1 END).
	ExtractCaseAggregates(sql string) ([]AggCase, error)

	// ExtractAggregateCompositions returns arithmetic compositions of aggregates per target alias.
	// Implementations should flatten '+' and '-' across SUM/COUNT function calls (possibly containing CASE).
	// Non-aggregate terms are ignored.
	ExtractAggregateCompositions(sql string) ([]AggComposition, error)

	// ExtractAggregates returns all aggregate functions (COUNT/SUM/AVG/MIN/MAX) from SELECT targets.
	// Each Aggregate includes function name, optional alias, argument column (or "*" for COUNT(*)),
	// DISTINCT flag, and optional table qualifier. This is the source of truth for aggregate detection,
	// replacing regex-based approaches.
	ExtractAggregates(sql string) ([]Aggregate, error)

	// ExtractDistinctSpec analyzes DISTINCT usage in a query.
	// It returns a DistinctSpec that distinguishes between:
	//   - SELECT DISTINCT (hasDistinct=true, columns populated)
	//   - COUNT(DISTINCT col) (hasCountDistinct=true, countColumns populated)
	// This eliminates the need for regex-based DISTINCT detection.
	// Implementations should return an empty DistinctSpec (all false) when no DISTINCT is present.
	ExtractDistinctSpec(sql string) (*DistinctSpec, error)

	// ExtractGroupBy returns ordered GROUP BY items from a query.
	// It resolves positional references (GROUP BY 1) to the corresponding SELECT target
	// alias or column, and provides the raw expression kind for each item.
	// Supported kinds: "column", "alias", "positional", "function", "expression"
	// This eliminates the need for regex-based GROUP BY detection.
	// Implementations should return an empty slice when no GROUP BY is present.
	ExtractGroupBy(sql string) ([]GroupItem, error)

	// ExtractTemporalOps analyzes temporal operations in a query.
	// It detects:
	//   - NOW() or CURRENT_TIMESTAMP usage (hasNow)
	//   - DATE_TRUNC() function usage (hasDateTrunc)
	//   - Time-based WHERE filters (whereRanges): NOW() - INTERVAL, BETWEEN, comparison operators
	// This eliminates the need for ad-hoc time detection in datetime strategy.
	// Implementations should return an empty TemporalOps (all false, empty slice) when no temporal ops are present.
	ExtractTemporalOps(sql string) (*TemporalOps, error)

	// ExtractJSONPaths extracts all JSON/JSONB path operations from a query.
	// It detects usage of JSON operators (->, ->>, #>, #>>) in SELECT targets and WHERE conditions,
	// extracting the column name, path components, operator type, and result type.
	// This eliminates the need for regex-based JSON path extraction.
	// Implementations should return an empty slice when no JSON operations are present.
	ExtractJSONPaths(sql string) ([]JSONPath, error)

	// DetectSlidingWindow analyzes WHERE clause for sliding window patterns.
	// It detects queries with time-based filters like "ts > NOW() - INTERVAL '1 hour'"
	// that define a rolling time window. This is distinct from time-bucketed GROUP BY
	// queries (detected via HasTimeBucket in QueryMetadata).
	//
	// Supported patterns:
	//   - ts > NOW() - INTERVAL '1 hour'
	//   - created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
	//   - updated_at > NOW() - INTERVAL '5 minutes'
	//
	// Returns nil when no sliding window pattern is detected.
	// The returned SlidingWindowInfo includes the timestamp column, operator,
	// interval duration, and raw SQL expression for the time boundary.
	DetectSlidingWindow(sql string) (*SlidingWindowInfo, error)
}

// testParserFactory provides a factory for constructing a Parser in tests.
// It is registered by the concrete database parser implementation (e.g., Postgres)
// via RegisterTestParserFactory from its CGO build.
var testParserFactory func() Parser

// RegisterTestParserFactory registers a constructor used by tests to obtain
// a concrete parser implementation without introducing an import cycle.
func RegisterTestParserFactory(fn func() Parser) {
	testParserFactory = fn
}

// NewTestParser returns a parser instance for tests when a factory is registered.
// When no factory is registered (e.g., non-CGO build), it returns nil and tests
// should be skipped or run with the required build tags.
func NewTestParser() Parser {
	if testParserFactory == nil {
		panic("parser: PG test parser factory not registered; build with CGO enabled")
	}
	return testParserFactory()
}
