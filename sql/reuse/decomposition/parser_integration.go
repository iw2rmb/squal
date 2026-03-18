package decomposition

import (
	"fmt"
	"strings"

	"github.com/iw2rmb/squal/parser"
)

// ParsedQuery represents a parsed SQL query for decomposition analysis
type ParsedQuery struct {
	SQL        string                `json:"sql"`
	Normalized string                `json:"normalized"`
	Metadata   *parser.QueryMetadata `json:"metadata"`
	AST        interface{}           `json:"ast,omitempty"` // sqlparser.Statement
}

// ParseSQL parses a SQL query and returns a ParsedQuery suitable for decomposition.
// Requires a non-nil parser implementation.
func ParseSQL(sql string, p parser.Parser) (*ParsedQuery, error) {
	if sql == "" {
		return nil, fmt.Errorf("SQL cannot be empty")
	}

	// Basic SQL validation - check for obvious invalid patterns
	if err := validateBasicSQL(sql); err != nil {
		return nil, fmt.Errorf("invalid SQL: %w", err)
	}

	if p == nil {
		return nil, fmt.Errorf("parser cannot be nil")
	}

	// Extract comprehensive metadata including advanced features
	metadata, err := p.ExtractMetadata(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to extract metadata: %w", err)
	}

	// Additional validation based on extracted metadata
	if err := validateExtractedMetadata(metadata, sql); err != nil {
		return nil, fmt.Errorf("SQL validation failed: %w", err)
	}

	// Normalize the query for consistent processing
	normalized, err := p.NormalizeQuery(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize query: %w", err)
	}

	return &ParsedQuery{
		SQL:        sql,
		Normalized: normalized,
		Metadata:   metadata,
		AST:        nil, // Could be populated with actual AST if needed
	}, nil
}

// validateBasicSQL performs basic SQL syntax validation
func validateBasicSQL(sql string) error {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))

	// Check for some obvious invalid patterns
	if strings.Contains(upperSQL, "SYNTAX ERROR") {
		return fmt.Errorf("SQL contains syntax error keywords")
	}

	if strings.Contains(upperSQL, "INVALID") && !strings.Contains(upperSQL, "WHERE") {
		return fmt.Errorf("SQL appears to contain invalid syntax")
	}

	// Check for basic SQL statement structure
	validStarts := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "WITH"}
	hasValidStart := false
	for _, start := range validStarts {
		if strings.HasPrefix(upperSQL, start) {
			hasValidStart = true
			break
		}
	}

	if !hasValidStart {
		return fmt.Errorf("SQL does not start with a valid statement keyword")
	}

	return nil
}

// validateExtractedMetadata validates that the extracted metadata makes sense
func validateExtractedMetadata(metadata *parser.QueryMetadata, sql string) error {
	if metadata == nil {
		return fmt.Errorf("metadata extraction returned nil")
	}

	// If it starts with SELECT, it should have some valid structure
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upperSQL, "SELECT") {
		// SELECT queries should have operations
		if len(metadata.Operations) == 0 {
			return fmt.Errorf("SELECT query should have operations metadata")
		}

		// Should identify at least some structure
		if len(metadata.Tables) == 0 && !strings.Contains(upperSQL, "DUAL") {
			// Check if it's a synthetic query or has obvious table reference
			if strings.Contains(upperSQL, "FROM") && !strings.Contains(upperSQL, "DUAL") {
				return fmt.Errorf("SELECT with FROM clause should identify tables")
			}
		}
	}

	return nil
}

// HasSubqueries checks if the parsed query contains subqueries
func (pq *ParsedQuery) HasSubqueries() bool {
	return pq.Metadata != nil && pq.Metadata.HasSubquery
}

// HasCTEs checks if the parsed query contains Common Table Expressions
func (pq *ParsedQuery) HasCTEs() bool {
	return pq.Metadata != nil && pq.Metadata.HasCTEs
}

// HasWindowFunctions checks if the parsed query contains window functions
func (pq *ParsedQuery) HasWindowFunctions() bool {
	return pq.Metadata != nil && pq.Metadata.HasWindowFunctions
}

// HasDistinct checks if the parsed query contains DISTINCT operations.
// This returns true for SELECT DISTINCT queries or queries with COUNT(DISTINCT).
func (pq *ParsedQuery) HasDistinct() bool {
	return pq.Metadata != nil && pq.Metadata.HasDistinct
}

// GetDistinctColumns returns the columns specified in SELECT DISTINCT.
// For "SELECT DISTINCT col1, col2 FROM t", this returns ["col1", "col2"].
// For COUNT(DISTINCT col) without SELECT DISTINCT, the columns are in
// the DistinctSpec returned by the parser (via ExtractDistinctSpec).
// Returns empty slice if no DISTINCT is present or metadata is nil.
func (pq *ParsedQuery) GetDistinctColumns() []string {
	if pq.Metadata == nil || len(pq.Metadata.DistinctColumns) == 0 {
		return []string{}
	}
	return pq.Metadata.DistinctColumns
}

// GetTables returns the list of tables referenced in the query
func (pq *ParsedQuery) GetTables() []string {
	if pq.Metadata == nil {
		return []string{}
	}
	return pq.Metadata.Tables
}

// GetColumns returns the list of columns selected in the query
func (pq *ParsedQuery) GetColumns() []string {
	if pq.Metadata == nil {
		return []string{}
	}
	return pq.Metadata.Columns
}

// IsAggregate checks if the query contains aggregation functions
func (pq *ParsedQuery) IsAggregate() bool {
	return pq.Metadata != nil && pq.Metadata.IsAggregate
}

// GetAggregations returns the list of aggregation functions in the query
func (pq *ParsedQuery) GetAggregations() []string {
	if pq.Metadata == nil {
		return []string{}
	}
	return pq.Metadata.Aggregations
}

// GetComplexityScore calculates a complexity score for the query
func (pq *ParsedQuery) GetComplexityScore() int {
	if pq.Metadata == nil {
		return 1
	}

	score := 1 // Base score

	// Add score based on various factors
	score += len(pq.Metadata.Tables) * 2
	score += len(pq.Metadata.Columns)
	score += len(pq.Metadata.Aggregations) * 3
	score += len(pq.Metadata.JoinConditions) * 5

	if pq.Metadata.HasSubquery {
		score += len(pq.Metadata.Subqueries) * 10
	}

	if pq.Metadata.HasCTEs {
		score += len(pq.Metadata.CTENames) * 8
	}

	if pq.Metadata.HasWindowFunctions {
		score += len(pq.Metadata.WindowFunctions) * 6
	}

	if pq.Metadata.HasDistinct {
		score += 4
	}

	if pq.Metadata.HasDatabaseSpecificOps {
		score += len(pq.Metadata.DatabaseOperations) * 3
	}

	return score
}

// GetCacheabilityScore calculates how cacheable this query is
func (pq *ParsedQuery) GetCacheabilityScore() float64 {
	if pq.Metadata == nil {
		return 0.0
	}

	score := 1.0

	// Higher score for deterministic operations
	if len(pq.Metadata.Operations) > 0 && pq.Metadata.Operations[0] == "SELECT" {
		score += 0.3
	}

	// Lower score for operations that might not be deterministic
	for _, op := range pq.Metadata.DatabaseOperations {
		if op == "NOW" || op == "RANDOM" || op == "UUID" {
			score -= 0.4
			break
		}
	}

	// Aggregations are generally good for caching
	if pq.Metadata.IsAggregate {
		score += 0.2
	}

	// Window functions can be cached but are more complex
	if pq.Metadata.HasWindowFunctions {
		score += 0.1
	}

	// CTEs can improve cacheable if they're reused
	if pq.Metadata.HasCTEs {
		score += 0.15
	}

	// Ensure score is between 0 and 1
	if score < 0 {
		score = 0
	}
	if score > 1 {
		score = 1
	}

	return score
}

// String returns a string representation of the parsed query
func (pq *ParsedQuery) String() string {
	if pq.Metadata == nil {
		return fmt.Sprintf("ParsedQuery{SQL: %q}", pq.SQL)
	}

	return fmt.Sprintf("ParsedQuery{SQL: %q, Tables: %v, Operations: %v, HasSubqueries: %v}",
		pq.SQL, pq.Metadata.Tables, pq.Metadata.Operations, pq.Metadata.HasSubquery)
}

// QueryComplexityAnalyzer provides analysis of query complexity for decomposition decisions
type QueryComplexityAnalyzer struct {
	thresholds ComplexityThresholds
}

// ComplexityThresholds defines thresholds for different complexity levels
type ComplexityThresholds struct {
	MinCacheableScore      float64 `json:"min_cacheable_score"`
	MaxSimpleComplexity    int     `json:"max_simple_complexity"`
	MaxMediumComplexity    int     `json:"max_medium_complexity"`
	DecompositionThreshold int     `json:"decomposition_threshold"`
}

// DefaultComplexityThresholds returns sensible defaults for complexity analysis
func DefaultComplexityThresholds() ComplexityThresholds {
	return ComplexityThresholds{
		MinCacheableScore:      0.5,
		MaxSimpleComplexity:    10,
		MaxMediumComplexity:    25,
		DecompositionThreshold: 15,
	}
}

// NewQueryComplexityAnalyzer creates a new complexity analyzer
func NewQueryComplexityAnalyzer() *QueryComplexityAnalyzer {
	return &QueryComplexityAnalyzer{
		thresholds: DefaultComplexityThresholds(),
	}
}

// AnalyzeComplexity analyzes the complexity of a parsed query
func (qca *QueryComplexityAnalyzer) AnalyzeComplexity(pq *ParsedQuery) *ComplexityAnalysis {
	return &ComplexityAnalysis{
		ComplexityScore:         pq.GetComplexityScore(),
		CacheabilityScore:       pq.GetCacheabilityScore(),
		DecompositionWorthwhile: pq.GetComplexityScore() >= qca.thresholds.DecompositionThreshold,
		ComplexityLevel:         qca.categorizeComplexity(pq.GetComplexityScore()),
		RecommendedAction:       qca.recommendAction(pq),
	}
}

// ComplexityAnalysis represents the result of complexity analysis
type ComplexityAnalysis struct {
	ComplexityScore         int             `json:"complexity_score"`
	CacheabilityScore       float64         `json:"cacheability_score"`
	DecompositionWorthwhile bool            `json:"decomposition_worthwhile"`
	ComplexityLevel         ComplexityLevel `json:"complexity_level"`
	RecommendedAction       string          `json:"recommended_action"`
}

// ComplexityLevel represents different levels of query complexity
type ComplexityLevel string

const (
	ComplexitySimple  ComplexityLevel = "simple"
	ComplexityMedium  ComplexityLevel = "medium"
	ComplexityHigh    ComplexityLevel = "high"
	ComplexityExtreme ComplexityLevel = "extreme"
)

// categorizeComplexity categorizes the complexity level based on score
func (qca *QueryComplexityAnalyzer) categorizeComplexity(score int) ComplexityLevel {
	if score <= qca.thresholds.MaxSimpleComplexity {
		return ComplexitySimple
	} else if score <= qca.thresholds.MaxMediumComplexity {
		return ComplexityMedium
	} else if score <= 50 {
		return ComplexityHigh
	} else {
		return ComplexityExtreme
	}
}

// recommendAction recommends the best action based on query analysis
func (qca *QueryComplexityAnalyzer) recommendAction(pq *ParsedQuery) string {
	complexity := pq.GetComplexityScore()
	cacheability := pq.GetCacheabilityScore()

	if cacheability < qca.thresholds.MinCacheableScore {
		return "skip_caching" // Not worth caching
	}

	if complexity < qca.thresholds.DecompositionThreshold {
		return "simple_caching" // Cache as-is without decomposition
	}

	if pq.HasSubqueries() || pq.HasCTEs() {
		return "decomposition_recommended" // Decompose for better caching
	}

	if pq.HasWindowFunctions() || pq.HasDistinct() {
		return "advanced_strategies" // Use advanced incremental strategies
	}

	return "standard_caching" // Standard caching approach
}
