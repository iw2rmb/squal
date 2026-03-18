package decomposition

import "time"

// QueryID identifies a query in decomposition workflows.
type QueryID string

// SQLText stores raw SQL text passed through decomposition workflows.
type SQLText string

// Milliseconds stores a timeout in milliseconds and supports duration conversion.
type Milliseconds int64

// Duration converts millisecond count to time.Duration.
func (m Milliseconds) Duration() time.Duration {
	return time.Duration(m) * time.Millisecond
}

// Seconds stores TTL values in seconds and supports duration conversion.
type Seconds int64

// Duration converts second count to time.Duration.
func (s Seconds) Duration() time.Duration {
	return time.Duration(s) * time.Second
}

// DecompositionResult represents the result of query decomposition analysis
type DecompositionResult struct {
	// Original query information
	QueryID     QueryID   `json:"query_id"`
	OriginalSQL SQLText   `json:"original_sql"`
	Timestamp   time.Time `json:"timestamp"`

	// Decomposition analysis results
	Subqueries         []*Subquery         `json:"subqueries"`
	CTEs               []*CTE              `json:"ctes"`
	ReuseOpportunities []*ReuseOpportunity `json:"reuse_opportunities"`

	// Metadata
	ComplexityScore   int     `json:"complexity_score"`
	DecompositionTime int64   `json:"decomposition_time_ns"`
	CacheableScore    float64 `json:"cacheable_score"`
}

// Subquery represents an identified subquery pattern that can be cached independently
type Subquery struct {
	// Identification
	ID            string `json:"id"`
	Hash          string `json:"hash"`
	NormalizedSQL string `json:"normalized_sql"`
	OriginalSQL   string `json:"original_sql"`

	// Positioning in parent query
	Position      SubqueryPosition `json:"position"`
	ParentContext string           `json:"parent_context"`

	// Analysis metadata
	Tables       []string    `json:"tables"`
	Columns      []string    `json:"columns"`
	Parameters   []Parameter `json:"parameters"`
	Dependencies []string    `json:"dependencies"`

	// Caching potential
	CachingScore    float64 `json:"caching_score"`
	EstimatedCost   int64   `json:"estimated_cost"`
	ReuseFrequency  int     `json:"reuse_frequency"`
	IsParameterized bool    `json:"is_parameterized"`
}

// CTE represents a Common Table Expression that can be cached
type CTE struct {
	// Identification
	Name          string `json:"name"`
	Hash          string `json:"hash"`
	NormalizedSQL string `json:"normalized_sql"`
	OriginalSQL   string `json:"original_sql"`

	// CTE metadata
	IsRecursive  bool     `json:"is_recursive"`
	Dependencies []string `json:"dependencies"`
	UsageCount   int      `json:"usage_count"`

	// Caching analysis
	CachingScore  float64 `json:"caching_score"`
	EstimatedCost int64   `json:"estimated_cost"`
}

// ReuseOpportunity represents an opportunity to reuse cached subquery results
type ReuseOpportunity struct {
	// Opportunity identification
	OpportunityID string  `json:"opportunity_id"`
	SubqueryHash  string  `json:"subquery_hash"`
	CachedQueryID QueryID `json:"cached_query_id"`

	// Reuse analysis
	MatchScore      float64 `json:"match_score"`
	ParameterMatch  bool    `json:"parameter_match"`
	EstimatedSaving int64   `json:"estimated_saving_ns"`

	// Validation
	RequiresValidation bool      `json:"requires_validation"`
	ValidatedAt        time.Time `json:"validated_at"`
}

// SubqueryPosition indicates where a subquery appears in the parent query
type SubqueryPosition struct {
	Type   SubqueryType `json:"type"`
	Clause string       `json:"clause"` // SELECT, WHERE, FROM, etc.
	Level  int          `json:"level"`  // Nesting level
	Index  int          `json:"index"`  // Position within clause
}

// SubqueryType categorizes the type of subquery
type SubqueryType string

const (
	SubqueryTypeScalar     SubqueryType = "scalar"     // Single value return
	SubqueryTypeExists     SubqueryType = "exists"     // EXISTS clause
	SubqueryTypeIn         SubqueryType = "in"         // IN clause
	SubqueryTypeCorrelated SubqueryType = "correlated" // Correlated subquery
	SubqueryTypeDerived    SubqueryType = "derived"    // FROM clause subquery
)

// Parameter represents a parameterized value in a subquery
type Parameter struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	Value        interface{} `json:"value"`
	IsCorrelated bool        `json:"is_correlated"`
}

// DecompositionConfig contains configuration for the decomposition process
type DecompositionConfig struct {
	// Analysis thresholds
	MaxSubqueryDepth   int     `json:"max_subquery_depth"`
	MinCachingScore    float64 `json:"min_caching_score"`
	MaxComplexityScore int     `json:"max_complexity_score"`

	// Performance settings
	AnalysisTimeoutMS  Milliseconds `json:"analysis_timeout_ms"`
	EnableCTEAnalysis  bool         `json:"enable_cte_analysis"`
	EnableCorrelatedSQ bool         `json:"enable_correlated_subqueries"`

	// Caching settings
	SubqueryCacheTTL Seconds `json:"subquery_cache_ttl_seconds"`
	MaxSubqueryCache int     `json:"max_subquery_cache_entries"`
}

// DefaultDecompositionConfig returns sensible defaults for decomposition
func DefaultDecompositionConfig() *DecompositionConfig {
	return &DecompositionConfig{
		MaxSubqueryDepth:   5,
		MinCachingScore:    0.5,
		MaxComplexityScore: 100,
		AnalysisTimeoutMS:  1000,
		EnableCTEAnalysis:  true,
		EnableCorrelatedSQ: false, // Start with non-correlated for simplicity
		SubqueryCacheTTL:   3600,  // 1 hour
		MaxSubqueryCache:   1000,
	}
}

// DecompositionMetrics tracks metrics for the decomposition system
type DecompositionMetrics struct {
	// Analysis metrics
	QueriesAnalyzed     int64 `json:"queries_analyzed"`
	SubqueriesExtracted int64 `json:"subqueries_extracted"`
	CTEsExtracted       int64 `json:"ctes_extracted"`

	// Performance metrics
	AvgAnalysisTimeNS int64 `json:"avg_analysis_time_ns"`
	AnalysisTimeouts  int64 `json:"analysis_timeouts"`

	// Reuse metrics
	ReuseOpportunities  int64   `json:"reuse_opportunities"`
	SuccessfulReuses    int64   `json:"successful_reuses"`
	CacheHitImprovement float64 `json:"cache_hit_improvement"`

	// Error metrics
	AnalysisErrors     int64 `json:"analysis_errors"`
	ValidationFailures int64 `json:"validation_failures"`
}

// ValidationResult represents the result of validating a reuse opportunity
type ValidationResult struct {
	OpportunityID string    `json:"opportunity_id"`
	IsValid       bool      `json:"is_valid"`
	ValidatedAt   time.Time `json:"validated_at"`
	ErrorMessage  string    `json:"error_message,omitempty"`

	// Performance comparison
	OriginalExecutionNS int64   `json:"original_execution_ns"`
	CachedExecutionNS   int64   `json:"cached_execution_ns"`
	SpeedupFactor       float64 `json:"speedup_factor"`
}
