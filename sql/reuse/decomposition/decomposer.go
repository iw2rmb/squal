// Package decomposition performs SQL query decomposition to produce
// a reusable plan for caching and compilation. The decomposer parses
// a query, extracts subqueries and CTEs, analyzes reuse opportunities,
// and returns a DecompositionResult that summarizes the plan structure
// and cacheability signals.
//
// Plan structure
//   - DecompositionResult: top-level container with timestamps and the
//     original SQL. It includes:
//   - Subqueries: normalized fragments with position, dependencies,
//     and caching scores.
//   - CTEs: WITH items (including recursive) with dependency hints.
//   - ReuseOpportunities: matches to existing cached work with
//     match scores and validation flags.
//   - ComplexityScore and CacheableScore: coarse guidance for
//     downstream planners.
//
// Failure behavior
//   - Parse error: DecomposeQuery returns a wrapped error; metrics increment
//     AnalysisErrors; no partial result is returned.
//   - Context timeout/cancel: respects caller deadlines; returns an error
//     and increments AnalysisTimeouts; no partial result is returned.
//   - Optional passes (CTE extraction, reuse analysis): failures are treated
//     as best-effort; the decomposer logs a warning and continues with any
//     successfully computed components.
//
// Thread-safety
//   - The decomposer holds configuration and metrics but launches no
//     background goroutines. Callers manage concurrency; independent
//     instances can be used per request or shared behind external
//     synchronization.
package decomposition

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/iw2rmb/squal/parser"
)

// QueryDecomposer provides the main interface for query decomposition and analysis
type QueryDecomposer interface {
	// Core decomposition functionality
	DecomposeQuery(ctx context.Context, sql string) (*DecompositionResult, error)

	// Subquery analysis
	ExtractSubqueries(query *ParsedQuery) ([]*Subquery, error)
	IdentifyReuseOpportunities(subqueries []*Subquery) ([]*ReuseOpportunity, error)

	// CTE analysis
	ExtractCTEs(query *ParsedQuery) ([]*CTE, error)

	// Configuration and metrics
	UpdateConfig(config *DecompositionConfig) error
	GetMetrics() *DecompositionMetrics
	Reset() error
}

// decomposer implements QueryDecomposer interface
type decomposer struct {
	config            *DecompositionConfig
	subqueryExtractor SubqueryExtractor
	reuseAnalyzer     ReuseAnalyzer
	metrics           *DecompositionMetrics
	parser            parser.Parser
}

// NewQueryDecomposer creates a new QueryDecomposer with default configuration
func NewQueryDecomposer() QueryDecomposer {
	return NewQueryDecomposerWithParser(nil)
}

// NewQueryDecomposerWithConfig creates a QueryDecomposer with custom configuration
func NewQueryDecomposerWithConfig(config *DecompositionConfig) QueryDecomposer {
	return NewQueryDecomposerWithConfigAndParser(config, nil)
}

// NewQueryDecomposerWithParser creates a QueryDecomposer with a custom parser.
func NewQueryDecomposerWithParser(p parser.Parser) QueryDecomposer {
	config := DefaultDecompositionConfig()
	return NewQueryDecomposerWithConfigAndParser(config, p)
}

// NewQueryDecomposerWithConfigAndParser creates a QueryDecomposer with custom configuration and parser.
func NewQueryDecomposerWithConfigAndParser(config *DecompositionConfig, p parser.Parser) QueryDecomposer {
	if p == nil {
		panic("parser must be provided; call sites must inject parser explicitly")
	}
	return &decomposer{
		config:            config,
		subqueryExtractor: NewSubqueryExtractor(config),
		reuseAnalyzer:     NewReuseAnalyzer(config),
		metrics:           &DecompositionMetrics{},
		parser:            p,
	}
}

// DecomposeQuery performs comprehensive query decomposition analysis
func (d *decomposer) DecomposeQuery(ctx context.Context, sql string) (*DecompositionResult, error) {
	startTime := time.Now()

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, d.config.AnalysisTimeoutMS.Duration())
	defer cancel()

	// Parse the SQL query
	parsedQuery, err := ParseSQL(sql, d.parser)
	if err != nil {
		d.metrics.AnalysisErrors++
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	result := &DecompositionResult{
		QueryID:     generateQueryID(sql),
		OriginalSQL: SQLText(sql),
		Timestamp:   startTime,
	}

	// Check for timeout or cancellation
	select {
	case <-timeoutCtx.Done():
		d.metrics.AnalysisTimeouts++
		if timeoutCtx.Err() == context.Canceled {
			return nil, fmt.Errorf("decomposition analysis cancelled: %w", timeoutCtx.Err())
		}
		return nil, fmt.Errorf("decomposition analysis timed out: %w", timeoutCtx.Err())
	default:
	}

	// Extract subqueries
	subqueries, err := d.ExtractSubqueries(parsedQuery)
	if err != nil {
		d.metrics.AnalysisErrors++
		return nil, fmt.Errorf("failed to extract subqueries: %w", err)
	}
	result.Subqueries = subqueries
	d.metrics.SubqueriesExtracted += int64(len(subqueries))

	// Extract CTEs if enabled
	if d.config.EnableCTEAnalysis {
		ctes, err := d.ExtractCTEs(parsedQuery)
		if err != nil {
			log.Printf("Warning: failed to extract CTEs: %v", err)
		} else {
			result.CTEs = ctes
			d.metrics.CTEsExtracted += int64(len(ctes))
		}
	}

	// Identify reuse opportunities
	reuseOpportunities, err := d.IdentifyReuseOpportunities(subqueries)
	if err != nil {
		log.Printf("Warning: failed to identify reuse opportunities: %v", err)
	} else {
		result.ReuseOpportunities = reuseOpportunities
		d.metrics.ReuseOpportunities += int64(len(reuseOpportunities))
	}

	// Calculate complexity score
	result.ComplexityScore = d.calculateComplexityScore(result)

	// Calculate caching score
	result.CacheableScore = d.calculateCacheableScore(result)

	// Record timing
	analysisTime := time.Since(startTime)
	result.DecompositionTime = analysisTime.Nanoseconds()

	// Update metrics
	d.metrics.QueriesAnalyzed++
	if d.metrics.AvgAnalysisTimeNS == 0 {
		d.metrics.AvgAnalysisTimeNS = analysisTime.Nanoseconds()
	} else {
		// Running average
		d.metrics.AvgAnalysisTimeNS = (d.metrics.AvgAnalysisTimeNS + analysisTime.Nanoseconds()) / 2
	}

	return result, nil
}

// ExtractSubqueries extracts all identifiable subqueries from a parsed query
func (d *decomposer) ExtractSubqueries(query *ParsedQuery) ([]*Subquery, error) {
	return d.subqueryExtractor.Extract(query)
}

// IdentifyReuseOpportunities analyzes subqueries for potential cache reuse
func (d *decomposer) IdentifyReuseOpportunities(subqueries []*Subquery) ([]*ReuseOpportunity, error) {
	return d.reuseAnalyzer.AnalyzeReuse(subqueries)
}

// ExtractCTEs extracts Common Table Expressions from a parsed query
func (d *decomposer) ExtractCTEs(query *ParsedQuery) ([]*CTE, error) {
	if query == nil || query.SQL == "" {
		return []*CTE{}, nil
	}

	// Normalize whitespace
	normalizedSQL := regexp.MustCompile(`\s+`).ReplaceAllString(query.SQL, " ")

	// Check if it's a recursive CTE
	isRecursive := regexp.MustCompile(`(?i)WITH\s+RECURSIVE`).MatchString(normalizedSQL)

	var ctes []*CTE

	// Find the WITH block - look for WITH...SELECT pattern
	// Use lazy matching and handle nested parentheses
	withStart := regexp.MustCompile(`(?i)WITH\s+(?:RECURSIVE\s+)?`).FindStringIndex(normalizedSQL)
	if withStart == nil {
		return ctes, nil
	}

	// Find where the main SELECT starts (not inside parentheses)
	mainSelectStart := -1
	parenDepth := 0
	searchSQL := normalizedSQL[withStart[1]:]

	for i := 0; i < len(searchSQL)-6; i++ {
		if searchSQL[i] == '(' {
			parenDepth++
		} else if searchSQL[i] == ')' {
			parenDepth--
		} else if parenDepth == 0 && strings.HasPrefix(strings.ToUpper(searchSQL[i:]), "SELECT") {
			mainSelectStart = i
			break
		}
	}

	if mainSelectStart == -1 {
		return ctes, nil
	}

	// Extract the CTE block
	cteBlock := searchSQL[:mainSelectStart]

	// Pattern to match individual CTEs - handle nested parentheses properly
	// Match: name AS (content)
	ctePattern := regexp.MustCompile(`(\w+)\s+AS\s*\(`)
	cteNameMatches := ctePattern.FindAllStringSubmatchIndex(cteBlock, -1)

	for _, nameMatch := range cteNameMatches {
		if len(nameMatch) < 4 {
			continue
		}

		cteName := cteBlock[nameMatch[2]:nameMatch[3]]

		// Find the matching closing parenthesis for this CTE
		startPos := nameMatch[1]
		parenCount := 1
		endPos := startPos

		for i := startPos; i < len(cteBlock) && parenCount > 0; i++ {
			if cteBlock[i] == '(' {
				parenCount++
			} else if cteBlock[i] == ')' {
				parenCount--
				if parenCount == 0 {
					endPos = i
				}
			}
		}

		if endPos > startPos {
			cteContent := cteBlock[startPos:endPos]

			cte := &CTE{
				Name:          cteName,
				Hash:          string(generateQueryID(cteContent)),
				NormalizedSQL: strings.ToUpper(cteContent),
				OriginalSQL:   cteContent,
				IsRecursive:   isRecursive || strings.Contains(strings.ToUpper(cteContent), "UNION ALL"),
				Dependencies:  d.subqueryExtractor.(*subqueryExtractor).extractTables(cteContent),
				UsageCount:    1,
			}
			ctes = append(ctes, cte)
		}
	}

	return ctes, nil
}

// UpdateConfig updates the decomposer configuration
func (d *decomposer) UpdateConfig(config *DecompositionConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	d.config = config

	// Update component configurations
	d.subqueryExtractor.UpdateConfig(config)
	d.reuseAnalyzer.UpdateConfig(config)

	return nil
}

// GetMetrics returns current decomposition metrics
func (d *decomposer) GetMetrics() *DecompositionMetrics {
	// Return a copy to prevent external modification
	metrics := *d.metrics
	return &metrics
}

// Reset resets metrics and clears internal caches
func (d *decomposer) Reset() error {
	d.metrics = &DecompositionMetrics{}

	// Reset components
	if err := d.subqueryExtractor.Reset(); err != nil {
		return fmt.Errorf("failed to reset subquery extractor: %w", err)
	}

	if err := d.reuseAnalyzer.Reset(); err != nil {
		return fmt.Errorf("failed to reset reuse analyzer: %w", err)
	}

	return nil
}

// calculateComplexityScore calculates a complexity score for the decomposition
func (d *decomposer) calculateComplexityScore(result *DecompositionResult) int {
	score := 10 // Base score

	// Base score from number of subqueries
	score += len(result.Subqueries) * 20

	// Additional score for CTEs (more complex)
	score += len(result.CTEs) * 30

	// Extra score for recursive CTEs (very complex)
	for _, cte := range result.CTEs {
		if cte.IsRecursive {
			score += 40
		}
	}

	// Score based on nesting depth
	maxDepth := 0
	for _, sq := range result.Subqueries {
		if sq.Position.Level > maxDepth {
			maxDepth = sq.Position.Level
		}
	}
	score += maxDepth * 10

	// Score for different subquery types
	for _, sq := range result.Subqueries {
		switch sq.Position.Type {
		case SubqueryTypeCorrelated:
			score += 25
		case SubqueryTypeDerived:
			score += 15
		case SubqueryTypeExists:
			score += 10
		case SubqueryTypeIn:
			score += 8
		case SubqueryTypeScalar:
			score += 5
		}

		// Extra score for parameterized subqueries
		if sq.IsParameterized {
			score += 10
		}
	}

	// Score for reuse opportunities
	score += len(result.ReuseOpportunities) * 5

	return score
}

// calculateCacheableScore calculates how cacheable this query decomposition is
func (d *decomposer) calculateCacheableScore(result *DecompositionResult) float64 {
	if len(result.Subqueries) == 0 {
		// For queries without subqueries, calculate main query cacheability
		return d.calculateMainQueryCacheability(string(result.OriginalSQL))
	}

	totalScore := 0.0
	for _, sq := range result.Subqueries {
		totalScore += sq.CachingScore
	}

	// Average caching score across subqueries
	return totalScore / float64(len(result.Subqueries))
}

// calculateMainQueryCacheability calculates cacheability for main queries without subqueries
func (d *decomposer) calculateMainQueryCacheability(sql string) float64 {
	score := 0.8 // Base score for simple queries

	upperSQL := strings.ToUpper(sql)

	// Reduce score for non-deterministic functions
	nonDeterministicFunctions := []string{"NOW()", "CURRENT_TIMESTAMP", "RAND()", "RANDOM()", "UUID()"}
	for _, fn := range nonDeterministicFunctions {
		if strings.Contains(upperSQL, fn) {
			score -= 0.3
			break
		}
	}

	// Increase score for aggregations
	if strings.Contains(upperSQL, "COUNT") || strings.Contains(upperSQL, "SUM") ||
		strings.Contains(upperSQL, "AVG") || strings.Contains(upperSQL, "GROUP BY") {
		score += 0.2
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

// generateQueryID creates a unique identifier for a query
func generateQueryID(sql string) QueryID {
	hash := md5.Sum([]byte(sql))
	return QueryID(fmt.Sprintf("query_%x", hash)[:16])
}
