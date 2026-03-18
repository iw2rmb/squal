// Subquery extractor: heuristics vs. parser
//
// This file documents the intent and limits of the subquery extractor.
//
// Heuristics-first
//   - The extractor uses lightweight heuristics (regular expressions and
//     normalization) to identify common subquery shapes: scalar, EXISTS, IN,
//     and derived table subqueries. It does not require an SQL AST and can
//     operate on the raw SQL string for speed and resilience.
//
// Parser relationship
//   - When a real parser is available (see ParsedQuery and parser.Parser), the
//     decomposer still invokes this extractor as a best-effort pass. Results
//     can be cross-validated with parser metadata, but this file intentionally
//     has no hard dependency on an AST to keep failure modes localized.
//
// Known limitations (caveats)
//   - Dialect sensitivity: patterns are tuned for common PostgreSQL-like SQL and
//     may miss or misclassify vendor-specific syntax.
//   - Nesting/parentheses: balanced-parenthesis handling is heuristic; deeply
//     nested constructs and expressions with many parentheses can yield false
//     negatives or partial matches.
//   - Correlated references: detection is minimal; correlated subqueries are not
//     fully classified and parameter extraction may be empty.
//   - Quoted identifiers and schemas: quoted or schema-qualified names, JSON
//     operators, and window/CTE interactions are only partially handled.
//   - Comments/strings: regexes avoid obvious string literals, but complex
//     quoting and inline comments can still confuse matching.
//   - False positives/negatives: this is a best-effort extractor intended to
//     surface cacheable candidates; downstream validation must tolerate errors.
//
// Guidance
//   - Treat outputs as hints. Use normalization and hashing to cluster likely
//     duplicates, then validate before reuse.
//   - Prefer parser-backed extraction when available; keep the heuristic path as
//     a fast fallback and for robustness when parsing fails.
package decomposition

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"strings"
)

// SubqueryExtractor handles extraction and analysis of subqueries from SQL AST
type SubqueryExtractor interface {
	Extract(query *ParsedQuery) ([]*Subquery, error)
	UpdateConfig(config *DecompositionConfig) error
	Reset() error
}

// subqueryExtractor implements SubqueryExtractor interface
type subqueryExtractor struct {
	config          *DecompositionConfig
	extractionRules map[SubqueryType]*ExtractionRule
}

// ExtractionRule defines how to extract and analyze specific types of subqueries
type ExtractionRule struct {
	Type       SubqueryType
	Pattern    *regexp.Regexp
	Analyzer   func(match string, context string) (*Subquery, error)
	CostWeight int
}

// NewSubqueryExtractor creates a new SubqueryExtractor with given configuration
func NewSubqueryExtractor(config *DecompositionConfig) SubqueryExtractor {
	extractor := &subqueryExtractor{
		config:          config,
		extractionRules: make(map[SubqueryType]*ExtractionRule),
	}

	extractor.initializeExtractionRules()
	return extractor
}

// Extract extracts all identifiable subqueries from a parsed query
func (se *subqueryExtractor) Extract(query *ParsedQuery) ([]*Subquery, error) {
	if query == nil || query.SQL == "" {
		return []*Subquery{}, nil
	}

	var subqueries []*Subquery

	// Extract different types of subqueries
	scalarSubqueries, err := se.extractScalarSubqueries(query.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to extract scalar subqueries: %w", err)
	}
	subqueries = append(subqueries, scalarSubqueries...)

	existsSubqueries, err := se.extractExistsSubqueries(query.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to extract EXISTS subqueries: %w", err)
	}
	subqueries = append(subqueries, existsSubqueries...)

	inSubqueries, err := se.extractInSubqueries(query.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to extract IN subqueries: %w", err)
	}
	subqueries = append(subqueries, inSubqueries...)

	derivedTables, err := se.extractDerivedTables(query.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to extract derived tables: %w", err)
	}
	subqueries = append(subqueries, derivedTables...)

	// Enhance subqueries with metadata
	for i, sq := range subqueries {
		enhanced, err := se.enhanceSubqueryMetadata(sq, query)
		if err != nil {
			return nil, fmt.Errorf("failed to enhance subquery metadata: %w", err)
		}
		subqueries[i] = enhanced
	}

	return subqueries, nil
}

// extractScalarSubqueries extracts scalar subqueries (returning single values)
func (se *subqueryExtractor) extractScalarSubqueries(sql string) ([]*Subquery, error) {
	// Normalize whitespace in SQL for pattern matching
	normalizedSQL := regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")

	// Pattern for scalar subqueries in SELECT clause: (SELECT ... FROM ...)
	// Using (?s) flag for dot to match newlines, and lazy matching with *?
	pattern := regexp.MustCompile(`\(\s*SELECT\s+(?:[^()]+|\([^)]*\))*?\s+FROM\s+(?:[^()]+|\([^)]*\))*?\)`)
	matches := pattern.FindAllString(normalizedSQL, -1)

	var subqueries []*Subquery
	for i, match := range matches {
		// Skip if this is part of EXISTS or IN clause
		startIdx := strings.Index(normalizedSQL, match)
		if startIdx > 0 {
			prefix := normalizedSQL[max(0, startIdx-10):startIdx]
			if strings.Contains(strings.ToUpper(prefix), "EXISTS") ||
				strings.Contains(strings.ToUpper(prefix), "IN") {
				continue
			}
		}

		sq := &Subquery{
			ID:            fmt.Sprintf("scalar_%d", i),
			Hash:          generateSubqueryHash(match),
			OriginalSQL:   match,
			NormalizedSQL: se.normalizeSQL(match),
			Position: SubqueryPosition{
				Type:   SubqueryTypeScalar,
				Clause: "SELECT",
				Level:  se.calculateNestingLevel(sql, match),
				Index:  i,
			},
			CachingScore: se.calculateCachingScore(match),
		}
		subqueries = append(subqueries, sq)
	}

	return subqueries, nil
}

// extractExistsSubqueries extracts EXISTS subqueries
func (se *subqueryExtractor) extractExistsSubqueries(sql string) ([]*Subquery, error) {
	// Normalize whitespace in SQL for pattern matching
	normalizedSQL := regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")

	// Pattern for EXISTS subqueries: EXISTS (SELECT ... FROM ...)
	pattern := regexp.MustCompile(`(?i)EXISTS\s*\(\s*SELECT\s+(?:[^()]+|\([^)]*\))*?\s+FROM\s+(?:[^()]+|\([^)]*\))*?\)`)
	matches := pattern.FindAllString(normalizedSQL, -1)

	var subqueries []*Subquery
	for i, match := range matches {
		// Extract just the SELECT part
		selectPattern := regexp.MustCompile(`\(\s*SELECT\s+(?:[^()]+|\([^)]*\))*?\s+FROM\s+(?:[^()]+|\([^)]*\))*?\)`)
		selectMatch := selectPattern.FindString(match)

		sq := &Subquery{
			ID:            fmt.Sprintf("exists_%d", i),
			Hash:          generateSubqueryHash(selectMatch),
			OriginalSQL:   selectMatch,
			NormalizedSQL: se.normalizeSQL(selectMatch),
			Position: SubqueryPosition{
				Type:   SubqueryTypeExists,
				Clause: "WHERE",
				Level:  se.calculateNestingLevel(sql, match),
				Index:  i,
			},
			CachingScore: se.calculateCachingScore(selectMatch),
		}
		subqueries = append(subqueries, sq)
	}

	return subqueries, nil
}

// extractInSubqueries extracts IN subqueries
func (se *subqueryExtractor) extractInSubqueries(sql string) ([]*Subquery, error) {
	// Normalize whitespace in SQL for pattern matching
	normalizedSQL := regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")

	// Pattern for IN subqueries: column IN (SELECT ... FROM ...)
	// Must have a column or expression before IN
	pattern := regexp.MustCompile(`(?i)\w+\s+IN\s*\(\s*SELECT\s+(?:[^()]+|\([^)]*\))*?\s+FROM\s+(?:[^()]+|\([^)]*\))*?\)`)
	matches := pattern.FindAllString(normalizedSQL, -1)

	var subqueries []*Subquery
	for i, match := range matches {
		// Extract just the SELECT part
		selectPattern := regexp.MustCompile(`\(\s*SELECT\s+(?:[^()]+|\([^)]*\))*?\s+FROM\s+(?:[^()]+|\([^)]*\))*?\)`)
		selectMatch := selectPattern.FindString(match)

		sq := &Subquery{
			ID:            fmt.Sprintf("in_%d", i),
			Hash:          generateSubqueryHash(selectMatch),
			OriginalSQL:   selectMatch,
			NormalizedSQL: se.normalizeSQL(selectMatch),
			Position: SubqueryPosition{
				Type:   SubqueryTypeIn,
				Clause: "WHERE",
				Level:  se.calculateNestingLevel(sql, match),
				Index:  i,
			},
			CachingScore: se.calculateCachingScore(selectMatch),
		}
		subqueries = append(subqueries, sq)
	}

	return subqueries, nil
}

// extractDerivedTables extracts derived table subqueries (FROM clause subqueries)
func (se *subqueryExtractor) extractDerivedTables(sql string) ([]*Subquery, error) {
	// Normalize whitespace in SQL for pattern matching
	normalizedSQL := regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")

	// Pattern for derived tables: FROM (SELECT ... FROM ...) AS alias or JOIN (SELECT ...)
	pattern := regexp.MustCompile(`(?i)(?:FROM|JOIN)\s*\(\s*SELECT\s+(?:[^()]+|\([^)]*\))*?\s+FROM\s+(?:[^()]+|\([^)]*\))*?\)\s*(?:AS\s+\w+|\w+)?`)
	matches := pattern.FindAllString(normalizedSQL, -1)

	var subqueries []*Subquery
	for i, match := range matches {
		// Extract just the SELECT part
		selectPattern := regexp.MustCompile(`\(\s*SELECT\s+(?:[^()]+|\([^)]*\))*?\s+FROM\s+(?:[^()]+|\([^)]*\))*?\)`)
		selectMatch := selectPattern.FindString(match)

		sq := &Subquery{
			ID:            fmt.Sprintf("derived_%d", i),
			Hash:          generateSubqueryHash(selectMatch),
			OriginalSQL:   selectMatch,
			NormalizedSQL: se.normalizeSQL(selectMatch),
			Position: SubqueryPosition{
				Type:   SubqueryTypeDerived,
				Clause: "FROM",
				Level:  se.calculateNestingLevel(sql, match),
				Index:  i,
			},
			CachingScore: se.calculateCachingScore(selectMatch),
		}
		subqueries = append(subqueries, sq)
	}

	return subqueries, nil
}

// enhanceSubqueryMetadata adds additional metadata to subqueries
func (se *subqueryExtractor) enhanceSubqueryMetadata(sq *Subquery, query *ParsedQuery) (*Subquery, error) {
	// Extract tables referenced in subquery
	sq.Tables = se.extractTables(sq.NormalizedSQL)

	// Extract columns referenced in subquery
	sq.Columns = se.extractColumns(sq.NormalizedSQL)

	// Detect parameters and correlated references
	sq.Parameters = se.extractParameters(sq.OriginalSQL, query.SQL)

	// Check if subquery is parameterized
	sq.IsParameterized = len(sq.Parameters) > 0

	// Calculate estimated cost
	sq.EstimatedCost = se.estimateSubqueryCost(sq)

	// Initialize reuse frequency
	sq.ReuseFrequency = 1

	return sq, nil
}

// normalizeSQL normalizes SQL for consistent comparison
func (se *subqueryExtractor) normalizeSQL(sql string) string {
	// Remove extra whitespace
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(sql), " ")

	// Convert to uppercase for consistency
	sql = strings.ToUpper(sql)

	// Replace literal values with placeholders for better reuse
	// Numbers
	sql = regexp.MustCompile(`\b\d+\b`).ReplaceAllString(sql, "?")

	// String literals
	sql = regexp.MustCompile(`'[^']*'`).ReplaceAllString(sql, "?")

	// Normalize correlated references for better matching
	// Replace table.column patterns with generic parameter
	sql = regexp.MustCompile(`\b[A-Z]+\.[A-Z_]+\b`).ReplaceAllString(sql, "?.?")

	return sql
}

// calculateNestingLevel calculates how deeply nested a subquery is
func (se *subqueryExtractor) calculateNestingLevel(fullSQL, subquerySQL string) int {
	// Count opening parentheses before the subquery
	index := strings.Index(fullSQL, subquerySQL)
	if index == -1 {
		return 1
	}

	level := 0
	for i := 0; i < index; i++ {
		if fullSQL[i] == '(' {
			level++
		} else if fullSQL[i] == ')' {
			level--
		}
	}

	if level < 1 {
		return 1
	}
	return level
}

// calculateCachingScore calculates how suitable a subquery is for caching
func (se *subqueryExtractor) calculateCachingScore(sql string) float64 {
	score := 1.0

	// Higher score for simple SELECT statements
	if strings.Contains(strings.ToUpper(sql), "SELECT") {
		score += 0.3
	}

	// Lower score for complex operations
	sqlUpper := strings.ToUpper(sql)
	if strings.Contains(sqlUpper, "ORDER BY") {
		score += 0.1
	}
	if strings.Contains(sqlUpper, "GROUP BY") {
		score += 0.2
	}
	if strings.Contains(sqlUpper, "DISTINCT") {
		score += 0.1
	}

	// Lower score for functions that may be non-deterministic
	nonDeterministic := []string{"NOW()", "RANDOM()", "RAND()", "UUID()"}
	for _, fn := range nonDeterministic {
		if strings.Contains(sqlUpper, fn) {
			score -= 0.5
			break
		}
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

// extractTables extracts table names from SQL
func (se *subqueryExtractor) extractTables(sql string) []string {
	// Pattern to extract table names from FROM clause (handles aliases)
	pattern := regexp.MustCompile(`(?i)FROM\s+(\w+)(?:\s+(?:AS\s+)?\w+)?`)
	matches := pattern.FindAllStringSubmatch(sql, -1)

	var tables []string
	seen := make(map[string]bool)

	for _, match := range matches {
		if len(match) > 1 {
			table := strings.ToLower(match[1])
			if !seen[table] {
				tables = append(tables, table)
				seen[table] = true
			}
		}
	}

	// Also check for JOIN clauses
	joinPattern := regexp.MustCompile(`(?i)JOIN\s+(\w+)(?:\s+(?:AS\s+)?\w+)?`)
	joinMatches := joinPattern.FindAllStringSubmatch(sql, -1)
	for _, match := range joinMatches {
		if len(match) > 1 {
			table := strings.ToLower(match[1])
			if !seen[table] {
				tables = append(tables, table)
				seen[table] = true
			}
		}
	}

	return tables
}

// extractColumns extracts column names from SQL
func (se *subqueryExtractor) extractColumns(sql string) []string {
	// Simple pattern to extract columns from SELECT clause
	// This is a simplified implementation - real implementation would need AST parsing
	pattern := regexp.MustCompile(`(?i)SELECT\s+([^FROM]+)`)
	match := pattern.FindStringSubmatch(sql)

	if len(match) < 2 {
		return []string{}
	}

	columnsStr := strings.TrimSpace(match[1])
	if columnsStr == "*" {
		return []string{"*"}
	}

	// Split by comma and clean up
	columns := strings.Split(columnsStr, ",")
	var cleanColumns []string
	for _, col := range columns {
		col = strings.TrimSpace(col)
		if col != "" {
			cleanColumns = append(cleanColumns, col)
		}
	}

	return cleanColumns
}

// extractParameters extracts parameters from subqueries
func (se *subqueryExtractor) extractParameters(subquerySQL, fullSQL string) []Parameter {
	// For now, return empty slice - full implementation would detect correlated references
	return []Parameter{}
}

// estimateSubqueryCost estimates the execution cost of a subquery
func (se *subqueryExtractor) estimateSubqueryCost(sq *Subquery) int64 {
	// Simple heuristic-based cost estimation
	baseCost := int64(100) // Base cost in microseconds

	// Add cost based on complexity
	sqlUpper := strings.ToUpper(sq.NormalizedSQL)

	if strings.Contains(sqlUpper, "JOIN") {
		baseCost += 200
	}
	if strings.Contains(sqlUpper, "ORDER BY") {
		baseCost += 150
	}
	if strings.Contains(sqlUpper, "GROUP BY") {
		baseCost += 100
	}
	if strings.Contains(sqlUpper, "DISTINCT") {
		baseCost += 75
	}

	// Add cost based on nesting level
	baseCost += int64(sq.Position.Level * 50)

	return baseCost
}

// UpdateConfig updates the extractor configuration
func (se *subqueryExtractor) UpdateConfig(config *DecompositionConfig) error {
	se.config = config
	return nil
}

// Reset resets the extractor state
func (se *subqueryExtractor) Reset() error {
	// Reset any cached state if needed
	return nil
}

// initializeExtractionRules sets up the extraction rules for different subquery types
func (se *subqueryExtractor) initializeExtractionRules() {
	// Rules are currently embedded in individual extraction methods
	// This method is reserved for future extensibility
}

// generateSubqueryHash generates a hash for a subquery
func generateSubqueryHash(sql string) string {
	hash := md5.Sum([]byte(sql))
	return fmt.Sprintf("sq_%x", hash)[:12]
}
