package decomposition

import (
	"crypto/md5"
	"fmt"
	"strings"
	"time"
)

// ReuseAnalyzer analyzes subqueries for potential cache reuse opportunities
type ReuseAnalyzer interface {
	AnalyzeReuse(subqueries []*Subquery) ([]*ReuseOpportunity, error)
	FindMatchingCachedQueries(subquery *Subquery) ([]*CachedQueryMatch, error)
	ValidateReuseOpportunity(opportunity *ReuseOpportunity) (*ValidationResult, error)
	UpdateConfig(config *DecompositionConfig) error
	Reset() error
}

// CachedQueryMatch represents a potential match with a cached query
type CachedQueryMatch struct {
	QueryID      QueryID     `json:"query_id"`
	SubqueryHash string      `json:"subquery_hash"`
	MatchScore   float64     `json:"match_score"`
	CacheEntry   *CacheEntry `json:"cache_entry"`
	LastAccessed time.Time   `json:"last_accessed"`
}

// CacheEntry represents a cached query entry (simplified for this implementation)
type CacheEntry struct {
	QueryID   QueryID     `json:"query_id"`
	SQL       string      `json:"sql"`
	Hash      string      `json:"hash"`
	Result    interface{} `json:"result"`
	CreatedAt time.Time   `json:"created_at"`
	LastUsed  time.Time   `json:"last_used"`
	HitCount  int64       `json:"hit_count"`
	TTL       int64       `json:"ttl_seconds"`
}

// reuseAnalyzer implements ReuseAnalyzer interface
type reuseAnalyzer struct {
	config        *DecompositionConfig
	cachedQueries map[QueryID]*CacheEntry // Simplified cache simulation
	reuseHistory  map[string][]*ReuseOpportunity
}

// NewReuseAnalyzer creates a new ReuseAnalyzer with given configuration
func NewReuseAnalyzer(config *DecompositionConfig) ReuseAnalyzer {
	return &reuseAnalyzer{
		config:        config,
		cachedQueries: make(map[QueryID]*CacheEntry),
		reuseHistory:  make(map[string][]*ReuseOpportunity),
	}
}

// AnalyzeReuse analyzes subqueries for potential reuse opportunities
func (ra *reuseAnalyzer) AnalyzeReuse(subqueries []*Subquery) ([]*ReuseOpportunity, error) {
	opportunities := make([]*ReuseOpportunity, 0)

	for _, subquery := range subqueries {
		// Skip if caching score is too low
		if subquery.CachingScore < ra.config.MinCachingScore {
			continue
		}

		// Find matching cached queries
		matches, err := ra.FindMatchingCachedQueries(subquery)
		if err != nil {
			return nil, fmt.Errorf("failed to find matching cached queries for %s: %w", subquery.ID, err)
		}

		// Create reuse opportunities from matches
		for _, match := range matches {
			opportunity := &ReuseOpportunity{
				OpportunityID:      generateOpportunityID(subquery.Hash, string(match.QueryID)),
				SubqueryHash:       subquery.Hash,
				CachedQueryID:      match.QueryID,
				MatchScore:         match.MatchScore,
				ParameterMatch:     ra.checkParameterCompatibility(subquery, match.CacheEntry),
				EstimatedSaving:    ra.estimateTimeSaving(subquery, match.CacheEntry),
				RequiresValidation: match.MatchScore < 1.0, // Perfect matches don't need validation
			}

			opportunities = append(opportunities, opportunity)
		}

		// Store for reuse tracking
		ra.reuseHistory[subquery.Hash] = append(ra.reuseHistory[subquery.Hash], opportunities...)
	}

	// Sort opportunities by estimated saving (descending)
	ra.sortOpportunitiesByValue(opportunities)

	return opportunities, nil
}

// FindMatchingCachedQueries finds cached queries that might be reusable for a subquery
func (ra *reuseAnalyzer) FindMatchingCachedQueries(subquery *Subquery) ([]*CachedQueryMatch, error) {
	var matches []*CachedQueryMatch

	// Search through cached queries
	for queryID, cacheEntry := range ra.cachedQueries {
		matchScore := ra.calculateMatchScore(subquery, cacheEntry)

		// Only consider matches above minimum threshold
		if matchScore >= ra.config.MinCachingScore {
			match := &CachedQueryMatch{
				QueryID:      queryID,
				SubqueryHash: subquery.Hash,
				MatchScore:   matchScore,
				CacheEntry:   cacheEntry,
				LastAccessed: cacheEntry.LastUsed,
			}
			matches = append(matches, match)
		}
	}

	return matches, nil
}

// ValidateReuseOpportunity validates that a reuse opportunity is actually beneficial
func (ra *reuseAnalyzer) ValidateReuseOpportunity(opportunity *ReuseOpportunity) (*ValidationResult, error) {
	result := &ValidationResult{
		OpportunityID: opportunity.OpportunityID,
		ValidatedAt:   time.Now(),
		IsValid:       true,
	}

	// Check if the cached entry still exists
	cacheEntry, exists := ra.cachedQueries[opportunity.CachedQueryID]
	if !exists {
		result.IsValid = false
		result.ErrorMessage = "cached query no longer exists"
		return result, nil
	}

	// Check if cache entry is still fresh
	if ra.isCacheEntryExpired(cacheEntry) {
		result.IsValid = false
		result.ErrorMessage = "cached query has expired"
		return result, nil
	}

	// Estimate performance benefit
	result.OriginalExecutionNS = ra.estimateOriginalExecutionTime(opportunity.SubqueryHash)
	result.CachedExecutionNS = ra.estimateCacheRetrievalTime(cacheEntry)

	if result.OriginalExecutionNS > 0 && result.CachedExecutionNS > 0 {
		result.SpeedupFactor = float64(result.OriginalExecutionNS) / float64(result.CachedExecutionNS)

		// Only consider valid if there's a meaningful speedup
		if result.SpeedupFactor < 2.0 {
			result.IsValid = false
			result.ErrorMessage = "insufficient performance benefit"
		}
	}

	return result, nil
}

// calculateMatchScore calculates how well a subquery matches a cached query
func (ra *reuseAnalyzer) calculateMatchScore(subquery *Subquery, cacheEntry *CacheEntry) float64 {
	score := 0.0

	// Exact hash match gives perfect score
	if subquery.Hash == cacheEntry.Hash {
		return 1.0
	}

	// Normalized SQL comparison
	normalizedSubquery := ra.normalizeForComparison(subquery.NormalizedSQL)
	normalizedCache := ra.normalizeForComparison(cacheEntry.SQL)

	if normalizedSubquery == normalizedCache {
		score = 0.95
	} else {
		// String similarity comparison
		score = ra.calculateStringSimilarity(normalizedSubquery, normalizedCache)
	}

	// Adjust score based on cache entry quality
	if cacheEntry.HitCount > 10 {
		score += 0.05 // Frequently used cache entries are more valuable
	}

	if time.Since(cacheEntry.LastUsed) < time.Hour {
		score += 0.03 // Recently used entries are more reliable
	}

	// Table overlap bonus
	subqueryTables := subquery.Tables
	cacheTables := ra.extractTablesFromSQL(cacheEntry.SQL)
	tableOverlap := ra.calculateTableOverlap(subqueryTables, cacheTables)
	score += tableOverlap * 0.1

	return min(1.0, score)
}

// checkParameterCompatibility checks if parameters between subquery and cache are compatible
func (ra *reuseAnalyzer) checkParameterCompatibility(subquery *Subquery, cacheEntry *CacheEntry) bool {
	// For now, assume parameters are compatible if both are parameterized or both are not
	// Real implementation would need detailed parameter analysis
	subqueryParameterized := subquery.IsParameterized
	cacheParameterized := strings.Contains(cacheEntry.SQL, "?")

	return subqueryParameterized == cacheParameterized
}

// estimateTimeSaving estimates the time saving from reusing a cached query
func (ra *reuseAnalyzer) estimateTimeSaving(subquery *Subquery, cacheEntry *CacheEntry) int64 {
	// Estimate original execution time
	originalTime := subquery.EstimatedCost * 1000 // Convert to nanoseconds

	// Estimate cache retrieval time (much faster)
	cacheRetrievalTime := int64(1000) // 1 microsecond

	// Add network/processing overhead
	cacheRetrievalTime += int64(100) // 100 nanoseconds overhead

	return originalTime - cacheRetrievalTime
}

// normalizeForComparison normalizes SQL for comparison purposes
func (ra *reuseAnalyzer) normalizeForComparison(sql string) string {
	// Additional normalization beyond what's done in subquery extraction
	normalized := strings.ToUpper(strings.TrimSpace(sql))

	// Remove extra spaces
	normalized = strings.Join(strings.Fields(normalized), " ")

	// Remove parentheses for comparison
	normalized = strings.ReplaceAll(normalized, "(", "")
	normalized = strings.ReplaceAll(normalized, ")", "")

	return normalized
}

// calculateStringSimilarity calculates similarity between two strings
func (ra *reuseAnalyzer) calculateStringSimilarity(s1, s2 string) float64 {
	// Simple Jaccard similarity using words
	words1 := strings.Fields(s1)
	words2 := strings.Fields(s2)

	set1 := make(map[string]bool)
	set2 := make(map[string]bool)

	for _, word := range words1 {
		set1[word] = true
	}
	for _, word := range words2 {
		set2[word] = true
	}

	// Calculate intersection and union
	intersection := 0
	union := len(set1)

	for word := range set2 {
		if set1[word] {
			intersection++
		} else {
			union++
		}
	}

	if union == 0 {
		return 0.0
	}

	return float64(intersection) / float64(union)
}

// extractTablesFromSQL extracts table names from SQL (simplified)
func (ra *reuseAnalyzer) extractTablesFromSQL(sql string) []string {
	// Use the same logic as in subquery extractor
	extractor := &subqueryExtractor{}
	return extractor.extractTables(sql)
}

// calculateTableOverlap calculates overlap ratio between two table lists
func (ra *reuseAnalyzer) calculateTableOverlap(tables1, tables2 []string) float64 {
	if len(tables1) == 0 || len(tables2) == 0 {
		return 0.0
	}

	set1 := make(map[string]bool)
	for _, table := range tables1 {
		set1[table] = true
	}

	overlap := 0
	for _, table := range tables2 {
		if set1[table] {
			overlap++
		}
	}

	maxTables := max(len(tables1), len(tables2))
	return float64(overlap) / float64(maxTables)
}

// isCacheEntryExpired checks if a cache entry has expired
func (ra *reuseAnalyzer) isCacheEntryExpired(entry *CacheEntry) bool {
	if entry.TTL <= 0 {
		return false // No TTL means no expiration
	}

	expiration := entry.CreatedAt.Add(time.Duration(entry.TTL) * time.Second)
	return time.Now().After(expiration)
}

// estimateOriginalExecutionTime estimates how long the subquery would take to execute
func (ra *reuseAnalyzer) estimateOriginalExecutionTime(subqueryHash string) int64 {
	// Look up from reuse history or use default
	if history, exists := ra.reuseHistory[subqueryHash]; exists && len(history) > 0 {
		return history[0].EstimatedSaving + 1000 // Add cache retrieval time back
	}

	return 10000000 // 10ms default
}

// estimateCacheRetrievalTime estimates how long it takes to retrieve from cache
func (ra *reuseAnalyzer) estimateCacheRetrievalTime(entry *CacheEntry) int64 {
	baseCost := int64(1000) // 1 microsecond base cost

	// Add cost based on entry age (older entries might be slower)
	age := time.Since(entry.CreatedAt)
	if age > time.Hour {
		baseCost += 500 // Additional 500ns for older entries
	}

	return baseCost
}

// sortOpportunitiesByValue sorts opportunities by estimated time saving (descending)
func (ra *reuseAnalyzer) sortOpportunitiesByValue(opportunities []*ReuseOpportunity) {
	// Simple bubble sort for now - could use more efficient sorting
	n := len(opportunities)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if opportunities[j].EstimatedSaving < opportunities[j+1].EstimatedSaving {
				opportunities[j], opportunities[j+1] = opportunities[j+1], opportunities[j]
			}
		}
	}
}

// UpdateConfig updates the analyzer configuration
func (ra *reuseAnalyzer) UpdateConfig(config *DecompositionConfig) error {
	ra.config = config
	return nil
}

// Reset resets the analyzer state
func (ra *reuseAnalyzer) Reset() error {
	ra.cachedQueries = make(map[QueryID]*CacheEntry)
	ra.reuseHistory = make(map[string][]*ReuseOpportunity)
	return nil
}

// AddCachedQuery adds a cached query to the analyzer (for testing/simulation)
func (ra *reuseAnalyzer) AddCachedQuery(queryID QueryID, sql string, result interface{}) {
	hash := md5.Sum([]byte(sql))
	entry := &CacheEntry{
		QueryID:   queryID,
		SQL:       sql,
		Hash:      fmt.Sprintf("%x", hash),
		Result:    result,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
		HitCount:  1,
		TTL:       3600, // 1 hour default
	}

	ra.cachedQueries[queryID] = entry
}

// generateOpportunityID generates a unique ID for a reuse opportunity
func generateOpportunityID(subqueryHash, cachedQueryID string) string {
	combined := subqueryHash + ":" + cachedQueryID
	hash := md5.Sum([]byte(combined))
	return fmt.Sprintf("opp_%x", hash)[:16]
}
