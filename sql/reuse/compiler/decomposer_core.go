package compiler

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// ComponentType represents the type of query component
type ComponentType string

const (
	SELECT_COMPONENT   ComponentType = "SELECT"
	FROM_COMPONENT     ComponentType = "FROM"
	WHERE_COMPONENT    ComponentType = "WHERE"
	JOIN_COMPONENT     ComponentType = "JOIN"
	GROUP_BY_COMPONENT ComponentType = "GROUP_BY"
	AGG_COMPONENT      ComponentType = "AGGREGATION"
	ORDER_BY_COMPONENT ComponentType = "ORDER_BY"
	LIMIT_COMPONENT    ComponentType = "LIMIT"
)

// QueryComponent represents a decomposed part of a SQL query
type QueryComponent struct {
	Type         ComponentType
	Tables       []string
	Columns      []string
	Filters      []FilterExpr
	Aggregations []AggExpr
	Dependencies []string
	Signature    string
}

// FilterExpr represents a filter expression
type FilterExpr struct {
	Column   string
	Operator string
	Value    interface{}
}

// AggExpr represents an aggregation expression
type AggExpr struct {
	Function string // COUNT, SUM, AVG, etc.
	Column   string
	Alias    string
}

// QueryPlan represents a decomposed query with its components
type QueryPlan struct {
	OriginalQuery string
	Components    []QueryComponent
	Dependencies  map[string][]string
}

// Pre-compiled regex patterns for better performance
var (
	selectPattern     = regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM`)
	fromPattern       = regexp.MustCompile(`(?i)FROM\s+(.+?)(?:\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)`)
	multiTablePattern = regexp.MustCompile(`([^\s,]+)(?:\s+(?:as\s+)?(\w+))?\s*(?:,|$)`)
	wherePattern      = regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)`)
	joinPattern       = regexp.MustCompile(`(?i)(INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\s+([^\s]+)\s+(?:AS\s+)?(\w+)?\s*ON`)
	groupByPattern    = regexp.MustCompile(`(?i)GROUP\s+BY\s+([^O]+?)(?:\s+ORDER|\s+HAVING|\s+LIMIT|$)`)
	orderByPattern    = regexp.MustCompile(`(?i)ORDER\s+BY\s+(.+?)(?:\s+LIMIT|$)`)
	limitPattern      = regexp.MustCompile(`(?i)LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?`)
	aggPattern        = regexp.MustCompile(`(?i)(COUNT|SUM|AVG|MAX|MIN)\s*\([^)]+\)(?:\s+as\s+(\w+))?`)
	conditionPattern  = regexp.MustCompile(`(?i)(\w+(?:\.\w+)?)\s*([><=!]+|BETWEEN|IS\s+NOT\s+NULL|IS\s+NULL|IN|LIKE)\s*([^)]+?)(?:\s+AND|\s+OR|$)`)
	normalizePattern  = regexp.MustCompile(`\s+`)
)

// QueryDecomposer decomposes SQL queries into reusable components
type QueryDecomposer struct {
	queryCache     *sync.Map // Cache parsed queries
	signatureCache *sync.Map // Cache generated signatures
}

// NewQueryDecomposer creates a new query decomposer
func NewQueryDecomposer() *QueryDecomposer {
	return &QueryDecomposer{
		queryCache:     &sync.Map{},
		signatureCache: &sync.Map{},
	}
}

// Decompose breaks down a SQL query into reusable components
func (qd *QueryDecomposer) Decompose(sql string) (*QueryPlan, error) {
	// Normalize SQL for parsing
	normalizedSQL := strings.TrimSpace(sql)
	normalizedSQL = normalizePattern.ReplaceAllString(normalizedSQL, " ")

	// Check cache first for performance
	if cached, ok := qd.queryCache.Load(normalizedSQL); ok {
		return cached.(*QueryPlan), nil
	}

	plan := &QueryPlan{
		OriginalQuery: sql,
		Components:    []QueryComponent{},
		Dependencies:  make(map[string][]string),
	}

	// Extract SELECT component
	selectComponent := qd.extractSelectComponent(normalizedSQL)
	if selectComponent != nil {
		plan.Components = append(plan.Components, *selectComponent)
	}

	// Extract FROM component
	fromComponent := qd.extractFromComponent(normalizedSQL)
	if fromComponent != nil {
		plan.Components = append(plan.Components, *fromComponent)
	}

	// Extract WHERE component
	whereComponent := qd.extractWhereComponent(normalizedSQL)
	if whereComponent != nil {
		plan.Components = append(plan.Components, *whereComponent)
	}

	// Extract JOIN component
	joinComponent := qd.extractJoinComponent(normalizedSQL)
	if joinComponent != nil {
		plan.Components = append(plan.Components, *joinComponent)
	}

	// Extract GROUP BY component
	groupByComponent := qd.extractGroupByComponent(normalizedSQL)
	if groupByComponent != nil {
		plan.Components = append(plan.Components, *groupByComponent)
	}

	// Extract aggregations
	aggComponents := qd.extractAggregationComponents(normalizedSQL)
	for _, agg := range aggComponents {
		// If there's a GROUP BY component, set dependency
		if groupByComponent != nil {
			agg.Dependencies = []string{groupByComponent.Signature}
		}
		plan.Components = append(plan.Components, agg)
	}

	// Extract ORDER BY component
	orderByComponent := qd.extractOrderByComponent(normalizedSQL)
	if orderByComponent != nil {
		plan.Components = append(plan.Components, *orderByComponent)
	}

	// Extract LIMIT component
	limitComponent := qd.extractLimitComponent(normalizedSQL)
	if limitComponent != nil {
		plan.Components = append(plan.Components, *limitComponent)
	}

	// Build dependency graph
	qd.buildDependencies(plan)

	// Cache the parsed plan for future reuse
	qd.queryCache.Store(normalizedSQL, plan)

	return plan, nil
}

func (qd *QueryDecomposer) generateSignature(componentType string, values []string) string {
	// Check cache first
	cacheKey := fmt.Sprintf("%s:%s", componentType, strings.Join(values, "|"))
	if cached, ok := qd.signatureCache.Load(cacheKey); ok {
		return cached.(string)
	}

	// Generate a unique signature for the component
	data := componentType + "_" + strings.Join(values, "_")
	hash := md5.Sum([]byte(data))
	signature := fmt.Sprintf("%s_%x", componentType, hash[:8])

	// Cache the signature
	qd.signatureCache.Store(cacheKey, signature)

	return signature
}
