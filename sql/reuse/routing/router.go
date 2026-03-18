package routing

import (
	"fmt"
	"sync"
	"time"

	"github.com/iw2rmb/squall/parser"
)

// ExecutionStrategy defines how a query should be executed.
type ExecutionStrategy int

const (
	LOCAL_CACHE ExecutionStrategy = iota
	LOCAL_DATABASE
	BYPASS_CACHE
)

func (es ExecutionStrategy) String() string {
	switch es {
	case LOCAL_CACHE:
		return "LOCAL_CACHE"
	case LOCAL_DATABASE:
		return "LOCAL_DATABASE"
	case BYPASS_CACHE:
		return "BYPASS_CACHE"
	default:
		return "UNKNOWN"
	}
}

// RoutingDecision contains the router's decision for query execution.
type RoutingDecision struct {
	Strategy         ExecutionStrategy
	Reasoning        string
	EstimatedLatency time.Duration
	CacheHitChance   float64
	LoadFactor       float64
}

// SystemMetrics represents current system state for routing decisions.
type SystemMetrics struct {
	CacheHitRate    float64
	DatabaseLoad    float64
	CacheSize       int64
	AvailableMemory int64
	QueryCount      int64
}

// QueryRouter makes parser-aware routing decisions for query execution.
type QueryRouter struct {
	mu         sync.RWMutex
	metrics    *SystemMetrics
	parser     parser.Parser
	aggregator *MetricsAggregator
}

// NewQueryRouter creates a query router and requires parser injection.
func NewQueryRouter() *QueryRouter {
	panic("NewQueryRouter() requires parser injection; use NewQueryRouterWithParser(p parser.Parser)")
}

// NewQueryRouterWithParser creates a query router with a parser implementation.
func NewQueryRouterWithParser(p parser.Parser) *QueryRouter {
	aggregator := NewMetricsAggregator()
	return &QueryRouter{
		metrics: &SystemMetrics{
			CacheHitRate:    0.0,
			DatabaseLoad:    0.0,
			CacheSize:       0,
			AvailableMemory: 1024 * 1024 * 1024,
			QueryCount:      0,
		},
		parser:     p,
		aggregator: aggregator,
	}
}

// RouteQuery analyzes SQL and selects an execution strategy.
func (qr *QueryRouter) RouteQuery(sql, queryID string, cacheExists bool) (*RoutingDecision, error) {
	metadata, err := qr.parser.ExtractMetadata(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query for routing: %w", err)
	}

	qr.mu.RLock()
	metricsSnapshot := *qr.metrics
	qr.mu.RUnlock()

	complexityScore := qr.calculateComplexityScore(metadata)

	if cacheExists && metricsSnapshot.DatabaseLoad > 0.7 {
		return &RoutingDecision{
			Strategy:         LOCAL_CACHE,
			Reasoning:        fmt.Sprintf("Cache exists and database load high (%.2f)", metricsSnapshot.DatabaseLoad),
			EstimatedLatency: time.Millisecond * 5,
			CacheHitChance:   1.0,
			LoadFactor:       metricsSnapshot.DatabaseLoad,
		}, nil
	}

	if !cacheExists && complexityScore > 0.8 && metricsSnapshot.AvailableMemory < metricsSnapshot.CacheSize/2 {
		return &RoutingDecision{
			Strategy:         BYPASS_CACHE,
			Reasoning:        fmt.Sprintf("Complex query (%.2f) with low memory availability", complexityScore),
			EstimatedLatency: time.Millisecond * 200,
			CacheHitChance:   0.0,
			LoadFactor:       metricsSnapshot.DatabaseLoad,
		}, nil
	}

	strategy := LOCAL_DATABASE
	estimatedLatency := time.Millisecond * 100
	reasoning := "Standard execution with caching"
	cacheChance := 0.0

	if cacheExists {
		strategy = LOCAL_CACHE
		estimatedLatency = time.Millisecond * 5
		reasoning = "Serving from existing cache"
		cacheChance = 1.0
	}

	return &RoutingDecision{
		Strategy:         strategy,
		Reasoning:        reasoning,
		EstimatedLatency: estimatedLatency,
		CacheHitChance:   cacheChance,
		LoadFactor:       metricsSnapshot.DatabaseLoad,
	}, nil
}

// UpdateMetrics replaces the router's metrics snapshot.
func (qr *QueryRouter) UpdateMetrics(newMetrics *SystemMetrics) {
	if newMetrics == nil {
		return
	}
	metricsCopy := *newMetrics
	qr.mu.Lock()
	qr.metrics = &metricsCopy
	qr.mu.Unlock()
}

// calculateComplexityScore computes query complexity in the [0.0, 1.0] range.
func (qr *QueryRouter) calculateComplexityScore(metadata *parser.QueryMetadata) float64 {
	score := 0.0

	for _, op := range metadata.Operations {
		if op == "SELECT" {
			score += 0.1
		} else if op == "INSERT" || op == "UPDATE" || op == "DELETE" {
			score += 0.3
			break
		}
	}

	if metadata.HasCTEs {
		score += 0.3
		if metadata.IsRecursiveCTE {
			score += 0.2
		}
	}

	if metadata.HasWindowFunctions {
		score += 0.2
	}
	if metadata.HasDistinct {
		score += 0.1
	}
	if metadata.IsAggregate {
		score += 0.1
	}
	if metadata.HasDatabaseSpecificOps {
		score += 0.1
	}

	if len(metadata.Tables) > 3 {
		score += 0.2
	} else if len(metadata.Tables) > 1 {
		score += 0.1
	}

	if score > 1.0 {
		score = 1.0
	}
	return score
}

// GetCurrentMetrics returns a copy of current metrics.
func (qr *QueryRouter) GetCurrentMetrics() *SystemMetrics {
	qr.mu.RLock()
	defer qr.mu.RUnlock()
	if qr.metrics == nil {
		return nil
	}
	m := *qr.metrics
	return &m
}

// RecordQueryExecution records query execution data for routing metrics.
func (qr *QueryRouter) RecordQueryExecution(queryID string, cacheHit bool, responseTime time.Duration, strategy ExecutionStrategy) {
	event := QueryEvent{
		Timestamp:    time.Now(),
		QueryID:      queryID,
		CacheHit:     cacheHit,
		ResponseTime: responseTime,
		Strategy:     strategy,
	}
	qr.aggregator.RecordQuery(event)
}

// UpdateMetricsFromAggregator refreshes system metrics from aggregated data.
func (qr *QueryRouter) UpdateMetricsFromAggregator(cacheSize int64) {
	metrics := qr.aggregator.GetSystemMetrics(cacheSize)
	qr.mu.Lock()
	qr.metrics = metrics
	qr.mu.Unlock()
}

// GetRoutingStats returns aggregated routing stats.
func (qr *QueryRouter) GetRoutingStats() map[string]any {
	stats := qr.aggregator.GetStats()
	stats["performance_by_strategy"] = qr.aggregator.GetRecentPerformance()
	return stats
}
