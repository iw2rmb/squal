package routing

import (
	"runtime"
	"sync"
	"time"
)

// MetricsAggregator collects and maintains system metrics for routing decisions.
type MetricsAggregator struct {
	mu sync.RWMutex

	recentQueries  []QueryEvent
	windowDuration time.Duration

	totalQueries    int64
	cacheHits       int64
	cacheMisses     int64
	bypassedQueries int64

	databaseConnections int
	maxConnections      int
}

// QueryEvent represents a single query execution event.
type QueryEvent struct {
	Timestamp    time.Time
	QueryID      string
	CacheHit     bool
	ResponseTime time.Duration
	Strategy     ExecutionStrategy
}

// NewMetricsAggregator creates a metrics aggregator with default rolling-window settings.
func NewMetricsAggregator() *MetricsAggregator {
	return &MetricsAggregator{
		recentQueries:  make([]QueryEvent, 0, 1000),
		windowDuration: 5 * time.Minute,
		maxConnections: 100,
	}
}

// RecordQuery records a query execution event.
func (ma *MetricsAggregator) RecordQuery(event QueryEvent) {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	ma.recentQueries = append(ma.recentQueries, event)
	ma.totalQueries++
	if event.CacheHit {
		ma.cacheHits++
	} else {
		ma.cacheMisses++
	}
	if event.Strategy == BYPASS_CACHE {
		ma.bypassedQueries++
	}

	ma.cleanOldEvents()
}

func (ma *MetricsAggregator) cleanOldEvents() {
	cutoff := time.Now().Add(-ma.windowDuration)

	keepFrom := 0
	for i, event := range ma.recentQueries {
		if event.Timestamp.After(cutoff) {
			keepFrom = i
			break
		}
	}
	if keepFrom > 0 {
		ma.recentQueries = ma.recentQueries[keepFrom:]
	}

	if len(ma.recentQueries) > 10000 {
		ma.recentQueries = ma.recentQueries[len(ma.recentQueries)-5000:]
	}
}

// GetSystemMetrics calculates current metrics used by routing decisions.
func (ma *MetricsAggregator) GetSystemMetrics(cacheSize int64) *SystemMetrics {
	ma.mu.RLock()
	defer ma.mu.RUnlock()

	recentHits := 0
	recentTotal := 0
	cutoff := time.Now().Add(-time.Minute)
	for i := len(ma.recentQueries) - 1; i >= 0 && ma.recentQueries[i].Timestamp.After(cutoff); i-- {
		recentTotal++
		if ma.recentQueries[i].CacheHit {
			recentHits++
		}
	}

	cacheHitRate := 0.0
	if recentTotal > 0 {
		cacheHitRate = float64(recentHits) / float64(recentTotal)
	} else if ma.totalQueries > 0 {
		cacheHitRate = float64(ma.cacheHits) / float64(ma.totalQueries)
	}

	databaseLoad := 0.0
	if ma.maxConnections > 0 {
		databaseLoad = float64(ma.databaseConnections) / float64(ma.maxConnections)
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return &SystemMetrics{
		CacheHitRate:    cacheHitRate,
		DatabaseLoad:    databaseLoad,
		CacheSize:       cacheSize,
		AvailableMemory: int64(memStats.Sys - memStats.Alloc),
		QueryCount:      ma.totalQueries,
	}
}

// UpdateDatabaseConnections updates current and max db connection counts.
func (ma *MetricsAggregator) UpdateDatabaseConnections(active, max int) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	ma.databaseConnections = active
	ma.maxConnections = max
}

// GetRecentPerformance returns average response time per strategy in the rolling window.
func (ma *MetricsAggregator) GetRecentPerformance() map[ExecutionStrategy]time.Duration {
	ma.mu.RLock()
	defer ma.mu.RUnlock()

	strategyTimes := make(map[ExecutionStrategy][]time.Duration)
	cutoff := time.Now().Add(-ma.windowDuration)
	for _, event := range ma.recentQueries {
		if event.Timestamp.After(cutoff) {
			strategyTimes[event.Strategy] = append(strategyTimes[event.Strategy], event.ResponseTime)
		}
	}

	avgTimes := make(map[ExecutionStrategy]time.Duration)
	for strategy, times := range strategyTimes {
		if len(times) == 0 {
			continue
		}
		var total time.Duration
		for _, t := range times {
			total += t
		}
		avgTimes[strategy] = total / time.Duration(len(times))
	}
	return avgTimes
}

// GetStats returns aggregate counters and current window size.
func (ma *MetricsAggregator) GetStats() map[string]any {
	ma.mu.RLock()
	defer ma.mu.RUnlock()

	hitRate := 0.0
	if ma.totalQueries > 0 {
		hitRate = float64(ma.cacheHits) / float64(ma.totalQueries)
	}

	return map[string]any{
		"total_queries":    ma.totalQueries,
		"cache_hits":       ma.cacheHits,
		"cache_misses":     ma.cacheMisses,
		"cache_hit_rate":   hitRate,
		"bypassed_queries": ma.bypassedQueries,
		"recent_queries":   len(ma.recentQueries),
		"db_connections":   ma.databaseConnections,
		"max_connections":  ma.maxConnections,
	}
}
