package routing

import (
	"testing"
	"time"
)

func TestRoutingMetricsAggregator_BasicCounters(t *testing.T) {
	t.Parallel()

	aggregator := NewMetricsAggregator()
	events := []QueryEvent{
		{Timestamp: time.Now(), QueryID: "q1", CacheHit: true, ResponseTime: 5 * time.Millisecond, Strategy: LOCAL_CACHE},
		{Timestamp: time.Now(), QueryID: "q2", CacheHit: false, ResponseTime: 100 * time.Millisecond, Strategy: LOCAL_DATABASE},
		{Timestamp: time.Now(), QueryID: "q3", CacheHit: true, ResponseTime: 3 * time.Millisecond, Strategy: LOCAL_CACHE},
	}
	for _, event := range events {
		aggregator.RecordQuery(event)
	}

	stats := aggregator.GetStats()
	if got := stats["total_queries"].(int64); got != 3 {
		t.Fatalf("total_queries=%d want=3", got)
	}
	if got := stats["cache_hits"].(int64); got != 2 {
		t.Fatalf("cache_hits=%d want=2", got)
	}
	if got := stats["cache_misses"].(int64); got != 1 {
		t.Fatalf("cache_misses=%d want=1", got)
	}

	hitRate := stats["cache_hit_rate"].(float64)
	if hitRate < 0.65 || hitRate > 0.68 {
		t.Fatalf("cache_hit_rate=%f want around 0.666", hitRate)
	}
}

func TestRoutingMetricsAggregator_SystemAndPerformance(t *testing.T) {
	t.Parallel()

	aggregator := NewMetricsAggregator()
	aggregator.UpdateDatabaseConnections(25, 100)
	for i := 0; i < 5; i++ {
		aggregator.RecordQuery(QueryEvent{Timestamp: time.Now(), QueryID: "c", CacheHit: true, ResponseTime: 10 * time.Millisecond, Strategy: LOCAL_CACHE})
	}
	for i := 0; i < 3; i++ {
		aggregator.RecordQuery(QueryEvent{Timestamp: time.Now(), QueryID: "d", CacheHit: false, ResponseTime: 200 * time.Millisecond, Strategy: LOCAL_DATABASE})
	}

	metrics := aggregator.GetSystemMetrics(1024 * 1024)
	if metrics.CacheSize != 1024*1024 {
		t.Fatalf("cache_size=%d want=%d", metrics.CacheSize, 1024*1024)
	}
	if metrics.DatabaseLoad != 0.25 {
		t.Fatalf("database_load=%f want=0.25", metrics.DatabaseLoad)
	}
	if metrics.AvailableMemory <= 0 {
		t.Fatalf("available_memory=%d want > 0", metrics.AvailableMemory)
	}

	perf := aggregator.GetRecentPerformance()
	if perf[LOCAL_CACHE] <= 0 || perf[LOCAL_DATABASE] <= 0 {
		t.Fatalf("expected non-zero perf stats, got %+v", perf)
	}
	if perf[LOCAL_CACHE] >= perf[LOCAL_DATABASE] {
		t.Fatalf("expected cache faster than db, got cache=%v db=%v", perf[LOCAL_CACHE], perf[LOCAL_DATABASE])
	}
}

func TestRoutingMetricsAggregator_WindowCap(t *testing.T) {
	t.Parallel()

	aggregator := NewMetricsAggregator()
	for i := 0; i < 15000; i++ {
		aggregator.RecordQuery(QueryEvent{
			Timestamp:    time.Now(),
			QueryID:      "bulk",
			CacheHit:     i%2 == 0,
			ResponseTime: time.Millisecond,
			Strategy:     LOCAL_CACHE,
		})
	}

	recentQueries := aggregator.GetStats()["recent_queries"].(int)
	if recentQueries > 10000 {
		t.Fatalf("recent_queries=%d want <= 10000", recentQueries)
	}
	if recentQueries < 5000 {
		t.Fatalf("recent_queries=%d want >= 5000", recentQueries)
	}
}
