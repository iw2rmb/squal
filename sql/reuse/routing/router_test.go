package routing

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iw2rmb/squall/parser"
)

type mockParser struct {
	metadata *parser.QueryMetadata
	err      error
}

func (m *mockParser) ExtractMetadata(sql string) (*parser.QueryMetadata, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.metadata != nil {
		return m.metadata, nil
	}
	return &parser.QueryMetadata{Operations: []string{"SELECT"}, Tables: []string{"users"}}, nil
}

func (m *mockParser) NormalizeQuery(sql string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return sql, nil
}

func (m *mockParser) GenerateFingerprint(sql string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return "mock-fingerprint", nil
}

func (m *mockParser) ExtractTables(sql string) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.metadata != nil && len(m.metadata.Tables) > 0 {
		return m.metadata.Tables, nil
	}
	return []string{"users"}, nil
}

func (m *mockParser) ExtractCaseAggregates(sql string) ([]parser.AggCase, error) {
	return []parser.AggCase{}, nil
}

func (m *mockParser) ExtractAggregateCompositions(sql string) ([]parser.AggComposition, error) {
	return []parser.AggComposition{}, nil
}

func (m *mockParser) ExtractAggregates(sql string) ([]parser.Aggregate, error) {
	return []parser.Aggregate{}, nil
}

func (m *mockParser) ExtractDistinctSpec(sql string) (*parser.DistinctSpec, error) {
	return &parser.DistinctSpec{}, nil
}

func (m *mockParser) ExtractGroupBy(sql string) ([]parser.GroupItem, error) {
	return []parser.GroupItem{}, nil
}

func (m *mockParser) ExtractTemporalOps(sql string) (*parser.TemporalOps, error) {
	return &parser.TemporalOps{WhereRanges: []parser.TimeRange{}}, nil
}

func (m *mockParser) ExtractJSONPaths(sql string) ([]parser.JSONPath, error) {
	return []parser.JSONPath{}, nil
}

func (m *mockParser) DetectSlidingWindow(sql string) (*parser.SlidingWindowInfo, error) {
	return nil, nil
}

func TestQueryRouter_RouteQuery(t *testing.T) {
	t.Parallel()

	router := NewQueryRouterWithParser(&mockParser{})
	router.UpdateMetrics(&SystemMetrics{DatabaseLoad: 0.3, CacheHitRate: 0.8, AvailableMemory: 1024 * 1024 * 1024})

	tests := []struct {
		name           string
		cacheExists    bool
		metrics        *SystemMetrics
		metadata       *parser.QueryMetadata
		expectStrategy ExecutionStrategy
		reasonContains string
	}{
		{
			name:           "cache hit routes to local cache",
			cacheExists:    true,
			expectStrategy: LOCAL_CACHE,
			reasonContains: "Serving from existing cache",
		},
		{
			name:           "cache miss routes to local db",
			cacheExists:    false,
			expectStrategy: LOCAL_DATABASE,
			reasonContains: "Standard execution with caching",
		},
		{
			name:        "high db load prefers cache",
			cacheExists: true,
			metrics: &SystemMetrics{
				DatabaseLoad:    0.8,
				AvailableMemory: 1024 * 1024 * 1024,
				CacheSize:       1024,
			},
			expectStrategy: LOCAL_CACHE,
			reasonContains: "database load high",
		},
		{
			name:        "complex query low memory bypasses cache",
			cacheExists: false,
			metadata: &parser.QueryMetadata{
				Operations:             []string{"SELECT"},
				Tables:                 []string{"users", "orders", "products", "inventory"},
				HasCTEs:                true,
				IsRecursiveCTE:         true,
				HasWindowFunctions:     true,
				HasDistinct:            true,
				IsAggregate:            true,
				HasDatabaseSpecificOps: true,
			},
			metrics: &SystemMetrics{
				DatabaseLoad:    0.4,
				CacheSize:       1024 * 1024 * 100,
				AvailableMemory: 1024 * 1024 * 40,
			},
			expectStrategy: BYPASS_CACHE,
			reasonContains: "Complex query",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.metadata != nil {
				router.parser = &mockParser{metadata: tc.metadata}
			} else {
				router.parser = &mockParser{}
			}
			if tc.metrics != nil {
				router.UpdateMetrics(tc.metrics)
			}

			decision, err := router.RouteQuery("SELECT * FROM users", "qid", tc.cacheExists)
			if err != nil {
				t.Fatalf("route query: %v", err)
			}
			if decision.Strategy != tc.expectStrategy {
				t.Fatalf("strategy=%v want=%v", decision.Strategy, tc.expectStrategy)
			}
			if tc.reasonContains != "" && !strings.Contains(decision.Reasoning, tc.reasonContains) {
				t.Fatalf("reason=%q does not contain %q", decision.Reasoning, tc.reasonContains)
			}
			if decision.EstimatedLatency <= 0 {
				t.Fatalf("expected positive latency, got %v", decision.EstimatedLatency)
			}
		})
	}
}

func TestQueryRouter_ParseFailure(t *testing.T) {
	t.Parallel()

	router := NewQueryRouterWithParser(&mockParser{err: errors.New("boom")})
	if _, err := router.RouteQuery("SELECT * FROM users", "qid", false); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestQueryRouter_ComplexityScore(t *testing.T) {
	t.Parallel()

	router := NewQueryRouterWithParser(&mockParser{})
	tests := []struct {
		name     string
		metadata *parser.QueryMetadata
		wantMin  float64
		wantMax  float64
	}{
		{
			name:     "simple",
			metadata: &parser.QueryMetadata{Operations: []string{"SELECT"}, Tables: []string{"users"}},
			wantMin:  0.0,
			wantMax:  0.3,
		},
		{
			name: "complex",
			metadata: &parser.QueryMetadata{
				Operations:         []string{"SELECT"},
				Tables:             []string{"users", "orders", "products", "inventory"},
				HasCTEs:            true,
				IsRecursiveCTE:     true,
				HasWindowFunctions: true,
				HasDistinct:        true,
				IsAggregate:        true,
			},
			wantMin: 0.8,
			wantMax: 1.0,
		},
		{
			name:     "dml",
			metadata: &parser.QueryMetadata{Operations: []string{"UPDATE"}, Tables: []string{"users"}},
			wantMin:  0.3,
			wantMax:  0.5,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			score := router.calculateComplexityScore(tc.metadata)
			if score < tc.wantMin || score > tc.wantMax {
				t.Fatalf("score=%f not in [%f,%f]", score, tc.wantMin, tc.wantMax)
			}
			if score > 1.0 {
				t.Fatalf("score=%f must be capped at 1.0", score)
			}
		})
	}
}

func TestQueryRouter_MetricsSnapshotAndRecording(t *testing.T) {
	t.Parallel()

	router := NewQueryRouterWithParser(&mockParser{})
	metrics := &SystemMetrics{CacheHitRate: 0.7, QueryCount: 10}
	router.UpdateMetrics(metrics)

	copied := router.GetCurrentMetrics()
	if copied == nil {
		t.Fatal("expected metrics copy")
	}
	copied.QueryCount = 999
	if router.GetCurrentMetrics().QueryCount == 999 {
		t.Fatal("expected defensive copy")
	}

	router.RecordQueryExecution("q1", true, 5*time.Millisecond, LOCAL_CACHE)
	router.RecordQueryExecution("q2", false, 100*time.Millisecond, LOCAL_DATABASE)
	router.UpdateMetricsFromAggregator(1024 * 1024)

	stats := router.GetRoutingStats()
	if stats["total_queries"].(int64) < 2 {
		t.Fatalf("expected at least 2 queries, got %v", stats["total_queries"])
	}
	if _, ok := stats["performance_by_strategy"].(map[ExecutionStrategy]time.Duration); !ok {
		t.Fatalf("expected performance_by_strategy map, got %T", stats["performance_by_strategy"])
	}
}
