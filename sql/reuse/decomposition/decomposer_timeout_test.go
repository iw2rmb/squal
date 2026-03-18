package decomposition

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iw2rmb/squall/parser"
)

// sleepyParser implements parser.Parser and deliberately sleeps to trigger timeouts.
type sleepyParser struct{}

func (sleepyParser) ExtractMetadata(sql string) (*parser.QueryMetadata, error) {
	time.Sleep(50 * time.Millisecond)
	return &parser.QueryMetadata{
		Operations: []string{"SELECT"},
		Tables:     []string{"dual"},
	}, nil
}

func (sleepyParser) NormalizeQuery(sql string) (string, error) {
	time.Sleep(10 * time.Millisecond)
	return sql, nil
}

func (sleepyParser) GenerateFingerprint(sql string) (string, error) { return "fp", nil }
func (sleepyParser) ExtractTables(sql string) ([]string, error)     { return []string{"dual"}, nil }

func (sleepyParser) ExtractCaseAggregates(sql string) ([]parser.AggCase, error) {
	return []parser.AggCase{}, nil
}

func (sleepyParser) ExtractAggregateCompositions(sql string) ([]parser.AggComposition, error) {
	return []parser.AggComposition{}, nil
}

func (sleepyParser) ExtractAggregates(sql string) ([]parser.Aggregate, error) {
	return []parser.Aggregate{}, nil
}

func (sleepyParser) ExtractDistinctSpec(sql string) (*parser.DistinctSpec, error) {
	return &parser.DistinctSpec{}, nil
}

// New interface method: ExtractGroupBy — return empty for tests
func (sleepyParser) ExtractGroupBy(sql string) ([]parser.GroupItem, error) {
	return []parser.GroupItem{}, nil
}

// Satisfy new parser.Parser method for tests: ExtractTemporalOps
func (sleepyParser) ExtractTemporalOps(sql string) (*parser.TemporalOps, error) {
	return &parser.TemporalOps{HasNow: false, HasDateTrunc: false, WhereRanges: []parser.TimeRange{}}, nil
}

// Satisfy parser.Parser: ExtractJSONPaths (unused in this test)
func (sleepyParser) ExtractJSONPaths(sql string) ([]parser.JSONPath, error) {
	return []parser.JSONPath{}, nil
}

// Satisfy parser.Parser: DetectSlidingWindow — unused in timeout test.
func (sleepyParser) DetectSlidingWindow(sql string) (*parser.SlidingWindowInfo, error) {
	return nil, nil
}

func TestDecomposeQueryHonorsAnalysisTimeoutMilliseconds(t *testing.T) {
	cfg := DefaultDecompositionConfig()
	// Use a very small timeout to force deadline before parser returns.
	cfg.AnalysisTimeoutMS = 5 // milliseconds

	d := NewQueryDecomposerWithConfigAndParser(cfg, sleepyParser{})

	_, err := d.DecomposeQuery(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}

	// Be tolerant of exact error wording; require context deadline signal.
	if got := err.Error(); !(strings.Contains(got, "timed out") || strings.Contains(got, "deadline exceeded")) {
		t.Fatalf("expected timeout/deadline error, got: %v", err)
	}
}
