package graph

import (
	"testing"

	"github.com/iw2rmb/squall/parser"
)

// mockParser implements parser.Parser without parserpg-specific behavior.
type mockParser struct{}

func (m *mockParser) ExtractMetadata(sql string) (*parser.QueryMetadata, error) {
	tables, _ := m.ExtractTables(sql)
	return &parser.QueryMetadata{
		Tables:        tables,
		SelectColumns: []parser.ColumnRef{{Column: "*"}},
	}, nil
}

func (m *mockParser) NormalizeQuery(sql string) (string, error)      { return sql, nil }
func (m *mockParser) GenerateFingerprint(sql string) (string, error) { return "mockfp", nil }

func (m *mockParser) ExtractTables(sql string) ([]string, error) {
	out := []string{}
	if containsCI(sql, "users") {
		out = append(out, "users")
	}
	if containsCI(sql, "orders") {
		out = append(out, "orders")
	}
	return out, nil
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
	return &parser.TemporalOps{
		HasNow:       false,
		HasDateTrunc: false,
		WhereRanges:  []parser.TimeRange{},
	}, nil
}

func (m *mockParser) ExtractJSONPaths(sql string) ([]parser.JSONPath, error) {
	return []parser.JSONPath{}, nil
}

func (m *mockParser) DetectSlidingWindow(sql string) (*parser.SlidingWindowInfo, error) {
	return nil, nil
}

func containsCI(s, sub string) bool {
	ls := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		ls[i] = c
	}
	lu := make([]byte, len(sub))
	for i := 0; i < len(sub); i++ {
		c := sub[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		lu[i] = c
	}

	L := string(ls)
	U := string(lu)
	return len(L) >= len(U) && indexOf(L, U) >= 0
}

func indexOf(s, sub string) int {
	n, m := len(s), len(sub)
	if m == 0 {
		return 0
	}
	if m > n {
		return -1
	}
	for i := 0; i <= n-m; i++ {
		if s[i:i+m] == sub {
			return i
		}
	}
	return -1
}

func TestQueryGraph_WithInjectedParser_NoParserPGMethods(t *testing.T) {
	g := NewQueryGraphWithParser(&mockParser{})

	if _, err := g.AddQuery(QueryID("q1"), SQLText("SELECT * FROM users")); err != nil {
		t.Fatalf("AddQuery failed: %v", err)
	}

	if !g.CanReuse(QueryID("q1"), "SELECT id FROM users WHERE status='active'") {
		t.Fatalf("expected CanReuse to be true via basic fallback")
	}

	if g.CanReuse(QueryID("q1"), "SELECT * FROM orders") {
		t.Fatalf("expected CanReuse to be false for different table")
	}

	got := g.FindReusableCachedQueries("SELECT id FROM users")
	if len(got) != 1 {
		t.Fatalf("expected 1 reusable query, got %d", len(got))
	}
	if got[0].ID != QueryID("q1") {
		t.Fatalf("reusable query ID = %s, want q1", got[0].ID)
	}
	if got[0].Confidence <= 0 {
		t.Fatalf("reusable confidence = %f, want > 0", got[0].Confidence)
	}

	queries := g.FindQueriesByTable(TableName("users"))
	if len(queries) != 1 {
		t.Fatalf("expected 1 query for users table, got %d", len(queries))
	}
	if queries[0] != QueryID("q1") {
		t.Fatalf("expected query ID q1, got %s", queries[0])
	}
}
