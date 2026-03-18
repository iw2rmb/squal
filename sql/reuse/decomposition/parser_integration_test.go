package decomposition

import (
	"testing"

	"github.com/iw2rmb/squal/parser"
)

// TestParseSQL_WithParser verifies parsing succeeds when a parser is provided.
func TestParseSQL_WithParser(t *testing.T) {
	pq, err := ParseSQL("SELECT 1", parser.NewTestParser())
	if err != nil {
		t.Fatalf("ParseSQL returned error: %v", err)
	}
	if pq == nil || pq.Metadata == nil {
		t.Fatalf("expected non-nil parsed query and metadata")
	}
	if pq.Normalized == "" {
		t.Fatalf("expected non-empty normalized SQL")
	}
}

// TestParseSQL_Empty guards against empty input.
func TestParseSQL_Empty(t *testing.T) {
	if _, err := ParseSQL("", parser.NewTestParser()); err == nil {
		t.Fatalf("expected error for empty SQL")
	}
}

// FuzzParseSQL_NoPanic ensures arbitrary inputs do not panic.
func FuzzParseSQL_NoPanic(f *testing.F) {
	seeds := []string{
		"SELECT 1",
		"select * from users",
		"WITH t AS (SELECT 1) SELECT * FROM t",
		"DELETE FROM x WHERE id = 1",
		"SYNTAX ERROR",
		"",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	p := parser.NewTestParser()
	f.Fuzz(func(t *testing.T, s string) {
		// Expect either a result or a well-formed error; no panics.
		_, _ = ParseSQL(s, p)
	})
}

// TestParsedQuery_HasDistinct verifies that HasDistinct() returns true for SELECT DISTINCT queries.
// This ensures parser metadata is sufficient for GroupByStrategy to reason about SELECT DISTINCT.
// ROADMAP.md line 101: Assert ParsedQuery.HasDistinct() is populated.
func TestParsedQuery_HasDistinct(t *testing.T) {
	t.Parallel()
	p := parser.NewTestParser()

	tests := []struct {
		name            string
		sql             string
		wantHasDistinct bool
	}{
		{
			name:            "SELECT DISTINCT single column",
			sql:             "SELECT DISTINCT category FROM products",
			wantHasDistinct: true,
		},
		{
			name:            "SELECT DISTINCT multiple columns",
			sql:             "SELECT DISTINCT category, brand FROM products",
			wantHasDistinct: true,
		},
		{
			name:            "SELECT DISTINCT with WHERE",
			sql:             "SELECT DISTINCT category FROM products WHERE active = true",
			wantHasDistinct: true,
		},
		{
			name:            "SELECT DISTINCT with ORDER BY",
			sql:             "SELECT DISTINCT category FROM products ORDER BY category",
			wantHasDistinct: true,
		},
		{
			name:            "regular SELECT without DISTINCT",
			sql:             "SELECT category FROM products",
			wantHasDistinct: false,
		},
		{
			name:            "SELECT with GROUP BY but no DISTINCT",
			sql:             "SELECT category, COUNT(*) FROM products GROUP BY category",
			wantHasDistinct: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pq, err := ParseSQL(tc.sql, p)
			if err != nil {
				t.Fatalf("ParseSQL failed: %v", err)
			}
			if got := pq.HasDistinct(); got != tc.wantHasDistinct {
				t.Errorf("HasDistinct() = %v, want %v", got, tc.wantHasDistinct)
			}
		})
	}
}

// TestParsedQuery_GetDistinctColumns verifies that GetDistinctColumns() returns the
// columns specified in SELECT DISTINCT. This is the key metadata for treating
// SELECT DISTINCT as a grouping query (where distinct columns become group keys).
// ROADMAP.md line 101: Assert DistinctColumns/DistinctSpec.Columns are populated.
func TestParsedQuery_GetDistinctColumns(t *testing.T) {
	t.Parallel()
	p := parser.NewTestParser()

	tests := []struct {
		name        string
		sql         string
		wantColumns []string
	}{
		{
			name:        "SELECT DISTINCT single column",
			sql:         "SELECT DISTINCT category FROM products",
			wantColumns: []string{"category"},
		},
		{
			name:        "SELECT DISTINCT multiple columns",
			sql:         "SELECT DISTINCT category, brand FROM products",
			wantColumns: []string{"category", "brand"},
		},
		{
			name:        "SELECT DISTINCT with aliased column",
			sql:         "SELECT DISTINCT category AS cat FROM products",
			wantColumns: []string{"category"},
		},
		{
			name:        "SELECT DISTINCT with table-qualified column",
			sql:         "SELECT DISTINCT p.category FROM products p",
			wantColumns: []string{"category"},
		},
		{
			name:        "regular SELECT returns empty",
			sql:         "SELECT category FROM products",
			wantColumns: []string{},
		},
		{
			name:        "GROUP BY without DISTINCT returns empty",
			sql:         "SELECT category, COUNT(*) FROM products GROUP BY category",
			wantColumns: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pq, err := ParseSQL(tc.sql, p)
			if err != nil {
				t.Fatalf("ParseSQL failed: %v", err)
			}
			got := pq.GetDistinctColumns()
			if len(got) != len(tc.wantColumns) {
				t.Fatalf("GetDistinctColumns() length = %d (%v), want %d (%v)",
					len(got), got, len(tc.wantColumns), tc.wantColumns)
			}
			for i, want := range tc.wantColumns {
				if got[i] != want {
					t.Errorf("GetDistinctColumns()[%d] = %q, want %q", i, got[i], want)
				}
			}
		})
	}
}

// TestParsedQuery_DistinctAsGroupingQuery verifies that SELECT DISTINCT queries
// provide sufficient metadata for GroupByStrategy to treat them as grouping queries.
// The key insight: SELECT DISTINCT col1, col2 is semantically equivalent to
// SELECT col1, col2 GROUP BY col1, col2 (for non-aggregate queries).
// ROADMAP.md line 98-101: Model SELECT DISTINCT as GROUP BY in metadata.
func TestParsedQuery_DistinctAsGroupingQuery(t *testing.T) {
	t.Parallel()
	p := parser.NewTestParser()

	// This test validates that:
	// 1. HasDistinct() returns true for SELECT DISTINCT
	// 2. GetDistinctColumns() returns the columns that form the implicit group key
	// 3. IsAggregate() returns false (SELECT DISTINCT without aggregates is not an aggregate query)
	// This combination allows GroupByStrategy to map SELECT DISTINCT to a degenerate GROUP BY.
	tests := []struct {
		name        string
		sql         string
		wantDistCol []string
		wantAgg     bool
	}{
		{
			name:        "SELECT DISTINCT category - single group key",
			sql:         "SELECT DISTINCT category FROM products",
			wantDistCol: []string{"category"},
			wantAgg:     false,
		},
		{
			name:        "SELECT DISTINCT category, brand - composite group key",
			sql:         "SELECT DISTINCT category, brand FROM products",
			wantDistCol: []string{"category", "brand"},
			wantAgg:     false,
		},
		{
			name:        "SELECT DISTINCT with WHERE - filtered grouping",
			sql:         "SELECT DISTINCT category FROM products WHERE active = true",
			wantDistCol: []string{"category"},
			wantAgg:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pq, err := ParseSQL(tc.sql, p)
			if err != nil {
				t.Fatalf("ParseSQL failed: %v", err)
			}
			// Verify DISTINCT is detected
			if !pq.HasDistinct() {
				t.Error("HasDistinct() = false, want true")
			}
			// Verify columns are extracted as group keys
			got := pq.GetDistinctColumns()
			if len(got) != len(tc.wantDistCol) {
				t.Fatalf("GetDistinctColumns() = %v, want %v", got, tc.wantDistCol)
			}
			for i, want := range tc.wantDistCol {
				if got[i] != want {
					t.Errorf("GetDistinctColumns()[%d] = %q, want %q", i, got[i], want)
				}
			}
			// Verify it's not flagged as an aggregate query
			if pq.IsAggregate() != tc.wantAgg {
				t.Errorf("IsAggregate() = %v, want %v", pq.IsAggregate(), tc.wantAgg)
			}
		})
	}
}
