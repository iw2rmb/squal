package complete

import (
	"reflect"
	"strings"
	"testing"

	"github.com/iw2rmb/squal/core"
	"github.com/iw2rmb/squal/parser"
)

func TestCatalogCandidates(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders", "customers"},
				JoinConditions: []parser.JoinCondition{
					{
						Type:       core.JoinTypeInner,
						LeftTable:  "orders",
						RightTable: "customers",
						LeftAlias:  "o",
						RightAlias: "c",
					},
				},
			},
		},
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	req := Request{
		SQL:            "select o.id from orders o join customers c on o.customer_id = c.id where ",
		CursorByte:     len("select o.id from orders o join customers c on o.customer_id = c.id where "),
		CatalogVersion: version,
		MaxCandidates:  200,
	}

	respA, err := engine.Complete(req)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	respB, err := engine.Complete(req)
	if err != nil {
		t.Fatalf("Complete() second error = %v", err)
	}
	if !reflect.DeepEqual(respA.Candidates, respB.Candidates) {
		t.Fatalf("Complete() candidates are not deterministic:\nA=%#v\nB=%#v", respA.Candidates, respB.Candidates)
	}

	if !hasCandidate(respA.Candidates, CandidateKindTable, "public.orders", "orders") {
		t.Fatalf("missing table candidate for public.orders: %#v", respA.Candidates)
	}
	if !hasCandidate(respA.Candidates, CandidateKindColumn, "o.id", "o.id") {
		t.Fatalf("missing alias-aware column candidate o.id: %#v", respA.Candidates)
	}
	if !hasCandidate(respA.Candidates, CandidateKindColumn, "c.email", "c.email") {
		t.Fatalf("missing alias-aware column candidate c.email: %#v", respA.Candidates)
	}
	if hasCandidate(respA.Candidates, CandidateKindColumn, "events.ts", "events.ts") {
		t.Fatalf("unexpected unrelated table column candidate events.ts: %#v", respA.Candidates)
	}
}

func TestJoinSuggestions(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders"},
			},
		},
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	resp, err := engine.Complete(Request{
		SQL:            "select orders.id from orders where ",
		CursorByte:     len("select orders.id from orders where "),
		CatalogVersion: version,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if !hasJoinContaining(resp.Candidates, "JOIN customers ON orders.customer_id = customers.id") {
		t.Fatalf("missing FK join suggestion: %#v", resp.Candidates)
	}

	engineWithTargetVisible := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders", "customers"},
			},
		},
	})
	versionWithTargetVisible, err := engineWithTargetVisible.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog(target visible) error = %v", err)
	}

	respWithTargetVisible, err := engineWithTargetVisible.Complete(Request{
		SQL:            "select orders.id from orders join customers on orders.customer_id = customers.id where ",
		CursorByte:     len("select orders.id from orders join customers on orders.customer_id = customers.id where "),
		CatalogVersion: versionWithTargetVisible,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete(target visible) error = %v", err)
	}
	if hasJoinContaining(respWithTargetVisible.Candidates, "JOIN customers ON") {
		t.Fatalf("unexpected join suggestion for already visible target table: %#v", respWithTargetVisible.Candidates)
	}
}

func TestClauseScopedSnippets(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders", "customers"},
			},
		},
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	tests := []struct {
		name       string
		sql        string
		wantSelect bool
		wantWhere  bool
		wantJoinOn bool
	}{
		{
			name:       "unknown clause allows select/from snippet",
			sql:        " ",
			wantSelect: true,
		},
		{
			name:       "from tail allows where and join snippets",
			sql:        "select * from orders ",
			wantWhere:  true,
			wantJoinOn: true,
		},
		{
			name: "where clause suppresses snippets",
			sql:  "select o.id from orders o where ",
		},
		{
			name: "join on clause suppresses snippets",
			sql:  "select o.id from orders o join customers c on ",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp, err := engine.Complete(Request{
				SQL:            tc.sql,
				CursorByte:     len(tc.sql),
				CatalogVersion: version,
				MaxCandidates:  200,
			})
			if err != nil {
				t.Fatalf("Complete() error = %v", err)
			}

			if got := hasSnippet(resp.Candidates, "SELECT ... FROM ..."); got != tc.wantSelect {
				t.Fatalf("select/from snippet mismatch: got=%v want=%v candidates=%#v", got, tc.wantSelect, resp.Candidates)
			}
			if got := hasSnippet(resp.Candidates, "WHERE ..."); got != tc.wantWhere {
				t.Fatalf("where snippet mismatch: got=%v want=%v candidates=%#v", got, tc.wantWhere, resp.Candidates)
			}
			if got := hasSnippet(resp.Candidates, "JOIN ... ON ..."); got != tc.wantJoinOn {
				t.Fatalf("join/on snippet mismatch: got=%v want=%v candidates=%#v", got, tc.wantJoinOn, resp.Candidates)
			}
		})
	}
}

func TestSelectClauseBareCandidatesUseStarFromForm(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{Parser: healthyParserStub()})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	resp, err := engine.Complete(Request{
		SQL:            "select ",
		CursorByte:     len("select "),
		CatalogVersion: version,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if got, want := len(resp.Candidates), 5; got != want {
		t.Fatalf("candidate count = %d, want %d; candidates=%#v", got, want, resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindSchema, "analytics", "analytics") {
		t.Fatalf("missing schema candidate for analytics: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindSchema, "public", "public") {
		t.Fatalf("missing schema candidate for public: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindSnippet, "* FROM public.customers", "* FROM customers") {
		t.Fatalf("missing select/from candidate for customers: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindSnippet, "* FROM public.orders", "* FROM orders") {
		t.Fatalf("missing select/from candidate for orders: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindSnippet, "* FROM analytics.events", "* FROM events") {
		t.Fatalf("missing select/from candidate for events: %#v", resp.Candidates)
	}
	for _, candidate := range resp.Candidates {
		if candidate.Kind == CandidateKindSchema {
			continue
		}
		if !strings.HasPrefix(candidate.InsertText, "* FROM ") {
			t.Fatalf("unexpected non-star-from candidate: %#v", candidate)
		}
	}
}

func TestSelectClausePrefixCandidatesUseColumnFromForm(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{Parser: healthyParserStub()})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	resp, err := engine.Complete(Request{
		SQL:            "select e",
		CursorByte:     len("select e"),
		CatalogVersion: version,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if !hasCandidate(resp.Candidates, CandidateKindColumn, "email FROM public.customers", "email FROM customers") {
		t.Fatalf("missing select-prefix candidate for customers.email: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindColumn, "event_id FROM analytics.events", "event_id FROM events") {
		t.Fatalf("missing select-prefix candidate for events.event_id: %#v", resp.Candidates)
	}
	if hasCandidate(resp.Candidates, CandidateKindSnippet, "* FROM public.orders", "* FROM orders") {
		t.Fatalf("unexpected bare select star/from candidate in prefix mode: %#v", resp.Candidates)
	}
	for _, candidate := range resp.Candidates {
		if !strings.HasPrefix(strings.ToLower(candidate.InsertText), "e") {
			t.Fatalf("unexpected non-prefix select candidate: %#v", candidate)
		}
		if !strings.Contains(candidate.InsertText, " FROM ") {
			t.Fatalf("expected final-form select/from candidate: %#v", candidate)
		}
	}
}

func TestFromTailPrefersJoinAndClauseKeywords(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders"},
			},
		},
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	sql := "select * from orders "
	resp, err := engine.Complete(Request{
		SQL:            sql,
		CursorByte:     len(sql),
		CatalogVersion: version,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	if hasKind(resp.Candidates, CandidateKindTable) {
		t.Fatalf("unexpected table candidates in FROM tail context: %#v", resp.Candidates)
	}
	if hasKind(resp.Candidates, CandidateKindColumn) {
		t.Fatalf("unexpected column candidates in FROM tail context: %#v", resp.Candidates)
	}
	if !hasJoinContaining(resp.Candidates, "JOIN customers ON orders.customer_id = customers.id") {
		t.Fatalf("missing FK join suggestion in FROM tail context: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindKeyword, "WHERE", "WHERE ") {
		t.Fatalf("missing WHERE keyword in FROM tail context: %#v", resp.Candidates)
	}
}

func TestFromClauseStillSuggestsTablesWhileTypingSource(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{Parser: healthyParserStub()})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	sql := "select * from ord"
	resp, err := engine.Complete(Request{
		SQL:            sql,
		CursorByte:     len(sql),
		CatalogVersion: version,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	if !hasCandidate(resp.Candidates, CandidateKindTable, "public.orders", "orders") {
		t.Fatalf("missing table candidate while typing source table: %#v", resp.Candidates)
	}
}

func TestJoinOnPrefersColumnsAndPredicates(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders", "customers"},
				JoinConditions: []parser.JoinCondition{
					{
						Type:        core.JoinTypeInner,
						LeftTable:   "orders",
						RightTable:  "customers",
						LeftAlias:   "o",
						RightAlias:  "c",
						LeftColumn:  "customer_id",
						RightColumn: "id",
					},
				},
			},
		},
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	sql := "select o.id from orders o join customers c on "
	resp, err := engine.Complete(Request{
		SQL:            sql,
		CursorByte:     len(sql),
		CatalogVersion: version,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	if len(resp.Candidates) == 0 {
		t.Fatalf("Complete() candidates = 0, want >0")
	}
	if hasKind(resp.Candidates, CandidateKindTable) {
		t.Fatalf("unexpected table candidates in JOIN ON context: %#v", resp.Candidates)
	}
	if hasKind(resp.Candidates, CandidateKindSchema) {
		t.Fatalf("unexpected schema candidates in JOIN ON context: %#v", resp.Candidates)
	}
	if hasKind(resp.Candidates, CandidateKindJoin) {
		t.Fatalf("unexpected join candidates in JOIN ON context: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindColumn, "o.customer_id", "o.customer_id") {
		t.Fatalf("missing left-side alias column in JOIN ON context: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindColumn, "c.id", "c.id") {
		t.Fatalf("missing right-side alias column in JOIN ON context: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindKeyword, "AND", "AND ") {
		t.Fatalf("missing AND keyword in JOIN ON context: %#v", resp.Candidates)
	}
	if !hasCandidate(resp.Candidates, CandidateKindKeyword, "=", "= ") {
		t.Fatalf("missing comparison operator keyword in JOIN ON context: %#v", resp.Candidates)
	}
	if resp.Candidates[0].Kind != CandidateKindColumn {
		t.Fatalf("top candidate kind = %q, want %q; candidates=%#v", resp.Candidates[0].Kind, CandidateKindColumn, resp.Candidates)
	}

	firstKeyword := -1
	firstSnippet := -1
	for i, candidate := range resp.Candidates {
		if firstKeyword < 0 && candidate.Kind == CandidateKindKeyword {
			firstKeyword = i
		}
		if firstSnippet < 0 && candidate.Kind == CandidateKindSnippet {
			firstSnippet = i
		}
	}
	if firstKeyword < 0 {
		t.Fatalf("no keyword candidates in JOIN ON context: %#v", resp.Candidates)
	}
	if firstSnippet >= 0 && firstKeyword > firstSnippet {
		t.Fatalf("keyword candidates should rank above snippets in JOIN ON context: keywords=%d snippets=%d candidates=%#v", firstKeyword, firstSnippet, resp.Candidates)
	}
}

func TestWhereSuppressesSchemaCandidates(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders", "customers"},
			},
		},
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	sql := "select o.id from orders o join customers c on o.customer_id = c.id where "
	resp, err := engine.Complete(Request{
		SQL:            sql,
		CursorByte:     len(sql),
		CatalogVersion: version,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if hasKind(resp.Candidates, CandidateKindSchema) {
		t.Fatalf("unexpected schema candidates in WHERE context: %#v", resp.Candidates)
	}
}

func TestJoinOnSuppressesSchemaCandidates(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: &parserStub{
			metadata: &parser.QueryMetadata{
				Tables: []string{"orders", "customers"},
			},
		},
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	sql := "select o.id from orders o join customers c on "
	resp, err := engine.Complete(Request{
		SQL:            sql,
		CursorByte:     len(sql),
		CatalogVersion: version,
		MaxCandidates:  200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if hasKind(resp.Candidates, CandidateKindSchema) {
		t.Fatalf("unexpected schema candidates in JOIN ON context: %#v", resp.Candidates)
	}
}

func TestParseDegradedClauseScopedCandidates(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{Parser: failedParserStub()})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	tests := []struct {
		name             string
		sql              string
		wantTable        bool
		wantSchema       bool
		wantColumn       bool
		wantWhereKeyword bool
	}{
		{
			name:       "degraded where suppresses schema and table",
			sql:        "select o.id from orders o join customers c on o.customer_id = c.id where ",
			wantTable:  false,
			wantSchema: false,
			wantColumn: false,
		},
		{
			name:       "degraded join on suppresses schema and table",
			sql:        "select o.id from orders o join customers c on ",
			wantTable:  false,
			wantSchema: false,
			wantColumn: false,
		},
		{
			name:       "degraded from keeps table candidates",
			sql:        "select * from ",
			wantTable:  true,
			wantSchema: true,
			wantColumn: false,
		},
		{
			name:             "degraded from tail keeps continuation keywords",
			sql:              "select * from orders ",
			wantTable:        false,
			wantSchema:       false,
			wantColumn:       false,
			wantWhereKeyword: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp, err := engine.Complete(Request{
				SQL:            tc.sql,
				CursorByte:     len(tc.sql),
				CatalogVersion: version,
				MaxCandidates:  200,
			})
			if err != nil {
				t.Fatalf("Complete() error = %v", err)
			}

			if len(resp.Candidates) == 0 {
				t.Fatalf("Complete() candidates = 0, want >0")
			}
			if len(resp.Diagnostics) != 1 || resp.Diagnostics[0].Code != ParseDegraded {
				t.Fatalf("Complete() diagnostics = %#v, want one %q diagnostic", resp.Diagnostics, ParseDegraded)
			}

			if hasKind(resp.Candidates, CandidateKindTable) != tc.wantTable {
				t.Fatalf("table candidates mismatch: got=%v want=%v candidates=%#v", hasKind(resp.Candidates, CandidateKindTable), tc.wantTable, resp.Candidates)
			}
			if hasKind(resp.Candidates, CandidateKindSchema) != tc.wantSchema {
				t.Fatalf("schema candidates mismatch: got=%v want=%v candidates=%#v", hasKind(resp.Candidates, CandidateKindSchema), tc.wantSchema, resp.Candidates)
			}
			if hasKind(resp.Candidates, CandidateKindColumn) != tc.wantColumn {
				t.Fatalf("column candidates mismatch: got=%v want=%v candidates=%#v", hasKind(resp.Candidates, CandidateKindColumn), tc.wantColumn, resp.Candidates)
			}
			if tc.wantWhereKeyword && !hasCandidate(resp.Candidates, CandidateKindKeyword, "WHERE", "WHERE ") {
				t.Fatalf("missing WHERE keyword candidate: %#v", resp.Candidates)
			}
		})
	}
}

func hasCandidate(candidates []Candidate, kind CandidateKind, label string, insert string) bool {
	for _, candidate := range candidates {
		if candidate.Kind == kind && candidate.Label == label && candidate.InsertText == insert {
			return true
		}
	}
	return false
}

func hasJoinContaining(candidates []Candidate, fragment string) bool {
	for _, candidate := range candidates {
		if candidate.Kind != CandidateKindJoin {
			continue
		}
		if strings.Contains(candidate.InsertText, fragment) {
			return true
		}
	}
	return false
}

func hasSnippet(candidates []Candidate, label string) bool {
	for _, candidate := range candidates {
		if candidate.Kind != CandidateKindSnippet {
			continue
		}
		if candidate.Source != CandidateSourceSnippet {
			continue
		}
		if candidate.Label == label {
			return true
		}
	}
	return false
}

func hasKind(candidates []Candidate, kind CandidateKind) bool {
	for _, candidate := range candidates {
		if candidate.Kind == kind {
			return true
		}
	}
	return false
}
