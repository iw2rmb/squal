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

	if !hasCandidate(respA.Candidates, CandidateKindSchema, "public", "public") {
		t.Fatalf("missing schema candidate for public: %#v", respA.Candidates)
	}
	if !hasCandidate(respA.Candidates, CandidateKindSchema, "analytics", "analytics") {
		t.Fatalf("missing schema candidate for analytics: %#v", respA.Candidates)
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

func TestSnippetCandidates(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{Parser: healthyParserStub()})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	resp, err := engine.Complete(Request{
		SQL:             "select ",
		CursorByte:      len("select "),
		CatalogVersion:  version,
		IncludeSnippets: false,
		MaxCandidates:   200,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	if !hasSnippet(resp.Candidates, "SELECT ... FROM ...") {
		t.Fatalf("missing select/from snippet candidate: %#v", resp.Candidates)
	}
	if !hasSnippet(resp.Candidates, "WHERE ...") {
		t.Fatalf("missing where snippet candidate: %#v", resp.Candidates)
	}
	if !hasSnippet(resp.Candidates, "JOIN ... ON ...") {
		t.Fatalf("missing join snippet candidate: %#v", resp.Candidates)
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
