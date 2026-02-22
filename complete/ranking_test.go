package complete

import (
	"reflect"
	"testing"

	"github.com/iw2rmb/squal/core"
	"github.com/iw2rmb/squal/parser"
)

func TestRankingDeterminism(t *testing.T) {
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
		SQL:            "select o.i from orders o join customers c on o.customer_id = c.id where o.i",
		CursorByte:     len("select o.i from orders o join customers c on o.customer_id = c.id where o.i"),
		CatalogVersion: version,
		MaxCandidates:  200,
	}

	var baseline []Candidate
	for i := 0; i < 10; i++ {
		resp, err := engine.Complete(req)
		if err != nil {
			t.Fatalf("Complete() run %d error = %v", i, err)
		}
		if len(resp.Candidates) == 0 {
			t.Fatalf("Complete() run %d returned no candidates", i)
		}
		if i == 0 {
			baseline = resp.Candidates
		} else if !reflect.DeepEqual(baseline, resp.Candidates) {
			t.Fatalf("Complete() run %d candidates differ from baseline:\nbaseline=%#v\ncurrent=%#v", i, baseline, resp.Candidates)
		}
	}

	if !candidatesAreSorted(baseline) {
		t.Fatalf("candidates are not sorted by deterministic ranking comparator: %#v", baseline)
	}

	candidate, found := findCandidate(baseline, CandidateKindColumn, "o.id")
	if !found {
		t.Fatalf("missing expected candidate o.id in %#v", baseline)
	}
	if !candidate.SortKey.ExactPrefix {
		t.Fatalf("candidate o.id exact prefix = false, want true for prefix \"i\": %#v", candidate)
	}
}

func TestStableTiebreaks(t *testing.T) {
	t.Parallel()

	set := newCandidateSet()
	set.add(Candidate{
		ID:         "table:beta",
		Label:      "beta",
		InsertText: "beta",
		Kind:       CandidateKindTable,
		Score:      100,
		SortKey: CandidateSortKey{
			ExactPrefix: false,
		},
		Source: CandidateSourceCatalog,
	})
	set.add(Candidate{
		ID:         "table:alpha",
		Label:      "alpha",
		InsertText: "alpha",
		Kind:       CandidateKindTable,
		Score:      100,
		SortKey: CandidateSortKey{
			ExactPrefix: true,
		},
		Source: CandidateSourceCatalog,
	})
	set.add(Candidate{
		ID:         "schema:zeta",
		Label:      "zeta",
		InsertText: "zeta",
		Kind:       CandidateKindSchema,
		Score:      100,
		SortKey: CandidateSortKey{
			ExactPrefix: false,
		},
		Source: CandidateSourceCatalog,
	})
	set.add(Candidate{
		ID:         "table:gamma",
		Label:      "gamma",
		InsertText: "gamma",
		Kind:       CandidateKindTable,
		Score:      100,
		SortKey: CandidateSortKey{
			ExactPrefix: false,
		},
		Source: CandidateSourceCatalog,
	})

	got := set.finalize(0)

	wantOrder := []string{
		"schema:zeta",
		"table:alpha",
		"table:beta",
		"table:gamma",
	}
	if len(got) != len(wantOrder) {
		t.Fatalf("finalize() candidate count = %d, want %d", len(got), len(wantOrder))
	}
	for i, wantID := range wantOrder {
		if got[i].ID != wantID {
			t.Fatalf("finalize() candidate[%d].ID = %q, want %q; got=%#v", i, got[i].ID, wantID, got)
		}
	}
}

func candidatesAreSorted(candidates []Candidate) bool {
	for i := 1; i < len(candidates); i++ {
		if candidateLess(candidates[i], candidates[i-1]) {
			return false
		}
	}
	return true
}

func findCandidate(candidates []Candidate, kind CandidateKind, insertText string) (Candidate, bool) {
	for _, candidate := range candidates {
		if candidate.Kind == kind && candidate.InsertText == insertText {
			return candidate, true
		}
	}
	return Candidate{}, false
}
