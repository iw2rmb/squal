package complete

import (
	"testing"

	"github.com/iw2rmb/squall/core"
	"github.com/iw2rmb/squall/parser"
)

func TestTypesContractCompile(t *testing.T) {
	t.Parallel()

	snapshot := CatalogSnapshot{
		Schemas: []core.CatalogSchema{
			{
				Name: "public",
			},
		},
		SearchPath: []string{"public"},
	}
	req := Request{
		SQL:            "select 1",
		CursorByte:     8,
		CatalogVersion: CatalogVersion("v1"),
		MaxCandidates:  20,
	}
	candidate := Candidate{
		ID:         "kw.select",
		Label:      "SELECT",
		InsertText: "SELECT ",
		Kind:       CandidateKindKeyword,
		Score:      1.0,
		ScoreComponents: ScoreComponents{
			Context: 0.5,
			Prefix:  0.5,
		},
		SortKey: CandidateSortKey{
			KindPriority:  10,
			ExactPrefix:   true,
			LabelLexical:  "SELECT",
			InsertLexical: "SELECT ",
		},
		Source: CandidateSourceParser,
	}
	plan := EditPlan{
		ReplacementSpan: core.Span{StartByte: 0, EndByte: 0},
		Edits: []core.TextEdit{
			{
				Span:    core.Span{StartByte: 0, EndByte: 0},
				NewText: "SELECT ",
			},
		},
		Validation: EditPlanValidation{
			InBounds:       true,
			NonOverlapping: true,
		},
	}
	resp := Response{
		Candidates:       []Candidate{candidate},
		SelectedEditPlan: &plan,
		Diagnostics: []Diagnostic{
			{Code: ParseDegraded, Message: "degraded"},
		},
		Source: CompletionSourceParser,
	}

	if snapshot.Schemas[0].Name != "public" {
		t.Fatalf("unexpected schema name: %q", snapshot.Schemas[0].Name)
	}
	if req.CatalogVersion != "v1" {
		t.Fatalf("unexpected catalog version: %q", req.CatalogVersion)
	}
	if len(plan.Edits) != 1 {
		t.Fatalf("unexpected edit count: %d", len(plan.Edits))
	}
	if !plan.Validation.NonOverlapping {
		t.Fatal("expected non-overlapping plan")
	}
	if len(resp.Candidates) != 1 {
		t.Fatalf("unexpected candidate count: %d", len(resp.Candidates))
	}
	if resp.SelectedEditPlan == nil {
		t.Fatal("expected selected edit plan")
	}
}

type engineStub struct{}

func (e *engineStub) InitCatalog(snapshot CatalogSnapshot) (CatalogVersion, error) {
	return CatalogVersion("v1"), nil
}

func (e *engineStub) UpdateCatalog(snapshot CatalogSnapshot) (CatalogVersion, error) {
	return CatalogVersion("v2"), nil
}

func (e *engineStub) Complete(req Request) (Response, error) {
	return Response{}, nil
}

func (e *engineStub) PlanEdit(req Request, accepted Candidate) (EditPlan, []Diagnostic, error) {
	return EditPlan{}, nil, nil
}

func TestEngineContractCompile(t *testing.T) {
	t.Parallel()

	var _ Engine = (*engineStub)(nil)

	var dependency parser.MetadataExtractor
	var provider CompletionProvider
	cfg := Config{
		Parser:   dependency,
		Provider: provider,
	}
	if cfg.Parser != nil {
		t.Fatal("unexpected non-nil parser dependency")
	}
	if cfg.Provider != nil {
		t.Fatal("unexpected non-nil provider dependency")
	}
}
