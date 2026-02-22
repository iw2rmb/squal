package complete

import (
	"testing"

	"github.com/iw2rmb/sql/core"
	"github.com/iw2rmb/sql/parser"
)

func TestSkeletonTypesCompile(t *testing.T) {
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
	}
	candidate := Candidate{
		Label:      "SELECT",
		InsertText: "SELECT ",
		Kind:       "keyword",
	}
	plan := EditPlan{
		Edits: []core.TextEdit{
			{
				Span:    core.Span{StartByte: 0, EndByte: 0},
				NewText: "SELECT ",
			},
		},
	}
	resp := Response{
		Candidates: []Candidate{candidate},
		Diagnostics: []Diagnostic{
			{Code: ParseDegraded, Message: "degraded"},
		},
		Source: "parser",
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
	if len(resp.Candidates) != 1 {
		t.Fatalf("unexpected candidate count: %d", len(resp.Candidates))
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

	var dependency parser.Parser
	cfg := Config{
		Parser: dependency,
	}
	if cfg.Parser != nil {
		t.Fatal("unexpected non-nil parser dependency")
	}
}
