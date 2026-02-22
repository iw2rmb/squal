package complete

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/iw2rmb/squal/core"
	"github.com/iw2rmb/squal/parser"
)

var updateGolden = flag.Bool("update", false, "update golden fixtures")

func TestGoldenCandidates(t *testing.T) {
	engine, version := newGoldenEngine(t)
	req := Request{
		SQL:            "select o.id from orders o join customers c on o.customer_id = c.id where ",
		CursorByte:     len("select o.id from orders o join customers c on o.customer_id = c.id where "),
		CatalogVersion: version,
		MaxCandidates:  200,
	}

	resp, err := engine.Complete(req)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	payload := struct {
		Source      CompletionSource `json:"source"`
		Diagnostics []Diagnostic     `json:"diagnostics,omitempty"`
		Candidates  []Candidate      `json:"candidates"`
	}{
		Source:      resp.Source,
		Diagnostics: resp.Diagnostics,
		Candidates:  resp.Candidates,
	}

	assertGoldenJSON(t, "testdata/candidates/basic_where.golden.json", payload)
}

func TestGoldenPlanEdit(t *testing.T) {
	engine := NewEngine(Config{})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	sql := "select o.cu from public.orders o"
	req := Request{
		SQL:            sql,
		CursorByte:     len("select o.cu"),
		CatalogVersion: version,
	}

	plan, diags, err := engine.PlanEdit(req, Candidate{
		Label:      "o.customer_id",
		InsertText: "o.customer_id",
		Kind:       CandidateKindColumn,
		Source:     CandidateSourceCatalog,
	})
	if err != nil {
		t.Fatalf("PlanEdit() error = %v", err)
	}

	payload := struct {
		Plan        EditPlan     `json:"plan"`
		Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
		RequestSpan core.Span    `json:"request_span"`
		RequestSQL  string       `json:"request_sql"`
	}{
		Plan:        plan,
		Diagnostics: diags,
		RequestSpan: plan.ReplacementSpan,
		RequestSQL:  sql,
	}

	assertGoldenJSON(t, "testdata/edits/qualified_column_replace.golden.json", payload)
}

func newGoldenEngine(t *testing.T) (Engine, CatalogVersion) {
	t.Helper()

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
	return engine, version
}

func assertGoldenJSON(t *testing.T, relativePath string, gotValue any) {
	t.Helper()

	got, err := json.MarshalIndent(gotValue, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent() error = %v", err)
	}
	got = append(got, '\n')

	goldenPath := filepath.Clean(relativePath)
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(goldenPath), err)
		}
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", goldenPath, err)
		}
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", goldenPath, err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("golden mismatch for %s\n--- want ---\n%s\n--- got ---\n%s", goldenPath, string(want), string(got))
	}
}
