package complete

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/iw2rmb/squall/core"
	"github.com/iw2rmb/squall/parser"
)

var updateGolden = flag.Bool("update", false, "update golden fixtures")

func TestGoldenCandidates(t *testing.T) {
	testCases := []struct {
		name    string
		fixture string
		sql     string
		parser  parser.MetadataExtractor
	}{
		{
			name:    "basic where",
			fixture: "testdata/candidates/basic_where.golden.json",
			sql:     "select o.id from orders o join customers c on o.customer_id = c.id where ",
			parser:  goldenParserStub(),
		},
		{
			name:    "from tail continuation",
			fixture: "testdata/candidates/from_tail_continuation.golden.json",
			sql:     "select * from orders ",
			parser:  goldenParserStub(),
		},
		{
			name:    "degraded where",
			fixture: "testdata/candidates/degraded_where.golden.json",
			sql:     "select o.id from orders o join customers c on o.customer_id = c.id where ",
			parser:  failedParserStub(),
		},
		{
			name:    "degraded join on",
			fixture: "testdata/candidates/degraded_join_on.golden.json",
			sql:     "select o.id from orders o join customers c on ",
			parser:  failedParserStub(),
		},
		{
			name:    "join on context",
			fixture: "testdata/candidates/join_on_context.golden.json",
			sql:     "select o.id from orders o join customers c on ",
			parser:  goldenParserStub(),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			engine, version := newGoldenEngine(t, tc.parser)
			resp, err := engine.Complete(Request{
				SQL:            tc.sql,
				CursorByte:     len(tc.sql),
				CatalogVersion: version,
				MaxCandidates:  200,
			})
			if err != nil {
				t.Fatalf("Complete() error = %v", err)
			}

			payload := goldenCandidatePayload{
				Scenario:     tc.name,
				SQL:          tc.sql,
				ActiveClause: activeClauseAtCursor(tc.sql, len(tc.sql)),
				Source:       resp.Source,
				Diagnostics:  resp.Diagnostics,
				Candidates:   resp.Candidates,
			}

			assertGoldenJSON(t, tc.fixture, payload)
		})
	}

	t.Run("quoted comment keyword safety", func(t *testing.T) {
		engine, version := newGoldenEngine(t, failedParserStub())
		safetyCases := []struct {
			name string
			sql  string
		}{
			{
				name: "single quoted from does not switch clause",
				sql:  "select 'from' as s where ",
			},
			{
				name: "line comment where does not switch clause",
				sql:  "select 1 -- where\nfrom orders ",
			},
			{
				name: "double quoted join does not switch clause",
				sql:  `select "join" from orders `,
			},
		}

		payload := goldenKeywordSafetyPayload{
			Scenario: "quoted_comment_keyword_safety",
			Cases:    make([]goldenKeywordSafetyCase, 0, len(safetyCases)),
		}
		for _, c := range safetyCases {
			resp, err := engine.Complete(Request{
				SQL:            c.sql,
				CursorByte:     len(c.sql),
				CatalogVersion: version,
				MaxCandidates:  200,
			})
			if err != nil {
				t.Fatalf("Complete() error for case %q = %v", c.name, err)
			}
			payload.Cases = append(payload.Cases, goldenKeywordSafetyCase{
				Name:         c.name,
				SQL:          c.sql,
				ActiveClause: activeClauseAtCursor(c.sql, len(c.sql)),
				Source:       resp.Source,
				Diagnostics:  resp.Diagnostics,
				Candidates:   resp.Candidates,
			})
		}

		assertGoldenJSON(t, "testdata/candidates/quoted_comment_keyword_safety.golden.json", payload)
	})
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

type goldenCandidatePayload struct {
	Scenario     string           `json:"scenario"`
	SQL          string           `json:"sql"`
	ActiveClause contextClause    `json:"active_clause"`
	Source       CompletionSource `json:"source"`
	Diagnostics  []Diagnostic     `json:"diagnostics,omitempty"`
	Candidates   []Candidate      `json:"candidates"`
}

type goldenKeywordSafetyPayload struct {
	Scenario string                    `json:"scenario"`
	Cases    []goldenKeywordSafetyCase `json:"cases"`
}

type goldenKeywordSafetyCase struct {
	Name         string           `json:"name"`
	SQL          string           `json:"sql"`
	ActiveClause contextClause    `json:"active_clause"`
	Source       CompletionSource `json:"source"`
	Diagnostics  []Diagnostic     `json:"diagnostics,omitempty"`
	Candidates   []Candidate      `json:"candidates"`
}

func newGoldenEngine(t *testing.T, completionParser parser.MetadataExtractor) (Engine, CatalogVersion) {
	t.Helper()

	engine := NewEngine(Config{
		Parser: completionParser,
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}
	return engine, version
}

func goldenParserStub() parser.MetadataExtractor {
	return &parserStub{
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
	}
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
