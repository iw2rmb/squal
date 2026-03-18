package complete

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/iw2rmb/squall/core"
	"github.com/iw2rmb/squall/parser"
)

func FuzzCompleteAndPlanEditProperties(f *testing.F) {
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
		f.Fatalf("InitCatalog() error = %v", err)
	}

	midPi := strings.Index("select π from public.orders", "π") + 1
	f.Add("select o.cu from public.orders o where o.cu", len("select o.cu from public.orders o where o.cu"), "o.customer_id")
	f.Add("select o.cu from public.orders o", len("select o.cu"), "o.customer_id")
	f.Add("select π from public.orders", midPi, "WHERE ")
	f.Add("select '漢字' as value from public.orders where o.cu", len("select '漢字' as value from public.orders where o.cu"), "o.customer_id")
	f.Add("", 0, "WHERE ")
	f.Add("select 1", -1, "WHERE ")
	f.Add("select 1", 999, "WHERE ")
	f.Add("select o.cu from public.orders o", len("select o.cu"), "")
	f.Add("select \xff", len("select \xff"), "WHERE ")

	f.Fuzz(func(t *testing.T, sql string, cursor int, insertText string) {
		completeReq := Request{
			SQL:            sql,
			CursorByte:     cursor,
			CatalogVersion: version,
			MaxCandidates:  64,
		}

		respA, err := engine.Complete(completeReq)
		if err != nil {
			t.Fatalf("Complete() first call error = %v", err)
		}
		respB, err := engine.Complete(completeReq)
		if err != nil {
			t.Fatalf("Complete() second call error = %v", err)
		}
		if !reflect.DeepEqual(respA, respB) {
			t.Fatalf("Complete() is non-deterministic for sql=%q cursor=%d\nA=%#v\nB=%#v", sql, cursor, respA, respB)
		}

		if sql == "" {
			if !hasDiagnosticCode(respA.Diagnostics, CatalogMissing) {
				t.Fatalf("expected %q for empty sql, got %#v", CatalogMissing, respA.Diagnostics)
			}
		} else if cursor < 0 || cursor > len(sql) {
			if !hasDiagnosticCode(respA.Diagnostics, InvalidCursorSpan) {
				t.Fatalf("expected %q for out-of-bounds cursor=%d len=%d, got %#v", InvalidCursorSpan, cursor, len(sql), respA.Diagnostics)
			}
			if len(respA.Candidates) != 0 {
				t.Fatalf("out-of-bounds cursor should return zero candidates, got %d", len(respA.Candidates))
			}
		} else if hasDiagnosticCode(respA.Diagnostics, InvalidCursorSpan) {
			t.Fatalf("unexpected %q for in-bounds cursor=%d len=%d diagnostics=%#v", InvalidCursorSpan, cursor, len(sql), respA.Diagnostics)
		}

		planReq := Request{
			SQL:            sql,
			CursorByte:     cursor,
			CatalogVersion: version,
		}
		accepted := Candidate{
			Label:      "fuzz",
			InsertText: insertText,
			Kind:       CandidateKindColumn,
			Source:     CandidateSourceCatalog,
		}

		planA, diagsA, err := engine.PlanEdit(planReq, accepted)
		if err != nil {
			t.Fatalf("PlanEdit() first call error = %v", err)
		}
		planB, diagsB, err := engine.PlanEdit(planReq, accepted)
		if err != nil {
			t.Fatalf("PlanEdit() second call error = %v", err)
		}
		if !reflect.DeepEqual(planA, planB) {
			t.Fatalf("PlanEdit() is non-deterministic for sql=%q cursor=%d insert=%q\nA=%#v\nB=%#v", sql, cursor, insertText, planA, planB)
		}
		if !reflect.DeepEqual(diagsA, diagsB) {
			t.Fatalf("PlanEdit() diagnostics are non-deterministic for sql=%q cursor=%d insert=%q\nA=%#v\nB=%#v", sql, cursor, insertText, diagsA, diagsB)
		}

		if sql == "" {
			if !hasDiagnosticCode(diagsA, CatalogMissing) {
				t.Fatalf("expected %q for empty sql, got %#v", CatalogMissing, diagsA)
			}
			if len(planA.Edits) != 0 {
				t.Fatalf("empty sql should produce zero edits, got %d", len(planA.Edits))
			}
			return
		}

		if cursor < 0 || cursor > len(sql) {
			if !hasDiagnosticCode(diagsA, InvalidCursorSpan) {
				t.Fatalf("expected %q for out-of-bounds cursor=%d len=%d, got %#v", InvalidCursorSpan, cursor, len(sql), diagsA)
			}
			if len(planA.Edits) != 0 {
				t.Fatalf("out-of-bounds cursor should produce zero edits, got %d", len(planA.Edits))
			}
			return
		}

		if len(planA.Edits) == 0 {
			if len(diagsA) == 0 {
				t.Fatalf("expected diagnostics when no edits are produced")
			}
			for _, diag := range diagsA {
				if diag.Code != EditConflict {
					t.Fatalf("unexpected diagnostic code %q with zero edits; diagnostics=%#v", diag.Code, diagsA)
				}
			}
			return
		}

		if len(diagsA) != 0 {
			t.Fatalf("expected zero diagnostics when edits are produced, got %#v", diagsA)
		}

		changeSet := core.TextChangeSet{Edits: planA.Edits}
		if !changeSet.Validate(len(sql)) {
			t.Fatalf("planned edit set failed validation for sql len %d: %#v", len(sql), planA)
		}

		edited, ok := applyEdits(sql, planA.Edits)
		if !ok {
			t.Fatalf("applyEdits() rejected valid plan: %#v", planA)
		}

		if utf8.ValidString(sql) && utf8.ValidString(insertText) {
			for _, edit := range planA.Edits {
				if !utf8.ValidString(sql[:edit.Span.StartByte]) {
					t.Fatalf("start span %d splits rune in valid input %q", edit.Span.StartByte, sql)
				}
				if !utf8.ValidString(sql[:edit.Span.EndByte]) {
					t.Fatalf("end span %d splits rune in valid input %q", edit.Span.EndByte, sql)
				}
			}
			if !utf8.ValidString(edited) {
				t.Fatalf("edited sql became invalid utf-8: %q", edited)
			}
		}
	})
}

func hasDiagnosticCode(diags []Diagnostic, code DiagnosticCode) bool {
	for _, diag := range diags {
		if diag.Code == code {
			return true
		}
	}
	return false
}

func applyEdits(sql string, edits []core.TextEdit) (string, bool) {
	changeSet := core.TextChangeSet{Edits: edits}
	if !changeSet.Validate(len(sql)) {
		return "", false
	}

	canonical := changeSet.Canonicalize().Edits
	out := sql
	for i := len(canonical) - 1; i >= 0; i-- {
		edit := canonical[i]
		out = out[:edit.Span.StartByte] + edit.NewText + out[edit.Span.EndByte:]
	}
	return out, true
}
