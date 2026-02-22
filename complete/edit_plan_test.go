package complete

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPlanEditDeterminism(t *testing.T) {
	t.Parallel()

	engine, version := newPlanEditEngine(t)
	sql := "select o.cu from public.orders o"
	cursor := strings.Index(sql, "cu") + len("cu")
	req := Request{
		SQL:            sql,
		CursorByte:     cursor,
		CatalogVersion: version,
	}
	accepted := Candidate{
		Label:      "o.customer_id",
		InsertText: "o.customer_id",
		Kind:       CandidateKindColumn,
		Source:     CandidateSourceCatalog,
	}

	var baselinePlan EditPlan
	var baselineDiagnostics []Diagnostic
	for i := 0; i < 5; i++ {
		plan, diags, err := engine.PlanEdit(req, accepted)
		if err != nil {
			t.Fatalf("PlanEdit() run %d error = %v", i, err)
		}

		if i == 0 {
			baselinePlan = plan
			baselineDiagnostics = diags
			continue
		}
		if !reflect.DeepEqual(plan, baselinePlan) {
			t.Fatalf("PlanEdit() run %d plan is non-deterministic:\nplan=%#v\nbaseline=%#v", i, plan, baselinePlan)
		}
		if !reflect.DeepEqual(diags, baselineDiagnostics) {
			t.Fatalf("PlanEdit() run %d diagnostics are non-deterministic:\ndiags=%#v\nbaseline=%#v", i, diags, baselineDiagnostics)
		}
	}
}

func TestPlanEditTrailingTokenReplacement(t *testing.T) {
	t.Parallel()

	engine, version := newPlanEditEngine(t)
	sql := "select o.cu from public.orders o"
	start := strings.Index(sql, "cu")
	req := Request{
		SQL:            sql,
		CursorByte:     start + len("cu"),
		CatalogVersion: version,
	}
	accepted := Candidate{
		Label:      "o.customer_id",
		InsertText: "o.customer_id",
		Kind:       CandidateKindColumn,
		Source:     CandidateSourceCatalog,
	}

	plan, diags, err := engine.PlanEdit(req, accepted)
	if err != nil {
		t.Fatalf("PlanEdit() error = %v", err)
	}
	if len(diags) != 0 {
		t.Fatalf("PlanEdit() diagnostics = %#v, want none", diags)
	}
	if len(plan.Edits) != 1 {
		t.Fatalf("PlanEdit() edits = %d, want 1", len(plan.Edits))
	}
	if plan.ReplacementSpan.StartByte != start || plan.ReplacementSpan.EndByte != start+2 {
		t.Fatalf("PlanEdit() replacement span = %#v, want [%d,%d)", plan.ReplacementSpan, start, start+2)
	}
	if plan.Edits[0].Span != plan.ReplacementSpan {
		t.Fatalf("PlanEdit() edit span = %#v, want %#v", plan.Edits[0].Span, plan.ReplacementSpan)
	}
	if plan.Edits[0].NewText != "customer_id" {
		t.Fatalf("PlanEdit() edit text = %q, want %q", plan.Edits[0].NewText, "customer_id")
	}
	if !plan.Validation.InBounds || !plan.Validation.NonOverlapping {
		t.Fatalf("PlanEdit() validation = %#v, want in-bounds and non-overlapping", plan.Validation)
	}
}

func TestPlanEditInsertAtCursorWhenNoToken(t *testing.T) {
	t.Parallel()

	engine, version := newPlanEditEngine(t)
	sql := "select * from public.orders "
	req := Request{
		SQL:            sql,
		CursorByte:     len(sql),
		CatalogVersion: version,
	}
	accepted := Candidate{
		Label:      "WHERE",
		InsertText: "WHERE ",
		Kind:       CandidateKindKeyword,
		Source:     CandidateSourceParser,
	}

	plan, diags, err := engine.PlanEdit(req, accepted)
	if err != nil {
		t.Fatalf("PlanEdit() error = %v", err)
	}
	if len(diags) != 0 {
		t.Fatalf("PlanEdit() diagnostics = %#v, want none", diags)
	}
	if len(plan.Edits) != 1 {
		t.Fatalf("PlanEdit() edits = %d, want 1", len(plan.Edits))
	}
	if plan.ReplacementSpan.StartByte != len(sql) || plan.ReplacementSpan.EndByte != len(sql) {
		t.Fatalf("PlanEdit() replacement span = %#v, want [%d,%d)", plan.ReplacementSpan, len(sql), len(sql))
	}
	if plan.Edits[0].NewText != "WHERE " {
		t.Fatalf("PlanEdit() edit text = %q, want %q", plan.Edits[0].NewText, "WHERE ")
	}
}

func TestEditConflictInvalidCandidate(t *testing.T) {
	t.Parallel()

	engine, version := newPlanEditEngine(t)
	req := Request{
		SQL:            "select o.id from public.orders o",
		CursorByte:     strings.Index("select o.id from public.orders o", "id") + len("id"),
		CatalogVersion: version,
	}

	plan, diags, err := engine.PlanEdit(req, Candidate{
		Label:      "o.customer_id",
		InsertText: "",
		Kind:       CandidateKindColumn,
	})
	if err != nil {
		t.Fatalf("PlanEdit() error = %v", err)
	}
	if len(plan.Edits) != 0 {
		t.Fatalf("PlanEdit() edits = %d, want 0", len(plan.Edits))
	}
	if len(diags) != 1 {
		t.Fatalf("PlanEdit() diagnostics = %#v, want exactly one", diags)
	}
	if diags[0].Code != EditConflict {
		t.Fatalf("PlanEdit() diagnostics[0].Code = %q, want %q", diags[0].Code, EditConflict)
	}
}

func TestUnicodeSpanSafety(t *testing.T) {
	t.Parallel()

	t.Run("replacement spans stay on rune boundaries", func(t *testing.T) {
		engine, version := newPlanEditEngine(t)
		sql := "select 'π' as value from public.orders o where o.cu"
		start := strings.LastIndex(sql, "cu")
		req := Request{
			SQL:            sql,
			CursorByte:     start + len("cu"),
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
		if len(diags) != 0 {
			t.Fatalf("PlanEdit() diagnostics = %#v, want none", diags)
		}
		if len(plan.Edits) != 1 {
			t.Fatalf("PlanEdit() edits = %d, want 1", len(plan.Edits))
		}

		span := plan.Edits[0].Span
		if !utf8.ValidString(sql[:span.StartByte]) {
			t.Fatalf("edit start byte %d splits a rune", span.StartByte)
		}
		if !utf8.ValidString(sql[:span.EndByte]) {
			t.Fatalf("edit end byte %d splits a rune", span.EndByte)
		}

		edited := sql[:span.StartByte] + plan.Edits[0].NewText + sql[span.EndByte:]
		if !utf8.ValidString(edited) {
			t.Fatalf("edited sql is not valid utf-8: %q", edited)
		}
		if !strings.Contains(edited, "o.customer_id") {
			t.Fatalf("edited sql = %q, expected replacement to keep qualifier + new identifier", edited)
		}
	})

	t.Run("cursor in middle of rune returns conflict", func(t *testing.T) {
		engine, version := newPlanEditEngine(t)
		sql := "select π from public.orders"
		pi := strings.Index(sql, "π")
		req := Request{
			SQL:            sql,
			CursorByte:     pi + 1,
			CatalogVersion: version,
		}

		plan, diags, err := engine.PlanEdit(req, Candidate{
			Label:      "WHERE",
			InsertText: "WHERE ",
			Kind:       CandidateKindKeyword,
		})
		if err != nil {
			t.Fatalf("PlanEdit() error = %v", err)
		}
		if len(plan.Edits) != 0 {
			t.Fatalf("PlanEdit() edits = %d, want 0", len(plan.Edits))
		}
		if len(diags) != 1 {
			t.Fatalf("PlanEdit() diagnostics = %#v, want exactly one", diags)
		}
		if diags[0].Code != EditConflict {
			t.Fatalf("PlanEdit() diagnostics[0].Code = %q, want %q", diags[0].Code, EditConflict)
		}
	})
}

func newPlanEditEngine(t *testing.T) (Engine, CatalogVersion) {
	t.Helper()

	engine := NewEngine(Config{})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}
	return engine, version
}
