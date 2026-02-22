package complete

import "testing"

func TestValidateRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		req      Request
		wantCode DiagnosticCode
	}{
		{
			name: "valid",
			req: Request{
				SQL:        "select 1",
				CursorByte: 8,
			},
		},
		{
			name: "missing sql snapshot",
			req: Request{
				SQL:        "",
				CursorByte: 0,
			},
			wantCode: CatalogMissing,
		},
		{
			name: "negative cursor",
			req: Request{
				SQL:        "select 1",
				CursorByte: -1,
			},
			wantCode: InvalidCursorSpan,
		},
		{
			name: "cursor beyond sql length",
			req: Request{
				SQL:        "select 1",
				CursorByte: 9,
			},
			wantCode: InvalidCursorSpan,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diags := validateRequest(tc.req)
			if tc.wantCode == "" {
				if len(diags) != 0 {
					t.Fatalf("validateRequest() diagnostics = %#v, want none", diags)
				}
				return
			}

			if len(diags) != 1 {
				t.Fatalf("validateRequest() diagnostics = %#v, want exactly one", diags)
			}
			if diags[0].Code != tc.wantCode {
				t.Fatalf("validateRequest() code = %q, want %q", diags[0].Code, tc.wantCode)
			}
		})
	}
}

func TestRequestNormalization(t *testing.T) {
	t.Parallel()

	t.Run("applies defaults", func(t *testing.T) {
		t.Parallel()

		normalized := normalizeRequest(Request{
			SQL:             "select * from t",
			CursorByte:      7,
			MaxCandidates:   0,
			IncludeSnippets: false,
		})
		if normalized.MaxCandidates != defaultMaxCandidates {
			t.Fatalf("normalizeRequest() max candidates = %d, want %d", normalized.MaxCandidates, defaultMaxCandidates)
		}
		if !normalized.IncludeSnippets {
			t.Fatal("normalizeRequest() snippets = false, want true")
		}
	})

	t.Run("preserves positive max candidates", func(t *testing.T) {
		t.Parallel()

		normalized := normalizeRequest(Request{
			SQL:             "select * from t",
			CursorByte:      7,
			MaxCandidates:   5,
			IncludeSnippets: false,
		})
		if normalized.MaxCandidates != 5 {
			t.Fatalf("normalizeRequest() max candidates = %d, want 5", normalized.MaxCandidates)
		}
		if !normalized.IncludeSnippets {
			t.Fatal("normalizeRequest() snippets = false, want true")
		}
	})
}

func TestInvalidCursorSpan(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{})

	completeResp, err := engine.Complete(Request{
		SQL:            "select 1",
		CursorByte:     -1,
		CatalogVersion: "",
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(completeResp.Candidates) != 0 {
		t.Fatalf("Complete() candidates = %d, want 0", len(completeResp.Candidates))
	}
	if len(completeResp.Diagnostics) != 1 {
		t.Fatalf("Complete() diagnostics = %#v, want exactly one", completeResp.Diagnostics)
	}
	if completeResp.Diagnostics[0].Code != InvalidCursorSpan {
		t.Fatalf("Complete() diagnostic code = %q, want %q", completeResp.Diagnostics[0].Code, InvalidCursorSpan)
	}

	editPlan, planDiags, err := engine.PlanEdit(Request{
		SQL:            "select 1",
		CursorByte:     -1,
		CatalogVersion: "",
	}, Candidate{
		Label:      "SELECT",
		InsertText: "SELECT ",
		Kind:       CandidateKindKeyword,
	})
	if err != nil {
		t.Fatalf("PlanEdit() error = %v", err)
	}
	if len(editPlan.Edits) != 0 {
		t.Fatalf("PlanEdit() edits = %d, want 0", len(editPlan.Edits))
	}
	if len(planDiags) != 1 {
		t.Fatalf("PlanEdit() diagnostics = %#v, want exactly one", planDiags)
	}
	if planDiags[0].Code != InvalidCursorSpan {
		t.Fatalf("PlanEdit() diagnostic code = %q, want %q", planDiags[0].Code, InvalidCursorSpan)
	}
}
