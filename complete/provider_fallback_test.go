package complete

import (
	"reflect"
	"testing"
)

func TestProviderSuccessPath(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: healthyParserStub(),
		Provider: &providerStub{
			result: ProviderResult{
				Candidates: []Candidate{
					{
						Label:      "cust_name",
						InsertText: "cust_name",
						Kind:       CandidateKindColumn,
						Source:     CandidateSourceCatalog,
					},
					{
						Label:      "customers",
						InsertText: "customers",
						Kind:       CandidateKindTable,
						Source:     CandidateSourceParser,
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
		SQL:            "select cust",
		CursorByte:     len("select cust"),
		CatalogVersion: version,
		MaxCandidates:  20,
	}

	var baselineCandidates []Candidate
	for i := 0; i < 3; i++ {
		resp, err := engine.Complete(req)
		if err != nil {
			t.Fatalf("Complete() run %d error = %v", i, err)
		}
		if resp.Source != CompletionSourceProvider {
			t.Fatalf("Complete() run %d source = %q, want %q", i, resp.Source, CompletionSourceProvider)
		}
		if len(resp.Diagnostics) != 0 {
			t.Fatalf("Complete() run %d diagnostics = %#v, want none", i, resp.Diagnostics)
		}
		if len(resp.Candidates) != 2 {
			t.Fatalf("Complete() run %d candidates = %d, want 2", i, len(resp.Candidates))
		}
		for _, candidate := range resp.Candidates {
			if candidate.Source != CandidateSourceProvider {
				t.Fatalf("Complete() run %d candidate source = %q, want %q", i, candidate.Source, CandidateSourceProvider)
			}
		}

		if i == 0 {
			baselineCandidates = resp.Candidates
			continue
		}
		if !reflect.DeepEqual(resp.Candidates, baselineCandidates) {
			t.Fatalf("Complete() run %d candidates differ from baseline:\nbaseline=%#v\ncurrent=%#v", i, baselineCandidates, resp.Candidates)
		}
	}
}

func TestProviderUnavailableFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provider CompletionProvider
	}{
		{
			name: "provider returns error",
			provider: &providerStub{
				err: errProviderUnavailable,
			},
		},
		{
			name: "provider returns empty candidates",
			provider: &providerStub{
				result: ProviderResult{Candidates: nil},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := NewEngine(Config{
				Parser:   healthyParserStub(),
				Provider: tc.provider,
			})
			version, err := engine.InitCatalog(catalogSnapshotVariantA())
			if err != nil {
				t.Fatalf("InitCatalog() error = %v", err)
			}

			req := Request{
				SQL:            "select o.id from orders o where ",
				CursorByte:     len("select o.id from orders o where "),
				CatalogVersion: version,
				MaxCandidates:  100,
			}

			var baselineResponse Response
			for i := 0; i < 3; i++ {
				resp, err := engine.Complete(req)
				if err != nil {
					t.Fatalf("Complete() run %d error = %v", i, err)
				}
				if resp.Source != CompletionSourceParserFallback {
					t.Fatalf("Complete() run %d source = %q, want %q", i, resp.Source, CompletionSourceParserFallback)
				}
				if len(resp.Candidates) == 0 {
					t.Fatalf("Complete() run %d candidates = 0, want >0", i)
				}
				if len(resp.Diagnostics) == 0 {
					t.Fatalf("Complete() run %d diagnostics = %#v, want at least one", i, resp.Diagnostics)
				}
				if resp.Diagnostics[0].Code != ProviderUnavailable {
					t.Fatalf("Complete() run %d diagnostics[0].Code = %q, want %q", i, resp.Diagnostics[0].Code, ProviderUnavailable)
				}

				if i == 0 {
					baselineResponse = resp
					continue
				}
				if !reflect.DeepEqual(resp, baselineResponse) {
					t.Fatalf("Complete() run %d response differs from baseline:\nbaseline=%#v\ncurrent=%#v", i, baselineResponse, resp)
				}
			}
		})
	}
}

func TestNoProviderConfiguredUsesParserPath(t *testing.T) {
	t.Parallel()

	engine := NewEngine(Config{
		Parser: healthyParserStub(),
	})
	version, err := engine.InitCatalog(catalogSnapshotVariantA())
	if err != nil {
		t.Fatalf("InitCatalog() error = %v", err)
	}

	resp, err := engine.Complete(Request{
		SQL:            "select o.id from orders o where ",
		CursorByte:     len("select o.id from orders o where "),
		CatalogVersion: version,
		MaxCandidates:  100,
	})
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if resp.Source != CompletionSourceParser {
		t.Fatalf("Complete() source = %q, want %q", resp.Source, CompletionSourceParser)
	}
	for _, diag := range resp.Diagnostics {
		if diag.Code == ProviderUnavailable {
			t.Fatalf("Complete() diagnostics unexpectedly contain %q: %#v", ProviderUnavailable, resp.Diagnostics)
		}
	}
}
