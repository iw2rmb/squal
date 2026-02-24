package complete

import "sync"

const providerUnavailableMessage = "provider unavailable; using parser fallback"

// EngineImpl is the default completion engine implementation.
type EngineImpl struct {
	cfg Config

	catalogs     *catalogStore
	catalogsOnce sync.Once
}

// NewEngine creates a completion engine with deterministic catalog lifecycle storage.
func NewEngine(cfg Config) Engine {
	return &EngineImpl{
		cfg: cfg,
	}
}

func (e *EngineImpl) InitCatalog(snapshot CatalogSnapshot) (CatalogVersion, error) {
	return e.catalogStore().put(snapshot)
}

func (e *EngineImpl) UpdateCatalog(snapshot CatalogSnapshot) (CatalogVersion, error) {
	return e.catalogStore().put(snapshot)
}

func (e *EngineImpl) Complete(req Request) (Response, error) {
	normalized, catalog, diags := e.prepareRequest(req)
	if diags != nil {
		return Response{
			Candidates:  []Candidate{},
			Diagnostics: diags,
		}, nil
	}

	if e.cfg.Provider == nil {
		return e.completeWithParser(normalized, catalog), nil
	}

	if resp, ok := e.completeWithProvider(normalized); ok {
		return resp, nil
	}

	resp := e.completeWithParser(normalized, catalog)
	resp.Diagnostics = append([]Diagnostic{providerUnavailableDiagnostic()}, resp.Diagnostics...)
	resp.Source = CompletionSourceParserFallback
	return resp, nil
}

func (e *EngineImpl) PlanEdit(req Request, accepted Candidate) (EditPlan, []Diagnostic, error) {
	normalized, _, diags := e.prepareRequest(req)
	if diags != nil {
		return EditPlan{}, diags, nil
	}

	plan, planDiags := planSingleEdit(normalized, accepted)
	return plan, planDiags, nil
}

// prepareRequest normalizes, validates, and resolves the catalog for a request.
// Returns non-nil diagnostics on failure.
func (e *EngineImpl) prepareRequest(req Request) (Request, CatalogSnapshot, []Diagnostic) {
	normalized := normalizeRequest(req)
	if diags := validateRequest(normalized); len(diags) > 0 {
		return normalized, CatalogSnapshot{}, diags
	}
	catalog, diags, ok := e.resolveCatalog(normalized.CatalogVersion)
	if !ok {
		return normalized, CatalogSnapshot{}, diags
	}
	return normalized, catalog, nil
}

func (e *EngineImpl) catalogStore() *catalogStore {
	e.catalogsOnce.Do(func() {
		if e.catalogs == nil {
			e.catalogs = newCatalogStore()
		}
	})
	return e.catalogs
}

func (e *EngineImpl) resolveCatalog(version CatalogVersion) (CatalogSnapshot, []Diagnostic, bool) {
	if version == "" {
		return CatalogSnapshot{}, []Diagnostic{
			{
				Code:    CatalogVersionUnknown,
				Message: "catalog version is not initialized",
			},
		}, false
	}

	snapshot, ok := e.catalogStore().get(version)
	if !ok {
		return CatalogSnapshot{}, []Diagnostic{
			{
				Code:    CatalogVersionUnknown,
				Message: "catalog version is unknown",
			},
		}, false
	}

	return snapshot, nil, true
}

func (e *EngineImpl) completeWithProvider(req Request) (Response, bool) {
	result, err := e.cfg.Provider.Complete(req)
	if err != nil || len(result.Candidates) == 0 {
		return Response{}, false
	}

	return Response{
		Candidates: rankProviderCandidates(req, result.Candidates),
		Source:     CompletionSourceProvider,
	}, true
}

func (e *EngineImpl) completeWithParser(req Request, catalog CatalogSnapshot) Response {
	ctx, contextDiags := e.buildContext(req)
	candidates := generateCandidates(ctx, catalog, req)
	return Response{
		Candidates:  candidates,
		Diagnostics: contextDiags,
		Source:      CompletionSourceParser,
	}
}

func rankProviderCandidates(req Request, candidates []Candidate) []Candidate {
	out := newCandidateSet()
	for _, candidate := range candidates {
		candidate.Source = CandidateSourceProvider
		out.add(candidate)
	}

	out.applyRanking(rankingContext{
		cursorPrefix: cursorPrefixAt(req.SQL, req.CursorByte),
	})

	return out.finalize(req.MaxCandidates)
}

func providerUnavailableDiagnostic() Diagnostic {
	return Diagnostic{
		Code:    ProviderUnavailable,
		Message: providerUnavailableMessage,
	}
}
