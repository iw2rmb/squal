package complete

import "sync"

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
	normalized := normalizeRequest(req)
	if diags := validateRequest(normalized); len(diags) > 0 {
		return Response{
			Candidates:  []Candidate{},
			Diagnostics: diags,
		}, nil
	}

	catalog, diags, ok := e.resolveCatalog(normalized.CatalogVersion)
	if !ok {
		return Response{
			Candidates:  []Candidate{},
			Diagnostics: diags,
		}, nil
	}

	ctx, contextDiags := e.buildContext(normalized)
	candidates := generateCandidates(ctx, catalog, normalized)
	return Response{
		Candidates:  candidates,
		Diagnostics: contextDiags,
		Source:      CompletionSourceParser,
	}, nil
}

func (e *EngineImpl) PlanEdit(req Request, accepted Candidate) (EditPlan, []Diagnostic, error) {
	normalized := normalizeRequest(req)
	if diags := validateRequest(normalized); len(diags) > 0 {
		return EditPlan{}, diags, nil
	}

	_, diags, ok := e.resolveCatalog(normalized.CatalogVersion)
	if !ok {
		return EditPlan{}, diags, nil
	}

	return EditPlan{}, nil, nil
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
