package complete

import "github.com/iw2rmb/squal/parser"

// Engine defines completion lifecycle and request handlers.
type Engine interface {
	InitCatalog(snapshot CatalogSnapshot) (CatalogVersion, error)
	UpdateCatalog(snapshot CatalogSnapshot) (CatalogVersion, error)
	Complete(req Request) (Response, error)
	PlanEdit(req Request, accepted Candidate) (EditPlan, []Diagnostic, error)
}

// CompletionProvider is an optional provider-assisted completion dependency.
type CompletionProvider interface {
	Complete(req Request) (ProviderResult, error)
}

// ProviderResult is the provider-assisted completion response.
type ProviderResult struct {
	Candidates []Candidate
}

// Config wires dependencies for engine implementations.
type Config struct {
	Parser   parser.Parser
	Provider CompletionProvider
}
