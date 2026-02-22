package complete

import "github.com/iw2rmb/sql/parser"

// Engine defines completion lifecycle and request handlers.
type Engine interface {
	InitCatalog(snapshot CatalogSnapshot) (CatalogVersion, error)
	UpdateCatalog(snapshot CatalogSnapshot) (CatalogVersion, error)
	Complete(req Request) (Response, error)
	PlanEdit(req Request, accepted Candidate) (EditPlan, []Diagnostic, error)
}

// Config wires dependencies for engine implementations.
// This shape stays implementation-neutral and parser-only.
type Config struct {
	Parser parser.Parser
}
