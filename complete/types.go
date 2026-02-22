package complete

import "github.com/iw2rmb/sql/core"

// CatalogSnapshot is the completion catalog input.
// Field set is intentionally minimal in skeleton phase and will be expanded.
type CatalogSnapshot struct {
	Schemas    []core.CatalogSchema `json:"schemas"`
	SearchPath []string             `json:"search_path,omitempty"`
}

// CatalogVersion identifies a catalog lifecycle version.
type CatalogVersion string

// Request is the completion request envelope.
// Field set is intentionally minimal in skeleton phase and will be expanded.
type Request struct {
	SQL            string         `json:"sql"`
	CursorByte     int            `json:"cursor_byte"`
	CatalogVersion CatalogVersion `json:"catalog_version"`
}

// Candidate is one completion candidate.
// Field set is intentionally minimal in skeleton phase and will be expanded.
type Candidate struct {
	Label      string `json:"label"`
	InsertText string `json:"insert_text"`
	Kind       string `json:"kind"`
}

// EditPlan is a deterministic edit result for an accepted candidate.
// Field set is intentionally minimal in skeleton phase and will be expanded.
type EditPlan struct {
	Edits []core.TextEdit `json:"edits"`
}

// Response is the completion response envelope.
// Field set is intentionally minimal in skeleton phase and will be expanded.
type Response struct {
	Candidates  []Candidate  `json:"candidates"`
	Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
	Source      string       `json:"source,omitempty"`
}
