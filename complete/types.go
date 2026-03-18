package complete

import "github.com/iw2rmb/squall/core"

// CatalogSnapshot is the completion catalog input.
type CatalogSnapshot struct {
	Schemas    []core.CatalogSchema `json:"schemas"`
	SearchPath []string             `json:"search_path,omitempty"`
}

// CatalogVersion identifies a catalog lifecycle version.
type CatalogVersion string

// Request is the completion request envelope.
type Request struct {
	SQL            string         `json:"sql"`
	CursorByte     int            `json:"cursor_byte"`
	CatalogVersion CatalogVersion `json:"catalog_version"`
	MaxCandidates  int            `json:"max_candidates,omitempty"`
}

// CandidateKind classifies completion candidate families.
type CandidateKind string

const (
	CandidateKindSchema  CandidateKind = "schema"
	CandidateKindTable   CandidateKind = "table"
	CandidateKindColumn  CandidateKind = "column"
	CandidateKindJoin    CandidateKind = "join"
	CandidateKindSnippet CandidateKind = "snippet"
	CandidateKindKeyword CandidateKind = "keyword"
)

// CandidateSource identifies how a candidate was produced.
type CandidateSource string

const (
	CandidateSourceParser   CandidateSource = "parser"
	CandidateSourceCatalog  CandidateSource = "catalog"
	CandidateSourceProvider CandidateSource = "provider"
	CandidateSourceSnippet  CandidateSource = "snippet"
)

// ScoreComponents provides deterministic scoring decomposition used for ranking.
type ScoreComponents struct {
	Context  float64 `json:"context,omitempty"`
	Catalog  float64 `json:"catalog,omitempty"`
	Prefix   float64 `json:"prefix,omitempty"`
	Snippet  float64 `json:"snippet,omitempty"`
	Provider float64 `json:"provider,omitempty"`
}

// CandidateSortKey carries deterministic tie-break inputs.
type CandidateSortKey struct {
	KindPriority  int    `json:"kind_priority,omitempty"`
	ExactPrefix   bool   `json:"exact_prefix,omitempty"`
	LabelLexical  string `json:"label_lexical,omitempty"`
	InsertLexical string `json:"insert_lexical,omitempty"`
}

// Candidate is one completion candidate.
type Candidate struct {
	ID              string           `json:"id,omitempty"`
	Label           string           `json:"label"`
	InsertText      string           `json:"insert_text"`
	Kind            CandidateKind    `json:"kind"`
	Score           float64          `json:"score,omitempty"`
	ScoreComponents ScoreComponents  `json:"score_components,omitempty"`
	SortKey         CandidateSortKey `json:"sort_key,omitempty"`
	Source          CandidateSource  `json:"source,omitempty"`
}

// EditPlan is a deterministic edit result for an accepted candidate.
// Validation flags are explicit for deterministic conflict/overlap reporting.
type EditPlanValidation struct {
	InBounds       bool `json:"in_bounds"`
	NonOverlapping bool `json:"non_overlapping"`
}

// EditPlan is a deterministic edit result for an accepted candidate.
type EditPlan struct {
	Edits           []core.TextEdit    `json:"edits"`
	ReplacementSpan core.Span          `json:"replacement_span"`
	Validation      EditPlanValidation `json:"validation"`
}

// CompletionSource identifies the response path used to produce candidates.
type CompletionSource string

const (
	CompletionSourceParser         CompletionSource = "parser"
	CompletionSourceProvider       CompletionSource = "provider"
	CompletionSourceParserFallback CompletionSource = "parser_fallback"
)

// Response is the completion response envelope.
type Response struct {
	Candidates       []Candidate      `json:"candidates"`
	SelectedEditPlan *EditPlan        `json:"selected_edit_plan,omitempty"`
	Diagnostics      []Diagnostic     `json:"diagnostics,omitempty"`
	Source           CompletionSource `json:"source,omitempty"`
}
