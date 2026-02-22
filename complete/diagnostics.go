package complete

// DiagnosticCode is a stable machine-readable completion diagnostic code.
type DiagnosticCode string

const (
	InvalidCursorSpan     DiagnosticCode = "InvalidCursorSpan"
	ParseDegraded         DiagnosticCode = "ParseDegraded"
	AmbiguousContext      DiagnosticCode = "AmbiguousContext"
	EditConflict          DiagnosticCode = "EditConflict"
	ProviderUnavailable   DiagnosticCode = "ProviderUnavailable"
	CatalogMissing        DiagnosticCode = "CatalogMissing"
	CatalogIncompatible   DiagnosticCode = "CatalogIncompatible"
	CatalogVersionUnknown DiagnosticCode = "CatalogVersionUnknown"
)

// Diagnostic is a stable completion diagnostic record.
type Diagnostic struct {
	Code    DiagnosticCode `json:"code"`
	Message string         `json:"message,omitempty"`
}
