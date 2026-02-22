// Package complete owns SQL completion contracts and orchestration.
//
// This package defines the completion-facing API surface and diagnostics.
// It provides deterministic catalog lifecycle storage and versioning.
// It includes parser-context extraction and catalog-aware candidate generation.
// It includes deterministic ranking with explicit tie-break ordering.
// It includes deterministic edit planning behavior for accepted candidates.
//
// Dependency boundary:
//   - allowed SQL package imports: core, parser
//   - forbidden SQL package imports: PostgreSQL parser implementation package
package complete
