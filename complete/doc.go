// Package complete owns SQL completion contracts and orchestration.
//
// This package defines the completion-facing API surface and diagnostics.
// It provides deterministic catalog lifecycle storage and versioning.
// It includes parser-context extraction and catalog-aware candidate generation.
// Later roadmap steps add deterministic ranking and edit planning behavior.
//
// Dependency boundary:
//   - allowed SQL package imports: core, parser
//   - forbidden SQL package imports: PostgreSQL parser implementation package
package complete
