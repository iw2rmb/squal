// Package complete owns SQL completion contracts and orchestration.
//
// This package defines the completion-facing API surface and diagnostics.
// Later roadmap steps add catalog lifecycle behavior, parser-context binding,
// ranking, and deterministic edit planning.
//
// Dependency boundary:
//   - allowed SQL package imports: core, parser
//   - forbidden SQL package imports: PostgreSQL parser implementation package
package complete
