# Design: SQL Completion Engine

Related:
- Research baseline: `../research/sql.md`
- Shared platform baseline: `./sql.md`
- Parser platform design: `./parser.md`
- Parser migration status: `../docs/parser-migration.md`
- Aster SQL LST integration ownership: `../../aster/research/sql-lst.md`

## Goal

Define completion architecture in this repository without introducing SQL-owned LST/traverse APIs:
- deterministic candidate generation from parser-backed context
- deterministic text edit planning for accepted candidates
- provider fallback behavior that keeps completion available

## Current State

- `core`, `parser`, and `parserpg` are implemented and tested.
- parser migration from `mill` is complete.
- `complete` package has contract surface (`doc.go`, `types.go`, `engine.go`, `diagnostics.go`), deterministic catalog lifecycle storage with canonicalized version hashing/lookup, deterministic request normalization/cursor validation, parser-context extraction, catalog-aware candidate generation, deterministic ranking/tie-break sorting, and deterministic edit planning in the default engine implementation.
- SQL LST ownership is moved to `aster`, not this repository.

## Scope

In scope:
- introduce `complete` package in this repository
- define completion request/response contracts
- define ranking and deterministic edit-planning flow
- define provider fallback policy and diagnostics
- define tests for determinism and Unicode-safe edits

Out of scope:
- SQL LST node model and traversal APIs
- generic traverse/mutate/walk orchestration (owned by `aster`)
- editor/runtime session orchestration details

## Target Architecture

1. `core`
- owns neutral text/edit DTOs and shared completion DTO contracts

2. `parser`
- provides parser-neutral analysis DTOs used by completion context extraction

3. `complete`
- owns completion classification/ranking
- owns deterministic edit planning
- owns fallback policy when provider-assisted completion is unavailable

Dependency direction:
- `core` <- `complete`
- `parser` <- `complete`
- forbidden: `complete` -> `parserpg`

## Public Interfaces

`complete` public surface:
- `type CatalogSnapshot struct`
  - schemas/search path
  - tables/views
  - columns and types
  - primary/foreign keys (including composite keys)
- `type CatalogVersion string`
- `type Request struct`
  - SQL text snapshot
  - cursor byte offset
  - catalog version reference
- `type Candidate struct`
  - label/insert text
  - kind
  - score components
  - deterministic sort keys
- `type EditPlan struct`
  - one or more `core.TextEdit`
  - replacement span contracts
  - conflict/overlap validation result
- `type Response struct`
  - ordered candidates
  - optional selected candidate edit plan
  - diagnostics and fallback source metadata
- `type Engine interface`
  - `InitCatalog(snapshot CatalogSnapshot) (CatalogVersion, error)`
  - `UpdateCatalog(snapshot CatalogSnapshot) (CatalogVersion, error)`
  - `Complete(req Request) (Response, error)`
  - `PlanEdit(req Request, accepted Candidate) (EditPlan, []Diagnostic, error)`

## Host Input Contract

Host must provide:
- package import/integration for `parser`/`parserpg` and `complete`
- SQL text snapshot for the current document state
- cursor position as byte offset in that snapshot
- catalog lifecycle:
  - initialize engine with a `CatalogSnapshot`
  - update catalog when schema changes
  - pass `CatalogVersion` in each completion request

`complete` must use catalog/schema input for:
- object-name candidates (schema/table/column)
- type-aware ranking and filtering
- join-path suggestions based on FK graph
- snippets are always enabled

## Completion Pipeline

1. Normalize request:
- validate cursor span and SQL bytes
- canonicalize request options

2. Parse/context extraction:
- use `parser` outputs and diagnostics
- derive scope context (projection, table refs, aliases, predicates, join candidates)

3. Catalog binding:
- resolve `CatalogVersion` to the initialized catalog snapshot
- bind visible aliases/tables to catalog objects
- resolve type and FK context for ranking and join suggestions

4. Candidate generation:
- catalog-aware candidates
- parser-context candidates
- snippets/templates

5. Ranking:
- deterministic weighted scoring
- canonical tie-break ordering

6. Edit planning:
- produce deterministic replacement span and inserted text
- reject overlapping/invalid edits with stable diagnostics

7. Fallback path:
- if provider-assisted path fails, return parser-backed completion with explicit source marker

## Determinism Rules

- identical `(sql, cursor, context)` yields identical ordered candidates and scores.
- identical `CatalogVersion` is part of that identity.
- identical accepted candidate yields identical edit plan.
- tie-breakers are explicit and stable.
- diagnostics are machine-consumable and stable by code.

## Error Model

Diagnostics include:
- `InvalidCursorSpan`
- `ParseDegraded`
- `AmbiguousContext`
- `EditConflict`
- `ProviderUnavailable`
- `CatalogMissing`
- `CatalogIncompatible`
- `CatalogVersionUnknown`

Rules:
- malformed request returns deterministic diagnostics and no edits.
- missing/incompatible catalog returns deterministic diagnostics and no edits.
- unknown catalog version returns deterministic diagnostics and no edits.
- parse degradation may still return fallback candidates when safe.
- edit planning must never emit overlapping edits.

## Aster Integration Boundary

`sql` package responsibilities:
- parse-backed completion candidates
- deterministic edit plans for accepted candidates

`aster` responsibilities:
- SQL document graph/LST ownership
- traversal/mutation orchestration
- lifecycle/session handling for editor workflows
- applying returned edits to workspace state

## Validation

Required checks:
- `go test ./complete/...`
- determinism fixtures for candidate ordering
- catalog-aware ranking fixtures (schema-qualified names, FK-driven joins)
- edit planner fixtures for span correctness and Unicode safety
- fallback behavior tests for provider-unavailable cases

## Exit Criteria

- `complete` package exists with stable request/response/edit-plan contracts.
- deterministic candidate and edit-plan behavior is covered by tests.
- fallback completion path works without provider dependency.
- integration contract with `aster` is explicit and documented.

## Next Link

After `complete` implementation, integrate `cmd/aster-adapter-sql` with:
- completion request forwarding
- deterministic edit-plan application
- Aster-owned SQL LST/traversal workflows.
