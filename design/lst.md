# Design: SQL LST Platform

Related:
- Research baseline: `../research/sql.md`
- Shared platform baseline: `./sql.md`
- Implementation steps: `../roadmap/lst.md`

## Goal

Define the `lst` package architecture for SQL structural editing:
- stable node identity for deterministic query/traverse/mutate flows
- capture taxonomy for reusable semantic selection
- lossless emit for untouched byte ranges
- deterministic rewritten output for changed ranges

## Current State

- `core`, `parser`, and `parserpg` are implemented.
- PostgreSQL parser ownership and migration are complete.
- `lst` does not exist yet in this repository.
- `complete` depends on `lst` and must consume stable `lst` interfaces.

## Scope

In scope:
- introduce a new `lst` package/module in this repository
- define public node/document/capture interfaces
- define deterministic traversal and mutation interfaces
- define emit pipeline and contracts
- define diagnostics for invalid edits and degraded parse states
- define fixture and golden test strategy for determinism and lossless behavior

Out of scope:
- completion ranking and provider fallback logic (owned by `complete`)
- Aster adapter process/runtime integration details
- Cow runtime orchestration details

## Target Architecture

1. `core`
- owns neutral text contracts used by `lst` (`Span`, `TextEdit`, `TextChangeSet`)

2. `parser`
- owns parser contracts and diagnostics consumed by `lst` builder

3. `lst`
- owns SQL structural model, capture registry, traversal/mutation APIs, and emit behavior

Dependency direction:
- `core` <- `lst`
- `parser` <- `lst`
- `lst` <- `complete`
- forbidden: `lst` -> `parserpg`

## Public Interfaces And Types

`lst` public contract:
- `type NodeID string`
  - stable identifier within one `Document` lifecycle
  - deterministic for unchanged structure
- `type Document struct`
  - source SQL bytes
  - immutable node graph root
  - capture index
  - parser diagnostics snapshot
- `type Node struct`
  - `ID NodeID`
  - `Kind NodeKind`
  - `Span core.Span`
  - parent/child references
  - optional typed properties map for SQL-kind-specific fields
- `type Capture struct`
  - `Kind CaptureKind`
  - `NodeID`
  - `Span core.Span`
  - optional labels (alias, table, schema, function, operator)
- `type CaptureKind string`
  - fixed registry of capture kinds (identifier, table_ref, column_ref, predicate, join_edge, projection_item, ordering_item, literal, function_call)
- `type Emitter interface`
  - `Emit(doc Document) ([]byte, error)`
  - `EmitWithChanges(doc Document, changes core.TextChangeSet) ([]byte, []Diagnostic, error)`

Traversal/mutation/query surface:
- deterministic depth-first traversal API
- selector API by `NodeID`, `NodeKind`, `CaptureKind`, and span overlap
- mutation planning API that validates edit spans against node boundaries before execution

## Data Flow

1. Input:
- raw SQL bytes
- parser output (`parser` contracts + diagnostics)

2. Build:
- normalize parser output into `Document` and `Node` graph
- assign deterministic `NodeID` values
- materialize capture index by `CaptureKind`

3. Operations:
- query/traverse against immutable `Document`
- produce validated `TextChangeSet` through mutation planner

4. Emit:
- unchanged spans are copied byte-for-byte from original input
- changed spans are rewritten from node/changeset synthesis
- final output is deterministic for identical `(input, changeset)`

## Determinism Rules

- Identical input SQL and parser output produce identical `NodeID` and capture ordering.
- Traversal order is canonical pre-order DFS.
- Capture index order is stable by `(start_byte, end_byte, kind, node_id)`.
- No-op changeset must emit byte-identical SQL.
- Equivalent changesets with identical sorted edit spans must emit byte-identical output.

## Error Model

`lst` exposes structured diagnostics:
- `DiagnosticCodeInvalidSpan`
- `DiagnosticCodeNodeNotFound`
- `DiagnosticCodeOverlappingEdits`
- `DiagnosticCodeDegradedParseState`
- `DiagnosticCodeUnsupportedMutation`

Rules:
- invalid mutation requests return diagnostics and do not partially mutate output
- degraded parse state remains queryable when safe, but mutate/emit operations requiring missing structure return deterministic diagnostics

## Testing Strategy

Unit tests:
- node graph invariants (parent/child consistency, span containment)
- `NodeID` stability for no-op rebuilds
- capture index ordering and lookup determinism
- mutation planner validation rules

Golden tests:
- parse -> build -> emit no-op roundtrip fixtures (byte-identical)
- targeted mutation fixtures where only expected spans differ
- deterministic output snapshots for repeated runs

Fuzz/property tests:
- Unicode-heavy SQL to validate rune/byte offset correctness
- random edit ordering to verify overlap rejection and canonicalization

Integration-facing tests:
- fixture compatibility contracts consumed by future `complete` package

## Exit Criteria

- `lst` module/package exists with documented public interfaces.
- deterministic traversal/query/mutation behavior is covered by automated tests.
- lossless untouched-span emit rule is validated with golden fixtures.
- diagnostics are stable and machine-consumable.
- dependency boundaries hold (`lst` imports only allowed packages).

## Next Link

After `lst` completion, implement `complete` using `lst` traversal/capture and deterministic edit contracts.
