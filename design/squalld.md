# SquallD API Design

Related:
- `../research/sql.md`
- `../docs/parser-migration.md`
- `../complete/engine.go`
- `../complete/types.go`
- `../core/catalog.go`

## Goal

Define `squalld`: a small API server that owns datasource connectivity and schema lifecycle, while reusing existing `squall` parsing/completion libraries unchanged.

## Decision

Use a two-layer model:
- Library layer (`core`, `parser`, `parserpg`, `complete`): deterministic SQL semantics.
- Server layer (`squalld`): datasource connections, catalog snapshots, request routing, session-safe API.

This keeps current package boundaries intact and removes duplicated schema introspection logic from clients.

## Ownership Boundary

`squall` owns:
- SQL parsing and metadata extraction.
- SQL completion generation and deterministic ranking/edit planning.
- Catalog DTO contracts (`core.CatalogSchema` graph).

`squalld` owns:
- Datasource registration and connection pooling.
- Introspection and `CatalogSnapshot` construction.
- Catalog version lifecycle (`InitCatalog`/`UpdateCatalog` orchestration).
- API endpoints for parse/completion/edit-plan calls.

`aster` owns:
- SQL LST integration.
- Traverse/mutate/walk orchestration.
- Adapter/workspace/session orchestration.

## API Surface (v1)

Transport can be HTTP+JSON or JSON-RPC over HTTP. Methods below are transport-agnostic.

1. `datasource.register`
- Input: datasource DSN/settings.
- Output: `datasource_id`.
- Effect: validates connection and stores datasource config.

2. `catalog.refresh`
- Input: `datasource_id`.
- Output: `catalog_version`, summary (`schemas`, `tables`, `columns` counts).
- Effect: introspects database, maps to `complete.CatalogSnapshot`, calls `InitCatalog` (first time) or `UpdateCatalog`.

3. `sql.parse`
- Input: `sql`, optional dialect/profile.
- Output: parser metadata/diagnostics from `parser` contracts.

4. `sql.complete`
- Input: `sql`, `cursor_byte`, `catalog_version`, optional `max_candidates`.
- Output: `complete.Response`.

5. `sql.plan_edit`
- Input: completion request identity + accepted candidate.
- Output: deterministic `EditPlan` + diagnostics.

6. `catalog.get`
- Input: `catalog_version`.
- Output: canonical catalog snapshot for debug/inspection.

## Data Model

Catalog payload follows existing contracts:
- `CatalogSnapshot{Schemas, SearchPath}` from `complete/types.go`.
- `CatalogSchema`/`CatalogTable`/`CatalogColumn`/`CatalogForeignKey` from `core/catalog.go`.

No new schema format is introduced.

## Runtime Model

1. Startup
- Create parser dependency (`parserpg` in CGO builds).
- Create completion engine with parser dependency.
- Initialize datasource registry and catalog version index.

2. Refresh loop
- Explicit refresh via `catalog.refresh`.
- Optional periodic refresh per datasource.
- On refresh, replace active catalog version atomically.

3. Request handling
- `sql.complete` validates provided `catalog_version`.
- Unknown version returns `CatalogVersionUnknown` diagnostics (current behavior).
- Provider fallback remains in `complete` package behavior.

## Failure and Fallback

- Introspection failure: keep last known good `catalog_version` active.
- Parser failure during completion: return parser-degraded completion path and diagnostics.
- Unknown catalog version: deterministic empty candidate response with diagnostics.

## Security and Isolation

- DSNs/credentials are write-only at API boundary.
- Do not return secrets in any endpoint payload.
- Add per-datasource access control in API layer, not in library packages.

## Compatibility

- No backward compatibility requirement for pre-server wiring.
- Core library APIs remain source of truth.
- Server package must import `squall` packages; reverse dependency remains forbidden.

## Rollout Plan

1. Implement server skeleton with in-memory datasource registry.
2. Add PostgreSQL introspector to build `CatalogSnapshot`.
3. Wire `catalog.refresh` -> `InitCatalog`/`UpdateCatalog`.
4. Expose `sql.parse`, `sql.complete`, `sql.plan_edit`.
5. Add integration tests with real PG instance and deterministic completion assertions.
