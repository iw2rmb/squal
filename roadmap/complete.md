# SQL Completion Engine Implementation

Scope: Implement `complete` package in `sql` repository with deterministic, catalog-aware SQL completion and deterministic edit planning, without introducing SQL-owned LST traversal/mutation orchestration.

Documentation:
- `research/sql.md`
- `design/sql.md`
- `design/parser.md`
- `design/completions.md`
- `roadmap/parser.md`
- `docs/parser-migration.md`
- `../aster/research/sql-lst.md`
- `parser/interface.go`
- `parser/types.go`

Legend: [ ] todo, [x] done.

## Implementation Steps
- [x] Add shared core DTOs required by completion contracts — Define stable cross-package primitives before implementing completion engine APIs.
  - Repository: `sql`
  - Component: `core`
  - Scope: Add `core` types for byte-addressed edits and catalog graph contracts used by `complete` (`Span`, `TextEdit`, `TextChangeSet`, schema/table/column/FK DTOs). Keep `JoinType`/`CompareOp` ownership in `core/sql_types.go`.
  - Snippets: `type Span struct { StartByte int; EndByte int }`; `type TextEdit struct { Span Span; NewText string }`
  - Tests: `go test ./core/...` — core DTOs serialize/validate deterministically and compile for downstream imports.

- [x] Create `complete` package skeleton and public file layout — Establish package boundary and explicit ownership for completion-only logic.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Create initial files (`complete/doc.go`, `complete/types.go`, `complete/engine.go`, `complete/diagnostics.go`) and wire imports only to `core` and `parser` (forbid `parserpg` import in `complete`).
  - Snippets: `package complete`; `type Engine interface { ... }`
  - Tests: `go test ./complete/...` — package builds with baseline type/interface tests.

- [ ] Implement completion API contracts from design — Freeze external contract before algorithm implementation to prevent drift.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Implement public DTOs and interface from `design/completions.md`: `CatalogSnapshot`, `CatalogVersion`, `Request`, `Candidate`, `EditPlan`, `Response`, `Engine` (`InitCatalog`, `UpdateCatalog`, `Complete`, `PlanEdit`).
  - Snippets: `func (e *EngineImpl) InitCatalog(snapshot CatalogSnapshot) (CatalogVersion, error)`
  - Tests: `go test ./complete/... -run 'Test(Types|EngineContract)'` — contract shape is compile-stable and behaviorally testable.

- [ ] Implement deterministic catalog lifecycle store — Make catalog initialization/update efficient and reusable across many completion requests.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Add catalog store keyed by stable version hash; canonicalize catalog snapshot ordering; support replace/update semantics with deterministic versioning and lookup by `CatalogVersion`.
  - Snippets: `type catalogStore struct { versions map[CatalogVersion]CatalogSnapshot }`
  - Tests: `go test ./complete/... -run 'Test(CatalogInit|CatalogUpdate|CatalogVersionDeterminism)'` — equivalent snapshots yield equivalent versions; unknown versions return deterministic diagnostics.

- [ ] Implement request normalization and cursor validation — Enforce byte-safe request preconditions before parse/context work.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Add validation for SQL snapshot presence, cursor byte bounds, catalog version resolution, and deterministic request canonicalization.
  - Snippets: `func validateRequest(req Request) []Diagnostic`
  - Tests: `go test ./complete/... -run 'Test(ValidateRequest|InvalidCursorSpan|CatalogVersionUnknown)'` — invalid requests fail with stable diagnostic codes and zero edits.

- [ ] Implement parser-context extraction for completion — Build deterministic completion context from parser outputs and cursor position.
  - Repository: `sql`
  - Component: `complete`, `parser` integration
  - Scope: Use `parser.Parser.ExtractMetadata` from `parser/interface.go` and `parser.QueryMetadata` from `parser/types.go` to derive active scope context (tables, aliases, projection targets, predicates, join conditions).
  - Snippets: `meta, err := p.ExtractMetadata(req.SQL)`; `func buildContext(meta *parser.QueryMetadata, cursor int) Context`
  - Tests: `go test ./complete/... -run 'Test(BuildContext|ParseDegraded|AmbiguousContext)'` — context extraction is deterministic across equivalent inputs.

- [ ] Implement catalog-aware candidate generation — Generate schema/table/column/join/snippet candidates from bound parser+catalog context.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Implement generators for object-name candidates, alias-aware column candidates, FK-based join-path suggestions, and always-on snippets/templates.
  - Snippets: `func generateCandidates(ctx Context, catalog CatalogSnapshot) []Candidate`
  - Tests: `go test ./complete/... -run 'Test(CatalogCandidates|JoinSuggestions|SnippetCandidates)'` — expected candidate families are present and context-scoped.

- [ ] Implement deterministic ranking and tie-break sorting — Guarantee stable candidate ordering for identical request identity.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Add weighted scoring model and explicit tie-break keys (kind priority, exact-prefix match, lexical fallback) with deterministic sort implementation.
  - Snippets: `sort.Slice(cands, func(i, j int) bool { return less(cands[i], cands[j]) })`
  - Tests: `go test ./complete/... -run 'Test(RankingDeterminism|StableTiebreaks)'` — repeated runs return byte-identical candidate ordering.

- [ ] Implement deterministic edit planning for accepted candidates — Produce safe replacement edits with stable spans and conflict checks.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Implement `PlanEdit` to compute replacement range around cursor/token, emit `core.TextEdit` plans, and reject overlapping/invalid spans with deterministic diagnostics.
  - Snippets: `func (e *EngineImpl) PlanEdit(req Request, accepted Candidate) (EditPlan, []Diagnostic, error)`
  - Tests: `go test ./complete/... -run 'Test(PlanEdit|EditConflict|UnicodeSpanSafety)'` — edits are deterministic, non-overlapping, and byte-correct for Unicode inputs.

- [ ] Implement provider-assisted completion fallback semantics — Keep completion available when provider path fails.
  - Repository: `sql`
  - Component: `complete`
  - Scope: Add optional provider abstraction in engine internals; when provider fails/unavailable, return parser-backed candidate path with explicit source metadata and stable `ProviderUnavailable` diagnostic.
  - Snippets: `resp.Source = "parser_fallback"`
  - Tests: `go test ./complete/... -run 'Test(ProviderUnavailableFallback|ProviderSuccessPath)'` — fallback is deterministic and preserves non-empty completion path when possible.

- [ ] Add fixture, golden, and fuzz coverage for determinism and Unicode correctness — Lock behavior across edge cases and regression surfaces.
  - Repository: `sql`
  - Component: `complete` tests
  - Scope: Add `complete/testdata` fixtures for candidate ordering and planned edits; add fuzz/property coverage for cursor bounds and mixed rune-width SQL tokens.
  - Snippets: `go test ./complete/... -run TestGolden`; `go test ./complete/... -run Fuzz -fuzz=Fuzz -fuzztime=10s`
  - Tests: `go test ./complete/...` — golden outputs stable; fuzz does not reveal panics/span corruption.

- [ ] Run acceptance gate and synchronize documentation state — Mark roadmap completion only when contracts, behavior, and docs are aligned.
  - Repository: `sql`
  - Component: `core`, `complete`, docs
  - Scope: Run full targeted suite, enforce forbidden import boundary, then update docs/roadmap checkboxes (`research/sql.md`, `design/completions.md`, `roadmap/complete.md`) to reflect implemented state.
  - Snippets: `rg -n "parserpg" complete`; `go test ./core/... ./parser/... ./complete/...`
  - Tests: `go test ./core/... ./parser/... ./complete/...` and `rg -n "parserpg" complete` — tests green and dependency direction preserved.
