# Phase 1: Parser Contracts Extraction

Scope: Extract parser-neutral SQL contracts from `mill/internal/mill/parser` into shared modules in `sql`, then migrate `mill` to consume those modules with behavior parity.

Documentation:
- `research/sql.md`
- `design/sql.md`
- `../../mill/internal/mill/parser/interface.go`
- `../../mill/internal/mill/parser/types.go`
- `../../mill/internal/mill/parser/interval_parser.go`
- `../../mill/internal/mill/types/sql.go`
- `../../mill/internal/mill/db/postgres/parser_pgquery_*.go`
- `../../mill/internal/mill/db/postgres/parser_cgo.go`

Legend: [ ] todo, [x] done.

## Phase 1 Execution Steps

- [x] Create roadmap scaffold for SQL phase work — Establish canonical location for phase decomposition docs.
  - Repository: `sql`
  - Component: `roadmap/`
  - Scope: Create `roadmap/sql/` and store this phase file as `roadmap/sql/phase-1-parser-contracts.md`.
  - Snippets: `mkdir -p roadmap/sql`
  - Tests: `test -f roadmap/sql/phase-1-parser-contracts.md` — file exists at expected path.

- [x] Add shared enum ownership in `core` — Break parser DTO dependency on `mill/internal/mill/types`.
  - Repository: `sql`
  - Component: `core`
  - Scope: Add `core/sql_types.go` with `JoinType` and `CompareOp` enums (plus constants) matching current semantics from `../../mill/internal/mill/types/sql.go`.
  - Snippets: `package core`; `type JoinType string`; `type CompareOp string`
  - Tests: `go test ./core/...` — module/package compiles and exports required enum symbols.

- [x] Extract parser interface into `parser` module — Establish shared contract surface.
  - Repository: `sql`
  - Component: `parser`
  - Scope: Add `parser/interface.go` with `Parser` interface and test factory API (`RegisterTestParserFactory`, `NewTestParser`) matching current API behavior.
  - Snippets: `type Parser interface { ExtractMetadata(...); NormalizeQuery(...); ... }`
  - Tests: `go test ./parser/... -run Test.*Parser` — interface-level tests compile and test factory panic behavior stays explicit.

- [x] Extract parser DTOs into `parser` module — Move metadata/analysis types to shared ownership.
  - Repository: `sql`
  - Component: `parser`
  - Scope: Add `parser/types.go` by porting DTOs from `../../mill/internal/mill/parser/types.go`; replace enum imports with `github.com/iw2rmb/sql/core`; preserve JSON tags and field names.
  - Snippets: `import "github.com/iw2rmb/sql/core"`; `type JoinCondition struct { Type core.JoinType ... }`
  - Tests: `go test ./parser/... -run Test.*Types` — DTO compile checks and serialization expectations pass.

- [ ] Extract interval parsing helpers into `parser` module — Preserve sliding-window duration semantics.
  - Repository: `sql`
  - Component: `parser`
  - Scope: Add `parser/interval_parser.go` by porting from `../../mill/internal/mill/parser/interval_parser.go` without behavior changes.
  - Snippets: `func ParseInterval(expr string) (time.Duration, error)`
  - Tests: `go test ./parser/... -run 'TestParseInterval|FuzzParseInterval'` — interval forms (quoted, cast, make_interval, ISO-8601, negative) remain supported.

- [ ] Port parser contract tests into SQL repo — Lock parity before consumer migration.
  - Repository: `sql`
  - Component: `parser` tests
  - Scope: Port tests from:
    - `../../mill/internal/mill/parser/interval_parser_test.go`
    - `../../mill/internal/mill/parser/fuzz_interval_test.go`
    - `../../mill/internal/mill/parser/types_test.go`
    Keep assertions behavior-equivalent; adjust imports only.
  - Snippets: `go test ./parser/...`
  - Tests: `go test ./parser/...` — all ported tests pass in `sql` repository.

- [ ] Enforce dependency boundary in shared contracts — Prevent accidental coupling back to `mill/internal`.
  - Repository: `sql`
  - Component: `core`, `parser`
  - Scope: Add CI/lint check ensuring no import path contains `mill/internal` under shared modules.
  - Snippets: `rg -n "mill/internal" core parser`
  - Tests: `rg -n "mill/internal" core parser` — returns no matches.

- [ ] Migrate `mill` parser imports to shared module — Switch consumers from internal contracts to shared contracts.
  - Repository: `mill`
  - Component: all parser consumers
  - Scope: Replace `github.com/iw2rmb/mill/internal/mill/parser` imports with `github.com/iw2rmb/sql/parser` across parser users (current baseline: 141 importing files). Keep behavior unchanged.
  - Snippets: `import sqlparser "github.com/iw2rmb/sql/parser"`
  - Tests: `go test ./...` (or scoped packages below) — build and parser-dependent tests remain green after import swap.

- [ ] Migrate parser-related enums in `mill` to shared `core` — Remove remaining parser DTO dependency on internal types.
  - Repository: `mill`
  - Component: parser DTO call sites + postgres parser implementation
  - Scope: Replace parser-surface usage of `github.com/iw2rmb/mill/internal/mill/types` with `github.com/iw2rmb/sql/core` where it participates in parser DTOs/contracts.
  - Snippets: `import sqlcore "github.com/iw2rmb/sql/core"`
  - Tests: `go test ./internal/mill/...` — compile-time type consistency is preserved for parser metadata flows.

- [ ] Rebind PostgreSQL parser implementation in `mill` to shared interface — Keep Phase 1 implementation location while adopting shared contract.
  - Repository: `mill`
  - Component: `internal/mill/db/postgres`
  - Scope: Keep `parser_pgquery_*.go` in `mill`; update types/imports to implement `github.com/iw2rmb/sql/parser.Parser`; keep CGO build-tag registration in `parser_cgo.go` but wired to shared `RegisterTestParserFactory`.
  - Snippets: `func init() { parser.RegisterTestParserFactory(...) }` where `parser` import points to shared module
  - Tests: `go test ./internal/mill/db/postgres/...` — parser implementation compiles and tests pass with shared contract types.

- [ ] Validate mill parser integration paths with focused parity tests — Prove no behavioral regressions in key consumers.
  - Repository: `mill`
  - Component: decomposition, routing, cache
  - Scope: Run focused tests that consume parser metadata/fingerprint contracts:
    - `internal/mill/decomposition/parser_integration_test.go`
    - `internal/mill/routing/router_test.go`
    - `internal/mill/cache/manager_test.go`
  - Snippets: `go test ./internal/mill/decomposition -run ParserIntegration`
  - Tests:
    - `go test ./internal/mill/decomposition -run ParserIntegration` — parser integration behavior unchanged.
    - `go test ./internal/mill/routing -run Router` — routing metadata logic unchanged.
    - `go test ./internal/mill/cache -run Manager` — fingerprint/metadata cache paths unchanged.

- [ ] Phase 1 acceptance gate — Mark phase complete only when extraction and migration both satisfy contract parity.
  - Repository: `sql`, `mill`
  - Component: release readiness
  - Scope: Phase is complete only if all conditions hold:
    - shared modules compile and tests pass in `sql`
    - `mill` parser consumer tests pass after migration
    - no shared-module imports from `mill/internal`
  - Snippets: `go test ./parser/...`; `rg -n "mill/internal" core parser`; focused `mill` test commands above
  - Tests: All listed commands pass in CI and local runs.
