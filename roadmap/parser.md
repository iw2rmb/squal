# Parser Platform Migration

Scope: Move PostgreSQL parser implementation from `mill` to this repository under `parserpg`, while preserving behavior parity with existing parser contracts in `core` and `parser`.

Documentation:
- `research/sql.md`
- `design/parser.md`
- `design/sql.md`
- `roadmap/sql/phase-1-parser-contracts.md`
- `../../mill/internal/mill/db/postgres/parser_pgquery_*.go`
- `../../mill/internal/mill/db/postgres/parser_cgo.go`

Legend: [ ] todo, [x] done.

## Implementation Steps
- [x] Complete parser contract extraction baseline — Establish shared contract ownership before parser implementation migration.
  - Repository: `sql`
  - Component: `core`, `parser`
  - Scope: Keep `core/sql_types.go`, `parser/interface.go`, `parser/types.go`, and `parser/interval_parser.go` as canonical shared contract sources.
  - Snippets: `go test ./core/... ./parser/...`
  - Tests: `go test ./core/... ./parser/...` — baseline contract packages are green before migration.

- [ ] Create `parserpg` package skeleton — Isolate PostgreSQL parser implementation and CGO boundary in this repository.
  - Repository: `sql`
  - Component: `parserpg`
  - Scope: Add package layout, build tags, CGO integration stubs, and module/package wiring needed for parser implementation files.
  - Snippets: `package parserpg`
  - Tests: `go test ./parserpg/...` — package builds and test bootstrap is valid.

- [ ] Port PostgreSQL parser implementation from `mill` — Make `parserpg` the canonical PG parser owner.
  - Repository: `sql`
  - Component: `parserpg`
  - Scope: Port parser logic from `../../mill/internal/mill/db/postgres/parser_pgquery_*.go`; adapt imports to `github.com/iw2rmb/sql/parser` and `github.com/iw2rmb/sql/core`.
  - Snippets: `func (p *PGQueryParser) ExtractMetadata(...)`
  - Tests: `CGO_ENABLED=1 go test ./parserpg/...` — core parser behavior compiles and passes parserpg tests.

- [ ] Rebind shared test parser registration to `parserpg` — Preserve current `NewTestParser()` behavior with migrated implementation.
  - Repository: `sql`
  - Component: `parser`, `parserpg`
  - Scope: Implement registration path equivalent to `parser_cgo.go` behavior so shared parser factory resolves PG implementation under correct build tags.
  - Snippets: `func init() { parser.RegisterTestParserFactory(...) }`
  - Tests: `CGO_ENABLED=1 go test ./parser/... ./parserpg/...` — factory resolution works and panic semantics remain explicit when unavailable.

- [ ] Port parserpg parity tests into `sql` — Lock migrated implementation behavior.
  - Repository: `sql`
  - Component: `parserpg` tests
  - Scope: Port relevant parser PG tests from `mill` with behavior-equivalent assertions and fixture coverage.
  - Snippets: `go test ./parserpg/... -run Test`
  - Tests: `CGO_ENABLED=1 go test ./parserpg/...` — PG parser output and diagnostics remain parity-compatible.

- [ ] Migrate `mill` to consume `parserpg` implementation — Remove parser implementation ownership from `mill`.
  - Repository: `mill`
  - Component: parser integration points
  - Scope: Switch `mill` parser construction/import wiring to `github.com/iw2rmb/sql/parserpg`; remove migrated parser implementation files from `mill` ownership.
  - Snippets: `import "github.com/iw2rmb/sql/parserpg"`
  - Tests: `go test ./internal/mill/db/postgres/...` — `mill` compiles and uses shared parser implementation path.

- [ ] Run focused parity validation in `mill` — Confirm no regressions in parser-dependent subsystems.
  - Repository: `mill`
  - Component: decomposition, routing, cache
  - Scope: Re-run parser-dependent test suites after migration.
  - Snippets: `CGO_ENABLED=1 go test ./internal/mill/decomposition -run 'Test(ParseSQL|ParsedQuery)'`
  - Tests: `go test ./internal/mill/routing -run 'Test(NewQueryRouter|RouteQuery)'` and `go test ./internal/mill/cache -run 'Test(NewManager|WithParser|Manager_)'` — behavior remains unchanged.

- [ ] Update documentation after migration completion — Keep docs, design, and roadmap aligned with actual ownership.
  - Repository: `sql`
  - Component: `docs`, `design`, `roadmap`, `research`
  - Scope: Document final parser ownership, migration status, and cross-links to `lst`/`complete` next phases.
  - Snippets: `rg -n "sqlparserpg|sqllst|sqlcomplete|sqlcore|sqlparser" research design roadmap docs`
  - Tests: `rg -n "TODO|TBD" design roadmap docs research` — unresolved placeholders are intentional and tracked.
