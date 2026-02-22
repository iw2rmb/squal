# Design: SQL Platform Phase 1 (Parser Contracts Extraction)

Related:
- Research baseline: `../research/sql.md`
- Source of truth to extract from:
  - `../../mill/internal/mill/parser/interface.go`
  - `../../mill/internal/mill/parser/types.go`
  - `../../mill/internal/mill/parser/interval_parser.go`
  - `../../mill/internal/mill/types/sql.go`

## Phase 1 Goal

Create a standalone parser-contract module in this repo that can be imported by `mill`, `cow`, and future SQL services.

This document is phase-1 specific. Phase 2 parser migration is complete and tracked in `./parser.md` and `../roadmap/parser.md`.

## Current State (researched in `../mill`)

- Contract surface lives in `mill` internal package:
  - `internal/mill/parser` (8 files)
- DTOs in parser depend on `internal/mill/types` for `JoinType` and `CompareOp`.
- `mill` usage is broad:
  - 141 files import `internal/mill/parser`
  - 441 call sites use `NewTestParser()`
- PostgreSQL implementation is separate and CGO-backed:
  - `internal/mill/db/postgres/parser_pgquery_*.go` + `parser_cgo.go` (22 files)

## Scope

In scope:
- Extract parser interface and DTOs into this repo as public module(s).
- Remove dependency on `mill/internal/*` types.
- Preserve semantics of interval parsing and parser test-factory API.
- Provide migration path so `mill` can switch imports without behavioral drift.

Out of scope:
- Moving PG parser implementation (`pg_query`) from `mill`.
- LST/completion engine work.
- Aster/cow runtime integration.

## Target Architecture (Phase 1)

### 1. `parser` module

Owns parser-neutral contracts and helpers.

Proposed package content:
- `parser/interface.go`
  - `type Parser interface { ... }`
  - `RegisterTestParserFactory`, `NewTestParser`
- `parser/types.go`
  - `QueryMetadata`, `Aggregate`, `DistinctSpec`, `GroupItem`, `TemporalOps`, `SlidingWindowInfo`, and related DTOs
- `parser/interval_parser.go`
  - `ParseInterval(...)` and helper functions

### 2. `core` minimal shared enums (required to break `mill` coupling)

Owns neutral enum types currently taken from `mill/internal/mill/types/sql.go`:
- `JoinType`
- `CompareOp`

`parser/types.go` imports these enums from `core` instead of `mill/internal/mill/types`.

## API And Compatibility Rules

- Contract behavior must match `mill/internal/mill/parser` behavior for Phase 1.
- DTO JSON field names stay unchanged.
- `NewTestParser()` remains available and panic behavior remains unchanged when factory is missing.
- No backward compatibility with old package path is required in this repo.

## Migration Design For `mill` (Phase 1 Consumer)

1. Add dependency on `parser` (and `core` if separate module).
2. Replace imports of `github.com/iw2rmb/mill/internal/mill/parser` with `github.com/iw2rmb/sql/parser`.
3. Replace `github.com/iw2rmb/mill/internal/mill/types` usage only where tied to parser DTOs with `core` enums.
4. Keep current PG parser implementation in `mill/internal/mill/db/postgres` and make it implement `parser.Parser`.
5. Keep CGO build-tag registration (`RegisterTestParserFactory`) in `mill` until PG implementation is moved in Phase 2.

## File Mapping (Extraction)

- `../../mill/internal/mill/parser/interface.go` -> `parser/interface.go`
- `../../mill/internal/mill/parser/types.go` -> `parser/types.go`
- `../../mill/internal/mill/parser/interval_parser.go` -> `parser/interval_parser.go`
- `../../mill/internal/mill/types/sql.go` -> `core/sql_types.go` (or equivalent)

Tests to copy in Phase 1:
- `../../mill/internal/mill/parser/interval_parser_test.go`
- `../../mill/internal/mill/parser/fuzz_interval_test.go`
- `../../mill/internal/mill/parser/types_test.go`

## Validation Plan

Required checks after implementation:
- Contract package tests pass in this repo:
  - `go test ./parser/...`
- `mill` compiles after import swap with no parser API changes.
- Parity checks in `mill` still pass for parser-dependent paths:
  - `internal/mill/decomposition/parser_integration_test.go`
  - `internal/mill/routing/router_test.go`
  - `internal/mill/cache/manager_test.go`

## Risks And Mitigations

- Risk: hidden dependency on `mill/internal/mill/types` leaks back in.
  - Mitigation: keep enum ownership in `core`; block `mill/internal` imports in `parser` module lint.
- Risk: migration churn from 141 import sites.
  - Mitigation: perform mechanical import rewrite in one change; no behavioral edits in same PR.
- Risk: tests rely on `NewTestParser()` factory registration side effects.
  - Mitigation: keep registration path explicit in PG parser package and verify with parser-dependent tests.

## Exit Criteria (Phase 1 complete)

- `parser` contracts exist in this repo with copied tests passing.
- No `mill/internal/*` dependency exists in `parser` code.
- `mill` can compile and run parser-dependent tests using `parser` contracts with existing PG implementation.

## Next Phase Link

Phase 2 completed: PG parser implementation moved from `mill/internal/mill/db/postgres/parser_pgquery_*.go` into `parserpg`. See `./parser.md` and `../roadmap/parser.md`.
