# Design: Shared Parser Platform

Related:
- Research baseline: `../research/sql.md`
- Completed extraction baseline: `./sql.md`
- Existing phase execution log: `../roadmap/sql/phase-1-parser-contracts.md`

## Goal

Define parser-focused architecture after Phase 1 extraction is complete:
- keep parser contracts in shared packages
- move PostgreSQL parser implementation from `mill` into this repository
- preserve current behavior and test parity during migration

## Current State

- `core` and `parser` exist in this repository and are used as shared contracts.
- PostgreSQL parser implementation is now owned by `sql/parserpg` (ported from `../mill/internal/mill/db/postgres`); `mill` consumer migration is tracked separately.
- `mill` integration depends on CGO-backed `pg_query` parser registration.

## Scope

In scope:
- introduce `parserpg` package in this repository
- port PostgreSQL parser implementation from `mill/internal/mill/db/postgres/parser_pgquery_*.go`
- keep CGO boundary isolated inside `parserpg`
- preserve parser contract behavior (`parser.Parser`, metadata, normalize/fingerprint)
- keep test parser factory wiring for parity in tests

Out of scope:
- `lst` and `complete` implementation
- Aster adapter implementation details
- Cow runtime integration details

## Target Architecture

1. `core`
- parser-adjacent shared enums and neutral types

2. `parser`
- parser-neutral contracts and DTOs
- test parser factory API

3. `parserpg`
- PostgreSQL parser implementation via `pg_query`
- all CGO code isolated here
- implements `parser.Parser`

Dependency direction:
- `core` <- `parser`
- `parser` <- `parserpg`
- `parserpg` must not import `lst` or `complete`

## Migration Design

1. Create `parserpg` package skeleton with build tags and CGO linkage.
2. Port parser source files from `mill/internal/mill/db/postgres`.
3. Replace imports to shared `parser` and `core` packages.
4. Rewire `RegisterTestParserFactory` integration to shared `parser`.
5. Port parserpg unit/integration tests from `mill` and keep behavior-equivalent assertions.
6. Validate parity in both repositories:
- `sql`: parser/core/parserpg tests
- `mill`: parser-dependent decomposition/routing/cache tests with shared parser contracts

## Compatibility Rules

- No backward compatibility with internal `mill` paths is required.
- Public parser contract behavior must remain stable.
- Normalization/fingerprint outputs must remain behavior-compatible with existing `mill` expectations.
- DTO field names and semantics stay unchanged.

## Validation

Required checks:
- `go test ./core/... ./parser/... ./parserpg/...`
- CGO-enabled parserpg tests pass
- `mill` parser-dependent tests pass after switching to `parserpg`

## Exit Criteria

- `parserpg` is the canonical PostgreSQL parser implementation in this repo.
- `mill` no longer owns parser implementation code.
- CGO parser boundary is isolated to `parserpg`.
- parity tests are green in both `sql` and `mill`.

## Next Link

After parser migration completes, continue with:
- `lst` implementation
- `complete` implementation
