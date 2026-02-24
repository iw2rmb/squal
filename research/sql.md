# Research: Shared SQL Platform

This document defines the target architecture for shared SQL capabilities used by:
- `aster`
- `mill`
- `cow`
- optional standalone SQL tooling

Related:
- Parser migration status: `../docs/parser-migration.md`
- Completion hardening roadmap status: `../roadmap/comp.md`
- Aster-side SQL LST ownership note: `../../aster/research/sql-lst.md`

## Decision

Build SQL as an independent shared platform in `sql` repo, and keep SQL-specific traversal/mutation/LST orchestration in `aster`.

Rationale:
- `mill` and `cow` need stable semver imports for parser and completion capabilities.
- Completion in editors can be implemented deterministically with parser + context + edit planning, without requiring SQL-side LST ownership.
- Aster already owns generic traversal/mutation/walk orchestration and should remain the place for SQL LST integration logic.

## Scope

## In scope

- Parser-neutral SQL core contracts.
- PostgreSQL parser implementation with isolated CGO boundary.
- SQL completion classification/ranking.
- Deterministic completion edit planning.
- Reusable packages for batch and live consumers.

## Out of scope

- SQL-owned LST package in this repository.
- Aster protocol server implementation details.
- Aster workspace/session internals.
- App-specific UI/editor code.

## Repository Domain And Ownership

This repo owns SQL parsing and completion semantics.

Aster owns traversal/mutation orchestration and SQL LST integration behavior.

Import rule:
- allowed: `aster -> sql`
- forbidden: `sql -> aster`

## Package Architecture

Top-level package split:

1. `core`
- neutral contracts and shared data model
- no parser backend
- no CGO

2. `parser`
- parser interfaces and analysis DTOs
- no CGO
- no provider-specific runtime logic

3. `parserpg`
- PostgreSQL parser implementation via `pg_query`
- only package with CGO
- no app/runtime orchestration logic

4. `complete`
- completion classification/ranking
- deterministic edit planning
- join path suggestions (including multi-column FK)
- parser-context completion fallback behavior

Allowed dependency direction:
- `core` <- `parser`, `complete`
- `parser` <- `parserpg`, `complete`
- `parserpg` must not import `complete`

## Module And Versioning Strategy

Use separate Go modules per package boundary for clear release and consumption:
- `core/go.mod`
- `parser/go.mod`
- `parserpg/go.mod`
- `complete/go.mod`

Version policy:
- semver tags per module
- compatibility guarantees are module-local
- release notes include API and behavior changes for deterministic edits/completions

## Functional Requirements

## Shared platform requirements

- byte-accurate positions/ranges/edits
- deterministic output contracts for parse/query/edit planning
- Unicode-safe coordinate handling
- stable and explicit DTO schema for cross-project consumers

## Aster-facing requirements

Aster must be able to implement `cmd/aster-adapter-sql` using this repo for SQL parse and completion logic.

`cmd/aster-adapter-sql` in Aster must provide:
- `initialize`
- `didOpen`/`didChange`/`didClose`/`didReset`
- `parse`
- completion request handling backed by `complete`
- traversal/mutation/walk integration in Aster-owned SQL LST layer

Aster package boundaries:
- keep SQL parsing/completion logic out of Aster generic workspace/core packages
- keep Aster as orchestrator for traversal/mutation/walk execution

## Cow-facing requirements

- completion must remain available if upstream provider fails
- parser-backed fallback completion path must be first-class
- completion edits must be deterministic and Unicode-safe

## Mill-facing requirements

- parser contracts currently in `mill/internal` must be consumable through `parser`
- PostgreSQL parser features migrate into `parserpg` without decomposition regressions
- normalization/fingerprinting must remain reusable from shared packages

## API Contracts

## `core`

Must define canonical DTOs for:
- `Span`, `TextEdit`, `TextChangeSet`
- completion item and ranking fields
- catalog graph (`Table`, `Column`, FK edges incl. composite keys)
- query context and scoring metadata

## `parser`

Must define:
- parser interface (`Parse`, metadata extraction, normalization/fingerprint)
- analysis DTOs (joins, group-by, distinct, aggregates, temporal/sliding windows, json-path)
- parse diagnostics model with stable fields

## `complete`

Must define:
- completion request context input
- deterministic completion candidates output
- deterministic edit plans for accepted candidate
- optional join/snippet proposal outputs

## Completion And Session Ownership

Completion semantics are owned by SQL packages/services.

Aster session/workspace mode remains the owner of orchestration and document-graph traversal behavior.

Implications:
- SQL completion lifecycle can evolve independently.
- Aster can integrate SQL completion while retaining existing traversal/mutation/walk engine ownership.

## Aster Integration Boundary

Keep in Aster:
- `cmd/aster-adapter-sql`
- adapter inference (`.sql` -> adapter)
- workspace/sdk/mod generic flows (`Traverse`, `Mutate`, `Walk`, canonical metadata)
- SQL LST integration and traversal/mutation orchestration

Keep in SQL repo:
- parser implementations
- SQL completion ranking and edit planning
- SQL provider/fallback policy for completion

## Batch And Live Architecture

Two supported consumption tracks share the same SQL packages:

1. Batch track
- Aster adapter calls SQL packages for parse/completion planning and applies edits through Aster orchestration.

2. Live track
- dedicated SQL tooling uses SQL packages for low-latency completion and deterministic edit planning.

Constraint:
- both tracks must produce consistent deterministic edit semantics for the same input state

## Testing Strategy

## Unit tests

- parser DTO conformance
- completion ranking determinism
- edit planner determinism
- Unicode and byte-span correctness

## Integration tests

- parity cases across `aster` adapter and SQL live path
- crash/fallback behavior for completion providers
- CGO boundary behavior for `parserpg`

## Golden tests

- canonical parse/completion fixtures
- deterministic output snapshots

## CI And Toolchain

SQL repo CI owns:
- CGO toolchain and `pg_query` setup
- module-level tests and lint
- release/tag workflow per module

Aster CI consumes released SQL module versions; it does not build SQL parser internals as part of generic Aster pipeline.

## Migration Plan

Status:
- completed: parser contract extraction into shared `parser` package
- completed: PostgreSQL parser migration to `parserpg` and `mill` consumer switch (`docs/parser-migration.md`)
- completed: `complete` package implementation baseline; current relevance hardening tracked in `roadmap/comp.md`

1. Extract parser contracts from `mill/internal` into `parser`.
2. Port PostgreSQL parser implementation to `parserpg` with test parity.
3. Completed: implement `complete` with deterministic edits and fallback-oriented behavior.
4. Integrate `cmd/aster-adapter-sql` in Aster using released SQL modules.
5. Integrate `cow` completion stack with SQL modules and fallback policies.
6. Maintain `mill` parser consumption on shared modules.

## Risks And Mitigations

- Risk: module split increases release overhead.
  - Mitigation: automate tagging/changelog, keep strict dependency direction.

- Risk: divergence between SQL completion behavior and Aster traversal/mutation application.
  - Mitigation: shared fixtures and parity integration tests across both tracks.

- Risk: CGO instability impacts consumers.
  - Mitigation: isolate CGO in `parserpg`; keep parser-neutral and completion contracts in non-CGO modules.
