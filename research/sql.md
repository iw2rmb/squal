# Design: Shared SQL Platform

This document defines the implementation design for the shared SQL platform used by:
- `aster`
- `mill`
- `cow`
- optional standalone SQL LSP services

Related:
- Aster target alignment: `../../aster/research/target.md`
- Aster protocol shape: `../../aster/docs/protocol/README.md`
- Aster SDK LST surface: `../../aster/docs/sdk/README.md`

## Decision

Build SQL as an independent shared platform in `sql` repo, not inside `aster/internal`.

Rationale:
- `mill` and `cow` must import stable semver tags directly.
- SQL CGO/toolchain and CI must be owned independently from Aster runtime CI.
- SQL completion and live editing are domain-specific and should be owned by SQL packages/services, not by Aster session internals.

## Scope

## In scope

- Parser-neutral SQL core contracts.
- PostgreSQL parser implementation with isolated CGO boundary.
- SQL LST model and deterministic emit behavior.
- Completion and edit planning engine.
- Reusable packages for both batch and live consumers.

## Out of scope

- Aster protocol server implementation.
- Aster workspace/session orchestration internals.
- App-specific UI/editor code.

## Repository Domain And Ownership

This repo owns SQL language intelligence and SQL live-completion semantics.

Aster owns orchestration and protocol integration.

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

4. `lst`
- SQL LST/CST representation
- capture taxonomy for deterministic query/traverse/mutate
- lossless emit guarantees for untouched ranges

5. `complete`
- completion classification/ranking
- deterministic edit planning
- join path suggestions (including multi-column FK)

Allowed dependency direction:
- `core` <- `parser`, `lst`, `complete`
- `parser` <- `parserpg`, `lst`, `complete`
- `lst` <- `complete`
- `parserpg` must not import `lst` or `complete`

## Module And Versioning Strategy

Use separate Go modules per package boundary for clear release and consumption:
- `core/go.mod`
- `parser/go.mod`
- `parserpg/go.mod`
- `lst/go.mod`
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

Aster must be able to implement `cmd/aster-adapter-sql` using this repo only for SQL logic.

`cmd/aster-adapter-sql` in Aster must provide:
- `initialize`
- `didOpen`/`didChange`/`didClose`/`didReset`
- `parse`/`emit`
- `query`
- `traverse`/`mutate`/`walk` via existing Aster protocol flows

Aster package boundaries:
- keep SQL-specific parsing/LST/completion logic out of Aster generic workspace/core packages
- keep Aster as batch-first orchestrator with deterministic contracts

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

## `lst`

Must define:
- SQL LST node model with stable node IDs
- capture schema and kind registry
- emit API with lossless roundtrip guarantees for untouched spans

## `complete`

Must define:
- completion request context input
- deterministic completion candidates output
- deterministic edit plans for accepted candidate
- optional join/snippet proposal outputs

## Completion And Session Ownership

Completion is owned by SQL platform packages/services.

Aster session mode is not required to own SQL completion behavior.

Implications:
- SQL live-completion lifecycle can evolve independently.
- Aster may keep using stateless/batch orchestration while integrating SQL adapter functionality.
- If Aster later adds long-lived sessions, that is for generic orchestration efficiency, not a prerequisite for SQL completion features.

## Aster Integration Boundary

Keep in Aster:
- `cmd/aster-adapter-sql`
- adapter inference (`.sql` -> adapter)
- workspace/sdk/mod generic flows (`Traverse`, `Mutate`, `Walk`, canonical metadata)

Keep in SQL repo:
- parser implementations
- SQL LST model and capture taxonomy
- SQL completion and edit planning
- SQL live provider/fallback policy

## Batch And Live Architecture

Two supported consumption tracks share the same SQL packages:

1. Batch track
- Aster adapter calls SQL packages for parse/query/rewrite/traverse/mutate

2. Live track
- dedicated SQL LSP/service uses SQL packages for low-latency completion/editing

Constraint:
- both tracks must produce consistent deterministic edit semantics for the same input state

## Testing Strategy

## Unit tests

- parser DTO conformance
- LST node/capture stability
- edit planner determinism
- Unicode and byte-span correctness

## Integration tests

- parity cases across `aster` adapter and SQL live service
- crash/fallback behavior for completion providers
- CGO boundary behavior for `parserpg`

## Golden tests

- canonical parse/query/lst/completion fixtures
- deterministic output snapshots

## CI And Toolchain

SQL repo CI owns:
- CGO toolchain and `pg_query` setup
- module-level tests and lint
- release/tag workflow per module

Aster CI consumes released SQL module versions; it does not build SQL parser internals as part of generic Aster pipeline.

## Migration Plan

1. Extract parser contracts from `mill/internal` into `parser`.
2. Port PostgreSQL parser implementation to `parserpg` with test parity.
3. Implement `lst` node/capture schema and lossless emit tests.
4. Implement `complete` with deterministic edits and fallback-oriented behavior.
5. Integrate `cmd/aster-adapter-sql` in Aster using released SQL modules.
6. Integrate `cow` completion stack with SQL modules and fallback policies.
7. Migrate `mill` call-sites from internal parser to SQL modules.

## Risks And Mitigations

- Risk: module split increases release overhead.
  - Mitigation: automate tagging/changelog, keep strict dependency direction.

- Risk: divergence between batch and live behaviors.
  - Mitigation: shared fixtures and parity integration tests across both tracks.

- Risk: CGO instability impacts consumers.
  - Mitigation: isolate CGO in `parserpg`; keep parser-neutral contracts in non-CGO modules.
