# Squall Extracted Module Contracts

This document describes the stable Squall-owned modules consumed by Mill.

## Ownership Boundary

- Squall owns generic SQL parser/runtime/reuse/graph modules:
  - `parser`, `parserpg`
  - `sql/runtime/pg`, `sql/runtime/pg/cdc`, `sql/runtime/pg/snapshot`
  - `sql/reuse/*`
  - `sql/graph`
- Host applications (including Mill) own orchestration, runtime strategy, and application-specific state transitions around those modules.
- Shared modules must not import `mill/internal`.

## Parser Contracts

- `parser` defines parser contracts and DTOs used by all higher-level modules.
- `parserpg` provides the PostgreSQL parser implementation and compatibility normalization required by host integrations.
- CGO-specific parser wiring remains isolated in `parserpg`.

## PostgreSQL Runtime Contracts

- `sql/runtime/pg` defines host-facing provider contracts:
  - parser access (`Parser()`)
  - CDC source lifecycle (`CDC()`)
  - query execution (`Exec()`)
  - schema introspection (`Schema()`)
  - capability and strategy hints (`Caps()`, `IMV()`)
- `sql/runtime/pg/cdc` owns publication/probe/checkpoint/consumer runtime primitives.
- `sql/runtime/pg/snapshot` owns replication-slot snapshot export/import primitives.

CDC defaults are Squall-owned and host-neutral:
- default slot: `squall_slot`
- checkpoint table: `squall_cdc_checkpoint`
- checkpoint index: `idx_squall_cdc_checkpoint_updated_at`

## Reuse Contracts

- `sql/reuse` defines host-facing contracts for decomposition/compiler/router modules.
- Concrete implementations are split into:
  - `sql/reuse/decomposition`
  - `sql/reuse/compiler`
  - `sql/reuse/routing`

## Graph Contracts

- `sql/graph` defines parser-injected query dependency graph behavior.
- Graph modules own query indexing and dependency traversal primitives; host applications own orchestration that reacts to graph results.

## Boundary Enforcement

- Repository checks enforce that extracted shared modules do not import `mill/internal`.
- Import-boundary tests in shared SQL modules enforce package-level dependency boundaries.
