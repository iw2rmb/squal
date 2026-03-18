# Squall Extractions for Mill Core Narrowing

Scope: Implement and stabilize Squall-owned parser/runtime/reuse/graph modules for Mill consumption.

Documentation: `docs/extracted-modules.md`, `design/squalld.md`.

Legend: [ ] todo, [x] done.

## Boundaries

- Squall owns extracted generic modules: parser compatibility/parserpg normalization, PostgreSQL CDC/snapshot protocol runtime, query decomposition/compiler/router reuse modules, and query dependency graph.
- Squall must not depend on `mill/internal` in extracted packages.
- Mill remains owner of IMV-specific engine semantics and runtime orchestration.

## Completion Snapshot (Transient)

Detailed per-track execution notes were pruned after stabilization completion.

- [x] 1.1 Define extraction contracts and import boundaries for tracks 1..4.
- [x] 1.2 Migrate Mill parser compatibility behavior into Squall parser packages.
- [x] 2.1.a Freeze CDC contracts and ownership boundary before extraction.
- [x] 2.1.b Extract publication/probe/checkpoint primitives into Squall first.
- [x] 2.1.c Extract CDC consumer loop with callback-based integration.
- [x] 2.2 Extract replication snapshot export/import primitives into Squall.
- [x] 3.1 Extract query decomposition package to Squall reuse module.
- [x] 3.2 Extract query compiler and matcher package to Squall reuse module.
- [x] 3.3 Extract query router and metrics aggregator package to Squall reuse module.
- [x] 4.1 Extract query dependency graph package to Squall graph module.
- [x] 4.3 Stabilize Squall-side extracted modules for Mill consumption.

## Validation

- `go test ./...`
