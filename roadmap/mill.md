# Squall Extractions for Mill Core Narrowing

Scope: Implement Squall-owned parser/runtime/reuse/graph modules so Mill (../mill) can consume them and remove duplicated implementations.

Documentation: `docs/parser-migration.md`, `design/squalld.md`.

Legend: [ ] todo, [x] done.

## Boundaries

- Squall owns extracted generic modules: parser compatibility/parserpg normalization, PostgreSQL CDC/snapshot protocol runtime, query decomposition/compiler/router reuse modules, and query dependency graph.
- Squall must not depend on `mill/internal` in extracted packages.
- Mill remains owner of IMV-specific engine semantics and runtime orchestration.

- [x] 1.1 Define extraction contracts and import boundaries for tracks 1..4
  - Repository: `squall`
    1. Define public contracts for parser runtime, CDC batch delivery, reuse modules, and graph integration consumed by Mill.
    2. Add package-level import boundary checks for extracted packages to block `mill/internal` dependencies.
    3. Define module entrypoints for `sql/runtime/pg`, `sql/reuse`, and `sql/graph` with no backward-compatibility shims.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/db/common/provider.go` (`DatabaseProvider`, `CDCSource`, parser/provider shape).
    2. `../mill/internal/mill/cdc/types.go` (`TxEvent`, `TxBatch`) and `../mill/internal/mill/types/cdc.go` (`LSN`, `SlotName`, `BackpressurePolicy`).
    3. `../mill/internal/mill/cdc/dispatcher.go` (`CheckpointSaver` interface and batch processing contract).
    4. `../mill/internal/mill/graph/graph_core.go` (parser-injected graph construction contract).
  - Verification:
    1. Run `scripts/check_no_mill_internal.sh`.
    2. Run `go test ./...`.
  - Reasoning: high

- [x] 1.2 Migrate Mill parser compatibility behavior into Squall parser packages
  - Repository: `squall`
    1. Move parser compatibility behavior from Mill parser adapters into `parserpg` as parser output normalization hooks.
    2. Consolidate interval parser behavior in `parser/interval_parser.go` as the single canonical implementation.
    3. Provide parser test-factory registration so Mill no longer needs local parser adapter factories.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/db/postgres/parser_adapter.go` (`wrapPGQueryParser`, `ExtractMetadata`, `ExtractJSONPaths`, JSON path normalization helpers).
    2. `../mill/internal/mill/parser/interval_parser.go` (`ParseInterval`, `parseIntervalString`, `parseMakeInterval`, `parseISO8601Interval`).
    3. `../mill/internal/mill/db/postgres/parser_cgo.go` (`NewPGQueryParser`, CGO build-tag wiring and parser factory registration).
    4. `../mill/internal/mill/parser/interface.go` and `../mill/internal/mill/parser/test_parser_cgo.go` (test parser registration pattern).
    5. `../mill/internal/mill/decomposition/test_parser_factory_cgo_test.go` (cross-package parser factory adaptation).
  - Verification:
    1. Run `CGO_ENABLED=1 go test ./parser ./parserpg -v`.
    2. Run `CGO_ENABLED=1 go test ./...`.
  - Reasoning: high

- [x] 2.1.a Freeze CDC contracts and ownership boundary before extraction
  - Repository: `squall`
    1. Define Squall-owned CDC contracts for `LSN`, `TxEvent`, `TxBatch`, checkpoint save/load, and consumer batch handler callback.
    2. Define ownership boundary in package docs: Squall owns replication/publication/checkpoint runtime, host app owns domain batch handling.
    3. Add extraction guard tests to enforce contract stability across `sql/runtime/pg/cdc`.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/cdc/types.go` and `../mill/internal/mill/types/cdc.go` (event and LSN type contracts).
    2. `../mill/internal/mill/db/postgres/cdc/consumer.go` (`ConsumerConfig`, `EventHandler` callback contract).
    3. `../mill/internal/mill/db/postgres/cdc/checkpoint.go` (`CheckpointManager`, `SaveCheckpoint`, `AckLSN`, `LoadLSN`, `SaveLSN`).
    4. `../mill/internal/mill/cdc/dispatcher.go` (`DispatchWithCheckpoint` success/ack sequencing contract).
  - Verification:
    1. Run `go test ./... -run '(CDC|Contract|Boundary)' -v`.
    2. Run `go test ./...`.
  - Reasoning: high

- [x] 2.1.b Extract publication/probe/checkpoint primitives into Squall first
  - Repository: `squall`
    1. Implement publication management in `sql/runtime/pg/cdc` for `pubExists`, `createPublication`, `addTables`, and `ListPublicationTables`.
    2. Implement probe and checkpoint table/save/load logic in `sql/runtime/pg/cdc`.
    3. Preserve deterministic table ordering and idempotent DDL behavior from current Mill implementation.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/db/postgres/cdc/publication.go` (`pubExists`, `createPublication`, `addTables`, `ListPublicationTables`, `EnsurePublication`, identifier validation helpers).
    2. `../mill/internal/mill/db/postgres/cdc/probe.go` (`Probe`, error classifications, metrics hooks).
    3. `../mill/internal/mill/db/postgres/cdc/checkpoint.go` (`EnsureCheckpointTable`, `LoadLSN`, `SaveLSN`, monotonic LSN enforcement).
    4. `../mill/internal/mill/db/postgres/provider.go` (`EnsurePublication`, `Probe`, `EnsureCheckpointTable` provider-facing wrappers).
    5. `../mill/internal/mill/db/postgres/cdc/publication_test.go`, `probe_test.go`, `checkpoint_test.go` (behavioral test matrix to port).
  - Verification:
    1. Run `go test ./... -run '(Publication|Probe|Checkpoint)' -v`.
    2. Run `go test ./...`.
  - Reasoning: high

- [x] 2.1.c Extract CDC consumer loop with callback-based integration
  - Repository: `squall`
    1. Implement consumer start/stop/retry/backoff/backpressure logic in `sql/runtime/pg/cdc` without Mill-specific dependencies.
    2. Expose consumer constructor accepting batch callback interface for host-side processing.
    3. Keep behavior parity for retry classification, status interval, and graceful shutdown semantics.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/db/postgres/cdc/consumer.go` (`NewConsumer`, `Start`, `Stop`, retry loop, backoff, shutdown timeout).
    2. `../mill/internal/mill/db/postgres/cdc/stream.go` (`runReplicationStream`, stream lifecycle and context cancellation behavior).
    3. `../mill/internal/mill/db/postgres/cdc/messages.go` (batch assembly and `BackpressurePolicy` handling).
    4. `../mill/internal/mill/db/postgres/cdc/errors.go` (`shouldRetry`, `shouldReconcileSlot`, `shouldReconcilePublication`, error logging policy).
    5. `../mill/internal/mill/db/postgres/cdc/status.go` (status heartbeat/ack cadence).
    6. `../mill/internal/mill/db/postgres/cdc/consumer_test.go`, `consumer_backpressure_extra_test.go`, `consumer_resiliency_test.go` (parity tests to port).
  - Verification:
    1. Run `go test ./... -run '(Consumer|Backpressure|Retry|Shutdown)' -v`.
    2. Run `go test ./...`.
  - Reasoning: xhigh

- [ ] 2.2 Extract replication snapshot export/import primitives into Squall
  - Repository: `squall`
    1. Implement snapshot runtime package `sql/runtime/pg/snapshot` with `ExportWithSlot`, `Import`, and `DropSlot`.
    2. Keep slot lifecycle APIs explicit and avoid Mill logging/metrics dependencies.
    3. Add tests preserving temporary slot cleanup behavior and LSN propagation semantics.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/db/postgres/snapshot/snapshot.go` (`ExportWithSlot`, `Import`, cleanup behavior).
    2. `../mill/internal/mill/db/postgres/snapshot/repl.go` (`OpenReplication`, `CreateTempSlotExportSnapshot`, `DropSlot`).
    3. `../mill/internal/mill/db/postgres/snapshot/snapshot_test.go`, `repl_test.go`, `integration_test.go` (unit and integration behavior).
    4. `../mill/internal/mill/incremental/delta_integration.go` and `../mill/internal/mill/delta/execute_full.go` (host-side usage contract and call ordering).
  - Verification:
    1. Run `go test ./... -run '(Snapshot|Replication|Slot)' -v`.
    2. Run `go test ./...`.
  - Reasoning: high

- [ ] 3.1 Extract query decomposition package to Squall reuse module
  - Repository: `squall`
    1. Implement `sql/reuse/decomposition` from Mill decomposition package with `squal/parser`-only parser dependency.
    2. Replace Mill-specific identifiers with Squall-neutral value types.
    3. Preserve decomposition config and metrics interfaces to minimize migration risk.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/decomposition/decomposer.go` (core interface, config wiring, metrics flow).
    2. `../mill/internal/mill/decomposition/parser_integration.go` (parse + metadata validation path).
    3. `../mill/internal/mill/decomposition/subquery_extractor.go`, `reuse_analyzer.go`, `types.go` (subquery and reuse internals).
    4. `../mill/internal/mill/cache/subquery_manager.go` (host integration surface and expected API shape).
    5. `../mill/tests/decomposition_test.go`, `decomposition_integration_test.go`, `decomposition_benchmark_test.go` (behavior/perf coverage).
  - Verification:
    1. Run `go test ./... -run '(Decompose|Subquery|ReuseAnalyzer)' -v`.
    2. Run `go test ./...`.
  - Reasoning: high

- [ ] 3.2 Extract query compiler and matcher package to Squall reuse module
  - Repository: `squall`
    1. Implement `sql/reuse/compiler` from Mill compiler package with preserved component signatures and threshold behavior.
    2. Keep storage interface and plan-generation contracts independent from persistence backend.
    3. Add boundary tests to keep compiler package free of Mill cache/websocket/api dependencies.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/compiler/compiler.go` (`QueryCompiler`, threshold and TTL gating, storage sync behavior).
    2. `../mill/internal/mill/compiler/matcher.go` (`ComponentMatcher`, exact/superset matching, execution plan generation).
    3. `../mill/internal/mill/compiler/decomposer_core.go` and extraction helpers (`extract_*`, `dependencies.go`) for component model.
    4. `../mill/internal/mill/cache/manager_core.go` and `compiler_integration.go` (host-side compiler integration contract).
    5. `../mill/tests/query_compiler_test.go`, `query_compiler_phase_4_2_test.go`, `component_matcher_test.go` (parity test cases).
  - Verification:
    1. Run `go test ./... -run '(QueryCompiler|ComponentMatcher)' -v`.
    2. Run `go test ./...`.
  - Reasoning: high

- [ ] 3.3 Extract query router and metrics aggregator package to Squall reuse module
  - Repository: `squall`
    1. Implement `sql/reuse/routing` from Mill routing package with preserved decision rules and complexity scoring.
    2. Keep routing package parser-based and detached from any host cache manager internals.
    3. Preserve metrics interfaces used by host integration layers.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/routing/router.go` (`RouteQuery`, complexity scoring, metrics snapshot logic).
    2. `../mill/internal/mill/routing/metrics.go` (aggregator state and rolling metrics behavior).
    3. `../mill/internal/mill/cache/manager_core.go` and `routing_integration.go` (host integration points and router toggles).
    4. `../mill/tests/query_router_test.go` and `routing_metrics_test.go` (behavioral contract to preserve).
  - Verification:
    1. Run `go test ./... -run '(QueryRouter|RoutingMetrics)' -v`.
    2. Run `go test ./...`.
  - Reasoning: medium

- [ ] 4.1 Extract query dependency graph package to Squall graph module
  - Repository: `squall`
    1. Implement `sql/graph` from Mill graph package with parser injection and table/fingerprint indexing behavior preserved.
    2. Replace Mill-specific `QueryID`/`SQLText`/`TableName` types with Squall-neutral graph identifiers.
    3. Preserve concurrency behavior and dependency traversal semantics.
  - References in Mill (copy/reuse):
    1. `../mill/internal/mill/graph/graph_core.go` (data model, parser injection, lock/invariant contract).
    2. `../mill/internal/mill/graph/mutate.go` (`AddQuery`, `RemoveQuery`, index/dependency updates).
    3. `../mill/internal/mill/graph/index.go` (`FindAffectedQueries`, dependent traversal).
    4. `../mill/internal/mill/graph/analyze.go` (reuse/dependency analysis helpers and fallback behavior).
    5. `../mill/internal/mill/cdc/dispatcher.go` and `../mill/cmd/mill/bootstrap.go` (host-side graph usage contract).
    6. `../mill/tests/dependency_detection_test.go` and `../mill/internal/mill/graph/query_graph_injection_test.go` (parity tests).
  - Verification:
    1. Run `go test ./... -run '(QueryGraph|Dependency|AffectedQueries)' -v`.
    2. Run `go test ./...`.
  - Reasoning: high

- [ ] 4.3 Stabilize Squall-side extracted modules for Mill consumption
  - Repository: `squall`
    1. Remove temporary migration glue after Mill integration is complete across parser/runtime/reuse/graph tracks.
    2. Update long-lived Squall docs to reflect final ownership and usage contracts.
    3. Keep `roadmap/mill.md` transient and prune completed transient notes when no longer needed.
  - References in Mill (copy/reuse):
    1. `../mill/roadmap/squall.md` (Mill-side adoption dependencies and completion gates).
    2. `../mill/internal/mill/db/postgres/provider.go` and `../mill/internal/mill/db/common/provider.go` (provider-facing API expectations to keep stable for Mill adoption).
    3. `../mill/cmd/mill/bootstrap.go` (final integration wiring expectations across parser/runtime/graph/reuse modules).
  - Verification:
    1. Run `go test ./...`.
  - Reasoning: high
