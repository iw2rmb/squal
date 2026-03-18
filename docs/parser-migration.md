# Parser Migration State

## Current Ownership

- `parser` owns parser contracts and DTOs.
- `parserpg` owns PostgreSQL parser implementation (CGO + `pg_query` boundary).
- `complete` owns deterministic SQL completion ranking, fallback behavior, and deterministic edit planning.
- `mill` consumes `parserpg` and no longer owns `internal/mill/db/postgres/parser_pgquery_*.go`.

## Mill Integration Shape

- Runtime parser construction in `../mill/internal/mill/db/postgres/parser_cgo.go` creates `parserpg` instances.
- `mill` parser test factory (`../mill/internal/mill/parser/test_parser_cgo.go`) adapts shared parser output to local parser DTOs used by remaining internal tests.

## Validation Executed

- `CGO_ENABLED=1 go test ./internal/mill/db/postgres/...`
- `CGO_ENABLED=1 go test ./internal/mill/decomposition -run 'Test(ParseSQL|ParsedQuery)'`
- `CGO_ENABLED=1 go test ./internal/mill/routing -run 'Test(NewQueryRouter|RouteQuery)'`
- `CGO_ENABLED=1 go test ./internal/mill/cache -run 'Test(NewManager|WithParser|Manager_)'`

## Next Work

- Integrate `cmd/aster-adapter-sql` completion forwarding and edit-plan application in `aster` using `complete`.
- Integrate `cow` completion stack with shared SQL modules and fallback policy from `complete`.

## Extraction Contracts Baseline

- `sql/runtime/pg` defines provider/runtime contracts.
- `sql/runtime/pg/cdc` freezes CDC contracts (`LSN`, `TxEvent`, `TxBatch`, `EventHandler`, `CheckpointLoader`/`CheckpointSaver`/`CheckpointStore`, dispatcher sequencing): Squall owns replication/publication/checkpoint runtime contracts, while host applications own domain batch handling.
- `sql/reuse` defines host-facing decomposition/compiler/router contracts.
- `sql/graph` defines parser-injected graph contracts for host integration.
- `scripts/check_no_mill_internal.sh` now scans extracted packages (`core`, `parser`, `parserpg`, `sql/runtime/pg`, `sql/runtime/pg/cdc`, `sql/reuse`, `sql/graph`) for forbidden `mill/internal` references.
