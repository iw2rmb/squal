// Package cdc defines PostgreSQL CDC contracts and extraction boundaries.
//
// Ownership boundary:
//   - Squall owns replication/publication/checkpoint runtime contracts.
//   - Host applications own domain batch handling through EventHandler callbacks.
//   - This package intentionally exposes runtime-neutral CDC interfaces only.
package cdc
