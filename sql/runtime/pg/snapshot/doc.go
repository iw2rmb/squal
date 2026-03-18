// Package snapshot defines PostgreSQL replication snapshot export/import primitives.
//
// Ownership boundary:
//   - Squall owns slot-based snapshot export/import runtime behavior.
//   - Host applications own orchestration and call ordering around these primitives.
package snapshot
