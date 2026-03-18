// Package graph tracks query dependencies and provides fast lookups for
// invalidation and incremental maintenance.
//
// Contract
//   - QueryGraph stores QueryNode objects and maintains table indexes and
//     bidirectional edges (Dependencies/Dependents). A RWMutex guards state.
//   - Parser injection is mandatory; the graph relies on a parser.Parser to
//     produce fingerprints, table lists, and metadata.
//   - Analysis helpers (e.g., dependency chains) support targeted invalidation
//     and reuse planning.
//
// Key types
//   - QueryGraph, QueryNode.
//   - ReusableQuery, DependencyChain, DependencyStep.
package graph
