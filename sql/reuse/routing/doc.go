// Package routing provides parser-aware query execution routing and rolling
// metrics aggregation used by host cache/runtime integrations.
//
// Contract
//   - QueryRouter decides LOCAL_CACHE, LOCAL_DATABASE, or BYPASS_CACHE.
//   - RouteQuery preserves deterministic decision rules based on parser metadata
//     and current SystemMetrics snapshots.
//   - MetricsAggregator tracks rolling query stats used by the router.
//
// Key types
//   - QueryRouter, RoutingDecision, ExecutionStrategy, SystemMetrics.
//   - MetricsAggregator, QueryEvent.
package routing
