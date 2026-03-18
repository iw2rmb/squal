// Package compiler analyzes SQL, decomposes queries into reusable components,
// and produces execution/optimization plans that can leverage cached pieces.
//
// Contract
//   - QueryCompiler orchestrates decomposition, matching, and plan creation.
//   - ComponentStorage abstracts storage of compiled components for reuse.
//   - Decomposer extracts a QueryPlan (components, filters, aggs) from SQL.
//   - Matcher inspects cached components and proposes a ReuseStrategy and
//     ExecutionPlan to minimize work.
//
// Key types
//   - QueryCompiler, CompiledQuery, PerformanceGain.
//   - ComponentStorage (interface).
//   - QueryDecomposer, QueryPlan, ComponentType, QueryComponent, FilterExpr, AggExpr.
//   - ComponentMatcher, ReuseType, Schema, CachedComponent, ComponentReuse,
//     ReuseStrategy, ExecutionPlan, ExecutionStep, PerformanceModel.
package compiler
