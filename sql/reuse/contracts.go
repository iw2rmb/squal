package reuse

import (
	"context"
	"time"
)

// QueryID identifies a query in reuse workflows.
type QueryID string

// SQLText stores raw SQL text passed through reuse components.
type SQLText string

// ExecutionStrategy is the selected route for query execution.
type ExecutionStrategy string

const (
	ExecutionStrategyLocalCache    ExecutionStrategy = "LOCAL_CACHE"
	ExecutionStrategyLocalDatabase ExecutionStrategy = "LOCAL_DATABASE"
	ExecutionStrategyBypassCache   ExecutionStrategy = "BYPASS_CACHE"
)

// DecompositionResult is the high-level decomposition contract.
type DecompositionResult struct {
	QueryID     QueryID   `json:"query_id"`
	OriginalSQL SQLText   `json:"original_sql"`
	Tables      []string  `json:"tables"`
	Operations  []string  `json:"operations"`
	GeneratedAt time.Time `json:"generated_at"`
}

// CompiledQuery is the compiler output contract consumed by host orchestration.
type CompiledQuery struct {
	ComponentsReused int     `json:"components_reused"`
	ComponentCount   int     `json:"component_count"`
	SavedCost        float64 `json:"saved_cost"`
	WasOptimized     bool    `json:"was_optimized"`
}

// RoutingDecision is the router output contract consumed by host orchestration.
type RoutingDecision struct {
	Strategy         ExecutionStrategy `json:"strategy"`
	Reasoning        string            `json:"reasoning"`
	EstimatedLatency time.Duration     `json:"estimated_latency"`
	CacheHitChance   float64           `json:"cache_hit_chance"`
	LoadFactor       float64           `json:"load_factor"`
}

// Decomposer is the entrypoint contract for query decomposition modules.
type Decomposer interface {
	DecomposeQuery(ctx context.Context, sql SQLText) (*DecompositionResult, error)
}

// Compiler is the entrypoint contract for query compilation and matching modules.
type Compiler interface {
	CompileQuery(sql SQLText) (*CompiledQuery, error)
}

// Router is the entrypoint contract for query routing modules.
type Router interface {
	RouteQuery(sql SQLText, queryID QueryID, cacheExists bool) (*RoutingDecision, error)
}
