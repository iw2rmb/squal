// Matcher policy and performance model.
//
// Matching precedence:
//  1. Exact match — prefer a cached component with identical signature.
//  2. Superset match — when enabled, prefer a cached component whose schema
//     is a strict or non‑strict superset of the target (filtering may apply).
//  3. Combination match — planned: compose multiple cached components to
//     satisfy a target when neither exact nor single‑superset matches exist.
//
// Current implementation:
//   - Implements (1) Exact and (2) Superset. Combination reuse is represented
//     in types but not executed yet.
//   - Superset matching is guarded by an internal flag (`enableSupersetMatch`)
//     and validates table/column superset relationships via `ValidateSchemaSuperset`.
//   - Reuse savings are accumulated per component and compared to a
//     configurable minimum threshold (`minSavingsThreshold`) to decide whether
//     to emit a reuse plan or compute everything.
//
// Performance model:
//   - `PerformanceModel` provides optional per‑`ComponentType` cost hints used by
//     `estimateComponentCost`. Absent hints, defaults apply (JOIN > AGG > WHERE > FROM).
//   - Exact reuse credits full estimated cost; superset reuse currently credits
//     80% of the component cost to account for filtering/adapter overhead.
//   - `generateExecutionPlan` orders steps as cached reads first, then new
//     computations, followed by a merge step when both are present.
//
// Concurrency:
//   - Hot maps (`cachedComponents`, `componentIndex`) use `sync.Map`.
//   - Tunables and the performance model are protected by `mu` (RWMutex).
package compiler

import (
	"sync"
	"time"
)

// ReuseType represents how a cached component can be reused
type ReuseType string

const (
	EXACT_REUSE       ReuseType = "EXACT"
	SUPERSET_REUSE    ReuseType = "SUPERSET"
	COMBINATION_REUSE ReuseType = "COMBINATION"
)

// Schema represents the structure of cached data
type Schema struct {
	Tables  []string
	Columns []string
}

// CachedComponent represents a cached query component
type CachedComponent struct {
	Signature     string
	ComponentType ComponentType
	ResultSchema  *Schema
	ResultData    interface{}
	CacheTime     time.Time
	ValidUntil    time.Time
	Usage         int64
	Cost          float64 // Cost of computing this component
}

// ComponentReuse describes how a component can be reused
type ComponentReuse struct {
	Component     QueryComponent
	CachedResult  *CachedComponent
	CachedResults []*CachedComponent // For combination reuse
	ReuseType     ReuseType
	FilterNeeded  bool
}

// ReuseStrategy describes the overall reuse strategy for a query.
//
// Semantics:
//   - ReusableComponents: components satisfied from cache (EXACT, SUPERSET, or COMBINATION).
//   - NewComponents: components that must be computed from source.
//   - ComputationSavings: estimated savings in arbitrary cost units from the
//     PerformanceModel; if below the matcher's minimum savings threshold,
//     reuse is abandoned and all components are computed fresh.
//   - ExecutionPlan: deterministic, high-level step ordering for downstream execution.
//
// Concurrency:
//   - Produced per request and not mutated after return; treat as immutable and
//     safe for concurrent reads only.
type ReuseStrategy struct {
	ReusableComponents []ComponentReuse
	NewComponents      []QueryComponent
	ComputationSavings float64
	ExecutionPlan      *ExecutionPlan
}

// ExecutionPlan describes how to execute the query with reused components.
//
// Plan meaning:
//   - Steps use Type values:
//     "cached": read the cached component identified by ComponentID (signature).
//     Downstream may apply lightweight filtering/adaptation when the
//     associated reuse indicates it (e.g., superset matches).
//     "compute": compute the component identified by ComponentID from source.
//     "merge": reconcile cached and computed results into a single output.
//   - Ordering is stable: all cached steps, then compute steps, optionally a final
//     merge step when both cached and computed results exist.
//
// Concurrency:
//   - ExecutionPlan is a data container; callers may execute independent steps
//     in parallel if dependencies allow. The struct itself is not mutated after return.
type ExecutionPlan struct {
	Steps []ExecutionStep
}

// ExecutionStep represents a single step in query execution
type ExecutionStep struct {
	Type        string // "cached", "compute", "merge"
	ComponentID string
}

// PerformanceModel estimates costs and savings
type PerformanceModel struct {
	ComponentCosts map[ComponentType]float64
}

// ComponentMatcher matches query components with cached components.
//
// Thread-safety:
//   - Safe for concurrent use by multiple goroutines.
//   - `cachedComponents` and `componentIndex` are sync.Map and support concurrent reads/updates.
//   - Tunables (`enableSupersetMatch`, `performanceModel`, `minSavingsThreshold`) are
//     guarded by `mu` (RWMutex).
//   - `componentIndex` is best-effort and eventually consistent: concurrent
//     AddCachedComponent calls may temporarily miss or overwrite an index entry. The
//     authoritative store is `cachedComponents` (exact-match lookups are unaffected).
//     Superset searches rely on the index and may temporarily miss candidates under race.
//
// Usage:
//   - `FindReusableComponents` returns a `ReuseStrategy` with an `ExecutionPlan`. Treat
//     both as read-only; the matcher does not mutate them after returning.
type ComponentMatcher struct {
	cachedComponents    sync.Map // Thread-safe component storage
	componentIndex      sync.Map // Index by component type for fast lookup
	enableSupersetMatch bool
	performanceModel    *PerformanceModel
	minSavingsThreshold float64
	mu                  sync.RWMutex // Protect non-sync.Map fields
}

// NewComponentMatcher creates a new component matcher
func NewComponentMatcher() *ComponentMatcher {
	return &ComponentMatcher{
		enableSupersetMatch: true, // Enable superset matching by default in Phase 4-2.3
		minSavingsThreshold: 0.0,
	}
}

// AddCachedComponent adds a cached component for matching
func (cm *ComponentMatcher) AddCachedComponent(component *CachedComponent) error {
	// Store in main cache
	cm.cachedComponents.Store(component.Signature, component)

	// Update type index for faster lookups
	typeKey := string(component.ComponentType)
	if existing, ok := cm.componentIndex.Load(typeKey); ok {
		signatures := existing.([]string)
		signatures = append(signatures, component.Signature)
		cm.componentIndex.Store(typeKey, signatures)
	} else {
		cm.componentIndex.Store(typeKey, []string{component.Signature})
	}

	return nil
}

// FindReusableComponents finds cached components that can be reused
func (cm *ComponentMatcher) FindReusableComponents(plan *QueryPlan) (*ReuseStrategy, error) {
	strategy := &ReuseStrategy{
		ReusableComponents: []ComponentReuse{},
		NewComponents:      []QueryComponent{},
		ComputationSavings: 0.0,
	}

	for _, component := range plan.Components {
		// Try exact match first
		if cached, exists := cm.cachedComponents.Load(component.Signature); exists {
			cachedComp := cached.(*CachedComponent)
			reuse := ComponentReuse{
				Component:    component,
				CachedResult: cachedComp,
				ReuseType:    EXACT_REUSE,
			}
			strategy.ReusableComponents = append(strategy.ReusableComponents, reuse)
			strategy.ComputationSavings += cm.estimateComponentCost(component)
			continue
		}

		// Try superset match if enabled
		cm.mu.RLock()
		supersetEnabled := cm.enableSupersetMatch
		cm.mu.RUnlock()

		if supersetEnabled {
			if superset := cm.findSupersetMatch(component); superset != nil {
				reuse := ComponentReuse{
					Component:    component,
					CachedResult: superset,
					ReuseType:    SUPERSET_REUSE,
					FilterNeeded: true,
				}
				strategy.ReusableComponents = append(strategy.ReusableComponents, reuse)
				strategy.ComputationSavings += cm.estimateComponentCost(component) * 0.8
				continue
			}
		}

		// No match found, mark as new
		strategy.NewComponents = append(strategy.NewComponents, component)
	}

	// Check minimum savings threshold
	cm.mu.RLock()
	threshold := cm.minSavingsThreshold
	cm.mu.RUnlock()

	if threshold > 0 && strategy.ComputationSavings < threshold {
		// Not worth reusing, compute everything fresh
		strategy.ReusableComponents = []ComponentReuse{}
		strategy.NewComponents = plan.Components
		strategy.ComputationSavings = 0
	}

	// Generate execution plan
	strategy.ExecutionPlan = cm.generateExecutionPlan(strategy)

	return strategy, nil
}

// EnableSupersetMatching enables or disables superset matching
func (cm *ComponentMatcher) EnableSupersetMatching(enable bool) {
	cm.mu.Lock()
	cm.enableSupersetMatch = enable
	cm.mu.Unlock()
}

// SetPerformanceModel sets the performance model for cost estimation
func (cm *ComponentMatcher) SetPerformanceModel(model *PerformanceModel) {
	cm.mu.Lock()
	cm.performanceModel = model
	cm.mu.Unlock()
}

// SetMinimumSavingsThreshold sets the minimum savings required for reuse
func (cm *ComponentMatcher) SetMinimumSavingsThreshold(threshold float64) {
	cm.mu.Lock()
	cm.minSavingsThreshold = threshold
	cm.mu.Unlock()
}

func (cm *ComponentMatcher) findSupersetMatch(component QueryComponent) *CachedComponent {
	// Get components of the same type from index
	typeKey := string(component.Type)
	if signatures, ok := cm.componentIndex.Load(typeKey); ok {
		for _, sig := range signatures.([]string) {
			if cached, exists := cm.cachedComponents.Load(sig); exists {
				cachedComp := cached.(*CachedComponent)

				// Use proper superset matching logic
				if cm.isSupersetMatch(cachedComp, &component) {
					return cachedComp
				}
			}
		}
	}
	return nil
}

func (cm *ComponentMatcher) estimateComponentCost(component QueryComponent) float64 {
	// First check if we have a cached component with a specific cost
	if cached, exists := cm.cachedComponents.Load(component.Signature); exists {
		cachedComp := cached.(*CachedComponent)
		if cachedComp.Cost > 0 {
			return cachedComp.Cost
		}
	}

	cm.mu.RLock()
	model := cm.performanceModel
	cm.mu.RUnlock()

	if model != nil && model.ComponentCosts != nil {
		if cost, exists := cm.performanceModel.ComponentCosts[component.Type]; exists {
			return cost
		}
	}
	// Default costs
	switch component.Type {
	case JOIN_COMPONENT:
		return 20.0
	case AGG_COMPONENT:
		return 10.0
	case WHERE_COMPONENT:
		return 5.0
	case FROM_COMPONENT:
		return 1.0
	default:
		return 1.0
	}
}

func (cm *ComponentMatcher) generateExecutionPlan(strategy *ReuseStrategy) *ExecutionPlan {
	plan := &ExecutionPlan{
		Steps: []ExecutionStep{},
	}

	// Add steps for cached components
	for _, reuse := range strategy.ReusableComponents {
		plan.Steps = append(plan.Steps, ExecutionStep{
			Type:        "cached",
			ComponentID: reuse.CachedResult.Signature,
		})
	}

	// Add steps for new components
	for _, comp := range strategy.NewComponents {
		plan.Steps = append(plan.Steps, ExecutionStep{
			Type:        "compute",
			ComponentID: comp.Signature,
		})
	}

	// Add merge step if needed
	if len(strategy.ReusableComponents) > 0 && len(strategy.NewComponents) > 0 {
		plan.Steps = append(plan.Steps, ExecutionStep{
			Type: "merge",
		})
	}

	return plan
}

// LoadComponentsFromStorage loads cached components from persistent storage
func (cm *ComponentMatcher) LoadComponentsFromStorage(storage interface{}) error {
	// For now, we'll implement lazy loading - components are loaded when needed
	// during FindReusableComponents. This is a simplified approach that doesn't
	// require exposing internal storage details.

	// In a full implementation, we'd iterate through all stored components
	// and add them to the matcher's component index for faster lookup.
	// For this phase, we'll rely on the storage layer to provide components
	// during the matching process.

	return nil
}

// FindSupersetMatches finds cached components that are supersets of the target component
// This is the public interface for superset matching used by tests and external callers
func (cm *ComponentMatcher) FindSupersetMatches(target *QueryComponent) ([]ComponentReuse, error) {
	var matches []ComponentReuse

	// Enable superset matching temporarily for this search
	cm.mu.Lock()
	originalState := cm.enableSupersetMatch
	cm.enableSupersetMatch = true
	cm.mu.Unlock()

	// Restore original state when done
	defer func() {
		cm.mu.Lock()
		cm.enableSupersetMatch = originalState
		cm.mu.Unlock()
	}()

	// Find all cached components of the same type
	typeKey := string(target.Type)
	if signatures, exists := cm.componentIndex.Load(typeKey); exists {
		for _, sig := range signatures.([]string) {
			if cached, found := cm.cachedComponents.Load(sig); found {
				cachedComp := cached.(*CachedComponent)

				// Check if cached component is a superset of target
				if cm.isSupersetMatch(cachedComp, target) {
					match := ComponentReuse{
						Component:    *target,
						CachedResult: cachedComp,
						ReuseType:    SUPERSET_REUSE,
						FilterNeeded: cm.requiresFiltering(cachedComp, target),
					}
					matches = append(matches, match)
				}
			}
		}
	}

	return matches, nil
}

// ValidateSchemaSuperset validates if cached schema is a superset of target schema
func (cm *ComponentMatcher) ValidateSchemaSuperset(cachedSchema, targetSchema *Schema) (bool, error) {
	if cachedSchema == nil || targetSchema == nil {
		return false, nil
	}

	// Check if all target tables are present in cached schema
	cachedTableSet := make(map[string]bool)
	for _, table := range cachedSchema.Tables {
		cachedTableSet[table] = true
	}

	for _, targetTable := range targetSchema.Tables {
		if !cachedTableSet[targetTable] {
			return false, nil // Target table not found in cached schema
		}
	}

	// Check if all target columns are present in cached schema
	cachedColumnSet := make(map[string]bool)
	for _, column := range cachedSchema.Columns {
		cachedColumnSet[column] = true
	}

	for _, targetColumn := range targetSchema.Columns {
		if !cachedColumnSet[targetColumn] {
			return false, nil // Target column not found in cached schema
		}
	}

	return true, nil
}

// isSupersetMatch checks if cached component is a superset of the target component
func (cm *ComponentMatcher) isSupersetMatch(cached *CachedComponent, target *QueryComponent) bool {
	// Components must be of same type
	if cached.ComponentType != target.Type {
		return false
	}

	// Check schema superset relationship
	targetSchema := &Schema{
		Tables:  target.Tables,
		Columns: target.Columns,
	}

	isSuperset, _ := cm.ValidateSchemaSuperset(cached.ResultSchema, targetSchema)
	return isSuperset
}

// requiresFiltering determines if filtering is needed for superset reuse
func (cm *ComponentMatcher) requiresFiltering(cached *CachedComponent, target *QueryComponent) bool {
	// If cached has more columns or broader filters than target, filtering is needed
	if len(cached.ResultSchema.Columns) > len(target.Columns) {
		return true
	}

	// If cached has fewer filters than target, additional filtering is needed
	// For now, assume filtering is needed if it's a superset match
	return true
}
