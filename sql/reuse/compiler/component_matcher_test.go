package compiler

import "testing"

func TestComponentMatcher_ExactMatchAndNoMatch(t *testing.T) {
	t.Parallel()

	t.Run("exact_match", func(t *testing.T) {
		t.Parallel()
		matcher := NewComponentMatcher()

		cached := &CachedComponent{
			Signature:     "WHERE_users_age_gt_18",
			ComponentType: WHERE_COMPONENT,
			ResultSchema: &Schema{
				Tables:  []string{"users"},
				Columns: []string{"age"},
			},
		}
		if err := matcher.AddCachedComponent(cached); err != nil {
			t.Fatalf("add cached component: %v", err)
		}

		plan := &QueryPlan{
			Components: []QueryComponent{
				{
					Type:      WHERE_COMPONENT,
					Signature: "WHERE_users_age_gt_18",
					Filters: []FilterExpr{
						{Column: "age", Operator: ">", Value: 18},
					},
				},
			},
		}

		strategy, err := matcher.FindReusableComponents(plan)
		if err != nil {
			t.Fatalf("find reusable components: %v", err)
		}
		if len(strategy.ReusableComponents) != 1 {
			t.Fatalf("expected 1 reusable component, got %d", len(strategy.ReusableComponents))
		}
		reuse := strategy.ReusableComponents[0]
		if reuse.ReuseType != EXACT_REUSE {
			t.Fatalf("expected EXACT reuse type, got %s", reuse.ReuseType)
		}
		if reuse.CachedResult != cached {
			t.Fatal("expected exact cached component to be reused")
		}
	})

	t.Run("different_signature_is_new_component", func(t *testing.T) {
		t.Parallel()
		matcher := NewComponentMatcher()
		_ = matcher.AddCachedComponent(&CachedComponent{
			Signature:     "WHERE_orders_status_eq_completed",
			ComponentType: WHERE_COMPONENT,
		})

		plan := &QueryPlan{
			Components: []QueryComponent{
				{Type: WHERE_COMPONENT, Signature: "WHERE_orders_status_eq_pending"},
			},
		}
		strategy, err := matcher.FindReusableComponents(plan)
		if err != nil {
			t.Fatalf("find reusable components: %v", err)
		}
		if len(strategy.ReusableComponents) != 0 {
			t.Fatalf("expected 0 reusable components, got %d", len(strategy.ReusableComponents))
		}
		if len(strategy.NewComponents) != 1 {
			t.Fatalf("expected 1 new component, got %d", len(strategy.NewComponents))
		}
	})
}

func TestComponentMatcher_SupersetMatching(t *testing.T) {
	t.Parallel()

	matcher := NewComponentMatcher()
	cached := &CachedComponent{
		Signature:     "AGG_SUM_amount_GROUP_BY_user_id",
		ComponentType: AGG_COMPONENT,
		ResultSchema: &Schema{
			Tables:  []string{"orders"},
			Columns: []string{"user_id", "total", "status"},
		},
	}
	if err := matcher.AddCachedComponent(cached); err != nil {
		t.Fatalf("add cached component: %v", err)
	}

	plan := &QueryPlan{
		Components: []QueryComponent{
			{
				Type:      AGG_COMPONENT,
				Signature: "AGG_SUM_amount_GROUP_BY_user_id_WHERE_status_completed",
				Tables:    []string{"orders"},
				Columns:   []string{"user_id", "total"},
				Aggregations: []AggExpr{
					{Function: "SUM", Column: "amount"},
				},
			},
		},
	}

	strategy, err := matcher.FindReusableComponents(plan)
	if err != nil {
		t.Fatalf("find reusable components: %v", err)
	}
	if len(strategy.ReusableComponents) != 1 {
		t.Fatalf("expected 1 reusable component, got %d", len(strategy.ReusableComponents))
	}
	reuse := strategy.ReusableComponents[0]
	if reuse.ReuseType != SUPERSET_REUSE {
		t.Fatalf("expected SUPERSET reuse type, got %s", reuse.ReuseType)
	}
	if !reuse.FilterNeeded {
		t.Fatal("expected filter to be required for superset match")
	}
	if strategy.ComputationSavings <= 0 {
		t.Fatalf("expected positive computation savings, got %.2f", strategy.ComputationSavings)
	}

	matches, err := matcher.FindSupersetMatches(&plan.Components[0])
	if err != nil {
		t.Fatalf("find superset matches: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("expected public superset match API to return matches")
	}
}

func TestComponentMatcher_MinimumSavingsThreshold(t *testing.T) {
	t.Parallel()

	matcher := NewComponentMatcher()
	matcher.SetMinimumSavingsThreshold(10.0)
	matcher.SetPerformanceModel(&PerformanceModel{
		ComponentCosts: map[ComponentType]float64{
			FROM_COMPONENT: 1.0,
		},
	})
	_ = matcher.AddCachedComponent(&CachedComponent{
		Signature:     "FROM_users",
		ComponentType: FROM_COMPONENT,
	})

	plan := &QueryPlan{
		Components: []QueryComponent{
			{Type: FROM_COMPONENT, Signature: "FROM_users"},
		},
	}

	strategy, err := matcher.FindReusableComponents(plan)
	if err != nil {
		t.Fatalf("find reusable components: %v", err)
	}
	if len(strategy.ReusableComponents) != 0 {
		t.Fatalf("expected 0 reusable components below threshold, got %d", len(strategy.ReusableComponents))
	}
	if len(strategy.NewComponents) != 1 {
		t.Fatalf("expected 1 new component below threshold, got %d", len(strategy.NewComponents))
	}
}

func TestComponentMatcher_GeneratesMergePlanWhenPartiallyReusable(t *testing.T) {
	t.Parallel()

	matcher := NewComponentMatcher()
	_ = matcher.AddCachedComponent(&CachedComponent{
		Signature:     "JOIN_users_orders",
		ComponentType: JOIN_COMPONENT,
	})

	plan := &QueryPlan{
		Components: []QueryComponent{
			{Type: JOIN_COMPONENT, Signature: "JOIN_users_orders"},
			{Type: WHERE_COMPONENT, Signature: "WHERE_status_active"},
		},
	}

	strategy, err := matcher.FindReusableComponents(plan)
	if err != nil {
		t.Fatalf("find reusable components: %v", err)
	}
	if strategy.ExecutionPlan == nil {
		t.Fatal("expected execution plan")
	}
	if len(strategy.ExecutionPlan.Steps) != 3 {
		t.Fatalf("expected cached + compute + merge steps, got %d", len(strategy.ExecutionPlan.Steps))
	}
	if strategy.ExecutionPlan.Steps[0].Type != "cached" {
		t.Fatalf("expected first step to be cached, got %s", strategy.ExecutionPlan.Steps[0].Type)
	}
	if strategy.ExecutionPlan.Steps[1].Type != "compute" {
		t.Fatalf("expected second step to be compute, got %s", strategy.ExecutionPlan.Steps[1].Type)
	}
	if strategy.ExecutionPlan.Steps[2].Type != "merge" {
		t.Fatalf("expected third step to be merge, got %s", strategy.ExecutionPlan.Steps[2].Type)
	}
}
