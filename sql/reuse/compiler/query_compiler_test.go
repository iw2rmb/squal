package compiler

import (
	"testing"
	"time"
)

func TestQueryCompiler_BasicCompilation(t *testing.T) {
	t.Parallel()

	qc := NewQueryCompiler()
	compiled, err := qc.CompileQuery("SELECT user_id, name FROM users WHERE age > 18")
	if err != nil {
		t.Fatalf("compile query: %v", err)
	}
	if compiled == nil {
		t.Fatal("expected compiled query")
	}
	if compiled.ExecutionPlan == nil {
		t.Fatal("expected execution plan")
	}
	if len(compiled.ExecutionPlan.Steps) == 0 {
		t.Fatal("expected non-empty execution plan")
	}
	if compiled.PerformanceGain == nil {
		t.Fatal("expected performance gain")
	}
	if compiled.ComponentsReused < 0 {
		t.Fatalf("expected non-negative reused components, got %d", compiled.ComponentsReused)
	}
}

func TestQueryCompiler_CachesAndReusesComponents(t *testing.T) {
	t.Parallel()

	qc := NewQueryCompiler()

	first, err := qc.CompileQuery("SELECT * FROM users WHERE city_id = 1")
	if err != nil {
		t.Fatalf("compile first query: %v", err)
	}
	if err := qc.CacheComponents(first); err != nil {
		t.Fatalf("cache components: %v", err)
	}

	second, err := qc.CompileQuery("SELECT name, email FROM users WHERE city_id = 1")
	if err != nil {
		t.Fatalf("compile second query: %v", err)
	}
	if second.ComponentsReused == 0 {
		t.Fatal("expected cached component reuse")
	}
	if second.PerformanceGain == nil || second.PerformanceGain.SavedCost <= 0 {
		t.Fatal("expected positive performance gain")
	}
}

func TestQueryCompiler_ThresholdBehavior(t *testing.T) {
	t.Parallel()

	t.Run("skip_below_threshold", func(t *testing.T) {
		qc := NewQueryCompiler()
		qc.SetMinimumSavingsThreshold(100.0)

		compiled, err := qc.CompileQuery("SELECT * FROM users")
		if err != nil {
			t.Fatalf("compile query: %v", err)
		}
		if compiled.WasOptimized {
			t.Fatal("expected optimization to be skipped")
		}
		if compiled.ComponentsReused != 0 {
			t.Fatalf("expected no reused components, got %d", compiled.ComponentsReused)
		}
	})

	t.Run("optimize_above_threshold", func(t *testing.T) {
		qc := NewQueryCompiler()

		seed, err := qc.CompileQuery("SELECT u.name FROM users u WHERE u.active = true")
		if err != nil {
			t.Fatalf("compile seed query: %v", err)
		}
		if err := qc.CacheComponents(seed); err != nil {
			t.Fatalf("cache seed query: %v", err)
		}
		qc.SetMinimumSavingsThreshold(0.1)

		compiled, err := qc.CompileQuery("SELECT u.email FROM users u WHERE u.active = true")
		if err != nil {
			t.Fatalf("compile query: %v", err)
		}
		if !compiled.WasOptimized {
			t.Fatal("expected optimization to be enabled")
		}
		if compiled.ComponentsReused == 0 {
			t.Fatal("expected reused components above threshold")
		}
	})
}

func TestQueryCompiler_ValidationErrors(t *testing.T) {
	t.Parallel()

	qc := NewQueryCompiler()
	cases := []struct {
		name string
		sql  string
	}{
		{name: "empty_sql", sql: ""},
		{name: "obviously_invalid_select", sql: "SELECT FROM WHERE"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := qc.CompileQuery(tc.sql); err == nil {
				t.Fatalf("expected error for %q", tc.sql)
			}
		})
	}
}

func TestQueryCompiler_ExpireCacheRemovesReusableComponents(t *testing.T) {
	t.Parallel()

	qc := NewQueryCompiler()
	qc.SetCacheTTL(1)

	compiled, err := qc.CompileQuery("SELECT * FROM users")
	if err != nil {
		t.Fatalf("compile query: %v", err)
	}
	if err := qc.CacheComponents(compiled); err != nil {
		t.Fatalf("cache components: %v", err)
	}

	qc.ExpireCache()

	next, err := qc.CompileQuery("SELECT * FROM users")
	if err != nil {
		t.Fatalf("compile query after expiration: %v", err)
	}
	if next.ComponentsReused != 0 {
		t.Fatalf("expected no component reuse after expiration, got %d", next.ComponentsReused)
	}
}

func TestQueryCompiler_LoadComponentsFromStorageAndDeduplicates(t *testing.T) {
	t.Parallel()

	seedCompiler := NewQueryCompiler()
	seed, err := seedCompiler.CompileQuery("SELECT user_id, SUM(amount) FROM payments GROUP BY user_id")
	if err != nil {
		t.Fatalf("compile seed query: %v", err)
	}
	if len(seed.components) == 0 {
		t.Fatal("expected seed components")
	}

	now := time.Now()
	stored := make([]*CachedComponent, 0, len(seed.components)+1)
	for i, component := range seed.components {
		stored = append(stored, &CachedComponent{
			Signature:     component.Signature,
			ComponentType: component.Type,
			CacheTime:     now,
			ValidUntil:    now.Add(time.Hour),
		})
		if i == 0 {
			stored = append(stored, &CachedComponent{
				Signature:     component.Signature, // duplicate signature should be ignored
				ComponentType: component.Type,
				CacheTime:     now,
				ValidUntil:    now.Add(time.Hour),
			})
		}
	}

	qc := NewQueryCompiler()
	qc.SetStorage(staticComponentStorage{items: stored})
	qc.LoadComponentsFromStorage()

	uniqueCount := make(map[string]struct{}, len(seed.components))
	for _, component := range seed.components {
		uniqueCount[component.Signature] = struct{}{}
	}

	if got := qc.GetCachedComponentCount(); got != len(uniqueCount) {
		t.Fatalf("expected %d unique cached components, got %d", len(uniqueCount), got)
	}

	compiled, err := qc.CompileQuery("SELECT user_id, SUM(amount) FROM payments GROUP BY user_id")
	if err != nil {
		t.Fatalf("compile query: %v", err)
	}
	if compiled.ComponentsReused == 0 {
		t.Fatal("expected component reuse from storage-synced cache")
	}
}

type staticComponentStorage struct {
	items []*CachedComponent
	err   error
}

func (s staticComponentStorage) GetAllStoredComponents() ([]*CachedComponent, error) {
	return s.items, s.err
}
