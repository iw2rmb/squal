package graph

import (
	"slices"
	"testing"
)

func TestQueryGraph_NewQueryGraphPanicsWithoutParser(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic from NewQueryGraph without parser injection")
		}
	}()

	_ = NewQueryGraph()
}

func TestQueryGraph_AddRemoveQueryAndTableIndex(t *testing.T) {
	t.Parallel()

	g := NewQueryGraphWithParser(&mockParser{})

	if _, err := g.AddQuery(QueryID("q-users"), SQLText("SELECT id FROM users")); err != nil {
		t.Fatalf("AddQuery(users) failed: %v", err)
	}
	if _, err := g.AddQuery(QueryID("q-orders"), SQLText("SELECT id FROM orders")); err != nil {
		t.Fatalf("AddQuery(orders) failed: %v", err)
	}

	users := g.FindQueriesByTable(TableName("users"))
	if len(users) != 1 || users[0] != QueryID("q-users") {
		t.Fatalf("unexpected users table index: %+v", users)
	}

	orders := g.FindQueriesByTable(TableName("orders"))
	if len(orders) != 1 || orders[0] != QueryID("q-orders") {
		t.Fatalf("unexpected orders table index: %+v", orders)
	}

	orders[0] = QueryID("mutated")
	stillOrders := g.FindQueriesByTable(TableName("orders"))
	if len(stillOrders) != 1 || stillOrders[0] != QueryID("q-orders") {
		t.Fatalf("FindQueriesByTable must return a copy, got %+v", stillOrders)
	}

	g.RemoveQuery(QueryID("q-users"))
	usersAfterRemove := g.FindQueriesByTable(TableName("users"))
	if len(usersAfterRemove) != 0 {
		t.Fatalf("expected users index to be empty after remove, got %+v", usersAfterRemove)
	}
}

func TestQueryGraph_GetDependencyChain_DepthFirstDeduplicated(t *testing.T) {
	t.Parallel()

	g := NewQueryGraphWithParser(&mockParser{})
	g.nodes = map[QueryID]*QueryNode{
		"q1": {ID: "q1", Dependencies: []QueryID{"q3"}},
		"q2": {ID: "q2", Dependencies: []QueryID{"q1"}},
		"q3": {ID: "q3", Dependencies: []QueryID{"q2"}},
	}

	chain := g.GetDependencyChain(QueryID("q3"))
	if !slices.Equal(chain, []QueryID{"q1", "q2", "q3"}) {
		t.Fatalf("unexpected dependency chain: %+v", chain)
	}
}

func TestAffectedQueries_IncludeTransitiveDependents(t *testing.T) {
	t.Parallel()

	g := NewQueryGraphWithParser(&mockParser{})
	g.nodes = map[QueryID]*QueryNode{
		"base": {
			ID:           "base",
			Tables:       []TableName{"users"},
			Dependencies: []QueryID{},
			Dependents:   []QueryID{"mid"},
		},
		"mid": {
			ID:           "mid",
			Tables:       []TableName{"users"},
			Dependencies: []QueryID{"base"},
			Dependents:   []QueryID{"leaf"},
		},
		"leaf": {
			ID:           "leaf",
			Tables:       []TableName{"users"},
			Dependencies: []QueryID{"mid"},
			Dependents:   []QueryID{},
		},
	}
	g.tableIndex = map[TableName][]QueryID{
		"users": {"base"},
	}

	affected := g.FindAffectedQueries(TableName("users"), "UPDATE")
	if !sameIDs(affected, []QueryID{"base", "mid", "leaf"}) {
		t.Fatalf("unexpected affected query IDs: %+v", affected)
	}
}

func TestQueryGraph_ExtractTables_MultipleJoins(t *testing.T) {
	t.Parallel()

	g := NewQueryGraphWithParser(&mockParser{})
	tables := g.extractTables("SELECT * FROM a JOIN b ON a.id=b.id JOIN c ON c.id=b.id")
	if !sameElements(tables, []string{"a", "b", "c"}) {
		t.Fatalf("unexpected tables from join fallback parser: %+v", tables)
	}
}

func sameIDs(got []QueryID, want []QueryID) bool {
	return sameElements(got, want)
}

func sameElements[T comparable](got []T, want []T) bool {
	if len(got) != len(want) {
		return false
	}
	set := make(map[T]int, len(got))
	for _, v := range got {
		set[v]++
	}
	for _, v := range want {
		count := set[v]
		if count == 0 {
			return false
		}
		set[v] = count - 1
	}
	for _, count := range set {
		if count != 0 {
			return false
		}
	}
	return true
}
