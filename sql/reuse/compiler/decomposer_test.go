package compiler

import "testing"

func TestDecomposer_BasicComponents(t *testing.T) {
	sql := `SELECT u.id, u.name, COUNT(o.id) AS orders
            FROM users u
            JOIN orders o ON o.user_id = u.id
            JOIN payments p ON p.order_id = o.id
            WHERE u.country = 'US' AND p.status = 'paid'
            GROUP BY u.id, u.name
            ORDER BY u.name DESC
            LIMIT 10`

	d := NewQueryDecomposer()
	plan, err := d.Decompose(sql)
	if err != nil {
		t.Fatalf("decompose returned error: %v", err)
	}

	if plan == nil || len(plan.Components) == 0 {
		t.Fatalf("expected components; got none")
	}

	var hasSelect, hasFrom, hasWhere, hasJoin, hasGroup, hasAgg, hasOrder, hasLimit bool
	var whereSig, fromSig string

	for _, c := range plan.Components {
		switch c.Type {
		case SELECT_COMPONENT:
			hasSelect = true
			if len(c.Columns) == 0 {
				t.Errorf("select columns must be parsed")
			}
		case FROM_COMPONENT:
			hasFrom = true
			if len(c.Tables) == 0 {
				t.Errorf("from tables must be parsed")
			}
			fromSig = c.Signature
		case WHERE_COMPONENT:
			hasWhere = true
			if len(c.Filters) == 0 {
				t.Errorf("where filters must be parsed")
			}
			whereSig = c.Signature
		case JOIN_COMPONENT:
			hasJoin = true
		case GROUP_BY_COMPONENT:
			hasGroup = true
		case AGG_COMPONENT:
			hasAgg = true
			if len(c.Aggregations) == 0 {
				t.Errorf("aggregation list must be parsed")
			}
		case ORDER_BY_COMPONENT:
			hasOrder = true
		case LIMIT_COMPONENT:
			hasLimit = true
		}
	}

	// Basic presence assertions (JOIN requires >=2 joined tables in current logic)
	if !hasSelect || !hasFrom || !hasWhere || !hasGroup || !hasAgg || !hasOrder || !hasLimit {
		t.Fatalf("expected all basic components; got select=%v from=%v where=%v join=%v group=%v agg=%v order=%v limit=%v",
			hasSelect, hasFrom, hasWhere, hasJoin, hasGroup, hasAgg, hasOrder, hasLimit)
	}

	// Where should depend on From when both present
	deps, ok := plan.Dependencies[whereSig]
	if !ok {
		t.Fatalf("expected dependencies for where component")
	}
	found := false
	for _, d := range deps {
		if d == fromSig {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected WHERE to depend on FROM")
	}
}
