package core

import (
	"encoding/json"
	"testing"
)

func TestJoinTypeString(t *testing.T) {
	tests := []struct {
		name     string
		joinType JoinType
		want     string
	}{
		{"inner join", JoinTypeInner, "INNER"},
		{"left join", JoinTypeLeft, "LEFT"},
		{"right join", JoinTypeRight, "RIGHT"},
		{"full join", JoinTypeFull, "FULL"},
		{"cross join", JoinTypeCross, "CROSS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.joinType.String(); got != tt.want {
				t.Errorf("JoinType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompareOpString(t *testing.T) {
	tests := []struct {
		name      string
		compareOp CompareOp
		want      string
	}{
		{"equal", CompareOpEqual, "="},
		{"not equal", CompareOpNotEqual, "!="},
		{"less than", CompareOpLessThan, "<"},
		{"less than or equal", CompareOpLessThanEqual, "<="},
		{"greater than", CompareOpGreaterThan, ">"},
		{"greater than or equal", CompareOpGreaterThanEqual, ">="},
		{"like", CompareOpLike, "LIKE"},
		{"not like", CompareOpNotLike, "NOT LIKE"},
		{"ilike", CompareOpILike, "ILIKE"},
		{"not ilike", CompareOpNotILike, "NOT ILIKE"},
		{"in", CompareOpIn, "IN"},
		{"not in", CompareOpNotIn, "NOT IN"},
		{"is", CompareOpIs, "IS"},
		{"is not", CompareOpIsNot, "IS NOT"},
		{"between", CompareOpBetween, "BETWEEN"},
		{"not between", CompareOpNotBetween, "NOT BETWEEN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.compareOp.String(); got != tt.want {
				t.Errorf("CompareOp.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJoinTypeJSON(t *testing.T) {
	tests := []struct {
		name     string
		joinType JoinType
		wantJSON string
	}{
		{"inner join", JoinTypeInner, `"INNER"`},
		{"left join", JoinTypeLeft, `"LEFT"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.joinType)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			if string(got) != tt.wantJSON {
				t.Errorf("json.Marshal() = %v, want %v", string(got), tt.wantJSON)
			}

			var joinType JoinType
			if err := json.Unmarshal(got, &joinType); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if joinType != tt.joinType {
				t.Errorf("json.Unmarshal() = %v, want %v", joinType, tt.joinType)
			}
		})
	}
}

func TestCompareOpJSON(t *testing.T) {
	tests := []struct {
		name      string
		compareOp CompareOp
		wantJSON  string
	}{
		{"equal", CompareOpEqual, `"="`},
		{"in", CompareOpIn, `"IN"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.compareOp)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			if string(got) != tt.wantJSON {
				t.Errorf("json.Marshal() = %v, want %v", string(got), tt.wantJSON)
			}

			var compareOp CompareOp
			if err := json.Unmarshal(got, &compareOp); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if compareOp != tt.compareOp {
				t.Errorf("json.Unmarshal() = %v, want %v", compareOp, tt.compareOp)
			}
		})
	}
}
