package core

// JoinType represents the type of SQL JOIN operation.
type JoinType string

// Join type constants.
const (
	JoinTypeInner JoinType = "INNER"
	JoinTypeLeft  JoinType = "LEFT"
	JoinTypeRight JoinType = "RIGHT"
	JoinTypeFull  JoinType = "FULL"
	JoinTypeCross JoinType = "CROSS"
)

// String returns the string representation of the join type.
func (j JoinType) String() string {
	return string(j)
}

// CompareOp represents a SQL comparison operator.
type CompareOp string

// Comparison operator constants.
const (
	CompareOpEqual            CompareOp = "="
	CompareOpNotEqual         CompareOp = "!="
	CompareOpLessThan         CompareOp = "<"
	CompareOpLessThanEqual    CompareOp = "<="
	CompareOpGreaterThan      CompareOp = ">"
	CompareOpGreaterThanEqual CompareOp = ">="
	CompareOpLike             CompareOp = "LIKE"
	CompareOpNotLike          CompareOp = "NOT LIKE"
	CompareOpILike            CompareOp = "ILIKE"
	CompareOpNotILike         CompareOp = "NOT ILIKE"
	CompareOpIn               CompareOp = "IN"
	CompareOpNotIn            CompareOp = "NOT IN"
	CompareOpIs               CompareOp = "IS"
	CompareOpIsNot            CompareOp = "IS NOT"
	CompareOpBetween          CompareOp = "BETWEEN"
	CompareOpNotBetween       CompareOp = "NOT BETWEEN"
)

// String returns the string representation of the comparison operator.
func (c CompareOp) String() string {
	return string(c)
}
