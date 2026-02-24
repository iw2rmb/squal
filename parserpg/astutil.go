//go:build cgo
// +build cgo

package parserpg

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// What: Small AST helpers shared across domains.
// How: Keeps leaf utilities (name extraction, const decoding) decoupled and reusable.

type colNames struct{ table, column string }

// columnRefToNames extracts table and column from a Node if it is a ColumnRef.
func (p *PGQueryParser) columnRefToNames(node *pg_query.Node) (colNames, bool) {
	if node == nil {
		return colNames{}, false
	}
	if cr := node.GetColumnRef(); cr != nil && len(cr.Fields) > 0 {
		var table, column string
		if len(cr.Fields) == 1 {
			if s := cr.Fields[0].GetString_(); s != nil {
				column = s.Sval
			}
		} else {
			if s := cr.Fields[0].GetString_(); s != nil {
				table = s.Sval
			}
			if s := cr.Fields[len(cr.Fields)-1].GetString_(); s != nil {
				column = s.Sval
			}
		}
		if column != "" {
			return colNames{table: table, column: column}, true
		}
	}
	return colNames{}, false
}

// extractColumnFromRef extracts column and table names from a ColumnRef node.
func (p *PGQueryParser) extractColumnFromRef(cr *pg_query.ColumnRef) (column, table string) {
	if cr == nil {
		return "", ""
	}
	node := &pg_query.Node{Node: &pg_query.Node_ColumnRef{ColumnRef: cr}}
	names, ok := p.columnRefToNames(node)
	if !ok {
		return "", ""
	}
	return names.column, names.table
}

// extractColumnFromRefNode extracts column and table names from a node if it's a ColumnRef.
func (p *PGQueryParser) extractColumnFromRefNode(node *pg_query.Node) (column, table string) {
	if node == nil {
		return "", ""
	}
	cr := node.GetColumnRef()
	if cr == nil {
		return "", ""
	}
	return p.extractColumnFromRef(cr)
}

// constNodeToValue converts a Node to a Go value if it is a constant.
func (p *PGQueryParser) constNodeToValue(node *pg_query.Node) (interface{}, bool) {
	if node == nil {
		return nil, false
	}
	if c := node.GetAConst(); c != nil {
		if v := c.GetSval(); v != nil {
			return v.Sval, true
		}
		if v := c.GetIval(); v != nil {
			return int64(v.Ival), true
		}
		if v := c.GetFval(); v != nil {
			// Keep numeric constants as string for equality consistency
			return v.Fval, true
		}
		if v := c.GetBoolval(); v != nil {
			return v.Boolval, true
		}
		if c.GetIsnull() {
			return nil, true
		}
	}
	return nil, false
}
