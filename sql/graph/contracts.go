package graph

import "github.com/iw2rmb/squal/parser"

// QueryID identifies a query node in the dependency graph.
type QueryID string

// SQLText stores canonical query SQL associated with a graph node.
type SQLText string

// Fingerprint stores a normalized semantic fingerprint for a query.
type Fingerprint string

// TableName identifies a table used for dependency indexing.
type TableName string

// QueryNode is the public graph node contract.
type QueryNode struct {
	ID           QueryID               `json:"id"`
	SQL          SQLText               `json:"sql"`
	Fingerprint  Fingerprint           `json:"fingerprint"`
	Tables       []TableName           `json:"tables"`
	Dependencies []QueryID             `json:"dependencies"`
	Dependents   []QueryID             `json:"dependents"`
	Metadata     *parser.QueryMetadata `json:"metadata,omitempty"`
}

// QueryGraph defines parser-injected graph behavior consumed by host runtimes.
type QueryGraph interface {
	GetParser() parser.Parser
	AddQuery(id QueryID, sql SQLText) (*QueryNode, error)
	RemoveQuery(id QueryID)
	GetNode(id QueryID) *QueryNode
	GetDependencyChain(id QueryID) []QueryID
	FindAffectedQueries(table TableName, operation string) []QueryID
	FindQueriesByTable(table TableName) []QueryID
}

// Builder defines parser-injected graph construction.
//
// Implementations must require parser injection and avoid hidden global parser state.
type Builder interface {
	NewQueryGraphWithParser(p parser.Parser) QueryGraph
}
