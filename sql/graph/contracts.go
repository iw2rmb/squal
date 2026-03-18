package graph

import "github.com/iw2rmb/squall/parser"

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

// Graph defines parser-injected graph behavior consumed by host runtimes.
type Graph interface {
	GetParser() parser.Parser
	AddQuery(id QueryID, sql SQLText) (*QueryNode, error)
	RemoveQuery(id QueryID)
	GetNode(id QueryID) *QueryNode
	GetDependencyChain(id QueryID) []QueryID
	FindAffectedQueries(table TableName, operation string) []QueryID
	FindQueriesByTable(table TableName) []QueryID
	FindDependencies(sql string) []QueryID
	CanReuse(cachedID QueryID, newSQL string) bool
	FindReusableCachedQueries(targetSQL string) []ReusableQuery
	BuildDependencyChain(targetSQL string) *DependencyChain
	Stats() map[string]interface{}
	Visualize() string
}
