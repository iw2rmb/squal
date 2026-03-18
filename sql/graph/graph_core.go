package graph

import (
	"sync"

	"github.com/iw2rmb/squal/parser"
)

// QueryGraph stores queries, their dependencies, and table indexes.
type QueryGraph struct {
	mu     sync.RWMutex
	nodes  map[QueryID]*QueryNode
	parser parser.Parser

	// Index for quick lookups.
	tableIndex map[TableName][]QueryID // table -> query IDs
}

// GetParser returns the injected SQL parser used by the graph.
func (g *QueryGraph) GetParser() parser.Parser {
	return g.parser
}

// NewQueryGraphWithParser creates a query dependency graph with parser injection.
func NewQueryGraphWithParser(p parser.Parser) *QueryGraph {
	return &QueryGraph{
		nodes:      make(map[QueryID]*QueryNode),
		tableIndex: make(map[TableName][]QueryID),
		parser:     p,
	}
}

// NewQueryGraph panics intentionally. Parser injection is mandatory.
func NewQueryGraph() *QueryGraph {
	panic("NewQueryGraph() requires parser injection; use NewQueryGraphWithParser(p parser.Parser)")
}

// GetNode returns the node for the given ID or nil if absent.
func (g *QueryGraph) GetNode(id QueryID) *QueryNode {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.nodes[id]
}

// GetDependencyChain returns a depth-first, duplicate-free list for the root ID.
func (g *QueryGraph) GetDependencyChain(id QueryID) []QueryID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	visited := make(map[QueryID]bool)
	chain := []QueryID{}

	g.collectDependencies(id, visited, &chain)
	return chain
}

// collectDependencies performs DFS to build an ordered chain.
func (g *QueryGraph) collectDependencies(id QueryID, visited map[QueryID]bool, chain *[]QueryID) {
	if visited[id] {
		return
	}
	visited[id] = true

	if node, ok := g.nodes[id]; ok {
		for _, depID := range node.Dependencies {
			g.collectDependencies(depID, visited, chain)
		}
		*chain = append(*chain, id)
	}
}
