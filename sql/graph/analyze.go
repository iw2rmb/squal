package graph

import (
	"fmt"
	"strings"

	"github.com/iw2rmb/squal/parser"
)

// FindDependencies finds queries that this query could depend on.
func (g *QueryGraph) FindDependencies(sql string) []QueryID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	tables, err := g.parser.ExtractTables(sql)
	if err != nil {
		tables = g.extractTables(sql)
	}

	return g.findPotentialDependencies(sql, tables)
}

// CanReuse checks if a cached query can be reused as a subquery.
func (g *QueryGraph) CanReuse(cachedID QueryID, newSQL string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	cachedNode, ok := g.nodes[cachedID]
	if !ok {
		return false
	}

	if cachedNode.Metadata != nil {
		_, err := g.parser.ExtractMetadata(newSQL)
		if err == nil {
			// Fall through to basic analysis.
		}
	}

	newTables := g.extractTables(newSQL)
	for _, table := range cachedNode.Tables {
		found := false
		for _, newTable := range newTables {
			if strings.EqualFold(string(table), newTable) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// FindReusableCachedQueries finds all cached queries that can potentially be reused.
func (g *QueryGraph) FindReusableCachedQueries(targetSQL string) []ReusableQuery {
	g.mu.RLock()
	defer g.mu.RUnlock()

	_, err := g.parser.ExtractMetadata(targetSQL)
	if err != nil {
		return []ReusableQuery{}
	}

	reusable := []ReusableQuery{}

	for _, node := range g.nodes {
		if node.Metadata == nil {
			continue
		}

		// Basic compatibility checking - removed legacy parser-specific logic.
		_ = node
	}

	for i := 0; i < len(reusable)-1; i++ {
		for j := 0; j < len(reusable)-i-1; j++ {
			if reusable[j].Confidence < reusable[j+1].Confidence {
				reusable[j], reusable[j+1] = reusable[j+1], reusable[j]
			}
		}
	}

	return reusable
}

// ReusableQuery represents a cached query that can be reused.
type ReusableQuery struct {
	ID         QueryID
	SQL        SQLText
	Confidence float64
	Metadata   *parser.QueryMetadata
}

// BuildDependencyChain builds a complete dependency chain for optimal execution.
func (g *QueryGraph) BuildDependencyChain(targetSQL string) *DependencyChain {
	g.mu.RLock()
	defer g.mu.RUnlock()

	chain := &DependencyChain{
		TargetSQL: targetSQL,
		Steps:     []DependencyStep{},
	}

	reusable := g.FindReusableCachedQueries(targetSQL)
	usedQueries := make(map[QueryID]bool)

	for _, rq := range reusable {
		if usedQueries[rq.ID] {
			continue
		}

		step := DependencyStep{
			QueryID:    rq.ID,
			SQL:        rq.SQL,
			Type:       "cached",
			Confidence: rq.Confidence,
		}

		chain.Steps = append(chain.Steps, step)
		usedQueries[rq.ID] = true

		if len(chain.Steps) >= 3 {
			break
		}
	}

	if len(chain.Steps) > 0 {
		chain.Steps = append(chain.Steps, DependencyStep{
			Type:    "compute",
			SQL:     SQLText(targetSQL),
			QueryID: "",
		})
	}

	return chain
}

// DependencyChain represents an execution plan using cached queries.
type DependencyChain struct {
	TargetSQL string
	Steps     []DependencyStep
}

// DependencyStep represents a step in the dependency chain.
type DependencyStep struct {
	QueryID    QueryID
	SQL        SQLText
	Type       string // "cached" or "compute"
	Confidence float64
}

// Stats returns graph statistics.
func (g *QueryGraph) Stats() map[string]interface{} {
	g.mu.RLock()
	defer g.mu.RUnlock()

	totalNodes := len(g.nodes)
	totalEdges := 0
	maxDependencies := 0
	maxDependents := 0

	for _, node := range g.nodes {
		totalEdges += len(node.Dependencies)
		if len(node.Dependencies) > maxDependencies {
			maxDependencies = len(node.Dependencies)
		}
		if len(node.Dependents) > maxDependents {
			maxDependents = len(node.Dependents)
		}
	}

	return map[string]interface{}{
		"total_queries":    totalNodes,
		"total_edges":      totalEdges,
		"max_dependencies": maxDependencies,
		"max_dependents":   maxDependents,
		"tables_tracked":   len(g.tableIndex),
	}
}

func (g *QueryGraph) extractTables(sql string) []string {
	tables := []string{}
	sql = strings.ToLower(sql)

	fromIndex := strings.Index(sql, "from ")
	if fromIndex == -1 {
		return tables
	}

	fromClause := sql[fromIndex+5:]
	endIndex := strings.IndexAny(fromClause, " where group order limit")
	if endIndex > 0 {
		fromClause = fromClause[:endIndex]
	}

	parts := strings.Split(fromClause, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if spaceIdx := strings.Index(part, " "); spaceIdx > 0 {
			part = part[:spaceIdx]
		}
		if part != "" {
			tables = append(tables, part)
		}
	}

	joinPattern := []string{"join ", "inner join ", "left join ", "right join "}
	for _, pattern := range joinPattern {
		idx := 0
		for {
			idx = strings.Index(sql[idx:], pattern)
			if idx == -1 {
				break
			}
			idx += len(pattern)
			remaining := sql[idx:]
			endIdx := strings.IndexAny(remaining, " on using")
			if endIdx > 0 {
				table := strings.TrimSpace(remaining[:endIdx])
				if spaceIdx := strings.Index(table, " "); spaceIdx > 0 {
					table = table[:spaceIdx]
				}
				tables = append(tables, table)
			}
		}
	}

	return tables
}

func (g *QueryGraph) findPotentialDependencies(sql string, tables []string) []QueryID {
	deps := []QueryID{}

	for id, node := range g.nodes {
		if g.couldBeSubquery(node, sql, tables) {
			deps = append(deps, id)
		}
	}

	return deps
}

func (g *QueryGraph) couldBeSubquery(node *QueryNode, sql string, tables []string) bool {
	if node.Metadata == nil {
		return g.basicCouldBeSubquery(node, tables)
	}

	_, err := g.parser.ExtractMetadata(sql)
	if err != nil {
		return false
	}

	// Basic compatibility checking - removed legacy parser-specific logic.
	return false
}

// basicCouldBeSubquery provides fallback logic when metadata is unavailable.
func (g *QueryGraph) basicCouldBeSubquery(node *QueryNode, tables []string) bool {
	for _, nodeTable := range node.Tables {
		found := false
		for _, table := range tables {
			if strings.EqualFold(string(nodeTable), table) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return len(node.Tables) > 0 && len(node.Tables) <= len(tables)
}

// Visualize returns a DOT graph representation.
func (g *QueryGraph) Visualize() string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var sb strings.Builder
	sb.WriteString("digraph QueryDependencies {\n")
	sb.WriteString("  rankdir=LR;\n")
	sb.WriteString("  node [shape=box];\n\n")

	for id, node := range g.nodes {
		label := fmt.Sprintf("%s\\n%d tables", id, len(node.Tables))
		sb.WriteString(fmt.Sprintf("  \"%s\" [label=\"%s\"];\n", id, label))
	}

	sb.WriteString("\n")

	for id, node := range g.nodes {
		_ = id
		for _, depID := range node.Dependencies {
			sb.WriteString(fmt.Sprintf("  \"%s\" -> \"%s\";\n", depID, id))
		}
	}

	sb.WriteString("}\n")
	return sb.String()
}
