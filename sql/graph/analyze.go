package graph

import (
	"fmt"
	"sort"
	"strings"

	"github.com/iw2rmb/squal/parser"
)

// FindDependencies finds queries that this query could depend on.
func (g *QueryGraph) FindDependencies(sql string) []QueryID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	targetMetadata, tables := g.extractTargetProfile(sql)

	return g.findPotentialDependencies(tables, targetMetadata)
}

// CanReuse checks if a cached query can be reused as a subquery.
func (g *QueryGraph) CanReuse(cachedID QueryID, newSQL string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	cachedNode, ok := g.nodes[cachedID]
	if !ok {
		return false
	}

	targetMetadata, targetTables := g.extractTargetProfile(newSQL)
	return g.couldBeSubquery(cachedNode, targetTables, targetMetadata)
}

// FindReusableCachedQueries finds all cached queries that can potentially be reused.
func (g *QueryGraph) FindReusableCachedQueries(targetSQL string) []ReusableQuery {
	g.mu.RLock()
	defer g.mu.RUnlock()

	targetMetadata, targetTables := g.extractTargetProfile(targetSQL)
	if len(targetTables) == 0 {
		return []ReusableQuery{}
	}

	reusable := []ReusableQuery{}

	for _, node := range g.nodes {
		if !g.couldBeSubquery(node, targetTables, targetMetadata) {
			continue
		}

		reusable = append(reusable, ReusableQuery{
			ID:         node.ID,
			SQL:        node.SQL,
			Confidence: g.reuseConfidence(node, targetMetadata),
			Metadata:   node.Metadata,
		})
	}

	sort.Slice(reusable, func(i, j int) bool {
		if reusable[i].Confidence == reusable[j].Confidence {
			return reusable[i].ID < reusable[j].ID
		}
		return reusable[i].Confidence > reusable[j].Confidence
	})

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
		searchStart := 0
		for {
			relIdx := strings.Index(sql[searchStart:], pattern)
			if relIdx == -1 {
				break
			}
			joinTableStart := searchStart + relIdx + len(pattern)
			remaining := sql[joinTableStart:]
			endIdx := strings.IndexAny(remaining, " on using")
			if endIdx > 0 {
				table := strings.TrimSpace(remaining[:endIdx])
				if spaceIdx := strings.Index(table, " "); spaceIdx > 0 {
					table = table[:spaceIdx]
				}
				tables = append(tables, table)
			}
			searchStart = joinTableStart
		}
	}

	return tables
}

func (g *QueryGraph) findPotentialDependencies(tables []string, targetMetadata *parser.QueryMetadata) []QueryID {
	deps := []QueryID{}

	for id, node := range g.nodes {
		if g.couldBeSubquery(node, tables, targetMetadata) {
			deps = append(deps, id)
		}
	}

	sort.Slice(deps, func(i, j int) bool {
		return deps[i] < deps[j]
	})

	return deps
}

func (g *QueryGraph) couldBeSubquery(node *QueryNode, tables []string, targetMetadata *parser.QueryMetadata) bool {
	if !g.basicCouldBeSubquery(node, tables) {
		return false
	}

	if node.Metadata == nil || targetMetadata == nil {
		return true
	}

	return operationsCompatible(node.Metadata, targetMetadata)
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

func (g *QueryGraph) extractTargetProfile(sql string) (*parser.QueryMetadata, []string) {
	metadata, err := g.parser.ExtractMetadata(sql)
	if err == nil && metadata != nil && len(metadata.Tables) > 0 {
		return metadata, metadata.Tables
	}

	tables, err := g.parser.ExtractTables(sql)
	if err != nil || len(tables) == 0 {
		tables = g.extractTables(sql)
	}

	if metadata != nil && len(metadata.Tables) == 0 {
		metadata.Tables = tables
	}

	return metadata, tables
}

func (g *QueryGraph) reuseConfidence(node *QueryNode, targetMetadata *parser.QueryMetadata) float64 {
	confidence := 0.7
	if node.Metadata == nil || targetMetadata == nil {
		return confidence
	}

	if operationsCompatible(node.Metadata, targetMetadata) {
		confidence += 0.2
	}
	if node.Metadata.IsAggregate == targetMetadata.IsAggregate {
		confidence += 0.1
	}
	if confidence > 1.0 {
		confidence = 1.0
	}
	return confidence
}

func operationsCompatible(left, right *parser.QueryMetadata) bool {
	if left == nil || right == nil {
		return true
	}
	if len(left.Operations) == 0 || len(right.Operations) == 0 {
		return true
	}
	return strings.EqualFold(left.Operations[0], right.Operations[0])
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
