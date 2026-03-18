package graph

import "fmt"

// AddQuery adds a query to the graph.
func (g *QueryGraph) AddQuery(id QueryID, sql SQLText) (*QueryNode, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	metadata, err := g.parser.ExtractMetadata(string(sql))
	if err != nil {
		return nil, fmt.Errorf("failed to parse query metadata: %w", err)
	}
	if metadata == nil {
		return nil, fmt.Errorf("failed to parse query metadata: parser returned nil metadata")
	}

	fingerprint, err := g.parser.GenerateFingerprint(string(sql))
	if err != nil {
		return nil, fmt.Errorf("failed to generate query fingerprint: %w", err)
	}

	tables := make([]TableName, len(metadata.Tables))
	for i, t := range metadata.Tables {
		tables[i] = TableName(t)
	}

	deps := g.findPotentialDependencies(string(sql), metadata.Tables)

	node := &QueryNode{
		ID:           id,
		SQL:          sql,
		Fingerprint:  Fingerprint(fingerprint),
		Tables:       tables,
		Dependencies: deps,
		Dependents:   []QueryID{},
		Metadata:     metadata,
	}

	g.nodes[id] = node

	for _, table := range tables {
		g.tableIndex[table] = append(g.tableIndex[table], id)
	}

	for _, depID := range deps {
		if depNode, ok := g.nodes[depID]; ok {
			depNode.Dependents = append(depNode.Dependents, id)
		}
	}

	return node, nil
}

// RemoveQuery removes a query from the graph.
func (g *QueryGraph) RemoveQuery(id QueryID) {
	g.mu.Lock()
	defer g.mu.Unlock()

	node, ok := g.nodes[id]
	if !ok {
		return
	}

	for _, table := range node.Tables {
		if queries, ok := g.tableIndex[table]; ok {
			g.removeFromSlice(&queries, id)
			g.tableIndex[table] = queries
		}
	}

	for _, depID := range node.Dependencies {
		if depNode, ok := g.nodes[depID]; ok {
			g.removeFromSlice(&depNode.Dependents, id)
		}
	}

	for _, depID := range node.Dependents {
		if depNode, ok := g.nodes[depID]; ok {
			g.removeFromSlice(&depNode.Dependencies, id)
		}
	}

	delete(g.nodes, id)
}

func (g *QueryGraph) removeFromSlice(slice *[]QueryID, value QueryID) {
	for i, v := range *slice {
		if v == value {
			*slice = append((*slice)[:i], (*slice)[i+1:]...)
			return
		}
	}
}
