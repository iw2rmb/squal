//go:build cgo
// +build cgo

package parserpg

import (
	"regexp"
	"sort"
	"strings"

	"github.com/iw2rmb/squall/parser"
)

var (
	fromTableRE  = regexp.MustCompile(`(?i)\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	arrowChainRE = regexp.MustCompile(`(?i)(?:\b([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)(\s*(?:->>|->)\s*'[^']*')+`)
	arrowStepRE  = regexp.MustCompile(`(?i)(->>|->)\s*'([^']*)'`)
	hashPathRE   = regexp.MustCompile(`(?i)(?:\b([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*(#>>|#>)\s*'\{([^}]*)\}'`)
	aggregateRE  = regexp.MustCompile(`(?i)\b(count|sum|avg|min|max|array_agg|string_agg|json_agg|jsonb_agg)\s*\(`)
)

type positionedPath struct {
	start int
	path  parser.JSONPath
}

// normalizeMetadataCompatibility keeps parserpg metadata behavior compatible with
// historic Mill parser adapter behavior.
func (p *PGQueryParser) normalizeMetadataCompatibility(sql string, md *parser.QueryMetadata) *parser.QueryMetadata {
	if md == nil {
		return nil
	}

	if len(md.Operations) == 0 {
		if op := operationFromSQL(sql); op != "" {
			md.Operations = []string{op}
		}
	}

	if md.IsAggregate {
		return md
	}

	// Fallback checks for parser versions that omit IsAggregate for composed
	// aggregate expressions while still exposing parsed aggregate structures.
	if aggs, err := p.ExtractAggregates(sql); err == nil && len(aggs) > 0 {
		md.IsAggregate = true
		return md
	}
	if cases, err := p.ExtractCaseAggregates(sql); err == nil && len(cases) > 0 {
		md.IsAggregate = true
		return md
	}
	if comps, err := p.ExtractAggregateCompositions(sql); err == nil && len(comps) > 0 {
		md.IsAggregate = true
		return md
	}
	if aggregateRE.MatchString(sql) {
		md.IsAggregate = true
		return md
	}

	return md
}

func operationFromSQL(sql string) string {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	switch {
	case strings.HasPrefix(upperSQL, "SELECT"),
		strings.HasPrefix(upperSQL, "WITH"):
		return "SELECT"
	case strings.HasPrefix(upperSQL, "INSERT"):
		return "INSERT"
	case strings.HasPrefix(upperSQL, "UPDATE"):
		return "UPDATE"
	case strings.HasPrefix(upperSQL, "DELETE"):
		return "DELETE"
	default:
		return ""
	}
}

func normalizeJSONPaths(sql string, parsed []parser.JSONPath) []parser.JSONPath {
	fallback := extractJSONPathsFallback(sql)
	if len(fallback) == 0 {
		return parsed
	}
	if len(parsed) == 0 {
		return fallback
	}

	normalized := make([]parser.JSONPath, len(parsed))
	copy(normalized, parsed)
	usedFallback := make([]bool, len(fallback))

	for i := range normalized {
		match := bestFallbackMatch(normalized[i], fallback, usedFallback)
		if match < 0 {
			continue
		}
		usedFallback[match] = true
		candidate := fallback[match]

		if shouldReplacePath(normalized[i].Path, candidate.Path) {
			normalized[i].Path = candidate.Path
		}
		if normalized[i].Column == "" {
			normalized[i].Column = candidate.Column
		}
		if normalized[i].Table == "" {
			normalized[i].Table = candidate.Table
		}
		if normalized[i].Operator == "" {
			normalized[i].Operator = candidate.Operator
		}
		if candidate.IsText {
			normalized[i].IsText = true
		}
	}

	// Preserve parser-reported entries and append truly missing fallback entries.
	for i, fp := range fallback {
		if !usedFallback[i] {
			normalized = append(normalized, fp)
		}
	}

	return normalized
}

func shouldReplacePath(current, candidate []string) bool {
	if len(current) == 0 {
		return len(candidate) > 0
	}
	if len(candidate) <= len(current) {
		return false
	}
	return isPrefix(current, candidate)
}

func isPrefix(prefix, full []string) bool {
	if len(prefix) > len(full) {
		return false
	}
	for i := range prefix {
		if prefix[i] != full[i] {
			return false
		}
	}
	return true
}

func bestFallbackMatch(parsed parser.JSONPath, fallback []parser.JSONPath, used []bool) int {
	bestIdx := -1
	bestScore := -1

	for i, candidate := range fallback {
		if used[i] {
			continue
		}
		score := 0

		if parsed.Column != "" {
			if !strings.EqualFold(parsed.Column, candidate.Column) {
				continue
			}
			score += 4
		}
		if parsed.Operator != "" {
			if parsed.Operator != candidate.Operator {
				continue
			}
			score += 2
		}
		if len(parsed.Path) == 0 {
			if len(candidate.Path) > 0 {
				score++
			}
		} else if isPrefix(parsed.Path, candidate.Path) {
			score++
			if len(candidate.Path) > len(parsed.Path) {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}

	if bestScore <= 0 {
		return -1
	}
	return bestIdx
}

func extractJSONPathsFallback(sql string) []parser.JSONPath {
	defaultTable := ""
	if matches := fromTableRE.FindStringSubmatch(sql); len(matches) > 1 {
		defaultTable = strings.TrimSpace(matches[1])
	}

	var collected []positionedPath

	for _, m := range hashPathRE.FindAllStringSubmatchIndex(sql, -1) {
		table := submatch(sql, m, 1)
		column := submatch(sql, m, 2)
		operator := submatch(sql, m, 3)
		rawPath := submatch(sql, m, 4)
		if table == "" {
			table = defaultTable
		}
		path := splitJSONPathList(rawPath)
		if column == "" || len(path) == 0 {
			continue
		}
		collected = append(collected, positionedPath{
			start: m[0],
			path: parser.JSONPath{
				Table:    table,
				Column:   column,
				Path:     path,
				Operator: operator,
				IsText:   operator == "#>>",
			},
		})
	}

	for _, m := range arrowChainRE.FindAllStringSubmatchIndex(sql, -1) {
		table := submatch(sql, m, 1)
		column := submatch(sql, m, 2)
		chain := strings.TrimSpace(sql[m[0]:m[1]])
		if table == "" {
			table = defaultTable
		}
		steps := arrowStepRE.FindAllStringSubmatch(chain, -1)
		if column == "" || len(steps) == 0 {
			continue
		}
		path := make([]string, 0, len(steps))
		operator := ""
		for _, step := range steps {
			operator = step[1]
			path = append(path, step[2])
		}
		collected = append(collected, positionedPath{
			start: m[0],
			path: parser.JSONPath{
				Table:    table,
				Column:   column,
				Path:     path,
				Operator: operator,
				IsText:   operator == "->>",
			},
		})
	}

	sort.SliceStable(collected, func(i, j int) bool {
		return collected[i].start < collected[j].start
	})

	out := make([]parser.JSONPath, 0, len(collected))
	for _, item := range collected {
		out = append(out, item.path)
	}
	return out
}

func submatch(sql string, idx []int, group int) string {
	start := idx[group*2]
	end := idx[group*2+1]
	if start < 0 || end < 0 || end <= start {
		return ""
	}
	return strings.TrimSpace(sql[start:end])
}

func splitJSONPathList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		p := strings.TrimSpace(strings.Trim(part, `"'`))
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
