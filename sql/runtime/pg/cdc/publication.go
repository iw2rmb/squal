package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
)

// EnsurePublication ensures publication existence and table reconciliation.
func EnsurePublication(ctx context.Context, db *sql.DB, name string, tables []string, ops []string, metrics MetricsRecorder) error {
	startTime := time.Now()
	recordMetric := func(result string) {
		if metrics != nil {
			latencySeconds := time.Since(startTime).Seconds()
			metrics.RecordEnsureWithLatency(result, latencySeconds)
		}
	}

	if db == nil {
		recordMetric("error")
		return ErrNilDatabase
	}
	if name == "" {
		recordMetric("error")
		return fmt.Errorf("publication name cannot be empty")
	}
	if !isValidIdentifier(name) {
		recordMetric("error")
		return fmt.Errorf("invalid publication name: %q", name)
	}
	if len(tables) == 0 {
		recordMetric("error")
		return fmt.Errorf("tables cannot be empty")
	}
	if len(ops) == 0 {
		recordMetric("error")
		return fmt.Errorf("operations cannot be empty")
	}

	exists, err := pubExists(ctx, db, name)
	if err != nil {
		recordMetric("error")
		return err
	}

	if !exists {
		if err := createPublication(ctx, db, name, tables, ops); err != nil {
			recordMetric("error")
			return err
		}
		recordMetric("created")
		return nil
	}

	existingTables, err := ListPublicationTables(ctx, db, name)
	if err != nil {
		recordMetric("error")
		return err
	}

	existingSet := make(map[string]struct{}, len(existingTables))
	for _, t := range existingTables {
		existingSet[t] = struct{}{}
	}

	var missingTables []string
	for _, t := range tables {
		if _, ok := existingSet[t]; !ok {
			missingTables = append(missingTables, t)
		}
	}

	if len(missingTables) > 0 {
		if err := addTables(ctx, db, name, missingTables); err != nil {
			recordMetric("error")
			return err
		}
		recordMetric("reconciled")
		return nil
	}

	recordMetric("unchanged")
	return nil
}

func pubExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	if db == nil {
		return false, ErrNilDatabase
	}

	const query = "SELECT 1 FROM pg_publication WHERE pubname = $1"

	var exists int
	err := db.QueryRowContext(ctx, query, name).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if publication exists: %w", err)
	}

	return true, nil
}

func createPublication(ctx context.Context, db *sql.DB, name string, tables []string, ops []string) error {
	if db == nil {
		return ErrNilDatabase
	}
	if name == "" {
		return fmt.Errorf("publication name cannot be empty")
	}
	if !isValidIdentifier(name) {
		return fmt.Errorf("invalid publication name: %q", name)
	}
	if len(tables) == 0 {
		return fmt.Errorf("tables cannot be empty")
	}
	if len(ops) == 0 {
		return fmt.Errorf("operations cannot be empty")
	}

	for _, t := range tables {
		if !isValidIdentifier(t) {
			return fmt.Errorf("invalid table identifier: %q", t)
		}
	}

	sortedTables := normalizeAndSortTables(tables)

	sortedOps := make([]string, len(ops))
	copy(sortedOps, ops)
	sort.Strings(sortedOps)

	tablesList := strings.Join(sortedTables, ",")
	opsList := strings.Join(sortedOps, ",")
	ddl := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = '%s')", name, tablesList, opsList)

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	return nil
}

// ListPublicationTables returns schema-qualified publication tables in stable order.
func ListPublicationTables(ctx context.Context, db *sql.DB, name string) ([]string, error) {
	if db == nil {
		return nil, ErrNilDatabase
	}

	const query = "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1"

	rows, err := db.QueryContext(ctx, query, name)
	if err != nil {
		return nil, fmt.Errorf("failed to list publication tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var schemaName string
		var tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan publication table row: %w", err)
		}
		tables = append(tables, fmt.Sprintf("%s.%s", schemaName, tableName))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating publication tables: %w", err)
	}

	sort.Strings(tables)
	return tables, nil
}

func addTables(ctx context.Context, db *sql.DB, name string, tables []string) error {
	if db == nil {
		return ErrNilDatabase
	}
	if name == "" {
		return fmt.Errorf("publication name cannot be empty")
	}
	if !isValidIdentifier(name) {
		return fmt.Errorf("invalid publication name: %q", name)
	}
	if len(tables) == 0 {
		return nil
	}

	for _, t := range tables {
		if !isValidIdentifier(t) {
			return fmt.Errorf("invalid table identifier: %q", t)
		}
	}

	sortedTables := normalizeAndSortTables(tables)
	tablesList := strings.Join(sortedTables, ",")
	ddl := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", name, tablesList)

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("failed to add tables to publication: %w", err)
	}

	return nil
}

// QualifyTableName prepends a default schema to unqualified tables.
func QualifyTableName(tableName, defaultSchema string) string {
	if tableName == "" {
		return ""
	}
	if strings.Contains(tableName, ".") {
		return tableName
	}

	schema := defaultSchema
	if schema == "" {
		schema = "public"
	}
	return fmt.Sprintf("%s.%s", schema, tableName)
}

func isValidIdentifier(s string) bool {
	if s == "" {
		return false
	}
	partStart := 0
	for i, r := range s {
		if r == '.' {
			if !validIdentPart(s[partStart:i]) {
				return false
			}
			partStart = i + 1
		}
	}
	return validIdentPart(s[partStart:])
}

func validIdentPart(p string) bool {
	if p == "" {
		return false
	}
	for i, r := range p {
		if i == 0 {
			if !(unicode.IsLetter(r) || r == '_') {
				return false
			}
			continue
		}
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}
	return true
}

func normalizeAndSortTables(tables []string) []string {
	if len(tables) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(tables))
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}

	sort.Strings(out)
	return out
}
