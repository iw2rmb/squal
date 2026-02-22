//go:build cgo
// +build cgo

package parserpg

import (
	"testing"
	"time"

	"github.com/iw2rmb/sql/parser"
)

// TestDetectSlidingWindow tests the DetectSlidingWindow function with various SQL patterns.
func TestDetectSlidingWindow(t *testing.T) {
	p := newCGOParser(t)

	tests := []struct {
		name     string
		sql      string
		expected *parser.SlidingWindowInfo
		wantErr  bool
	}{
		{
			name: "basic NOW() - INTERVAL pattern with > operator",
			sql: `SELECT merchant_id, SUM(amount)
				  FROM transactions
				  WHERE ts > NOW() - INTERVAL '1 hour'
				  GROUP BY merchant_id`,
			expected: &parser.SlidingWindowInfo{
				Enabled:      true,
				Column:       "ts",
				Operator:     ">",
				Interval:     time.Hour,
				ReferenceSQL: "NOW() - INTERVAL '1 hour'",
			},
		},
		{
			name: "NOW() - INTERVAL pattern with >= operator",
			sql: `SELECT COUNT(*)
				  FROM sessions
				  WHERE created_at >= NOW() - INTERVAL '15 minutes'`,
			expected: &parser.SlidingWindowInfo{
				Enabled:      true,
				Column:       "created_at",
				Operator:     ">=",
				Interval:     15 * time.Minute,
				ReferenceSQL: "NOW() - INTERVAL '15 minutes'",
			},
		},
		{
			name: "CURRENT_TIMESTAMP - INTERVAL pattern",
			sql: `SELECT status, COUNT(*)
				  FROM jobs
				  WHERE updated_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
				  GROUP BY status`,
			expected: &parser.SlidingWindowInfo{
				Enabled:      true,
				Column:       "updated_at",
				Operator:     ">",
				Interval:     7 * 24 * time.Hour,
				ReferenceSQL: "CURRENT_TIMESTAMP - INTERVAL '7 days'",
			},
		},
		{
			name: "table-qualified column reference",
			sql: `SELECT t.user_id, COUNT(*)
				  FROM transactions t
				  WHERE t.ts > NOW() - INTERVAL '1 day'
				  GROUP BY t.user_id`,
			expected: &parser.SlidingWindowInfo{
				Enabled:      true,
				Column:       "ts",
				Table:        "t",
				Operator:     ">",
				Interval:     24 * time.Hour,
				ReferenceSQL: "NOW() - INTERVAL '1 day'",
			},
		},
		{
			name: "interval in seconds",
			sql: `SELECT *
				  FROM events
				  WHERE timestamp > NOW() - INTERVAL '30 seconds'`,
			expected: &parser.SlidingWindowInfo{
				Enabled:      true,
				Column:       "timestamp",
				Operator:     ">",
				Interval:     30 * time.Second,
				ReferenceSQL: "NOW() - INTERVAL '30 seconds'",
			},
		},
		{
			name: "interval in weeks",
			sql: `SELECT customer_id, SUM(amount)
				  FROM orders
				  WHERE order_date >= NOW() - INTERVAL '2 weeks'
				  GROUP BY customer_id`,
			expected: &parser.SlidingWindowInfo{
				Enabled:      true,
				Column:       "order_date",
				Operator:     ">=",
				Interval:     14 * 24 * time.Hour,
				ReferenceSQL: "NOW() - INTERVAL '2 weeks'",
			},
		},
		{
			name: "sliding window with AND condition",
			sql: `SELECT product_id, COUNT(*)
				  FROM purchases
				  WHERE ts > NOW() - INTERVAL '1 hour'
				    AND status = 'completed'
				  GROUP BY product_id`,
			expected: &parser.SlidingWindowInfo{
				Enabled:      true,
				Column:       "ts",
				Operator:     ">",
				Interval:     time.Hour,
				ReferenceSQL: "NOW() - INTERVAL '1 hour'",
			},
		},
		{
			name: "reversed comparison (interval on left side)",
			sql: `SELECT *
				  FROM logs
				  WHERE NOW() - INTERVAL '5 minutes' < timestamp`,
			expected: &parser.SlidingWindowInfo{
				Enabled:      true,
				Column:       "timestamp",
				Operator:     ">",
				Interval:     5 * time.Minute,
				ReferenceSQL: "NOW() - INTERVAL '5 minutes'",
			},
		},
		{
			name: "no sliding window - static timestamp comparison",
			sql: `SELECT *
				  FROM events
				  WHERE created_at > '2024-01-01'`,
			expected: nil,
		},
		{
			name: "no sliding window - no WHERE clause",
			sql: `SELECT merchant_id, SUM(amount)
				  FROM transactions
				  GROUP BY merchant_id`,
			expected: nil,
		},
		{
			name: "no sliding window - equality comparison",
			sql: `SELECT *
				  FROM sessions
				  WHERE created_at = NOW()`,
			expected: nil,
		},
		{
			name: "no sliding window - BETWEEN clause",
			sql: `SELECT *
				  FROM orders
				  WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31'`,
			expected: nil,
		},
		{
			name: "no sliding window - NOW() without subtraction",
			sql: `SELECT *
				  FROM events
				  WHERE timestamp > NOW()`,
			expected: nil,
		},
		{
			name: "no sliding window - complex OR condition",
			sql: `SELECT *
				  FROM logs
				  WHERE (ts > NOW() - INTERVAL '1 hour' OR status = 'error')`,
			expected: nil, // OR complicates window semantics
		},
		{
			name:     "invalid SQL syntax",
			sql:      `SELECT * FROM WHERE`,
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.DetectSlidingWindow(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expected == nil {
				if result != nil {
					t.Errorf("expected no sliding window, but got: %+v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("expected sliding window info, but got nil")
				return
			}

			// Compare fields
			if result.Enabled != tt.expected.Enabled {
				t.Errorf("Enabled: got %v, want %v", result.Enabled, tt.expected.Enabled)
			}
			if result.Column != tt.expected.Column {
				t.Errorf("Column: got %q, want %q", result.Column, tt.expected.Column)
			}
			if result.Table != tt.expected.Table {
				t.Errorf("Table: got %q, want %q", result.Table, tt.expected.Table)
			}
			if result.Operator != tt.expected.Operator {
				t.Errorf("Operator: got %q, want %q", result.Operator, tt.expected.Operator)
			}
			if result.Interval != tt.expected.Interval {
				t.Errorf("Interval: got %v, want %v", result.Interval, tt.expected.Interval)
			}
			if result.ReferenceSQL != tt.expected.ReferenceSQL {
				t.Errorf("ReferenceSQL: got %q, want %q", result.ReferenceSQL, tt.expected.ReferenceSQL)
			}
		})
	}
}

// TestDetectSlidingWindow_MultipleConditions tests behavior with multiple time filters.
func TestDetectSlidingWindow_MultipleConditions(t *testing.T) {
	p := newCGOParser(t)

	// Query with multiple AND-ed time conditions
	// Should return the first detected sliding window
	sql := `SELECT *
			FROM events
			WHERE ts > NOW() - INTERVAL '1 hour'
			  AND created_at > NOW() - INTERVAL '1 day'`

	result, err := p.DetectSlidingWindow(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected sliding window info, but got nil")
	}

	// Should detect the first condition (ts > NOW() - INTERVAL '1 hour')
	if result.Column != "ts" {
		t.Errorf("Column: got %q, want %q", result.Column, "ts")
	}
	if result.Interval != time.Hour {
		t.Errorf("Interval: got %v, want %v", result.Interval, time.Hour)
	}
}

// TestDetectSlidingWindow_CombinedWithTimeBucket tests queries with both
// sliding window (WHERE) and time bucketing (GROUP BY).
func TestDetectSlidingWindow_CombinedWithTimeBucket(t *testing.T) {
	p := newCGOParser(t)

	// This query has both:
	// - Sliding window: ts > NOW() - INTERVAL '1 hour'
	// - Time bucket: GROUP BY date_trunc('minute', ts)
	sql := `SELECT date_trunc('minute', ts) AS minute, COUNT(*) AS total
			FROM transactions
			WHERE ts > NOW() - INTERVAL '1 hour'
			GROUP BY 1`

	result, err := p.DetectSlidingWindow(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("expected sliding window info, but got nil")
	}

	if !result.Enabled {
		t.Error("Enabled should be true")
	}
	if result.Column != "ts" {
		t.Errorf("Column: got %q, want %q", result.Column, "ts")
	}
	if result.Operator != ">" {
		t.Errorf("Operator: got %q, want %q", result.Operator, ">")
	}
	if result.Interval != time.Hour {
		t.Errorf("Interval: got %v, want %v", result.Interval, time.Hour)
	}
}

// TestDetectSlidingWindow_VariousIntervalFormats tests different interval formats.
func TestDetectSlidingWindow_VariousIntervalFormats(t *testing.T) {
	p := newCGOParser(t)

	tests := []struct {
		name     string
		sql      string
		interval time.Duration
	}{
		{
			name:     "singular unit (1 hour)",
			sql:      `SELECT * FROM events WHERE ts > NOW() - INTERVAL '1 hour'`,
			interval: time.Hour,
		},
		{
			name:     "plural units (2 days)",
			sql:      `SELECT * FROM events WHERE ts > NOW() - INTERVAL '2 days'`,
			interval: 48 * time.Hour,
		},
		{
			name:     "large number (100 seconds)",
			sql:      `SELECT * FROM events WHERE ts > NOW() - INTERVAL '100 seconds'`,
			interval: 100 * time.Second,
		},
		{
			name:     "fractional (0.5 hours)",
			sql:      `SELECT * FROM events WHERE ts > NOW() - INTERVAL '0.5 hours'`,
			interval: 30 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.DetectSlidingWindow(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result == nil {
				t.Fatal("expected sliding window info, but got nil")
			}

			if result.Interval != tt.interval {
				t.Errorf("Interval: got %v, want %v", result.Interval, tt.interval)
			}
		})
	}
}
