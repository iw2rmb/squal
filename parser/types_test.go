package parser

import (
	"testing"
	"time"
)

// TestQueryMetadata_SlidingWindow verifies that QueryMetadata can hold SlidingWindowInfo.
// This test satisfies the compilation requirement from ROADMAP.md line 157.
func TestQueryMetadata_SlidingWindow(t *testing.T) {
	t.Parallel()

	// Test case 1: QueryMetadata with nil SlidingWindow
	qm1 := QueryMetadata{
		Tables:        []string{"transactions"},
		SlidingWindow: nil,
	}
	if qm1.SlidingWindow != nil {
		t.Errorf("Expected nil SlidingWindow, got %v", qm1.SlidingWindow)
	}

	// Test case 2: QueryMetadata with populated SlidingWindow
	qm2 := QueryMetadata{
		Tables: []string{"transactions"},
		SlidingWindow: &SlidingWindowInfo{
			Enabled:      true,
			Column:       "ts",
			Table:        "transactions",
			Operator:     ">",
			Interval:     time.Hour,
			ReferenceSQL: "now() - interval '1 hour'",
		},
	}

	if qm2.SlidingWindow == nil {
		t.Fatal("Expected non-nil SlidingWindow")
	}
	if !qm2.SlidingWindow.Enabled {
		t.Error("Expected Enabled to be true")
	}
	if qm2.SlidingWindow.Column != "ts" {
		t.Errorf("Expected Column 'ts', got %q", qm2.SlidingWindow.Column)
	}
	if qm2.SlidingWindow.Operator != ">" {
		t.Errorf("Expected Operator '>', got %q", qm2.SlidingWindow.Operator)
	}
	if qm2.SlidingWindow.Interval != time.Hour {
		t.Errorf("Expected Interval 1h, got %v", qm2.SlidingWindow.Interval)
	}
}

// TestSlidingWindowInfo_FieldTypes verifies that SlidingWindowInfo has the correct field types.
// This ensures compatibility with the expiry queue mechanism.
func TestSlidingWindowInfo_FieldTypes(t *testing.T) {
	t.Parallel()

	sw := SlidingWindowInfo{
		Enabled:      true,
		Column:       "created_at",
		Table:        "events",
		Operator:     ">=",
		Interval:     7 * 24 * time.Hour,
		ReferenceSQL: "CURRENT_TIMESTAMP - INTERVAL '7 days'",
	}

	// Verify Enabled is bool
	var _ bool = sw.Enabled

	// Verify Column is string
	var _ string = sw.Column

	// Verify Table is string
	var _ string = sw.Table

	// Verify Operator is string
	var _ string = sw.Operator

	// Verify Interval is time.Duration
	var _ time.Duration = sw.Interval

	// Verify ReferenceSQL is string
	var _ string = sw.ReferenceSQL

	// Ensure value equality semantics hold
	if sw.Interval != 7*24*time.Hour {
		t.Errorf("Expected 7 days duration, got %v", sw.Interval)
	}
}
