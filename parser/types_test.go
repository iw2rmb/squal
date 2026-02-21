package parser

import (
	"testing"
	"time"
)

// TestQueryMetadata_SlidingWindow verifies that QueryMetadata can hold SlidingWindowInfo.
func TestQueryMetadata_SlidingWindow(t *testing.T) {
	t.Parallel()

	qm1 := QueryMetadata{
		Tables:        []string{"transactions"},
		SlidingWindow: nil,
	}
	if qm1.SlidingWindow != nil {
		t.Errorf("expected nil SlidingWindow, got %v", qm1.SlidingWindow)
	}

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
		t.Fatal("expected non-nil SlidingWindow")
	}
	if !qm2.SlidingWindow.Enabled {
		t.Error("expected Enabled to be true")
	}
	if qm2.SlidingWindow.Column != "ts" {
		t.Errorf("expected Column 'ts', got %q", qm2.SlidingWindow.Column)
	}
	if qm2.SlidingWindow.Operator != ">" {
		t.Errorf("expected Operator '>', got %q", qm2.SlidingWindow.Operator)
	}
	if qm2.SlidingWindow.Interval != time.Hour {
		t.Errorf("expected Interval 1h, got %v", qm2.SlidingWindow.Interval)
	}
}

// TestSlidingWindowInfo_FieldTypes verifies that SlidingWindowInfo has the correct field types.
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

	var _ bool = sw.Enabled
	var _ string = sw.Column
	var _ string = sw.Table
	var _ string = sw.Operator
	var _ time.Duration = sw.Interval
	var _ string = sw.ReferenceSQL

	if sw.Interval != 7*24*time.Hour {
		t.Errorf("expected 7 days duration, got %v", sw.Interval)
	}
}
