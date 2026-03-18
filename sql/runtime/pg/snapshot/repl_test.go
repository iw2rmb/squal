package snapshot

import (
	"context"
	"strings"
	"testing"
)

func TestOpenReplication(t *testing.T) {
	t.Parallel()

	_, err := OpenReplication(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "dsn is empty") {
		t.Fatalf("error = %v, want substring %q", err, "dsn is empty")
	}
}

func TestCreateTempSlotExportSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		plugin      string
		prefix      string
		errContains string
	}{
		{
			name:        "nil connection",
			plugin:      "pgoutput",
			prefix:      "squal_snap_",
			errContains: "connection is nil",
		},
		{
			name:        "empty plugin",
			plugin:      "",
			prefix:      "squal_snap_",
			errContains: "connection is nil",
		},
		{
			name:        "empty prefix",
			plugin:      "pgoutput",
			prefix:      "",
			errContains: "connection is nil",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			slot, snap, lsn, err := CreateTempSlotExportSnapshot(context.Background(), nil, tt.plugin, tt.prefix)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Fatalf("error = %v, want substring %q", err, tt.errContains)
			}
			if slot != "" || snap != "" || lsn != "" {
				t.Fatalf("expected empty return values, got slot=%q snapshot=%q lsn=%q", slot, snap, lsn)
			}
		})
	}
}

func TestDropSlot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		slotName    string
		errContains string
	}{
		{
			name:        "nil connection",
			slotName:    "test_slot",
			errContains: "connection is nil",
		},
		{
			name:        "empty slot name",
			slotName:    "",
			errContains: "connection is nil",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := DropSlot(context.Background(), nil, tt.slotName)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Fatalf("error = %v, want substring %q", err, tt.errContains)
			}
		})
	}
}
