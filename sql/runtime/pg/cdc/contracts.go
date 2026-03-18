package cdc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LSN is a database change-stream position identifier.
type LSN string

// SlotName is a replication slot identifier.
type SlotName string

// BackpressurePolicy controls batch behavior when a consumer is overloaded.
type BackpressurePolicy string

const (
	BackpressurePolicyBlock BackpressurePolicy = "block"
	BackpressurePolicyDrop  BackpressurePolicy = "drop"
	BackpressurePolicyMerge BackpressurePolicy = "merge"
)

// Op is a row-level CDC operation.
type Op string

const (
	OpInsert Op = "INSERT"
	OpUpdate Op = "UPDATE"
	OpDelete Op = "DELETE"
)

// TxEvent is a single row-level change emitted by CDC.
type TxEvent struct {
	CommitLSN  LSN            `json:"commit_lsn"`
	Table      string         `json:"table"`
	Operation  Op             `json:"operation"`
	CommitTime time.Time      `json:"commit_time"`
	Keys       map[string]any `json:"keys,omitempty"`
	Old        map[string]any `json:"old,omitempty"`
	New        map[string]any `json:"new,omitempty"`
}

// TxBatch groups row-level events that share one commit position.
type TxBatch struct {
	LSN    LSN       `json:"lsn"`
	Events []TxEvent `json:"events"`
}

// BatchHandler applies a single CDC transaction batch.
type BatchHandler interface {
	HandleBatch(ctx context.Context, batch TxBatch) error
}

// CheckpointSaver persists and acknowledges successfully applied batch positions.
type CheckpointSaver interface {
	SaveCheckpoint(ctx context.Context, slotName SlotName, lsn LSN) error
	AckLSN(ctx context.Context, lsn LSN) error
}

// Dispatcher defines checkpoint-aware delivery sequencing.
//
// Contract:
//   - save checkpoint only after handler success;
//   - issue ack as best-effort after successful checkpoint save.
type Dispatcher interface {
	DispatchWithCheckpoint(ctx context.Context, batch TxBatch, slotName SlotName, checkpointSaver CheckpointSaver, handler BatchHandler) error
}

// CompareLSN compares two PostgreSQL-formatted LSN values ("X/Y").
func CompareLSN(lsn1, lsn2 LSN) (int, error) {
	if lsn1 == lsn2 {
		return 0, nil
	}

	val1, err := parseLSN(lsn1)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn1: %w", err)
	}

	val2, err := parseLSN(lsn2)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn2: %w", err)
	}

	if val1 < val2 {
		return -1, nil
	}
	if val1 > val2 {
		return 1, nil
	}
	return 0, nil
}

// LSNDistance returns signed distance between two PostgreSQL-formatted LSN values.
func LSNDistance(lsn1, lsn2 LSN) (int64, error) {
	val1, err := parseLSN(lsn1)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn1: %w", err)
	}

	val2, err := parseLSN(lsn2)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn2: %w", err)
	}

	return int64(val2) - int64(val1), nil
}

func parseLSN(lsn LSN) (uint64, error) {
	if lsn == "" {
		return 0, nil
	}

	parts := strings.Split(string(lsn), "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid lsn format: %s", lsn)
	}

	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn high part: %w", err)
	}

	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn low part: %w", err)
	}

	return (high << 32) | low, nil
}
