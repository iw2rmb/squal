package cdc

import (
	"context"
	"testing"
)

func TestConsumer_BackpressurePolicies(t *testing.T) {
	tests := []struct {
		name        string
		policy      BackpressurePolicy
		batchSize   int
		maxBatch    int
		wantInclude bool
		wantLog     string
	}{
		{
			name:        "drop when full",
			policy:      BackpressurePolicyDrop,
			batchSize:   3,
			maxBatch:    3,
			wantInclude: false,
			wantLog:     "Dropping event due to backpressure",
		},
		{
			name:        "block when full",
			policy:      BackpressurePolicyBlock,
			batchSize:   5,
			maxBatch:    5,
			wantInclude: true,
			wantLog:     "Blocking due to backpressure",
		},
		{
			name:        "merge currently blocks",
			policy:      BackpressurePolicyMerge,
			batchSize:   2,
			maxBatch:    2,
			wantInclude: true,
			wantLog:     "Merge policy not fully implemented, blocking",
		},
		{
			name:        "unknown defaults to block",
			policy:      BackpressurePolicy("unknown"),
			batchSize:   2,
			maxBatch:    2,
			wantInclude: true,
			wantLog:     "Unknown backpressure policy, defaulting to block",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			log := &probeLogger{}
			consumer := NewConsumer(ConsumerConfig{
				MaxBatchSize:       tt.maxBatch,
				BackpressurePolicy: tt.policy,
				StatusInterval:     defaultStatusInterval,
			}, log, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))

			if got := consumer.applyBackpressure(context.Background(), tt.batchSize); got != tt.wantInclude {
				t.Fatalf("applyBackpressure() = %v, want %v", got, tt.wantInclude)
			}
			if !containsLog(log.Events(), tt.wantLog) {
				t.Fatalf("expected log fragment %q, got %v", tt.wantLog, log.Events())
			}
		})
	}
}

func TestConsumer_BackpressureNoLimit(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		MaxBatchSize:       0,
		BackpressurePolicy: BackpressurePolicyDrop,
	}, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))

	if consumer.shouldApplyBackpressure(1000) {
		t.Fatal("shouldApplyBackpressure() = true, want false when MaxBatchSize <= 0")
	}
	if !consumer.applyBackpressure(context.Background(), 1000) {
		t.Fatal("applyBackpressure() = false, want true when MaxBatchSize <= 0")
	}
}

func TestConsumer_BackpressureNegativeMax(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		MaxBatchSize:       -10,
		BackpressurePolicy: BackpressurePolicyDrop,
	}, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))

	if consumer.shouldApplyBackpressure(0) {
		t.Fatal("shouldApplyBackpressure() = true for size=0, want false when MaxBatchSize <= 0")
	}
	if consumer.shouldApplyBackpressure(100) {
		t.Fatal("shouldApplyBackpressure() = true for size=100, want false when MaxBatchSize <= 0")
	}
	if !consumer.applyBackpressure(context.Background(), 10) {
		t.Fatal("applyBackpressure() = false, want true when MaxBatchSize <= 0")
	}
}

func FuzzApplyBackpressure_NoLimit(f *testing.F) {
	for _, policy := range []string{"", string(BackpressurePolicyDrop), string(BackpressurePolicyBlock), string(BackpressurePolicyMerge), "weird", "DROP"} {
		f.Add(policy, 0)
		f.Add(policy, 1)
		f.Add(policy, 1024)
	}

	f.Fuzz(func(t *testing.T, policy string, size int) {
		consumer := NewConsumer(ConsumerConfig{
			MaxBatchSize:       0,
			BackpressurePolicy: BackpressurePolicy(policy),
		}, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))

		if consumer.shouldApplyBackpressure(size) {
			t.Fatalf("shouldApplyBackpressure() = true, want false (policy=%q size=%d)", policy, size)
		}
		if !consumer.applyBackpressure(context.Background(), size) {
			t.Fatalf("applyBackpressure() = false, want true (policy=%q size=%d)", policy, size)
		}
	})
}
