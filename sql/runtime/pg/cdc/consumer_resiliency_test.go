package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestConsumer_ClassifyTransientError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "network timeout", err: errors.New("connection timeout"), want: true},
		{name: "connection refused", err: errors.New("connection refused"), want: true},
		{name: "connection reset", err: errors.New("connection reset by peer"), want: true},
		{name: "EOF", err: errors.New("EOF"), want: true},
		{name: "broken pipe", err: errors.New("broken pipe"), want: true},
		{name: "pg conn failure class", err: &pgconn.PgError{Code: "08006", Message: "connection failure"}, want: true},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientError(tt.err); got != tt.want {
				t.Fatalf("isTransientError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestConsumer_ClassifyFatalError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "unsupported version", err: &ErrUnsupportedVersion{Have: "90600", Want: ">= 100000"}, want: true},
		{name: "insufficient privileges", err: &ErrInsufficientPrivileges{Action: "CREATE PUBLICATION", Hint: "grant privileges"}, want: true},
		{name: "needs restart", err: &ErrPostgresNeedsRestart{Param: "wal_level", Have: "replica", Want: "logical"}, want: true},
		{name: "permission denied pg error", err: &pgconn.PgError{Code: "42501", Message: "permission denied"}, want: true},
		{name: "undefined object pg error", err: &pgconn.PgError{Code: "42704", Message: "publication does not exist"}, want: true},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := isFatalError(tt.err); got != tt.want {
				t.Fatalf("isFatalError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestConsumer_ClassifySlotNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "pg error slot missing", err: &pgconn.PgError{Code: "42704", Message: `replication slot "test_slot" does not exist`}, want: true},
		{name: "message slot missing", err: errors.New("replication slot does not exist"), want: true},
		{name: "other", err: errors.New("connection refused"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := isSlotNotFoundError(tt.err); got != tt.want {
				t.Fatalf("isSlotNotFoundError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestConsumer_ClassifyPublicationNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "pg error publication missing", err: &pgconn.PgError{Code: "42704", Message: `publication "test_pub" does not exist`}, want: true},
		{name: "message publication missing", err: errors.New("publication does not exist"), want: true},
		{name: "other", err: errors.New("connection refused"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := isPublicationNotFoundError(tt.err); got != tt.want {
				t.Fatalf("isPublicationNotFoundError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestConsumer_ShouldRetry(t *testing.T) {
	tests := []struct {
		name             string
		reconcileEnabled bool
		err              error
		wantRetry        bool
	}{
		{name: "transient errors retry", err: errors.New("connection refused"), wantRetry: true},
		{name: "fatal errors do not retry", err: &ErrUnsupportedVersion{Have: "90600", Want: ">= 100000"}, wantRetry: false},
		{name: "slot missing retries only with reconcile", reconcileEnabled: true, err: errors.New("replication slot does not exist"), wantRetry: true},
		{name: "slot missing no retry without reconcile", reconcileEnabled: false, err: errors.New("replication slot does not exist"), wantRetry: false},
		{name: "publication missing retries only with reconcile", reconcileEnabled: true, err: errors.New("publication does not exist"), wantRetry: true},
		{name: "unknown no retry", err: errors.New("something else"), wantRetry: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConsumerConfig()
			cfg.ReconcileOnError = tt.reconcileEnabled
			consumer := NewConsumer(cfg, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))

			if got := consumer.shouldRetry(tt.err); got != tt.wantRetry {
				t.Fatalf("shouldRetry(%v) = %v, want %v", tt.err, got, tt.wantRetry)
			}
		})
	}
}

func TestConsumer_ShouldReconcileSlot(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{ReconcileOnError: true}, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
	if !consumer.shouldReconcileSlot(errors.New("replication slot does not exist")) {
		t.Fatal("shouldReconcileSlot() = false, want true when enabled")
	}

	consumer = NewConsumer(ConsumerConfig{ReconcileOnError: false}, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
	if consumer.shouldReconcileSlot(errors.New("replication slot does not exist")) {
		t.Fatal("shouldReconcileSlot() = true, want false when disabled")
	}
}

func TestConsumer_ShouldReconcilePublication(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{ReconcileOnError: true}, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
	if !consumer.shouldReconcilePublication(errors.New("publication does not exist")) {
		t.Fatal("shouldReconcilePublication() = false, want true when enabled")
	}

	consumer = NewConsumer(ConsumerConfig{ReconcileOnError: false}, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
	if consumer.shouldReconcilePublication(errors.New("publication does not exist")) {
		t.Fatal("shouldReconcilePublication() = true, want false when disabled")
	}
}

func TestConsumer_LogErrorClassification(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantLog string
	}{
		{name: "transient", err: errors.New("connection timeout"), wantLog: "Transient error encountered, will retry with backoff"},
		{name: "fatal", err: &ErrUnsupportedVersion{Have: "90600", Want: ">= 100000"}, wantLog: "Fatal error encountered, consumer will stop"},
		{name: "publication missing", err: errors.New("publication does not exist"), wantLog: "CDC error: publication not found at start position"},
		{name: "unknown", err: errors.New("weird"), wantLog: "Error encountered"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			log := &probeLogger{}
			consumer := NewConsumer(ConsumerConfig{Publication: "pub", SlotName: SlotName("slot")}, log, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
			consumer.logError(tt.err)
			if !containsLog(log.Events(), tt.wantLog) {
				t.Fatalf("expected log %q, got %v", tt.wantLog, log.Events())
			}
		})
	}
}
