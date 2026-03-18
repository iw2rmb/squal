package cdc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	cfg := DefaultConsumerConfig()
	cfg.Publication = "test_pub"
	cfg.SlotName = SlotName("test_slot")

	consumer := NewConsumer(cfg, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
	if consumer == nil {
		t.Fatal("NewConsumer returned nil")
	}
	if consumer.config.Publication != "test_pub" {
		t.Fatalf("Publication = %q, want %q", consumer.config.Publication, "test_pub")
	}
	if consumer.config.SlotName != SlotName("test_slot") {
		t.Fatalf("SlotName = %q, want %q", consumer.config.SlotName, SlotName("test_slot"))
	}
}

func TestConsumer_StartStop(t *testing.T) {
	cfg := DefaultConsumerConfig()
	cfg.Publication = "test_pub"
	cfg.SlotName = SlotName("test_slot")
	cfg.ShutdownTimeout = 500 * time.Millisecond

	log := &probeLogger{}
	consumer := NewConsumer(cfg, log, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
	consumer.runReplicationStreamFn = func(context.Context) error {
		<-consumer.stopCh
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startDone := make(chan error, 1)
	go func() { startDone <- consumer.Start(ctx) }()

	if !waitForLog(log, "Starting CDC consumer", 250*time.Millisecond) {
		t.Fatal("startup log message not observed")
	}

	if err := consumer.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if err := <-startDone; err != nil {
		t.Fatalf("Start() returned error after Stop: %v", err)
	}

	if !containsLog(log.Events(), "CDC consumer stopped successfully") {
		t.Fatal("shutdown success log message not observed")
	}
}

func TestConsumer_ContextCancellation(t *testing.T) {
	t.Parallel()

	consumer := NewConsumer(DefaultConsumerConfig(), &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
	consumer.runReplicationStreamFn = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	consumer.retryBaseBackoff = time.Millisecond
	consumer.retryMaxBackoff = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	err := consumer.Start(ctx)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want context cancellation", err)
	}
}

func TestDefaultConsumerConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConsumerConfig()
	if cfg.SlotName != SlotName("mill_slot") {
		t.Fatalf("default SlotName = %q, want %q", cfg.SlotName, SlotName("mill_slot"))
	}
	if cfg.StatusInterval != 10*time.Second {
		t.Fatalf("default StatusInterval = %v, want 10s", cfg.StatusInterval)
	}
	if cfg.BackpressurePolicy != BackpressurePolicyBlock {
		t.Fatalf("default BackpressurePolicy = %q, want %q", cfg.BackpressurePolicy, BackpressurePolicyBlock)
	}
}

type mockConsumerCfg struct {
	publication    string
	slotName       string
	statusInterval time.Duration
}

func (m mockConsumerCfg) GetPublication() string           { return m.publication }
func (m mockConsumerCfg) GetSlotName() string              { return m.slotName }
func (m mockConsumerCfg) GetStatusInterval() time.Duration { return m.statusInterval }

func TestNewConsumerConfig(t *testing.T) {
	t.Parallel()

	cfg := NewConsumerConfig("postgres://localhost/test", mockConsumerCfg{
		publication:    "test_pub",
		slotName:       "test_slot",
		statusInterval: 30 * time.Second,
	})

	if cfg.ConnectionString != "postgres://localhost/test" {
		t.Fatalf("ConnectionString = %q, want %q", cfg.ConnectionString, "postgres://localhost/test")
	}
	if cfg.Publication != "test_pub" {
		t.Fatalf("Publication = %q, want %q", cfg.Publication, "test_pub")
	}
	if cfg.SlotName != SlotName("test_slot") {
		t.Fatalf("SlotName = %q, want %q", cfg.SlotName, SlotName("test_slot"))
	}
	if cfg.StatusInterval != 30*time.Second {
		t.Fatalf("StatusInterval = %v, want 30s", cfg.StatusInterval)
	}
}

func TestConsumer_ShutdownTimeout(t *testing.T) {
	tests := []struct {
		name            string
		shutdownTimeout time.Duration
	}{
		{name: "custom timeout", shutdownTimeout: 2 * time.Second},
		{name: "default timeout", shutdownTimeout: 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConsumerConfig()
			cfg.ShutdownTimeout = tt.shutdownTimeout

			consumer := NewConsumer(cfg, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
			consumer.runReplicationStreamFn = func(context.Context) error {
				<-consumer.stopCh
				return nil
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = consumer.Start(ctx) }()

			time.Sleep(25 * time.Millisecond)

			started := time.Now()
			if err := consumer.Stop(); err != nil {
				t.Fatalf("Stop() error = %v", err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("Stop() took too long: %v", elapsed)
			}
		})
	}
}

func TestConsumer_ShutdownTimeoutExceeded(t *testing.T) {
	cfg := DefaultConsumerConfig()
	cfg.ShutdownTimeout = 50 * time.Millisecond

	consumer := NewConsumer(cfg, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))
	consumer.doneCh = make(chan struct{})

	started := time.Now()
	err := consumer.Stop()
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("Stop() error = nil, want timeout error")
	}
	if !strings.Contains(err.Error(), "timeout waiting for CDC consumer to stop") {
		t.Fatalf("Stop() error = %q, want timeout message", err.Error())
	}
	if elapsed < cfg.ShutdownTimeout {
		t.Fatalf("Stop() elapsed = %v, want >= %v", elapsed, cfg.ShutdownTimeout)
	}
}

func TestConsumer_RetryWithBackoff_Transient(t *testing.T) {
	cfg := DefaultConsumerConfig()
	log := &probeLogger{}
	consumer := NewConsumer(cfg, log, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))

	attempts := 0
	consumer.runReplicationStreamFn = func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("connection refused")
		}
		return nil
	}
	consumer.retryBaseBackoff = time.Millisecond
	consumer.retryMaxBackoff = 2 * time.Millisecond
	consumer.randInt63n = func(int64) int64 { return 0 }

	if err := consumer.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if !containsLog(log.Events(), "Retrying CDC consumer after backoff") {
		t.Fatal("expected retry backoff log entry")
	}
}

func TestConsumer_RetryWithBackoff_Fatal(t *testing.T) {
	cfg := DefaultConsumerConfig()
	consumer := NewConsumer(cfg, &probeLogger{}, BatchHandlerFunc(func(context.Context, TxBatch) error { return nil }))

	attempts := 0
	fatalErr := &ErrUnsupportedVersion{Have: "90600", Want: ">= 100000"}
	consumer.runReplicationStreamFn = func(context.Context) error {
		attempts++
		return fatalErr
	}
	consumer.retryBaseBackoff = time.Millisecond
	consumer.retryMaxBackoff = time.Millisecond

	err := consumer.Start(context.Background())
	if !errors.Is(err, fatalErr) {
		t.Fatalf("Start() error = %v, want %v", err, fatalErr)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestConsumer_StartNilHandler(t *testing.T) {
	t.Parallel()

	consumer := NewConsumer(DefaultConsumerConfig(), &probeLogger{}, nil)
	err := consumer.Start(context.Background())
	if err == nil {
		t.Fatal("Start() error = nil, want nil-handler error")
	}
	if !strings.Contains(err.Error(), "batch handler is nil") {
		t.Fatalf("Start() error = %q, want nil-handler message", err.Error())
	}
}

func waitForLog(log *probeLogger, fragment string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if containsLog(log.Events(), fragment) {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return containsLog(log.Events(), fragment)
}

func containsLog(events []string, fragment string) bool {
	for _, event := range events {
		if strings.Contains(event, fragment) {
			return true
		}
	}
	return false
}
