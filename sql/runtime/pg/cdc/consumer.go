package cdc

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultStatusInterval  = 10 * time.Second
	defaultShutdownTimeout = 5 * time.Second
	defaultRetryBackoff    = time.Second
	defaultMaxRetryBackoff = 30 * time.Second
	defaultSlotName        = SlotName("squall_slot")
)

// ConsumerConfig holds PostgreSQL CDC consumer runtime settings.
type ConsumerConfig struct {
	// ConnectionString is the PostgreSQL connection string for replication mode.
	ConnectionString string

	// Publication is the logical replication publication name.
	Publication string

	// SlotName is the logical replication slot identifier.
	SlotName SlotName

	// StatusInterval controls standby status heartbeat cadence.
	StatusInterval time.Duration

	// ShutdownTimeout bounds graceful shutdown wait time in Stop().
	ShutdownTimeout time.Duration

	// MaxBatchSize limits number of row-level events accumulated per transaction batch.
	// Zero or negative means no explicit limit.
	MaxBatchSize int

	// BackpressurePolicy controls behavior when MaxBatchSize is reached.
	BackpressurePolicy BackpressurePolicy

	// ReconcileOnError enables retries for missing slot/publication errors.
	ReconcileOnError bool

	// StartFromLSN sets explicit initial replication position when no checkpoint is found.
	StartFromLSN LSN
}

// Consumer reads PostgreSQL logical replication and dispatches transaction batches.
type Consumer struct {
	config          ConsumerConfig
	logger          Logger
	handler         BatchHandler
	checkpointStore CheckpointStore
	dispatcher      Dispatcher

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
	doneOnce sync.Once

	// test/runtime hooks
	runReplicationStreamFn func(context.Context) error
	randInt63n             func(int64) int64
	retryBaseBackoff       time.Duration
	retryMaxBackoff        time.Duration
}

// NewConsumer creates a CDC consumer with callback-based batch handling.
func NewConsumer(config ConsumerConfig, logger Logger, handler BatchHandler) *Consumer {
	if config.BackpressurePolicy == "" {
		config.BackpressurePolicy = BackpressurePolicyBlock
	}
	if config.StatusInterval <= 0 {
		config.StatusInterval = defaultStatusInterval
	}

	return &Consumer{
		config:           config,
		logger:           ensureLogger(logger),
		handler:          handler,
		dispatcher:       callbackDispatcher{logger: ensureLogger(logger)},
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
		randInt63n:       rand.Int63n,
		retryBaseBackoff: defaultRetryBackoff,
		retryMaxBackoff:  defaultMaxRetryBackoff,
	}
}

// WithCheckpointManager attaches checkpoint persistence to the consumer.
func (c *Consumer) WithCheckpointManager(mgr *CheckpointManager) *Consumer {
	c.checkpointStore = mgr
	return c
}

// WithCheckpointStore attaches generic checkpoint persistence to the consumer.
func (c *Consumer) WithCheckpointStore(store CheckpointStore) *Consumer {
	c.checkpointStore = store
	return c
}

// WithDispatcher overrides default callback dispatcher behavior.
func (c *Consumer) WithDispatcher(dispatcher Dispatcher) *Consumer {
	if dispatcher != nil {
		c.dispatcher = dispatcher
	}
	return c
}

// Start begins CDC streaming with retry/backoff handling.
func (c *Consumer) Start(ctx context.Context) error {
	if c.handler == nil {
		return fmt.Errorf("batch handler is nil")
	}

	c.logger.Info().
		Str("publication", c.config.Publication).
		Str("slot", string(c.config.SlotName)).
		Msg("Starting CDC consumer")

	defer c.doneOnce.Do(func() {
		close(c.doneCh)
	})

	backoff := c.retryBaseBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}
	maxBackoff := c.retryMaxBackoff
	if maxBackoff < backoff {
		maxBackoff = defaultMaxRetryBackoff
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().Msg("CDC consumer stopped due to context cancellation")
			return ctx.Err()
		case <-c.stopCh:
			c.logger.Info().Msg("CDC consumer stopped gracefully")
			return nil
		default:
		}

		err := c.runStream(ctx)
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopCh:
			return nil
		default:
		}

		c.logError(err)
		if !c.shouldRetry(err) {
			c.logger.Error().
				Err(err).
				Msg("Not retrying CDC consumer after error")
			return err
		}

		sleep := c.retryDelay(backoff, maxBackoff)
		c.logger.Info().
			Str("backoff", sleep.String()).
			Msg("Retrying CDC consumer after backoff")

		stopped, waitErr := c.waitForRetry(ctx, sleep)
		if waitErr != nil {
			return waitErr
		}
		if stopped {
			return nil
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// Stop requests graceful shutdown and waits until Start exits or timeout expires.
func (c *Consumer) Stop() error {
	c.logger.Info().Msg("Stopping CDC consumer")
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})

	timeout := c.config.ShutdownTimeout
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}

	select {
	case <-c.doneCh:
		c.logger.Info().Msg("CDC consumer stopped successfully")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for CDC consumer to stop after %v", timeout)
	}
}

// DefaultConsumerConfig returns sensible CDC consumer defaults.
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		SlotName:           defaultSlotName,
		StatusInterval:     defaultStatusInterval,
		BackpressurePolicy: BackpressurePolicyBlock,
	}
}

// NewConsumerConfig builds ConsumerConfig from host-side CDC settings.
func NewConsumerConfig(connectionString string, cfg interface {
	GetPublication() string
	GetSlotName() string
	GetStatusInterval() time.Duration
}) ConsumerConfig {
	return ConsumerConfig{
		ConnectionString: connectionString,
		Publication:      cfg.GetPublication(),
		SlotName:         SlotName(cfg.GetSlotName()),
		StatusInterval:   cfg.GetStatusInterval(),
	}
}

func (c *Consumer) runStream(ctx context.Context) error {
	if c.runReplicationStreamFn != nil {
		return c.runReplicationStreamFn(ctx)
	}
	return c.runReplicationStream(ctx)
}

func (c *Consumer) retryDelay(backoff, maxBackoff time.Duration) time.Duration {
	jitterMax := backoff / 2
	jitter := time.Duration(c.nextRandInt63n(int64(jitterMax)))
	sleep := backoff + jitter
	if sleep > maxBackoff {
		return maxBackoff
	}
	return sleep
}

func (c *Consumer) nextRandInt63n(max int64) int64 {
	if max <= 0 {
		return 0
	}
	if c.randInt63n != nil {
		return c.randInt63n(max)
	}
	return rand.Int63n(max)
}

func (c *Consumer) waitForRetry(ctx context.Context, delay time.Duration) (bool, error) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.stopCh:
		return true, nil
	}
}

type callbackDispatcher struct {
	logger Logger
}

func (d callbackDispatcher) DispatchWithCheckpoint(
	ctx context.Context,
	batch TxBatch,
	slotName SlotName,
	checkpointSaver CheckpointSaver,
	handler BatchHandler,
) error {
	if err := handler.HandleBatch(ctx, batch); err != nil {
		return err
	}

	if checkpointSaver == nil {
		return nil
	}

	if err := checkpointSaver.SaveCheckpoint(ctx, slotName, batch.LSN); err != nil {
		return fmt.Errorf("failed to save checkpoint for LSN %s: %w", batch.LSN, err)
	}

	if err := checkpointSaver.AckLSN(ctx, batch.LSN); err != nil {
		d.logger.Warn().
			Err(err).
			Str("lsn", string(batch.LSN)).
			Msg("Failed to send replication ack (standby status update)")
	}

	return nil
}
