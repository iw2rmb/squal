package snapshot

import "errors"

var (
	// ErrEmptyDSN indicates missing replication connection string input.
	ErrEmptyDSN = errors.New("dsn is empty")

	// ErrNilConnection indicates missing replication connection input.
	ErrNilConnection = errors.New("replication connection is nil")

	// ErrEmptyPlugin indicates missing logical decoding plugin name.
	ErrEmptyPlugin = errors.New("plugin name is empty")

	// ErrEmptyPrefix indicates missing temporary slot prefix.
	ErrEmptyPrefix = errors.New("slot prefix is empty")

	// ErrEmptySlotName indicates missing replication slot name.
	ErrEmptySlotName = errors.New("slot name is empty")

	// ErrNilTransaction indicates missing SQL transaction input.
	ErrNilTransaction = errors.New("transaction is nil")

	// ErrEmptySnapshotName indicates missing PostgreSQL snapshot identifier.
	ErrEmptySnapshotName = errors.New("snapshot name is empty")
)
