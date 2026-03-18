package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Probe verifies PostgreSQL readiness for logical replication CDC.
func Probe(ctx context.Context, db *sql.DB, log Logger, metrics MetricsRecorder) error {
	if db == nil {
		if metrics != nil {
			metrics.RecordProbe(false)
		}
		return ErrNilDatabase
	}

	logger := ensureLogger(log)

	if err := db.PingContext(ctx); err != nil {
		logger.Error().Err(err).Msg("cdc probe: database ping failed")
		if metrics != nil {
			metrics.RecordProbe(false)
		}
		return err
	}

	logger.Info().Msg("cdc probe: verifying postgresql readiness for logical replication")

	if err := requireVersionPG10Plus(ctx, db); err != nil {
		logger.Error().Err(err).Msg("cdc probe: version check failed")
		if metrics != nil {
			metrics.RecordProbe(false)
		}
		return err
	}

	if err := requireWalLevelLogical(ctx, db); err != nil {
		logger.Error().Err(err).Msg("cdc probe: wal_level check failed")
		if metrics != nil {
			metrics.RecordProbe(false)
		}
		return err
	}

	warnWalSenders(ctx, db, logger)
	warnSlots(ctx, db, logger)

	canCreate, err := canCreatePublication(ctx, db)
	if err != nil {
		logger.Error().Err(err).Msg("cdc probe: privilege check failed")
		if metrics != nil {
			metrics.RecordProbe(false)
		}
		return err
	}
	if !canCreate {
		err := &ErrInsufficientPrivileges{
			Action: "CREATE PUBLICATION",
			Hint:   "use a role with CREATE privilege on the current database or pre-create the publication manually",
		}
		logger.Error().Err(err).Msg("cdc probe: insufficient privileges")
		if metrics != nil {
			metrics.RecordProbe(false)
		}
		return err
	}

	logger.Info().Msg("cdc probe: postgresql server is ready for logical replication")
	if metrics != nil {
		metrics.RecordProbe(true)
	}
	return nil
}

func requireWalLevelLogical(ctx context.Context, db *sql.DB) error {
	var walLevel string
	if err := db.QueryRowContext(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return fmt.Errorf("failed to query wal_level: %w", err)
	}

	wl := strings.ToLower(strings.TrimSpace(walLevel))
	if wl != "logical" {
		return &ErrPostgresNeedsRestart{
			Param: "wal_level",
			Have:  wl,
			Want:  "logical",
		}
	}

	return nil
}

func requireVersionPG10Plus(ctx context.Context, db *sql.DB) error {
	var versionStr string
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&versionStr); err != nil {
		return fmt.Errorf("failed to query server_version_num: %w", err)
	}

	versionStr = strings.TrimSpace(versionStr)

	var version int
	if _, err := fmt.Sscanf(versionStr, "%d", &version); err != nil {
		return fmt.Errorf("failed to parse server version %q: %w", versionStr, err)
	}

	const minVersion = 100000
	if version < minVersion {
		return &ErrUnsupportedVersion{
			Have: versionStr,
			Want: ">= 100000",
		}
	}

	return nil
}

func warnWalSenders(ctx context.Context, db *sql.DB, log Logger) {
	var valueStr string
	if err := db.QueryRowContext(ctx, "SHOW max_wal_senders").Scan(&valueStr); err != nil {
		log.Debug().Err(err).Str("param", "max_wal_senders").Msg("cdc probe: failed to query max_wal_senders; skipping warning")
		return
	}

	valueStr = strings.TrimSpace(valueStr)

	var value int
	if _, err := fmt.Sscanf(valueStr, "%d", &value); err != nil {
		log.Debug().Str("param", "max_wal_senders").Str("raw", valueStr).Msg("cdc probe: cannot parse max_wal_senders; skipping warning")
		return
	}

	if value == 0 {
		log.Warn().
			Str("param", "max_wal_senders").
			Int("value", 0).
			Msg("max_wal_senders is 0; replication connections will not be possible; consider setting to at least 1")
	}
}

func warnSlots(ctx context.Context, db *sql.DB, log Logger) {
	var valueStr string
	if err := db.QueryRowContext(ctx, "SHOW max_replication_slots").Scan(&valueStr); err != nil {
		log.Debug().Err(err).Str("param", "max_replication_slots").Msg("cdc probe: failed to query max_replication_slots; skipping warning")
		return
	}

	valueStr = strings.TrimSpace(valueStr)

	var value int
	if _, err := fmt.Sscanf(valueStr, "%d", &value); err != nil {
		log.Debug().Str("param", "max_replication_slots").Str("raw", valueStr).Msg("cdc probe: cannot parse max_replication_slots; skipping warning")
		return
	}

	if value == 0 {
		log.Warn().
			Str("param", "max_replication_slots").
			Int("value", 0).
			Msg("max_replication_slots is 0; replication slots will not be available; consider setting to at least 1")
	}
}

func canCreatePublication(ctx context.Context, db *sql.DB) (bool, error) {
	var hasPrivilege bool
	const query = "SELECT has_database_privilege(current_user, current_database(), 'CREATE')"
	if err := db.QueryRowContext(ctx, query).Scan(&hasPrivilege); err != nil {
		return false, fmt.Errorf("failed to check CREATE privilege: %w", err)
	}
	return hasPrivilege, nil
}
