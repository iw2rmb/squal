package cdc

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNilDatabase indicates a required *sql.DB dependency was not provided.
var ErrNilDatabase = errors.New("database connection is nil")

// ErrPostgresNeedsRestart indicates a PostgreSQL parameter that requires restart.
type ErrPostgresNeedsRestart struct {
	Param string
	Have  string
	Want  string
}

func (e *ErrPostgresNeedsRestart) Error() string {
	return fmt.Sprintf(
		"postgresql parameter %q is set to %q but must be %q for cdc; update postgresql.conf or ALTER SYSTEM SET %s = '%s', then restart postgresql",
		e.Param, e.Have, e.Want, e.Param, e.Want,
	)
}

func (e *ErrPostgresNeedsRestart) Is(target error) bool {
	_, ok := target.(*ErrPostgresNeedsRestart)
	return ok
}

// ErrUnsupportedVersion indicates PostgreSQL server version is too old for CDC.
type ErrUnsupportedVersion struct {
	Have string
	Want string
}

func (e *ErrUnsupportedVersion) Error() string {
	return fmt.Sprintf(
		"postgresql version %s is not supported; logical replication publications require version %s; upgrade your postgresql server to version 10 or later",
		e.Have, e.Want,
	)
}

func (e *ErrUnsupportedVersion) Is(target error) bool {
	_, ok := target.(*ErrUnsupportedVersion)
	return ok
}

// ErrInsufficientPrivileges indicates current database user lacks required grants.
type ErrInsufficientPrivileges struct {
	Action string
	Hint   string
}

func (e *ErrInsufficientPrivileges) Error() string {
	return fmt.Sprintf("insufficient privileges to perform %s; %s", e.Action, e.Hint)
}

func (e *ErrInsufficientPrivileges) Is(target error) bool {
	_, ok := target.(*ErrInsufficientPrivileges)
	return ok
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	transientPatterns := []string{
		"timeout",
		"connection refused",
		"connection reset",
		"EOF",
		"broken pipe",
		"no such host",
		"network is unreachable",
		"connection closed",
	}

	for _, pattern := range transientPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "08") {
			return true
		}
	}

	return false
}

func isFatalError(err error) bool {
	if err == nil {
		return false
	}

	var unsupportedVer *ErrUnsupportedVersion
	var insufficientPriv *ErrInsufficientPrivileges
	var needsRestart *ErrPostgresNeedsRestart
	if errors.As(err, &unsupportedVer) || errors.As(err, &insufficientPriv) || errors.As(err, &needsRestart) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "42") || strings.HasPrefix(pgErr.Code, "53") || strings.HasPrefix(pgErr.Code, "58") {
			return true
		}
	}

	return false
}

func isSlotNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, "replication slot") && strings.Contains(errMsg, "does not exist") {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "42704" && strings.Contains(pgErr.Message, "replication slot") {
			return true
		}
	}

	return false
}

func isPublicationNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, "publication") && strings.Contains(errMsg, "does not exist") {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "42704" && strings.Contains(pgErr.Message, "publication") {
			return true
		}
	}

	return false
}

func (c *Consumer) shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	if isFatalError(err) {
		return false
	}
	if isTransientError(err) {
		return true
	}
	if c.config.ReconcileOnError && (isSlotNotFoundError(err) || isPublicationNotFoundError(err)) {
		return true
	}

	return false
}

func (c *Consumer) shouldReconcileSlot(err error) bool {
	if !c.config.ReconcileOnError {
		return false
	}
	return isSlotNotFoundError(err)
}

func (c *Consumer) shouldReconcilePublication(err error) bool {
	if !c.config.ReconcileOnError {
		return false
	}
	return isPublicationNotFoundError(err)
}

func (c *Consumer) logError(err error) {
	if err == nil {
		return
	}

	if isPublicationNotFoundError(err) {
		c.logger.Error().
			Err(err).
			Str("publication", c.config.Publication).
			Str("slot", string(c.config.SlotName)).
			Str("classification", "publication_not_found").
			Msg("CDC error: publication not found at start position - ensure publication exists and includes required tables, or start from current WAL LSN")
		return
	}

	if isFatalError(err) {
		c.logger.Error().
			Err(err).
			Str("classification", "fatal").
			Msg("Fatal error encountered, consumer will stop")
		return
	}
	if isTransientError(err) {
		c.logger.Warn().
			Err(err).
			Str("classification", "transient").
			Msg("Transient error encountered, will retry with backoff")
		return
	}

	c.logger.Error().
		Err(err).
		Str("classification", "unknown").
		Msg("Error encountered")
}
