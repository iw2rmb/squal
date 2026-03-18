package cdc

import (
	"errors"
	"fmt"
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
