package clickhouse

import (
	"errors"
	"fmt"

	driver "github.com/ClickHouse/clickhouse-go/v2"

	"piperoom/internal/clients"
)

// ClickHouse error codes are stable protocol values. UNKNOWN_TABLE and
// UNKNOWN_DATABASE indicate that the requested warehouse object is absent;
// SHOW CREATE TABLE reports CANNOT_GET_CREATE_TABLE_QUERY instead of
// UNKNOWN_TABLE for the same case.
const (
	errUnknownTable         = 60
	errUnknownDatabase      = 81
	errReadonly             = 164
	errIllegalFinal         = 181
	errCannotGetCreateTable = 390
	errAccessDenied         = 497
)

func missingObject(err error) bool {
	var chErr *driver.Exception
	return errors.As(err, &chErr) && (chErr.Code == errUnknownTable || chErr.Code == errUnknownDatabase || chErr.Code == errCannotGetCreateTable)
}

func illegalFinal(err error) bool {
	var chErr *driver.Exception
	return errors.As(err, &chErr) && chErr.Code == errIllegalFinal
}

func accessDenied(err error) bool {
	var chErr *driver.Exception
	return errors.As(err, &chErr) && chErr.Code == errAccessDenied
}

// readonlyProfile reports READONLY: the user's profile has readonly=1, which
// rejects the readonly and max_execution_time settings the client sends with
// every query. The ping in New sends no settings, so this shows up only on the
// first real query.
func readonlyProfile(err error) bool {
	var chErr *driver.Exception
	return errors.As(err, &chErr) && chErr.Code == errReadonly
}

func (c *Client) queryError(operation, table string, err error) error {
	if missingObject(err) {
		return fmt.Errorf("clickhouse: %s %s: %w", operation, table, clients.ErrNotFound)
	}
	if readonlyProfile(err) {
		return fmt.Errorf("clickhouse: %s %s: the user's profile is readonly=1, which rejects the per-query settings the client sends; use readonly=2, or readonly=0 with SELECT-only grants: %w", operation, table, Redact(err, c.dsn))
	}
	return fmt.Errorf("clickhouse: %s %s: %w", operation, table, Redact(err, c.dsn))
}
