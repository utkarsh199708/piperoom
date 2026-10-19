package clickhouse

import (
	"errors"
	"fmt"

	driver "github.com/ClickHouse/clickhouse-go/v2"

	"piperoom/internal/clients"
)

// ClickHouse error codes are stable protocol values. UNKNOWN_TABLE and
// UNKNOWN_DATABASE indicate that the requested warehouse object is absent.
const (
	errUnknownTable    = 60
	errUnknownDatabase = 81
	errIllegalFinal    = 181
	errAccessDenied    = 497
)

func missingObject(err error) bool {
	var chErr *driver.Exception
	return errors.As(err, &chErr) && (chErr.Code == errUnknownTable || chErr.Code == errUnknownDatabase)
}

func illegalFinal(err error) bool {
	var chErr *driver.Exception
	return errors.As(err, &chErr) && chErr.Code == errIllegalFinal
}

func accessDenied(err error) bool {
	var chErr *driver.Exception
	return errors.As(err, &chErr) && chErr.Code == errAccessDenied
}

func (c *Client) queryError(operation, table string, err error) error {
	if missingObject(err) {
		return fmt.Errorf("clickhouse: %s %s: %w", operation, table, clients.ErrNotFound)
	}
	return fmt.Errorf("clickhouse: %s %s: %w", operation, table, Redact(err, c.dsn))
}
