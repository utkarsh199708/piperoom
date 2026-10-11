package snowflake

import (
	"errors"
	"fmt"

	"github.com/snowflakedb/gosnowflake"

	"piperoom/internal/clients"
)

// errObjectDoesNotExist is the Snowflake compilation error number for
// "SQL compilation error: Object does not exist, or operation cannot be
// performed" (SQLSTATE 42S02): the table, or the schema/database it is
// qualified with, is missing or not visible to the session's role.
const errObjectDoesNotExist = 2003

// wrapQueryErr wraps a query failure, mapping a missing table or schema to
// clients.ErrNotFound so callers can use errors.Is the same way they do for
// every other client.
func wrapQueryErr(op, table string, err error) error {
	var sfErr *gosnowflake.SnowflakeError
	if errors.As(err, &sfErr) && (sfErr.Number == errObjectDoesNotExist || sfErr.SQLState == "42S02") {
		return fmt.Errorf("snowflake: %s %s: %w", op, table, clients.ErrNotFound)
	}
	return fmt.Errorf("snowflake: %s %s: %w", op, table, err)
}
