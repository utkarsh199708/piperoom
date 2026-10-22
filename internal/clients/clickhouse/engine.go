package clickhouse

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/clients"
)

// EngineGroupReader reads the Kafka consumer group of a ClickHouse Kafka
// engine table (optional; asserted at use time by the topology builder).
type EngineGroupReader interface {
	// EngineConsumerGroup returns the group id the engine table consumes with,
	// as its CREATE statement sets kafka_group_name. It is read with a
	// read-only SHOW CREATE TABLE; the rest of the statement is not returned
	// (it can name broker credentials).
	EngineConsumerGroup(ctx context.Context, table string) (string, error)
}

// engineGroupRE matches kafka_group_name in a Kafka engine table's CREATE
// statement. ClickHouse renders settings as `name = 'value'` (single-quoted,
// backslash-escaped); SHOW CREATE TABLE, not system.kafka_consumers, is the
// source because the system table reports a consumer id and topic assignments,
// never the group id.
var engineGroupRE = regexp.MustCompile(`(?i)\bkafka_group_name\s*=\s*'((?:[^'\\]|\\.)*)'`)

// engineGroupIDRE is the subset of Kafka group ids that is surfaced. A group id
// is an opaque string to Kafka, but the map and tool summaries show it and
// accept it back as scope, so a value with spaces, quotes or control
// characters is refused: it is not a plain group id and could forge map or
// summary lines. Length is Kafka's short-string limit.
var engineGroupIDRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)

// EngineConsumerGroup returns the Kafka consumer group of a ClickHouse Kafka
// engine table. It reads the table's CREATE statement with SHOW CREATE TABLE
// (read-only; SELECT on the table is enough, verified against the dev
// ClickHouse 25.8) and parses kafka_group_name.
//
// A table that does not exist is clients.ErrNotFound. A table that is not a
// Kafka engine table, or sets no usable kafka_group_name, is
// clients.ErrNotSupported: there is no sink consumer to watch, which is not a
// failure. Any other error (unreachable, access denied) is returned as it came
// from the server, with the DSN redacted.
func (c *Client) EngineConsumerGroup(ctx context.Context, table string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	// No bind parameters: SHOW CREATE TABLE takes an identifier, not a value.
	rows, err := c.query(ctx, "SHOW CREATE TABLE "+qualified)
	if err != nil {
		return "", c.queryError("consumer group of", table, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", c.queryError("consumer group of", table, err)
		}
		return "", fmt.Errorf("clickhouse: consumer group of %s: SHOW CREATE TABLE returned no row", table)
	}
	var raw any
	if err := rows.Scan(&raw); err != nil {
		return "", c.queryError("decode consumer group of", table, err)
	}
	if err := rows.Err(); err != nil {
		return "", c.queryError("consumer group of", table, err)
	}
	ddl, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("clickhouse: consumer group of %s: SHOW CREATE TABLE returned %T, want a string", table, raw)
	}
	group, err := engineGroup(ddl)
	if err != nil {
		return "", fmt.Errorf("clickhouse: consumer group of %s: %w", table, err)
	}
	return group, nil
}

// engineGroup parses kafka_group_name out of one CREATE statement. The statement
// itself is never returned or logged: a Kafka engine table names its broker
// credentials in the same SETTINGS clause.
func engineGroup(ddl string) (string, error) {
	m := engineGroupRE.FindStringSubmatch(ddl)
	if m == nil {
		return "", fmt.Errorf("%w: the table is not a Kafka engine table or sets no kafka_group_name", clients.ErrNotSupported)
	}
	group := unescapeQuoted(m[1])
	if !engineGroupIDRE.MatchString(group) {
		return "", fmt.Errorf("%w: kafka_group_name is not a plain Kafka group id", clients.ErrNotSupported)
	}
	return group, nil
}

// unescapeQuoted undoes ClickHouse's backslash escaping inside a single-quoted
// string literal (\' and \\, and \\x for any other x).
func unescapeQuoted(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

var _ EngineGroupReader = (*Client)(nil)
