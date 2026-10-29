package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/setup/plan"
)

// Writer is the Postgres clients.SetupSourceWriter of a setup room
// (docs/design/setup-agent.md §4 item 4), wrapped around the SetupSourceWriter
// that executes the writes: the runner's remote client (remoteclients.Source)
// in a setup room, or SetupWriter when the runner holds its own write pool. It
// adds what the contract cannot express per source: identifier checks before
// any I/O, the ALTER SYSTEM allow-list and each parameter's meaning, the
// canary table's shape, and the read role's exact grants. The writes
// themselves are the wrapped writer's; no method reads or returns a password.
type Writer struct{ w clients.SetupSourceWriter }

var _ clients.SetupSourceWriter = (*Writer)(nil)

// NewWriter wraps the source setup writer. A nil writer is ErrNoWriter, so a
// setup room with write mode off gets a refusal at construction, not at the
// first write.
func NewWriter(w clients.SetupSourceWriter) (*Writer, error) {
	if w == nil {
		return nil, ErrNoWriter
	}
	return &Writer{w: w}, nil
}

// CreatePublication creates publication name FOR TABLE tables: the filtered
// form only. Only the pipeline's tables are decoded, and a publication the
// setup did not create is still never altered here (clients.SetupSourceWriter).
func (w *Writer) CreatePublication(ctx context.Context, name string, tables []string) error {
	if _, err := quoteIdent(name); err != nil {
		return fmt.Errorf("postgres writer: create publication: %w", err)
	}
	if err := checkTables(tables); err != nil {
		return fmt.Errorf("postgres writer: create publication %s: %w", clipName(name), err)
	}
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if err := w.w.CreatePublication(ctx, name, tables); err != nil {
		return fmt.Errorf("postgres writer: create publication %s: %w", clipName(name), err)
	}
	return nil
}

// CreateCanaryTable creates the ladder's canary table (idempotent). Its shape
// is CanaryTableSQL; the connector needs a primary key, which the id column
// gives it.
func (w *Writer) CreateCanaryTable(ctx context.Context, table string) error {
	if _, err := quoteTable(table); err != nil {
		return fmt.Errorf("postgres writer: create canary table: %w", err)
	}
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if err := w.w.CreateCanaryTable(ctx, table); err != nil {
		return fmt.Errorf("postgres writer: create canary table %s: %w", clipName(table), err)
	}
	return nil
}

// InsertCanary inserts one row with nonce (16 random bytes, hex, from the
// ladder runner) into the canary table.
func (w *Writer) InsertCanary(ctx context.Context, table, nonce string) error {
	if _, err := quoteTable(table); err != nil {
		return fmt.Errorf("postgres writer: insert canary: %w", err)
	}
	if !canaryNonceRe.MatchString(nonce) {
		return fmt.Errorf("%w: the canary nonce is 16 random bytes as 32 lower-case hex characters", ErrParams)
	}
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if err := w.w.InsertCanary(ctx, table, nonce); err != nil {
		return fmt.Errorf("postgres writer: insert canary into %s: %w", clipName(table), err)
	}
	return nil
}

// CreateReadRole creates role with the ReadRoleGrants grants, a generated
// password and the REPLICATION attribute the connector's slot needs, and
// returns only the name the password was stored under. The password is never
// in a return value, an error or this package.
func (w *Writer) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if _, err := quoteRole(role); err != nil {
		return "", fmt.Errorf("postgres writer: create read role: %w", err)
	}
	if err := checkTables(tables); err != nil {
		return "", fmt.Errorf("postgres writer: create read role %s: %w", clipName(role), err)
	}
	if w == nil || w.w == nil {
		return "", ErrNoWriter
	}
	name, err := w.w.CreateReadRole(ctx, role, tables)
	if err != nil {
		return "", fmt.Errorf("postgres writer: create read role %s: %w", clipName(role), err)
	}
	if !secretNameRe.MatchString(name) {
		return "", fmt.Errorf("postgres writer: create read role %s: the writer returned no usable secret name: %w", clipName(role), ErrParams)
	}
	return name, nil
}

// AlterSystem sets one allow-listed server setting and reports whether it
// needs a restart. A name outside plan.AlterSystemParams, or a value the
// server could not run with, is refused before any I/O: the refusal is decided
// here, never by the endpoint.
func (w *Writer) AlterSystem(ctx context.Context, name, value string) (bool, error) {
	if err := validateAlterSystem(name, value); err != nil {
		return false, err
	}
	if w == nil || w.w == nil {
		return false, ErrNoWriter
	}
	pending, err := w.w.AlterSystem(ctx, name, value)
	if err != nil {
		return false, fmt.Errorf("postgres writer: alter system %s: %w", name, err)
	}
	return pending, nil
}

// alterSystemRule is one allow-listed parameter's meaning rule. The contract
// (clients.SetupSourceWriter.AlterSystem) fixes the name allow-list; this
// package adds the values, because a name on the list with a value Postgres
// refuses to start with (wal_level = minimal next to max_wal_senders > 0) would
// turn a maintenance window into an outage.
type alterSystemRule struct {
	// text is the accepted values, in the refusal the person reads.
	text string
	ok   func(string) bool
}

// walSizesRe is a Postgres size with an explicit unit. A bare number is
// refused: the setting's unit is not part of the value, so "1024" would mean
// something the person did not type.
var walSizesRe = regexp.MustCompile(`^[0-9]+(kB|MB|GB|TB)$`)

// durationsRe is a Postgres time with an optional unit (bare = milliseconds,
// as Postgres reads it).
var durationsRe = regexp.MustCompile(`^[0-9]+(ms|s|min|h|d)?$`)

// positiveIntRe is a positive decimal integer.
var positiveIntRe = regexp.MustCompile(`^[1-9][0-9]*$`)

// alterSystemRules is the documented value allow-list, one entry per parameter
// of plan.AlterSystemParams:
//
//   - wal_level: logical or replica. logical is what pgoutput needs; replica is
//     the safe floor on a server whose other consumers still use physical
//     replication. minimal never: with max_wal_senders > 0 (this same allow-list)
//     Postgres refuses to start, and no CDC setup ever wants it.
//   - max_wal_senders: a positive integer (the connector's walsender is one).
//   - max_replication_slots: a positive integer (the connector's slot is one).
//   - max_slot_wal_keep_size: -1 (unlimited) or a size with a unit (10GB); a
//     bare number is refused, because its unit is not the person's.
//   - wal_sender_timeout: a positive duration (30s, 1min). Zero disables the
//     timeout, which lets a dead connection hold back WAL forever.
var alterSystemRules = map[string]alterSystemRule{
	"wal_level": {
		text: "logical or replica (minimal breaks logical decoding and Postgres refuses to start with it while max_wal_senders > 0)",
		ok:   func(v string) bool { return v == "logical" || v == "replica" },
	},
	"max_wal_senders": {
		text: "a positive integer (at least 1: the connector's walsender needs one)",
		ok:   positiveIntRe.MatchString,
	},
	"max_replication_slots": {
		text: "a positive integer (at least 1: the connector creates one logical slot)",
		ok:   positiveIntRe.MatchString,
	},
	"max_slot_wal_keep_size": {
		text: "-1 (retain without limit) or a positive size with a unit such as 10GB; a bare number is refused because its unit would be a guess, and 0 makes every slot unreserved at once",
		ok:   func(v string) bool { return v == "-1" || (walSizesRe.MatchString(v) && positiveLeadingNumber(v)) },
	},
	"wal_sender_timeout": {
		text: "a positive duration such as 30s or 1min; 0 disables the timeout, which lets a dead connection hold back WAL",
		ok:   func(v string) bool { return durationsRe.MatchString(v) && positiveLeadingNumber(v) },
	},
}

// positiveLeadingNumber reports whether v's leading digits are a positive
// number ("500ms", "10GB", "0s" and "0" are not).
func positiveLeadingNumber(v string) bool {
	n := 0
	for _, r := range v {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n > 0
}

// validateAlterSystem is the allow-list and the meaning check, in that order.
// Both run before any I/O and before the writer is inspected, so a write-mode
// off room still gets the precise refusal.
func validateAlterSystem(name, value string) error {
	rule, ok := alterSystemRules[name]
	if !ok {
		return fmt.Errorf("%w: %q is not on the ALTER SYSTEM allow-list (%s)", ErrParams, clipName(name), strings.Join(plan.AlterSystemParams(), ", "))
	}
	if !settingValueRe.MatchString(value) {
		return fmt.Errorf("%w: the value of %s must be letters, digits, '.', '_', '+', '-' or a space (at most 64 bytes)", ErrParams, name)
	}
	if !rule.ok(value) {
		return fmt.Errorf("%w: %s = %q is a value Postgres cannot use here: %s", ErrParams, name, clipName(value), rule.text)
	}
	return nil
}

// CanaryTableSQL is the DDL of the ladder's canary table: the nonce the ladder
// looks for is the primary key, so REPLICA IDENTITY DEFAULT keys the canary's
// change event by the nonce and the ladder's kafka_find_by_key can match it —
// the same shape as the SQL Server and Oracle canary tables. An id-keyed table
// made by an earlier build is not altered (IF NOT EXISTS): drop it and verify
// again.
func CanaryTableSQL(table string) (string, error) {
	q, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	return "CREATE TABLE IF NOT EXISTS " + q + " (id bigint GENERATED ALWAYS AS IDENTITY, nonce text NOT NULL PRIMARY KEY, inserted_at timestamptz NOT NULL DEFAULT now())", nil
}

// InsertCanarySQL is the canary insert; the nonce is the one parameter.
func InsertCanarySQL(table string) (string, error) {
	q, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	return "INSERT INTO " + q + " (nonce) VALUES ($1)", nil
}

// ReadRoleGrants is every statement CreateReadRole runs for role besides
// creating it: SELECT on exactly the listed tables and membership in
// pg_read_all_stats, nothing else (no schema-wide or ALL TABLES grant, no
// write privilege, no pg_monitor).
func ReadRoleGrants(role string, tables []string) ([]string, error) {
	rq, err := quoteRole(role)
	if err != nil {
		return nil, err
	}
	if err := checkTables(tables); err != nil {
		return nil, err
	}
	quoted := make([]string, len(tables))
	for i, t := range tables {
		if quoted[i], err = quoteTable(t); err != nil {
			return nil, err
		}
	}
	stats, err := quoteIdent(StatsRole)
	if err != nil {
		return nil, err
	}
	return []string{
		"GRANT SELECT ON TABLE " + strings.Join(quoted, ", ") + " TO " + rq,
		"GRANT " + stats + " TO " + rq,
	}, nil
}

// CreatePublicationSQL is the filtered publication's DDL: exactly the pipeline
// tables, never FOR ALL TABLES (that needs superuser and decodes every table in
// the database, outside the least-privilege model this setup is built on).
func CreatePublicationSQL(name string, tables []string) (string, error) {
	q, err := quoteIdent(name)
	if err != nil {
		return "", err
	}
	if err := checkTables(tables); err != nil {
		return "", err
	}
	quoted := make([]string, len(tables))
	for i, t := range tables {
		if quoted[i], err = quoteTable(t); err != nil {
			return "", err
		}
	}
	return "CREATE PUBLICATION " + q + " FOR TABLE " + strings.Join(quoted, ", "), nil
}
