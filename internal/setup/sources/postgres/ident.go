package postgres

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Errors of the driver and the writer. Compare with errors.Is.
var (
	// ErrIdentifier: a name is not a bare Postgres identifier (rule 6).
	ErrIdentifier = errors.New("setup/sources/postgres: invalid identifier")
	// ErrNoTables: an operation needs the pipeline's source tables and was
	// given none.
	ErrNoTables = errors.New("setup/sources/postgres: no source tables")
	// ErrNoWriter: the setup writer is absent (write mode off, or no runner).
	ErrNoWriter = errors.New("setup/sources/postgres: no source writer (write mode on and a paired runner are needed)")
	// ErrParams: a parameter is outside the range this package accepts.
	ErrParams = errors.New("setup/sources/postgres: bad parameter")
)

// maxIdentBytes is NAMEDATALEN - 1: a longer name cannot exist in Postgres.
const maxIdentBytes = 63

var (
	// identRe is AGENTS.md §4 rule 6: the only identifiers this package
	// quotes. Quoting alone is safe, but a name outside this shape is never
	// one a setup created, so it is refused rather than guessed at.
	identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	// slotNameRe is what Postgres accepts as a replication slot name
	// (lower-case letters, digits and _, at most 63 bytes).
	slotNameRe = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
	// settingValueRe is the shape of an ALTER SYSTEM value: no whitespace
	// beyond a space, no quote, no semicolon, so a value can never end the
	// statement even on a path that interpolates instead of parameterising.
	settingValueRe = regexp.MustCompile(`^[A-Za-z0-9_.+\- ]{1,64}$`)
	// secretNameRe is a name in the runner's secret store, the shape the
	// runner writes (<pipeline>/<name>).
	secretNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
	// connectorNameRe is Kafka Connect's connector name rule.
	connectorNameRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,254}$`)
	// databaseRe is a Postgres database name as a setup may type it. A name
	// outside this shape can exist, but it is not one this driver creates.
	databaseRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.$-]{0,62}$`)
	// topicPrefixRe is a Debezium topic prefix: the topics it makes are
	// <prefix>.<schema>.<table>.
	topicPrefixRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$`)
	// canaryNonceRe is the ladder's nonce: 16 random bytes in lower-case hex
	// (design §3.4). Anything else is not a nonce this room generated.
	canaryNonceRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// clipName cuts a name for an error message, on a byte boundary (every name
// this package clips is ASCII, so no rune is split).
func clipName(s string) string {
	if len(s) <= 80 {
		return s
	}
	return s[:80] + "…"
}

// quoteIdent checks name against identRe and quotes it. The check runs before
// Sanitize, per rule 6; Sanitize alone would accept anything.
func quoteIdent(name string) (string, error) {
	if len(name) == 0 || len(name) > maxIdentBytes || !identRe.MatchString(name) {
		return "", fmt.Errorf("%w: %q is not a Postgres identifier (letters, digits and _, not starting with a digit, at most %d bytes)", ErrIdentifier, clipName(name), maxIdentBytes)
	}
	return pgx.Identifier{name}.Sanitize(), nil
}

// tableParts splits and checks one "schema.table" name.
func tableParts(table string) (schema, name string, err error) {
	schema, name, ok := strings.Cut(table, ".")
	if !ok || strings.Contains(name, ".") {
		return "", "", fmt.Errorf("%w: %q is not \"schema.table\"", ErrIdentifier, clipName(table))
	}
	if _, err := quoteIdent(schema); err != nil {
		return "", "", err
	}
	if _, err := quoteIdent(name); err != nil {
		return "", "", err
	}
	return schema, name, nil
}

// quoteTable checks one "schema.table" name and quotes both parts.
func quoteTable(table string) (string, error) {
	schema, name, err := tableParts(table)
	if err != nil {
		return "", err
	}
	return pgx.Identifier{schema, name}.Sanitize(), nil
}

// checkTables checks a non-empty table list before any I/O.
func checkTables(tables []string) error {
	if len(tables) == 0 {
		return ErrNoTables
	}
	for _, t := range tables {
		if _, _, err := tableParts(t); err != nil {
			return err
		}
	}
	return nil
}

// quoteRole checks a role name for a grant and quotes it. Grant targets have
// their own refusals: PUBLIC and the current-session names would reach every
// role, and the pg_ namespace holds predefined roles whose members would
// inherit the grant.
func quoteRole(role string) (string, error) {
	q, err := quoteIdent(role)
	if err != nil {
		return "", err
	}
	switch strings.ToLower(role) {
	case "public", "current_user", "current_role", "session_user":
		return "", fmt.Errorf("%w: %q is a reserved grantee (it names the current session or every role)", ErrIdentifier, role)
	}
	if strings.HasPrefix(strings.ToLower(role), "pg_") {
		return "", fmt.Errorf("%w: %q is in the reserved pg_ role namespace", ErrIdentifier, role)
	}
	return q, nil
}

// quoteSlot checks a replication slot name (already lower-case) and quotes it.
func quoteSlot(slot string) (string, error) {
	if !slotNameRe.MatchString(slot) {
		return "", fmt.Errorf("%w: slot %q (lower-case letters, digits and _, at most 63 bytes)", ErrIdentifier, clipName(slot))
	}
	return pgx.Identifier{slot}.Sanitize(), nil
}
