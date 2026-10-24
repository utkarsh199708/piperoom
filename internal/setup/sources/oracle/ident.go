package oracle

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Errors of the driver and the writer. Compare with errors.Is.
var (
	// ErrIdentifier: a name is not an Oracle identifier this driver accepts
	// (rule 6, widened by '$' and '#' the way the U9a client widens it).
	ErrIdentifier = errors.New("setup/sources/oracle: invalid identifier")
	// ErrNoTables: an operation needs the pipeline's source tables and was
	// given none.
	ErrNoTables = errors.New("setup/sources/oracle: no source tables")
	// ErrNoWriter: the setup writer is absent (write mode off, or no runner).
	ErrNoWriter = errors.New("setup/sources/oracle: no source writer (write mode on and a paired runner are needed)")
	// ErrParams: a parameter is outside the range this package accepts.
	ErrParams = errors.New("setup/sources/oracle: bad parameter")
)

// maxIdentBytes is Oracle's identifier limit from 12.2 on (128 bytes).
const maxIdentBytes = 128

var (
	// identRe is AGENTS.md §4 rule 6 for Oracle: the U9a client's shape
	// (letters, digits, '_', '$' and '#', not starting with a digit), so a
	// common user such as C##DBZUSER passes. Lower case is refused separately
	// (upperIdent), because a quoted name is case-sensitive and Oracle stores
	// an unquoted name in upper case.
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$#]*$`)
	// secretNameRe is a name in the runner's secret store.
	secretNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
	// topicPrefixRe is a Debezium topic prefix: the topics it makes are
	// <prefix>.<owner>.<table>.
	topicPrefixRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$`)
	// topicRe is a Kafka topic name (the schema history topic).
	topicRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
	// dbNameRe is an Oracle database (CDB) name, the DB_NAME parameter: at
	// most 8 characters by Oracle's rule, letters first. 30 is accepted so a
	// service-style name a person typed gets the read's verdict, not a guess.
	dbNameRe = regexp.MustCompile(`^[A-Z][A-Z0-9_$#]{0,29}$`)
	// hostnameRe is a DNS name or a host label; an IP literal is accepted by
	// net.ParseIP instead.
	hostnameRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)
	// canaryNonceRe is the ladder's nonce: 16 random bytes in lower-case hex
	// (design §3.4). Anything else is not a nonce this room generated.
	canaryNonceRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// clipName cuts a name for an error message on a byte boundary.
func clipName(s string) string {
	if len(s) <= 80 {
		return s
	}
	return s[:80] + "…"
}

// upperIdent checks one identifier: the rule 6 shape, the length limit and
// upper case. A lower-case letter is refused rather than folded: the U9a
// client quotes every name (case-sensitive), so "orders" would name a table
// that an unquoted CREATE TABLE orders never made.
func upperIdent(name string) error {
	if len(name) == 0 || len(name) > maxIdentBytes || !identRe.MatchString(name) {
		return fmt.Errorf("%w: %q is not an Oracle identifier (letters, digits, '_', '$' and '#', not starting with a digit, at most %d bytes)", ErrIdentifier, clipName(name), maxIdentBytes)
	}
	if name != strings.ToUpper(name) {
		return fmt.Errorf("%w: %q has lower-case letters: give the name as the dictionary stores it (%s; Oracle upper-cases an unquoted name, and a quoted mixed-case name is not supported by this driver)", ErrIdentifier, clipName(name), clipName(strings.ToUpper(name)))
	}
	return nil
}

// quoteIdent checks name and quotes it the way Oracle quotes identifiers.
// identRe never admits a double quote, so there is nothing to escape.
func quoteIdent(name string) (string, error) {
	if err := upperIdent(name); err != nil {
		return "", err
	}
	return `"` + name + `"`, nil
}

// tableParts splits and checks one "OWNER.TABLE" name.
func tableParts(table string) (owner, name string, err error) {
	owner, name, ok := strings.Cut(table, ".")
	if !ok || strings.Contains(name, ".") {
		return "", "", fmt.Errorf("%w: %q is not \"OWNER.TABLE\"", ErrIdentifier, clipName(table))
	}
	if err := upperIdent(owner); err != nil {
		return "", "", err
	}
	if err := upperIdent(name); err != nil {
		return "", "", err
	}
	return owner, name, nil
}

// quoteTable checks one "OWNER.TABLE" name and quotes both parts.
func quoteTable(table string) (string, error) {
	owner, name, err := tableParts(table)
	if err != nil {
		return "", err
	}
	return `"` + owner + `"."` + name + `"`, nil
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

// reservedUsers are accounts the connector must never log in as: PUBLIC is
// every user, SYS cannot log in without SYSDBA, and the Debezium docs warn
// against SYS and SYSTEM for LogMiner sessions.
var reservedUsers = map[string]bool{"PUBLIC": true, "SYS": true, "SYSTEM": true}

// checkUser checks the connector's database.user.
func checkUser(user string) error {
	if user == "" {
		return fmt.Errorf("%w: the connector's database.user is required", ErrParams)
	}
	if err := upperIdent(user); err != nil {
		return fmt.Errorf("database.user: %w", err)
	}
	if reservedUsers[user] {
		return fmt.Errorf("%w: database.user %s is reserved (PUBLIC is every user; SYS and SYSTEM are not LogMiner accounts)", ErrIdentifier, user)
	}
	return nil
}

// isCommonUser reports whether user is a common user name of a CDB: Oracle's
// default COMMON_USER_PREFIX is C##.
func isCommonUser(user string) bool {
	return strings.HasPrefix(user, "C##") && len(user) > 3
}
