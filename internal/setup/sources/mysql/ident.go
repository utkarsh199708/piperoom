package mysql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Errors of the driver and the writer. Compare with errors.Is.
var (
	// ErrIdentifier: a name is not a bare MySQL identifier (rule 6).
	ErrIdentifier = errors.New("setup/sources/mysql: invalid identifier")
	// ErrNoTables: an operation needs the pipeline's source tables and was
	// given none.
	ErrNoTables = errors.New("setup/sources/mysql: no source tables")
	// ErrNoWriter: the setup writer is absent (write mode off, or no runner).
	ErrNoWriter = errors.New("setup/sources/mysql: no source writer (write mode on and a paired runner are needed)")
	// ErrParams: a parameter is outside the range this package accepts.
	ErrParams = errors.New("setup/sources/mysql: bad parameter")
)

// Length limits: a MySQL identifier is at most 64 characters, a user name 32.
const (
	maxIdentBytes = 64
	maxUserBytes  = 32
)

var (
	// identRe is AGENTS.md §4 rule 6 with MySQL's 64-character cap: the only
	// identifiers this package quotes.
	identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)
	// hostRe is the account-host rule of the design (§5.6): the wildcard % and
	// IPv6 colons, but no quote, backslash or space.
	hostRe = regexp.MustCompile(`^[A-Za-z0-9_.:%-]{1,255}$`)
	// secretNameRe is a name in the runner's secret store.
	secretNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
	// topicPrefixRe is a Debezium topic prefix: the topics it makes are
	// <prefix>.<db>.<table>.
	topicPrefixRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$`)
	// canaryNonceRe is the ladder's nonce: 16 random bytes in lower-case hex.
	canaryNonceRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
	// accountRe is one account as the Q8 client reports it (user@host): the
	// identifier rule for the user and hostRe for the host. A value read from
	// mysql.user is echoed into an ask only when it has exactly this shape.
	accountRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,31}@[A-Za-z0-9_.:%-]{1,255}$`)
)

// maxAskAccounts bounds how many accounts an ask lists; the rest are a count.
const maxAskAccounts = 5

// safeAccounts joins, for an ask, the accounts that pass accountRe: at most
// maxAskAccounts, then a count. The accounts are rows of the source's grant
// tables, data from the customer system that reaches the room's prompt through
// the ask, so a value that is not shaped like an account is left out, never
// echoed. It returns "" when no account qualifies.
func safeAccounts(accounts []string) string {
	var out []string
	for _, a := range accounts {
		if accountRe.MatchString(a) {
			out = append(out, a)
		}
	}
	switch {
	case len(out) == 0:
		return ""
	case len(out) > maxAskAccounts:
		return strings.Join(out[:maxAskAccounts], ", ") + fmt.Sprintf(" and %d more", len(out)-maxAskAccounts)
	}
	return strings.Join(out, ", ")
}

// readerUserHost is the host part of the account CreateReadRole makes. The
// contract gives the method no host, and the connector's address is not known
// to the source's owner; '%' matches the dev profile's debezium user. A person
// who wants a narrower host creates the user by hand (the read_role check
// names the grants).
const readerUserHost = "%"

// clipName cuts a name for an error message on a byte boundary (every name
// clipped here is ASCII or a refused value, so a split rune only shortens it).
func clipName(s string) string {
	if len(s) <= 80 {
		return s
	}
	return s[:80] + "…"
}

// quoteIdent checks name against identRe and backtick-quotes it. The check
// runs first, per rule 6; it also guarantees no backtick can appear.
func quoteIdent(name string) (string, error) {
	if !identRe.MatchString(name) {
		return "", fmt.Errorf("%w: %q is not a MySQL identifier (letters, digits and _, not starting with a digit, at most %d characters)", ErrIdentifier, clipName(name), maxIdentBytes)
	}
	return "`" + name + "`", nil
}

// tableParts splits and checks one "db.table" name.
func tableParts(table string) (db, name string, err error) {
	db, name, ok := strings.Cut(table, ".")
	if !ok || strings.Contains(name, ".") {
		return "", "", fmt.Errorf("%w: %q is not \"db.table\"", ErrIdentifier, clipName(table))
	}
	if _, err := quoteIdent(db); err != nil {
		return "", "", err
	}
	if _, err := quoteIdent(name); err != nil {
		return "", "", err
	}
	return db, name, nil
}

// quoteTable checks one "db.table" name and quotes both parts.
func quoteTable(table string) (string, error) {
	db, name, err := tableParts(table)
	if err != nil {
		return "", err
	}
	return "`" + db + "`.`" + name + "`", nil
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

// checkUser checks a user name: the identifier rule and MySQL's 32-character
// limit. The reserved names are accounts the setup must never create or change.
func checkUser(user string) error {
	if user == "" {
		return fmt.Errorf("%w: no user name", ErrIdentifier)
	}
	if len(user) > maxUserBytes {
		return fmt.Errorf("%w: user name is %d characters, at most %d", ErrIdentifier, len(user), maxUserBytes)
	}
	if !identRe.MatchString(user) {
		return fmt.Errorf("%w: user %q (letters, digits and _, not starting with a digit)", ErrIdentifier, clipName(user))
	}
	switch strings.ToLower(user) {
	case "root", "mysql", "mariadb", "rdsadmin", "admin", "debian_sys_maint":
		return fmt.Errorf("%w: %q is a reserved account name", ErrIdentifier, user)
	}
	if strings.HasPrefix(strings.ToLower(user), "mysql_") {
		return fmt.Errorf("%w: %q is in the reserved mysql_ account namespace", ErrIdentifier, user)
	}
	return nil
}

// quoteLiteral is s as a single-quoted MySQL string literal: a backslash and a
// quote are doubled, so under any sql_mode the text cannot close the literal.
func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'"
}

// accountLiteral is 'user'@'host' for a checked user and the fixed host.
func accountLiteral(user string) (string, error) {
	if err := checkUser(user); err != nil {
		return "", err
	}
	if !hostRe.MatchString(readerUserHost) {
		return "", fmt.Errorf("%w: account host", ErrIdentifier)
	}
	return quoteLiteral(user) + "@" + quoteLiteral(readerUserHost), nil
}
