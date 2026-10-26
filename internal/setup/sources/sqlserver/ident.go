package sqlserver

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	sqlclient "piperoom/internal/clients/sqlserver"
)

// Errors of the driver and the writer. Compare with errors.Is.
var (
	// ErrIdentifier: a name is not a bare SQL Server identifier (rule 6).
	ErrIdentifier = errors.New("setup/sources/sqlserver: invalid identifier")
	// ErrNoTables: an operation needs the pipeline's source tables and was
	// given none.
	ErrNoTables = errors.New("setup/sources/sqlserver: no source tables")
	// ErrNoWriter: the setup writer is absent (write mode off, or no runner).
	ErrNoWriter = errors.New("setup/sources/sqlserver: no source writer (write mode on and a paired runner are needed)")
	// ErrParams: a parameter is outside the range this package accepts.
	ErrParams = errors.New("setup/sources/sqlserver: bad parameter")
)

// maxIdentChars is sysname's length: a longer name cannot exist. Every name
// here is ASCII (identRe), so bytes are characters. The capture instance limit
// (sp_cdc_enable_table's 100 characters) is sqlclient.MaxCaptureInstanceChars,
// applied by the derivation this package shares with the executing writer
// (checkCaptureInstance).
const maxIdentChars = 128

var (
	identRe       = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	hostnameRe    = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)
	secretNameRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)
	topicPrefixRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$`)
	topicRe       = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
	canaryNonceRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// systemDatabases cannot have CDC enabled (sp_cdc_enable_db refuses them).
var systemDatabases = []string{"master", "model", "msdb", "tempdb"}

// reservedLogins are principals a setup must never create, grant to or name
// as the connector's login: the administrator, the database's built-in users
// (dbo, guest, sys, INFORMATION_SCHEMA), the cdc user and schema that
// sp_cdc_enable_db creates (CreateReadRole's CREATE USER would land on it and
// the grants would reach CDC's own principal), and the fixed server roles (a
// login of such a name would read as one). Matched case-insensitively, as SQL
// Server resolves principal names under its usual collations.
var reservedLogins = []string{
	"sa", "public", "dbo", "guest", "sys", "information_schema", "cdc",
	"sysadmin", "serveradmin", "securityadmin", "processadmin", "setupadmin",
	"bulkadmin", "diskadmin", "dbcreator",
}

// clipName cuts a name for an error message (every name here is ASCII).
func clipName(s string) string {
	if len(s) <= 80 {
		return s
	}
	return s[:80] + "…"
}

// checkIdent checks one name against rule 6 and the sysname limit.
func checkIdent(name string) error {
	if len(name) == 0 || len(name) > maxIdentChars || !identRe.MatchString(name) {
		return fmt.Errorf("%w: %q is not a SQL Server identifier (letters, digits and _, not starting with a digit, at most %d characters)", ErrIdentifier, clipName(name), maxIdentChars)
	}
	return nil
}

// tableParts splits and checks one "schema.table" name.
func tableParts(table string) (schema, name string, err error) {
	schema, name, ok := strings.Cut(table, ".")
	if !ok || strings.Contains(name, ".") {
		return "", "", fmt.Errorf("%w: %q is not \"schema.table\"", ErrIdentifier, clipName(table))
	}
	if err := checkIdent(schema); err != nil {
		return "", "", err
	}
	if err := checkIdent(name); err != nil {
		return "", "", err
	}
	return schema, name, nil
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

// checkLogin checks the connector's login name: an identifier, and not a
// reserved principal or a db_ fixed database role.
func checkLogin(login string) error {
	if err := checkIdent(login); err != nil {
		return err
	}
	l := strings.ToLower(login)
	if slices.Contains(reservedLogins, l) || strings.HasPrefix(l, "db_") {
		return fmt.Errorf("%w: %q is a reserved principal or fixed role", ErrIdentifier, login)
	}
	return nil
}

// checkDatabase checks the source database: an identifier, not a system
// database.
func checkDatabase(db string) error {
	if err := checkIdent(db); err != nil {
		return err
	}
	if slices.Contains(systemDatabases, strings.ToLower(db)) {
		return fmt.Errorf("%w: %q is a system database; CDC cannot be enabled on it", ErrParams, db)
	}
	return nil
}

// checkCaptureInstance checks the CreatePublication name against the capture
// instances the executing writer derives from it. sqlclient.CaptureInstances
// is the writer's own derivation (name for one table, name_schema_table for
// each of several, a duplicate counted once) and carries sp_cdc_enable_table's
// limit, so the driver can never accept a name the writer then refuses, nor
// check one the writer never creates.
func checkCaptureInstance(name string, tables []string) error {
	if err := checkIdent(name); err != nil {
		return err
	}
	if _, err := sqlclient.CaptureInstances(name, tables); err != nil {
		return fmt.Errorf("%w: %w", ErrParams, err)
	}
	return nil
}

// bracket quotes a checked identifier for an ask's example statement.
func bracket(name string) string {
	if checkIdent(name) != nil {
		return clipName(name)
	}
	return "[" + name + "]"
}

// bracketTable quotes a checked "schema.table" for an ask's example statement.
func bracketTable(table string) string {
	s, t, err := tableParts(table)
	if err != nil {
		return clipName(table)
	}
	return "[" + s + "].[" + t + "]"
}
