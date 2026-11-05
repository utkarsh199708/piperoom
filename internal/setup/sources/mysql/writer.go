package mysql

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"piperoom/internal/clients"
)

// Writer is the MySQL clients.SetupSourceWriter of a setup room
// (docs/design/setup-agent.md §4 item 4), wrapped around the SetupSourceWriter
// that executes the writes: the runner's remote client (remoteclients.Source) in
// a setup room, or SetupWriter when the runner holds its own write pool. It adds
// what the contract cannot express per source: identifier checks before any I/O,
// the canary table's shape, the read user's exact grants, and the two refusals
// MySQL needs (no publications, no ALTER SYSTEM). No method reads or returns a
// password.
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

// errNoPublication is the answer of CreatePublication on every MySQL writer.
func errNoPublication() error {
	return fmt.Errorf("mysql writer: create publication: MySQL has no publications (the connector reads the binlog of the databases and tables it is configured with): %w", clients.ErrNotSupported)
}

// errNoAlterSystem is the answer of AlterSystem on every MySQL writer. MySQL's
// equivalent, SET PERSIST, is deliberately not offered in this wave: it changes
// a server's configuration file semantics (mysqld-auto.cnf overrides my.cnf),
// and a variable that needs a restart needs the person anyway.
func errNoAlterSystem(name string) error {
	return fmt.Errorf("mysql writer: alter system %s: the setup changes no MySQL server variable (no SET PERSIST in this wave; change it in the option file or the RDS parameter group): %w", clipName(name), clients.ErrNotSupported)
}

// CreatePublication answers clients.ErrNotSupported before any I/O.
func (w *Writer) CreatePublication(context.Context, string, []string) error {
	return errNoPublication()
}

// AlterSystem answers clients.ErrNotSupported before any I/O, whatever the name:
// the refusal is decided here, never by the endpoint.
func (w *Writer) AlterSystem(_ context.Context, name, _ string) (bool, error) {
	return false, errNoAlterSystem(name)
}

// CreateCanaryTable creates the ladder's canary table (idempotent). Its shape is
// CanaryTableSQL; the connector needs no key, but the ladder finds its event by
// the nonce and the id gives every row a key.
func (w *Writer) CreateCanaryTable(ctx context.Context, table string) error {
	if _, err := quoteTable(table); err != nil {
		return fmt.Errorf("mysql writer: create canary table: %w", err)
	}
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if err := w.w.CreateCanaryTable(ctx, table); err != nil {
		return fmt.Errorf("mysql writer: create canary table %s: %w", clipName(table), err)
	}
	return nil
}

// InsertCanary inserts one row with nonce (16 random bytes, hex, from the ladder
// runner) into the canary table.
func (w *Writer) InsertCanary(ctx context.Context, table, nonce string) error {
	if _, err := quoteTable(table); err != nil {
		return fmt.Errorf("mysql writer: insert canary: %w", err)
	}
	if !canaryNonceRe.MatchString(nonce) {
		return fmt.Errorf("%w: the canary nonce is 16 random bytes as 32 lower-case hex characters", ErrParams)
	}
	if w == nil || w.w == nil {
		return ErrNoWriter
	}
	if err := w.w.InsertCanary(ctx, table, nonce); err != nil {
		return fmt.Errorf("mysql writer: insert canary into %s: %w", clipName(table), err)
	}
	return nil
}

// CreateReadRole creates user with the ReadUserGrants grants and a generated
// password, and returns only the name the password was stored under. The
// password is never in a return value, an error or this package.
func (w *Writer) CreateReadRole(ctx context.Context, role string, tables []string) (string, error) {
	if err := checkUser(role); err != nil {
		return "", fmt.Errorf("mysql writer: create read role: %w", err)
	}
	if err := checkTables(tables); err != nil {
		return "", fmt.Errorf("mysql writer: create read role %s: %w", clipName(role), err)
	}
	if w == nil || w.w == nil {
		return "", ErrNoWriter
	}
	name, err := w.w.CreateReadRole(ctx, role, tables)
	if err != nil {
		return "", fmt.Errorf("mysql writer: create read role %s: %w", clipName(role), err)
	}
	if !secretNameRe.MatchString(name) {
		return "", fmt.Errorf("mysql writer: create read role %s: the writer returned no usable secret name: %w", clipName(role), ErrParams)
	}
	return name, nil
}

// CanaryTableSQL is the DDL of the ladder's canary table: the nonce the ladder
// looks for is the primary key (the binlog event's key, which the ladder's
// kafka_find_by_key matches — the same shape as the SQL Server and Oracle
// canary tables), an auto-increment column (indexed, as AUTO_INCREMENT
// requires), and the insert time. InnoDB, so the insert is a transaction in
// the binlog like any other. An id-keyed table made by an earlier build is not
// altered (IF NOT EXISTS): drop it and verify again.
func CanaryTableSQL(table string) (string, error) {
	q, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	return "CREATE TABLE IF NOT EXISTS " + q + " (id BIGINT NOT NULL AUTO_INCREMENT, nonce VARCHAR(64) NOT NULL, inserted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), PRIMARY KEY (nonce), KEY piperoom_canary_id (id)) ENGINE=InnoDB", nil
}

// InsertCanarySQL is the canary insert; the nonce is the one parameter.
func InsertCanarySQL(table string) (string, error) {
	q, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	return "INSERT INTO " + q + " (nonce) VALUES (?)", nil
}

// connectorGlobalPrivileges are the global privileges of Debezium's MySQL
// connector docs, in grant order. LOCK TABLES is what RDS/Aurora needs in place
// of the global read lock RELOAD gives; it is granted everywhere because a user
// created for a source that later moves to RDS should not need a second grant.
var connectorGlobalPrivileges = []string{"RELOAD", "SHOW DATABASES", "LOCK TABLES", "REPLICATION SLAVE", "REPLICATION CLIENT"}

// CreateUserSQL creates the connector's user. password is the generated value
// the executing writer made (letters, digits, - and _: randomPassword); CREATE
// USER takes it as a literal, quoted by quoteLiteral, and the statement is never
// logged or wrapped into an error.
func CreateUserSQL(user, password string) (string, error) {
	acct, err := accountLiteral(user)
	if err != nil {
		return "", err
	}
	return "CREATE USER " + acct + " IDENTIFIED BY " + quoteLiteral(password), nil
}

// DropUserSQL drops a user the setup just created and could not finish.
func DropUserSQL(user string) (string, error) {
	acct, err := accountLiteral(user)
	if err != nil {
		return "", err
	}
	return "DROP USER IF EXISTS " + acct, nil
}

// ReadUserGrants is every statement CreateReadRole runs for user besides
// creating it: SELECT on exactly the listed tables (one GRANT each, because
// MySQL takes one table per object-level GRANT) and the connector's global
// privileges. No GRANT OPTION, no write privilege, no schema-wide SELECT.
func ReadUserGrants(user string, tables []string) ([]string, error) {
	acct, err := accountLiteral(user)
	if err != nil {
		return nil, err
	}
	if err := checkTables(tables); err != nil {
		return nil, err
	}
	var out []string
	var done []string
	for _, t := range tables {
		if slices.Contains(done, t) {
			continue
		}
		done = append(done, t)
		q, err := quoteTable(t)
		if err != nil {
			return nil, err
		}
		out = append(out, "GRANT SELECT ON "+q+" TO "+acct)
	}
	out = append(out, "GRANT "+strings.Join(connectorGlobalPrivileges, ", ")+" ON *.* TO "+acct)
	return out, nil
}
