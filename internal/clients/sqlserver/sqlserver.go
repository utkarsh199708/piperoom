// Package sqlserver implements the SQL Server client: a read-only half (whether
// CDC is enabled per database and per table, the SQL Server Agent state, the
// capture login's grants, and the server version and edition) and, on a
// separate write pool, the setup write half (clients.SetupSourceWriter in
// writer.go: CreatePublication via sp_cdc_enable_db / sp_cdc_enable_table, the
// canary table, the read role, and AlterSystem as clients.ErrNotSupported).
//
// Every method applies a 10 s deadline to the context. Names the caller
// supplies are checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ (AGENTS.md rule 6)
// before use; values are always passed as query parameters (@pN), never
// interpolated into the statement text, which is stronger than quoting.
// quoteIdent holds the bracket-quoting form the writer interpolates for the
// identifiers it cannot pass as parameters (CREATE TABLE, CREATE LOGIN, GRANT).
//
// The read client is the SQL Server subset of the planned clients.SourceSettingsReader
// (tasks/Q-board.md Q2a). That interface is not yet in internal/clients/clients.go,
// so this package declares its own SettingsReader adapter; the PR requests the
// contract change.
package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"piperoom/internal/clients/sqlserver/sqlservertypes"

	// Blank import registers the "sqlserver" database/sql driver.
	_ "github.com/microsoft/go-mssqldb"
)

// callTimeout bounds every call to the server (AGENTS.md §4 rule 4).
const callTimeout = 10 * time.Second

// db is the part of *sql.DB the client uses; tests substitute a fake.
type db interface {
	Query(ctx context.Context, query string, args ...any) (rows, error)
	QueryRow(ctx context.Context, query string, args ...any) scanner
	Ping(ctx context.Context) error
	Close() error
}

// rows is the part of *sql.Rows the client scans; tests substitute a fake.
type rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// scanner is the part of *sql.Row the client scans; tests substitute a fake.
type scanner interface {
	Scan(dest ...any) error
}

// sqlDB adapts *sql.DB to the db interface. Close is promoted from *sql.DB.
type sqlDB struct{ *sql.DB }

func (d sqlDB) Query(ctx context.Context, query string, args ...any) (rows, error) {
	return d.DB.QueryContext(ctx, query, args...)
}

func (d sqlDB) QueryRow(ctx context.Context, query string, args ...any) scanner {
	return d.DB.QueryRowContext(ctx, query, args...)
}

func (d sqlDB) Ping(ctx context.Context) error { return d.DB.PingContext(ctx) }

// The read client's result types and SettingsReader live in sqlservertypes,
// which imports no database driver: the server's remote client over the
// runner (internal/setup/remoteclients) and the runner's ops decode into them
// without linking go-mssqldb into the server binary. They are aliased here so
// this package's callers are unchanged.
type (
	// ServerInfo is the server version and edition.
	ServerInfo = sqlservertypes.ServerInfo
	// AgentState reports whether SQL Server Agent is running.
	AgentState = sqlservertypes.AgentState
	// TableCDC reports CDC enablement and the primary key for one table.
	TableCDC = sqlservertypes.TableCDC
	// RoleInfo reports the capture login's grants in the client's database.
	RoleInfo = sqlservertypes.RoleInfo
	// SettingsReader is the read-only SQL Server client the setup driver
	// (U4b) uses.
	SettingsReader = sqlservertypes.SettingsReader
)

// Client is a read-only SQL Server client.
type Client struct {
	db db
}

// New opens a connection pool to dsn (a go-mssqldb URL such as
// "sqlserver://user:pass@host:1433?database=db", or an ADO connection string)
// and verifies it with a ping under the 10 s deadline.
func New(ctx context.Context, dsn string) (*Client, error) {
	d, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: open: %w", err)
	}
	d.SetMaxOpenConns(4)
	d.SetMaxIdleConns(4)
	d.SetConnMaxLifetime(5 * time.Minute)
	s := sqlDB{d}
	pctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := s.Ping(pctx); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("sqlserver: ping: %w", err)
	}
	return &Client{db: s}, nil
}

// Close releases the connection pool.
func (c *Client) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

var _ SettingsReader = (*Client)(nil)
