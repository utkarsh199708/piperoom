package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	gomysql "github.com/go-sql-driver/mysql"
)

// setupProgram is the program_name connection attribute of the setup writer's
// pool. It starts with "piperoom", so EndSession never ends it.
const setupProgram = "piperoom-setup-writer"

// OpenSetupPool opens the write pool the setup driver
// (internal/setup/sources/mysql) executes its source writes on, for rawURL (a
// mysql:// URL, the same parsing rules and safety switches as New and
// NewWriter), pings it and probes the server version with the same floor as
// New and NewWriter (MySQL 8.0 or later, never MariaDB), refusing with
// clients.ErrNotSupported otherwise. The caller owns the pool and closes it;
// the setup package checks that the sessions are writable. A nil logger means
// slog.Default(). Errors never contain the URL or its password.
func OpenSetupPool(ctx context.Context, rawURL string, logger *slog.Logger) (*sql.DB, error) {
	if logger == nil {
		logger = slog.Default()
	}
	u, err := parseURL(rawURL)
	if err != nil {
		return nil, err
	}
	cfg, err := u.config(setupProgram, writeParams, driverLogger{l: logger})
	if err != nil {
		return nil, err
	}
	conn, err := gomysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql: setup connector: %w", err)
	}
	sdb := sql.OpenDB(conn)
	sdb.SetMaxOpenConns(writeMaxOpen)
	sdb.SetMaxIdleConns(writeMaxOpen)
	sdb.SetConnMaxLifetime(connMaxLifetime)
	if err := openSetupPool(ctx, sdb); err != nil {
		_ = sdb.Close()
		return nil, err
	}
	return sdb, nil
}

// openSetupPool pings sdb and probes the server version against the floor New
// and NewWriter enforce (setVersion), so the setup writer's CREATE USER, GRANT
// and DDL text never runs on a server the client refuses to read: MariaDB's
// grant tables and MySQL 5.7's defaults differ from what the statements assume.
// The session's writability is the setup package's check. Tests call it with a
// fake driver.
func openSetupPool(ctx context.Context, sdb *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := sdb.PingContext(ctx); err != nil {
		return writeFail("connect", err)
	}
	var version, comment string
	if err := sdb.QueryRowContext(ctx, `SELECT VERSION(), @@version_comment`).Scan(&version, &comment); err != nil {
		return writeFail("read server version", err)
	}
	return (&DB{}).setVersion(version, comment)
}
