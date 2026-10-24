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
// NewWriter) and pings it. The caller owns the pool and closes it; the setup
// package checks that the sessions are writable. A nil logger means
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
	pctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := sdb.PingContext(pctx); err != nil {
		_ = sdb.Close()
		return nil, writeFail("connect", err)
	}
	return sdb, nil
}
