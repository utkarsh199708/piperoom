package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

// Up applies every pending migration in FS to db and returns how many ran.
// cmd/server calls it at startup and the Postgres integration tests call it
// with a *sql.DB pinned to their own schema, so both use the exact same
// runner (review item 1: no hand-rolled applyUp).
//
// db is a database/sql handle (github.com/jackc/pgx/v5/stdlib); goose owns
// its version table (goose_db_version) inside whatever schema search_path
// resolves to. The caller owns db and closes it; Provider.Close is not
// called because it only closes that same handle.
func Up(ctx context.Context, db *sql.DB) (int, error) {
	provider, err := goose.NewProvider(goose.DialectPostgres, db, FS)
	if err != nil {
		return 0, fmt.Errorf("migrations: build provider: %w", err)
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return 0, fmt.Errorf("migrations: up: %w", err)
	}
	return len(results), nil
}
