package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// WithMigrationLock runs fn while holding the "migrations" session advisory
// lock (scoped to the schema) on one connection of db, so replicas that start
// together migrate one at a time: the first applies the migrations, the others
// then find none to apply (docs/design/replicas.md section 7).
func WithMigrationLock(ctx context.Context, db *sql.DB, fn func() error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("store: migrations lock: connect: %w", err)
	}
	// Closing the connection also frees the lock on every path.
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext(current_schema()), hashtext('migrations'))`); err != nil {
		return fmt.Errorf("store: migrations lock: %w", err)
	}
	ferr := fn()
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(uctx, `SELECT pg_advisory_unlock(hashtext(current_schema()), hashtext('migrations'))`); err != nil && ferr == nil {
		return fmt.Errorf("store: migrations unlock: %w", err)
	}
	return ferr
}
