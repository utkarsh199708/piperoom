package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	// conn.Close only returns the session to the pool; a session lock lives
	// as long as the session. Every path that may leave the lock held
	// discards the session instead (discardConn), so it ends and the lock
	// with it.
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext(current_schema()), hashtext('migrations'))`); err != nil {
		// The lock may have been granted before the error (a cancelled
		// context racing the reply).
		discardConn(conn)
		return fmt.Errorf("store: migrations lock: %w", err)
	}
	ferr := fn()
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(uctx, `SELECT pg_advisory_unlock(hashtext(current_schema()), hashtext('migrations'))`); err != nil {
		discardConn(conn)
		if ferr == nil {
			return fmt.Errorf("store: migrations unlock: %w", err)
		}
	}
	return ferr
}

// discardConn marks conn bad, so database/sql closes the driver connection
// (ending the Postgres session) when conn is closed, instead of pooling it.
func discardConn(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn }) // the error is the signal, not a failure
}
