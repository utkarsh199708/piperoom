package store_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"piperoom/internal/store"
)

// lockDriver is a database/sql driver whose connections record whether they
// were closed (the Postgres session ended) and can fail the unlock.
type lockDriver struct {
	failUnlock bool
	closed     atomic.Int32
}

func (d *lockDriver) Open(string) (driver.Conn, error) {
	return &lockConn{d: d}, nil
}

type lockConn struct{ d *lockDriver }

func (c *lockConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *lockConn) Close() error                        { c.d.closed.Add(1); return nil }
func (c *lockConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c *lockConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	if c.d.failUnlock && strings.Contains(q, "pg_advisory_unlock") {
		return nil, errors.New("unlock failed")
	}
	return driver.RowsAffected(0), nil
}

type lockConnector struct{ d *lockDriver }

func (c lockConnector) Connect(context.Context) (driver.Conn, error) { return c.d.Open("") }
func (c lockConnector) Driver() driver.Driver                        { return c.d }

func TestWithMigrationLockEndsSessionWhenUnlockFails(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failUnlock bool
		fnErr      error
		wantClosed int32
		wantErr    string
	}{
		{name: "unlock ok keeps the pooled session", wantClosed: 0},
		{name: "unlock fails ends the session", failUnlock: true, wantClosed: 1, wantErr: "migrations unlock"},
		{name: "unlock fails after fn error ends the session", failUnlock: true, fnErr: errors.New("boom"), wantClosed: 1, wantErr: "boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &lockDriver{failUnlock: tc.failUnlock}
			db := sql.OpenDB(lockConnector{d})
			err := store.WithMigrationLock(context.Background(), db, func() error { return tc.fnErr })
			if tc.wantErr == "" && err != nil {
				t.Fatalf("err = %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			if got := d.closed.Load(); got != tc.wantClosed {
				t.Fatalf("driver connections closed before db.Close = %d, want %d", got, tc.wantClosed)
			}
			_ = db.Close()
		})
	}
}
