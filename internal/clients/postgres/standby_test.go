package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// standbyQuerier answers like a hot standby: a statement that calls
// pg_current_wal_lsn() without first asking pg_is_in_recovery() fails the way
// PG 16 does during recovery (#420).
type standbyQuerier struct{ fakeQuerier }

var errRecovery = &pgconn.PgError{Code: "55000", Message: "recovery is in progress"}

func (s *standbyQuerier) unguarded(sql string) bool {
	i := strings.Index(sql, "pg_current_wal_lsn()")
	if i < 0 {
		return false
	}
	g := strings.Index(sql, "pg_is_in_recovery()")
	return g < 0 || g > i
}

func (s *standbyQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if s.unguarded(sql) {
		s.sqls = append(s.sqls, sql)
		return nil, errRecovery
	}
	return s.fakeQuerier.Query(ctx, sql, args...)
}

func (s *standbyQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if s.unguarded(sql) {
		s.sqls = append(s.sqls, sql)
		return fakeRow{err: errRecovery}
	}
	return s.fakeQuerier.QueryRow(ctx, sql, args...)
}

func TestSlotReadsWorkOnStandby(t *testing.T) {
	reads := map[string]func(*DB) error{
		"ReplicationSlots": func(d *DB) error {
			_, err := d.ReplicationSlots(context.Background())
			return err
		},
		"ReplicationSlotsNamed": func(d *DB) error {
			_, _, err := d.ReplicationSlotsNamed(context.Background(), []string{"debezium"})
			return err
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			q := &standbyQuerier{}
			if err := read(testDB(q)); err != nil {
				var pg *pgconn.PgError
				if errors.As(err, &pg) && pg.Code == "55000" {
					t.Fatalf("slot read fails on a standby: %v", err)
				}
				t.Fatalf("unexpected error: %v", err)
			}
			if len(q.sqls) == 0 || !strings.Contains(q.sqls[0], "pg_last_wal_replay_lsn()") || !strings.Contains(q.sqls[0], "pg_last_wal_receive_lsn()") {
				t.Fatalf("slot query does not use the standby WAL position: %q", q.sqls)
			}
		})
	}
}
