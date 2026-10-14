package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Elector runs loops that must run on one replica only: the watcher, the
// retention purge, the auth prune and the Slack mirror (docs/design/replicas.md
// section 3).
type Elector interface {
	// Run blocks until ctx ends. Each time this process becomes the leader
	// of name it calls fn with a context that ends when leadership is lost or
	// ctx ends; fn must return soon after that.
	Run(ctx context.Context, name string, fn func(ctx context.Context))
}

// SoloElector is the elector of a process that shares its store with no one
// (the in-memory store): it is always the leader.
type SoloElector struct{}

// Run implements Elector: it runs fn once with ctx and waits for ctx to end.
func (SoloElector) Run(ctx context.Context, name string, fn func(ctx context.Context)) {
	fn(ctx)
	<-ctx.Done()
}

// Elector timings (docs/design/replicas.md section 3). A new leader waits
// DefaultLeaderSettle before it starts the loop; that is longer than the
// DefaultLeaderCheck + DefaultLeaderCheckTimeout an old leader needs to notice
// its connection is gone, so the old loop is always cancelled first.
const (
	DefaultLeaderRetry        = 15 * time.Second
	DefaultLeaderCheck        = 2 * time.Second
	DefaultLeaderCheckTimeout = 2 * time.Second
	DefaultLeaderSettle       = 5 * time.Second
)

// errLoopReturned ends a leadership term whose loop returned on its own.
var errLoopReturned = errors.New("store: leader loop returned")

// PGElector elects one leader per loop name among every process that shares
// the room database, with a session advisory lock held on a dedicated
// connection for the whole term.
type PGElector struct {
	cfg     *pgx.ConnConfig
	replica string
	log     *slog.Logger

	// Timings; zero means the Default* value. Tests shorten them. Settle must
	// stay above Check + CheckTimeout.
	Retry, Check, CheckTimeout, Settle time.Duration
}

var _ Elector = (*PGElector)(nil)

// Elector returns a PGElector for this store's database. Its connections use
// the pool's configuration (address, TLS, search_path) but are not taken from
// the pool, so a leader never holds a request connection. replica names this
// process in logs and in the lock connection's application_name.
func (s *PG) Elector(replica string) *PGElector {
	return &PGElector{cfg: s.pool.Config().ConnConfig, replica: replica, log: s.log}
}

func pick(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// AppName is the application_name of name's lock connection on this replica,
// so ops (and tests) can find the leader's backend in pg_stat_activity.
// Postgres keeps 63 bytes; a longer name is shortened with a hash so it stays
// unique and is exactly what pg_stat_activity shows.
func (e *PGElector) AppName(name string) string {
	n := "piperoom-leader-" + name + "-" + e.replica
	if len(n) <= 63 {
		return n
	}
	sum := sha256.Sum256([]byte(n))
	return n[:50] + "-" + hex.EncodeToString(sum[:6])
}

// Run implements Elector.
func (e *PGElector) Run(ctx context.Context, name string, fn func(ctx context.Context)) {
	retry := pick(e.Retry, DefaultLeaderRetry)
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			closeConn(conn)
		}
	}()
	for ctx.Err() == nil {
		if conn == nil {
			c, err := e.connect(ctx, name)
			if err != nil {
				if ctx.Err() == nil {
					e.log.Warn("leader: cannot connect for the lock; retrying", "loop", name, "replica", e.replica, "err", err)
				}
				if !sleep(ctx, retry) {
					return
				}
				continue
			}
			conn = c
		}
		got, err := e.try(ctx, conn, name)
		if err != nil {
			if ctx.Err() == nil {
				e.log.Warn("leader: lock attempt failed; retrying", "loop", name, "replica", e.replica, "err", err)
			}
			closeConn(conn)
			conn = nil
			if !sleep(ctx, retry) {
				return
			}
			continue
		}
		if !got {
			if !sleep(ctx, retry) {
				return
			}
			continue
		}
		settle := pick(e.Settle, DefaultLeaderSettle)
		e.log.Info("leader: acquired", "loop", name, "replica", e.replica, "starts_in", settle.String())
		lerr := e.lead(ctx, conn, name, settle, fn)
		// Closing the session frees the lock whatever state it is in.
		closeConn(conn)
		conn = nil
		if ctx.Err() != nil {
			e.log.Info("leader: released", "loop", name, "replica", e.replica)
			return
		}
		e.log.Warn("leader: lost", "loop", name, "replica", e.replica, "err", lerr)
		if !sleep(ctx, retry) {
			return
		}
	}
}

// lead runs one leadership term: the settle wait, then fn until the
// connection check fails, fn returns, or ctx ends. fn's context is cancelled
// and fn has returned when lead returns.
func (e *PGElector) lead(ctx context.Context, conn *pgx.Conn, name string, settle time.Duration, fn func(context.Context)) error {
	if err := e.watch(ctx, conn, settle, nil); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	lctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(lctx)
	}()
	e.log.Info("leader: loop started", "loop", name, "replica", e.replica)
	err := e.watch(ctx, conn, 0, done)
	cancel()
	<-done
	return err
}

// watch checks conn every Check until d has passed (d == 0: no limit), ctx
// ends, or done closes. It returns the first failed check, errLoopReturned
// when done closed, and nil otherwise.
func (e *PGElector) watch(ctx context.Context, conn *pgx.Conn, d time.Duration, done <-chan struct{}) error {
	t := time.NewTicker(pick(e.Check, DefaultLeaderCheck))
	defer t.Stop()
	var deadline <-chan time.Time
	if d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		deadline = timer.C
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return errLoopReturned
		case <-deadline:
			return nil
		case <-t.C:
		}
		cctx, cancel := context.WithTimeout(ctx, pick(e.CheckTimeout, DefaultLeaderCheckTimeout))
		_, err := conn.Exec(cctx, "SELECT 1")
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("store: leader connection check: %w", err)
		}
	}
}

// try takes the loop's lock without waiting.
func (e *PGElector) try(ctx context.Context, conn *pgx.Conn, name string) (bool, error) {
	tctx, cancel := context.WithTimeout(ctx, pick(e.CheckTimeout, DefaultLeaderCheckTimeout))
	defer cancel()
	var got bool
	if err := conn.QueryRow(tctx,
		`SELECT pg_try_advisory_lock(hashtext(current_schema()), hashtext($1))`, "leader:"+name).Scan(&got); err != nil {
		return false, fmt.Errorf("store: leader lock %s: %w", name, err)
	}
	return got, nil
}

// connect opens the lock connection. Server-side TCP keepalives make
// Postgres end the session, and so free the lock, within about 25 s of a
// client that vanished without closing it.
func (e *PGElector) connect(ctx context.Context, name string) (*pgx.Conn, error) {
	cfg := e.cfg.Copy()
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["application_name"] = e.AppName(name)
	cfg.RuntimeParams["tcp_keepalives_idle"] = "10"
	cfg.RuntimeParams["tcp_keepalives_interval"] = "5"
	cfg.RuntimeParams["tcp_keepalives_count"] = "3"
	// And a write to a vanished client fails after 4 s instead of the kernel's
	// minutes of retransmits (milliseconds; Postgres 12+).
	cfg.RuntimeParams["tcp_user_timeout"] = "4000"
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(cctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: leader connect: %w", err)
	}
	return conn, nil
}
