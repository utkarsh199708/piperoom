package pgconv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReadOnlyPool runs every Query and QueryRow in its own READ ONLY transaction
// and always rolls it back.
//
// A session setting such as default_transaction_read_only is not enough: a
// view or function the query touches can call set_config(..., false), and the
// pooled connection would then serve the next read writable, or with another
// search_path. Inside BEGIN READ ONLY the mode cannot be turned off once the
// first query ran, and the rollback undoes every setting changed in the
// transaction, so the connection goes back to the pool as it came out.
type ReadOnlyPool struct {
	Pool *pgxpool.Pool
}

// Query begins the transaction and runs sql in it. The transaction ends when
// the rows are closed or fully read.
func (p ReadOnlyPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("pgconv: begin read-only transaction: %w", err)
	}
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		rollback(ctx, tx)
		return nil, err
	}
	return &txRows{Rows: rows, ctx: ctx, tx: tx}, nil
}

// QueryRow begins the transaction and runs sql in it. The transaction ends
// in Scan.
func (p ReadOnlyPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return errRow{fmt.Errorf("pgconv: begin read-only transaction: %w", err)}
	}
	return &txRow{Row: tx.QueryRow(ctx, sql, args...), ctx: ctx, tx: tx}
}

type txRows struct {
	pgx.Rows
	ctx  context.Context
	tx   pgx.Tx
	once sync.Once
}

func (r *txRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.end()
	return false
}

func (r *txRows) Close() {
	r.Rows.Close()
	r.end()
}

func (r *txRows) end() {
	r.once.Do(func() {
		r.Rows.Close()
		rollback(r.ctx, r.tx)
	})
}

type txRow struct {
	pgx.Row
	ctx context.Context
	tx  pgx.Tx
}

func (r *txRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	rollback(r.ctx, r.tx)
	return err
}

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

// rollback ends tx even when ctx is already done, with its own short
// deadline. When it fails pgx closes the connection, so a session that may
// hold a changed setting is never reused.
func rollback(ctx context.Context, tx pgx.Tx) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(rctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		slog.Warn("pgconv: rollback of read-only transaction failed; connection discarded", "err", err)
	}
}
