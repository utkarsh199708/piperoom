package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/slackbridge"
)

// slackThreadTimeout bounds each slack_threads statement: the bridge calls
// Put after every mirrored event and must not hang on a slow database.
const slackThreadTimeout = 5 * time.Second

// SlackThreads is the Postgres slackbridge.Threads, so a room keeps its Slack
// thread across server restarts.
type SlackThreads struct{ pool *pgxpool.Pool }

var _ slackbridge.Threads = (*SlackThreads)(nil)

// NewSlackThreads returns a SlackThreads over pool (table slack_threads,
// migrations/00003_slack_threads.sql).
func NewSlackThreads(pool *pgxpool.Pool) *SlackThreads { return &SlackThreads{pool: pool} }

// SlackThreads returns the slack_threads table on this store's pool.
func (s *PG) SlackThreads() *SlackThreads { return NewSlackThreads(s.pool) }

// Get implements slackbridge.Threads; a room with no row yet is ok=false.
func (t *SlackThreads) Get(ctx context.Context, roomID string) (slackbridge.Thread, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, slackThreadTimeout)
	defer cancel()
	var th slackbridge.Thread
	err := t.pool.QueryRow(ctx,
		`SELECT channel, ts, last_seq FROM slack_threads WHERE room_id = $1`, roomID,
	).Scan(&th.Channel, &th.TS, &th.LastSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return slackbridge.Thread{}, false, nil
	}
	if err != nil {
		return slackbridge.Thread{}, false, fmt.Errorf("store: slack thread get: %w", err)
	}
	return th, true, nil
}

// Put implements slackbridge.Threads as an upsert. The room must exist.
func (t *SlackThreads) Put(ctx context.Context, roomID string, th slackbridge.Thread) error {
	if err := CheckText(roomID, th.Channel, th.TS); err != nil {
		return fmt.Errorf("store: slack thread put: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, slackThreadTimeout)
	defer cancel()
	_, err := t.pool.Exec(ctx, `
INSERT INTO slack_threads (room_id, channel, ts, last_seq, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (room_id) DO UPDATE
SET channel = EXCLUDED.channel, ts = EXCLUDED.ts, last_seq = EXCLUDED.last_seq, updated_at = now()`,
		roomID, th.Channel, th.TS, th.LastSeq)
	if err != nil {
		return fmt.Errorf("store: slack thread put: %w", err)
	}
	return nil
}
