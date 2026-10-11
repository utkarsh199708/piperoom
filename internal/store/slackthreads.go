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

// slackThreadTimeout bounds each slack_threads statement; the bridge calls
// these once per mirrored event, so they must stay short.
const slackThreadTimeout = 5 * time.Second

// SlackThreads is the Postgres-backed slackbridge.Threads. The schema is
// migrations/002_slack_threads.sql. It keeps a room's Slack thread across
// restarts, so a restarted server replies in the same thread.
type SlackThreads struct{ pool *pgxpool.Pool }

var _ slackbridge.Threads = (*SlackThreads)(nil)

// NewSlackThreads returns a SlackThreads that uses pool.
func NewSlackThreads(pool *pgxpool.Pool) *SlackThreads { return &SlackThreads{pool: pool} }

// SlackThreads returns a slackbridge.Threads that shares this store's pool.
func (p *PG) SlackThreads() *SlackThreads { return NewSlackThreads(p.pool) }

// Get implements slackbridge.Threads. A room with no thread yet is ok=false.
func (s *SlackThreads) Get(ctx context.Context, roomID string) (slackbridge.Thread, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, slackThreadTimeout)
	defer cancel()
	var th slackbridge.Thread
	err := s.pool.QueryRow(ctx, `SELECT channel, ts, last_seq FROM slack_threads WHERE room_id = $1`, roomID).
		Scan(&th.Channel, &th.TS, &th.LastSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return slackbridge.Thread{}, false, nil
	}
	if err != nil {
		return slackbridge.Thread{}, false, fmt.Errorf("store: slack thread get: %w", err)
	}
	return th, true, nil
}

// Put implements slackbridge.Threads. It inserts or replaces the room's row;
// the room must exist in rooms.
func (s *SlackThreads) Put(ctx context.Context, roomID string, th slackbridge.Thread) error {
	ctx, cancel := context.WithTimeout(ctx, slackThreadTimeout)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
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
