package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/events"
	"piperoom/internal/store/pump"
)

const (
	// dbTimeout bounds every database call (AGENTS.md rule 4).
	dbTimeout = 10 * time.Second
	// pollInterval is the safety net that re-reads each subscriber's log even
	// if a notification was lost.
	pollInterval = 2 * time.Second
	// uniqueViolation is the SQLSTATE of a duplicate primary key.
	uniqueViolation = "23505"
)

var errClosed = errors.New("store: closed")

// PG is the Postgres-backed store. The schema is migrations/001_init.sql.
type PG struct {
	pool *pgxpool.Pool
	log  *slog.Logger
	hub  *hub

	// life ends when the ctx given to NewPG ends or Close is called; every
	// background goroutine runs under it.
	life   context.Context
	cancel context.CancelFunc

	// Subscriber tuning. Fixed in NewPG; only tests change them, before the
	// first Subscribe.
	poll      time.Duration
	slowAfter time.Duration

	mu         sync.Mutex // guards closed, hubStarted and wg.Add vs wg.Wait
	closed     bool
	hubStarted bool
	wg         sync.WaitGroup
	closeOnce  sync.Once
}

var _ Store = (*PG)(nil)

// NewPG connects to Postgres and checks the connection. ctx is the lifetime of
// the store: when it ends, all subscriptions close and the LISTEN connection
// stops, so pass the process context and not a short connect timeout. The
// caller must still call Close to release the pool. dsn may contain a
// password; it is never logged or put in an error.
func NewPG(ctx context.Context, dsn string, logger *slog.Logger) (*PG, error) {
	if logger == nil {
		logger = slog.Default()
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse postgres config: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: open postgres pool: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping postgres: %w", err)
	}

	life, stop := context.WithCancel(ctx)
	return &PG{
		pool:      pool,
		log:       logger,
		hub:       newHub(cfg.ConnConfig, logger),
		life:      life,
		cancel:    stop,
		poll:      pollInterval,
		slowAfter: pump.DefaultSlowAfter,
	}, nil
}

// Close stops every subscription and the LISTEN connection, waits for their
// goroutines, then closes the pool. It is safe to call more than once and
// after the NewPG context has ended.
func (p *PG) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.cancel()
		p.wg.Wait()
		p.pool.Close()
	})
}

// pingTimeout bounds Ping; a readiness probe must answer inside its own budget.
const pingTimeout = 2 * time.Second

// Ping checks that the database answers a trivial query. It backs GET /readyz.
func (p *PG) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	var one int
	if err := p.pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("store: ping: %w", err)
	}
	return nil
}

// CreateRoom registers an empty log for roomID.
func (p *PG) CreateRoom(ctx context.Context, roomID string) error {
	if roomID == "" {
		return errors.New("store: empty room id")
	}
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	if _, err := p.pool.Exec(ctx, `INSERT INTO rooms (id) VALUES ($1)`, roomID); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return fmt.Errorf("store: create room %q: %w", roomID, ErrRoomExists)
		}
		return fmt.Errorf("store: create room %q: %w", roomID, err)
	}
	return nil
}

// RoomIDs lists every registered room ID, oldest first.
func (p *PG) RoomIDs(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	rows, err := p.pool.Query(ctx, `SELECT id FROM rooms ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("store: list rooms: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("store: list rooms: %w", err)
	}
	if ids == nil {
		ids = []string{}
	}
	return ids, nil
}

// Append stores one event under the next seq of the room. Concurrent appends to
// one room queue on the room row, so seq is gap-free and commits happen in seq
// order; that ordering is what lets a reader that sees seq N+1 rely on N being
// visible too.
func (p *PG) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	raw, err := MarshalPayload(payload)
	if err != nil {
		return events.Event{}, err
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: marshal actor: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: begin append to room %q: %w", roomID, err)
	}
	defer func() {
		// A fresh context: if ctx is what failed, a rollback on it would fail
		// too and make pgx drop the connection.
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer rcancel()
		_ = tx.Rollback(rctx) // no-op after a successful Commit
	}()

	var seq int64
	err = tx.QueryRow(ctx, `UPDATE rooms SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq`, roomID).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return events.Event{}, fmt.Errorf("store: append to room %q: %w", roomID, ErrRoomNotFound)
	}
	if err != nil {
		return events.Event{}, fmt.Errorf("store: next seq of room %q: %w", roomID, err)
	}

	// clock_timestamp() is read after the room row lock is held, so "at" never
	// goes backwards as seq grows (now() would be the transaction start, taken
	// before waiting for the lock). RETURNING payload hands back the jsonb
	// normalised form, identical to what Since and Subscribe will later serve.
	var (
		at         time.Time
		storedJSON []byte
	)
	err = tx.QueryRow(ctx,
		`INSERT INTO room_events (room_id, seq, type, actor, payload, at)
		 VALUES ($1, $2, $3, $4, $5, clock_timestamp())
		 RETURNING at, payload`,
		roomID, seq, string(typ), actorJSON, []byte(raw)).Scan(&at, &storedJSON)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: insert event %s into room %q: %w", typ, roomID, err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannel, roomID+":"+strconv.FormatInt(seq, 10)); err != nil {
		return events.Event{}, fmt.Errorf("store: notify room %q: %w", roomID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return events.Event{}, fmt.Errorf("store: commit event %d of room %q: %w", seq, roomID, err)
	}
	return events.Event{
		RoomID:  roomID,
		Seq:     seq,
		Type:    typ,
		Actor:   actor,
		Payload: json.RawMessage(storedJSON),
		At:      at.UTC(),
	}, nil
}

// Since returns events with Seq > afterSeq in order. limit <= 0 means 500 and
// anything above 1000 is capped. The result is never nil.
func (p *PG) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	limit = ClampLimit(limit)
	if afterSeq < 0 {
		afterSeq = 0
	}
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	rows, err := p.pool.Query(ctx,
		`SELECT seq, type, actor, payload, at FROM room_events
		 WHERE room_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`,
		roomID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("store: query events of room %q: %w", roomID, err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (events.Event, error) {
		var (
			ev        = events.Event{RoomID: roomID}
			typ       string
			actorJSON []byte
			payload   []byte
		)
		if err := row.Scan(&ev.Seq, &typ, &actorJSON, &payload, &ev.At); err != nil {
			return ev, err
		}
		if err := json.Unmarshal(actorJSON, &ev.Actor); err != nil {
			return ev, fmt.Errorf("decode actor of seq %d: %w", ev.Seq, err)
		}
		ev.Type = events.Type(typ)
		ev.Payload = json.RawMessage(payload)
		ev.At = ev.At.UTC()
		return ev, nil
	})
	if err != nil {
		return nil, fmt.Errorf("store: read events of room %q: %w", roomID, err)
	}
	if len(out) > 0 {
		return out, nil
	}

	// Nothing returned: tell "empty log" from "no such room".
	var exists bool
	if err := p.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM rooms WHERE id = $1)`, roomID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("store: check room %q: %w", roomID, err)
	}
	if !exists {
		return nil, fmt.Errorf("store: events of room %q: %w", roomID, ErrRoomNotFound)
	}
	return []events.Event{}, nil
}

// Subscribe replays events after afterSeq, then streams new ones. The channel
// (capacity 256) is closed when ctx ends, when the store is closed, or when the
// consumer leaves it full for more than 5 s; in the last case the client
// resubscribes with the last seq it saw.
func (p *PG) Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan events.Event, error) {
	if afterSeq < 0 {
		afterSeq = 0
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errClosed
	}
	if !p.hubStarted {
		p.hubStarted = true
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.hub.run(p.life)
		}()
	}
	p.wg.Add(1) // released by the subscriber goroutine, or below on failure
	p.mu.Unlock()

	// Register for wake-ups before reading, so an append that lands between the
	// read and the registration still wakes us.
	wake, unregister := p.hub.add(roomID)
	sctx, cancel := context.WithCancel(ctx)
	stopAfter := context.AfterFunc(p.life, cancel) // Close / NewPG ctx ends the subscription too
	release := func() {
		stopAfter()
		cancel()
		unregister()
		p.wg.Done()
	}

	// The first read is synchronous so an unknown room is an error here and not
	// a channel that silently never delivers.
	first, err := p.Since(sctx, roomID, afterSeq, pump.DefaultPage)
	if err != nil {
		release()
		return nil, err
	}
	return pump.Start(sctx, pump.Config{
		Read: func(ctx context.Context, after int64, limit int) ([]events.Event, error) {
			return p.Since(ctx, roomID, after, limit)
		},
		Wake:      wake,
		Poll:      p.poll,
		SlowAfter: p.slowAfter,
		Log:       p.log.With("room_id", roomID),
		OnExit:    release,
	}, afterSeq, first), nil
}
