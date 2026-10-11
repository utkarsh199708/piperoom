// Package store implements events.Store on Postgres: an append-only room
// event log with atomic seq assignment and a LISTEN/NOTIFY fan-out hub.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/events"
)

// ErrNotFound is returned (wrapped) when a room/proposal/suggestion/handoff
// does not exist.
var ErrNotFound = errors.New("store: not found")

// ErrAlreadyExists is returned when a uniqueness rule is violated
// (e.g. a second decision on the same proposal layer).
var ErrAlreadyExists = errors.New("store: already exists")

// Room is the rooms table row.
type Room struct {
	ID               string
	Title            string
	Severity         string
	PipelineID       string
	Status           string
	OpenedAt         time.Time
	ClosedAt         *time.Time
	LastSeq          int64
	DriverID         string
	DriverName       string
	DriverLeaseUntil *time.Time
}

// ProposalRow is the proposals table row.
type ProposalRow struct {
	ProposalID     string
	RoomID         string
	Title          string
	Status         string
	RequiredLayers []string
	Steps          json.RawMessage
	Risk           string
	Rollback       string
	ExecutedCount  int
}

// SuggestionRow is the suggestions table row.
type SuggestionRow struct {
	SuggestionID string
	RoomID       string
	FromID       string
	FromName     string
	Text         string
	Status       string
}

// HandoffRow is the handoffs table row.
type HandoffRow struct {
	HandoffID     string
	RoomID        string
	ToID          string
	ToName        string
	Status        string
	Summary       json.RawMessage
	StayAsWatcher bool
}

// PG is a Postgres-backed Store plus the aux tables the room service uses to
// enforce rules across restarts.
type PG struct {
	pool       *pgxpool.Pool
	connString string
	hub        *hub
	log        *slog.Logger
	now        func() time.Time
	cancel     context.CancelFunc
}

// New connects to Postgres and starts the LISTEN/NOTIFY hub. The returned
// Store must be closed with Close.
func New(ctx context.Context, connString string, log *slog.Logger) (*PG, error) {
	if log == nil {
		log = slog.Default()
	}
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	hubCtx, cancel := context.WithCancel(context.Background())
	s := &PG{
		pool:       pool,
		connString: connString,
		log:        log,
		now:        time.Now,
		cancel:     cancel,
	}
	s.hub = newHub(s, connString, log)
	go s.hub.run(hubCtx)
	return s, nil
}

// Close releases the pool and stops the hub.
func (s *PG) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.pool.Close()
}

// ---- rooms ----

// CreateRoom inserts the room row with last_seq 0. The room.created event is
// appended afterwards by the room service.
func (s *PG) CreateRoom(ctx context.Context, r Room) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO rooms (id, title, severity, pipeline_id, status, opened_at, last_seq, driver_id, driver_name, driver_lease_until)
		 VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)`,
		r.ID, r.Title, r.Severity, r.PipelineID, r.Status, r.OpenedAt, nullString(r.DriverID), nullString(r.DriverName), r.DriverLeaseUntil)
	if err != nil {
		return fmt.Errorf("store: create room %s: %w", r.ID, err)
	}
	return nil
}

// GetRoom returns a room row or ErrNotFound.
func (s *PG) GetRoom(ctx context.Context, id string) (Room, error) {
	var r Room
	var closedAt *time.Time
	var driverID, driverName *string
	err := s.pool.QueryRow(ctx,
		`SELECT id, title, severity, pipeline_id, status, opened_at, closed_at, last_seq, driver_id, driver_name, driver_lease_until
		 FROM rooms WHERE id = $1`, id).
		Scan(&r.ID, &r.Title, &r.Severity, &r.PipelineID, &r.Status, &r.OpenedAt, &closedAt, &r.LastSeq, &driverID, &driverName, &r.DriverLeaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return Room{}, fmt.Errorf("store: get room %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return Room{}, fmt.Errorf("store: get room %s: %w", id, err)
	}
	r.ClosedAt = closedAt
	if driverID != nil {
		r.DriverID = *driverID
	}
	if driverName != nil {
		r.DriverName = *driverName
	}
	return r, nil
}

// ListRooms returns all rooms ordered by opened_at descending.
func (s *PG) ListRooms(ctx context.Context) ([]Room, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, title, severity, pipeline_id, status, opened_at, closed_at, last_seq, driver_id, driver_name, driver_lease_until
		 FROM rooms ORDER BY opened_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("store: list rooms: %w", err)
	}
	defer rows.Close()
	var out []Room
	for rows.Next() {
		var r Room
		var closedAt *time.Time
		var driverID, driverName *string
		if err := rows.Scan(&r.ID, &r.Title, &r.Severity, &r.PipelineID, &r.Status, &r.OpenedAt, &closedAt, &r.LastSeq, &driverID, &driverName, &r.DriverLeaseUntil); err != nil {
			return nil, fmt.Errorf("store: scan room: %w", err)
		}
		r.ClosedAt = closedAt
		if driverID != nil {
			r.DriverID = *driverID
		}
		if driverName != nil {
			r.DriverName = *driverName
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list rooms: %w", err)
	}
	return out, nil
}

// UpdateDriver records the current driver and lease on the rooms row (kept in
// sync with the event log; the log remains the source of truth).
func (s *PG) UpdateDriver(ctx context.Context, id, driverID, driverName string, leaseUntil time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE rooms SET driver_id = $2, driver_name = $3, driver_lease_until = $4 WHERE id = $1`,
		id, driverID, driverName, leaseUntil)
	if err != nil {
		return fmt.Errorf("store: update driver %s: %w", id, err)
	}
	return nil
}

// CloseRoom marks a room closed.
func (s *PG) CloseRoom(ctx context.Context, id string, closedAt time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE rooms SET status = 'closed', closed_at = $2 WHERE id = $1`, id, closedAt)
	if err != nil {
		return fmt.Errorf("store: close room %s: %w", id, err)
	}
	return nil
}

// ---- events.Store ----

// Append assigns the next seq atomically, inserts the event and notifies
// subscribers, all in one transaction. Seq has no gaps within a room.
func (s *PG) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: marshal payload: %w", err)
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: marshal actor: %w", err)
	}
	at := s.now().UTC()
	var seq int64
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`UPDATE rooms SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq`, roomID).Scan(&seq); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("store: append to unknown room %s: %w", roomID, ErrNotFound)
			}
			return fmt.Errorf("store: reserve seq for %s: %w", roomID, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO room_events (room_id, seq, type, actor, payload, at) VALUES ($1, $2, $3, $4, $5, $6)`,
			roomID, seq, string(typ), actorJSON, payloadJSON, at); err != nil {
			return fmt.Errorf("store: insert event %s/%d: %w", roomID, seq, err)
		}
		if _, err := tx.Exec(ctx,
			`SELECT pg_notify('room_events', $1)`, roomID+":"+strconv.FormatInt(seq, 10)); err != nil {
			return fmt.Errorf("store: notify %s/%d: %w", roomID, seq, err)
		}
		return nil
	})
	if err != nil {
		return events.Event{}, err
	}
	return events.Event{
		RoomID:  roomID,
		Seq:     seq,
		Type:    typ,
		Actor:   actor,
		Payload: payloadJSON,
		At:      at,
	}, nil
}

// Since returns events with Seq > afterSeq in order, at most limit.
func (s *PG) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	if limit <= 0 {
		return []events.Event{}, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT seq, type, actor, payload, at FROM room_events WHERE room_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3`,
		roomID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("store: since %s: %w", roomID, err)
	}
	defer rows.Close()
	var out []events.Event
	for rows.Next() {
		e, err := scanEvent(roomID, rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: since %s: %w", roomID, err)
	}
	return out, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanEvent(roomID string, row scanner) (events.Event, error) {
	var e events.Event
	var typ string
	var actorRaw, payloadRaw []byte
	if err := row.Scan(&e.Seq, &typ, &actorRaw, &payloadRaw, &e.At); err != nil {
		return events.Event{}, fmt.Errorf("store: scan event: %w", err)
	}
	e.RoomID = roomID
	e.Type = events.Type(typ)
	if err := json.Unmarshal(actorRaw, &e.Actor); err != nil {
		return events.Event{}, fmt.Errorf("store: decode actor: %w", err)
	}
	e.Payload = json.RawMessage(payloadRaw)
	return e, nil
}

// Subscribe replays events after afterSeq then streams new ones live from the
// shared LISTEN hub. The channel closes when ctx ends.
func (s *PG) Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan events.Event, error) {
	if _, err := s.GetRoom(ctx, roomID); err != nil {
		return nil, err
	}
	sub := &subscriber{
		roomID:  roomID,
		lastSeq: afterSeq,
		ch:      make(chan events.Event, 256),
		wake:    make(chan struct{}, 1),
	}
	s.hub.register(sub)
	go s.runSub(ctx, sub)
	return sub.ch, nil
}

// ---- proposals ----

// CreateProposal stores a proposal for cross-restart lookup.
func (s *PG) CreateProposal(ctx context.Context, p ProposalRow) error {
	layers, _ := json.Marshal(p.RequiredLayers)
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO proposals (proposal_id, room_id, title, status, required_layers, steps, risk, rollback, executed_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0)`,
		p.ProposalID, p.RoomID, p.Title, p.Status, layers, p.Steps, p.Risk, p.Rollback); err != nil {
		return fmt.Errorf("store: create proposal %s: %w", p.ProposalID, err)
	}
	return nil
}

// GetProposal returns a proposal row or ErrNotFound.
func (s *PG) GetProposal(ctx context.Context, proposalID string) (ProposalRow, error) {
	var p ProposalRow
	var layers, steps []byte
	err := s.pool.QueryRow(ctx,
		`SELECT proposal_id, room_id, title, status, required_layers, steps, risk, rollback, executed_count
		 FROM proposals WHERE proposal_id = $1`, proposalID).
		Scan(&p.ProposalID, &p.RoomID, &p.Title, &p.Status, &layers, &steps, &p.Risk, &p.Rollback, &p.ExecutedCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProposalRow{}, fmt.Errorf("store: proposal %s: %w", proposalID, ErrNotFound)
	}
	if err != nil {
		return ProposalRow{}, fmt.Errorf("store: get proposal %s: %w", proposalID, err)
	}
	if err := json.Unmarshal(layers, &p.RequiredLayers); err != nil {
		return ProposalRow{}, fmt.Errorf("store: decode proposal layers: %w", err)
	}
	p.Steps = json.RawMessage(steps)
	return p, nil
}

// RecordApproval records one decision on a proposal layer. A second decision
// on the same layer returns ErrAlreadyExists.
func (s *PG) RecordApproval(ctx context.Context, proposalID, layer, decision, reason string) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO approvals (proposal_id, layer, decision, reason) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (proposal_id, layer) DO NOTHING`,
		proposalID, layer, decision, reason)
	if err != nil {
		return fmt.Errorf("store: record approval %s/%s: %w", proposalID, layer, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: approval %s/%s: %w", proposalID, layer, ErrAlreadyExists)
	}
	return nil
}

// RecordExecution bumps the executed step count so a step never runs twice.
func (s *PG) RecordExecution(ctx context.Context, proposalID string, executedCount int) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE proposals SET executed_count = $2 WHERE proposal_id = $1 AND executed_count < $2`,
		proposalID, executedCount); err != nil {
		return fmt.Errorf("store: record execution %s: %w", proposalID, err)
	}
	return nil
}

// SetProposalStatus updates a proposal's status (executed / rejected).
func (s *PG) SetProposalStatus(ctx context.Context, proposalID, status string) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE proposals SET status = $2 WHERE proposal_id = $1`, proposalID, status); err != nil {
		return fmt.Errorf("store: set proposal status %s: %w", proposalID, err)
	}
	return nil
}

// ---- suggestions ----

// CreateSuggestion stores a pending suggestion.
func (s *PG) CreateSuggestion(ctx context.Context, r SuggestionRow) error {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO suggestions (suggestion_id, room_id, from_id, from_name, text, status)
		 VALUES ($1, $2, $3, $4, $5, 'pending')`,
		r.SuggestionID, r.RoomID, r.FromID, r.FromName, r.Text); err != nil {
		return fmt.Errorf("store: create suggestion %s: %w", r.SuggestionID, err)
	}
	return nil
}

// GetSuggestion returns a suggestion row or ErrNotFound.
func (s *PG) GetSuggestion(ctx context.Context, suggestionID string) (SuggestionRow, error) {
	var r SuggestionRow
	err := s.pool.QueryRow(ctx,
		`SELECT suggestion_id, room_id, from_id, from_name, text, status FROM suggestions WHERE suggestion_id = $1`,
		suggestionID).Scan(&r.SuggestionID, &r.RoomID, &r.FromID, &r.FromName, &r.Text, &r.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return SuggestionRow{}, fmt.Errorf("store: suggestion %s: %w", suggestionID, ErrNotFound)
	}
	if err != nil {
		return SuggestionRow{}, fmt.Errorf("store: get suggestion %s: %w", suggestionID, err)
	}
	return r, nil
}

// SetSuggestionStatus marks a suggestion sent.
func (s *PG) SetSuggestionStatus(ctx context.Context, suggestionID, status string) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE suggestions SET status = $2 WHERE suggestion_id = $1`, suggestionID, status); err != nil {
		return fmt.Errorf("store: set suggestion status %s: %w", suggestionID, err)
	}
	return nil
}

// ---- handoffs ----

// CreateHandoff stores a pending handoff.
func (s *PG) CreateHandoff(ctx context.Context, h HandoffRow) error {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO handoffs (handoff_id, room_id, to_id, to_name, status, summary, stay_as_watcher)
		 VALUES ($1, $2, $3, $4, 'pending', $5, $6)`,
		h.HandoffID, h.RoomID, h.ToID, h.ToName, h.Summary, h.StayAsWatcher); err != nil {
		return fmt.Errorf("store: create handoff %s: %w", h.HandoffID, err)
	}
	return nil
}

// GetHandoff returns a handoff row or ErrNotFound.
func (s *PG) GetHandoff(ctx context.Context, handoffID string) (HandoffRow, error) {
	var h HandoffRow
	var summary []byte
	err := s.pool.QueryRow(ctx,
		`SELECT handoff_id, room_id, to_id, to_name, status, summary, stay_as_watcher FROM handoffs WHERE handoff_id = $1`,
		handoffID).Scan(&h.HandoffID, &h.RoomID, &h.ToID, &h.ToName, &h.Status, &summary, &h.StayAsWatcher)
	if errors.Is(err, pgx.ErrNoRows) {
		return HandoffRow{}, fmt.Errorf("store: handoff %s: %w", handoffID, ErrNotFound)
	}
	if err != nil {
		return HandoffRow{}, fmt.Errorf("store: get handoff %s: %w", handoffID, err)
	}
	h.Summary = json.RawMessage(summary)
	return h, nil
}

// SetHandoffStatus marks a handoff accepted.
func (s *PG) SetHandoffStatus(ctx context.Context, handoffID, status string) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE handoffs SET status = $2 WHERE handoff_id = $1`, handoffID, status); err != nil {
		return fmt.Errorf("store: set handoff status %s: %w", handoffID, err)
	}
	return nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
