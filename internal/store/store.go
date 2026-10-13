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
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/events"
)

// ErrNotFound is returned (wrapped) when a room/proposal/suggestion/handoff
// does not exist.
var ErrNotFound = errors.New("store: not found")

// ErrRoomNotEmpty is returned by DeleteEmptyRoom for a room that has events.
var ErrRoomNotEmpty = errors.New("store: room has events")

// ErrAlreadyExists is returned when a uniqueness rule is violated
// (e.g. a second decision on the same proposal layer).
var ErrAlreadyExists = errors.New("store: already exists")

// ErrParamsHashConflict is returned by SetProposalParamsHash when the row
// already holds a different, non-empty params_hash.
var ErrParamsHashConflict = errors.New("store: proposal already has a different params_hash")

// ErrStepOrder is returned when a proposal step is used out of order: started
// twice, or finished without having been started (or finished twice). The
// room service re-exports it as room.ErrStepOrder and surfaces it instead of
// silently skipping the step.
var ErrStepOrder = errors.New("store: step order violation")

// defaultDBTimeout bounds any store call whose caller supplied no deadline.
const defaultDBTimeout = 10 * time.Second

// dbCtx adds a deadline when the caller's context has none so every DB call
// is bounded (rule 4).
func dbCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultDBTimeout)
}

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
	// ParamsHash is the proposal's params_hash (docs/design/auth.md section 3.1);
	// "" on a row written before auth PR 4.
	ParamsHash string
}

// ApprovalRow is the approvals table row: one decision on one layer, the
// params_hash the decider saw and who they were (design section 3.2).
type ApprovalRow struct {
	ProposalID string
	Layer      string
	Decision   string // "approved" | "rejected"
	Reason     string
	ParamsHash string
	// Identity columns. IdentityAuthTime is the zero time when the row has none.
	IdentityIss       string
	IdentitySub       string
	IdentitySessionID string
	IdentityMethod    string
	IdentityGrant     string
	IdentityAuthTime  time.Time
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
	FromID        string
	FromName      string
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
	hubWg      sync.WaitGroup
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
	return newPG(pool, connString, log), nil
}

// NewPool wraps an existing pool. Tests use it to pin search_path to a fresh
// schema; the returned Store must be closed with Close.
func NewPool(pool *pgxpool.Pool, connString string, log *slog.Logger) *PG {
	if log == nil {
		log = slog.Default()
	}
	return newPG(pool, connString, log)
}

func newPG(pool *pgxpool.Pool, connString string, log *slog.Logger) *PG {
	hubCtx, cancel := context.WithCancel(context.Background())
	s := &PG{
		pool:       pool,
		connString: connString,
		log:        log,
		now:        time.Now,
		cancel:     cancel,
	}
	s.hub = newHub(s, connString, listenAppName(), log)
	s.hubWg.Add(1)
	go func() {
		defer s.hubWg.Done()
		s.hub.run(hubCtx)
	}()
	return s
}

// ListenAppName returns this instance's LISTEN connection application_name.
// Tests use it to terminate exactly this hub's backend (integration test
// reconnection); it also shows up in pg_stat_activity for ops.
func (s *PG) ListenAppName() string { return s.hub.appName }

// pingTimeout caps Ping even when the caller's deadline is longer: a
// readiness probe must answer quickly.
const pingTimeout = 2 * time.Second

// Ping checks that the database answers a trivial query. The readiness probe
// (GET /readyz) uses it.
func (s *PG) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if _, err := s.pool.Exec(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("store: ping: %w", err)
	}
	return nil
}

// Close releases the pool and waits for the hub to stop.
func (s *PG) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.hubWg.Wait()
	s.pool.Close()
}

// ---- rooms ----

// CreateRoom inserts the room row with last_seq 0. The room.created event is
// appended afterwards by the room service.
func (s *PG) CreateRoom(ctx context.Context, r Room) error {
	if err := CheckText(r.ID, r.Title, r.Severity, r.PipelineID, r.Status, r.DriverID, r.DriverName); err != nil {
		return fmt.Errorf("store: create room: %w", err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO rooms (id, title, severity, pipeline_id, status, opened_at, last_seq, driver_id, driver_name, driver_lease_until)
		 VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)`,
		r.ID, r.Title, r.Severity, r.PipelineID, r.Status, r.OpenedAt, nullString(r.DriverID), nullString(r.DriverName), r.DriverLeaseUntil)
	if err != nil {
		return fmt.Errorf("store: create room %s: %w", r.ID, classify(err))
	}
	return nil
}

// GetRoom returns a room row or ErrNotFound.
func (s *PG) GetRoom(ctx context.Context, id string) (Room, error) {
	if CheckText(id) != nil {
		return Room{}, fmt.Errorf("store: get room: %w", ErrNotFound) // no such id can be stored
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
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
	utcTimes(&r)
	return r, nil
}

// ListRooms returns all rooms ordered by opened_at descending. Rows with the
// same opened_at are tie-broken by id so the order is total and stable.
func (s *PG) ListRooms(ctx context.Context) ([]Room, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT id, title, severity, pipeline_id, status, opened_at, closed_at, last_seq, driver_id, driver_name, driver_lease_until
		 FROM rooms ORDER BY opened_at DESC, id ASC`)
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
		utcTimes(&r)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list rooms: %w", err)
	}
	return out, nil
}

// LastRoomByDedupKey returns the newest room whose room.created payload holds
// dedupKey (see Store.LastRoomByDedupKey). It uses the partial index on
// payload->>'dedup_key' (migration 00005).
func (s *PG) LastRoomByDedupKey(ctx context.Context, dedupKey string) (Room, error) {
	if dedupKey == "" {
		return Room{}, fmt.Errorf("store: last room by dedup key: empty key: %w", ErrNotFound)
	}
	if CheckText(dedupKey) != nil {
		return Room{}, fmt.Errorf("store: last room by dedup key: %w", ErrNotFound)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var r Room
	var closedAt *time.Time
	var driverID, driverName *string
	err := s.pool.QueryRow(ctx,
		`SELECT r.id, r.title, r.severity, r.pipeline_id, r.status, r.opened_at, r.closed_at, r.last_seq, r.driver_id, r.driver_name, r.driver_lease_until
		 FROM room_events e JOIN rooms r ON r.id = e.room_id
		 WHERE e.type = 'room.created' AND e.payload->>'dedup_key' = $1
		 ORDER BY r.opened_at DESC, r.id ASC
		 LIMIT 1`, dedupKey).
		Scan(&r.ID, &r.Title, &r.Severity, &r.PipelineID, &r.Status, &r.OpenedAt, &closedAt, &r.LastSeq, &driverID, &driverName, &r.DriverLeaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return Room{}, fmt.Errorf("store: last room by dedup key: %w", ErrNotFound)
	}
	if err != nil {
		return Room{}, fmt.Errorf("store: last room by dedup key: %w", err)
	}
	r.ClosedAt = closedAt
	if driverID != nil {
		r.DriverID = *driverID
	}
	if driverName != nil {
		r.DriverName = *driverName
	}
	utcTimes(&r)
	return r, nil
}

// UpdateDriver records the current driver and lease on the rooms row (kept in
// sync with the event log; the log remains the source of truth).
func (s *PG) UpdateDriver(ctx context.Context, id, driverID, driverName string, leaseUntil time.Time) error {
	if err := CheckText(id, driverID, driverName); err != nil {
		return fmt.Errorf("store: update driver: %w", err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`UPDATE rooms SET driver_id = $2, driver_name = $3, driver_lease_until = $4 WHERE id = $1`,
		id, driverID, driverName, leaseUntil)
	if err != nil {
		return fmt.Errorf("store: update driver %s: %w", id, classify(err))
	}
	return nil
}

// CloseRoom marks a room closed.
func (s *PG) CloseRoom(ctx context.Context, id string, closedAt time.Time) error {
	if err := CheckText(id); err != nil {
		return fmt.Errorf("store: close room: %w", err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`UPDATE rooms SET status = 'closed', closed_at = $2 WHERE id = $1`, id, closedAt)
	if err != nil {
		return fmt.Errorf("store: close room %s: %w", id, err)
	}
	return nil
}

// DeleteEmptyRoom deletes the room row only while it has no events
// (last_seq = 0), so a failed room.created append leaves no orphan row that
// would show up as a room with an empty log. A room with events is kept and
// reported as ErrRoomNotEmpty; a missing room is a no-op.
func (s *PG) DeleteEmptyRoom(ctx context.Context, id string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `DELETE FROM rooms WHERE id = $1 AND last_seq = 0`, id)
	if err != nil {
		return fmt.Errorf("store: delete empty room %s: %w", id, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM rooms WHERE id = $1)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("store: delete empty room %s: %w", id, err)
	}
	if exists {
		return fmt.Errorf("store: delete empty room %s: %w", id, ErrRoomNotEmpty)
	}
	return nil
}

// ---- events.Store ----

// Append assigns the next seq atomically, inserts the event and notifies
// subscribers, all in one transaction. Seq has no gaps within a room.
func (s *PG) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: marshal payload: %w", err)
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: marshal actor: %w", err)
	}
	// Before the transaction: a refused event must not reserve a seq or leave
	// a gap (the INSERT failing would roll the seq back too, but a check here
	// costs no round trip).
	if err := CheckAppend(roomID, typ, payloadJSON, actorJSON); err != nil {
		return events.Event{}, fmt.Errorf("store: append to %s: %w", roomID, err)
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
			return fmt.Errorf("store: insert event %s/%d: %w", roomID, seq, classify(err))
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

// Since returns events with Seq > afterSeq in order, at most limit. Seq is the
// room's primary key part, so (room_id, seq) is total and the order is stable.
func (s *PG) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
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
	out := []events.Event{}
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
	// Snapshots rebuilt after a restart must be UTC like the live ones
	// (docs/api.md: times are RFC 3339 UTC), whatever the session timezone.
	e.At = e.At.UTC()
	e.RoomID = roomID
	e.Type = events.Type(typ)
	if err := json.Unmarshal(actorRaw, &e.Actor); err != nil {
		return events.Event{}, fmt.Errorf("store: decode actor: %w", err)
	}
	e.Payload = json.RawMessage(payloadRaw)
	return e, nil
}

// SubscribeBuffer is the channel capacity of one Subscribe call. On a full
// channel the subscriber goroutine blocks in its send (until the reader takes
// the event or ctx ends) and lastSeq only advances after a successful send,
// so the size only trades memory for burst absorption; nothing is lost.
// Each slot holds an events.Event by value (136 B), and the WebSocket gateway
// opens one subscription per connection and drains it into its own send
// buffer straight away, so 256 slots cost ~35 KB per open socket for no
// latency benefit (reviews/load-test-2026-10.md).
const SubscribeBuffer = 16

// Subscribe replays events after afterSeq then streams new ones live from the
// shared LISTEN hub. The channel closes when ctx ends.
func (s *PG) Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan events.Event, error) {
	if _, err := s.GetRoom(ctx, roomID); err != nil {
		return nil, err
	}
	sub := &subscriber{
		roomID:  roomID,
		lastSeq: afterSeq,
		ch:      make(chan events.Event, SubscribeBuffer),
		wake:    make(chan struct{}, 1),
	}
	s.hub.register(sub)
	go s.runSub(ctx, sub)
	return sub.ch, nil
}

// ---- proposals ----

// CreateProposal stores a proposal for cross-restart lookup.
func (s *PG) CreateProposal(ctx context.Context, p ProposalRow) error {
	if err := p.Check(); err != nil {
		return fmt.Errorf("store: create proposal %s: %w", p.ProposalID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	layers, err := json.Marshal(p.RequiredLayers)
	if err != nil {
		return fmt.Errorf("store: marshal proposal layers: %w", err)
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO proposals (proposal_id, room_id, title, status, required_layers, steps, risk, rollback, params_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		p.ProposalID, p.RoomID, p.Title, p.Status, layers, p.Steps, p.Risk, p.Rollback, p.ParamsHash); err != nil {
		return fmt.Errorf("store: create proposal %s: %w", p.ProposalID, classify(err))
	}
	return nil
}

// DeleteProposal removes a proposal row (compensation when the matching event
// append failed).
func (s *PG) DeleteProposal(ctx context.Context, proposalID string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx, `DELETE FROM proposals WHERE proposal_id = $1`, proposalID)
	if err != nil {
		return fmt.Errorf("store: delete proposal %s: %w", proposalID, err)
	}
	return nil
}

// GetProposal returns a proposal row or ErrNotFound.
func (s *PG) GetProposal(ctx context.Context, proposalID string) (ProposalRow, error) {
	if CheckText(proposalID) != nil {
		return ProposalRow{}, fmt.Errorf("store: proposal: %w", ErrNotFound)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var p ProposalRow
	var layers, steps []byte
	err := s.pool.QueryRow(ctx,
		`SELECT proposal_id, room_id, title, status, required_layers, steps, risk, rollback, params_hash
		 FROM proposals WHERE proposal_id = $1`, proposalID).
		Scan(&p.ProposalID, &p.RoomID, &p.Title, &p.Status, &layers, &steps, &p.Risk, &p.Rollback, &p.ParamsHash)
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

// RecordApproval records one decision on a proposal layer with its params_hash
// and identity. A second decision on the same layer returns ErrAlreadyExists.
func (s *PG) RecordApproval(ctx context.Context, a ApprovalRow) error {
	if err := a.Check(); err != nil {
		return fmt.Errorf("store: record approval %s/%s: %w", a.ProposalID, a.Layer, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var authTime *time.Time
	if !a.IdentityAuthTime.IsZero() {
		t := a.IdentityAuthTime.UTC()
		authTime = &t
	}
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO approvals (proposal_id, layer, decision, reason, params_hash,
		     identity_iss, identity_sub, identity_session_id, identity_method, identity_grant, identity_auth_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (proposal_id, layer) DO NOTHING`,
		a.ProposalID, a.Layer, a.Decision, a.Reason, a.ParamsHash,
		a.IdentityIss, a.IdentitySub, a.IdentitySessionID, a.IdentityMethod, a.IdentityGrant, authTime)
	if err != nil {
		return fmt.Errorf("store: record approval %s/%s: %w", a.ProposalID, a.Layer, classify(err))
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: approval %s/%s: %w", a.ProposalID, a.Layer, ErrAlreadyExists)
	}
	return nil
}

// ListApprovals returns the decisions recorded for a proposal, ordered by layer.
func (s *PG) ListApprovals(ctx context.Context, proposalID string) ([]ApprovalRow, error) {
	if CheckText(proposalID) != nil {
		return nil, nil
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT proposal_id, layer, decision, reason, params_hash,
		        identity_iss, identity_sub, identity_session_id, identity_method, identity_grant, identity_auth_time
		 FROM approvals WHERE proposal_id = $1 ORDER BY layer`, proposalID)
	if err != nil {
		return nil, fmt.Errorf("store: list approvals %s: %w", proposalID, err)
	}
	defer rows.Close()
	var out []ApprovalRow
	for rows.Next() {
		var a ApprovalRow
		var authTime *time.Time
		if err := rows.Scan(&a.ProposalID, &a.Layer, &a.Decision, &a.Reason, &a.ParamsHash,
			&a.IdentityIss, &a.IdentitySub, &a.IdentitySessionID, &a.IdentityMethod, &a.IdentityGrant, &authTime); err != nil {
			return nil, fmt.Errorf("store: scan approval: %w", err)
		}
		if authTime != nil {
			a.IdentityAuthTime = authTime.UTC()
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list approvals %s: %w", proposalID, err)
	}
	return out, nil
}

// SetProposalParamsHash stores the params_hash of a proposal that has none (a
// row written before auth PR 4). It never replaces an existing hash: a
// different value is ErrParamsHashConflict, the same value is a no-op. A missing
// proposal is ErrNotFound.
func (s *PG) SetProposalParamsHash(ctx context.Context, proposalID, hash string) error {
	if err := CheckText(proposalID, hash); err != nil {
		return fmt.Errorf("store: set params hash %s: %w", proposalID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE proposals SET params_hash = $2 WHERE proposal_id = $1 AND params_hash = ''`, proposalID, hash)
	if err != nil {
		return fmt.Errorf("store: set params hash %s: %w", proposalID, err)
	}
	if tag.RowsAffected() == 0 {
		var cur string
		err := s.pool.QueryRow(ctx, `SELECT params_hash FROM proposals WHERE proposal_id = $1`, proposalID).Scan(&cur)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("store: proposal %s: %w", proposalID, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("store: set params hash %s: %w", proposalID, err)
		}
		if cur != hash {
			return fmt.Errorf("store: set params hash %s: %w", proposalID, ErrParamsHashConflict)
		}
	}
	return nil
}

// DeleteApproval removes one approval row (compensation when the matching
// event append failed).
func (s *PG) DeleteApproval(ctx context.Context, proposalID, layer string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`DELETE FROM approvals WHERE proposal_id = $1 AND layer = $2`, proposalID, layer)
	if err != nil {
		return fmt.Errorf("store: delete approval %s/%s: %w", proposalID, layer, err)
	}
	return nil
}

// StartProposalStep marks a step as started so it never runs twice. A second
// call for the same (proposal, step) returns ErrStepOrder.
func (s *PG) StartProposalStep(ctx context.Context, proposalID string, stepIndex int) error {
	if err := CheckText(proposalID); err != nil {
		return fmt.Errorf("store: start step %s/%d: %w", proposalID, stepIndex, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO proposal_steps (proposal_id, step_index, status) VALUES ($1, $2, 'started')
		 ON CONFLICT (proposal_id, step_index) DO NOTHING`,
		proposalID, stepIndex)
	if err != nil {
		return fmt.Errorf("store: start step %s/%d: %w", proposalID, stepIndex, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: step %s/%d already started: %w", proposalID, stepIndex, ErrStepOrder)
	}
	return nil
}

// FinishProposalStep records the final state of a step ("done" or "failed").
// Only a step in state "started" can be finished; finishing a step that was
// never started (or finishing it twice) returns ErrStepOrder.
func (s *PG) FinishProposalStep(ctx context.Context, proposalID string, stepIndex int, status, output string) error {
	if err := CheckText(proposalID, status, output); err != nil {
		return fmt.Errorf("store: finish step %s/%d: %w", proposalID, stepIndex, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE proposal_steps SET status = $3, output = $4
		 WHERE proposal_id = $1 AND step_index = $2 AND status = 'started'`,
		proposalID, stepIndex, status, output)
	if err != nil {
		return fmt.Errorf("store: finish step %s/%d: %w", proposalID, stepIndex, classify(err))
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: step %s/%d is not started: %w", proposalID, stepIndex, ErrStepOrder)
	}
	return nil
}

// SetProposalStatus updates a proposal's status (executed / failed / rejected).
func (s *PG) SetProposalStatus(ctx context.Context, proposalID, status string) error {
	if err := CheckText(proposalID, status); err != nil {
		return fmt.Errorf("store: set proposal status %s: %w", proposalID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`UPDATE proposals SET status = $2 WHERE proposal_id = $1`, proposalID, status); err != nil {
		return fmt.Errorf("store: set proposal status %s: %w", proposalID, err)
	}
	return nil
}

// ---- suggestions ----

// CreateSuggestion stores a pending suggestion.
func (s *PG) CreateSuggestion(ctx context.Context, r SuggestionRow) error {
	if err := r.Check(); err != nil {
		return fmt.Errorf("store: create suggestion %s: %w", r.SuggestionID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO suggestions (suggestion_id, room_id, from_id, from_name, text, status)
		 VALUES ($1, $2, $3, $4, $5, 'pending')`,
		r.SuggestionID, r.RoomID, r.FromID, r.FromName, r.Text); err != nil {
		return fmt.Errorf("store: create suggestion %s: %w", r.SuggestionID, classify(err))
	}
	return nil
}

// DeleteSuggestion removes a suggestion row (compensation when the matching
// event append failed).
func (s *PG) DeleteSuggestion(ctx context.Context, suggestionID string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx, `DELETE FROM suggestions WHERE suggestion_id = $1`, suggestionID)
	if err != nil {
		return fmt.Errorf("store: delete suggestion %s: %w", suggestionID, err)
	}
	return nil
}

// GetSuggestion returns a suggestion row or ErrNotFound.
func (s *PG) GetSuggestion(ctx context.Context, suggestionID string) (SuggestionRow, error) {
	if CheckText(suggestionID) != nil {
		return SuggestionRow{}, fmt.Errorf("store: suggestion: %w", ErrNotFound)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
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
	if err := CheckText(suggestionID, status); err != nil {
		return fmt.Errorf("store: set suggestion status %s: %w", suggestionID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`UPDATE suggestions SET status = $2 WHERE suggestion_id = $1`, suggestionID, status); err != nil {
		return fmt.Errorf("store: set suggestion status %s: %w", suggestionID, err)
	}
	return nil
}

// ---- handoffs ----

// CreateHandoff stores a pending handoff.
func (s *PG) CreateHandoff(ctx context.Context, h HandoffRow) error {
	if err := h.Check(); err != nil {
		return fmt.Errorf("store: create handoff %s: %w", h.HandoffID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO handoffs (handoff_id, room_id, from_id, from_name, to_id, to_name, status, summary, stay_as_watcher)
		 VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8)`,
		h.HandoffID, h.RoomID, h.FromID, h.FromName, h.ToID, h.ToName, h.Summary, h.StayAsWatcher); err != nil {
		return fmt.Errorf("store: create handoff %s: %w", h.HandoffID, classify(err))
	}
	return nil
}

// DeleteHandoff removes a handoff row (compensation when the matching event
// append failed).
func (s *PG) DeleteHandoff(ctx context.Context, handoffID string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx, `DELETE FROM handoffs WHERE handoff_id = $1`, handoffID)
	if err != nil {
		return fmt.Errorf("store: delete handoff %s: %w", handoffID, err)
	}
	return nil
}

// GetHandoff returns a handoff row or ErrNotFound.
func (s *PG) GetHandoff(ctx context.Context, handoffID string) (HandoffRow, error) {
	if CheckText(handoffID) != nil {
		return HandoffRow{}, fmt.Errorf("store: handoff: %w", ErrNotFound)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var h HandoffRow
	var summary []byte
	err := s.pool.QueryRow(ctx,
		`SELECT handoff_id, room_id, from_id, from_name, to_id, to_name, status, summary, stay_as_watcher FROM handoffs WHERE handoff_id = $1`,
		handoffID).Scan(&h.HandoffID, &h.RoomID, &h.FromID, &h.FromName, &h.ToID, &h.ToName, &h.Status, &summary, &h.StayAsWatcher)
	if errors.Is(err, pgx.ErrNoRows) {
		return HandoffRow{}, fmt.Errorf("store: handoff %s: %w", handoffID, ErrNotFound)
	}
	if err != nil {
		return HandoffRow{}, fmt.Errorf("store: get handoff %s: %w", handoffID, err)
	}
	h.Summary = json.RawMessage(summary)
	return h, nil
}

// SetHandoffStatus marks a handoff accepted or voided.
func (s *PG) SetHandoffStatus(ctx context.Context, handoffID, status string) error {
	if err := CheckText(handoffID, status); err != nil {
		return fmt.Errorf("store: set handoff status %s: %w", handoffID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`UPDATE handoffs SET status = $2 WHERE handoff_id = $1`, handoffID, status); err != nil {
		return fmt.Errorf("store: set handoff status %s: %w", handoffID, err)
	}
	return nil
}

// VoidHandoffs voids every pending handoff in a room except the accepted one.
func (s *PG) VoidHandoffs(ctx context.Context, roomID, exceptHandoffID string) error {
	if err := CheckText(roomID, exceptHandoffID); err != nil {
		return fmt.Errorf("store: void handoffs %s: %w", roomID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`UPDATE handoffs SET status = 'voided' WHERE room_id = $1 AND status = 'pending' AND handoff_id <> $2`,
		roomID, exceptHandoffID); err != nil {
		return fmt.Errorf("store: void handoffs %s: %w", roomID, err)
	}
	return nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// utcTimes normalises the timestamps pgx scans in the session's local zone to
// UTC, so rooms read back after a restart keep the RFC 3339 UTC guarantee of
// docs/api.md:7 regardless of the host timezone.
func utcTimes(r *Room) {
	r.OpenedAt = r.OpenedAt.UTC()
	if r.ClosedAt != nil {
		c := r.ClosedAt.UTC()
		r.ClosedAt = &c
	}
	if r.DriverLeaseUntil != nil {
		l := r.DriverLeaseUntil.UTC()
		r.DriverLeaseUntil = &l
	}
}
