package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/events"
)

// Shadow run statuses (docs/design/shadow.md section 3.3).
const (
	ShadowQueued       = "queued"
	ShadowRunning      = "running"
	ShadowHypothesised = "hypothesised"
	ShadowNoHypothesis = "no_hypothesis"
	ShadowError        = "error"
	ShadowBudget       = "budget"
	ShadowInterrupted  = "interrupted"
)

// ShadowRun is the shadow_runs row. Its JSON form is the ShadowRun object of
// docs/api.md (GET /api/rooms/{id}/shadow): unset times are null, and
// final_hypothesis is null until the agent emits one.
type ShadowRun struct {
	RoomID             string          `json:"room_id"`
	Status             string          `json:"status"`
	LastSeq            int64           `json:"-"`
	Model              string          `json:"model"`
	PipelineMap        string          `json:"pipeline_map"`
	StartedAt          *time.Time      `json:"started_at"`
	FirstHypothesisAt  *time.Time      `json:"first_hypothesis_at"`
	FinalHypothesisAt  *time.Time      `json:"final_hypothesis_at"`
	FinishedAt         *time.Time      `json:"finished_at"`
	FinishedAfterClose bool            `json:"finished_after_close"`
	FinalHypothesis    json.RawMessage `json:"final_hypothesis"`
	ToolCalls          int             `json:"tool_calls"`
	Tokens             int64           `json:"tokens"`
	RevealedAt         *time.Time      `json:"revealed_at"`
}

// Finished reports whether the run has ended (finished_at is set).
func (r ShadowRun) Finished() bool { return r.FinishedAt != nil }

// Check is the pre-check for a shadow_runs row.
func (r ShadowRun) Check() error {
	if err := CheckText(r.RoomID, r.Status, r.Model, r.PipelineMap); err != nil {
		return err
	}
	return CheckJSON(r.FinalHypothesis)
}

// ShadowRunRoom is a run of a closed room with the room facts the metrics need.
type ShadowRunRoom struct {
	ShadowRun
	PipelineID  string
	AlertSource string
	OpenedAt    time.Time
	ClosedAt    time.Time
}

// utcPtr returns t in UTC, keeping nil.
func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// UTC normalises every time of the run to UTC (docs/api.md: times are RFC 3339
// UTC), whatever the database session's zone.
func (r *ShadowRun) UTC() {
	r.StartedAt = utcPtr(r.StartedAt)
	r.FirstHypothesisAt = utcPtr(r.FirstHypothesisAt)
	r.FinalHypothesisAt = utcPtr(r.FinalHypothesisAt)
	r.FinishedAt = utcPtr(r.FinishedAt)
	r.RevealedAt = utcPtr(r.RevealedAt)
}

// nullJSON makes an empty raw message a SQL NULL rather than invalid jsonb.
func nullJSON(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return []byte(raw)
}

var _ ShadowStore = (*PG)(nil)

// CreateShadowRun implements ShadowStore.
func (s *PG) CreateShadowRun(ctx context.Context, r ShadowRun) error {
	if err := r.Check(); err != nil {
		return fmt.Errorf("store: create shadow run %s: %w", r.RoomID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO shadow_runs (room_id, status, model, pipeline_map, started_at, first_hypothesis_at, final_hypothesis_at,
		     finished_at, finished_after_close, final_hypothesis, tool_calls, tokens)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 ON CONFLICT (room_id) DO NOTHING`,
		r.RoomID, r.Status, r.Model, r.PipelineMap, r.StartedAt, r.FirstHypothesisAt, r.FinalHypothesisAt,
		r.FinishedAt, r.FinishedAfterClose, nullJSON(r.FinalHypothesis), r.ToolCalls, r.Tokens)
	if err != nil {
		return fmt.Errorf("store: create shadow run %s: %w", r.RoomID, classify(err))
	}
	return nil
}

const shadowRunCols = `room_id, status, last_seq, model, pipeline_map, started_at, first_hypothesis_at, final_hypothesis_at,
	finished_at, finished_after_close, final_hypothesis, tool_calls, tokens, revealed_at`

func scanShadowRun(row scanner, extra ...any) (ShadowRun, error) {
	var r ShadowRun
	var hyp []byte
	dest := []any{&r.RoomID, &r.Status, &r.LastSeq, &r.Model, &r.PipelineMap, &r.StartedAt, &r.FirstHypothesisAt,
		&r.FinalHypothesisAt, &r.FinishedAt, &r.FinishedAfterClose, &hyp, &r.ToolCalls, &r.Tokens, &r.RevealedAt}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return ShadowRun{}, err
	}
	if len(hyp) > 0 {
		r.FinalHypothesis = json.RawMessage(hyp)
	}
	r.UTC()
	return r, nil
}

// GetShadowRun implements ShadowStore.
func (s *PG) GetShadowRun(ctx context.Context, roomID string) (ShadowRun, error) {
	if CheckText(roomID) != nil {
		return ShadowRun{}, fmt.Errorf("store: shadow run: %w", ErrNotFound)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	r, err := scanShadowRun(s.pool.QueryRow(ctx, `SELECT `+shadowRunCols+` FROM shadow_runs WHERE room_id = $1`, roomID))
	if errors.Is(err, pgx.ErrNoRows) {
		return ShadowRun{}, fmt.Errorf("store: shadow run %s: %w", roomID, ErrNotFound)
	}
	if err != nil {
		return ShadowRun{}, fmt.Errorf("store: get shadow run %s: %w", roomID, err)
	}
	return r, nil
}

// UpdateShadowRun implements ShadowStore.
func (s *PG) UpdateShadowRun(ctx context.Context, r ShadowRun) error {
	if err := r.Check(); err != nil {
		return fmt.Errorf("store: update shadow run %s: %w", r.RoomID, err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE shadow_runs SET status = $2, model = $3, pipeline_map = $4, started_at = $5,
		     finished_at = $6, finished_after_close = $7, tool_calls = $8, tokens = $9
		 WHERE room_id = $1`,
		r.RoomID, r.Status, r.Model, r.PipelineMap, r.StartedAt,
		r.FinishedAt, r.FinishedAfterClose, r.ToolCalls, r.Tokens)
	if err != nil {
		return fmt.Errorf("store: update shadow run %s: %w", r.RoomID, classify(err))
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: update shadow run %s: %w", r.RoomID, ErrNotFound)
	}
	return nil
}

// AppendShadow implements ShadowStore. The seq comes from shadow_runs.last_seq
// in the same transaction as the insert, like Append; there is no NOTIFY,
// because nothing streams the shadow log.
func (s *PG) AppendShadow(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: marshal shadow payload: %w", err)
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: marshal shadow actor: %w", err)
	}
	if err := CheckAppend(roomID, typ, payloadJSON, actorJSON); err != nil {
		return events.Event{}, fmt.Errorf("store: append shadow to %s: %w", roomID, err)
	}
	at := s.now().UTC()
	var seq int64
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`UPDATE shadow_runs SET last_seq = last_seq + 1 WHERE room_id = $1 RETURNING last_seq`, roomID).Scan(&seq); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("store: append shadow to %s: no run row: %w", roomID, ErrNotFound)
			}
			return fmt.Errorf("store: reserve shadow seq for %s: %w", roomID, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO shadow_events (room_id, seq, type, actor, payload, at) VALUES ($1, $2, $3, $4, $5, $6)`,
			roomID, seq, string(typ), actorJSON, payloadJSON, at); err != nil {
			return fmt.Errorf("store: insert shadow event %s/%d: %w", roomID, seq, classify(err))
		}
		if typ == events.AgentHypothesis {
			if _, err := tx.Exec(ctx,
				`UPDATE shadow_runs SET first_hypothesis_at = COALESCE(first_hypothesis_at, $2),
				     final_hypothesis_at = $2, final_hypothesis = $3
				 WHERE room_id = $1`, roomID, at, payloadJSON); err != nil {
				return fmt.Errorf("store: record shadow hypothesis %s: %w", roomID, classify(err))
			}
		}
		return nil
	})
	if err != nil {
		return events.Event{}, err
	}
	return events.Event{RoomID: roomID, Seq: seq, Type: typ, Actor: actor, Payload: payloadJSON, At: at}, nil
}

// ShadowSince implements ShadowStore.
func (s *PG) ShadowSince(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	if limit <= 0 {
		return []events.Event{}, nil
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT seq, type, actor, payload, at FROM shadow_events WHERE room_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3`,
		roomID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("store: shadow since %s: %w", roomID, err)
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
		return nil, fmt.Errorf("store: shadow since %s: %w", roomID, err)
	}
	return out, nil
}

// CountShadowEvents implements ShadowStore.
func (s *PG) CountShadowEvents(ctx context.Context, roomID string, typ events.Type) (int, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var n int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM shadow_events WHERE room_id = $1 AND type = $2`, roomID, string(typ)).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count shadow events %s: %w", roomID, err)
	}
	return n, nil
}

// MarkRevealed implements ShadowStore.
func (s *PG) MarkRevealed(ctx context.Context, roomID string, at time.Time) (bool, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`UPDATE shadow_runs SET revealed_at = $2 WHERE room_id = $1 AND revealed_at IS NULL`, roomID, at.UTC())
	if err != nil {
		return false, fmt.Errorf("store: mark revealed %s: %w", roomID, err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM shadow_runs WHERE room_id = $1)`, roomID).Scan(&exists); err != nil {
		return false, fmt.Errorf("store: mark revealed %s: %w", roomID, err)
	}
	if !exists {
		return false, fmt.Errorf("store: mark revealed %s: %w", roomID, ErrNotFound)
	}
	return false, nil
}

// ListShadowRuns implements ShadowStore. alert_source comes from the room's
// room.created event (seq 1), which is where the room service records it.
func (s *PG) ListShadowRuns(ctx context.Context, pipelineID string, since time.Time) ([]ShadowRunRoom, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT sr.room_id, sr.status, sr.last_seq, sr.model, sr.pipeline_map, sr.started_at, sr.first_hypothesis_at,
		        sr.final_hypothesis_at, sr.finished_at, sr.finished_after_close, sr.final_hypothesis, sr.tool_calls,
		        sr.tokens, sr.revealed_at,
		        r.pipeline_id, COALESCE((SELECT e.payload->>'alert_source' FROM room_events e
		                                 WHERE e.room_id = r.id AND e.seq = 1 AND e.type = 'room.created'), ''),
		        r.opened_at, r.closed_at
		 FROM shadow_runs sr JOIN rooms r ON r.id = sr.room_id
		 WHERE r.status = 'closed' AND r.closed_at IS NOT NULL AND r.closed_at >= $2
		   AND ($1 = '' OR r.pipeline_id = $1)
		 ORDER BY r.closed_at DESC, r.id ASC`, pipelineID, since.UTC())
	if err != nil {
		return nil, fmt.Errorf("store: list shadow runs: %w", err)
	}
	defer rows.Close()
	out := []ShadowRunRoom{}
	for rows.Next() {
		var rr ShadowRunRoom
		run, err := scanShadowRun(rows, &rr.PipelineID, &rr.AlertSource, &rr.OpenedAt, &rr.ClosedAt)
		if err != nil {
			return nil, fmt.Errorf("store: scan shadow run: %w", err)
		}
		rr.ShadowRun = run
		rr.OpenedAt, rr.ClosedAt = rr.OpenedAt.UTC(), rr.ClosedAt.UTC()
		out = append(out, rr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list shadow runs: %w", err)
	}
	return out, nil
}

// UnrevealedShadowRuns implements ShadowStore.
func (s *PG) UnrevealedShadowRuns(ctx context.Context) ([]string, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT sr.room_id FROM shadow_runs sr JOIN rooms r ON r.id = sr.room_id
		 WHERE r.status = 'closed' AND sr.finished_at IS NOT NULL AND sr.revealed_at IS NULL
		 ORDER BY sr.room_id`)
	if err != nil {
		return nil, fmt.Errorf("store: unrevealed shadow runs: %w", err)
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan unrevealed shadow run: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: unrevealed shadow runs: %w", err)
	}
	return ids, nil
}
