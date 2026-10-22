package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/clients"
)

// FleetStore is the read and write surface of the fleet view (X5a): the
// watcher's status rows (migration 00019) and the cross-pipeline change pages
// over pipeline_changes, plus the 24-hour change count GET /api/fleet shows
// per pipeline. Both *PG and memstore.Store implement it. It is separate from
// Store like ChangeStore.
type FleetStore interface {
	clients.FleetChangeReader
	clients.WatchStatusWriter
	clients.WatchStatusReader
	// CountChangesSince counts, per pipeline of pipelineIDs, the
	// pipeline_changes rows with at >= since, every kind. A pipeline with no
	// such row is absent from the map; pipelineIDs empty = an empty map.
	CountChangesSince(ctx context.Context, pipelineIDs []string, since time.Time) (map[string]int, error)
}

var _ FleetStore = (*PG)(nil)

// ErrBadWatchStatus is returned for a status row that breaks the
// clients.WatchStatus rules; nothing is written.
var ErrBadWatchStatus = errors.New("store: bad watch status")

// NormalizeWatchStatus checks a status row the way both stores do and returns
// the form they keep: times in UTC cut to microseconds (what Postgres keeps)
// and Signals never nil. An empty pipeline id, a zero CheckedAt, more than
// clients.MaxWatchSignals signals, a signal without a name or since, a state
// other than firing or acked, or a severity outside sev1..sev4 is refused.
func NormalizeWatchStatus(s clients.WatchStatus) (clients.WatchStatus, error) {
	switch {
	case s.PipelineID == "":
		return s, fmt.Errorf("%w: no pipeline id", ErrBadWatchStatus)
	case s.CheckedAt.IsZero():
		return s, fmt.Errorf("%w: no checked_at", ErrBadWatchStatus)
	case len(s.Signals) > clients.MaxWatchSignals:
		return s, fmt.Errorf("%w: %d signals, at most %d", ErrBadWatchStatus, len(s.Signals), clients.MaxWatchSignals)
	}
	s.CheckedAt = microUTC(s.CheckedAt)
	sigs := make([]clients.WatchSignal, 0, len(s.Signals))
	for _, g := range s.Signals {
		switch {
		case g.Signal == "":
			return s, fmt.Errorf("%w: a signal without a name", ErrBadWatchStatus)
		case g.State != clients.WatchFiring && g.State != clients.WatchAcked:
			return s, fmt.Errorf("%w: signal %s has state %q", ErrBadWatchStatus, g.Signal, g.State)
		case g.Severity != "sev1" && g.Severity != "sev2" && g.Severity != "sev3" && g.Severity != "sev4":
			return s, fmt.Errorf("%w: signal %s has severity %q", ErrBadWatchStatus, g.Signal, g.Severity)
		case g.Since.IsZero():
			return s, fmt.Errorf("%w: signal %s has no since", ErrBadWatchStatus, g.Signal)
		}
		g.Since = microUTC(g.Since)
		if g.ETA != nil {
			eta := microUTC(*g.ETA)
			g.ETA = &eta
		}
		sigs = append(sigs, g)
	}
	s.Signals = sigs
	return s, nil
}

func microUTC(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

// ClampFleetLimit bounds a ListFleetChanges limit to 0..MaxFleetChangesPage
// (0 = no rows, no I/O).
func ClampFleetLimit(limit int) int {
	return max(0, min(limit, clients.MaxFleetChangesPage))
}

// PutWatchStatus implements clients.WatchStatusWriter: the row of
// s.PipelineID is replaced.
func (s *PG) PutWatchStatus(ctx context.Context, st clients.WatchStatus) error {
	st, err := NormalizeWatchStatus(st)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(st.Signals)
	if err != nil {
		return fmt.Errorf("store: encode watch status: %w", err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO watch_status (pipeline_id, checked_at, signals) VALUES ($1, $2, $3)
		 ON CONFLICT (pipeline_id) DO UPDATE SET checked_at = EXCLUDED.checked_at, signals = EXCLUDED.signals`,
		st.PipelineID, st.CheckedAt, raw); err != nil {
		return fmt.Errorf("store: put watch status: %w", err)
	}
	return nil
}

// WatchStatuses implements clients.WatchStatusReader, in pipeline id order.
func (s *PG) WatchStatuses(ctx context.Context, pipelineIDs []string) ([]clients.WatchStatus, error) {
	out := []clients.WatchStatus{}
	if len(pipelineIDs) == 0 {
		return out, nil
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT pipeline_id, checked_at, signals FROM watch_status
		 WHERE pipeline_id = ANY($1) ORDER BY pipeline_id`, pipelineIDs)
	if err != nil {
		return nil, fmt.Errorf("store: list watch status: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var st clients.WatchStatus
		var raw []byte
		if err := rows.Scan(&st.PipelineID, &st.CheckedAt, &raw); err != nil {
			return nil, fmt.Errorf("store: scan watch status: %w", err)
		}
		st.CheckedAt = st.CheckedAt.UTC()
		if err := json.Unmarshal(raw, &st.Signals); err != nil {
			return nil, fmt.Errorf("store: decode watch status of %s: %w", st.PipelineID, err)
		}
		if st.Signals == nil {
			st.Signals = []clients.WatchSignal{}
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list watch status: %w", err)
	}
	return out, nil
}

// ListFleetChanges implements clients.FleetChangeReader. The order (at DESC,
// id DESC) and the row comparison against the cursor make a page a position:
// a row recorded after the first page never shifts a later one.
func (s *PG) ListFleetChanges(ctx context.Context, pipelineIDs []string, since time.Time, kinds []string, before clients.ChangeCursor, limit int) ([]clients.PipelineChange, error) {
	limit = ClampFleetLimit(limit)
	if limit == 0 || len(pipelineIDs) == 0 {
		return []clients.PipelineChange{}, nil
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var sinceArg, beforeAt any
	if !since.IsZero() {
		sinceArg = since.UTC()
	}
	if !before.At.IsZero() {
		beforeAt = before.At.UTC()
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, pipeline_id, kind, object, at, actor, source, fingerprint, before, after, summary
		 FROM pipeline_changes
		 WHERE pipeline_id = ANY($1)
		   AND ($2::timestamptz IS NULL OR at >= $2)
		   AND (cardinality($3::text[]) = 0 OR kind = ANY($3))
		   AND ($4::timestamptz IS NULL OR (at, id) < ($4, $5::bigint))
		 ORDER BY at DESC, id DESC LIMIT $6`,
		pipelineIDs, sinceArg, append([]string{}, kinds...), beforeAt, before.ID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list fleet changes: %w", err)
	}
	defer rows.Close()
	out := []clients.PipelineChange{}
	for rows.Next() {
		var c clients.PipelineChange
		var b, a []byte
		if err := rows.Scan(&c.ID, &c.PipelineID, &c.Kind, &c.Object, &c.At, &c.Actor, &c.Source, &c.Fingerprint, &b, &a, &c.Summary); err != nil {
			return nil, fmt.Errorf("store: scan fleet change: %w", err)
		}
		c.At = c.At.UTC()
		if len(b) > 0 {
			c.Before = json.RawMessage(b)
		}
		if len(a) > 0 {
			c.After = json.RawMessage(a)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list fleet changes: %w", err)
	}
	return out, nil
}

// CountChangesSince implements FleetStore.
func (s *PG) CountChangesSince(ctx context.Context, pipelineIDs []string, since time.Time) (map[string]int, error) {
	out := map[string]int{}
	if len(pipelineIDs) == 0 {
		return out, nil
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT pipeline_id, count(*) FROM pipeline_changes
		 WHERE pipeline_id = ANY($1) AND at >= $2 GROUP BY pipeline_id`, pipelineIDs, since.UTC())
	if err != nil {
		return nil, fmt.Errorf("store: count changes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("store: scan change count: %w", err)
		}
		out[id] = int(n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: count changes: %w", err)
	}
	return out, nil
}
