package memstore

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
)

var _ store.FleetStore = (*Store)(nil)

// PutWatchStatus implements clients.WatchStatusWriter with the rules of
// store.PG: the in-memory watch_status row (migration 00019) of the pipeline
// is replaced.
func (s *Store) PutWatchStatus(ctx context.Context, st clients.WatchStatus) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memstore: put watch status: %w", err)
	}
	st, err := store.NormalizeWatchStatus(st)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == nil {
		s.status = map[string]clients.WatchStatus{}
	}
	s.status[st.PipelineID] = copyStatus(st)
	return nil
}

// WatchStatuses implements clients.WatchStatusReader, in pipeline id order.
func (s *Store) WatchStatuses(ctx context.Context, pipelineIDs []string) ([]clients.WatchStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memstore: list watch status: %w", err)
	}
	out := []clients.WatchStatus{}
	s.mu.Lock()
	for _, id := range pipelineIDs {
		if st, ok := s.status[id]; ok {
			out = append(out, copyStatus(st))
		}
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].PipelineID < out[j].PipelineID })
	// A pipeline named twice is one row, as for WHERE pipeline_id = ANY($1).
	return slices.CompactFunc(out, func(a, b clients.WatchStatus) bool { return a.PipelineID == b.PipelineID }), nil
}

// copyStatus is a deep copy, so a caller never shares the stored slice or ETA.
func copyStatus(st clients.WatchStatus) clients.WatchStatus {
	sigs := make([]clients.WatchSignal, len(st.Signals))
	for i, g := range st.Signals {
		if g.ETA != nil {
			eta := *g.ETA
			g.ETA = &eta
		}
		if g.ClearingSince != nil {
			cs := *g.ClearingSince
			g.ClearingSince = &cs
		}
		sigs[i] = g
	}
	st.Signals = sigs
	return st
}

// ListFleetChanges implements clients.FleetChangeReader with the order of
// store.PG: (at, id) descending, rows strictly after before.
func (s *Store) ListFleetChanges(ctx context.Context, pipelineIDs []string, since time.Time, kinds []string, before clients.ChangeCursor, limit int) ([]clients.PipelineChange, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memstore: list fleet changes: %w", err)
	}
	limit = store.ClampFleetLimit(limit)
	out := []clients.PipelineChange{}
	if limit == 0 || len(pipelineIDs) == 0 {
		return out, nil
	}
	cursor := clients.PipelineChange{At: before.At, ID: before.ID}
	want := idSet(pipelineIDs)
	s.mu.Lock()
	for _, r := range s.changes.rows {
		if !want[r.PipelineID] || (!since.IsZero() && r.At.Before(since)) {
			continue
		}
		if len(kinds) > 0 && !slices.Contains(kinds, r.Kind) {
			continue
		}
		if !before.At.IsZero() && !newerChange(cursor, r) {
			continue
		}
		out = append(out, r)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return newerChange(out[i], out[j]) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// CountChangesSince implements store.FleetStore.
func (s *Store) CountChangesSince(ctx context.Context, pipelineIDs []string, since time.Time) (map[string]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memstore: count changes: %w", err)
	}
	out := map[string]int{}
	want := idSet(pipelineIDs)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.changes.rows {
		if !r.At.Before(since) && want[r.PipelineID] {
			out[r.PipelineID]++
		}
	}
	return out, nil
}

func idSet(ids []string) map[string]bool {
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}
