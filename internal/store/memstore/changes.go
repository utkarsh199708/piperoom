package memstore

import (
	"context"
	"fmt"
	"sort"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
)

var _ store.ChangeStore = (*Store)(nil)

// changeTable is the in-memory pipeline_changes table (migration 00015). It
// sits under Store.mu.
type changeTable struct {
	rows   []clients.PipelineChange
	nextID int64
}

// RecordChange implements clients.ChangeWriter with the rules of store.PG:
// dedup on (pipeline, kind, object, fingerprint), then the newest
// store.MaxChangesPerPipeline rows of the pipeline are kept.
func (s *Store) RecordChange(ctx context.Context, c clients.PipelineChange) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("memstore: record change: %w", err)
	}
	c, err := store.NormalizeChange(c)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.changes.rows {
		if r.PipelineID == c.PipelineID && r.Kind == c.Kind && r.Object == c.Object && r.Fingerprint == c.Fingerprint {
			return false, nil
		}
	}
	s.changes.nextID++
	c.ID = s.changes.nextID
	s.changes.rows = append(s.changes.rows, c)
	var mine []int // indexes of this pipeline's rows
	for i, r := range s.changes.rows {
		if r.PipelineID == c.PipelineID {
			mine = append(mine, i)
		}
	}
	if len(mine) > store.MaxChangesPerPipeline {
		sort.Slice(mine, func(a, b int) bool { return newerChange(s.changes.rows[mine[a]], s.changes.rows[mine[b]]) })
		drop := map[int]bool{}
		for _, i := range mine[store.MaxChangesPerPipeline:] {
			drop[i] = true
		}
		kept := s.changes.rows[:0]
		for i, r := range s.changes.rows {
			if !drop[i] {
				kept = append(kept, r)
			}
		}
		s.changes.rows = kept
	}
	return true, nil
}

// newerChange orders by at, then id, newest first.
func newerChange(a, b clients.PipelineChange) bool {
	if !a.At.Equal(b.At) {
		return a.At.After(b.At)
	}
	return a.ID > b.ID
}

// ListChanges implements clients.ChangeReader (since inclusive, until
// exclusive, zero = open).
func (s *Store) ListChanges(ctx context.Context, pipelineID string, since, until time.Time, kinds []string, limit int) ([]clients.PipelineChange, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memstore: list changes: %w", err)
	}
	limit = store.ClampChangeLimit(limit)
	out := []clients.PipelineChange{}
	if limit == 0 {
		return out, nil
	}
	s.mu.Lock()
	for _, r := range s.changes.rows {
		if r.PipelineID != pipelineID || (!since.IsZero() && r.At.Before(since)) || (!until.IsZero() && !r.At.Before(until)) {
			continue
		}
		if len(kinds) > 0 && !containsStr(kinds, r.Kind) {
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

// PurgeChangesBefore implements store.ChangeStore.
func (s *Store) PurgeChangesBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("memstore: purge changes: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	kept := s.changes.rows[:0]
	for _, r := range s.changes.rows {
		if r.At.Before(cutoff) {
			n++
			continue
		}
		kept = append(kept, r)
	}
	s.changes.rows = kept
	return n, nil
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
