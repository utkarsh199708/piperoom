package memstore

import (
	"context"
	"fmt"
	"sort"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
)

var _ store.SampleStore = (*Store)(nil)

// sampleKey names one series of the in-memory watch_samples table (migration
// 00018). The table sits under Store.mu.
type sampleKey struct{ pipeline, metric string }

// AppendSample implements clients.SampleWriter with the rules of store.PG:
// a sample with the same (pipeline, metric, at) is not written again; each
// series stays ordered by at.
func (s *Store) AppendSample(ctx context.Context, pipelineID, metric string, at time.Time, value float64) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memstore: append sample: %w", err)
	}
	at, err := store.NormalizeSample(pipelineID, metric, at, value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.samples == nil {
		s.samples = map[sampleKey][]clients.SamplePoint{}
	}
	k := sampleKey{pipelineID, metric}
	pts := s.samples[k]
	i := sort.Search(len(pts), func(i int) bool { return !pts[i].At.Before(at) })
	if i < len(pts) && pts[i].At.Equal(at) {
		return nil
	}
	pts = append(pts, clients.SamplePoint{})
	copy(pts[i+1:], pts[i:])
	pts[i] = clients.SamplePoint{At: at, Value: value}
	s.samples[k] = pts
	return nil
}

// Samples implements clients.SampleReader (at >= since, oldest first, the
// newest clients.MaxSamplesPerSeries when more match).
func (s *Store) Samples(ctx context.Context, pipelineID, metric string, since time.Time) ([]clients.SamplePoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memstore: list samples: %w", err)
	}
	if err := store.CheckMetric(metric); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pts := s.samples[sampleKey{pipelineID, metric}]
	i := sort.Search(len(pts), func(i int) bool { return !pts[i].At.Before(since) })
	pts = pts[i:]
	if len(pts) > clients.MaxSamplesPerSeries {
		pts = pts[len(pts)-clients.MaxSamplesPerSeries:]
	}
	return append([]clients.SamplePoint{}, pts...), nil
}

// PurgeSamplesBefore implements store.SampleStore.
func (s *Store) PurgeSamplesBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("memstore: purge samples: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for k, pts := range s.samples {
		i := sort.Search(len(pts), func(i int) bool { return !pts[i].At.Before(cutoff) })
		n += int64(i)
		if i == len(pts) {
			delete(s.samples, k)
			continue
		}
		if i > 0 {
			s.samples[k] = append([]clients.SamplePoint{}, pts[i:]...)
		}
	}
	return n, nil
}
