package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"piperoom/internal/clients"
)

// SampleStore is the watch sample history (X3a, migration 00018): the two
// client interfaces plus the age-based purge the samples retention loop calls
// (config.WatchSampleRetention). Both *PG and memstore.Store implement it. It
// is separate from Store like ChangeStore.
type SampleStore interface {
	clients.SampleWriter
	clients.SampleReader
	// PurgeSamplesBefore deletes every sample whose at is before cutoff, in
	// all pipelines, and returns how many.
	PurgeSamplesBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

var _ SampleStore = (*PG)(nil)

// ErrUnknownMetric is returned for a metric outside clients.WatchMetrics().
var ErrUnknownMetric = errors.New("store: unknown watch metric")

// NormalizeSample checks one sample the way both stores do and returns its
// time as stored: UTC, cut to microseconds (what Postgres keeps). An empty
// pipeline id, an unknown metric, a zero time and a NaN or infinite value are
// refused (clients.SampleWriter).
func NormalizeSample(pipelineID, metric string, at time.Time, value float64) (time.Time, error) {
	if err := CheckMetric(metric); err != nil {
		return time.Time{}, err
	}
	switch {
	case pipelineID == "":
		return time.Time{}, errors.New("store: sample needs a pipeline id")
	case at.IsZero():
		return time.Time{}, errors.New("store: sample needs an at time")
	case math.IsNaN(value) || math.IsInf(value, 0):
		return time.Time{}, fmt.Errorf("store: sample value of %s is not a finite number", metric)
	}
	return at.UTC().Truncate(time.Microsecond), nil
}

// CheckMetric returns ErrUnknownMetric (wrapped, naming the metric) for a name
// outside clients.WatchMetrics().
func CheckMetric(metric string) error {
	if !slices.Contains(clients.WatchMetrics(), metric) {
		return fmt.Errorf("%w %q", ErrUnknownMetric, metric)
	}
	return nil
}

// AppendSample implements clients.SampleWriter. A second sample with the same
// (pipeline, metric, at) is not written again.
func (s *PG) AppendSample(ctx context.Context, pipelineID, metric string, at time.Time, value float64) error {
	at, err := NormalizeSample(pipelineID, metric, at, value)
	if err != nil {
		return err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO watch_samples (pipeline_id, metric, at, value) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (pipeline_id, metric, at) DO NOTHING`,
		pipelineID, metric, at, value); err != nil {
		return fmt.Errorf("store: append sample: %w", err)
	}
	return nil
}

// Samples implements clients.SampleReader: at >= since, oldest first, the
// newest clients.MaxSamplesPerSeries when more match.
func (s *PG) Samples(ctx context.Context, pipelineID, metric string, since time.Time) ([]clients.SamplePoint, error) {
	if err := CheckMetric(metric); err != nil {
		return nil, err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT at, value FROM (
		   SELECT at, value FROM watch_samples
		   WHERE pipeline_id = $1 AND metric = $2 AND at >= $3
		   ORDER BY at DESC LIMIT $4) newest
		 ORDER BY at`,
		pipelineID, metric, since.UTC(), clients.MaxSamplesPerSeries)
	if err != nil {
		return nil, fmt.Errorf("store: list samples: %w", err)
	}
	defer rows.Close()
	out := []clients.SamplePoint{}
	for rows.Next() {
		var p clients.SamplePoint
		if err := rows.Scan(&p.At, &p.Value); err != nil {
			return nil, fmt.Errorf("store: scan sample: %w", err)
		}
		p.At = p.At.UTC()
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list samples: %w", err)
	}
	return out, nil
}

// PurgeSamplesBefore implements SampleStore.
func (s *PG) PurgeSamplesBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `DELETE FROM watch_samples WHERE at < $1`, cutoff.UTC())
	if err != nil {
		return 0, fmt.Errorf("store: purge samples: %w", err)
	}
	return tag.RowsAffected(), nil
}
