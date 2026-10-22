package kafka

import (
	"context"
	"errors"
	"time"

	"piperoom/internal/clients"
)

// ReplayReader is clients.RecordReader plus DLQOffsets on a read-only
// Client: what the dlq_replay action reads through to validate, preview and
// copy, with nothing that produces or commits. Wiring builds no Writer with
// write mode off, yet the action must still preview (LEAD-ARCH ruling on
// P3a-4), so it reads through this instead; it is the Writer's own
// ReadRecords over the reader's connections, so the preview and the replay
// read alike.
//
// It returns record keys and values: only package actions may be handed
// one, never a read-only tool, the topology builder or the watcher.
type ReplayReader struct {
	// w is a Writer over the reader Client. It is unexported and only
	// ReadRecords is called on it, so no write is reachable through a
	// ReplayReader. w.Close is never called: the Client belongs to the caller.
	w *Writer
}

var (
	_ clients.RecordReader = (*ReplayReader)(nil)
	_ clients.RateReader   = (*ReplayReader)(nil)
)

// NewReplayReader returns a ReplayReader reading through c. The caller keeps
// owning c (and closes it); a nil c gives a ReplayReader whose calls fail.
func NewReplayReader(c *Client) *ReplayReader {
	if c == nil {
		return &ReplayReader{}
	}
	return &ReplayReader{w: &Writer{c: c}}
}

var errNoReplayClient = errors.New("kafka: replay reader has no client")

// ReadRecords is Writer.ReadRecords.
func (r *ReplayReader) ReadRecords(ctx context.Context, topic string, partition int32, from int64, limit int) ([]clients.Record, error) {
	if r == nil || r.w == nil {
		return nil, errNoReplayClient
	}
	return r.w.ReadRecords(ctx, topic, partition, from, limit)
}

// DLQOffsets is Client.DLQOffsets: the start and end offset of every
// partition of each topic; a missing topic is absent, not an error.
func (r *ReplayReader) DLQOffsets(ctx context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	if r == nil || r.w == nil {
		return nil, errNoReplayClient
	}
	return r.w.c.DLQOffsets(ctx, topics...)
}

// PartitionRates is Client.PartitionRates (offsets by time, no record read):
// kafka_replay_range resolves a time range to offsets with it and its Verify
// counts the records written to the reprocess topic since the step (X2b).
func (r *ReplayReader) PartitionRates(ctx context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	if r == nil || r.w == nil {
		return nil, errNoReplayClient
	}
	return r.w.c.PartitionRates(ctx, since, topics...)
}
