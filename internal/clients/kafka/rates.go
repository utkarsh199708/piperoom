package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"piperoom/internal/clients"
)

var _ clients.RateReader = (*Client)(nil)

// PartitionRates counts the records written to each partition of topics since
// the given time, from offsets alone: the first offset at or after since and
// the end offset. No record is fetched, so no key or value is read.
func (c *Client) PartitionRates(ctx context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	if len(topics) == 0 {
		return nil, nil
	}
	ms, err := afterMilli(since)
	if err != nil {
		return nil, err
	}
	adm := c.clusterAdm()
	start := time.Now()
	firsts, err := adm.ListOffsetsAfterMilli(ctx, ms, topics...)
	c.record(ctx, start, "kadm.ListOffsetsAfterMilli(since, topics)", "since="+since.UTC().Format(time.RFC3339), topicsParam(topics))
	if err != nil {
		return nil, fmt.Errorf("kafka: offsets after %s: %w", since.UTC().Format(time.RFC3339), notSupported(err))
	}
	start = time.Now()
	ends, err := adm.ListEndOffsets(ctx, topics...)
	c.record(ctx, start, "kadm.ListEndOffsets(topics)", topicsParam(topics))
	if err != nil {
		return nil, fmt.Errorf("kafka: end offsets: %w", err)
	}
	var out []clients.PartitionRate
	for topic, parts := range ends {
		for p, e := range parts {
			if p < 0 {
				// kadm's marker for a topic-level error: only an unknown topic
				// is absent; any other (authorization, leader election) is a
				// failure the caller must see, not "not found".
				if e.Err != nil && !errors.Is(e.Err, kerr.UnknownTopicOrPartition) {
					return nil, fmt.Errorf("kafka: end offsets %s: %w", topic, e.Err)
				}
				continue
			}
			if e.Err != nil {
				return nil, fmt.Errorf("kafka: end offset %s/%d: %w", topic, p, mapNotFound(e.Err))
			}
			r := clients.PartitionRate{Topic: topic, Partition: p, End: e.Offset, OffsetAtStart: -1}
			f, ok := firsts.Lookup(topic, p)
			if ok && f.Err != nil {
				// A failed lookup is not "no record since": it would read as zero writes.
				return nil, fmt.Errorf("kafka: offset after %s for %s/%d: %w", since.UTC().Format(time.RFC3339), topic, p, notSupported(f.Err))
			}
			if ok && f.Offset >= 0 {
				r.OffsetAtStart = f.Offset
				if d := e.Offset - f.Offset; d > 0 {
					r.Records = d
				}
			}
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, nil
}

// notSupported adds clients.ErrNotSupported to a broker's refusal of a lookup
// by time (a topic in the pre-0.10 message format, which has no timestamps,
// or a broker without the request version), so a caller such as
// kafka_throughput falls back to lag alone instead of failing. The broker
// error stays in the chain.
func notSupported(err error) error {
	if errors.Is(err, kerr.UnsupportedForMessageFormat) || errors.Is(err, kerr.UnsupportedVersion) {
		return fmt.Errorf("%w: %w", clients.ErrNotSupported, err)
	}
	return err
}
