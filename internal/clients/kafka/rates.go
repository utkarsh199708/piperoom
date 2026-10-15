package kafka

import (
	"context"
	"fmt"
	"sort"
	"time"

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
		return nil, fmt.Errorf("kafka: offsets after %s: %w", since.UTC().Format(time.RFC3339), err)
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
			if p < 0 { // kadm's marker for an unknown topic
				continue
			}
			if e.Err != nil {
				return nil, fmt.Errorf("kafka: end offset %s/%d: %w", topic, p, mapNotFound(e.Err))
			}
			r := clients.PartitionRate{Topic: topic, Partition: p, End: e.Offset, OffsetAtStart: -1}
			if f, ok := firsts.Lookup(topic, p); ok && f.Err == nil && f.Offset >= 0 {
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
