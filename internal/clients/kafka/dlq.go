package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/dlq"
)

var _ clients.DLQReader = (*Client)(nil)

const (
	// connectErrorsPrefix is the only header namespace PeekDLQ returns.
	connectErrorsPrefix = "__connect.errors."
	headerStacktrace    = connectErrorsPrefix + "exception.stacktrace"
	headerMessage       = connectErrorsPrefix + "exception.message"

	defaultPeekLimit = 5
	maxPeekLimit     = 20
	// maxPeekBytes bounds what one PeekDLQ call reads. KIP-74 lets a broker
	// return a record larger than the fetch limits, so a running total, not
	// the fetch options, is what bounds memory.
	maxPeekBytes = 8 << 20
	// dlqCallTimeout is the deadline of every DLQ request when the caller's
	// context has none (AGENTS.md rule 4).
	dlqCallTimeout = 10 * time.Second
)

// offsetLister is the part of *kadm.Client that DLQOffsets uses; a seam so
// unit tests can count requests without a broker.
type offsetLister interface {
	ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
}

// dlqConsumer is the part of *kgo.Client that PeekDLQ uses.
type dlqConsumer interface {
	PollFetches(ctx context.Context) kgo.Fetches
	Close()
}

// DLQOffsets returns start and end offsets of every partition of each topic
// with two requests for all topics, never fetching a record, so
// LastTimestamp is zero. A topic that does not exist is absent from the
// result (not an error). With no topics it returns nil, nil and sends
// nothing: kadm lists the whole cluster for an empty list.
func (c *Client) DLQOffsets(ctx context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	topics = uniqueNonEmpty(topics)
	if len(topics) == 0 {
		return nil, nil
	}
	ctx, cancel := withDefaultTimeout(ctx, dlqCallTimeout)
	defer cancel()

	lister := c.lister()
	starts, err := lister.ListStartOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: dlq start offsets: %w", err)
	}
	ends, err := lister.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: dlq end offsets: %w", err)
	}

	out := make(map[string][]clients.PartitionOffsets, len(topics))
	for _, topic := range topics {
		sparts := starts[topic]
		if topicMissing(sparts) || topicMissing(ends[topic]) {
			continue
		}
		var parts []clients.PartitionOffsets
		for p, s := range sparts {
			if s.Err != nil {
				return nil, fmt.Errorf("kafka: dlq start offset %s/%d: %w", topic, p, s.Err)
			}
			e, ok := ends.Lookup(topic, p)
			if !ok {
				return nil, fmt.Errorf("kafka: dlq end offset %s/%d: missing from response", topic, p)
			}
			if e.Err != nil {
				return nil, fmt.Errorf("kafka: dlq end offset %s/%d: %w", topic, p, e.Err)
			}
			parts = append(parts, clients.PartitionOffsets{Partition: p, Start: s.Offset, End: e.Offset})
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i].Partition < parts[j].Partition })
		out[topic] = parts
	}
	return out, nil
}

// PeekDLQ returns the newest records of topic as DLQRecord, newest first, at
// most limit in total across partitions (limit <= 0 means 5, capped at 20).
// A topic that does not exist is an error wrapping clients.ErrNotFound
// (DLQOffsets, which the watcher uses, treats absence as normal; a peek of a
// topic the caller named is worth reporting). A topic with no records returns
// an empty slice and no error.
//
// Only __connect.errors.* headers are returned; neither key nor value ever
// leaves this function. The fetch does bring record bytes into the process
// for a moment (Kafka has no headers-only fetch); the read stops at 8 MiB
// of record bytes and returns what it has. If the deadline cuts the read
// short the result is an error, never a silently smaller sample.
func (c *Client) PeekDLQ(ctx context.Context, topic string, limit int) ([]clients.DLQRecord, error) {
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("kafka: peek dlq: empty topic")
	}
	limit = normalizePeekLimit(limit)
	ctx, cancel := withDefaultTimeout(ctx, dlqCallTimeout)
	defer cancel()

	offs, err := c.DLQOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: peek dlq %s: %w", topic, err)
	}
	parts, ok := offs[topic]
	if !ok {
		return nil, fmt.Errorf("kafka: peek dlq %s: %w", topic, clients.ErrNotFound)
	}
	plan := planPeek(parts, limit)
	if len(plan) == 0 {
		return []clients.DLQRecord{}, nil
	}

	startAt := map[int32]kgo.Offset{}
	for _, pl := range plan {
		startAt[pl.partition] = kgo.NewOffset().At(pl.from)
	}
	cons, err := c.newDLQConsumer(map[string]map[int32]kgo.Offset{topic: startAt})
	if err != nil {
		return nil, fmt.Errorf("kafka: peek dlq %s: consumer: %w", topic, err)
	}
	defer cons.Close()

	recs, err := collectPeek(ctx, cons, topic, plan)
	if err != nil {
		return nil, fmt.Errorf("kafka: peek dlq %s: %w", topic, err)
	}
	sort.SliceStable(recs, func(i, j int) bool {
		a, b := recs[i], recs[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.After(b.Timestamp)
		}
		if a.Partition != b.Partition {
			return a.Partition < b.Partition
		}
		return a.Offset > b.Offset
	})
	if len(recs) > limit {
		recs = recs[:limit]
	}
	return recs, nil
}

// peekPart is one partition's share of a peek: read [from, end).
type peekPart struct {
	partition int32
	from, end int64
}

// planPeek spreads limit records over the partitions, highest end offset
// first (it most likely holds the newest record; timestamps are compared
// after the fetch). The shares add up to at most limit, so a 64-partition
// topic with limit 20 reads 20 records, not 20 per partition.
func planPeek(parts []clients.PartitionOffsets, limit int) []peekPart {
	sorted := append([]clients.PartitionOffsets(nil), parts...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].End != sorted[j].End {
			return sorted[i].End > sorted[j].End
		}
		return sorted[i].Partition < sorted[j].Partition
	})
	var plan []peekPart
	left := int64(limit)
	for _, p := range sorted {
		if left <= 0 {
			break
		}
		avail := p.End - p.Start
		if avail <= 0 {
			continue
		}
		n := avail
		if n > left {
			n = left
		}
		plan = append(plan, peekPart{partition: p.Partition, from: p.End - n, end: p.End})
		left -= n
	}
	return plan
}

// collectPeek polls until every planned partition reached its end offset,
// 8 MiB of record bytes were read, or ctx ends (an error). Only DLQRecords
// come out: each *kgo.Record is dropped after its iteration.
func collectPeek(ctx context.Context, cons dlqConsumer, topic string, plan []peekPart) ([]clients.DLQRecord, error) {
	ends := map[int32]int64{}
	for _, pl := range plan {
		ends[pl.partition] = pl.end
	}
	done := map[int32]bool{}
	var (
		out   []clients.DLQRecord
		total int
	)
	for len(done) < len(ends) {
		fetches := cons.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil, errors.New("consumer closed")
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("read incomplete after %d records: %w", len(out), fe.Err)
			}
			return nil, fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
		for _, r := range fetches.Records() {
			end, planned := ends[r.Partition]
			if !planned || r.Offset >= end {
				continue
			}
			total += recordSize(r)
			out = append(out, dlqRecordFrom(topic, r))
			if r.Offset+1 >= end {
				done[r.Partition] = true
			}
			if total >= maxPeekBytes {
				return out, nil
			}
		}
		if err := ctx.Err(); err != nil && len(done) < len(ends) {
			return nil, fmt.Errorf("read incomplete after %d records: %w", len(out), err)
		}
	}
	return out, nil
}

// recordSize is what a record weighs against the 8 MiB bound.
func recordSize(r *kgo.Record) int {
	n := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return n
}

// dlqRecordFrom builds the DTO. Key and Value are not read: the DTO has no
// field for them.
func dlqRecordFrom(topic string, r *kgo.Record) clients.DLQRecord {
	return clients.DLQRecord{
		Topic:     topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
		Headers:   connectErrorHeaders(r.Headers),
	}
}

// connectErrorHeaders keeps only __connect.errors.* headers. Header values
// are written by whoever produced the record, so they are untrusted text:
// control characters are removed, the value is capped, the stack trace is cut
// to its first line, and message and trace line are redacted.
func connectErrorHeaders(hs []kgo.RecordHeader) map[string]string {
	var out map[string]string
	for _, h := range hs {
		if !strings.HasPrefix(h.Key, connectErrorsPrefix) {
			continue
		}
		v := string(h.Value)
		switch h.Key {
		case headerStacktrace:
			v = dlq.RedactMessage(cleanHeader(firstNonEmptyLine(v)))
		case headerMessage:
			v = dlq.RedactMessage(cleanHeader(v))
		default:
			v = cleanHeader(v)
		}
		if out == nil {
			out = map[string]string{}
		}
		out[h.Key] = v
	}
	return out
}

func firstNonEmptyLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			return line
		}
	}
	return ""
}

// cleanHeader removes control characters, trims space and caps at
// dlq.MaxMessageRunes runes.
func cleanHeader(s string) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, strings.ToValidUTF8(s, ""))
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > dlq.MaxMessageRunes {
		s = string(r[:dlq.MaxMessageRunes])
	}
	return s
}

func normalizePeekLimit(limit int) int {
	switch {
	case limit <= 0:
		return defaultPeekLimit
	case limit > maxPeekLimit:
		return maxPeekLimit
	}
	return limit
}

// topicMissing reports kadm's marker for a topic the broker does not know: a
// lone partition -1 carrying UnknownTopicOrPartition (or no entry at all).
// Any other -1 error (e.g. authorization) is not "missing"; it surfaces as
// the partition error.
func topicMissing(parts map[int32]kadm.ListedOffset) bool {
	if len(parts) == 0 {
		return true
	}
	m, ok := parts[-1]
	return ok && errors.Is(m.Err, kerr.UnknownTopicOrPartition)
}

func uniqueNonEmpty(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func withDefaultTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

func (c *Client) lister() offsetLister {
	if c.offs != nil {
		return c.offs
	}
	return c.adm
}

// newDLQConsumer opens a short-lived consumer on exactly the given
// partitions with bounded fetch sizes; the caller closes it.
func (c *Client) newDLQConsumer(parts map[string]map[int32]kgo.Offset) (dlqConsumer, error) {
	if c.consumerFn != nil {
		return c.consumerFn(parts)
	}
	return c.newKgo(
		kgo.ConsumePartitions(parts),
		kgo.FetchMaxBytes(4<<20),
		kgo.FetchMaxPartitionBytes(1<<20),
	)
}
