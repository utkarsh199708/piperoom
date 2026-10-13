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
//
// kadm caches topic metadata (MetadataMinAge, 5 s), so a topic created a
// moment ago may still look absent for up to that long. One topic the
// principal may not describe (TopicAuthorizationFailed) fails the whole call,
// not just that topic; callers that pass several topics should know that.
// end-start is an upper bound on the records present: compaction gaps and
// transaction markers occupy offsets without being records.
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
//
// It reads up to min(limit, available) records from the tail of every
// non-empty partition, merges them and cuts to limit; the 8 MiB bound and the
// 10 s deadline bound the cost. Within a partition records are ordered by
// offset, newest first; across partitions by record timestamp, newest first
// (ties: lower partition, then higher offset). A Connect DLQ record carries
// the ORIGINAL record's timestamp (DeadLetterQueueReporter), so "newest"
// means newest source event, not newest failure.
//
// A topic that does not exist is an error wrapping clients.ErrNotFound
// (DLQOffsets, which the watcher uses, treats absence as normal; a peek of a
// topic the caller named is worth reporting). A topic with no records returns
// an empty slice and no error.
//
// Only __connect.errors.* headers are returned; neither key nor value ever
// leaves this function. The fetch does bring record bytes into the process
// for a moment (Kafka has no headers-only fetch). Known limits: the read
// stops at 8 MiB of record bytes and returns what it has, with no flag; if
// the deadline cuts the read short and at least one record arrived, those
// records are returned (a sample), with none it is an error. Only committed
// records are read, and transaction markers count toward "partition done"
// but are never returned.
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
	return mergeNewest(recs, limit), nil
}

// mergeNewest orders records per partition by offset descending, then merges
// the partitions by timestamp descending (ties: lower partition, then higher
// offset) and cuts to limit.
func mergeNewest(recs []clients.DLQRecord, limit int) []clients.DLQRecord {
	byPart := map[int32][]clients.DLQRecord{}
	for _, r := range recs {
		byPart[r.Partition] = append(byPart[r.Partition], r)
	}
	lists := make([][]clients.DLQRecord, 0, len(byPart))
	for _, l := range byPart {
		sort.Slice(l, func(i, j int) bool { return l[i].Offset > l[j].Offset })
		lists = append(lists, l)
	}
	out := make([]clients.DLQRecord, 0, limit)
	for len(out) < limit {
		best := -1
		for i, l := range lists {
			if len(l) == 0 {
				continue
			}
			if best < 0 || newer(l[0], lists[best][0]) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		out = append(out, lists[best][0])
		lists[best] = lists[best][1:]
	}
	return out
}

func newer(a, b clients.DLQRecord) bool {
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.After(b.Timestamp)
	}
	if a.Partition != b.Partition {
		return a.Partition < b.Partition
	}
	return a.Offset > b.Offset
}

// peekPart is one partition's share of a peek: read [from, end).
type peekPart struct {
	partition int32
	from, end int64
}

// planPeek reads min(limit, end-start) records from the tail of every
// non-empty partition. A partition's end offset says how much was written,
// not how recently, so no partition is skipped on that basis. end-start is an
// upper bound: control markers and compaction gaps take offsets too.
func planPeek(parts []clients.PartitionOffsets, limit int) []peekPart {
	var plan []peekPart
	for _, p := range parts {
		avail := p.End - p.Start
		if avail <= 0 {
			continue
		}
		n := avail
		if n > int64(limit) {
			n = int64(limit)
		}
		plan = append(plan, peekPart{partition: p.Partition, from: p.End - n, end: p.End})
	}
	sort.Slice(plan, func(i, j int) bool { return plan[i].partition < plan[j].partition })
	return plan
}

// collectPeek polls until every planned partition reached its end offset,
// 8 MiB of record bytes were read, or ctx ends. Control records (transaction
// markers) are delivered because KeepControlRecords is on: they mark a
// partition done when they sit at its last offset, but are never returned or
// counted. Only DLQRecords come out: each *kgo.Record is dropped after its
// iteration.
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
			var loss *kgo.ErrDataLoss
			if errors.As(fe.Err, &loss) {
				continue // informational: kgo resets and goes on
			}
			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				if len(out) > 0 {
					return out, nil // a sample is better than nothing
				}
				return nil, fmt.Errorf("read incomplete after 0 records: %w", fe.Err)
			}
			return nil, fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
		for _, r := range fetches.Records() {
			end, planned := ends[r.Partition]
			if !planned || r.Offset >= end {
				continue
			}
			if r.Offset+1 >= end {
				done[r.Partition] = true
			}
			if r.Attrs.IsControl() {
				continue
			}
			total += recordSize(r)
			out = append(out, dlqRecordFrom(topic, r))
			if total >= maxPeekBytes {
				return out, nil
			}
		}
		if err := ctx.Err(); err != nil && len(done) < len(ends) {
			if len(out) > 0 {
				return out, nil
			}
			return nil, fmt.Errorf("read incomplete after 0 records: %w", err)
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
		kgo.KeepControlRecords(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
}
