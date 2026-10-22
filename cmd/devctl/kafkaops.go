package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// maxReplayRecords bounds what devctl reads back from a topic in one go. The
// dev DLQ holds tens of records; a topic this large is not a dev scenario.
const maxReplayRecords = 10000

// kafkaClient opens a franz-go client on the dev stack's brokers.
func (s *stack) kafkaClient(opts ...kgo.Opt) (*kgo.Client, error) {
	all := append([]kgo.Opt{kgo.SeedBrokers(strings.Split(s.cfg.KafkaBrokers, ",")...)}, opts...)
	cl, err := kgo.NewClient(all...)
	if err != nil {
		return nil, fmt.Errorf("devctl: kafka client: %w", err)
	}
	return cl, nil
}

// topicSpan is the start and end offset of every partition of a topic.
type topicSpan map[int32][2]int64

// records is how many records the span holds (end - start, summed).
func (t topicSpan) records() int64 {
	var n int64
	for _, se := range t {
		n += se[1] - se[0]
	}
	return n
}

// topicSpan lists the start and end offsets of topic. A topic that does not
// exist is (nil, false, nil): several scenarios create and delete topics, so a
// missing one is a state, not an error.
func (s *stack) topicSpan(ctx context.Context, topic string) (topicSpan, bool, error) {
	cl, err := s.kafkaClient()
	if err != nil {
		return nil, false, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, false, fmt.Errorf("devctl: start offsets of %s: %w", topic, err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, false, fmt.Errorf("devctl: end offsets of %s: %w", topic, err)
	}
	span := topicSpan{}
	missing := false
	collect := func(l kadm.ListedOffsets, i int) error {
		for _, o := range l[topic] {
			if errors.Is(o.Err, kerr.UnknownTopicOrPartition) {
				missing = true
				continue
			}
			if o.Err != nil {
				return fmt.Errorf("devctl: offsets of %s/%d: %w", topic, o.Partition, o.Err)
			}
			se := span[o.Partition]
			se[i] = o.Offset
			span[o.Partition] = se
		}
		return nil
	}
	if err := collect(starts, 0); err != nil {
		return nil, false, err
	}
	if err := collect(ends, 1); err != nil {
		return nil, false, err
	}
	if missing || len(span) == 0 {
		return nil, false, nil
	}
	return span, true, nil
}

// ensureTopic creates topic with the given partitions; an existing topic is
// left as it is.
func (s *stack) ensureTopic(ctx context.Context, topic string, partitions int32) error {
	cl, err := s.kafkaClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, partitions, 1, nil, topic); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("devctl: create topic %s: %w", topic, err)
	}
	return nil
}

// deleteTopics deletes topics; one that does not exist is not an error, so a
// fix that cleans up can run twice.
func (s *stack) deleteTopics(ctx context.Context, topics ...string) error {
	cl, err := s.kafkaClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).DeleteTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("devctl: delete topics %s: %w", strings.Join(topics, ", "), err)
	}
	for _, r := range resp {
		if r.Err != nil && !errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
			return fmt.Errorf("devctl: delete topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}

// readTopic returns every record of topic from the start to the end offsets
// listed when it is called, in offset order per partition (partitions in
// ascending order). A missing or empty topic yields no records.
func (s *stack) readTopic(ctx context.Context, topic string) ([]*kgo.Record, error) {
	return s.readTopicTail(ctx, topic, 0)
}

// readTopicTail is readTopic limited to the last tail records of each
// partition (tail <= 0 reads everything). The size guard applies to what is
// read, so a long-lived topic can still be read through its tail.
func (s *stack) readTopicTail(ctx context.Context, topic string, tail int64) ([]*kgo.Record, error) {
	span, ok, err := s.topicSpan(ctx, topic)
	if err != nil || !ok || span.records() == 0 {
		return nil, err
	}
	from := map[int32]kgo.Offset{}
	last := map[int32]int64{}
	var n int64
	for p, se := range span {
		if se[1] > se[0] {
			start := se[0]
			if tail > 0 {
				start = max(start, se[1]-tail)
			}
			from[p] = kgo.NewOffset().At(start)
			last[p] = se[1] - 1
			n += se[1] - start
		}
	}
	if n > maxReplayRecords {
		return nil, fmt.Errorf("devctl: %s holds %d records, more than the %d devctl reads", topic, n, maxReplayRecords)
	}
	cl, err := s.kafkaClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: from}))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	byPart := map[int32][]*kgo.Record{}
	done := map[int32]bool{}
	for len(done) < len(last) {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil, fmt.Errorf("devctl: read %s: %w", topic, ctx.Err())
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("devctl: read %s: %w", topic, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset > last[r.Partition] || done[r.Partition] {
				return
			}
			byPart[r.Partition] = append(byPart[r.Partition], r)
			if r.Offset == last[r.Partition] {
				done[r.Partition] = true
			}
		})
	}
	var out []*kgo.Record
	for _, p := range slices.Sorted(maps.Keys(byPart)) {
		out = append(out, byPart[p]...)
	}
	return out, nil
}

// produce writes recs in order and waits for every ack. Keyed records land on
// the partition Kafka's default partitioner would pick (murmur2 of the key),
// the same one Debezium's producer chose.
func (s *stack) produce(ctx context.Context, recs []*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	cl, err := s.kafkaClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("devctl: produce to %s: %w", recs[0].Topic, err)
	}
	return nil
}

// errGroupActive: offsets of a group with members cannot be moved; the
// members would overwrite them on their next commit.
var errGroupActive = errors.New("devctl: consumer group has active members")

// resetGroupToStart commits the start offset of every partition of topic for
// group, so the group re-reads the whole topic when it next starts. The group
// must have no members.
func (s *stack) resetGroupToStart(ctx context.Context, group, topic string) error {
	cl, err := s.kafkaClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	described, err := adm.DescribeGroups(ctx, group)
	if err != nil {
		return fmt.Errorf("devctl: describe group %s: %w", group, err)
	}
	if g, ok := described[group]; ok && len(g.Members) > 0 {
		return fmt.Errorf("%w: %s has %d (stop the consumer first)", errGroupActive, group, len(g.Members))
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("devctl: start offsets of %s: %w", topic, err)
	}
	if err := starts.Error(); err != nil {
		return fmt.Errorf("devctl: start offsets of %s: %w", topic, err)
	}
	if err := adm.CommitAllOffsets(ctx, group, starts.Offsets()); err != nil {
		return fmt.Errorf("devctl: commit start offsets of %s for %s: %w", topic, group, err)
	}
	return nil
}
