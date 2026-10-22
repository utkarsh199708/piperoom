package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

// Writer is the Kafka client of the P3 actions (P0 item 7): consumer group
// offset resets, produce, and whole-record reads. It is a type of its own,
// not methods on Client: Client is what read-only tools, the topology
// builder and the watcher hold, and it has no method that writes or returns
// a record value. A Writer is not a KafkaReader either, so it cannot be
// handed to them by mistake. Wiring builds one only with PIPEROOM_WRITE_MODE
// on and gives it to package actions alone.
//
// Pipeline scope (checkWriteScope): the two methods that write,
// ResetGroupOffsets and Produce, fail closed as tools.ScopeFor does, before
// any I/O:
//   - ctx carries the room's whole pipeline (tools.WithPipeline): the group
//     must be one of its consumer_groups (tools.Scope.CheckGroup, the rule of
//     the kafka_consumer_lag tool) and the topic one of its topics
//     (tools.Scope.CheckTopic: exactly "<topic_prefix>.<source_table>", the
//     rule of the kafka_topic_offsets and kafka_find_by_key tools) or its
//     declared reprocess_topic (X2b); a miss wraps tools.ErrOutOfScope;
//   - ctx carries the explicit opt-out (tools.WithUnscoped; the CLI and the
//     integration tests): nothing is checked;
//   - anything else, no scope at all or only a pipeline id
//     (tools.WithPipelineID: the Writer has no owners config to resolve it),
//     wraps tools.ErrNoScope.
//
// The other methods (ReadRecords, CheckGroupInactive, PlanGroupReset,
// PreviousOffsets) change nothing and are not checked here; the action's
// CheckScope scopes the names it passes them.
//
// Every method takes its deadline from ctx, or 10 s when ctx has none.
type Writer struct {
	// c holds the Writer's own connections (never shared with the reader the
	// tools get) and does its offset listings and record reads.
	c *Client

	// groups and produceFn are test seams; nil means c's kadm and kgo clients.
	groups    groupAdmin
	produceFn func(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
}

var (
	_ clients.KafkaWriter       = (*Writer)(nil)
	_ clients.RecordReader      = (*Writer)(nil)
	_ clients.GroupResetPlanner = (*Writer)(nil)
)

const (
	// writeCallTimeout is the deadline of every Writer call whose ctx has none.
	writeCallTimeout = 10 * time.Second
	// maxProduceRecordBytes caps one record: key, value and every header key
	// and value together (recordBytes). Produce refuses the whole call before
	// producing anything when a record is larger.
	maxProduceRecordBytes = 1 << 20
	// produceBatchMaxBytes is kgo's local batch limit. Produce sends one
	// record per batch, so it only has to sit above any record Produce
	// accepts plus its encoding overhead; the topic's max.message.bytes
	// still decides on the broker.
	produceBatchMaxBytes = 2 << 20
	// produceGrace is how long Produce waits, after ctx ended, for kgo to
	// say what became of the record in flight (kgo fails a record on ctx
	// end only when that is safe, and then answers at once).
	produceGrace = time.Second
	// maxReadRecords caps ReadRecords' limit (a replay copies at most 1000).
	maxReadRecords = 1000
	// maxReadBytes bounds what one ReadRecords call holds in memory.
	maxReadBytes = 32 << 20
)

// groupAdmin is the part of *kadm.Client the group offset reset uses; a seam
// so unit tests run without a broker.
type groupAdmin interface {
	DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error)
	DescribeConsumerGroups(ctx context.Context, groups ...string) (kadm.DescribedConsumerGroups, error)
	FetchOffsets(ctx context.Context, group string) (kadm.OffsetResponses, error)
	CommitOffsets(ctx context.Context, group string, os kadm.Offsets) (kadm.OffsetResponses, error)
}

// NewWriter connects a Writer to brokers. connOpts are connection options
// as for New (kgo.DialTLSConfig, kgo.SASL); most callers want
// NewSecureWriter. Records are produced with acks=all and kgo's idempotent
// writes (its default), so kgo's own retries never duplicate a record.
func NewWriter(brokers []string, connOpts ...kgo.Opt) (*Writer, error) {
	c, err := buildClient(brokers, nil, connOpts,
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(produceBatchMaxBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new writer: %w", err)
	}
	return &Writer{c: c}, nil
}

// NewSecureWriter is NewWriter with TLS and/or SASL from sec, as NewSecure.
func NewSecureWriter(brokers []string, sec netsec.Kafka) (*Writer, error) {
	opts, err := sec.Opts()
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	return NewWriter(brokers, opts...)
}

// Close releases the Writer's connections.
func (w *Writer) Close() { w.c.Close() }

func (w *Writer) admin() groupAdmin {
	if w.groups != nil {
		return w.groups
	}
	return w.c.adm
}

func (w *Writer) producer() func(context.Context, *kgo.Record, func(*kgo.Record, error)) {
	if w.produceFn != nil {
		return w.produceFn
	}
	return w.c.cl.Produce
}

// checkWriteScope is the fail-closed scope check of the writes (see Writer).
// check runs only when ctx carries the whole pipeline.
func checkWriteScope(ctx context.Context, check func(tools.Scope) error) error {
	if _, ok := tools.PipelineFrom(ctx); !ok {
		// The opt-out is the only way through without a pipeline. With no
		// owners config, ScopeFor answers a bare pipeline id with an error of
		// its own (the id wins over the opt-out); that is no scope here too.
		sc, err := tools.ScopeFor(ctx, nil)
		switch {
		case err == nil && sc.Unscoped():
			return nil
		case err == nil || errors.Is(err, tools.ErrNoScope):
			return fmt.Errorf("kafka: %w: a write needs the room's pipeline in ctx (tools.WithPipeline) or tools.WithUnscoped", tools.ErrNoScope)
		default:
			return fmt.Errorf("kafka: %w: ctx names the room's pipeline only by id; a write needs the whole pipeline (tools.WithPipeline)", tools.ErrNoScope)
		}
	}
	sc, err := tools.ScopeFor(ctx, nil) // a whole pipeline needs no owners config
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	if err := check(sc); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	return nil
}

// ---- consumer group offsets ----

// CommittedOffset is one partition's committed offset of a consumer group.
type CommittedOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// OffsetMove is one partition of a planned group offset reset: Committed is
// the group's committed offset now (-1 when the partition has none; the reset
// then adds one), and End-Target is the lag the reset leaves. An alias, so
// the Writer satisfies clients.GroupResetPlanner (P0 item 7).
type OffsetMove = clients.OffsetMove

// CheckGroupInactive returns nil when group has no active member and an
// error wrapping clients.ErrGroupActive when it has one. Only a group that is
// Empty (or unknown) with no member is inactive: one that is rebalancing can
// list no member for a moment. Both group protocols are asked: the classic
// DescribeGroups does not see a KIP-848 consumer group (Kafka 4 answers
// GROUP_ID_NOT_FOUND, older brokers "Dead"), so a group it does not know is
// described again with ConsumerGroupDescribe. If that request fails, as it
// does on a broker without KIP-848, the answer is an error, never
// "inactive". A group that neither describe knows has no member.
func (w *Writer) CheckGroupInactive(ctx context.Context, group string) error {
	if strings.TrimSpace(group) == "" {
		return errors.New("kafka: check group: empty group name")
	}
	ctx, cancel := withDefaultTimeout(ctx, writeCallTimeout)
	defer cancel()
	gparam := "group=" + group
	start := time.Now()
	dgs, err := w.admin().DescribeGroups(ctx, group)
	w.c.record(ctx, start, "kadm.DescribeGroups(group)", gparam)
	if err != nil {
		return fmt.Errorf("kafka: describe group %s: %w", group, err)
	}
	dg, ok := dgs[group]
	if !ok {
		return fmt.Errorf("kafka: describe group %s: missing from response", group)
	}
	switch {
	case dg.Err == nil && dg.State != "Dead":
		return groupBusy(group, dg.State, len(dg.Members))
	case dg.Err != nil && !errors.Is(dg.Err, kerr.GroupIDNotFound):
		return fmt.Errorf("kafka: describe group %s: %w", group, dg.Err)
	}
	start = time.Now()
	cgs, err := w.admin().DescribeConsumerGroups(ctx, group)
	w.c.record(ctx, start, "kadm.DescribeConsumerGroups(group)", gparam)
	if err != nil {
		return fmt.Errorf("kafka: describe consumer group %s (not a classic group): %w", group, err)
	}
	cg, ok := cgs[group]
	if !ok {
		return fmt.Errorf("kafka: describe consumer group %s: missing from response", group)
	}
	if errors.Is(cg.Err, kerr.GroupIDNotFound) {
		return nil
	}
	if cg.Err != nil {
		return fmt.Errorf("kafka: describe consumer group %s: %w", group, cg.Err)
	}
	return groupBusy(group, cg.State, len(cg.Members))
}

// groupBusy decides from one describe answer (see CheckGroupInactive).
func groupBusy(group, state string, members int) error {
	if members == 0 && (state == "Empty" || state == "Dead") {
		return nil
	}
	return fmt.Errorf("kafka: group %s is %s with %d members: %w", group, state, members, clients.ErrGroupActive)
}

// PreviousOffsets returns the group's committed offsets, sorted by topic and
// partition: what a reset overwrites. An action's Preview records them as
// the rollback point, because ResetGroupOffsets cannot return them
// (clients.KafkaWriter gives it only a count). Pending transactional offset
// commits are waited for (kadm.RequireStable). A group with no committed
// offset returns an empty slice.
func (w *Writer) PreviousOffsets(ctx context.Context, group string) ([]CommittedOffset, error) {
	ctx, cancel := withDefaultTimeout(ctx, writeCallTimeout)
	defer cancel()
	committed, err := w.committed(ctx, group)
	if err != nil {
		return nil, err
	}
	out := []CommittedOffset{}
	for topic, ps := range committed {
		for p, off := range ps {
			out = append(out, CommittedOffset{Topic: topic, Partition: p, Offset: off})
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

// committed fetches the group's committed offsets per topic and partition.
// A partition the broker lists with offset -1 has no commit and is left out.
func (w *Writer) committed(ctx context.Context, group string) (map[string]map[int32]int64, error) {
	if strings.TrimSpace(group) == "" {
		return nil, errors.New("kafka: committed offsets: empty group name")
	}
	start := time.Now()
	resp, err := w.admin().FetchOffsets(kadm.RequireStable(ctx), group)
	w.c.record(ctx, start, "kadm.FetchOffsets(group)", "group="+group)
	if err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			err = fmt.Errorf("%w: %w", err, clients.ErrNotFound)
		}
		return nil, fmt.Errorf("kafka: committed offsets of %s: %w", group, err)
	}
	out := map[string]map[int32]int64{}
	for _, r := range resp.Sorted() {
		if r.Err != nil {
			return nil, fmt.Errorf("kafka: committed offset of %s on %s/%d: %w", group, r.Topic, r.Partition, r.Err)
		}
		if r.At < 0 {
			continue
		}
		if out[r.Topic] == nil {
			out[r.Topic] = map[int32]int64{}
		}
		out[r.Topic][r.Partition] = r.At
	}
	return out, nil
}

// checkTarget refuses a target that is not exactly one of Earliest and a
// past Timestamp. The action refuses these too (clients.OffsetTarget); the
// client checks again because a future timestamp would move the group to
// the end of every partition, skipping every unread record.
func checkTarget(to clients.OffsetTarget, now time.Time) error {
	switch {
	case to.Earliest && !to.Timestamp.IsZero():
		return errors.New("kafka: offset target sets both earliest and timestamp")
	case !to.Earliest && to.Timestamp.IsZero():
		return errors.New("kafka: offset target sets neither earliest nor timestamp")
	case to.Timestamp.After(now):
		return fmt.Errorf("kafka: offset target %s is in the future", to.Timestamp.UTC().Format(time.RFC3339))
	}
	if !to.Earliest {
		if _, err := afterMilli(to.Timestamp); err != nil {
			return err
		}
	}
	return nil
}

func targetLabel(to clients.OffsetTarget) string {
	if to.Earliest {
		return "earliest"
	}
	return to.Timestamp.UTC().Format(time.RFC3339)
}

// PlanGroupReset returns, changing nothing, what ResetGroupOffsets(group,
// to) would commit now: one OffsetMove per partition of every topic the
// group has a committed offset on (partitions without a commit included),
// sorted by topic and partition. The target is the log start offset
// (Earliest) or the first offset whose record time is at or after
// to.Timestamp; a partition with no such record gets its end offset. A
// committed topic that no longer exists is left out: its stale commit is
// left alone. A group with no committed offset is an error wrapping
// clients.ErrNotFound. It does not check the group's members
// (CheckGroupInactive does).
func (w *Writer) PlanGroupReset(ctx context.Context, group string, to clients.OffsetTarget) ([]OffsetMove, error) {
	if err := checkTarget(to, time.Now()); err != nil {
		return nil, err
	}
	ctx, cancel := withDefaultTimeout(ctx, writeCallTimeout)
	defer cancel()
	committed, err := w.committed(ctx, group)
	if err != nil {
		return nil, err
	}
	if len(committed) == 0 {
		return nil, fmt.Errorf("kafka: group %s has no committed offsets: %w", group, clients.ErrNotFound)
	}
	topics := make([]string, 0, len(committed))
	for t := range committed {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	tparam := "topics=" + strings.Join(topics, ",")

	var targets kadm.ListedOffsets
	start := time.Now()
	if to.Earliest {
		targets, err = w.c.lister().ListStartOffsets(ctx, topics...)
		w.c.record(ctx, start, "kadm.ListStartOffsets(topics)", tparam)
	} else {
		var ms int64
		if ms, err = afterMilli(to.Timestamp); err != nil {
			return nil, err
		}
		targets, err = w.c.lister().ListOffsetsAfterMilli(ctx, ms, topics...)
		w.c.record(ctx, start, "kadm.ListOffsetsAfterMilli(topics, timestamp)", tparam, "timestamp="+targetLabel(to))
	}
	if err != nil {
		return nil, fmt.Errorf("kafka: target offsets of %s: %w", group, err)
	}
	start = time.Now()
	ends, err := w.c.lister().ListEndOffsets(ctx, topics...)
	w.c.record(ctx, start, "kadm.ListEndOffsets(topics)", tparam)
	if err != nil {
		return nil, fmt.Errorf("kafka: end offsets of %s: %w", group, err)
	}
	moves, err := planMoves(committed, targets, ends)
	if err != nil {
		return nil, fmt.Errorf("kafka: plan reset of %s: %w", group, err)
	}
	return moves, nil
}

// planMoves pairs each partition's committed offset with its target and end
// offset. A target the broker could not find (-1) or past the end becomes
// the end offset.
func planMoves(committed map[string]map[int32]int64, targets, ends kadm.ListedOffsets) ([]OffsetMove, error) {
	var out []OffsetMove
	for topic, cps := range committed {
		if topicMissing(targets[topic]) || topicMissing(ends[topic]) {
			continue
		}
		for p, t := range targets[topic] {
			if t.Err != nil {
				return nil, fmt.Errorf("target offset %s/%d: %w", topic, p, t.Err)
			}
			e, ok := ends.Lookup(topic, p)
			if !ok {
				return nil, fmt.Errorf("end offset %s/%d: missing from response", topic, p)
			}
			if e.Err != nil {
				return nil, fmt.Errorf("end offset %s/%d: %w", topic, p, e.Err)
			}
			target := t.Offset
			if target < 0 || target > e.Offset {
				target = e.Offset
			}
			prev, ok := cps[p]
			if !ok {
				prev = -1
			}
			out = append(out, OffsetMove{Topic: topic, Partition: p, Committed: prev, Target: target, End: e.Offset})
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

// ResetGroupOffsets moves every partition of the group's committed topics to
// the target (PlanGroupReset says where) and returns how many partitions'
// committed offset changed; partitions already there are not committed
// again. A group with an active member is refused with an error wrapping
// clients.ErrGroupActive and nothing is changed (CheckGroupInactive). A
// member can still join between that check and the commit; the broker then
// refuses the commit (UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS and the
// like), which is reported as clients.ErrGroupActive too. When some
// partitions commit and others fail, moved counts the committed ones and the
// error names the first failure. When the commit request gets no answer
// (the deadline cut it off, or the connection failed), moved is 0 and the
// error says the outcome is unknown: the commit may have been applied, and
// running the same reset again is safe (it commits the same offsets). The
// previous offsets are not returned: call PreviousOffsets (or
// PlanGroupReset) first to keep them. Before any I/O, a
// group outside the room's pipeline is refused (tools.ErrOutOfScope), and so
// is a ctx with no scope (tools.ErrNoScope); see Writer.
func (w *Writer) ResetGroupOffsets(ctx context.Context, group string, to clients.OffsetTarget) (int, error) {
	if err := checkWriteScope(ctx, func(sc tools.Scope) error { return sc.CheckGroup(group) }); err != nil {
		return 0, err
	}
	if err := checkTarget(to, time.Now()); err != nil {
		return 0, fmt.Errorf("kafka: reset group %s: %w", group, err)
	}
	ctx, cancel := withDefaultTimeout(ctx, writeCallTimeout)
	defer cancel()
	if err := w.CheckGroupInactive(ctx, group); err != nil {
		return 0, err
	}
	plan, err := w.PlanGroupReset(ctx, group, to)
	if err != nil {
		return 0, err
	}
	offs := kadm.Offsets{}
	n := 0
	for _, m := range plan {
		if m.Target == m.Committed {
			continue
		}
		// LeaderEpoch -1: the epoch of the target record is not known, and a
		// wrong one would make the consumer's epoch check misfire.
		offs.Add(kadm.Offset{Topic: m.Topic, Partition: m.Partition, At: m.Target, LeaderEpoch: -1})
		n++
	}
	if n == 0 {
		return 0, nil
	}
	start := time.Now()
	resp, err := w.admin().CommitOffsets(ctx, group, offs)
	w.c.record(ctx, start, "kadm.CommitOffsets(group, offsets)", "group="+group, "partitions="+strconv.Itoa(n))
	if err != nil {
		// kadm returns an error only when no answer came back: the broker
		// may still have applied the commit.
		return 0, fmt.Errorf("kafka: reset group %s: commit of %d partitions: outcome unknown, the commit may have been applied: %w", group, n, commitErr(err))
	}
	moved := 0
	var firstErr error
	for _, r := range resp.Sorted() {
		if r.Err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s/%d: %w", r.Topic, r.Partition, commitErr(r.Err))
			}
			continue
		}
		moved++
	}
	if firstErr != nil {
		return moved, fmt.Errorf("kafka: reset group %s: commit: %d of %d partitions moved: %w", group, moved, n, firstErr)
	}
	slog.InfoContext(ctx, "kafka: group offsets reset", "group", group, "target", targetLabel(to), "moved", moved)
	return moved, nil
}

// commitErr adds clients.ErrGroupActive to the commit answers that mean the
// group gained a member after CheckGroupInactive: an admin commit (no member
// id, generation -1) is accepted only by an empty group.
func commitErr(err error) error {
	for _, busy := range []error{kerr.UnknownMemberID, kerr.RebalanceInProgress, kerr.IllegalGeneration, kerr.StaleMemberEpoch, kerr.FencedMemberEpoch} {
		if errors.Is(err, busy) {
			return fmt.Errorf("%w: %w", err, clients.ErrGroupActive)
		}
	}
	return err
}

// ---- produce ----

// Produce writes recs to topic in order and returns how many were acked
// (clients.KafkaWriter). Records go one at a time: record i+1 is sent only
// after the broker acked record i (acks=all). That costs a round trip per
// record (a replay is at most 1000), but it is what makes acked exact and
// "stops at the first record not acked" true: in one batch, a later record
// on another partition could be written after an earlier one failed, and a
// re-run from acked would duplicate it.
//
// Record.Topic, Partition, Offset and Timestamp are ignored: the topic
// argument wins, kgo's default partitioner places a keyed record by the
// murmur2 hash of its key as Kafka's default partitioner does, and the
// record gets the produce time. Key (nil stays a null key) and headers are
// kept. A record over 1 MiB (key, value and headers together) fails the
// whole call before anything is produced, as does a topic that does not
// exist (clients.ErrNotFound; kgo
// never asks the broker to create one). When ctx ends while a record is in
// flight and kgo cannot tell within a second whether it was written, the
// error says its outcome is unknown: it is not counted, and it may still
// appear in the topic. Before any I/O, a topic outside the room's pipeline
// is refused (tools.ErrOutOfScope), and so is a ctx with no scope
// (tools.ErrNoScope); see Writer. The pipeline's topics here are its CDC
// topics plus its declared reprocess_topic (owners.yaml, X2b).
func (w *Writer) Produce(ctx context.Context, topic string, recs []clients.Record) (int, error) {
	if err := checkProduce(ctx, topic, recs); err != nil {
		return 0, err
	}
	if len(recs) == 0 {
		return 0, nil
	}
	ctx, cancel := withDefaultTimeout(ctx, writeCallTimeout)
	defer cancel()
	if err := w.topicExists(ctx, topic); err != nil {
		return 0, fmt.Errorf("kafka: produce %s: %w", topic, err)
	}
	acked := 0
	start := time.Now()
	defer func() {
		w.c.record(ctx, start, "kgo.Produce(topic, records)", "topic="+topic,
			"records="+strconv.Itoa(len(recs)), "acked="+strconv.Itoa(acked))
	}()
	for i := range recs {
		if err := w.produceOne(ctx, toKgoRecord(topic, recs[i])); err != nil {
			return acked, fmt.Errorf("kafka: produce %s: record %d of %d not acked, %d acked: %w", topic, i, len(recs), acked, err)
		}
		acked++
	}
	slog.InfoContext(ctx, "kafka: records produced", "topic", topic, "acked", acked)
	return acked, nil
}

// checkProduce is the part of Produce and ProduceAll that needs no I/O: a
// topic in the room's pipeline (a CDC topic or the declared reprocess_topic)
// and no record over the size cap.
func checkProduce(ctx context.Context, topic string, recs []clients.Record) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("kafka: produce: empty topic")
	}
	if err := checkWriteScope(ctx, func(sc tools.Scope) error {
		err := sc.CheckTopic(topic)
		if err != nil && topic == sc.Pipeline().ReprocessTopic {
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	for i := range recs {
		if n := recordBytes(recs[i]); n > maxProduceRecordBytes {
			return fmt.Errorf("kafka: produce %s: record %d is %d bytes (key, value and headers), over the %d byte cap; nothing was produced", topic, i, n, maxProduceRecordBytes)
		}
	}
	return nil
}

// ProduceAll writes recs to topic with every record in flight at once and
// returns how many the broker acked. It is Produce for a bulk copy
// (kafka_replay_range, up to 100 000 records in one 30 s step, where a round
// trip per record is too slow), with the same checks, scope and record
// handling. Order holds per target partition: the producer is idempotent,
// writes a partition's records in the order given, and fails every record
// buffered behind a failed one on that partition. Across partitions it does
// not, so after an error acked is an exact count but not a prefix of recs:
// the copy is not resumable from acked, and a re-run copies again. When ctx
// ends with records in flight and kgo cannot tell within a second what
// became of them, they are not counted and the error says how many may
// still be written.
func (w *Writer) ProduceAll(ctx context.Context, topic string, recs []clients.Record) (int, error) {
	if err := checkProduce(ctx, topic, recs); err != nil {
		return 0, err
	}
	if len(recs) == 0 {
		return 0, nil
	}
	ctx, cancel := withDefaultTimeout(ctx, writeCallTimeout)
	defer cancel()
	if err := w.topicExists(ctx, topic); err != nil {
		return 0, fmt.Errorf("kafka: produce %s: %w", topic, err)
	}
	acked := 0
	start := time.Now()
	defer func() {
		w.c.record(ctx, start, "kgo.Produce(topic, records) pipelined", "topic="+topic,
			"records="+strconv.Itoa(len(recs)), "acked="+strconv.Itoa(acked))
	}()
	// Buffered for every record: kgo calls each promise once, also after
	// this returned, and a promise must never block.
	done := make(chan error, len(recs))
	sent := 0
	for i := range recs {
		if ctx.Err() != nil {
			break
		}
		w.producer()(ctx, toKgoRecord(topic, recs[i]), func(_ *kgo.Record, err error) { done <- err })
		sent++
	}
	var firstErr error
	var grace <-chan time.Time // set once ctx ended
	ctxDone := ctx.Done()
	for got := 0; got < sent; {
		select {
		case err := <-done:
			got++
			if err == nil {
				acked++
			} else if firstErr == nil {
				firstErr = err
			}
		case <-ctxDone:
			ctxDone = nil
			t := time.NewTimer(produceGrace)
			defer t.Stop()
			grace = t.C
		case <-grace:
			return acked, fmt.Errorf("kafka: produce %s: %d of %d records acked, %d not sent, %d with an unknown outcome that may still be written: %w",
				topic, acked, len(recs), len(recs)-sent, sent-got, ctx.Err())
		}
	}
	if firstErr == nil && sent < len(recs) {
		firstErr = ctx.Err()
	}
	if firstErr != nil {
		return acked, fmt.Errorf("kafka: produce %s: %d of %d records acked, %d not sent: %w", topic, acked, len(recs), len(recs)-sent, firstErr)
	}
	slog.InfoContext(ctx, "kafka: records produced", "topic", topic, "acked", acked)
	return acked, nil
}

// produceOne produces r and waits for its ack.
func (w *Writer) produceOne(ctx context.Context, r *kgo.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Buffered: kgo calls the promise once, also after this returned.
	done := make(chan error, 1)
	w.producer()(ctx, r, func(_ *kgo.Record, err error) { done <- err })
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	t := time.NewTimer(produceGrace)
	defer t.Stop()
	select {
	case err := <-done:
		return err
	case <-t.C:
		return fmt.Errorf("outcome unknown, the record may still be written: %w", ctx.Err())
	}
}

// topicExists returns an error wrapping clients.ErrNotFound when the broker
// does not know topic.
func (w *Writer) topicExists(ctx context.Context, topic string) error {
	start := time.Now()
	tds, err := w.c.metadata().ListTopics(ctx, topic)
	w.c.record(ctx, start, "kadm.ListTopics(topic)", "topic="+topic)
	if err != nil {
		return fmt.Errorf("topic metadata: %w", err)
	}
	td, ok := tds[topic]
	if !ok {
		return fmt.Errorf("topic %s: %w", topic, clients.ErrNotFound)
	}
	if td.Err != nil {
		return fmt.Errorf("topic %s: %w", topic, mapNotFound(td.Err))
	}
	return nil
}

// recordBytes is what a record weighs against maxProduceRecordBytes.
func recordBytes(r clients.Record) int {
	n := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return n
}

// toKgoRecord builds the record to produce. Timestamp stays zero so kgo
// stamps the produce time.
func toKgoRecord(topic string, r clients.Record) *kgo.Record {
	kr := &kgo.Record{Topic: topic, Key: r.Key, Value: r.Value}
	if len(r.Headers) > 0 {
		kr.Headers = make([]kgo.RecordHeader, len(r.Headers))
		for i, h := range r.Headers {
			kr.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
		}
	}
	return kr
}

// ---- whole-record reads ----

// ReadRecords reads up to limit whole records (key, value, headers) of one
// partition from offset from, in offset order (clients.RecordReader). limit
// <= 0 is an error; it is capped at 1000. Only committed records are
// returned: records of aborted or open transactions and transaction markers
// are skipped, and like compaction they leave gaps in the offsets. It
// returns fewer records when the partition's readable end comes first, or
// once 32 MiB of record bytes were read (the caller reads on from the last
// offset + 1). from at or past the end returns no records; from below the
// log start is an error wrapping clients.ErrNotFound (those records were
// deleted, and a replay must not skip them silently; neither does the read,
// which never resets past a gap). When the deadline cuts the read short,
// the records read so far, all of them from from on with none skipped, come
// back with the error.
//
// This is the only read of the package that returns record values, so it
// lives on Writer, which only package actions may hold.
func (w *Writer) ReadRecords(ctx context.Context, topic string, partition int32, from int64, limit int) ([]clients.Record, error) {
	switch {
	case strings.TrimSpace(topic) == "":
		return nil, errors.New("kafka: read records: empty topic")
	case partition < 0:
		return nil, fmt.Errorf("kafka: read records %s: negative partition %d", topic, partition)
	case from < 0:
		return nil, fmt.Errorf("kafka: read records %s/%d: negative offset %d", topic, partition, from)
	case limit <= 0:
		return nil, fmt.Errorf("kafka: read records %s/%d: limit %d, want at least 1", topic, partition, limit)
	}
	limit = min(limit, maxReadRecords)
	ctx, cancel := withDefaultTimeout(ctx, writeCallTimeout)
	defer cancel()

	po, err := w.partitionBounds(ctx, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("kafka: read records %s/%d: %w", topic, partition, err)
	}
	if from < po.Start {
		return nil, fmt.Errorf("kafka: read records %s/%d: offset %d is below the log start %d (deleted): %w", topic, partition, from, po.Start, clients.ErrNotFound)
	}
	stable, err := w.c.clampToStable(ctx, topic, []clients.PartitionOffsets{po})
	if err != nil {
		return nil, fmt.Errorf("kafka: read records %s/%d: %w", topic, partition, err)
	}
	end := stable[0].End
	if from >= end {
		return []clients.Record{}, nil
	}

	cons, err := w.c.newConsumer(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().At(from)}},
		kgo.ConsumeResetOffset(kgo.NoResetOffset()))
	if err != nil {
		return nil, fmt.Errorf("kafka: read records %s/%d: consumer: %w", topic, partition, err)
	}
	defer cons.Close()
	pr := w.c.newProber(topic)
	pollStart := time.Now()
	recs, err := collectRecords(ctx, cons, topic, partition, readWindow{from: from, end: end}, limit,
		func(wins map[int32]readWindow) { pr.stalled(ctx, wins) })
	// Neither key nor value goes in the receipt.
	w.c.record(ctx, pollStart, "kgo.PollFetches(topic, partition, from, limit)", "topic="+topic,
		"partition="+strconv.Itoa(int(partition)), "from="+strconv.FormatInt(from, 10), "limit="+strconv.Itoa(limit))
	pr.record(ctx)
	if err != nil {
		return recs, fmt.Errorf("kafka: read records %s/%d from %d: %w", topic, partition, from, err)
	}
	return recs, nil
}

// partitionBounds lists one partition's log start and end offset.
func (w *Writer) partitionBounds(ctx context.Context, topic string, partition int32) (clients.PartitionOffsets, error) {
	tparam := "topic=" + topic
	start := time.Now()
	starts, err := w.c.lister().ListStartOffsets(ctx, topic)
	w.c.record(ctx, start, "kadm.ListStartOffsets(topic)", tparam)
	if err != nil {
		return clients.PartitionOffsets{}, fmt.Errorf("start offsets: %w", err)
	}
	if topicMissing(starts[topic]) {
		return clients.PartitionOffsets{}, fmt.Errorf("topic %s: %w", topic, clients.ErrNotFound)
	}
	s, ok := starts.Lookup(topic, partition)
	if !ok {
		return clients.PartitionOffsets{}, fmt.Errorf("partition %d: %w", partition, clients.ErrNotFound)
	}
	if s.Err != nil {
		return clients.PartitionOffsets{}, fmt.Errorf("start offset: %w", s.Err)
	}
	start = time.Now()
	ends, err := w.c.lister().ListEndOffsets(ctx, topic)
	w.c.record(ctx, start, "kadm.ListEndOffsets(topic)", tparam)
	if err != nil {
		return clients.PartitionOffsets{}, fmt.Errorf("end offsets: %w", err)
	}
	e, ok := ends.Lookup(topic, partition)
	if !ok {
		return clients.PartitionOffsets{}, errors.New("end offset: missing from response")
	}
	if e.Err != nil {
		return clients.PartitionOffsets{}, fmt.Errorf("end offset: %w", e.Err)
	}
	return clients.PartitionOffsets{Partition: partition, Start: s.Offset, End: e.Offset}, nil
}

// collectRecords polls one partition's window [from, end) until limit
// records or maxReadBytes were read, the window is done, or ctx ends.
// Markers move the window (and end it at its last offset) but are never
// returned; probe handles a window stuck behind an aborted tail (#300). Any
// fetch error, data loss included, ends the read: a replay must not skip.
func collectRecords(ctx context.Context, cons pollConsumer, topic string, partition int32, win readWindow, limit int, probe func(map[int32]readWindow)) ([]clients.Record, error) {
	out := []clients.Record{}
	total := 0
	wait := map[int32]readWindow{partition: win}
	for {
		fetches := pollBounded(ctx, cons)
		if fetches.IsClientClosed() {
			return out, errors.New("consumer closed")
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				return out, fmt.Errorf("read incomplete after %d records: %w", len(out), fe.Err)
			}
			return out, fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
		for _, r := range fetches.Records() {
			cur := wait[partition]
			if r.Partition != partition || r.Offset < cur.from {
				continue
			}
			if r.Offset >= win.end {
				return out, nil // an abort marker past the planned end
			}
			if !r.Attrs.IsControl() {
				out = append(out, recordFrom(topic, r))
				total += recordSize(r)
			}
			if r.Offset+1 >= win.end || len(out) >= limit || total >= maxReadBytes {
				return out, nil
			}
			wait[partition] = readWindow{from: r.Offset + 1, end: win.end}
		}
		probe(wait)
		if cur := wait[partition]; cur.from >= cur.end {
			return out, nil
		}
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("read incomplete after %d records: %w", len(out), err)
		}
	}
}

// recordFrom copies a fetched record into the DTO. Copies, so the result
// does not pin kgo's fetch buffers; bytes.Clone keeps a nil key nil.
func recordFrom(topic string, r *kgo.Record) clients.Record {
	out := clients.Record{
		Topic:     topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
		Key:       bytes.Clone(r.Key),
		Value:     bytes.Clone(r.Value),
	}
	if len(r.Headers) > 0 {
		out.Headers = make([]clients.RecordHeader, len(r.Headers))
		for i, h := range r.Headers {
			out.Headers[i] = clients.RecordHeader{Key: h.Key, Value: bytes.Clone(h.Value)}
		}
	}
	return out
}
