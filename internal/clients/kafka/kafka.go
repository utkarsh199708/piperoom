// Package kafka implements clients.KafkaAdmin against a Kafka cluster using
// franz-go (kgo for consuming, kadm for admin reads).
package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/tools"
)

// Client is a Kafka admin + small-reads client.
type Client struct {
	brokers []string
	// connOpts (TLS, SASL) go to every kgo client this Client creates,
	// including the short-lived consumers in FindRecord and TopicOffsets.
	connOpts []kgo.Opt
	adm      *kadm.Client
	cl       *kgo.Client
	sr       *schemareg.Client

	// offs, meta, clusterFn, consumerFn and probeFn are test seams; nil means
	// the real kadm/kgo clients. consumerFn gets the options newConsumer
	// builds with; probeFn stands in for one probe round (prober.round).
	offs       offsetLister
	meta       metaReader
	clusterFn  clusterAdmin
	consumerFn func(opts []kgo.Opt) (pollConsumer, error)
	probeFn    func(ctx context.Context, topic string, wins map[int32]readWindow) map[int32]int64

	srMu    sync.Mutex
	srCache map[int]avro.Schema
	// keyRefused caches, per schema id, why the schema is refused for key
	// decoding ("" = fine); the walk runs once per id.
	keyRefused map[int]string
	// keyAPI decodes untrusted keys with size caps; built in New, lazily for
	// zero-value test clients.
	keyAPI avro.API
	// decodeHook runs just before the Avro decode; a test seam, nil in production.
	decodeHook func()
}

// maxKeyBytes bounds a record key before any parsing: keys are untrusted and a
// legitimate Confluent-framed key is far smaller.
const maxKeyBytes = 64 << 10

// newKeyAPI returns the frozen, bounded Avro config for untrusted keys. The
// package-level avro decoders use DefaultConfig, which has no caps.
func newKeyAPI() avro.API {
	return avro.Config{MaxByteSliceSize: 64 << 10, MaxSliceAllocSize: 1 << 20}.Freeze()
}

var _ clients.KafkaAdmin = (*Client)(nil)

// metaReader is the part of *kadm.Client that Topics, ConsumerGroups,
// GroupLag and DescribeTopicConfigs use; a seam so unit tests run without a
// broker.
type metaReader interface {
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
	ListGroups(ctx context.Context, filterStates ...string) (kadm.ListedGroups, error)
	Lag(ctx context.Context, groups ...string) (kadm.DescribedGroupLags, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
}

func (c *Client) metadata() metaReader {
	if c.meta != nil {
		return c.meta
	}
	return c.adm
}

// New connects to brokers (e.g. localhost:29092). sr is used to decode Avro keys.
// connOpts are connection options such as kgo.DialTLSConfig and kgo.SASL; most
// callers want NewSecure instead.
func New(brokers []string, sr *schemareg.Client, connOpts ...kgo.Opt) (*Client, error) {
	c := &Client{brokers: brokers, connOpts: append([]kgo.Opt(nil), connOpts...), sr: sr, srCache: map[int]avro.Schema{}, keyRefused: map[int]string{}, keyAPI: newKeyAPI()}
	cl, err := c.newKgo()
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	c.cl, c.adm = cl, kadm.NewClient(cl)
	return c, nil
}

// NewSecure connects to brokers of a real cluster with TLS and/or SASL
// (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) from sec. The zero netsec.Kafka is
// PLAINTEXT, the same as New.
func NewSecure(brokers []string, sr *schemareg.Client, sec netsec.Kafka) (*Client, error) {
	opts, err := sec.Opts()
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	return New(brokers, sr, opts...)
}

// newKgo builds a kgo client for this cluster with the connection options
// plus extra (e.g. which partitions to consume).
func (c *Client) newKgo(extra ...kgo.Opt) (*kgo.Client, error) {
	opts := make([]kgo.Opt, 0, 1+len(c.connOpts)+len(extra))
	opts = append(opts, kgo.SeedBrokers(c.brokers...))
	opts = append(opts, c.connOpts...)
	opts = append(opts, extra...)
	return kgo.NewClient(opts...)
}

// record adds one provenance call for a kadm/kgo request that started at
// start. text is the literal method with argument names; params carry the
// argument values the tool already shows. A record key or value never goes
// in either.
func (c *Client) record(ctx context.Context, start time.Time, text string, params ...string) {
	tools.Record(ctx, tools.Call{
		Kind:           tools.CallKafka,
		Target:         "kafka " + strings.Join(c.brokers, ","),
		Text:           text,
		ParamsRedacted: params,
		At:             start,
		TookMS:         time.Since(start).Milliseconds(),
	})
}

// Close releases the underlying client.
func (c *Client) Close() { c.cl.Close() }

// Topics lists non-internal topic names, sorted.
func (c *Client) Topics(ctx context.Context) ([]string, error) {
	start := time.Now()
	td, err := c.metadata().ListTopics(ctx)
	c.record(ctx, start, "kadm.ListTopics()")
	if err != nil {
		return nil, fmt.Errorf("kafka: list topics: %w", err)
	}
	var out []string
	for name, d := range td {
		if d.Err != nil || isInternal(name) {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// TopicOffsets returns per-partition start/end offsets and newest record time.
// End is the high watermark. LastTimestamp is read where a ReadCommitted
// consumer can read: at the last stable offset - 1, which is End-1 unless a
// transaction is open at the tail. A transaction marker there counts; its
// timestamp is when the transaction ended.
func (c *Client) TopicOffsets(ctx context.Context, topic string) ([]clients.PartitionOffsets, error) {
	tparam := "topic=" + topic
	start := time.Now()
	starts, err := c.lister().ListStartOffsets(ctx, topic)
	c.record(ctx, start, "kadm.ListStartOffsets(topic)", tparam)
	if err != nil {
		return nil, fmt.Errorf("kafka: start offsets %s: %w", topic, err)
	}
	start = time.Now()
	ends, err := c.lister().ListEndOffsets(ctx, topic)
	c.record(ctx, start, "kadm.ListEndOffsets(topic)", tparam)
	if err != nil {
		return nil, fmt.Errorf("kafka: end offsets %s: %w", topic, err)
	}
	sparts := starts[topic]
	// Only UnknownTopicOrPartition means "no such topic"; any other error
	// (authorization, leader election) is a failure, not an absent topic.
	if topicMissing(sparts) {
		return nil, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
	}

	var out []clients.PartitionOffsets
	for p, s := range sparts {
		if s.Err != nil {
			return nil, fmt.Errorf("kafka: start offset %s/%d: %w", topic, p, s.Err)
		}
		e, ok := ends.Lookup(topic, p)
		if !ok {
			return nil, fmt.Errorf("kafka: end offset %s/%d: missing from response", topic, p)
		}
		if e.Err != nil {
			return nil, fmt.Errorf("kafka: end offset %s/%d: %w", topic, p, e.Err)
		}
		out = append(out, clients.PartitionOffsets{Partition: p, Start: s.Offset, End: e.Offset})
	}
	readable, err := c.clampToStable(ctx, topic, out)
	if err != nil {
		return nil, fmt.Errorf("kafka: topic offsets %s: %w", topic, err)
	}
	at, lo := map[int32]int64{}, map[int32]int64{}
	for _, r := range readable {
		if r.End > r.Start {
			at[r.Partition], lo[r.Partition] = r.End-1, r.Start
		}
	}
	timestamps, terr := c.recordTimestamps(ctx, topic, at, lo)
	if terr != nil {
		return nil, fmt.Errorf("kafka: record timestamps %s: %w", topic, terr)
	}
	for i := range out {
		if ts, ok := timestamps[out[i].Partition]; ok {
			out[i].LastTimestamp = ts
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out, nil
}

// TopicConfig is one topic's key configuration and shape, as returned by
// DescribeTopicConfigs. Config values are the raw Kafka strings; "-1" means
// unlimited where the config allows it. It lives in this package until
// clients.KafkaAdmin gains the method (contract change request in the PR);
// it is then meant to move to package clients unchanged.
type TopicConfig struct {
	Topic             string `json:"topic"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	RetentionMS       string `json:"retention_ms"`
	RetentionBytes    string `json:"retention_bytes"`
	CleanupPolicy     string `json:"cleanup_policy"`
	MinInsyncReplicas string `json:"min_insync_replicas"`
}

// DescribeTopicConfigs returns the topic's key configs (retention.ms,
// retention.bytes, cleanup.policy, min.insync.replicas) plus its partition
// count and replication factor. Values are effective: a broker default counts
// when the topic carries no override. An unknown topic is reported as
// clients.ErrNotFound.
func (c *Client) DescribeTopicConfigs(ctx context.Context, topic string) (TopicConfig, error) {
	start := time.Now()
	rcs, err := c.metadata().DescribeTopicConfigs(ctx, topic)
	c.record(ctx, start, "kadm.DescribeTopicConfigs(topic)", "topic="+topic)
	if err != nil {
		return TopicConfig{}, fmt.Errorf("kafka: topic configs %s: %w", topic, mapNotFound(err))
	}
	var rc *kadm.ResourceConfig
	for i := range rcs {
		if rcs[i].Name == topic {
			rc = &rcs[i]
			break
		}
	}
	if rc == nil {
		return TopicConfig{}, fmt.Errorf("kafka: topic configs %s: %w", topic, clients.ErrNotFound)
	}
	if rc.Err != nil {
		return TopicConfig{}, fmt.Errorf("kafka: topic configs %s: %w", topic, mapNotFound(rc.Err))
	}

	start = time.Now()
	tds, err := c.metadata().ListTopics(ctx, topic)
	c.record(ctx, start, "kadm.ListTopics(topic)", "topic="+topic)
	if err != nil {
		return TopicConfig{}, fmt.Errorf("kafka: topic details %s: %w", topic, err)
	}
	td, ok := tds[topic]
	if !ok {
		return TopicConfig{}, fmt.Errorf("kafka: topic details %s: %w", topic, clients.ErrNotFound)
	}
	if td.Err != nil {
		return TopicConfig{}, fmt.Errorf("kafka: topic details %s: %w", topic, mapNotFound(td.Err))
	}
	return topicConfigFrom(topic, *rc, td), nil
}

// topicConfigFrom maps kadm's config entries and partition details onto the
// DTO. Only the keys the tool reports are read, so no other (possibly
// sensitive) config value can leak into the result.
func topicConfigFrom(topic string, rc kadm.ResourceConfig, td kadm.TopicDetail) TopicConfig {
	cfg := TopicConfig{
		Topic:             topic,
		Partitions:        len(td.Partitions),
		ReplicationFactor: td.Partitions.NumReplicas(),
	}
	for i := range rc.Configs {
		switch rc.Configs[i].Key {
		case "retention.ms":
			cfg.RetentionMS = rc.Configs[i].MaybeValue()
		case "retention.bytes":
			cfg.RetentionBytes = rc.Configs[i].MaybeValue()
		case "cleanup.policy":
			cfg.CleanupPolicy = rc.Configs[i].MaybeValue()
		case "min.insync.replicas":
			cfg.MinInsyncReplicas = rc.Configs[i].MaybeValue()
		}
	}
	return cfg
}

// mapNotFound turns the broker's UNKNOWN_TOPIC_OR_PARTITION into
// clients.ErrNotFound so callers can tell "no such topic" from a broker
// failure. kadm reports it as the kerr sentinel pointer.
func mapNotFound(err error) error {
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return clients.ErrNotFound
	}
	return err
}

// ConsumerGroups lists consumer group names.
func (c *Client) ConsumerGroups(ctx context.Context) ([]string, error) {
	start := time.Now()
	lg, err := c.metadata().ListGroups(ctx)
	c.record(ctx, start, "kadm.ListGroups()")
	if err != nil {
		return nil, fmt.Errorf("kafka: list groups: %w", err)
	}
	groups := lg.Groups()
	sort.Strings(groups)
	return groups, nil
}

// GroupLag returns a group's state, member count, and per-partition lag.
func (c *Client) GroupLag(ctx context.Context, group string) (clients.GroupLag, error) {
	start := time.Now()
	dgs, err := c.metadata().Lag(ctx, group)
	c.record(ctx, start, "kadm.Lag(group)", "group="+group)
	if err != nil {
		return clients.GroupLag{}, fmt.Errorf("kafka: group lag %s: %w", group, err)
	}
	dg, ok := dgs[group]
	if !ok {
		return clients.GroupLag{}, fmt.Errorf("kafka: group %s: %w", group, clients.ErrNotFound)
	}
	if dg.DescribeErr != nil {
		return clients.GroupLag{}, fmt.Errorf("kafka: describe %s: %w", group, dg.DescribeErr)
	}
	if dg.FetchErr != nil {
		return clients.GroupLag{}, fmt.Errorf("kafka: fetch offsets %s: %w", group, dg.FetchErr)
	}
	out := clients.GroupLag{
		Group:   dg.Group,
		State:   dg.State,
		Members: len(dg.Members),
	}
	for _, l := range dg.Lag.Sorted() {
		if l.Err != nil {
			// kadm's Lag swallows a broker's shard error into Err: the
			// partition's lag is not known, which is not "no lag".
			out.Unknown = append(out.Unknown, unknownLag(l))
			continue
		}
		out.TotalLag += l.Lag
		out.Partitions = append(out.Partitions, clients.PartitionLag{
			Topic:     l.Topic,
			Partition: l.Partition,
			Committed: l.Commit.At,
			End:       l.End.Offset,
			Lag:       l.Lag,
		})
	}
	sortLags(out.Partitions)
	sortLags(out.Unknown)
	return out, nil
}

// unknownLag is a partition whose committed or end offset could not be read,
// with the offsets that were read and -1 for the others (clients.GroupLag).
// kadm sets Err to the commit error before the end offset error, so Err is
// the end offset's own error only when the commit was read. When the end
// offset is missing from the listing, kadm reports that whatever the commit
// did; the broker answers a failed offset fetch with offset -1, so Commit.At
// is then -1 anyway.
func unknownLag(l kadm.GroupMemberLag) clients.PartitionLag {
	pl := clients.PartitionLag{Topic: l.Topic, Partition: l.Partition, Committed: -1, End: -1, Lag: -1}
	endRead := l.End.Err == nil
	if endRead {
		pl.End = l.End.Offset
	}
	if !endRead && errors.Is(l.Err, l.End.Err) {
		pl.Committed = l.Commit.At
	}
	return pl
}

// sortLags orders partitions by topic, then partition.
func sortLags(ps []clients.PartitionLag) {
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].Topic != ps[j].Topic {
			return ps[i].Topic < ps[j].Topic
		}
		return ps[i].Partition < ps[j].Partition
	})
}

const (
	defaultMaxScan = 5000
	// maxFindScan bounds one partition's FindByKey window. The scan deadline
	// cuts a long scan anyway; the bound keeps start+maxScan from
	// overflowing into a window that ends before it starts.
	maxFindScan = 20000
)

// afterMilli is since as the timestamp of a ListOffsets-by-time request.
// Kafka reads negative timestamps as special offsets (-1 latest, -2
// earliest), so a time before the Unix epoch is refused, never sent.
func afterMilli(since time.Time) (int64, error) {
	ms := since.UnixMilli()
	if ms < 0 {
		return 0, fmt.Errorf("kafka: since %s is before the Unix epoch", since.UTC().Format(time.RFC3339Nano))
	}
	return ms, nil
}

// normalizeMaxScan maps maxScan <= 0 to the default and caps it.
func normalizeMaxScan(maxScan int) int {
	switch {
	case maxScan <= 0:
		return defaultMaxScan
	case maxScan > maxFindScan:
		return maxFindScan
	}
	return maxScan
}

// FindByKey scans the topic from the first offset at or after since and returns
// records whose decoded key equals key, per partition in offset order (so the
// newest match of a partition is its last). It stops at maxScan records per
// partition (<= 0 means 5000, capped at 20000), the partition end or the last
// stable offset: only committed records are read, so a record of an aborted
// or still open transaction never matches. A deadline that cuts the scan
// short before any match is returned as an error, not a false "not found";
// so is an unknown topic (clients.ErrNotFound). FindByKeyScan also says
// which partitions the scan did not finish.
func (c *Client) FindByKey(ctx context.Context, topic, key string, since time.Time, maxScan int) ([]clients.RecordRef, error) {
	scan, err := c.FindByKeyScan(ctx, topic, key, since, maxScan)
	return scan.Refs, err
}

var _ clients.KeyScanReader = (*Client)(nil)

// FindByKeyScan is FindByKey that also reports the partitions whose window the
// scan did not finish (clients.KeyScanReader).
func (c *Client) FindByKeyScan(ctx context.Context, topic, key string, since time.Time, maxScan int) (clients.KeyScan, error) {
	maxScan = normalizeMaxScan(maxScan)
	ms, err := afterMilli(since)
	if err != nil {
		return clients.KeyScan{}, err
	}
	tparam := "topic=" + topic
	start := time.Now()
	offs, err := c.lister().ListOffsetsAfterMilli(ctx, ms, topic)
	c.record(ctx, start, "kadm.ListOffsetsAfterMilli(topic, since)", tparam, "since="+since.UTC().Format(time.RFC3339))
	if err != nil {
		return clients.KeyScan{}, fmt.Errorf("kafka: offsets after %s: %w", since, err)
	}
	if topicMissing(offs[topic]) {
		return clients.KeyScan{}, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
	}
	start = time.Now()
	ends, err := c.lister().ListEndOffsets(ctx, topic)
	c.record(ctx, start, "kadm.ListEndOffsets(topic)", tparam)
	if err != nil {
		return clients.KeyScan{}, fmt.Errorf("kafka: end offsets %s: %w", topic, err)
	}

	var windows []clients.PartitionOffsets
	for p, lo := range offs[topic] {
		// A partition left out would turn into a false "not found".
		if lo.Err != nil {
			return clients.KeyScan{}, fmt.Errorf("kafka: offset after %s for %s/%d: %w", since, topic, p, lo.Err)
		}
		if lo.Offset < 0 {
			continue
		}
		// The window runs to the readable end first; maxScan cuts it after
		// the clamp, so a cut is told apart from an open transaction.
		end := int64(math.MaxInt64)
		if e, ok := ends.Lookup(topic, p); ok && e.Err == nil {
			end = e.Offset
		}
		windows = append(windows, clients.PartitionOffsets{Partition: p, Start: lo.Offset, End: end})
	}
	windows, err = c.clampToStable(ctx, topic, windows)
	if err != nil {
		return clients.KeyScan{}, fmt.Errorf("kafka: find by key %s: %w", topic, err)
	}
	partOffsets := map[string]map[int32]kgo.Offset{topic: {}}
	stopAt := map[int32]int64{}
	wait := map[int32]readWindow{} // what each unfinished partition has left
	finished := map[int32]bool{}
	var truncated []int32
	for _, w := range windows {
		if w.End <= w.Start {
			continue // nothing readable to scan in this partition
		}
		if w.End-w.Start > int64(maxScan) {
			w.End = w.Start + int64(maxScan)
			truncated = append(truncated, w.Partition)
		}
		partOffsets[topic][w.Partition] = kgo.NewOffset().At(w.Start)
		stopAt[w.Partition] = w.End
		wait[w.Partition] = readWindow{from: w.Start, end: w.End}
	}
	if len(partOffsets[topic]) == 0 {
		return clients.KeyScan{}, nil
	}

	cl, err := c.newConsumer(partOffsets)
	if err != nil {
		return clients.KeyScan{}, fmt.Errorf("kafka: scan client: %w", err)
	}
	defer cl.Close()
	pr := c.newProber(topic)
	defer pr.record(ctx) // runs after the scan's receipt below
	// One receipt for the whole scan, on every exit path. The key is a record
	// value (it can name a customer), so it is never recorded.
	scanStart := time.Now()
	defer func() {
		c.record(ctx, scanStart, "kgo.PollFetches(topic, since, maxScan)",
			tparam, "since="+since.UTC().Format(time.RFC3339), "max_scan="+strconv.Itoa(maxScan), "key=<redacted>")
	}()

	var found []clients.RecordRef
	scanned := 0
	// cut ends the scan at the deadline: matches found so far are kept and
	// the unfinished partitions reported, since a later match may exist
	// there; with no match it is an error, never a false "not found".
	cut := func(cause error) (clients.KeyScan, error) {
		if len(found) == 0 {
			return clients.KeyScan{}, fmt.Errorf("kafka: scan %s incomplete after %d records: %w", topic, scanned, cause)
		}
		for p := range stopAt {
			if !finished[p] && !slices.Contains(truncated, p) {
				truncated = append(truncated, p)
			}
		}
		return keyScan(found, truncated), nil
	}
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		fetches := pollBounded(cctx, cl)
		for _, fe := range fetches.Errors() {
			if fe.Err != nil && !errors.Is(fe.Err, context.Canceled) && !errors.Is(fe.Err, context.DeadlineExceeded) {
				return clients.KeyScan{}, fmt.Errorf("kafka: scan %s: %w", topic, fe.Err)
			}
		}
		for _, r := range fetches.Records() {
			// As in collectPeek: a transaction marker can be the last offset
			// of the window, and it finishes the partition but is no match.
			// So does a record past the window: compaction or an aborted
			// transaction can leave no record at the last offset.
			if r.Offset+1 >= stopAt[r.Partition] {
				finished[r.Partition] = true
				delete(wait, r.Partition)
			} else if w, ok := wait[r.Partition]; ok && r.Offset >= w.from {
				w.from = r.Offset + 1
				wait[r.Partition] = w
			}
			if r.Offset >= stopAt[r.Partition] || r.Attrs.IsControl() {
				continue
			}
			if cctx.Err() != nil {
				// Check between records so a batch of slow keys cannot run
				// past the scan deadline. This record is not checked, so its
				// partition is not finished.
				delete(finished, r.Partition)
				return cut(cctx.Err())
			}
			scanned++
			dk, derr := c.decodeKey(cctx, r.Key)
			if derr == nil && dk == key {
				found = append(found, clients.RecordRef{
					Topic:     r.Topic,
					Partition: r.Partition,
					Offset:    r.Offset,
					Timestamp: r.Timestamp,
					Key:       dk,
				})
			}
		}
		// An aborted tail below an open transaction delivers no record
		// (#300), while other partitions may keep delivering: probe each
		// window that stopped moving.
		pr.stalled(cctx, wait)
		for p, w := range wait {
			if w.from >= w.end {
				finished[p] = true
				delete(wait, p)
			}
		}
		if len(finished) == len(stopAt) {
			return keyScan(found, truncated), nil // scanned every window
		}
		if cctx.Err() != nil {
			return cut(cctx.Err())
		}
	}
}

// keyScan orders matches per partition by offset (producer timestamps can go
// backwards, the log cannot), partitions ascending, and sorts truncated.
func keyScan(found []clients.RecordRef, truncated []int32) clients.KeyScan {
	sort.Slice(found, func(i, j int) bool {
		if found[i].Partition != found[j].Partition {
			return found[i].Partition < found[j].Partition
		}
		return found[i].Offset < found[j].Offset
	})
	slices.Sort(truncated)
	return clients.KeyScan{Refs: found, Truncated: truncated}
}

// recordTimestamps fetches one record per partition (at the given offsets) in a
// single consumer and returns their timestamps. A transaction marker at the
// offset counts as the record; a later record stands in when the offset
// itself is never delivered but newer records are. When nothing readable is
// at or after the offset (an aborted record below an open transaction, #300),
// the newest readable record in [lo, offset) stands in; a partition with none
// at all has no entry.
func (c *Client) recordTimestamps(ctx context.Context, topic string, at, lo map[int32]int64) (map[int32]time.Time, error) {
	if len(at) == 0 {
		return map[int32]time.Time{}, nil
	}
	partOffsets := map[string]map[int32]kgo.Offset{topic: {}}
	for p, off := range at {
		partOffsets[topic][p] = kgo.NewOffset().At(off)
	}
	cl, err := c.newConsumer(partOffsets)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	rctx := ctx
	pr := c.newProber(topic)
	defer pr.record(rctx) // runs after the poll's receipt below
	pollStart := time.Now()
	defer func() { c.record(rctx, pollStart, "kgo.PollFetches(topic, offsets)", "topic="+topic) }()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	got := map[int32]time.Time{}
	wait := map[int32]readWindow{}
	for p, off := range at {
		wait[p] = readWindow{from: off, end: off + 1}
	}
	for len(wait) > 0 {
		fetches := pollBounded(ctx, cl)
		for _, r := range fetches.Records() {
			if _, waiting := wait[r.Partition]; waiting && r.Offset >= at[r.Partition] {
				got[r.Partition] = r.Timestamp
				delete(wait, r.Partition)
			}
		}
		if err := fetches.Err(); err != nil {
			return got, err
		}
		// An offset the probe passes holds no readable record (an aborted
		// one, #300): the newest readable one below stands in.
		pr.stalled(ctx, wait)
		for p, w := range wait {
			if w.from < w.end {
				continue
			}
			delete(wait, p)
			ts, ok, err := pr.newestReadable(ctx, p, lo[p], at[p])
			if err != nil {
				return got, fmt.Errorf("newest readable record %s/%d: %w", topic, p, err)
			}
			if ok {
				got[p] = ts
			}
		}
		if ctx.Err() != nil {
			return got, ctx.Err()
		}
	}
	return got, nil
}

// decodeKey decodes a Confluent Avro key into a comparable string. Raw
// (non-Confluent) keys are returned as-is. Parsed schemas are cached by id.
//
// The key is attacker-controlled, including the schema id, so the decode is
// bounded by construction (GO-2026-5046/5047/5048): keys over 64 KiB are
// refused; key schemas are parsed in a private cache and refused if they hold
// array/map types, recursion, fixed sizes, decimal precision/scale, size or
// node counts beyond the caps in checkKeySchema (the walk runs before any
// decoder is built; refusals are cached per id); the decoder is capped (see newKeyAPI); and a
// decoder panic becomes an error. Decode takes no context, so these limits,
// not a deadline, are what bound its time and memory; FindByKey also checks
// its deadline between records.
func (c *Client) decodeKey(ctx context.Context, key []byte) (out string, err error) {
	// Without a Schema Registry (SCHEMA_REGISTRY_URL empty or off) no key is
	// Avro-decoded: a key that merely starts with 0x00 is compared raw.
	if c.sr == nil || len(key) < 5 || key[0] != 0x00 {
		return string(key), nil
	}
	if len(key) > maxKeyBytes {
		return "", fmt.Errorf("kafka: avro key too large (%d bytes)", len(key))
	}
	defer func() {
		if r := recover(); r != nil {
			out, err = "", fmt.Errorf("kafka: avro key decode panicked: %v", r)
		}
	}()
	id := int(binary.BigEndian.Uint32(key[1:5]))
	schema, err := c.schemaByID(ctx, id)
	if err != nil {
		return "", err
	}
	c.srMu.Lock()
	if c.keyAPI == nil {
		c.keyAPI = newKeyAPI()
	}
	api := c.keyAPI
	c.srMu.Unlock()
	if c.decodeHook != nil {
		c.decodeHook()
	}
	dec := api.NewDecoder(schema, bytes.NewReader(key[5:]))
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	if m, ok := v.(map[string]any); ok && len(m) == 1 {
		for _, val := range m {
			return fmt.Sprint(val), nil
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// schemaByID returns a parsed, vetted key schema, cached by id (schema ids are
// immutable). A schema that is too large, does not parse or fails
// checkKeySchema is refused, and the refusal is cached too so a hostile id
// costs one fetch and parse, not one per key. Both caches stop growing at
// maxSchemaCache entries.
func (c *Client) schemaByID(ctx context.Context, id int) (avro.Schema, error) {
	c.srMu.Lock()
	if why := c.keyRefused[id]; why != "" {
		c.srMu.Unlock()
		return nil, fmt.Errorf("kafka: avro key schema %d refused: %s", id, why)
	}
	s, ok := c.srCache[id]
	c.srMu.Unlock()
	if ok {
		return s, nil
	}
	schemaStr, err := c.sr.SchemaByID(ctx, id)
	if err != nil {
		return nil, err
	}
	var why string
	if len(schemaStr) > maxKeySchemaBytes {
		why = fmt.Sprintf("schema text too large (%d bytes)", len(schemaStr))
	} else if s, err = avro.ParseWithCache(schemaStr, "", &avro.SchemaCache{}); err != nil {
		// A fresh cache per schema: avro.Parse shares a process-wide cache, so
		// a name defined by an earlier (safe) schema would shadow a hostile
		// redefinition in this one and hide it from the walk below.
		why = "does not parse: " + err.Error()
	} else if werr := checkKeySchema(s); werr != nil {
		why = werr.Error()
	}
	c.srMu.Lock()
	defer c.srMu.Unlock()
	if why != "" {
		if c.keyRefused == nil {
			c.keyRefused = map[int]string{}
		}
		if len(c.keyRefused) < maxSchemaCache {
			c.keyRefused[id] = why
		}
		return nil, fmt.Errorf("kafka: avro key schema %d refused: %s", id, why)
	}
	if len(c.srCache) < maxSchemaCache {
		c.srCache[id] = s
	}
	return s, nil
}

// maxDecimalDigits is the DECIMAL256 maximum; hamba computes 10^scale on every
// decode, so larger values are a cheap amplification.
const maxDecimalDigits = 76

// checkKeySchema returns an error if decoding a key with s could allocate or
// spin beyond what the 64 KiB key implies. hamba decodes every reference to a
// named type afresh, so the walk visits the schema the way it will be decoded
// (no shared-node shortcut) and gives up after maxSchemaNodes visits or
// maxSchemaDepth levels. It refuses array/map types (block counts), recursive
// records (stack overflow), fixed sizes above the key cap summed per visit
// (hamba allocates at decoder build time), and oversized decimal
// precision/scale. Records are tracked by pointer, never by name, so a
// redefinition cannot hide.
func checkKeySchema(s avro.Schema) error {
	w := &keyWalk{onPath: map[*avro.RecordSchema]bool{}}
	return w.walk(s, 0)
}

const (
	maxSchemaNodes = 1000 // fields, union branches and enum symbols count one each
	maxSchemaDepth = 32
	// maxKeySchemaBytes caps the schema text before parsing: hamba builds
	// fingerprint strings quadratically, and a real key schema is tiny.
	maxKeySchemaBytes = 64 << 10
	maxSchemaCache    = 1024
)

type keyWalk struct {
	onPath     map[*avro.RecordSchema]bool
	nodes      int
	fixedTotal int
}

func (w *keyWalk) walk(s avro.Schema, depth int) error {
	if depth > maxSchemaDepth {
		return errors.New("schema nested too deeply")
	}
	if w.nodes++; w.nodes > maxSchemaNodes {
		return errors.New("schema too large (node count)")
	}
	switch t := s.(type) {
	case *avro.ArraySchema, *avro.MapSchema:
		return errors.New("array/map type")
	case *avro.RefSchema:
		return w.walk(t.Schema(), depth+1)
	case *avro.RecordSchema:
		if w.onPath[t] {
			return errors.New("recursive record " + t.FullName())
		}
		w.onPath[t] = true
		defer delete(w.onPath, t)
		for _, f := range t.Fields() {
			if err := w.walk(f.Type(), depth+1); err != nil {
				return err
			}
		}
	case *avro.UnionSchema:
		for _, u := range t.Types() {
			if err := w.walk(u, depth+1); err != nil {
				return err
			}
		}
	case *avro.EnumSchema:
		if w.nodes += len(t.Symbols()); w.nodes > maxSchemaNodes {
			return errors.New("schema too large (node count)")
		}
	case *avro.FixedSchema:
		w.fixedTotal += t.Size()
		if t.Size() < 0 || w.fixedTotal > maxKeyBytes {
			return errors.New("fixed size above key cap")
		}
		return checkDecimal(t.Logical())
	case *avro.PrimitiveSchema:
		return checkDecimal(t.Logical())
	}
	return nil
}

func checkDecimal(l avro.LogicalSchema) error {
	if d, ok := l.(*avro.DecimalLogicalSchema); ok && (d.Precision() > maxDecimalDigits || d.Scale() > maxDecimalDigits) {
		return errors.New("decimal precision/scale above cap")
	}
	return nil
}

func isInternal(name string) bool {
	return strings.HasPrefix(name, "__") || strings.HasPrefix(name, "_connect_") || name == "_schemas"
}
