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

	// offs and consumerFn are test seams for the DLQ reader; nil means the
	// real kadm/kgo clients.
	offs       offsetLister
	clusterFn  clusterAdmin
	consumerFn func(map[string]map[int32]kgo.Offset) (dlqConsumer, error)

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
	td, err := c.adm.ListTopics(ctx)
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
func (c *Client) TopicOffsets(ctx context.Context, topic string) ([]clients.PartitionOffsets, error) {
	tparam := "topic=" + topic
	start := time.Now()
	starts, err := c.adm.ListStartOffsets(ctx, topic)
	c.record(ctx, start, "kadm.ListStartOffsets(topic)", tparam)
	if err != nil {
		return nil, fmt.Errorf("kafka: start offsets %s: %w", topic, err)
	}
	start = time.Now()
	ends, err := c.adm.ListEndOffsets(ctx, topic)
	c.record(ctx, start, "kadm.ListEndOffsets(topic)", tparam)
	if err != nil {
		return nil, fmt.Errorf("kafka: end offsets %s: %w", topic, err)
	}
	sparts, ok := starts[topic]
	if !ok || len(sparts) == 0 {
		return nil, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
	}
	// kadm reports an unknown topic as partition -1 with UnknownTopicOrPartition.
	for p, s := range sparts {
		if p < 0 || s.Err != nil {
			return nil, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
		}
	}

	at := map[int32]int64{}
	var out []clients.PartitionOffsets
	for p, s := range sparts {
		e, ok := ends.Lookup(topic, p)
		if !ok || e.Err != nil {
			return nil, fmt.Errorf("kafka: end offset %s/%d: %w", topic, p, e.Err)
		}
		po := clients.PartitionOffsets{Partition: p, Start: s.Offset, End: e.Offset}
		if e.Offset > s.Offset {
			at[p] = e.Offset - 1
		}
		out = append(out, po)
	}
	timestamps, terr := c.recordTimestamps(ctx, topic, at)
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
	rcs, err := c.adm.DescribeTopicConfigs(ctx, topic)
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
	tds, err := c.adm.ListTopics(ctx, topic)
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
	lg, err := c.adm.ListGroups(ctx)
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
	dgs, err := c.adm.Lag(ctx, group)
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
		Group:    dg.Group,
		State:    dg.State,
		Members:  len(dg.Members),
		TotalLag: dg.Lag.Total(),
	}
	for _, l := range dg.Lag.Sorted() {
		if l.Err != nil {
			continue // skip partitions whose commit/end fetch failed
		}
		out.Partitions = append(out.Partitions, clients.PartitionLag{
			Topic:     l.Topic,
			Partition: l.Partition,
			Committed: l.Commit.At,
			End:       l.End.Offset,
			Lag:       l.Lag,
		})
	}
	sort.Slice(out.Partitions, func(i, j int) bool {
		if out.Partitions[i].Topic != out.Partitions[j].Topic {
			return out.Partitions[i].Topic < out.Partitions[j].Topic
		}
		return out.Partitions[i].Partition < out.Partitions[j].Partition
	})
	return out, nil
}

// FindByKey scans the topic from the first offset at or after since and returns
// records whose decoded key equals key. It stops at maxScan records per
// partition (or the partition end). A deadline that cuts the scan short is
// returned as an error, not a false "not found".
func (c *Client) FindByKey(ctx context.Context, topic, key string, since time.Time, maxScan int) ([]clients.RecordRef, error) {
	if maxScan <= 0 {
		maxScan = 5000
	}
	tparam := "topic=" + topic
	start := time.Now()
	offs, err := c.adm.ListOffsetsAfterMilli(ctx, since.UnixMilli(), topic)
	c.record(ctx, start, "kadm.ListOffsetsAfterMilli(topic, since)", tparam, "since="+since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("kafka: offsets after %s: %w", since, err)
	}
	start = time.Now()
	ends, err := c.adm.ListEndOffsets(ctx, topic)
	c.record(ctx, start, "kadm.ListEndOffsets(topic)", tparam)
	if err != nil {
		return nil, fmt.Errorf("kafka: end offsets %s: %w", topic, err)
	}

	partOffsets := map[string]map[int32]kgo.Offset{topic: {}}
	stopAt := map[int32]int64{}
	finished := map[int32]bool{}
	for p, lo := range offs[topic] {
		if lo.Err != nil || lo.Offset < 0 {
			continue
		}
		end := lo.Offset + int64(maxScan)
		if e, ok := ends.Lookup(topic, p); ok && e.Err == nil && e.Offset < end {
			end = e.Offset
		}
		if end <= lo.Offset {
			finished[p] = true // nothing to scan in this partition
			continue
		}
		partOffsets[topic][p] = kgo.NewOffset().At(lo.Offset)
		stopAt[p] = end
	}
	if len(partOffsets[topic]) == 0 {
		return nil, nil
	}

	cl, err := c.newKgo(kgo.ConsumePartitions(partOffsets))
	if err != nil {
		return nil, fmt.Errorf("kafka: scan client: %w", err)
	}
	defer cl.Close()
	// One receipt for the whole scan, on every exit path. The key is a record
	// value (it can name a customer), so it is never recorded.
	scanStart := time.Now()
	defer func() {
		c.record(ctx, scanStart, "kgo.PollFetches(topic, since, maxScan)",
			tparam, "since="+since.UTC().Format(time.RFC3339), "max_scan="+strconv.Itoa(maxScan), "key=<redacted>")
	}()

	var found []clients.RecordRef
	scanned := map[int32]int{}
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		fetches := cl.PollFetches(cctx)
		for _, fe := range fetches.Errors() {
			if fe.Err != nil && !errors.Is(fe.Err, context.Canceled) && !errors.Is(fe.Err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("kafka: scan %s: %w", topic, fe.Err)
			}
		}
		for _, r := range fetches.Records() {
			if r.Offset >= stopAt[r.Partition] {
				finished[r.Partition] = true
				continue
			}
			if cctx.Err() != nil {
				// Check between records so a batch of slow keys cannot run past the scan deadline.
				total := 0
				for _, n := range scanned {
					total += n
				}
				return nil, fmt.Errorf("kafka: scan %s incomplete after %d records: %w", topic, total, cctx.Err())
			}
			scanned[r.Partition]++
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
			// This is the last record in the scan window for this partition.
			if r.Offset+1 >= stopAt[r.Partition] {
				finished[r.Partition] = true
			}
		}
		if len(found) > 0 {
			return found, nil
		}
		allDone := true
		for p := range partOffsets[topic] {
			if !finished[p] {
				allDone = false
				break
			}
		}
		if allDone {
			return found, nil // scanned the whole window; key genuinely absent
		}
		if cctx.Err() != nil {
			total := 0
			for _, n := range scanned {
				total += n
			}
			return nil, fmt.Errorf("kafka: scan %s incomplete after %d records: %w", topic, total, cctx.Err())
		}
	}
}

// recordTimestamps fetches one record per partition (at the given offsets) in a
// single consumer and returns their timestamps.
func (c *Client) recordTimestamps(ctx context.Context, topic string, at map[int32]int64) (map[int32]time.Time, error) {
	if len(at) == 0 {
		return map[int32]time.Time{}, nil
	}
	partOffsets := map[string]map[int32]kgo.Offset{topic: {}}
	for p, off := range at {
		partOffsets[topic][p] = kgo.NewOffset().At(off)
	}
	cl, err := c.newKgo(kgo.ConsumePartitions(partOffsets))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	rctx := ctx
	pollStart := time.Now()
	defer func() { c.record(rctx, pollStart, "kgo.PollFetches(topic, offsets)", "topic="+topic) }()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	got := map[int32]time.Time{}
	for len(got) < len(at) {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			if _, done := got[r.Partition]; done {
				continue
			}
			if want, ok := at[r.Partition]; ok && r.Offset == want {
				got[r.Partition] = r.Timestamp
			}
		}
		if err := fetches.Err(); err != nil {
			return got, err
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
