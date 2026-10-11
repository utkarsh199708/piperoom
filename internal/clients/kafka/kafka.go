// Package kafka implements clients.KafkaAdmin against a Kafka cluster using
// franz-go (kgo for consuming, kadm for admin reads).
package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/clients/schemareg"
)

// Client is a Kafka admin + small-reads client.
type Client struct {
	brokers []string
	adm     *kadm.Client
	cl      *kgo.Client
	sr      *schemareg.Client
}

// New connects to brokers (e.g. localhost:29092). sr is used to decode Avro keys.
func New(brokers []string, sr *schemareg.Client) (*Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return &Client{brokers: brokers, adm: kadm.NewClient(cl), cl: cl, sr: sr}, nil
}

// Close releases the underlying client.
func (c *Client) Close() { c.cl.Close() }

// Topics lists non-internal topic names, sorted.
func (c *Client) Topics(ctx context.Context) ([]string, error) {
	td, err := c.adm.ListTopics(ctx)
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
	starts, err := c.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: start offsets %s: %w", topic, err)
	}
	ends, err := c.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: end offsets %s: %w", topic, err)
	}
	var out []clients.PartitionOffsets
	for p, s := range starts[topic] {
		if s.Err != nil {
			continue
		}
		e, ok := ends.Lookup(topic, p)
		if !ok || e.Err != nil {
			continue
		}
		po := clients.PartitionOffsets{Partition: p, Start: s.Offset, End: e.Offset}
		if e.Offset > s.Offset {
			po.LastTimestamp, _ = c.recordTimestamp(ctx, topic, p, e.Offset-1)
		}
		out = append(out, po)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out, nil
}

// ConsumerGroups lists consumer group names.
func (c *Client) ConsumerGroups(ctx context.Context) ([]string, error) {
	lg, err := c.adm.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: list groups: %w", err)
	}
	groups := lg.Groups()
	sort.Strings(groups)
	return groups, nil
}

// GroupLag returns a group's state, member count, and per-partition lag.
func (c *Client) GroupLag(ctx context.Context, group string) (clients.GroupLag, error) {
	dgs, err := c.adm.Lag(ctx, group)
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
	out := clients.GroupLag{
		Group:    dg.Group,
		State:    dg.State,
		Members:  len(dg.Members),
		TotalLag: dg.Lag.Total(),
	}
	for _, l := range dg.Lag.Sorted() {
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
// records whose decoded key equals key. It stops at maxScan records per partition.
func (c *Client) FindByKey(ctx context.Context, topic, key string, since time.Time, maxScan int) ([]clients.RecordRef, error) {
	if maxScan <= 0 {
		maxScan = 5000
	}
	offs, err := c.adm.ListOffsetsAfterMilli(ctx, since.UnixMilli(), topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: offsets after %s: %w", since, err)
	}
	partOffsets := map[string]map[int32]kgo.Offset{topic: {}}
	for p, lo := range offs[topic] {
		if lo.Err != nil || lo.Offset < 0 {
			continue
		}
		partOffsets[topic][p] = kgo.NewOffset().At(lo.Offset)
	}
	if len(partOffsets[topic]) == 0 {
		return nil, nil
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.brokers...),
		kgo.ConsumePartitions(partOffsets),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: scan client: %w", err)
	}
	defer cl.Close()

	var found []clients.RecordRef
	scanned := map[int32]int{}
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		fetches := cl.PollFetches(cctx)
		for _, fe := range fetches.Errors() {
			if fe.Err != nil && !strings.Contains(fe.Err.Error(), "context") {
				return nil, fmt.Errorf("kafka: scan %s: %w", topic, fe.Err)
			}
		}
		done := true
		for _, r := range fetches.Records() {
			if scanned[r.Partition] >= maxScan {
				continue
			}
			done = false
			scanned[r.Partition]++
			dk, derr := c.decodeKey(cctx, r.Key)
			if derr != nil {
				continue // unreadable key: skip, keep scanning
			}
			if dk == key {
				found = append(found, clients.RecordRef{
					Topic:     r.Topic,
					Partition: r.Partition,
					Offset:    r.Offset,
					Timestamp: r.Timestamp,
					Key:       dk,
				})
			}
		}
		if len(found) > 0 || done || cctx.Err() != nil {
			break
		}
	}
	return found, nil
}

// recordTimestamp fetches exactly one record at offset and returns its timestamp.
func (c *Client) recordTimestamp(ctx context.Context, topic string, p int32, offset int64) (time.Time, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {p: kgo.NewOffset().At(offset)}}),
	)
	if err != nil {
		return time.Time{}, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			if r.Partition == p && r.Offset == offset {
				return r.Timestamp, nil
			}
		}
		if err := fetches.Err(); err != nil {
			return time.Time{}, err
		}
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
	}
}

// decodeKey decodes a Confluent Avro key into a comparable string. Raw
// (non-Confluent) keys are returned as-is.
func (c *Client) decodeKey(ctx context.Context, key []byte) (string, error) {
	if len(key) < 5 || key[0] != 0x00 {
		return string(key), nil
	}
	id := int(binary.BigEndian.Uint32(key[1:5]))
	schema, err := c.sr.SchemaByID(ctx, id)
	if err != nil {
		return "", err
	}
	dec, err := avro.NewDecoder(schema, bytes.NewReader(key[5:]))
	if err != nil {
		return "", err
	}
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

func isInternal(name string) bool {
	return strings.HasPrefix(name, "__") || strings.HasPrefix(name, "_connect_") || name == "_schemas"
}
