package remoteclients

import (
	"context"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/setup/protocol"
)

// Kafka is the Kafka reader over the runner: clients.KafkaAdmin (which adds
// nothing to clients.KafkaReader in this wave) plus the optional
// clients.KeyScanReader, so a tool that needs the truncated-partition report
// finds it by the usual type assertion.
//
// Result shapes (the runner's OpResult.Data; each is pinned by a test):
//
//	kafka_topics             {"topics": ["name", ...]}
//	kafka_topic_offsets      {"offsets": [clients.PartitionOffsets, ...]}
//	kafka_consumer_groups    {"groups": ["name", ...]}
//	kafka_group_lag          {"lag": clients.GroupLag}
//	kafka_find_by_key        {"records": [clients.RecordRef, ...]}
//	kafka_find_by_key_scan   {"scan": clients.KeyScan}
type Kafka struct{ s *Session }

var (
	_ clients.KafkaAdmin    = (*Kafka)(nil)
	_ clients.KeyScanReader = (*Kafka)(nil)
)

type topicParams struct {
	Topic string `json:"topic"`
}

type groupParams struct {
	Group string `json:"group"`
}

type findParams struct {
	Topic   string    `json:"topic"`
	Key     string    `json:"key"`
	Since   time.Time `json:"since"`
	MaxScan int       `json:"max_scan"`
}

// Topics lists the cluster's topics. The list is cluster metadata; the callers
// scope it to the pipeline.
func (k *Kafka) Topics(ctx context.Context) ([]string, error) {
	res, err := k.s.do(ctx, OpKafkaTopics, protocol.TierT0, nil)
	if err != nil {
		return nil, err
	}
	var out []string
	if err := decode(res, "topics", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// TopicOffsets reads each partition's start, end and last timestamp for topic.
func (k *Kafka) TopicOffsets(ctx context.Context, topic string) ([]clients.PartitionOffsets, error) {
	if err := requireArg(OpKafkaTopicOffsets, "topic", topic); err != nil {
		return nil, err
	}
	res, err := k.s.do(ctx, OpKafkaTopicOffsets, protocol.TierT0, topicParams{Topic: topic})
	if err != nil {
		return nil, err
	}
	var out []clients.PartitionOffsets
	if err := decode(res, "offsets", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ConsumerGroups lists the cluster's consumer groups (cluster metadata, not
// filtered by the pipeline).
func (k *Kafka) ConsumerGroups(ctx context.Context) ([]string, error) {
	res, err := k.s.do(ctx, OpKafkaConsumerGroups, protocol.TierT0, nil)
	if err != nil {
		return nil, err
	}
	var out []string
	if err := decode(res, "groups", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GroupLag reads group's state, members and per-partition lag.
func (k *Kafka) GroupLag(ctx context.Context, group string) (clients.GroupLag, error) {
	if err := requireArg(OpKafkaGroupLag, "group", group); err != nil {
		return clients.GroupLag{}, err
	}
	res, err := k.s.do(ctx, OpKafkaGroupLag, protocol.TierT0, groupParams{Group: group})
	if err != nil {
		return clients.GroupLag{}, err
	}
	var out clients.GroupLag
	if err := decode(res, "lag", &out); err != nil {
		return clients.GroupLag{}, err
	}
	return out, nil
}

// FindByKey scans topic from the first offset at or after since for records
// whose key matches.
func (k *Kafka) FindByKey(ctx context.Context, topic, key string, since time.Time, maxScan int) ([]clients.RecordRef, error) {
	res, err := k.find(ctx, OpKafkaFindByKey, topic, key, since, maxScan)
	if err != nil {
		return nil, err
	}
	var out []clients.RecordRef
	if err := decode(res, "records", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// FindByKeyScan is FindByKey with the partitions the scan did not finish, so a
// caller never reads a cut scan as "the record is not there".
func (k *Kafka) FindByKeyScan(ctx context.Context, topic, key string, since time.Time, maxScan int) (clients.KeyScan, error) {
	res, err := k.find(ctx, OpKafkaFindByKeyScan, topic, key, since, maxScan)
	if err != nil {
		return clients.KeyScan{}, err
	}
	var out clients.KeyScan
	if err := decode(res, "scan", &out); err != nil {
		return clients.KeyScan{}, err
	}
	return out, nil
}

func (k *Kafka) find(ctx context.Context, op, topic, key string, since time.Time, maxScan int) (protocol.OpResult, error) {
	if err := requireArg(op, "topic", topic); err != nil {
		return protocol.OpResult{}, err
	}
	if err := requireArg(op, "key", key); err != nil {
		return protocol.OpResult{}, err
	}
	return k.s.do(ctx, op, protocol.TierT0, findParams{Topic: topic, Key: key, Since: since, MaxScan: maxScan})
}
