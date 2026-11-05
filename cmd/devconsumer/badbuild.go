package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// rejection is a record the build refuses to load. It goes to the DLQ instead
// of crashing the consumer.
type rejection struct {
	id     int64
	reason string
}

func (r *rejection) Error() string {
	return fmt.Sprintf("devconsumer: order %d rejected: %s", r.id, r.reason)
}

// check applies the build's validation: with REJECT_STATUS set, an order in
// that status is rejected. Deletes carry no row and always pass.
func check(c change, o options) error {
	if o.rejectStatus == "" || c.After == nil || c.After.Status != o.rejectStatus {
		return nil
	}
	return &rejection{id: c.ID, reason: fmt.Sprintf("unknown status %q", c.After.Status)}
}

// validOps are the Debezium op codes.
var validOps = []string{"c", "u", "d", "r"}

// parseSkipOps reads SKIP_OPS, e.g. "c" or "c,u". Empty means none.
func parseSkipOps(raw string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, f := range strings.Split(raw, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if !slices.Contains(validOps, f) {
			return nil, fmt.Errorf("bad SKIP_OPS entry %q: want a Debezium op (c, u, d, r)", f)
		}
		out[f] = true
	}
	return out, nil
}

// joinOps prints the skipped ops in a stable order for the start log line.
func joinOps(ops map[string]bool) string {
	var out []string
	for _, op := range validOps {
		if ops[op] {
			out = append(out, op)
		}
	}
	return strings.Join(out, ",")
}

// Header names of Kafka Connect's DLQ context headers
// (errors.deadletterqueue.context.headers.enable). devconsumer writes the same
// ones, so the dlq_peek tool and dlq_replay treat its DLQ like a sink
// connector's.
const (
	hdrTopic     = "__connect.errors.topic"
	hdrPartition = "__connect.errors.partition"
	hdrOffset    = "__connect.errors.offset"
	hdrClass     = "__connect.errors.exception.class.name"
	hdrMessage   = "__connect.errors.exception.message"
)

// dlqRecord is the original record (key, value, headers) for the DLQ topic,
// plus the error context headers.
func dlqRecord(r *kgo.Record, topic string, rj *rejection) *kgo.Record {
	hs := append([]kgo.RecordHeader(nil), r.Headers...)
	hs = append(hs,
		kgo.RecordHeader{Key: hdrTopic, Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: hdrPartition, Value: []byte(strconv.FormatInt(int64(r.Partition), 10))},
		kgo.RecordHeader{Key: hdrOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: hdrClass, Value: []byte("devconsumer.ValidationError")},
		kgo.RecordHeader{Key: hdrMessage, Value: []byte(rj.reason)},
	)
	return &kgo.Record{Topic: topic, Key: r.Key, Value: r.Value, Headers: hs}
}

// sendDLQ produces the rejected record and waits for the ack.
func sendDLQ(ctx context.Context, cl *kgo.Client, topic string, r *kgo.Record, rj *rejection) error {
	if err := cl.ProduceSync(ctx, dlqRecord(r, topic, rj)).FirstErr(); err != nil {
		return fmt.Errorf("devconsumer: produce rejected record %s/%d@%d to %s: %w", r.Topic, r.Partition, r.Offset, topic, err)
	}
	return nil
}
