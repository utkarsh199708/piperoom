package kafkatools

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
)

// fakeScanKafka adds the optional FindByKeyReport to fakeKafka.
type fakeScanKafka struct {
	fakeKafka
	rep kafka.KeyScan
}

func (f *fakeScanKafka) FindByKeyReport(ctx context.Context, t, k string, s time.Time, m int) (kafka.KeyScan, error) {
	return f.rep, nil
}

func TestFindByKeyReportsTruncation(t *testing.T) {
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	fk := &fakeScanKafka{rep: kafka.KeyScan{
		Refs: []clients.RecordRef{
			{Topic: "t", Partition: 0, Offset: 2, Timestamp: t0, Key: "b"},
			{Topic: "t", Partition: 0, Offset: 3, Timestamp: t0.Add(time.Second), Key: "b"},
		},
		Truncated: []int32{0},
	}}
	r := runTool(t, toolFor(t, Deps{Kafka: fk}, "kafka_find_by_key"), `{"topic":"t","key":"b","since":"2026-10-14T00:00:00Z","max_scan":4}`)
	if strings.Contains(r.Summary, "(newest") {
		t.Fatalf("truncated scan labelled newest: %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "scan stopped at max_scan before partition end") ||
		!strings.Contains(r.Summary, "newer matches may exist") {
		t.Fatalf("summary does not report truncation: %q", r.Summary)
	}
	d := r.Data.(map[string]any)
	if d["truncated"] != true {
		t.Fatalf("data truncated = %v", d["truncated"])
	}
	if _, ok := d["newest"]; ok {
		t.Fatalf("data has newest on a truncated scan: %v", d["newest"])
	}
}

// TestFindByKeyNewestByOffset: offset 1 written after offset 0 but with an
// older producer timestamp is still the newest occurrence.
func TestFindByKeyNewestByOffset(t *testing.T) {
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	fk := &fakeKafka{findFn: func(ctx context.Context, topic, key string, since time.Time, max int) ([]clients.RecordRef, error) {
		return []clients.RecordRef{
			{Topic: topic, Partition: 0, Offset: 0, Timestamp: now, Key: key},
			{Topic: topic, Partition: 0, Offset: 1, Timestamp: now.Add(-time.Hour), Key: key},
		}, nil
	}}
	r := runTool(t, toolFor(t, Deps{Kafka: fk}, "kafka_find_by_key"), `{"topic":"t","key":"k","since":"2026-10-14T00:00:00Z"}`)
	if !strings.Contains(r.Summary, "offset 1, 2026-10-14T08:00:00Z (newest)") ||
		strings.Contains(r.Summary, "offset 0, 2026-10-14T09:00:00Z (newest)") {
		t.Fatalf("summary %q", r.Summary)
	}
	if n := r.Data.(map[string]any)["newest"].(clients.RecordRef); n.Offset != 1 {
		t.Fatalf("data newest = %+v", n)
	}
}

// TestFindByKeyNewestPerPartition: with matches in two partitions each
// partition's last offset is labelled, and only that one.
func TestFindByKeyNewestPerPartition(t *testing.T) {
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	fk := &fakeKafka{findFn: func(ctx context.Context, topic, key string, since time.Time, max int) ([]clients.RecordRef, error) {
		return []clients.RecordRef{
			{Topic: topic, Partition: 0, Offset: 5, Timestamp: t0, Key: key},
			{Topic: topic, Partition: 1, Offset: 8, Timestamp: t0.Add(time.Minute), Key: key},
			{Topic: topic, Partition: 0, Offset: 4, Timestamp: t0.Add(-time.Minute), Key: key},
		}, nil
	}}
	r := runTool(t, toolFor(t, Deps{Kafka: fk}, "kafka_find_by_key"), `{"topic":"t","key":"k","since":"2026-10-14T00:00:00Z"}`)
	if !strings.Contains(r.Summary, "partition 0, offset 5, 2026-10-14T09:00:00Z (newest in partition 0)") ||
		!strings.Contains(r.Summary, "partition 1, offset 8, 2026-10-14T09:01:00Z (newest in partition 1)") ||
		strings.Contains(r.Summary, "offset 4, 2026-10-14T08:59:00Z (newest") {
		t.Fatalf("summary %q", r.Summary)
	}
}
