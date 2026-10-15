package kafkatools

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

// reportingKafka is the real client's shape: FindByKeyReport says which
// partitions max_scan cut short.
type reportingKafka struct {
	*fakeKafka
	scan kafka.KeyScan
}

func (r *reportingKafka) FindByKeyReport(ctx context.Context, t, k string, s time.Time, m int) (kafka.KeyScan, error) {
	return r.scan, nil
}

// TestFindByKeyReportsTruncation: "not found" after a max_scan cut used to
// read as "the key is absent"; the summary and Data must say the scan
// stopped early and where.
func TestFindByKeyReportsTruncation(t *testing.T) {
	for _, tt := range []struct {
		name string
		scan kafka.KeyScan
		want string
	}{
		{"not found, cut", kafka.KeyScan{Truncated: []int32{0, 2}}, "not found in the scanned window"},
		{"found, cut", kafka.KeyScan{
			Refs:      []clients.RecordRef{{Topic: "t", Partition: 0, Offset: 7, Timestamp: time.Now(), Key: "k"}},
			Truncated: []int32{0},
		}, "found 1 record(s)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fk := &reportingKafka{fakeKafka: &fakeKafka{}, scan: tt.scan}
			reg := tools.NewRegistry()
			if err := Register(reg, Deps{Kafka: fk}); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("kafka_find_by_key")
			r := runTool(t, tool, `{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z","max_scan":10}`)
			if !strings.Contains(r.Summary, tt.want) || !strings.Contains(r.Summary, "max_scan 10 stopped the scan before the end of partition(s) ") {
				t.Fatalf("summary %q: want %q and the truncation line", r.Summary, tt.want)
			}
			data, _ := r.Data.(map[string]any)
			if data["truncated"] != true || data["truncated_partitions"] == nil {
				t.Fatalf("data %+v: want truncated and truncated_partitions", data)
			}
		})
	}
}
