package kafkatools

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// TestConsumerLagNoCommit: -1 is kadm's "no committed offset", not an
// offset; printed as "committed -1" it reads as a position.
func TestConsumerLagNoCommit(t *testing.T) {
	k := &fakeKafka{lagFn: func(context.Context, string) (clients.GroupLag, error) {
		return clients.GroupLag{Group: "g", State: "Stable", Members: 1, TotalLag: 40,
			Partitions: []clients.PartitionLag{{Topic: "t", Partition: 0, Committed: -1, End: 40, Lag: 40}}}, nil
	}}
	r, err := (&consumerLag{d: Deps{Kafka: k}}).Run(tools.WithUnscoped(context.Background()), []byte(`{"group":"g"}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.Summary, "committed -1") || !strings.Contains(r.Summary, "no committed offset") {
		t.Fatalf("summary %q: want the no-commit case named", r.Summary)
	}
}

// TestRateWindowLabel: a 90 s window was labelled "last 1m" while the rate
// divided by 90 s.
func TestRateWindowLabel(t *testing.T) {
	for d, want := range map[time.Duration]string{90 * time.Second: "last 1m30s", 5 * time.Minute: "last 5m", time.Hour: "last 1h"} {
		if s := rateSummary("t", d, []clients.PartitionRate{{Partition: 0, Records: 90}}); !strings.Contains(s, want+":") {
			t.Errorf("%s window: %q, want %q", d, s, want)
		}
	}
}
