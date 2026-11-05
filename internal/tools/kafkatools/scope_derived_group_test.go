package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/tools"
)

// TC2 (KAN-459): the read tools see the consumer group the topology derived
// from the pipeline's own ClickHouse engine table (kafka_group_name), while the
// write guard keeps refusing it (internal/tools/scope_derived_test.go).
func TestReadToolsAcceptDerivedConsumerGroup(t *testing.T) {
	d, c := scopeFakes(t)
	const chGroup = "piperoom-clickhouse-orders"
	fk := d.Kafka.(*fakeKafkaConfig)
	fk.groupsFn = func(context.Context) ([]string, error) {
		c.add("kafka.groups")
		return []string{"order-sync", "bill-sync", chGroup, "other-group"}, nil
	}

	// Before a topology build recorded the group, the read tool refuses it
	// before any client call (fail closed).
	_, err := scopeTool(t, d, "kafka_consumer_lag").Run(roomCtx(), json.RawMessage(`{"group":"`+chGroup+`"}`))
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("before a build: err = %v, want ErrOutOfScope", err)
	}
	if c.n() != 0 {
		t.Fatalf("a refused group reached %d client calls", c.n())
	}

	d.Owners.SetDerivedConsumerGroups("orders", []string{chGroup})

	r, err := scopeTool(t, d, "kafka_consumer_lag").Run(roomCtx(), json.RawMessage(`{"group":"`+chGroup+`"}`))
	if err != nil {
		t.Fatalf("kafka_consumer_lag refused the derived group: %v", err)
	}
	if !strings.Contains(r.Summary, chGroup+": Stable") || !c.has("kafka.lag:"+chGroup) {
		t.Fatalf("kafka_consumer_lag summary = %q, lag call = %v", r.Summary, c.has("kafka.lag:"+chGroup))
	}

	r, err = scopeTool(t, d, "kafka_list_groups").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("kafka_list_groups: %v", err)
	}
	for _, want := range []string{"order-sync: Stable", chGroup + ": Stable", "2 other consumer groups not shown"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("kafka_list_groups summary = %q, want %q", r.Summary, want)
		}
	}
	if strings.Contains(r.Summary, "bill-sync") || strings.Contains(r.Summary, "other-group") {
		t.Fatalf("kafka_list_groups names a group outside the pipeline: %q", r.Summary)
	}
	if c.has("kafka.lag:bill-sync") {
		t.Fatal("kafka_list_groups read the lag of another pipeline's group")
	}

	// Another pipeline's group is still refused by the read tool.
	if _, err := scopeTool(t, d, "kafka_consumer_lag").Run(roomCtx(), json.RawMessage(`{"group":"bill-sync"}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("other pipeline's group: err = %v, want ErrOutOfScope", err)
	}
}
