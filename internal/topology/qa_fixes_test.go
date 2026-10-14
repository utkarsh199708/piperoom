package topology

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// QA sweep 2026-10-14 (qa4, qa6) regressions.

func nodeByID(t *testing.T, g Graph, id string) Node {
	t.Helper()
	for _, n := range g.Nodes {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("node %s missing: %+v", id, g.Nodes)
	return Node{}
}

// A sink row stamped in the future (clock skew, a timestamp without time
// zone read in the wrong zone, a bad row) must not read as "ok, 0s".
func TestSinkHealthFutureTimestamp(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	th := DefaultThresholds()

	h, facts := sinkHealth(now.Add(5*time.Hour), now, th)
	if h == HealthOK {
		t.Fatalf("future timestamp health = ok, facts %v", facts)
	}
	if facts["future_timestamp"] == "" {
		t.Fatalf("facts %v lack future_timestamp", facts)
	}
	if facts["stale_for"] == "0s" {
		t.Fatalf("future timestamp reads as fresh: %v", facts)
	}

	// Skew under the tolerance is normal and still reads as fresh.
	h, facts = sinkHealth(now.Add(10*time.Second), now, th)
	if h != HealthOK || facts["future_timestamp"] != "" {
		t.Fatalf("small skew: health %s facts %v, want ok without future_timestamp", h, facts)
	}
}

func TestTopicHealthFutureTimestamp(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	h, facts := topicHealth(nil, now.Add(2*time.Hour), now)
	if h != HealthWarn || facts["future_timestamp"] == "" {
		t.Fatalf("health %s facts %v, want warn with future_timestamp", h, facts)
	}
}

// With Connect down, the topic node uses owners.yaml's topic_prefix (the real
// name) and Kafka is still asked about it.
func TestBuildConnectDownUsesTopicPrefix(t *testing.T) {
	o := testOwners()
	o.Pipelines[0].TopicPrefix = "pg"
	b := NewBuilder(o,
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("down")}},
		&fakeKafka{offsets: map[string][]clients.PartitionOffsets{
			"pg.public.orders": {{Partition: 0, Start: 0, End: 5, LastTimestamp: time.Now().Add(-time.Minute)}},
		}},
		&fakeSource{},
		&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": time.Now()}},
	)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	n := nodeByID(t, g, "topic:pg.public.orders")
	if n.Health != HealthOK || n.Facts["error"] != "" {
		t.Fatalf("topic node = %+v, want ok (Kafka answered)", n)
	}
	for _, nd := range g.Nodes {
		if nd.ID == "topic:public.orders" {
			t.Fatalf("placeholder topic named after the table: %+v", g.Nodes)
		}
	}
	// The edge into it is still only inferred: the connector config is unread.
	for _, e := range g.Edges {
		if e.To == "topic:pg.public.orders" && !e.IsInferred() {
			t.Fatalf("edge %+v should be inferred", e)
		}
	}
}

func TestBuildMissingResourcesAreMissingNotUnreachable(t *testing.T) {
	o := testOwners()
	o.Pipelines[0].TopicPrefix = "pg"
	nf := fmt.Errorf("x: %w", clients.ErrNotFound)
	b := NewBuilder(o,
		&fakeConnect{config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg"}}},
		&fakeKafka{err: map[string]error{"topic:pg.public.orders": nf, "group:order-sync": nf}},
		&fakeSource{},
		&fakeWarehouse{freshErr: map[string]error{"analytics.orders": nf}},
	)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"topic:pg.public.orders", "group:order-sync", "wh:analytics.orders"} {
		n := nodeByID(t, g, id)
		if n.Facts["missing"] != "true" || n.Facts["error"] != "" {
			t.Fatalf("%s facts = %v, want missing=true and no error", id, n.Facts)
		}
		if n.Health != HealthFail && n.Health != HealthWarn {
			t.Fatalf("%s health = %s, want fail or warn", id, n.Health)
		}
	}
}

// QA6: with Connect unreachable, Trace must not read arbitrary tables (a row
// existence oracle); only the pipeline's source tables are traceable.
func TestTraceConnectDownRefusesTablesOutsidePipeline(t *testing.T) {
	src := &fakeSource{rows: map[string]map[string]any{
		"piperoom_audit.ddl_log|1": {"id": "1"},
		"public.secrets|1":         {"id": "1"},
	}}
	b := NewBuilder(testOwners(),
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("down")}},
		&fakeKafka{}, src, &fakeWarehouse{})
	for _, tbl := range []string{"piperoom_audit.ddl_log", "public.secrets", ""} {
		if _, err := b.Trace(context.Background(), "orders", tbl, "1"); !errors.Is(err, ErrBadInput) {
			t.Fatalf("Trace(%q) err = %v, want ErrBadInput", tbl, err)
		}
	}
	// A pipeline table still traces, with the topic_prefix name when known.
	o := testOwners()
	o.Pipelines[0].TopicPrefix = "pg"
	b = NewBuilder(o,
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("down")}},
		&fakeKafka{}, &fakeSource{}, &fakeWarehouse{})
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "1")
	if err != nil {
		t.Fatalf("Trace pipeline table: %v", err)
	}
	if stages[2].Name != "pg.public.orders" {
		t.Fatalf("topic stage = %q, want pg.public.orders", stages[2].Name)
	}
}

func TestDecideTraceMissingStages(t *testing.T) {
	nf := fmt.Errorf("x: %w", clients.ErrNotFound)

	f := traceFactsOK()
	f.groupErr, f.groupCommitted = nf, -1
	st := decideTrace(f)
	if st[3].Status != "stopped_here" || st[3].Facts["missing"] != "true" {
		t.Fatalf("missing group stage = %+v", st[3])
	}

	f = traceFactsOK()
	f.sinkErr, f.sinkFound = nf, false
	st = decideTrace(f)
	if st[4].Status != "stopped_here" || st[4].Facts["missing"] != "true" {
		t.Fatalf("missing sink stage = %+v", st[4])
	}

	f = traceFactsOK()
	f.sinkErr, f.sinkFound = errors.New("down"), false
	st = decideTrace(f)
	if st[4].Status != "unknown" || st[4].Facts["error"] != "unreachable" {
		t.Fatalf("unreachable sink stage = %+v", st[4])
	}
}

// Even with Connect up, a table a connector happens to stream but owners.yaml
// does not list is outside the pipeline.
func TestTraceConnectUpRefusesTablesOutsidePipeline(t *testing.T) {
	b := NewBuilder(testOwners(),
		&fakeConnect{config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg"}}},
		&fakeKafka{}, &fakeSource{}, &fakeWarehouse{})
	if _, err := b.Trace(context.Background(), "orders", "public.secrets", "1"); !errors.Is(err, ErrBadInput) {
		t.Fatalf("err = %v, want ErrBadInput", err)
	}
}
