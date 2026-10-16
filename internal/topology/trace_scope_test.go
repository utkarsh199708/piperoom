package topology

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// With Connect unreachable the trace cannot ask which connector streams the
// table, and used to read any table of the source database (and its key in
// the sink): a row-existence oracle from POST /api/rooms/{id}/trace (QA
// sweep, "trace leaves pipeline scope"). The table must then be one of the
// pipeline's source tables.
func TestTraceConnectDownStaysInScope(t *testing.T) {
	src := &fakeSource{rows: map[string]map[string]any{
		"public.orders|7":  {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)},
		"public.payroll|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)},
	}}
	b := NewBuilder(
		testOwners(),
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("EOF")}},
		&fakeKafka{},
		src,
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.payroll", "7")
	if !errors.Is(err, ErrBadInput) {
		t.Fatalf("out-of-pipeline table with Connect down: stages %+v, err %v; want ErrBadInput", stages, err)
	}
	if _, err := b.Trace(context.Background(), "orders", "public.orders", "7"); err != nil {
		t.Fatalf("the pipeline's own table must still trace: %v", err)
	}
}

// With Connect down the trace used the bare table name as the topic, so the
// Kafka stage looked in a topic that does not exist and came back unknown.
// owners.yaml requires topic_prefix: the Debezium default name
// <topic_prefix>.<table> is the better guess (QA sweep).
func TestTraceConnectDownUsesTopicPrefix(t *testing.T) {
	o := testOwners()
	o.Pipelines[0].TopicPrefix = "pg"
	b := NewBuilder(
		o,
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("EOF")}},
		&fakeKafka{findByKey: map[string][]clients.RecordRef{"pg.public.orders": {{Key: "7", Partition: 0, Offset: 3}}}},
		&fakeSource{rows: map[string]map[string]any{"public.orders|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatal(err)
	}
	if stages[2].Name != "pg.public.orders" || stages[2].Status != "seen" {
		t.Fatalf("kafka stage = %+v, want pg.public.orders seen", stages[2])
	}
}

// With Connect up, a connector config with no table.include.list streams
// every table of the database, so the connector check alone let a trace read
// any of them: the oracle of #381. owners.yaml's source tables decide.
func TestTraceConnectUpStaysInScope(t *testing.T) {
	now := time.Now().Format(time.RFC3339Nano)
	b := NewBuilder(
		testOwners(),
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "schema.include.list": "public"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
		},
		&fakeKafka{},
		&fakeSource{rows: map[string]map[string]any{
			"public.orders|7":  {"id": "7", "updated_at": now},
			"public.payroll|7": {"id": "7", "updated_at": now},
		}},
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.payroll", "7")
	if !errors.Is(err, ErrBadInput) {
		t.Fatalf("out-of-pipeline table with Connect up: stages %+v, err %v; want ErrBadInput", stages, err)
	}
	if _, err := b.Trace(context.Background(), "orders", "public.orders", "7"); err != nil {
		t.Fatalf("the pipeline's own table must still trace: %v", err)
	}
}
