package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/tools"
)

// ---- fakes ----

type fakeConnect struct {
	listFn     func(context.Context) ([]string, error)
	statusFn   func(context.Context, string) (clients.ConnectorStatus, error)
	configFn   func(context.Context, string) (map[string]string, error)
	restartTFn func(context.Context, string, int) error
	restartCFn func(context.Context, string) error
}

func (f *fakeConnect) ListConnectors(ctx context.Context) ([]string, error) { return f.listFn(ctx) }
func (f *fakeConnect) Status(ctx context.Context, n string) (clients.ConnectorStatus, error) {
	return f.statusFn(ctx, n)
}
func (f *fakeConnect) Config(ctx context.Context, n string) (map[string]string, error) {
	return f.configFn(ctx, n)
}
func (f *fakeConnect) RestartTask(ctx context.Context, n string, t int) error {
	return f.restartTFn(ctx, n, t)
}
func (f *fakeConnect) RestartConnector(ctx context.Context, n string) error {
	return f.restartCFn(ctx, n)
}

type fakeRegistry struct {
	subjectsFn func(context.Context) ([]string, error)
	versionsFn func(context.Context, string) ([]int, error)
	schemaFn   func(context.Context, string, int) (clients.SchemaVersion, error)
	compatFn   func(context.Context, string) (string, bool, error)
	setFn      func(context.Context, string, string) error
	registerFn func(context.Context, string, string) (int, error)
}

func (f *fakeRegistry) Subjects(ctx context.Context) ([]string, error) { return f.subjectsFn(ctx) }
func (f *fakeRegistry) Versions(ctx context.Context, s string) ([]int, error) {
	return f.versionsFn(ctx, s)
}
func (f *fakeRegistry) Schema(ctx context.Context, s string, v int) (clients.SchemaVersion, error) {
	return f.schemaFn(ctx, s, v)
}
func (f *fakeRegistry) Compatibility(ctx context.Context, s string) (string, bool, error) {
	return f.compatFn(ctx, s)
}
func (f *fakeRegistry) SetCompatibility(ctx context.Context, s, l string) error {
	return f.setFn(ctx, s, l)
}
func (f *fakeRegistry) Register(ctx context.Context, s, sc string) (int, error) {
	return f.registerFn(ctx, s, sc)
}

type fakeKafka struct {
	topicsFn  func(context.Context) ([]string, error)
	offsetsFn func(context.Context, string) ([]clients.PartitionOffsets, error)
	groupsFn  func(context.Context) ([]string, error)
	lagFn     func(context.Context, string) (clients.GroupLag, error)
	findFn    func(context.Context, string, string, time.Time, int) ([]clients.RecordRef, error)
}

func (f *fakeKafka) Topics(ctx context.Context) ([]string, error) { return f.topicsFn(ctx) }
func (f *fakeKafka) TopicOffsets(ctx context.Context, t string) ([]clients.PartitionOffsets, error) {
	return f.offsetsFn(ctx, t)
}
func (f *fakeKafka) ConsumerGroups(ctx context.Context) ([]string, error) { return f.groupsFn(ctx) }
func (f *fakeKafka) GroupLag(ctx context.Context, g string) (clients.GroupLag, error) {
	return f.lagFn(ctx, g)
}
func (f *fakeKafka) FindByKey(ctx context.Context, t, k string, s time.Time, m int) ([]clients.RecordRef, error) {
	return f.findFn(ctx, t, k, s, m)
}

func runTool(t *testing.T, tool tools.Tool, input string) tools.Result {
	t.Helper()
	r, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(input))
	if err != nil {
		t.Fatalf("%s: %v", tool.Name(), err)
	}
	return r
}

// ---- tests ----

func TestConnectStatusCondensesTrace(t *testing.T) {
	trace := `org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java:220)
Caused by: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders
	at io.confluent.connect.avro.AvroConverter.fromConnectData(AvroConverter.java:96)
Caused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema{"type":"record","name":"Envelope","fields":[{"name":"id","type":"long"},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}}]}
	at io.confluent.kafka.serializers.AbstractKafkaAvroSerializer.serializeImpl(AbstractKafkaAvroSerializer.java:121)
Caused by: RestClientException: Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value", details: [{errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type'}, {oldSchemaVersion: 1}, {oldSchema: '{"type":"record","name":"Envelope","fields":[{"name":"id","type":"long"},{"name":"amount","type":"int"}]}'}, {compatibility: 'BACKWARD'}]; error code: 409`
	fc := &fakeConnect{
		statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
			return clients.ConnectorStatus{
				Name: "pg-orders-connector", Type: "source", State: "RUNNING",
				Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: trace}},
			}, nil
		},
	}
	fr := &fakeRegistry{
		schemaFn: func(ctx context.Context, s string, v int) (clients.SchemaVersion, error) {
			return clients.SchemaVersion{Subject: s, Version: 1, ID: 1,
				Schema: `{"type":"record","name":"Envelope","fields":[{"name":"id","type":"long"},{"name":"amount","type":"int"}]}`}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc, Registry: fr}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_status")
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)
	if !strings.Contains(r.Summary, "Task 0: FAILED") {
		t.Fatalf("summary %q", r.Summary)
	}
	if strings.Contains(r.Summary, "at x.java") || strings.Contains(r.Summary, "logicalType") || strings.Contains(r.Summary, `"type":"record"`) {
		t.Fatalf("did not strip: %q", r.Summary)
	}
	for _, want := range []string{"ConnectException: Tolerance exceeded", "Caused by:", "MISSING_UNION_BRANCH", "error code: 409"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q: %q", want, r.Summary)
		}
	}
	if !strings.Contains(r.Summary, "Rejected schema vs pg.public.orders-value v1") {
		t.Fatalf("missing rejected-schema diff: %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "amount: int → bytes(decimal 12,2)") {
		t.Fatalf("missing field change: %q", r.Summary)
	}
}

func TestConnectConfigRedacts(t *testing.T) {
	fc := &fakeConnect{
		configFn: func(ctx context.Context, n string) (map[string]string, error) {
			return map[string]string{
				"connector.class":        "io.debezium.connector.postgresql.PostgresConnector",
				"slot.name":              "debezium_orders",
				"key.converter":          "io.confluent.connect.avro.AvroConverter",
				"database.password":      "piperoom-dev",
				"database.hostname":      "postgres",
				"ssl.key.password":       "secretpw",
				"transforms":             "unwrap",
				"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
			}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_config")
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)
	if strings.Contains(r.Summary, "piperoom-dev") || strings.Contains(r.Summary, "secretpw") {
		t.Fatalf("leaked secret: %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "database.password: ***") {
		t.Fatalf("no redaction marker: %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "key.converter: io.confluent") {
		t.Fatalf("converter wrongly redacted: %q", r.Summary)
	}
}

func TestSchemaRegistryDiff(t *testing.T) {
	v1 := `{"type":"record","name":"Envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"amount","type":"int"}]}]},{"name":"after","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"amount","type":"int"}]}]}]}`
	v2 := `{"type":"record","name":"Envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}}]}]},{"name":"after","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}}]}]}]}`
	fr := &fakeRegistry{
		schemaFn: func(ctx context.Context, s string, v int) (clients.SchemaVersion, error) {
			if v == 1 {
				return clients.SchemaVersion{Subject: s, Version: 1, ID: 1, Schema: v1}, nil
			}
			return clients.SchemaVersion{Subject: s, Version: 2, ID: 2, Schema: v2}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Registry: fr}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("schema_registry_diff")
	r := runTool(t, tool, `{"subject":"pg.public.orders-value","from_version":1,"to_version":2}`)
	if !strings.Contains(r.Summary, "amount: int → bytes(decimal 12,2)") {
		t.Fatalf("diff summary %q", r.Summary)
	}
}

func TestBadInput(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"connect_status", "connect_config", "schema_registry_versions", "schema_registry_diff", "kafka_topic_offsets", "kafka_consumer_lag", "kafka_find_by_key", "kafka_topic_config"} {
		tool, _ := reg.Get(name)
		if _, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`)); err == nil || !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s: want ErrBadInput, got %v", name, err)
		}
	}
}

// v1Schema is the real pg.public.orders-value v1 registered schema (amount int),
// captured from the dev stack — the schema the failed connector was rejected against.
const v1Schema = `{"type":"record","name":"Envelope","namespace":"pg.public.orders","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":{"type":"long","connect.default":0},"default":0},{"name":"customer_id","type":"long"},{"name":"amount","type":"int"},{"name":"status","type":{"type":"string","connect.default":"placed"},"default":"placed"},{"name":"updated_at","type":{"type":"string","connect.version":1,"connect.default":"1970-01-01T00:00:00.000000Z","connect.name":"io.debezium.time.ZonedTimestamp"},"default":"1970-01-01T00:00:00.000000Z"}],"connect.name":"pg.public.orders.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.postgresql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,first,first_in_data_collection,last_in_data_collection,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"ts_us","type":["null","long"],"default":null},{"name":"ts_ns","type":["null","long"],"default":null},{"name":"schema","type":"string"},{"name":"table","type":"string"},{"name":"txId","type":["null","long"],"default":null},{"name":"lsn","type":["null","long"],"default":null},{"name":"xmin","type":["null","long"],"default":null}],"connect.version":1,"connect.name":"io.debezium.connector.postgresql.Source"}},{"name":"transaction","type":["null",{"type":"record","name":"block","namespace":"event","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}],"connect.version":1,"connect.name":"event.block"}],"default":null},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"ts_us","type":["null","long"],"default":null},{"name":"ts_ns","type":["null","long"],"default":null}],"connect.version":2,"connect.name":"pg.public.orders.Envelope"}`

// TestConnectStatusRealFixture runs connect_status through the real Connect
// client against the real captured FAILED status body (10.4 KB, 9.3 KB trace)
// and a registry fake serving the real v1 — the task's "Done when" path.
func TestConnectStatusRealFixture(t *testing.T) {
	body, err := os.ReadFile("../../clients/connect/testdata/status_failed.json")
	if err != nil {
		t.Fatalf("read status_failed.json: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/connectors/pg-orders-connector/status" {
			http.NotFound(w, r)
			return
		}
		w.Write(body)
	}))
	defer srv.Close()

	fr := &fakeRegistry{
		schemaFn: func(ctx context.Context, s string, v int) (clients.SchemaVersion, error) {
			return clients.SchemaVersion{Subject: s, Version: 1, ID: 2, Schema: v1Schema}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: connect.New(srv.URL, srv.Client()), Registry: fr}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_status")
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)

	if len(r.Summary) > 2000 {
		t.Fatalf("summary %d chars > 2000", len(r.Summary))
	}
	for _, want := range []string{"Task 0: FAILED", "incompatible", "error code: 409", "MISSING_UNION_BRANCH", "amount: int → bytes(decimal 12,2)"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, r.Summary)
		}
	}
	for _, banned := range []string{`"type":"record"`, "logicalType", "at org.apache.kafka.connect", "connect.default"} {
		if strings.Contains(r.Summary, banned) {
			t.Fatalf("summary should not contain %q:\n%s", banned, r.Summary)
		}
	}
}

func TestConnectListConnectors(t *testing.T) {
	fc := &fakeConnect{
		listFn: func(context.Context) ([]string, error) { return []string{"a", "b"}, nil },
		statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
			if n == "b" {
				return clients.ConnectorStatus{Name: "b", Type: "sink", State: "RUNNING",
					Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED"}, {ID: 1, State: "RUNNING"}}}, nil
			}
			return clients.ConnectorStatus{}, errors.New("down")
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_list_connectors")
	r := runTool(t, tool, `{}`)
	if !strings.Contains(r.Summary, "a (): UNKNOWN") {
		t.Fatalf("summary %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "b (sink): RUNNING, 1 failed tasks") {
		t.Fatalf("summary %q", r.Summary)
	}
}

func TestSchemaRegistrySubjects(t *testing.T) {
	fr := &fakeRegistry{
		subjectsFn: func(context.Context) ([]string, error) {
			return []string{"pg.public.orders-value", "pg.public.orders-key", "other-value"}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Registry: fr}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("schema_registry_subjects")
	r := runTool(t, tool, `{"prefix":"pg.public."}`)
	if strings.Contains(r.Summary, "other") {
		t.Fatalf("prefix filter failed: %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "orders-key") || !strings.Contains(r.Summary, "orders-value") {
		t.Fatalf("summary %q", r.Summary)
	}
}

func TestSchemaRegistryVersions(t *testing.T) {
	fr := &fakeRegistry{
		versionsFn: func(context.Context, string) ([]int, error) { return []int{1, 2}, nil },
		schemaFn: func(ctx context.Context, s string, v int) (clients.SchemaVersion, error) {
			return clients.SchemaVersion{Subject: s, Version: 2, ID: 3, Schema: "{}"}, nil
		},
		compatFn: func(context.Context, string) (string, bool, error) { return "BACKWARD", true, nil },
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Registry: fr}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("schema_registry_versions")
	r := runTool(t, tool, `{"subject":"pg.public.orders-value"}`)
	for _, want := range []string{"versions [1 2]", "latest id 3", "BACKWARD", "subject"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q: %q", want, r.Summary)
		}
	}
}

func TestKafkaTopicOffsets(t *testing.T) {
	fk := &fakeKafka{
		offsetsFn: func(ctx context.Context, topic string) ([]clients.PartitionOffsets, error) {
			if topic == "nope" {
				return nil, clients.ErrNotFound
			}
			return []clients.PartitionOffsets{
				{Partition: 0, Start: 0, End: 100, LastTimestamp: time.Now().Add(-5 * time.Minute)},
			}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: fk}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("kafka_topic_offsets")
	r := runTool(t, tool, `{"topic":"t"}`)
	if !strings.Contains(r.Summary, "partition 0: 0–100 (100 records)") || !strings.Contains(r.Summary, "newest 5 min ago") {
		t.Fatalf("summary %q", r.Summary)
	}
	if _, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"topic":"nope"}`)); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestKafkaConsumerLag(t *testing.T) {
	fk := &fakeKafka{
		lagFn: func(ctx context.Context, g string) (clients.GroupLag, error) {
			return clients.GroupLag{Group: g, State: "Stable", Members: 2, TotalLag: 1200, Partitions: []clients.PartitionLag{
				{Topic: "t", Partition: 0, Committed: 10, End: 110, Lag: 100},
				{Topic: "t", Partition: 2, Committed: 0, End: 500, Lag: 500},
				{Topic: "t", Partition: 1, Committed: 40, End: 240, Lag: 200},
				{Topic: "t", Partition: 3, Committed: 100, End: 130, Lag: 30},
			}}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: fk}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("kafka_consumer_lag")
	r := runTool(t, tool, `{"group":"g"}`)
	if !strings.Contains(r.Summary, "total lag 1200") {
		t.Fatalf("summary %q", r.Summary)
	}
	// worst-5 must be sorted descending: partition 2 (500) before 1 (200) before 0 (100).
	i2, i1, i0 := strings.Index(r.Summary, "t/2"), strings.Index(r.Summary, "t/1"), strings.Index(r.Summary, "t/0")
	if !(i2 < i1 && i1 < i0) {
		t.Fatalf("worst partitions not sorted: %q", r.Summary)
	}
}

func TestKafkaListGroups(t *testing.T) {
	fk := &fakeKafka{
		groupsFn: func(context.Context) ([]string, error) { return []string{"g1", "g2"}, nil },
		lagFn: func(ctx context.Context, g string) (clients.GroupLag, error) {
			if g == "g1" {
				return clients.GroupLag{Group: g, State: "Stable", Partitions: []clients.PartitionLag{{Topic: "orders"}}}, nil
			}
			return clients.GroupLag{Group: g, State: "Stable", Partitions: []clients.PartitionLag{{Topic: "other"}}}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: fk}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("kafka_list_groups")
	r := runTool(t, tool, `{"topic":"orders"}`)
	if !strings.Contains(r.Summary, "g1: Stable") || strings.Contains(r.Summary, "g2") {
		t.Fatalf("topic filter failed: %q", r.Summary)
	}
}

func TestKafkaFindByKey(t *testing.T) {
	fk := &fakeKafka{
		findFn: func(ctx context.Context, topic, key string, since time.Time, max int) ([]clients.RecordRef, error) {
			if key == "missing" {
				return nil, nil
			}
			return []clients.RecordRef{{Topic: topic, Partition: 0, Offset: 7, Timestamp: time.Now(), Key: key}}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: fk}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("kafka_find_by_key")
	r := runTool(t, tool, `{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z"}`)
	if !strings.Contains(r.Summary, "found 1 record(s)") {
		t.Fatalf("summary %q", r.Summary)
	}
	r = runTool(t, tool, `{"topic":"t","key":"missing","since":"2026-10-11T00:00:00Z"}`)
	if !strings.Contains(r.Summary, "not found") {
		t.Fatalf("summary %q", r.Summary)
	}
}

// TestToolRespectsCancelledContext asserts a tool returns promptly with the
// context error when the caller cancels mid-flight.
func TestToolRespectsCancelledContext(t *testing.T) {
	blocked := make(chan struct{})
	fk := &fakeKafka{
		findFn: func(ctx context.Context, topic, key string, since time.Time, max int) ([]clients.RecordRef, error) {
			close(blocked)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: fk}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("kafka_find_by_key")
	ctx, cancel := context.WithCancel(tools.WithUnscoped(context.Background()))
	done := make(chan error, 1)
	go func() {
		_, err := tool.Run(ctx, json.RawMessage(`{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z"}`))
		done <- err
	}()
	<-blocked
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tool did not return after cancel")
	}
}
