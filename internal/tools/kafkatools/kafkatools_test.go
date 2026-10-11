package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
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
	r, err := tool.Run(context.Background(), json.RawMessage(input))
	if err != nil {
		t.Fatalf("%s: %v", tool.Name(), err)
	}
	return r
}

// ---- tests ----

func TestConnectStatusCondensesTrace(t *testing.T) {
	fc := &fakeConnect{
		statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
			return clients.ConnectorStatus{
				Name:  "pg-orders-connector",
				Type:  "source",
				State: "RUNNING",
				Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: "org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler\n\tat x.java:1\nCaused by: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data\n\tat y.java:2\nCaused by: RestClientException: incompatible, details: [{errorType:'MISSING_UNION_BRANCH'}, {oldSchema: '{...FULL OLD SCHEMA JSON...}'}, {compatibility: 'BACKWARD'}]"}},
			}, nil
		},
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_status")
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)
	if !strings.Contains(r.Summary, "Task 0: FAILED") {
		t.Fatalf("summary %q", r.Summary)
	}
	if strings.Contains(r.Summary, "at x.java") || strings.Contains(r.Summary, "FULL OLD SCHEMA") {
		t.Fatalf("did not strip: %q", r.Summary)
	}
	for _, want := range []string{"ConnectException: Tolerance exceeded", "Caused by:", "MISSING_UNION_BRANCH"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q: %q", want, r.Summary)
		}
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
	for _, name := range []string{"connect_status", "connect_config", "schema_registry_versions", "schema_registry_diff", "kafka_topic_offsets", "kafka_consumer_lag", "kafka_find_by_key"} {
		tool, _ := reg.Get(name)
		if _, err := tool.Run(context.Background(), json.RawMessage(`{}`)); err == nil || !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s: want ErrBadInput, got %v", name, err)
		}
	}
}
