package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

func toolFor(t *testing.T, d Deps, name string) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get(name)
	if !ok {
		t.Fatalf("no tool %s", name)
	}
	return tool
}

func runErr(tool tools.Tool, input string) (tools.Result, error) {
	return tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(input))
}

// QA2's case: the latest registered version has amount int, note and x; the
// raw schema changes amount to decimal and drops note and x.
const (
	qa2Latest = `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"amount","type":"int"},{"name":"note","type":"string"},{"name":"x","type":"int"}]}`
	qa2V1     = `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"}]}`
	qa2Raw    = `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}}]}`
)

func qa2Registry() *fakeRegistry {
	return &fakeRegistry{schemaFn: func(ctx context.Context, s string, v int) (clients.SchemaVersion, error) {
		switch v {
		case 1:
			return clients.SchemaVersion{Subject: s, Version: 1, ID: 1, Schema: qa2V1}, nil
		case -1, 3:
			return clients.SchemaVersion{Subject: s, Version: 3, ID: 3, Schema: qa2Latest}, nil
		}
		return clients.SchemaVersion{}, clients.ErrNotFound
	}}
}

func TestRegistryDiffRawSchemaIsTheToSide(t *testing.T) {
	tool := toolFor(t, Deps{Registry: qa2Registry()}, "schema_registry_diff")

	// from_version omitted: latest (v3) → raw.
	r := runTool(t, tool, `{"subject":"pg.public.orders-value","schema":`+jsonString(qa2Raw)+`}`)
	for _, want := range []string{"Value.amount: int → bytes(decimal 12,2)", "- Value.note: string", "- Value.x: int"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary %q lacks %q", r.Summary, want)
		}
	}
	if strings.Contains(r.Summary, "+ Value.note") {
		t.Fatalf("diff is reversed: %q", r.Summary)
	}
	data := r.Data.(map[string]any)
	if data["from_version"] != 3 || data["to_version"] != 0 {
		t.Fatalf("versions in data: %+v", data)
	}

	// from_version honoured: v1 → raw adds amount.
	r = runTool(t, tool, `{"subject":"pg.public.orders-value","from_version":1,"schema":`+jsonString(qa2Raw)+`}`)
	if !strings.Contains(r.Summary, "+ Value.amount: bytes(decimal 12,2)") || strings.Contains(r.Summary, "note") {
		t.Fatalf("from_version ignored: %q", r.Summary)
	}

	// Two versions with from defaulting to latest.
	r = runTool(t, tool, `{"subject":"pg.public.orders-value","to_version":1}`)
	if !strings.Contains(r.Summary, "- Value.amount: int") {
		t.Fatalf("latest → v1: %q", r.Summary)
	}
}

func TestRegistryDiffBadInput(t *testing.T) {
	tool := toolFor(t, Deps{Registry: qa2Registry()}, "schema_registry_diff")
	tests := []struct {
		name, input, msg string
	}{
		{"wrong type", `{"subject":"s","from_version":"1"}`, "invalid input"},
		{"subject wrong type", `{"subject":5}`, "invalid input"},
		{"missing subject", `{}`, "subject is required"},
		{"negative version", `{"subject":"s","from_version":-2}`, "positive"},
		{"schema and to_version", `{"subject":"s","to_version":3,"schema":"{}"}`, "either to_version or schema"},
		{"raw not JSON", `{"subject":"s","schema":"not json"}`, "not a valid Avro schema"},
		{"raw not Avro", `{"subject":"s","schema":"{\"foo\":1}"}`, "not a valid Avro schema"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runErr(tool, tt.input)
			if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), tt.msg) {
				t.Fatalf("err = %v, want ErrBadInput with %q", err, tt.msg)
			}
		})
	}
}

// typedFake adds the registry's schemaType like the real client does.
type typedFake struct {
	*fakeRegistry
	typ string
}

func (f typedFake) SchemaTyped(ctx context.Context, s string, v int) (clients.SchemaVersion, string, error) {
	sv, err := f.Schema(ctx, s, v)
	return sv, f.typ, err
}

func TestRegistryDiffJSONSchema(t *testing.T) {
	v1 := `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"id":{"type":"integer"},"amount":{"type":"integer"},"note":{"type":"string"},"addr":{"type":"object","properties":{"zip":{"type":"string"}}}},"required":["id"]}`
	v2 := `{"type":"object","properties":{"id":{"type":"integer"},"amount":{"type":"number"},"email":{"type":"string","format":"email"},"addr":{"type":"object","properties":{"zip":{"type":"integer"}}}},"required":["id","amount"]}`
	fr := &fakeRegistry{schemaFn: func(ctx context.Context, s string, v int) (clients.SchemaVersion, error) {
		if v == 1 {
			return clients.SchemaVersion{Subject: s, Version: 1, Schema: v1}, nil
		}
		return clients.SchemaVersion{Subject: s, Version: 2, Schema: v2}, nil
	}}
	for name, reg := range map[string]clients.SchemaRegistryClient{
		"typed":   typedFake{fr, "JSON"},
		"sniffed": fr,
	} {
		t.Run(name, func(t *testing.T) {
			tool := toolFor(t, Deps{Registry: reg}, "schema_registry_diff")
			r := runTool(t, tool, `{"subject":"s","from_version":1,"to_version":2}`)
			for _, want := range []string{
				"amount: integer → number required",
				"+ email: string(email)",
				"- note: string",
				"addr.zip: string → integer",
			} {
				if !strings.Contains(r.Summary, want) {
					t.Fatalf("summary %q lacks %q", r.Summary, want)
				}
			}
		})
	}
}

func TestRegistryDiffProtobufNotSupported(t *testing.T) {
	proto := "syntax = \"proto3\";\nmessage Order { int64 id = 1; }"
	fr := &fakeRegistry{schemaFn: func(ctx context.Context, s string, v int) (clients.SchemaVersion, error) {
		return clients.SchemaVersion{Subject: s, Version: v, Schema: proto}, nil
	}}
	for name, reg := range map[string]clients.SchemaRegistryClient{
		"typed":   typedFake{fr, "PROTOBUF"},
		"sniffed": fr,
	} {
		t.Run(name, func(t *testing.T) {
			tool := toolFor(t, Deps{Registry: reg}, "schema_registry_diff")
			r := runTool(t, tool, `{"subject":"s","from_version":1,"to_version":2}`)
			if !strings.Contains(r.Summary, "diff not supported for PROTOBUF") {
				t.Fatalf("summary %q", r.Summary)
			}
		})
	}
}

func TestConnectStatusRedactsAndDedupesTraces(t *testing.T) {
	trace := func(task int) string {
		return "org.apache.kafka.connect.errors.DataException: Failed to deserialize {\"email\":\"jane@example.com\",\"ssn\":\"123-45-6789\"} for customer jane.doe@example.com card 4111 1111 1111 1111\n" +
			"\tat org.apache.kafka.connect.runtime.WorkerSinkTask{id=sink-" + string(rune('0'+task)) + "}.convert(WorkerSinkTask.java:1)\n" +
			"Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"users_email_key\" Detail: Key (email)=(jane@example.com) already exists."
	}
	fc := &fakeConnect{statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
		return clients.ConnectorStatus{Name: n, Type: "sink", State: "RUNNING", Tasks: []clients.TaskStatus{
			{ID: 0, State: "FAILED", Trace: trace(0)},
			{ID: 1, State: "FAILED", Trace: trace(1)},
			{ID: 2, State: "FAILED", Trace: "org.apache.kafka.connect.errors.ConnectException: other"},
		}}, nil
	}}
	r := runTool(t, toolFor(t, Deps{Connect: fc}, "connect_status"), `{"connector":"sink"}`)
	for _, leak := range []string{"jane", "123-45-6789", "4111"} {
		if strings.Contains(r.Summary, leak) {
			t.Fatalf("summary leaks %q: %s", leak, r.Summary)
		}
	}
	for _, want := range []string{`"users_email_key"`, "DataException", "PSQLException", "Task 1: FAILED: same error as task 0", "ConnectException: other"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary lacks %q: %s", want, r.Summary)
		}
	}
	if n := strings.Count(r.Summary, "PSQLException"); n != 1 {
		t.Fatalf("trace shown %d times: %s", n, r.Summary)
	}
}

func TestConnectListSaysWhenBudgetRunsOut(t *testing.T) {
	fc := &fakeConnect{
		listFn: func(context.Context) ([]string, error) { return []string{"a", "b", "c"}, nil },
		statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
			if n == "a" {
				return clients.ConnectorStatus{Name: n, Type: "source", State: "RUNNING"}, nil
			}
			<-ctx.Done() // Connect hangs
			return clients.ConnectorStatus{}, ctx.Err()
		},
	}
	ctx, cancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 200*time.Millisecond)
	defer cancel()
	r, err := toolFor(t, Deps{Connect: fc}, "connect_list_connectors").Run(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "a (source): RUNNING") || !strings.Contains(r.Summary, "INCOMPLETE") ||
		!strings.Contains(r.Summary, "2 not checked") || strings.Contains(r.Summary, "UNKNOWN") {
		t.Fatalf("summary %q", r.Summary)
	}
}

func TestConnectStatusPartialOverCap(t *testing.T) {
	body := `{"name":"big","connector":{"state":"RUNNING","worker_id":"w1"},"tasks":[` +
		`{"id":0,"state":"FAILED","worker_id":"w1","trace":"org.apache.kafka.connect.errors.ConnectException: huge` +
		strings.Repeat(`\n\tat x.Y.z(Y.java:1)`, 300_000) + `"}],"type":"source"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/status") {
			io.WriteString(w, body)
			return
		}
		io.WriteString(w, `["big"]`)
	}))
	defer srv.Close()
	cc := connect.New(srv.URL, srv.Client())

	r := runTool(t, toolFor(t, Deps{Connect: cc}, "connect_status"), `{"connector":"big"}`)
	if !strings.Contains(r.Summary, "PARTIAL") || !strings.Contains(r.Summary, "RUNNING on w1") ||
		!strings.Contains(r.Summary, "Task 0: FAILED") || !strings.Contains(r.Summary, "ConnectException: huge") {
		t.Fatalf("summary %q", r.Summary)
	}
	r = runTool(t, toolFor(t, Deps{Connect: cc}, "connect_list_connectors"), `{}`)
	if !strings.Contains(r.Summary, "RUNNING, 1 failed tasks") {
		t.Fatalf("list summary %q", r.Summary)
	}
}

// reportingKafka adds GroupLagReport like the real client.
type reportingKafka struct {
	*fakeKafka
	rep kafka.LagReport
	err error
}

func (k reportingKafka) GroupLagReport(context.Context, string) (kafka.LagReport, error) {
	return k.rep, k.err
}

func TestConsumerLagShowsAnomalies(t *testing.T) {
	k := reportingKafka{fakeKafka: &fakeKafka{}, rep: kafka.LagReport{
		GroupLag: clients.GroupLag{Group: "g", State: "Empty", Partitions: []clients.PartitionLag{
			{Topic: "t", Partition: 0, Committed: 3, End: 120, Lag: 117},
		}},
		Anomalies: []kafka.LagAnomaly{
			{Topic: "t", Partition: 0, Kind: kafka.AnomalyCommitBelowLogStart, Committed: 3, Start: 100, End: 120},
			{Topic: "gone", Partition: 0, Kind: kafka.AnomalyTopicMissing, Committed: 7, Start: -1, End: -1},
			{Topic: "t2", Partition: 1, Kind: kafka.AnomalyCommitBeyondEnd, Committed: 50, End: 1},
		},
	}}
	r := runTool(t, toolFor(t, Deps{Kafka: k}, "kafka_consumer_lag"), `{"group":"g"}`)
	for _, want := range []string{"below the log start 100", "retention deleted 97", "gone/0: committed 7 on a topic/partition that no longer exists", "past the log end 1"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary lacks %q: %s", want, r.Summary)
		}
	}
	if _, ok := r.Data.(map[string]any)["anomalies"]; !ok {
		t.Fatalf("no anomalies in data: %+v", r.Data)
	}

	k.err = clients.ErrNotFound
	r = runTool(t, toolFor(t, Deps{Kafka: k}, "kafka_consumer_lag"), `{"group":"nope"}`)
	if !strings.Contains(r.Summary, `not found: consumer group "nope" does not exist`) {
		t.Fatalf("summary %q", r.Summary)
	}
}

func TestFindByKeyNewestLastAndMissingTopic(t *testing.T) {
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	fk := &fakeKafka{findFn: func(ctx context.Context, topic, key string, since time.Time, max int) ([]clients.RecordRef, error) {
		if topic == "missing" {
			return nil, clients.ErrNotFound
		}
		return []clients.RecordRef{
			{Topic: topic, Partition: 0, Offset: 9, Timestamp: t0.Add(time.Hour), Key: key},
			{Topic: topic, Partition: 0, Offset: 1, Timestamp: t0, Key: key},
		}, nil
	}}
	tool := toolFor(t, Deps{Kafka: fk}, "kafka_find_by_key")
	r := runTool(t, tool, `{"topic":"t","key":"1","since":"2026-10-14T00:00:00Z"}`)
	if !strings.Contains(r.Summary, "offset 9, 2026-10-14T10:00:00Z (newest)") ||
		strings.Index(r.Summary, "offset 1,") > strings.Index(r.Summary, "offset 9,") {
		t.Fatalf("summary %q", r.Summary)
	}
	r = runTool(t, tool, `{"topic":"missing","key":"1","since":"2026-10-14T00:00:00Z"}`)
	if !strings.Contains(r.Summary, `topic "missing" does not exist`) {
		t.Fatalf("summary %q", r.Summary)
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
