package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Tests for finding F11: the read tools only see the room's own pipeline.

// scopeOwners has the room's pipeline ("orders") and another team's ("billing").
func scopeOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{
		{
			ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"},
			Connectors: []string{"pg-orders-connector"}, ConsumerGroups: []string{"order-sync"},
		},
		{
			ID: "billing", TopicPrefix: "bill", SourceTables: []string{"public.invoices"},
			Connectors: []string{"bill-connector"}, ConsumerGroups: []string{"bill-sync"},
		},
	}}
}

// counted counts client calls and remembers which names they were for.
type counted struct {
	mu    sync.Mutex
	calls []string
}

func (c *counted) add(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, s)
}

func (c *counted) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func (c *counted) has(s string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Contains(c.calls, s)
}

// scopeFakes returns fakes that serve both pipelines' objects and count every call.
func scopeFakes(t *testing.T) (Deps, *counted) {
	c := &counted{}
	fc := &fakeConnect{
		listFn: func(context.Context) ([]string, error) {
			c.add("connect.list")
			return []string{"pg-orders-connector", "bill-connector", "other-connector"}, nil
		},
		statusFn: func(_ context.Context, n string) (clients.ConnectorStatus, error) {
			c.add("connect.status:" + n)
			return clients.ConnectorStatus{Name: n, Type: "source", State: "RUNNING"}, nil
		},
		configFn: func(_ context.Context, n string) (map[string]string, error) {
			c.add("connect.config:" + n)
			return map[string]string{"connector.class": "x", "database.hostname": "db-of-" + n}, nil
		},
	}
	guardWrites(t, fc)
	fr := &fakeRegistry{
		subjectsFn: func(context.Context) ([]string, error) {
			c.add("registry.subjects")
			return []string{
				"pg.public.orders-value", "pg.public.orders-key",
				"bill.public.invoices-value", "pg.public.customers-value",
				"billing.pg.public.orders-value", "orders-value",
			}, nil
		},
		versionsFn: func(_ context.Context, s string) ([]int, error) {
			c.add("registry.versions:" + s)
			return []int{1, 2}, nil
		},
		schemaFn: func(_ context.Context, s string, v int) (clients.SchemaVersion, error) {
			c.add("registry.schema:" + s)
			if v < 0 {
				v = 2
			}
			return clients.SchemaVersion{Subject: s, Version: v, ID: v, Schema: `{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}`}, nil
		},
		compatFn: func(_ context.Context, s string) (string, bool, error) {
			c.add("registry.compat:" + s)
			return "BACKWARD", true, nil
		},
	}
	guardWrites(t, fr)
	fk := &fakeKafkaConfig{
		fakeKafka: &fakeKafka{
			offsetsFn: func(_ context.Context, topic string) ([]clients.PartitionOffsets, error) {
				c.add("kafka.offsets:" + topic)
				return []clients.PartitionOffsets{{Partition: 0, Start: 0, End: 10}}, nil
			},
			groupsFn: func(context.Context) ([]string, error) {
				c.add("kafka.groups")
				return []string{"order-sync", "bill-sync", "other-group"}, nil
			},
			lagFn: func(_ context.Context, g string) (clients.GroupLag, error) {
				c.add("kafka.lag:" + g)
				return clients.GroupLag{Group: g, State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders"}}}, nil
			},
			findFn: func(_ context.Context, topic, _ string, _ time.Time, _ int) ([]clients.RecordRef, error) {
				c.add("kafka.find:" + topic)
				return nil, nil
			},
		},
		cfgFn: func(_ context.Context, topic string) (kafka.TopicConfig, error) {
			c.add("kafka.topicconfig:" + topic)
			return kafka.TopicConfig{Topic: topic, Partitions: 3, ReplicationFactor: 1}, nil
		},
	}
	// The worker log reader counts its calls too, so the scope tests prove
	// that a refused connector never reaches it.
	fl := &fakeLogReader{fn: func(_ context.Context, q clients.WorkerLogQuery) ([]clients.LogLine, error) {
		c.add("log.read:" + q.Connector)
		return []clients.LogLine{{At: time.Date(2026, 10, 15, 4, 10, 9, 0, time.UTC), Level: "ERROR", Message: "boom"}}, nil
	}}
	return Deps{Connect: fc, Registry: fr, Kafka: fk, Owners: scopeOwners(), ConnectLog: fl}, c
}

func scopeTool(t *testing.T, d Deps, name string) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get(name)
	if !ok {
		t.Fatalf("%s not registered", name)
	}
	return tool
}

func roomCtx() context.Context { return tools.WithPipelineID(context.Background(), "orders") }

const since = "2026-10-11T00:00:00Z"

// inputs: one in-scope and one out-of-scope input per tool.
var scopeCases = []struct {
	tool string
	in   string // in the room's pipeline
	out  string // names something outside it
}{
	{"connect_status", `{"connector":"pg-orders-connector"}`, `{"connector":"bill-connector"}`},
	{"connect_config", `{"connector":"pg-orders-connector"}`, `{"connector":"bill-connector"}`},
	{"connect_worker_log", `{"connector":"pg-orders-connector"}`, `{"connector":"bill-connector"}`},
	{"schema_registry_versions", `{"subject":"pg.public.orders-value"}`, `{"subject":"bill.public.invoices-value"}`},
	{"schema_registry_diff", `{"subject":"pg.public.orders-key","from_version":1}`, `{"subject":"bill.public.invoices-value","from_version":1}`},
	{"kafka_topic_offsets", `{"topic":"pg.public.orders"}`, `{"topic":"bill.public.invoices"}`},
	{"kafka_find_by_key", `{"topic":"pg.public.orders","key":"1","since":"` + since + `"}`, `{"topic":"bill.public.invoices","key":"1","since":"` + since + `"}`},
	{"kafka_consumer_lag", `{"group":"order-sync"}`, `{"group":"bill-sync"}`},
	{"kafka_list_groups", `{"topic":"pg.public.orders"}`, `{"topic":"bill.public.invoices"}`},
	{"kafka_topic_config", `{"pipeline_id":"orders"}`, `{"pipeline_id":"billing"}`},
}

func TestScopeInScopeWorks(t *testing.T) {
	for _, tc := range scopeCases {
		t.Run(tc.tool, func(t *testing.T) {
			d, c := scopeFakes(t)
			if _, err := scopeTool(t, d, tc.tool).Run(roomCtx(), json.RawMessage(tc.in)); err != nil {
				t.Fatalf("in-scope call failed: %v", err)
			}
			if c.n() == 0 {
				t.Fatal("no client was called")
			}
		})
	}
}

func TestScopeOutOfScopeRefusedWithoutClientCall(t *testing.T) {
	for _, tc := range scopeCases {
		t.Run(tc.tool, func(t *testing.T) {
			d, c := scopeFakes(t)
			res, err := scopeTool(t, d, tc.tool).Run(roomCtx(), json.RawMessage(tc.out))
			if !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("err = %v, want ErrOutOfScope", err)
			}
			if !strings.Contains(err.Error(), "orders") {
				t.Errorf("error does not name the room's pipeline: %v", err)
			}
			if c.n() != 0 {
				t.Fatalf("clients were called for an out-of-scope name: %v", c.calls)
			}
			if res.Summary != "" || res.Data != nil {
				t.Fatalf("refusal returned data: %+v", res)
			}
		})
	}
}

func TestScopePipelineIDMismatchRefused(t *testing.T) {
	d, c := scopeFakes(t)
	tool := scopeTool(t, d, "kafka_topic_config")
	// "billing" exists in owners, but it is not this room's pipeline.
	_, err := tool.Run(roomCtx(), json.RawMessage(`{"pipeline_id":"billing"}`))
	if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "pipeline_id") {
		t.Fatalf("err = %v, want a pipeline_id ErrOutOfScope", err)
	}
	if c.n() != 0 {
		t.Fatalf("clients called: %v", c.calls)
	}
}

// A prompt-injected agent names another pipeline's connector, topic, group and
// subject in every tool that takes a name.
func TestScopePromptInjectionNamesOtherPipeline(t *testing.T) {
	d, c := scopeFakes(t)
	for _, tc := range scopeCases {
		if _, err := scopeTool(t, d, tc.tool).Run(roomCtx(), json.RawMessage(tc.out)); err == nil {
			t.Errorf("%s accepted %s", tc.tool, tc.out)
		}
	}
	if c.n() != 0 {
		t.Fatalf("clients called: %v", c.calls)
	}
	// The error names the rule and the allowed connector, never the other
	// pipeline's config.
	tool := scopeTool(t, d, "connect_config")
	_, err := tool.Run(roomCtx(), json.RawMessage(`{"connector":"bill-connector"}`))
	if err == nil || !strings.Contains(err.Error(), "allowed connectors: pg-orders-connector") || strings.Contains(err.Error(), "db-of-") {
		t.Fatalf("error = %v", err)
	}
}

func TestScopeNoRoomRefusedByDefault(t *testing.T) {
	tl := append([]struct{ tool, in, out string }{
		{"connect_list_connectors", `{}`, ``},
		{"schema_registry_subjects", `{}`, ``},
		{"kafka_list_groups", `{}`, ``},
	}, scopeCases...)
	for _, tc := range tl {
		t.Run(tc.tool, func(t *testing.T) {
			d, c := scopeFakes(t)
			_, err := scopeTool(t, d, tc.tool).Run(context.Background(), json.RawMessage(tc.in))
			if !errors.Is(err, tools.ErrNoScope) {
				t.Fatalf("err = %v, want ErrNoScope", err)
			}
			if c.n() != 0 {
				t.Fatalf("clients called: %v", c.calls)
			}
		})
	}
}

func TestScopeOptOutKeepsOldBehaviour(t *testing.T) {
	ctx := tools.WithUnscoped(context.Background())
	for _, tc := range scopeCases {
		t.Run(tc.tool, func(t *testing.T) {
			d, _ := scopeFakes(t)
			tool := scopeTool(t, d, tc.tool)
			for _, in := range []string{tc.in, tc.out} {
				if _, err := tool.Run(ctx, json.RawMessage(in)); err != nil {
					t.Fatalf("opted-out call %s failed: %v", in, err)
				}
			}
		})
	}
	// Unscoped lists are not filtered.
	d, _ := scopeFakes(t)
	r, err := scopeTool(t, d, "kafka_list_groups").Run(ctx, json.RawMessage(`{}`))
	if err != nil || !strings.Contains(r.Summary, "other-group") {
		t.Fatalf("unfiltered list = %q, %v", r.Summary, err)
	}
}

func TestScopeRoomPipelineWinsOverOptOut(t *testing.T) {
	ctx := tools.WithUnscoped(roomCtx())
	d, c := scopeFakes(t)
	_, err := scopeTool(t, d, "connect_status").Run(ctx, json.RawMessage(`{"connector":"bill-connector"}`))
	if !errors.Is(err, tools.ErrOutOfScope) || c.n() != 0 {
		t.Fatalf("err = %v, calls = %v", err, c.calls)
	}
}

func TestScopeUnknownRoomPipelineRefused(t *testing.T) {
	d, c := scopeFakes(t)
	ctx := tools.WithPipelineID(context.Background(), "no-such-pipeline")
	_, err := scopeTool(t, d, "connect_list_connectors").Run(ctx, json.RawMessage(`{}`))
	if !errors.Is(err, tools.ErrOutOfScope) || c.n() != 0 {
		t.Fatalf("err = %v, calls = %v", err, c.calls)
	}
}

func TestScopeMissingOwnersIsAnErrorNotAPanic(t *testing.T) {
	d, c := scopeFakes(t)
	d.Owners = nil
	_, err := scopeTool(t, d, "connect_list_connectors").Run(roomCtx(), json.RawMessage(`{}`))
	if err == nil || c.n() != 0 {
		t.Fatalf("err = %v, calls = %v", err, c.calls)
	}
}

func TestScopeFullPipelineInContext(t *testing.T) {
	d, _ := scopeFakes(t)
	d.Owners = nil // not needed when the caller passes the whole pipeline
	p, _ := scopeOwners().Pipeline("orders")
	ctx := tools.WithPipeline(context.Background(), p)
	if _, err := scopeTool(t, d, "connect_status").Run(ctx, json.RawMessage(`{"connector":"pg-orders-connector"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := scopeTool(t, d, "connect_status").Run(ctx, json.RawMessage(`{"connector":"bill-connector"}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("err = %v", err)
	}
}

func TestScopeListToolsAreFiltered(t *testing.T) {
	t.Run("connect_list_connectors", func(t *testing.T) {
		d, c := scopeFakes(t)
		r, err := scopeTool(t, d, "connect_list_connectors").Run(roomCtx(), json.RawMessage(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(r.Summary, "pg-orders-connector") || strings.Contains(r.Summary, "bill-connector") || strings.Contains(r.Summary, "other-connector") {
			t.Fatalf("summary = %q", r.Summary)
		}
		if c.has("connect.status:bill-connector") || c.has("connect.status:other-connector") {
			t.Fatalf("Status called for out-of-scope connectors: %v", c.calls)
		}
	})
	t.Run("kafka_list_groups", func(t *testing.T) {
		d, c := scopeFakes(t)
		r, err := scopeTool(t, d, "kafka_list_groups").Run(roomCtx(), json.RawMessage(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(r.Summary, "order-sync") || strings.Contains(r.Summary, "bill-sync") || strings.Contains(r.Summary, "other-group") {
			t.Fatalf("summary = %q", r.Summary)
		}
		if c.has("kafka.lag:bill-sync") || c.has("kafka.lag:other-group") {
			t.Fatalf("GroupLag called for out-of-scope groups: %v", c.calls)
		}
	})
	t.Run("schema_registry_subjects", func(t *testing.T) {
		d, _ := scopeFakes(t)
		r, err := scopeTool(t, d, "schema_registry_subjects").Run(roomCtx(), json.RawMessage(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		got := strings.Split(r.Summary, "\n")
		const countLine = "4 other subjects not shown (not part of this room's pipeline)"
		if got[len(got)-1] != countLine {
			t.Fatalf("last line = %q, want %q", got[len(got)-1], countLine)
		}
		got = got[:len(got)-1]
		slices.Sort(got)
		want := []string{"pg.public.orders-key", "pg.public.orders-value"}
		if !slices.Equal(got, want) {
			t.Fatalf("subjects = %v, want %v", got, want)
		}
		// A prefix cannot widen the scope, and the count does not depend on it,
		// so a prefix cannot probe other subjects' names either.
		r, err = scopeTool(t, d, "schema_registry_subjects").Run(roomCtx(), json.RawMessage(`{"prefix":"bill"}`))
		if err != nil || r.Summary != countLine || len(r.Data.([]string)) != 0 {
			t.Fatalf("prefix bill gave %q, %v, %v", r.Summary, r.Data, err)
		}
	})
}

func TestConnectStatusDoesNotDiffAnOutOfScopeSubject(t *testing.T) {
	trace := `org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded
Caused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}
Caused by: RestClientException: Schema being registered is incompatible with an earlier schema for subject "bill.public.invoices-value", error code: 409`
	d, c := scopeFakes(t)
	d.Connect.(*fakeConnect).statusFn = func(_ context.Context, n string) (clients.ConnectorStatus, error) {
		c.add("connect.status:" + n)
		return clients.ConnectorStatus{Name: n, Type: "source", State: "RUNNING",
			Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: trace}}}, nil
	}
	r, err := scopeTool(t, d, "connect_status").Run(roomCtx(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.Summary, "Rejected schema vs") {
		t.Fatalf("diffed another pipeline's subject: %q", r.Summary)
	}
	for _, call := range c.calls {
		if strings.HasPrefix(call, "registry.") {
			t.Fatalf("registry called for an out-of-scope subject: %v", c.calls)
		}
	}
}

func TestConnectConfigRedactsTransforms(t *testing.T) {
	d, _ := scopeFakes(t)
	d.Connect.(*fakeConnect).configFn = func(context.Context, string) (map[string]string, error) {
		return map[string]string{
			"connector.class":                     "io.debezium.connector.postgresql.PostgresConnector",
			"database.password":                   "pw-secret",
			"transforms":                          "unwrap,mask,ins,route",
			"transforms.unwrap.type":              "io.debezium.transforms.ExtractNewRecordState",
			"transforms.unwrap.drop.tombstones":   "false",
			"transforms.mask.type":                "org.apache.kafka.connect.transforms.MaskField$Value",
			"transforms.mask.fields":              "card_number,ssn",
			"transforms.mask.replacement":         "XXXX-LITERAL",
			"transforms.ins.type":                 "org.apache.kafka.connect.transforms.InsertField$Value",
			"transforms.ins.static.field":         "src",
			"transforms.ins.static.value":         "api-key-literal",
			"transforms.route.type":               "org.apache.kafka.connect.transforms.RegexRouter",
			"transforms.route.regex":              "internal-topic-(.*)",
			"transforms.route.replacement":        "$1",
			"transforms.route.predicate":          "p",
			"transformsX":                         "not-a-transform-key",
			"transforms.weird":                    "no-setting-segment",
			"transforms.ins.field.name":           "event_ts",
			"transforms.ins.timestamp.field.name": "should-hide",
		}, nil
	}
	r, err := scopeTool(t, d, "connect_config").Run(roomCtx(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatal(err)
	}
	shown := r.Data.(map[string]string)
	wantShown := map[string]string{
		"transforms":                "unwrap,mask,ins,route",
		"transforms.unwrap.type":    "io.debezium.transforms.ExtractNewRecordState",
		"transforms.mask.type":      "org.apache.kafka.connect.transforms.MaskField$Value",
		"transforms.mask.fields":    "card_number,ssn",
		"transforms.ins.type":       "org.apache.kafka.connect.transforms.InsertField$Value",
		"transforms.ins.field.name": "event_ts",
		"transforms.route.type":     "org.apache.kafka.connect.transforms.RegexRouter",
	}
	for k, v := range wantShown {
		if shown[k] != v {
			t.Errorf("%s = %q, want %q", k, shown[k], v)
		}
	}
	for _, k := range []string{
		"transforms.unwrap.drop.tombstones", "transforms.mask.replacement", "transforms.ins.static.field",
		"transforms.ins.static.value", "transforms.route.regex", "transforms.route.replacement",
		"transforms.route.predicate", "transformsX", "transforms.weird", "transforms.ins.timestamp.field.name",
	} {
		if shown[k] != "***" {
			t.Errorf("%s = %q, want redacted", k, shown[k])
		}
	}
	if shown["database.password"] != "***" {
		t.Errorf("password not masked: %q", shown["database.password"])
	}
	for _, leak := range []string{"XXXX-LITERAL", "api-key-literal", "internal-topic", "pw-secret", "should-hide"} {
		if strings.Contains(r.Summary, leak) {
			t.Errorf("summary leaks %q: %q", leak, r.Summary)
		}
	}
}
