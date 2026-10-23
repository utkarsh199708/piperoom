package wiring

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/workerlogs"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// workerKafka is a Kafka admin with the Connect worker-group read (Q3a). It
// records the internal topic names connect_worker_info asks for.
type workerKafka struct {
	clients.KafkaAdmin
	gotNames *[]string
}

func (workerKafka) ConnectWorkerGroups(context.Context) ([]kafka.ConnectWorkerGroup, error) {
	return []kafka.ConnectWorkerGroup{{GroupID: "connect-prod", State: "Stable", Workers: []kafka.ConnectWorker{
		{MemberID: "m1", Host: "10.0.0.5", Leader: true, Connectors: []string{"orders-cdc"}},
	}}}, nil
}

func (k workerKafka) InternalTopics(_ context.Context, names []string) ([]kafka.InternalTopic, error) {
	*k.gotNames = append([]string(nil), names...)
	return nil, nil
}

// workerLogsSpy is a worker log reader that counts its reads.
type workerLogsSpy struct{ calls int }

func (s *workerLogsSpy) WorkerLogs(context.Context, workerlogs.LogQuery) ([]workerlogs.LogLine, error) {
	s.calls++
	return nil, nil
}

func q3Owners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", Connectors: []string{"orders-cdc"}}}}
}

func q3Ctx() context.Context {
	return tools.WithPipeline(context.Background(), q3Owners().Pipelines[0])
}

func registerQ3(t *testing.T, w *Wired) *tools.Registry {
	t.Helper()
	reg := tools.NewReadOnlyRegistry()
	if err := RegisterTools(reg, actions.NewRegistry(), w); err != nil {
		t.Fatalf("RegisterTools: %v", err)
	}
	return reg
}

func runQ3(t *testing.T, reg *tools.Registry, name, in string) tools.Result {
	t.Helper()
	tl, ok := reg.Get(name)
	if !ok {
		t.Fatalf("%s is not registered", name)
	}
	res, err := tl.Run(q3Ctx(), json.RawMessage(in))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return res
}

// TestWorkerVisibilityToolsRegistered (Q3e): a CDC set registers
// connect_worker_metrics and connect_worker_logs whatever is configured, and
// connect_worker_info gets the configured internal topics and the Connect
// log levels through the read-only Connect value.
func TestWorkerVisibilityToolsRegistered(t *testing.T) {
	var names []string
	w := &Wired{
		Connect:               lifecycleSpy{&writeSpy{}},
		Kafka:                 workerKafka{gotNames: &names},
		Owners:                q3Owners(),
		ConnectInternalTopics: " prod-configs, prod-offsets ,prod-status ",
	}
	reg := registerQ3(t, w)
	for _, n := range []string{"connect_worker_metrics", "connect_worker_logs", "connect_worker_info"} {
		if _, ok := reg.Get(n); !ok {
			t.Errorf("%s is not registered", n)
		}
	}

	res := runQ3(t, reg, "connect_worker_info", `{}`)
	if want := []string{"prod-configs", "prod-offsets", "prod-status"}; !slices.Equal(names, want) {
		t.Errorf("internal topics asked = %q, want %q (CONNECT_INTERNAL_TOPICS parsed)", names, want)
	}
	b, err := json.Marshal(res.Data)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "io.debezium") {
		t.Errorf("connect_worker_info has no logger levels; the read-only Connect value must forward Loggers: %s", b)
	}

	// No log store: the tool answers, it does not fail.
	if res := runQ3(t, reg, "connect_worker_logs", `{"connector":"orders-cdc"}`); !strings.Contains(res.Summary, "not configured") {
		t.Errorf("connect_worker_logs without a log store = %q, want not configured", res.Summary)
	}
	// No exporter URL: the same for the worker metrics.
	if res := runQ3(t, reg, "connect_worker_metrics", `{"connector":"orders-cdc"}`); !strings.Contains(res.Summary, "not configured") {
		t.Errorf("connect_worker_metrics without DEBEZIUM_METRICS_URL = %q, want not configured", res.Summary)
	}
}

// TestWorkerLogsReaderReachesTool: a log store set on the Wired is the one
// connect_worker_logs reads; a typed nil one counts as none.
func TestWorkerLogsReaderReachesTool(t *testing.T) {
	spy := &workerLogsSpy{}
	reg := registerQ3(t, &Wired{Owners: q3Owners(), WorkerLogs: spy})
	runQ3(t, reg, "connect_worker_logs", `{"connector":"orders-cdc"}`)
	if spy.calls != 1 {
		t.Fatalf("log store reads = %d, want 1", spy.calls)
	}

	var typedNil *workerLogsSpy
	reg = registerQ3(t, &Wired{Owners: q3Owners(), WorkerLogs: typedNil})
	if res := runQ3(t, reg, "connect_worker_logs", `{"connector":"orders-cdc"}`); !strings.Contains(res.Summary, "not configured") {
		t.Fatalf("typed nil log store = %q, want not configured", res.Summary)
	}
}

// TestConnectMetricsPrefixReachesTool: the prefix is handed to
// connect_worker_metrics, which refuses one that is not a metric name.
func TestConnectMetricsPrefixReachesTool(t *testing.T) {
	err := RegisterTools(tools.NewReadOnlyRegistry(), actions.NewRegistry(), &Wired{Owners: q3Owners(), ConnectMetricsPrefix: "9-not-a-prefix"})
	if err == nil || !strings.Contains(err.Error(), "CONNECT_METRICS_PREFIX") {
		t.Fatalf("RegisterTools with a bad prefix = %v, want the prefix refused", err)
	}
	registerQ3(t, &Wired{Owners: q3Owners(), ConnectMetricsPrefix: "connect_"})
}
