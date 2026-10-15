package wiring

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients/connect"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// CONNECT_WORKER_LOG_FILE decides whether the model gets connect_worker_log.
func TestBuildA2ClientsWorkerLogSource(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		want       bool
	}{
		{"unset", "", false},
		{"set", "/var/log/connect.log", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := devA2Config()
			cfg.ConnectWorkerLogFile = tc.path
			a2, err := buildA2Clients(cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer a2.Kafka.Close()
			// A typed nil inside the interface would read as configured.
			if got := a2.ConnectLog != nil; got != tc.want {
				t.Fatalf("ConnectLog set = %v, want %v", got, tc.want)
			}
			w := assemble(&owners.Owners{}, "", a2, nil, nil, func() {})
			if got := w.ConnectLog != nil; got != tc.want {
				t.Fatalf("Wired.ConnectLog set = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRegisterToolsAddsWorkerLogOnlyWithALogSource(t *testing.T) {
	for _, withLog := range []bool{false, true} {
		w := &Wired{Owners: &owners.Owners{}}
		if withLog {
			w.ConnectLog = connect.NewLogReader("/nonexistent/connect.log")
		}
		reg := tools.NewReadOnlyRegistry()
		if err := RegisterTools(reg, actions.NewRegistry(), w); err != nil {
			t.Fatalf("RegisterTools(log=%v): %v", withLog, err)
		}
		tl, ok := reg.Get("connect_worker_log")
		if ok != withLog {
			t.Errorf("log=%v: connect_worker_log registered = %v", withLog, ok)
		}
		if ok && tl.Kind() != tools.KindRead {
			t.Errorf("connect_worker_log kind = %v, want read", tl.Kind())
		}
	}
}

// The real file reader under the real tool: the connector-auth log lines come
// out of a file, are scoped to the room's connector and masked.
func TestWorkerLogToolReadsTheConfiguredFile(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "connect.log")
	// Times relative to now: the tool reads "the last N minutes" of the real clock.
	at := time.Now().UTC().Add(-2 * time.Minute).Format("2006-01-02 15:04:05,000")
	content := at + " ERROR  Postgres|pg-orders-connector|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]\n" +
		"org.apache.kafka.connect.errors.ConnectException: Unable to update filtered publication dbz_publication\n" +
		"\tat io.debezium.Foo.bar(Foo.java:1)\n" +
		"Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders\n" +
		at + " ERROR  Postgres|pg-payments-connector|streaming  Producer failure password=hunter2   [io.debezium.pipeline.ErrorHandler]\n"
	if err := os.WriteFile(logFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	pipe := owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders-connector"}, SourceTables: []string{"public.orders"}, TopicPrefix: "pg"}
	w := &Wired{Owners: &owners.Owners{Pipelines: []owners.Pipeline{pipe}}, ConnectLog: connect.NewLogReader(logFile)}
	reg := tools.NewReadOnlyRegistry()
	if err := RegisterTools(reg, actions.NewRegistry(), w); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("connect_worker_log")
	if !ok {
		t.Fatal("connect_worker_log not registered")
	}
	ctx := tools.WithPipeline(context.Background(), pipe)

	r, err := tl.Run(ctx, json.RawMessage(`{"since_minutes":10}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "root cause: org.postgresql.util.PSQLException: ERROR: must be owner of table orders") {
		t.Errorf("summary = %s", r.Summary)
	}
	if strings.Contains(r.Summary, "pg-payments") || strings.Contains(r.Summary, "hunter2") {
		t.Errorf("another connector's line reached the result: %s", r.Summary)
	}
	// Naming the other pipeline's connector is refused before the file is read.
	if _, err := tl.Run(ctx, json.RawMessage(`{"connector":"pg-payments-connector"}`)); err == nil || !strings.Contains(err.Error(), "out of scope") {
		t.Errorf("err = %v, want out of scope", err)
	}
}

// A pipeline-specific client set starts from an empty config, so it never
// reads the process-wide log file.
func TestPipelineConfigHasNoWorkerLogFile(t *testing.T) {
	p := owners.Pipeline{ID: "x", Connections: owners.Connections{"connect_url": {Value: "http://connect.x:8083"}}}
	pc, err := pipelineConfig(p, osConnSource)
	if err != nil {
		t.Fatal(err)
	}
	var zero config.Config
	if pc.ConnectWorkerLogFile != zero.ConnectWorkerLogFile || pc.ConnectWorkerLogFile != "" {
		t.Fatalf("ConnectWorkerLogFile = %q, want empty", pc.ConnectWorkerLogFile)
	}
}
