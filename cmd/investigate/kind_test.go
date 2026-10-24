package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// mixedOwners is one cdc and one batch pipeline (tasks/P4 "Done when").
const mixedOwners = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
  - id: daily-revenue
    kind: batch
    dags: [revenue_daily]
    models: [fct_revenue, dim_customer]
    sink_tables: [analytics.fct_revenue]
    sink_ts_column: loaded_at
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
  orchestrator: priya
  transform: priya
`

func writeMixedOwners(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(mixedOwners), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := owners.Load(path); err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return path
}

func listOutput(t *testing.T, o runOptions) []string {
	t.Helper()
	var buf bytes.Buffer
	o.ListTools, o.Stdout = true, &buf
	if o.Title == "" {
		o.Title = "t"
	}
	if err := run(o); err != nil {
		t.Fatalf("run %+v: %v", o, err)
	}
	return strings.Fields(buf.String())
}

func hasFamily(names []string, prefixes ...string) string {
	for _, n := range names {
		for _, p := range prefixes {
			if strings.HasPrefix(n, p) {
				return n
			}
		}
	}
	return ""
}

// cmd/investigate -kind batch -list-tools: no kafka_*/pg_*/connect_* tool;
// a CDC room no dag_*/dbt_* tool. No client is opened (nothing listens).
func TestListToolsByKind(t *testing.T) {
	t.Setenv("OWNERS_FILE", writeMixedOwners(t))

	batch := listOutput(t, runOptions{Pipeline: "orders", Kind: "batch"})
	if n := hasFamily(batch, "kafka_", "pg_", "connect_", "schema_registry_", "debezium_", "dlq_"); n != "" {
		t.Fatalf("batch list has %s: %v", n, batch)
	}
	for _, want := range []string{"warehouse_freshness", "pipeline_map", "runbook_lookup", "what_changed"} {
		if !slices.Contains(batch, want) {
			t.Fatalf("batch list lacks %s: %v", want, batch)
		}
	}
	// The batch and dbt packages (P4c-2, P4d-2), wired by P4b-3, in
	// registration order.
	var batchOnly []string
	for _, n := range batch {
		if hasFamily([]string{n}, "dag_", "task_", "dbt_") != "" {
			batchOnly = append(batchOnly, n)
		}
	}
	wantBatch := []string{"dag_runs", "task_instances", "task_log_tail", "dag_schedule_gap",
		"dbt_run_results", "dbt_model_lineage", "dbt_model_sql"}
	if !slices.Equal(batchOnly, wantBatch) {
		t.Fatalf("batch list's batch tools %v, want %v (full list %v)", batchOnly, wantBatch, batch)
	}
	// Without -kind, the pipeline's own kind.
	if got := listOutput(t, runOptions{Pipeline: "daily-revenue"}); !slices.Equal(got, batch) {
		t.Fatalf("daily-revenue list %v, want the batch list %v", got, batch)
	}

	cdc := listOutput(t, runOptions{Pipeline: "orders"})
	if n := hasFamily(cdc, "dag_", "dbt_", "task_"); n != "" {
		t.Fatalf("cdc list has %s: %v", n, cdc)
	}
	for _, want := range []string{"kafka_topic_offsets", "pg_replication_slots", "connect_status", "warehouse_freshness"} {
		if !slices.Contains(cdc, want) {
			t.Fatalf("cdc list lacks %s: %v", want, cdc)
		}
	}
	if got := listOutput(t, runOptions{Pipeline: "daily-revenue", Kind: "cdc"}); !slices.Equal(got, cdc) {
		t.Fatalf("-kind cdc list %v, want %v", got, cdc)
	}
}

func TestListToolsFakeTools(t *testing.T) {
	cdc := listOutput(t, runOptions{Pipeline: "orders", FakeTools: true})
	if len(cdc) == 0 {
		t.Fatal("no fake tools listed")
	}
	batch := listOutput(t, runOptions{Pipeline: "orders", FakeTools: true, Kind: "batch"})
	if len(batch) != 0 {
		t.Fatalf("the CDC replay tools are offered to a batch room: %v", batch)
	}
}

func TestListToolsBadKind(t *testing.T) {
	err := run(runOptions{Pipeline: "orders", FakeTools: true, Kind: "stream", ListTools: true, Stdout: &bytes.Buffer{}})
	if err == nil || !strings.Contains(err.Error(), "-kind") {
		t.Fatalf("err = %v", err)
	}
}

func TestRoomKind(t *testing.T) {
	cdcP := owners.Pipeline{ID: "orders"}
	batchP := owners.Pipeline{ID: "rev", Kind: "batch"}
	for _, tt := range []struct {
		flag     string
		pipe     owners.Pipeline
		havePipe bool
		want     string
		wantErr  bool
	}{
		{"", cdcP, true, "cdc", false},
		{"", batchP, true, "batch", false},
		{"", owners.Pipeline{}, false, "", false},
		{"batch", owners.Pipeline{}, false, "batch", false},
		{"cdc", cdcP, true, "cdc", false},
		{"batch", cdcP, true, "", true}, // the room follows its pipeline
		{"cdc", batchP, true, "", true},
		{"Batch", owners.Pipeline{}, false, "", true},
	} {
		got, err := roomKind(tt.flag, tt.pipe, tt.havePipe)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("roomKind(%q, %s, %v) = %q, %v", tt.flag, tt.pipe.ID, tt.havePipe, got, err)
		}
	}
}

func TestWithModelFlags(t *testing.T) {
	base := config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5"}
	if got := withModelFlags(base, "", ""); got.ModelProvider != "anthropic" || got.Model != "claude-opus-5-5" {
		t.Fatalf("empty flags changed the config: %+v", got)
	}
	if got := withModelFlags(base, "openai", "gpt-x"); got.ModelProvider != "openai" || got.Model != "gpt-x" {
		t.Fatalf("flags not applied: %q %q", got.ModelProvider, got.Model)
	}
}
