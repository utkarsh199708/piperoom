package owners

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validYAML = `
pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValid(t *testing.T) {
	o, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, ok := o.Pipeline("orders")
	if !ok {
		t.Fatal("pipeline orders not found")
	}
	if len(p.SourceTables) != 1 || p.SourceTables[0] != "public.orders" {
		t.Fatalf("source tables: %v", p.SourceTables)
	}
	if p.TopicPrefix != "pg" {
		t.Fatalf("topic prefix = %q, want pg", p.TopicPrefix)
	}
	if got, ok := o.LayerOwner("warehouse"); !ok || got != "meera" {
		t.Fatalf("warehouse owner = %q, %v", got, ok)
	}
	if _, ok := o.Pipeline("missing"); ok {
		t.Fatal("expected missing pipeline to not be found")
	}
}

func TestLoadValidation(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "unknown layer",
			yaml:    "pipelines:\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1]}\nlayers:\n  bogus: x\n",
			wantErr: "unknown layer",
		},
		{
			name:    "no source tables",
			yaml:    "pipelines:\n  - {id: orders, topic_prefix: pg, connectors: [c1]}\nlayers: {}\n",
			wantErr: "no source tables",
		},
		{
			name:    "no connectors",
			yaml:    "pipelines:\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders]}\nlayers: {}\n",
			wantErr: "no connectors",
		},
		{
			name:    "missing topic prefix",
			yaml:    "pipelines:\n  - {id: orders, source_tables: [public.orders], connectors: [c1]}\nlayers: {}\n",
			wantErr: "topic_prefix is required",
		},
		{
			name:    "duplicate id",
			yaml:    "pipelines:\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1]}\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1]}\nlayers: {}\n",
			wantErr: "duplicate",
		},
		{
			name:    "sink without ts column",
			yaml:    "pipelines:\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1], sink_tables: [analytics.orders]}\nlayers: {}\n",
			wantErr: "sink_ts_column",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}
