package owners

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// QA4: Load refuses duplicate list entries, badly formed names and pipelines
// whose layers nobody owns.

func TestLoadRejectsBadPipelineNames(t *testing.T) {
	cases := []struct {
		name, from, to, wantErr string
	}{
		{"duplicate source table", "source_tables: [public.orders]", "source_tables: [public.orders, public.orders]", "duplicate"},
		{"duplicate connector", "connectors: [pg-orders-connector]", "connectors: [pg-orders-connector, pg-orders-connector]", "duplicate"},
		{"duplicate group", "consumer_groups: [order-sync]", "consumer_groups: [order-sync, order-sync]", "duplicate"},
		{"duplicate sink table", "sink_tables: [analytics.orders]", "sink_tables: [analytics.orders, analytics.orders]", "duplicate"},
		{"source table not schema.table", "source_tables: [public.orders]", "source_tables: [orders]", "source_tables"},
		{"source table injection", "source_tables: [public.orders]", `source_tables: ["public.orders; drop table x"]`, "source_tables"},
		{"source table three parts", "source_tables: [public.orders]", "source_tables: [db.public.orders]", "source_tables"},
		{"empty source table", "source_tables: [public.orders]", `source_tables: [public.orders, ""]`, "source_tables"},
		{"sink table bad", "sink_tables: [analytics.orders]", `sink_tables: ["analytics.orders x"]`, "sink_tables"},
		{"sink ts column bad", "sink_ts_column: updated_at", `sink_ts_column: "updated_at; --"`, "sink_ts_column"},
		{"topic prefix illegal", "topic_prefix: pg", `topic_prefix: "pg prod"`, "topic_prefix"},
		{"topic prefix newline", "topic_prefix: pg", `topic_prefix: "pg\nx"`, "topic_prefix"},
		{"empty connector", "connectors: [pg-orders-connector]", `connectors: [""]`, "connectors"},
		{"connector with newline", "connectors: [pg-orders-connector]", `connectors: ["a\nb"]`, "connectors"},
		{"connector padded", "connectors: [pg-orders-connector]", `connectors: [" pg "]`, "connectors"},
		{"empty group", "consumer_groups: [order-sync]", `consumer_groups: ["  "]`, "consumer_groups"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			y := strings.Replace(validYAML, tc.from, tc.to, 1)
			if y == validYAML {
				t.Fatalf("replacement %q not applied", tc.from)
			}
			_, err := Load(writeTemp(t, y))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load err = %v, want one mentioning %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadAcceptsGoodNames(t *testing.T) {
	y := strings.NewReplacer(
		"sink_tables: [analytics.orders]", "sink_tables: [dwh.analytics.orders, analytics.ORDERS_V2]",
		"sink_ts_column: updated_at", "sink_ts_column: _loaded$at",
		"topic_prefix: pg", "topic_prefix: pg-prod_1.eu",
	).Replace(validYAML)
	if _, err := Load(writeTemp(t, y)); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func mustLoad(t *testing.T, y string) *Owners {
	t.Helper()
	o, err := Load(writeTemp(t, y))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return o
}

func TestCheckLayerOwners(t *testing.T) {
	if err := mustLoad(t, validYAML).CheckLayerOwners(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for _, layer := range []string{"postgres", "kafka-connect", "kafka", "consumer", "warehouse"} {
		t.Run(layer, func(t *testing.T) {
			var kept []string
			for _, l := range strings.Split(validYAML, "\n") {
				if !strings.HasPrefix(l, "  "+layer+":") {
					kept = append(kept, l)
				}
			}
			err := mustLoad(t, strings.Join(kept, "\n")).CheckLayerOwners()
			if err == nil || !strings.Contains(err.Error(), `"`+layer+`"`) || !strings.Contains(err.Error(), "no owner") {
				t.Fatalf("CheckLayerOwners without a %s owner: err = %v", layer, err)
			}
		})
	}

	// A per-pipeline owners: block is enough.
	y := strings.Replace(validYAML, "  warehouse: meera\n", "", 1)
	y = strings.Replace(y, "    sink_ts_column: updated_at\n", "    sink_ts_column: updated_at\n    owners:\n      warehouse: {approvers: [user:meera]}\n", 1)
	if err := mustLoad(t, y).CheckLayerOwners(); err != nil {
		t.Fatalf("pipeline warehouse owner: %v", err)
	}

	// A pipeline without sink tables or groups needs no warehouse or consumer owner.
	y = strings.NewReplacer(
		"  warehouse: meera\n", "",
		"  consumer: kabir\n", "",
		"    consumer_groups: [order-sync]\n", "",
		"    sink_tables: [analytics.orders]\n", "",
		"    sink_ts_column: updated_at\n", "",
	).Replace(validYAML)
	if err := mustLoad(t, y).CheckLayerOwners(); err != nil {
		t.Fatalf("source-only pipeline: %v", err)
	}
}

// The checked-in owners files must keep loading.
func TestCheckedInOwnersFilesLoad(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(here), "..", "..")
	for _, rel := range []string{
		"dev/owners.yaml",
		"deploy/config/owners.example.yaml",
		"test/e2e/oidc/config/owners.yaml",
		"evals/scenarios/heldout/fixtures/owners.yaml",
	} {
		path := filepath.Join(root, rel)
		if _, err := os.Stat(path); err != nil {
			t.Logf("skip %s: %v", rel, err)
			continue
		}
		o, err := Load(path)
		if err != nil {
			t.Errorf("%s: %v", rel, err)
			continue
		}
		if err := o.CheckLayerOwners(); err != nil {
			t.Errorf("%s: %v", rel, err)
		}
	}
}
