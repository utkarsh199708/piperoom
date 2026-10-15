package owners_test

import (
	"strings"
	"testing"
)

// A name listed twice in one pipeline list became two graph nodes with the
// same id (and doubled edges) in the pipeline map (QA sweep). Load refuses it.
func TestLoadRefusesDuplicateListEntries(t *testing.T) {
	const head = "pipelines:\n  - id: orders\n    topic_prefix: pg\n    sink_ts_column: updated_at\n"
	for name, lists := range map[string]string{
		"source_tables":   "    source_tables: [public.orders, public.orders]\n    connectors: [c1]\n",
		"connectors":      "    source_tables: [public.orders]\n    connectors: [c1, c1]\n",
		"consumer_groups": "    source_tables: [public.orders]\n    connectors: [c1]\n    consumer_groups: [g, g]\n",
		"sink_tables":     "    source_tables: [public.orders]\n    connectors: [c1]\n    sink_tables: [analytics.orders, analytics.orders]\n",
	} {
		_, err := loadOwners(t, head+lists)
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: err = %v, want a duplicate error naming the list", name, err)
		}
	}
	if _, err := loadOwners(t, head+"    source_tables: [public.orders, public.items]\n    connectors: [c1, c2]\n"); err != nil {
		t.Fatalf("distinct entries: %v", err)
	}
}
