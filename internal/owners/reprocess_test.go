package owners

import (
	"strings"
	"testing"
)

func TestReprocessTopic(t *testing.T) {
	o, err := Load(writeTemp(t, twoPipelines("    reprocess_topic: orders-reprocess\n", "")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if p, _ := o.Pipeline("orders"); p.ReprocessTopic != "orders-reprocess" {
		t.Fatalf("ReprocessTopic = %q", p.ReprocessTopic)
	}
	if p, _ := o.Pipeline("payments"); p.ReprocessTopic != "" {
		t.Fatalf("payments ReprocessTopic = %q, want none", p.ReprocessTopic)
	}
}

func TestReprocessTopicRefused(t *testing.T) {
	dlq := "    watch:\n      dlq_topics:\n        - {topic: orders-dlq, written_by: order-sync}\n"
	tests := []struct {
		name, orders, payments, want string
	}{
		{"bad name", "    reprocess_topic: \"orders reprocess\"\n", "", "not a valid Kafka topic name"},
		{"too long", "    reprocess_topic: " + strings.Repeat("a", 250) + "\n", "", "not a valid Kafka topic name"},
		{"own CDC topic", "    reprocess_topic: pg.public.orders\n", "", "CDC topic of this pipeline"},
		{"another pipeline's CDC topic", "    reprocess_topic: pay.public.payments\n", "", `a CDC topic of pipeline "payments"`},
		{"own DLQ", "    reprocess_topic: orders-dlq\n" + dlq, "", `a DLQ topic of pipeline "orders"`},
		{"two pipelines", "    reprocess_topic: replay\n", "    reprocess_topic: replay\n", `the reprocess_topic of pipeline "orders"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, twoPipelines(tc.orders, tc.payments)))
			if err == nil || !strings.Contains(err.Error(), "reprocess_topic") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want a reprocess_topic refusal with %q", err, tc.want)
			}
		})
	}
}

func TestReprocessTopicOnBatchRefused(t *testing.T) {
	_, err := Load(writeTemp(t, `
pipelines:
  - id: nightly
    kind: batch
    dags: [nightly_load]
    reprocess_topic: nightly-reprocess
layers:
  orchestrator: priya
`))
	if err == nil || !strings.Contains(err.Error(), "reprocess_topic is a cdc field") {
		t.Fatalf("err = %v, want a cdc-field refusal", err)
	}
}
