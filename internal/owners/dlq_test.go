package owners

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// twoPipelines is a file with the orders pipeline plus a payments pipeline.
// Both get the given watch blocks (already indented under the pipeline).
func twoPipelines(ordersWatch, paymentsWatch string) string {
	return `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector, pg-orders-sink]
    consumer_groups: [order-sync]
` + ordersWatch + `
  - id: payments
    topic_prefix: pay
    source_tables: [public.payments]
    connectors: [pay-connector]
    consumer_groups: [pay-sync]
` + paymentsWatch + `
layers:
  postgres: priya
  kafka-connect: arjun
  kafka: arjun
  consumer: kabir
`
}

func TestDLQDefaults(t *testing.T) {
	d := DefaultWatch()
	if g := d.DLQGrowth; !g.Enabled || g.Action != ActionRoom || g.For != 5*time.Minute || g.FireRecords != 1 || g.Window != time.Hour {
		t.Fatalf("dlq_growth defaults = %+v", g)
	}
	if c := d.ConfigRisk; !c.Enabled || c.Action != ActionLog || c.For != time.Minute {
		t.Fatalf("config_risk defaults = %+v", c)
	}
	if d.DLQPeekHeaders != DLQHeadersClasses {
		t.Fatalf("dlq_peek.headers default = %q, want classes", d.DLQPeekHeaders)
	}
	if len(d.DLQTopics) != 0 {
		t.Fatalf("DLQTopics = %v, want none", d.DLQTopics)
	}
	// Loading a file with no DLQ settings gives the same defaults.
	o, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := o.Pipeline("orders")
	if !reflect.DeepEqual(p.Watch(), d) {
		t.Fatalf("loaded defaults differ: %+v", p.Watch())
	}
}

func TestDLQGrowthOverrides(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch(`    watch:
      signals:
        dlq_growth: {for: 10m, fire: 5, window: 2h, action: log}
        config_risk: {action: room, for: 2m}
      dlq_peek:
        headers: messages
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	w := p.Watch()
	g := w.DLQGrowth
	if !g.Enabled || g.Action != ActionLog || g.For != 10*time.Minute || g.FireRecords != 5 || g.Window != 2*time.Hour {
		t.Fatalf("dlq_growth = %+v", g)
	}
	if w.ConfigRisk.Action != ActionRoom || w.ConfigRisk.For != 2*time.Minute || !w.ConfigRisk.Enabled {
		t.Fatalf("config_risk = %+v", w.ConfigRisk)
	}
	if w.DLQPeekHeaders != DLQHeadersMessages {
		t.Fatalf("headers = %q", w.DLQPeekHeaders)
	}
	// Other signals keep their defaults.
	if w.SlotWAL != DefaultWatch().SlotWAL {
		t.Fatalf("slot_wal changed: %+v", w.SlotWAL)
	}
}

func TestDLQGrowthDisabled(t *testing.T) {
	o, err := Load(writeTemp(t, withWatch(`    watch:
      signals:
        dlq_growth: {enabled: false}
        config_risk: {enabled: false}
`)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	if w := p.Watch(); w.DLQGrowth.Enabled || w.ConfigRisk.Enabled {
		t.Fatalf("signals still enabled: %+v / %+v", w.DLQGrowth, w.ConfigRisk)
	}
}

func TestDLQWindowBoundary(t *testing.T) {
	ok := func(window string, want time.Duration) {
		t.Helper()
		o, err := Load(writeTemp(t, withWatch("    watch:\n      signals:\n        dlq_growth: {window: "+window+"}\n")))
		if err != nil {
			t.Fatalf("window %s: %v", window, err)
		}
		p, _ := o.Pipeline("orders")
		if got := p.Watch().DLQGrowth.Window; got != want {
			t.Fatalf("window %s = %v, want %v", window, got, want)
		}
	}
	ok("30m", 30*time.Minute)
	ok("1800s", 30*time.Minute)
	ok("24h", 24*time.Hour)
}

func TestDLQBadValues(t *testing.T) {
	tests := []struct {
		name  string
		block string
		want  []string
	}{
		{"fire zero", "signals:\n        dlq_growth: {fire: 0}\n", []string{"watch.signals.dlq_growth", "fire", "at least 1"}},
		{"fire negative", "signals:\n        dlq_growth: {fire: -3}\n", []string{"watch.signals.dlq_growth", "fire"}},
		{"fire as a size", "signals:\n        dlq_growth: {fire: 1k}\n", []string{"watch.signals.dlq_growth", "fire", "whole number of records"}},
		{"fire as a byte size", "signals:\n        dlq_growth: {fire: 1GiB}\n", []string{"fire", "whole number of records"}},
		{"fire fractional", "signals:\n        dlq_growth: {fire: 1.5}\n", []string{"fire", "whole number"}},
		{"fire too large", "signals:\n        dlq_growth: {fire: 99999999999999999999}\n", []string{"fire"}},
		{"window 15m", "signals:\n        dlq_growth: {window: 15m}\n", []string{"watch.signals.dlq_growth", "window", "at least 30m"}},
		{"window 29m59s", "signals:\n        dlq_growth: {window: 29m59s}\n", []string{"window", "at least 30m"}},
		{"window bare number", "signals:\n        dlq_growth: {window: 3600}\n", []string{"window", "unit"}},
		{"window zero", "signals:\n        dlq_growth: {window: 0s}\n", []string{"window", "greater than zero"}},
		{"window words", "signals:\n        dlq_growth: {window: 1 hour}\n", []string{"window", "duration"}},
		{"bad action", "signals:\n        dlq_growth: {action: page}\n", []string{"dlq_growth", "action", "log or room"}},
		{"bad for", "signals:\n        dlq_growth: {for: soon}\n", []string{"dlq_growth", "for"}},
		{"unknown key on dlq_growth", "signals:\n        dlq_growth: {clear: 1}\n", []string{"watch.signals.dlq_growth", `unknown field "clear"`}},
		{"window on another signal", "signals:\n        slot_wal: {window: 1h}\n", []string{"watch.signals.slot_wal", `unknown field "window"`}},
		{"fire on config_risk", "signals:\n        config_risk: {fire: 1}\n", []string{"watch.signals.config_risk", `unknown field "fire"`}},
		{"window on config_risk", "signals:\n        config_risk: {window: 1h}\n", []string{"watch.signals.config_risk", `unknown field "window"`}},
		{"bad config_risk action", "signals:\n        config_risk: {action: pager}\n", []string{"config_risk", "action"}},
		{"unknown signal lists the new ones", "signals:\n        dlq_grow: {}\n", []string{"watch.signals.dlq_grow", "unknown signal", "dlq_growth", "config_risk"}},
		{"dlq_peek loud", "dlq_peek:\n        headers: loud\n", []string{"watch.dlq_peek.headers", `"loud"`, "counts_only", "classes", "messages"}},
		{"dlq_peek unknown key", "dlq_peek:\n        header: classes\n", []string{"watch.dlq_peek", `unknown field "header"`, "headers"}},
		{"dlq_topics unknown key", "dlq_topics:\n        - {topic: x-dlq, written_by: order-sync, writen_by: x}\n", []string{"watch.dlq_topics[]", `unknown field "writen_by"`, "written_by"}},
		{"dlq_topics key typo drops nothing silently", "dlq_topics:\n        - {topic: x-dlq, writer: order-sync}\n", []string{`unknown field "writer"`}},
		{"unknown written_by", "dlq_topics:\n        - {topic: x-dlq, written_by: nobody}\n", []string{"watch.dlq_topics[0]", `"nobody"`, "neither a consumer group nor a connector", `"orders"`}},
		{"written_by from another pipeline", "dlq_topics:\n        - {topic: x-dlq, written_by: pay-sync}\n", []string{"written_by", `"pay-sync"`}},
		{"missing written_by", "dlq_topics:\n        - {topic: x-dlq}\n", []string{"written_by is required"}},
		{"missing topic", "dlq_topics:\n        - {written_by: order-sync}\n", []string{"watch.dlq_topics[0]", "not a valid Kafka topic name"}},
		{"topic with space", "dlq_topics:\n        - {topic: \"a b\", written_by: order-sync}\n", []string{"not a valid Kafka topic name"}},
		{"topic with slash", "dlq_topics:\n        - {topic: \"a/b\", written_by: order-sync}\n", []string{"not a valid Kafka topic name"}},
		{"topic dot", "dlq_topics:\n        - {topic: \".\", written_by: order-sync}\n", []string{"not a valid Kafka topic name"}},
		{"topic dot dot", "dlq_topics:\n        - {topic: \"..\", written_by: order-sync}\n", []string{"not a valid Kafka topic name"}},
		{"topic 250 characters", "dlq_topics:\n        - {topic: " + strings.Repeat("a", 250) + ", written_by: order-sync}\n", []string{"not a valid Kafka topic name"}},
		{"topic is a CDC topic of this pipeline", "dlq_topics:\n        - {topic: pg.public.orders, written_by: order-sync}\n", []string{`pipeline "orders"`, `"pg.public.orders"`, "CDC topic"}},
		{"duplicate in one pipeline", "dlq_topics:\n        - {topic: x-dlq, written_by: order-sync}\n        - {topic: x-dlq, written_by: pg-orders-sink}\n", []string{"watch.dlq_topics[1]", `"x-dlq"`, "declared twice"}},
		{"dlq_topics not a list", "dlq_topics: x-dlq\n", []string{"cannot unmarshal"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yml := twoPipelines("    watch:\n      "+tc.block, "")
			_, err := Load(writeTemp(t, yml))
			if err == nil {
				t.Fatal("want an error")
			}
			for _, sub := range tc.want {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error %q does not contain %q", err, sub)
				}
			}
		})
	}
}

func TestDLQTopicsValid(t *testing.T) {
	o, err := Load(writeTemp(t, twoPipelines(`    watch:
      dlq_topics:
        - topic: order-sync-dlq
          written_by: order-sync
        - topic: orders.sink_DLQ-1
          written_by: pg-orders-sink
          shared: false
`, "")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	want := []DLQTopic{
		{Topic: "order-sync-dlq", WrittenBy: "order-sync"},
		{Topic: "orders.sink_DLQ-1", WrittenBy: "pg-orders-sink"},
	}
	if got := p.Watch().DLQTopics; !reflect.DeepEqual(got, want) {
		t.Fatalf("DLQTopics = %+v, want %+v", got, want)
	}
	// A pipeline without a watch block has none.
	if pp, _ := o.Pipeline("payments"); len(pp.Watch().DLQTopics) != 0 {
		t.Fatalf("payments DLQTopics = %v", pp.Watch().DLQTopics)
	}
	// Watch() hands out a copy: editing it does not change the pipeline.
	got := p.Watch().DLQTopics
	got[0].Topic = "tampered"
	if p.Watch().DLQTopics[0].Topic != "order-sync-dlq" {
		t.Fatal("Watch().DLQTopics shares memory with the pipeline")
	}
}

func TestDLQTopic249Characters(t *testing.T) {
	long := strings.Repeat("a", 249)
	o, err := Load(writeTemp(t, twoPipelines("    watch:\n      dlq_topics:\n        - {topic: "+long+", written_by: order-sync}\n", "")))
	if err != nil {
		t.Fatalf("a 249-character topic is legal: %v", err)
	}
	p, _ := o.Pipeline("orders")
	if p.Watch().DLQTopics[0].Topic != long {
		t.Fatal("topic changed")
	}
}

// A DLQ topic may not equal the CDC topic of ANY pipeline, not only its own.
func TestDLQTopicIsCDCTopicOfAnotherPipeline(t *testing.T) {
	_, err := Load(writeTemp(t, twoPipelines(`    watch:
      dlq_topics:
        - {topic: pay.public.payments, written_by: order-sync}
`, "")))
	if err == nil || !strings.Contains(err.Error(), "CDC topic") || !strings.Contains(err.Error(), "pay.public.payments") {
		t.Fatalf("err = %v, want a CDC topic refusal", err)
	}
}

func TestDLQSharedAcrossPipelines(t *testing.T) {
	decl := func(shared string) string {
		s := ""
		if shared != "" {
			s = ", shared: " + shared
		}
		return "{topic: all-dlq, written_by: %s" + s + "}"
	}
	orders := func(shared string) string {
		return "    watch:\n      dlq_topics:\n        - " + strings.Replace(decl(shared), "%s", "order-sync", 1) + "\n"
	}
	payments := func(shared string) string {
		return "    watch:\n      dlq_topics:\n        - " + strings.Replace(decl(shared), "%s", "pay-sync", 1) + "\n"
	}
	tests := []struct {
		name            string
		orders, payment string
		wantErr         bool
	}{
		{"neither shared", orders(""), payments(""), true},
		{"both explicitly false", orders("false"), payments("false"), true},
		{"only the first shared", orders("true"), payments(""), true},
		{"only the second shared", orders(""), payments("true"), true},
		{"both shared", orders("true"), payments("true"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, err := Load(writeTemp(t, twoPipelines(tc.orders, tc.payment)))
			if tc.wantErr {
				if err == nil {
					t.Fatal("want an error")
				}
				for _, sub := range []string{`"all-dlq"`, `"orders"`, `"payments"`, "shared: true"} {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error %q does not contain %q", err, sub)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			for _, id := range []string{"orders", "payments"} {
				p, _ := o.Pipeline(id)
				if d := p.Watch().DLQTopics; len(d) != 1 || !d[0].Shared {
					t.Errorf("%s DLQTopics = %+v, want one shared topic", id, d)
				}
			}
		})
	}
}

// shared: true on a topic only one pipeline declares is allowed (the other
// side may be a discovered sink DLQ the file cannot see).
func TestDLQSharedAloneIsFine(t *testing.T) {
	if _, err := Load(writeTemp(t, twoPipelines("    watch:\n      dlq_topics:\n        - {topic: all-dlq, written_by: order-sync, shared: true}\n", ""))); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

// Three pipelines on one topic: all three must agree.
func TestDLQSharedByThree(t *testing.T) {
	yml := twoPipelines("    watch:\n      dlq_topics:\n        - {topic: all-dlq, written_by: order-sync, shared: true}\n",
		"    watch:\n      dlq_topics:\n        - {topic: all-dlq, written_by: pay-sync, shared: true}\n") + `
`
	yml = strings.Replace(yml, "layers:", `  - id: refunds
    topic_prefix: ref
    source_tables: [public.refunds]
    connectors: [ref-connector]
    consumer_groups: [ref-sync]
    watch:
      dlq_topics:
        - {topic: all-dlq, written_by: ref-sync}
layers:`, 1)
	_, err := Load(writeTemp(t, yml))
	if err == nil || !strings.Contains(err.Error(), `"refunds"`) {
		t.Fatalf("err = %v, want a sharing refusal naming refunds", err)
	}
}

// A disabled watcher still validates its declarations.
func TestDLQTopicsValidatedWhenWatchDisabled(t *testing.T) {
	_, err := Load(writeTemp(t, withWatch("    watch:\n      enabled: false\n      dlq_topics:\n        - {topic: x, written_by: nobody}\n")))
	if err == nil {
		t.Fatal("want an error")
	}
}

// Every owners file in the repo still loads, and the new defaults apply.
func TestDLQRepoOwnersFilesLoadWithDefaults(t *testing.T) {
	root := filepath.Join("..", "..")
	files := []string{
		"dev/owners.yaml",
		"deploy/config/owners.example.yaml",
		"test/e2e/oidc/config/owners.yaml",
	}
	for _, f := range files {
		t.Run(f, func(t *testing.T) {
			o, err := Load(filepath.Join(root, filepath.FromSlash(f)))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if len(o.Pipelines) == 0 {
				t.Fatal("no pipelines")
			}
			for _, p := range o.Pipelines {
				w := p.Watch()
				if w.DLQGrowth.Window != time.Hour || w.DLQPeekHeaders != DLQHeadersClasses || w.ConfigRisk.Action != ActionLog {
					t.Errorf("%s: DLQ defaults not applied: %+v", p.ID, w)
				}
			}
		})
	}
}
