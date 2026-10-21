package main

import (
	"strings"
	"testing"
)

func TestMissedTableKafkaSignalBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()

	if err := breakMissedTableKafkaSignal(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if !w.refundsExists || !w.signalExists || w.refundsInPub || w.refundRows != refundRows {
			t.Errorf("after break: refunds=%v signal=%v inPub=%v rows=%d", w.refundsExists, w.signalExists, w.refundsInPub, w.refundRows)
		}
		cfg := w.connCfg
		if cfg["publication.autocreate.mode"] != "disabled" ||
			cfg["signal.data.collection"] != signalTable ||
			cfg["signal.enabled.channels"] != "source,kafka" ||
			cfg["signal.kafka.topic"] != kafkaSignalTopic ||
			cfg["signal.kafka.bootstrap.servers"] != "kafka:9092" {
			t.Errorf("connector config after break = %v", cfg)
		}
		if !strings.Contains(cfg["table.include.list"].(string), "public.devctl_refunds") {
			t.Errorf("include list after break = %v", cfg["table.include.list"])
		}
		if cfg["database.hostname"] != "postgres" {
			t.Errorf("break lost the base connector keys: %v", cfg)
		}
	})
	// The Kafka signal topic exists with one partition and holds no records.
	span, ok, err := s.topicSpan(ctx, kafkaSignalTopic)
	if err != nil || !ok || span.records() != 0 {
		t.Fatalf("signal topic after break: %v %v %v", span, ok, err)
	}
	// The refunds still never reached Kafka: the break is a missed-table.
	if span, ok, err := s.topicSpan(ctx, refundsTopic); err != nil || (ok && span.records() > 0) {
		t.Fatalf("refunds reached Kafka during the break: %v %v %v", span, ok, err)
	}

	if err := fixMissedTableKafkaSignal(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.refundsExists || w.signalExists || w.refundsInPub {
			t.Errorf("fix left devctl_* objects: refunds=%v signal=%v inPub=%v", w.refundsExists, w.signalExists, w.refundsInPub)
		}
		if w.connCfg["publication.autocreate.mode"] != "filtered" || w.connCfg["table.include.list"] != "public.orders" {
			t.Errorf("connector config after fix = %v, want the dev/connectors baseline", w.connCfg)
		}
		if _, ok := w.connCfg["signal.enabled.channels"]; ok {
			t.Errorf("fix left signal.enabled.channels = %v", w.connCfg["signal.enabled.channels"])
		}
	})
	for _, topic := range []string{refundsTopic, signalTopic, kafkaSignalTopic} {
		if _, ok, err := s.topicSpan(ctx, topic); err != nil || ok {
			t.Errorf("topic %s still exists after fix (err %v)", topic, err)
		}
	}
	// Idempotent: a fixed stack is fixed again without error.
	if err := fixMissedTableKafkaSignal(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
}

// The break refuses a table that is already published, like missed-table does.
func TestMissedTableKafkaSignalBreakRefusesPublishedTable(t *testing.T) {
	w := newP3World(t)
	w.refundsInPub = true
	s, _ := w.stack()
	err := breakMissedTableKafkaSignal(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "already in "+publicationName) {
		t.Fatalf("break = %v, want already-published refusal", err)
	}
}
