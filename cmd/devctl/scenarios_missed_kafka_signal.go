package main

import (
	"context"
	"fmt"
	"time"
)

// missed-table-kafka-signal is the missed-table break with Debezium's Kafka
// signal channel switched on (signal.enabled.channels includes "kafka"), for
// the first live check of #620's signal format: connect_snapshot_table via
// signal_topic. The failure itself is unchanged — public.devctl_refunds is in
// table.include.list but not in the publication, so its rows never reach Kafka
// and nothing fails — but the connector now takes its snapshot signal from a
// Kafka topic (devctl.signal) instead of the source signal table.

// kafkaSignalTopic is the connector's signal.kafka.topic during the break; one
// partition, because Debezium reads signals from partition 0 only.
const kafkaSignalTopic = "devctl.signal"

// missedTableKafkaSignalConnector is the connector config of the break: the
// publication is the DBA's (autocreate disabled), the new table is in the
// include list but not published, and the Kafka signal channel is on.
var missedTableKafkaSignalConnector = map[string]string{
	"publication.autocreate.mode":    "disabled",
	"table.include.list":             "public.orders,public.devctl_refunds,public.devctl_signal",
	"signal.data.collection":         "public.devctl_signal", // watermarks, even with the Kafka channel
	"signal.enabled.channels":        "source,kafka",
	"signal.kafka.topic":             kafkaSignalTopic,
	"signal.kafka.bootstrap.servers": "kafka:9092",
}

// missedTableKafkaSignalAlert is the reconciliation alert this break needs: no
// watcher signal sees a table that is missing from Kafka, so the room opens
// through `devctl alert missed-table-kafka-signal`.
const missedTableKafkaSignalAlert = `{
  "title": "orders: refunds not arriving in Kafka (Kafka signalling enabled)",
  "severity": "sev2",
  "pipeline_id": "orders",
  "dedup_key": "devctl-missed-table-kafka-signal",
  "source": "reconciliation",
  "description": "public.devctl_refunds has rows in the source database, but topic pg.public.devctl_refunds has no records. The connector reports RUNNING and takes snapshot signals over Kafka (signal.kafka.topic devctl.signal)."
}`

func breakMissedTableKafkaSignal(ctx context.Context, s *stack) error {
	if n, err := s.psql(ctx, "orders", sqlRefundsInPub); err != nil {
		return err
	} else if n != "0" {
		return fmt.Errorf("devctl: %s is already in %s — run `devctl fix missed-table-kafka-signal` (or reset) first", refundsTable, publicationName)
	}
	// The signal topic must exist before the connector starts its Kafka signal
	// channel; Debezium reads partition 0 of a one-partition topic only.
	if err := s.ensureTopic(ctx, kafkaSignalTopic, 1); err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlCreateSignal, sqlCreateRefunds); err != nil {
		return err
	}
	if err := s.registerConnectorWith(ctx, missedTableKafkaSignalConnector); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return fmt.Errorf("devctl: connector not RUNNING after the include-list/signalling change: %w", err)
	}
	// With autocreate disabled Debezium no longer touches the publication, so
	// this is the state it streams from: the new table is not published.
	if _, err := s.psql(ctx, "orders", sqlPubWithoutRefunds, sqlInsertRefunds); err != nil {
		return err
	}
	// Proof: an order written after the refunds reaches the warehouse, so the
	// connector streams; the refunds, written first, are not in Kafka.
	if err := s.waitMarkerOrder(ctx); err != nil {
		return err
	}
	span, ok, err := s.topicSpan(ctx, refundsTopic)
	if err != nil {
		return err
	}
	if ok && span.records() > 0 {
		return fmt.Errorf("devctl: %s has %d records: the break did not hold", refundsTopic, span.records())
	}
	s.log.Info("broken: "+refundsTable+" is in table.include.list but not in "+publicationName+" — its rows never reach Kafka, and nothing fails; Kafka signalling is on ("+kafkaSignalTopic+")",
		"refund_rows", refundRows, "topic", refundsTopic)
	return nil
}

// The fix is what missed-table does, plus removing the Kafka signal topic: add
// the table to the publication (new changes flow), ask Debezium for an
// incremental snapshot (the rows written before), then put the stack back as
// `reset` leaves it — the connector config from dev/connectors (Kafka
// signalling off again), the publication on orders only, and no devctl_* table,
// topic or subject.
func fixMissedTableKafkaSignal(ctx context.Context, s *stack) error {
	if err := fixMissedTable(ctx, s); err != nil {
		return err
	}
	if err := s.deleteTopics(ctx, kafkaSignalTopic); err != nil {
		return err
	}
	s.log.Info("fixed: Kafka signal topic " + kafkaSignalTopic + " removed")
	return nil
}
