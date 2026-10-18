package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// Debezium's Kafka signal channel (signal.enabled.channels includes "kafka"):
// the connector consumes partition 0 of signal.kafka.topic, skips a record
// whose key is not its logical name (topic.prefix), reads the value as
// {"type": ..., "data": {...}} and uses the record offset as the signal id.
// The execute-snapshot data is the same JSON the signal table row carries
// (internal/clients/postgres InsertSnapshotSignal).

// maxSignalTables caps the tables of one snapshot signal.
const maxSignalTables = 50

// signalTableRe is one "schema.table" of a signal: AGENTS.md §4 rule 6 for
// each part. Debezium reads data-collections as regular expressions; with
// this check "." is the only metacharacter left, and it can only match the
// schema separator of a name of the same length, so an entry matches its own
// table and no other.
var signalTableRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`)

// SnapshotSignal is the record SendSnapshotSignal produced.
type SnapshotSignal struct {
	// ID is the signal id Debezium logs for it: the record offset.
	ID        string `json:"id"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type snapshotSignalValue struct {
	Type string             `json:"type"`
	Data snapshotSignalData `json:"data"`
}

type snapshotSignalData struct {
	DataCollections []string `json:"data-collections"`
	Type            string   `json:"type"`
}

// SnapshotSignalValue is the value of an incremental execute-snapshot signal
// for tables ("schema.table" each, deduplicated in order). It refuses an
// empty list, more than 50 tables and a name that is not two identifiers.
func SnapshotSignalValue(tables []string) ([]byte, error) {
	list, err := signalTables(tables)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(snapshotSignalValue{Type: "execute-snapshot", Data: snapshotSignalData{DataCollections: list, Type: "incremental"}})
	if err != nil {
		return nil, fmt.Errorf("kafka: snapshot signal: encode: %w", err)
	}
	return b, nil
}

// StopSnapshotSignalValue is the value of the stop-snapshot signal that ends
// an incremental snapshot of tables (the rollback text of a preview).
func StopSnapshotSignalValue(tables []string) ([]byte, error) {
	list, err := signalTables(tables)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(snapshotSignalValue{Type: "stop-snapshot", Data: snapshotSignalData{DataCollections: list, Type: "incremental"}})
	if err != nil {
		return nil, fmt.Errorf("kafka: stop-snapshot signal: encode: %w", err)
	}
	return b, nil
}

func signalTables(tables []string) ([]string, error) {
	if len(tables) == 0 {
		return nil, errors.New("kafka: snapshot signal: no tables")
	}
	seen := make(map[string]bool, len(tables))
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		if !signalTableRe.MatchString(t) {
			return nil, fmt.Errorf("kafka: snapshot signal: table %q is not schema.table (letters, digits and _)", cut(t))
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	if len(out) > maxSignalTables {
		return nil, fmt.Errorf("kafka: snapshot signal: %d tables, at most %d per signal", len(out), maxSignalTables)
	}
	return out, nil
}

// SendSnapshotSignal produces one execute-snapshot signal asking connector
// for an incremental snapshot of tables, to signalTopic (the connector's
// signal.kafka.topic), keyed by logicalName (its topic.prefix), and waits
// for the ack.
//
// It exists next to Produce because Produce only writes the pipeline's own
// CDC topics ("<topic_prefix>.<source_table>"), and a signal topic is none of
// them. The scope exemption is narrow: the record is built here, and only
// ever asks for a snapshot. With the room's pipeline in ctx, connector must
// be one of its connectors, every table one of its source tables and
// logicalName its topic_prefix (tools.ErrOutOfScope); without a scope it
// fails closed as Produce does (tools.ErrNoScope, unless tools.WithUnscoped).
// The caller names the topic from the connector's own config; that the topic
// is the connector's is not checked here, since the Writer cannot read
// Connect.
//
// Debezium consumes partition 0 of the signal topic only, so a topic with
// more partitions is refused; a missing one is clients.ErrNotFound. Every
// check runs before anything is produced.
func (w *Writer) SendSnapshotSignal(ctx context.Context, signalTopic, connector, logicalName string, tables []string) (SnapshotSignal, error) {
	switch {
	case strings.TrimSpace(signalTopic) == "":
		return SnapshotSignal{}, errors.New("kafka: snapshot signal: empty signal topic")
	case strings.TrimSpace(connector) == "":
		return SnapshotSignal{}, errors.New("kafka: snapshot signal: no connector given")
	case strings.TrimSpace(logicalName) == "":
		return SnapshotSignal{}, errors.New("kafka: snapshot signal: empty key (the connector's topic.prefix)")
	}
	list, err := signalTables(tables)
	if err != nil {
		return SnapshotSignal{}, err
	}
	if err := checkWriteScope(ctx, func(sc tools.Scope) error { return signalScope(sc, connector, logicalName, list) }); err != nil {
		return SnapshotSignal{}, err
	}
	value, err := SnapshotSignalValue(list)
	if err != nil {
		return SnapshotSignal{}, err
	}
	ctx, cancel := withDefaultTimeout(ctx, writeCallTimeout)
	defer cancel()
	if err := w.signalTopicReady(ctx, signalTopic); err != nil {
		return SnapshotSignal{}, fmt.Errorf("kafka: snapshot signal to %s: %w", signalTopic, err)
	}
	rec := &kgo.Record{Topic: signalTopic, Key: []byte(logicalName), Value: value}
	start := time.Now()
	err = w.produceOne(ctx, rec)
	w.c.record(ctx, start, "kgo.Produce(signal record)", "topic="+signalTopic, "tables="+strconv.Itoa(len(list)))
	if err != nil {
		return SnapshotSignal{}, fmt.Errorf("kafka: snapshot signal to %s: not acked: %w", signalTopic, err)
	}
	sig := SnapshotSignal{ID: strconv.FormatInt(rec.Offset, 10), Topic: signalTopic, Partition: rec.Partition, Offset: rec.Offset}
	slog.InfoContext(ctx, "kafka: snapshot signal produced", "topic", signalTopic, "connector", connector, "tables", len(list), "offset", rec.Offset)
	return sig, nil
}

// signalScope is SendSnapshotSignal's check against the room's pipeline.
func signalScope(sc tools.Scope, connector, logicalName string, tables []string) error {
	if err := sc.CheckConnector(connector); err != nil {
		return err
	}
	for _, t := range tables {
		if err := sc.CheckSourceTable(t); err != nil {
			return err
		}
	}
	if p := sc.Pipeline(); !sc.Unscoped() && logicalName != p.TopicPrefix {
		return fmt.Errorf("%w: signal key %q is not pipeline %q's topic_prefix %q", tools.ErrOutOfScope, cut(logicalName), p.ID, p.TopicPrefix)
	}
	return nil
}

// signalTopicReady refuses a missing signal topic (clients.ErrNotFound) and
// one with other than exactly one partition.
func (w *Writer) signalTopicReady(ctx context.Context, topic string) error {
	start := time.Now()
	tds, err := w.c.metadata().ListTopics(ctx, topic)
	w.c.record(ctx, start, "kadm.ListTopics(topic)", "topic="+topic)
	if err != nil {
		return fmt.Errorf("topic metadata: %w", err)
	}
	td, ok := tds[topic]
	if !ok {
		return fmt.Errorf("topic %s: %w", topic, clients.ErrNotFound)
	}
	if td.Err != nil {
		return fmt.Errorf("topic %s: %w", topic, mapNotFound(td.Err))
	}
	if n := len(td.Partitions); n != 1 {
		return fmt.Errorf("topic %s has %d partitions; Debezium reads signals from partition 0 of a one-partition topic only, so a keyed signal could land where the connector never reads it", topic, n)
	}
	return nil
}

// cut caps a caller-supplied name before it is echoed in an error.
func cut(s string) string {
	const n = 80
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}
