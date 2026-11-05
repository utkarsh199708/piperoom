package kafka

import (
	"bytes"
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

// SnapshotSignal is the record SendSnapshotSignal produced; ID is the signal
// id Debezium logs for it (the record offset). An alias, so the Writer
// satisfies clients.SnapshotSignaler (P0 item 7), as with OffsetMove.
type SnapshotSignal = clients.SnapshotSignal

var (
	_ clients.SnapshotSignaler          = (*Writer)(nil)
	_ clients.SnapshotConditionSignaler = (*Writer)(nil)
)

type snapshotSignalValue struct {
	Type string             `json:"type"`
	Data snapshotSignalData `json:"data"`
}

type snapshotSignalData struct {
	DataCollections []string `json:"data-collections"`
	Type            string   `json:"type"`
	// AdditionalConditions is set by SendSnapshotSignalWhere only (X2a), so
	// an unconditional signal's bytes are unchanged.
	AdditionalConditions []signalCondition `json:"additional-conditions,omitempty"`
}

// signalCondition is one entry of Debezium's additional-conditions: the
// snapshot of DataCollection reads only the rows matching Filter.
type signalCondition struct {
	DataCollection string `json:"data-collection"`
	Filter         string `json:"filter"`
}

// SnapshotSignalValue is the value of an incremental execute-snapshot signal
// for tables ("schema.table" each, deduplicated in order). It refuses an
// empty list, more than 50 tables and a name that is not two identifiers.
func SnapshotSignalValue(tables []string) ([]byte, error) {
	list, err := signalTables(tables)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(snapshotSignalValue{Type: "execute-snapshot", Data: snapshotSignalData{DataCollections: list, Type: "INCREMENTAL"}})
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
	b, err := json.Marshal(snapshotSignalValue{Type: "stop-snapshot", Data: snapshotSignalData{DataCollections: list, Type: "INCREMENTAL"}})
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
// Connect. What is checked is that it is no CDC topic: never
// "<logicalName>.<table>" for a requested table, and with the room's
// pipeline in ctx never "<topic_prefix>.<source_table>" for any of its source
// tables (tools.ErrOutOfScope), so this narrow path cannot put a signal
// record among the data records consumers read.
//
// Debezium consumes partition 0 of the signal topic only, so a topic with
// more partitions is refused; a missing one is clients.ErrNotFound. Every
// check runs before anything is produced.
func (w *Writer) SendSnapshotSignal(ctx context.Context, signalTopic, connector, logicalName string, tables []string) (SnapshotSignal, error) {
	return w.sendSignal(ctx, signalTopic, connector, logicalName, tables, "")
}

// SendSnapshotSignalWhere implements clients.SnapshotConditionSignaler (X2a,
// pg_resnapshot_rows on a connector with signal.kafka.topic):
// SendSnapshotSignal whose data also carries Debezium's
// additional-conditions, one {"data-collection", "filter"} entry per table,
// so the connector re-reads only the matching rows. The condition is built
// by the action from typed params; it is checked here with
// clients.ValidateSnapshotCondition before anything else (an empty one
// included: an unconditional snapshot is SendSnapshotSignal), and it is
// never logged or put in a provenance receipt or an error. Every other rule
// is SendSnapshotSignal's.
func (w *Writer) SendSnapshotSignalWhere(ctx context.Context, signalTopic, connector, logicalName string, tables []string, additionalCondition string) (SnapshotSignal, error) {
	if err := clients.ValidateSnapshotCondition(additionalCondition); err != nil {
		return SnapshotSignal{}, fmt.Errorf("kafka: snapshot signal: %w", err)
	}
	return w.sendSignal(ctx, signalTopic, connector, logicalName, tables, additionalCondition)
}

// conditionalSignalValue is SnapshotSignalValue with cond as every table's
// additional condition. No HTML escaping, so the condition's >= stays as
// written.
func conditionalSignalValue(list []string, cond string) ([]byte, error) {
	data := snapshotSignalData{DataCollections: list, Type: "INCREMENTAL"}
	for _, t := range list {
		data.AdditionalConditions = append(data.AdditionalConditions, signalCondition{DataCollection: t, Filter: cond})
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(snapshotSignalValue{Type: "execute-snapshot", Data: data}); err != nil {
		return nil, fmt.Errorf("kafka: snapshot signal: encode: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// sendSignal is SendSnapshotSignal with an optional row condition (cond ""
// = none); a non-empty cond was validated by the caller.
func (w *Writer) sendSignal(ctx context.Context, signalTopic, connector, logicalName string, tables []string, cond string) (SnapshotSignal, error) {
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
	for _, t := range list {
		if signalTopic == logicalName+"."+t {
			return SnapshotSignal{}, fmt.Errorf("kafka: snapshot signal: %w: signal topic %q is the CDC topic of %s", tools.ErrOutOfScope, cut(signalTopic), t)
		}
	}
	if err := checkWriteScope(ctx, func(sc tools.Scope) error { return signalScope(sc, signalTopic, connector, logicalName, list) }); err != nil {
		return SnapshotSignal{}, err
	}
	var value []byte
	if cond == "" {
		value, err = SnapshotSignalValue(list)
	} else {
		value, err = conditionalSignalValue(list, cond)
	}
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
func signalScope(sc tools.Scope, signalTopic, connector, logicalName string, tables []string) error {
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
	if p := sc.Pipeline(); !sc.Unscoped() {
		for _, t := range p.SourceTables {
			if signalTopic == p.TopicPrefix+"."+t {
				return fmt.Errorf("%w: signal topic %q is pipeline %q's CDC topic of %s", tools.ErrOutOfScope, cut(signalTopic), p.ID, t)
			}
		}
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
