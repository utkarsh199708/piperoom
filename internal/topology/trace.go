package topology

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// pkColumner is implemented by SourceDB implementations that can report a
// table's primary key column. Others fall back to "id".
type pkColumner interface {
	PrimaryKeyColumn(ctx context.Context, table string) (string, error)
}

// maxScan bounds FindByKey scans per partition during a trace.
const maxScan = 1000

// traceFacts holds the observations Trace gathers before deciding statuses.
type traceFacts struct {
	table          string
	connector      string
	topic          string
	group          string
	sink           string
	sourceErr      error
	sourceFound    bool
	sourceTS       time.Time
	connectorErr   error
	connectorState string
	connectorStuck bool // connector or any task is not RUNNING
	tasksFailed    int
	topicErr       error
	topicFound     bool
	topicOffset    int64
	topicPartition int32
	groupErr       error
	groupCommitted int64 // committed offset for the record's partition; -1 if unknown
	sinkErr        error
	sinkFound      bool
	sinkDiff       []string
}

// Trace follows one source row by primary key through every pipeline stage.
// Unreachable systems produce "unknown" stages, never an error for the whole
// trace.
func (b *builder) Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error) {
	p, ok := b.o.Pipeline(pipelineID)
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrBadInput, pipelineID)
	}
	if key == "" {
		return nil, fmt.Errorf("%w: trace: empty key", ErrBadInput)
	}
	if len(p.ConsumerGroups) == 0 || len(p.SinkTables) == 0 {
		return nil, fmt.Errorf("topology: pipeline %q has no consumer group or sink table", pipelineID)
	}

	f := traceFacts{
		table:          table,
		group:          p.ConsumerGroups[0],
		sink:           p.SinkTables[0],
		groupCommitted: -1,
	}

	// Resolve the connector and topic. A Connect outage must not fail the
	// trace: fall back to placeholder names and mark the connector unknown.
	conn, topic, cerr := b.connectorForTable(ctx, p, table)
	if cerr != nil {
		if errors.Is(cerr, ErrBadInput) {
			return nil, cerr
		}
		conn = p.Connectors[0]
		topic = fallbackTopic(table)
		f.connectorErr = cerr
	}
	f.connector, f.topic = conn, topic

	// Primary key lookup. A failure here means the source DB is unreachable;
	// fall back to "id" and let the source row read report the problem.
	pk, err := b.pkColumn(ctx, table)
	if err != nil {
		pk = "id"
	}

	// 1. Source row.
	srcRow, found, err := b.src.Row(ctx, table, pk, key)
	if err != nil {
		f.sourceErr = err
	} else {
		f.sourceFound = found
		if found {
			f.sourceTS = tsFromRow(srcRow)
		}
	}

	// 2. Connector status (skip when Connect is already known unreachable).
	if f.connectorErr == nil {
		st, err := b.connect.Status(ctx, conn)
		f.connectorErr = err
		if err == nil {
			f.connectorState = st.State
			f.connectorStuck = st.State != "RUNNING"
			for _, task := range st.Tasks {
				if task.State == "FAILED" {
					f.tasksFailed++
				}
				if task.State != "RUNNING" {
					f.connectorStuck = true
				}
			}
		}
	}

	// 3. Topic: search for the record from just before it was written.
	since := f.sourceTS.Add(-time.Minute)
	if f.sourceTS.IsZero() {
		since = time.Now().Add(-time.Hour)
	}
	refs, err := b.kafka.FindByKey(ctx, topic, key, since, maxScan)
	f.topicErr = err
	if err == nil && len(refs) > 0 {
		// Use the newest matching record: the consumer commits past it.
		last := refs[len(refs)-1]
		f.topicFound = true
		f.topicOffset = last.Offset
		f.topicPartition = last.Partition
	}

	// 4. Consumer group: committed offset for the record's partition.
	if f.topicFound {
		lag, err := b.kafka.GroupLag(ctx, f.group)
		f.groupErr = err
		if err == nil {
			for _, pl := range lag.Partitions {
				if pl.Topic == topic && pl.Partition == f.topicPartition {
					f.groupCommitted = pl.Committed
					break
				}
			}
		}
	}

	// 5. Sink row and column diff.
	sinkRow, found, err := b.wh.Row(ctx, f.sink, pk, key)
	f.sinkErr = err
	f.sinkFound = found
	if found && f.sourceFound {
		f.sinkDiff = diffColumns(srcRow, sinkRow)
	}

	return decideTrace(f), nil
}

// connectorForTable returns the connector that streams table and its Debezium
// topic name.
func (b *builder) connectorForTable(ctx context.Context, p owners.Pipeline, table string) (string, string, error) {
	var lastErr error
	for _, cn := range p.Connectors {
		cfg, err := b.connect.Config(ctx, cn)
		if err != nil {
			lastErr = err
			continue
		}
		if streamsTable(cfg["table.include.list"], table) {
			return cn, cfg["topic.prefix"] + "." + table, nil
		}
	}
	if lastErr != nil {
		return "", "", fmt.Errorf("topology: connector config: %w", lastErr)
	}
	return "", "", fmt.Errorf("%w: no connector streams table %q in pipeline %q", ErrBadInput, table, p.ID)
}

// fallbackTopic is used when connector config is unavailable, so the trace
// keeps a topic stage. The Debezium topic prefix is unknown, so the
// fully-qualified table name stands in for the topic.
func fallbackTopic(table string) string {
	return table
}

func (b *builder) pkColumn(ctx context.Context, table string) (string, error) {
	if d, ok := b.src.(pkColumner); ok {
		col, err := d.PrimaryKeyColumn(ctx, table)
		if err != nil {
			return "", fmt.Errorf("topology: primary key of %s: %w", table, err)
		}
		if col != "" {
			return col, nil
		}
	}
	return "id", nil
}

// decideTrace maps gathered facts to one TraceStage per layer. Exactly one
// stage is "stopped_here" when the record is stuck.
func decideTrace(f traceFacts) []events.TraceStage {
	stages := make([]events.TraceStage, 0, 5)
	stage := func(layer, name, status string, facts map[string]string) {
		stages = append(stages, events.TraceStage{Layer: layer, Name: name, Status: status, Facts: facts})
	}

	// 1. Source.
	if f.sourceErr != nil {
		stage("postgres", f.table, "unknown", map[string]string{"error": "unreachable"})
	} else if !f.sourceFound {
		stage("postgres", f.table, "not_seen", nil)
		stage("kafka-connect", f.connector, "not_seen", nil)
		stage("kafka", f.topic, "not_seen", nil)
		stage("consumer", f.group, "not_seen", nil)
		stage("warehouse", f.sink, "not_seen", nil)
		return stages
	} else {
		srcFacts := map[string]string{}
		if !f.sourceTS.IsZero() {
			srcFacts["updated_at"] = FormatTS(f.sourceTS)
		}
		stage("postgres", f.table, "seen", srcFacts)
	}

	// 2. Connector. It is "seen" only when the record actually reached the
	// topic; a non-RUNNING connector/task while the record is missing from
	// the topic is the stuck point.
	connStatus := "not_seen"
	connFacts := map[string]string{}
	switch {
	case f.connectorErr != nil:
		connStatus = "unknown"
		connFacts["error"] = "unreachable"
	case f.topicFound:
		connStatus = "seen"
	case f.sourceFound && f.topicErr == nil && f.connectorStuck:
		connStatus = "stopped_here"
		if f.connectorState != "" && f.connectorState != "RUNNING" {
			connFacts["state"] = f.connectorState
		}
		if f.tasksFailed > 0 {
			connFacts["tasks_failed"] = strconv.Itoa(f.tasksFailed)
		}
	}
	stage("kafka-connect", f.connector, connStatus, connFacts)

	// 3. Topic.
	topicStatus := "not_seen"
	topicFacts := map[string]string{}
	if f.topicErr != nil {
		topicStatus = "unknown"
		topicFacts["error"] = "unreachable"
	} else if f.topicFound {
		topicStatus = "seen"
		topicFacts["offset"] = strconv.FormatInt(f.topicOffset, 10)
		topicFacts["partition"] = strconv.FormatInt(int64(f.topicPartition), 10)
	}
	stage("kafka", f.topic, topicStatus, topicFacts)

	// 4. Consumer group.
	consumed := f.topicFound && f.groupErr == nil && f.groupCommitted > f.topicOffset
	groupStatus := "not_seen"
	groupFacts := map[string]string{}
	if f.groupErr != nil {
		groupStatus = "unknown"
		groupFacts["error"] = "unreachable"
	} else if f.topicFound {
		if f.groupCommitted >= 0 {
			groupFacts["committed"] = strconv.FormatInt(f.groupCommitted, 10)
		}
		if consumed {
			groupStatus = "seen"
		}
	}
	stage("consumer", f.group, groupStatus, groupFacts)

	// 5. Sink.
	sinkStatus := "not_seen"
	sinkFacts := map[string]string{}
	if f.sinkErr != nil {
		sinkStatus = "unknown"
		sinkFacts["error"] = "unreachable"
	} else if consumed && f.sinkFound {
		sinkStatus = "seen"
		if len(f.sinkDiff) > 0 {
			sinkFacts["diff_columns"] = strings.Join(f.sinkDiff, ",")
		}
	}
	stage("warehouse", f.sink, sinkStatus, sinkFacts)

	// Decide the single stuck point downstream of the connector.
	switch {
	case f.topicFound && f.groupErr == nil && !consumed:
		stages[3].Status = "stopped_here"
	case f.topicFound && f.groupErr == nil && consumed && !f.sinkFound && f.sinkErr == nil:
		stages[4].Status = "stopped_here"
	}
	return stages
}

func tsFromRow(row map[string]any) time.Time {
	v, ok := row["updated_at"]
	if !ok {
		return time.Time{}
	}
	s, ok := v.(string)
	if !ok {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// diffColumns returns the names of columns present in both maps whose values
// differ. Values never leave this function. Numbers are compared numerically
// so an integer source column and a numeric sink column (e.g. 50 vs 50.00)
// are not flagged as different.
func diffColumns(a, b map[string]any) []string {
	var diffs []string
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			continue
		}
		if canonical(av) != canonical(bv) {
			diffs = append(diffs, k)
		}
	}
	sort.Strings(diffs)
	return diffs
}

// canonical renders a value for comparison, normalizing numbers.
func canonical(v any) string {
	s := fmt.Sprint(v)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return s
}

// FormatTS renders a timestamp as RFC 3339, or "" for the zero time.
func FormatTS(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
