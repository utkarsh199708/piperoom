package plan

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// SinkNone is the sink kind of a setup that set no sink up.
const SinkNone = "none"

// Rung names, in ladder order. They are stable identifiers: setup.verified
// and the room UI key on them, so a name is never reused for another rung.
const (
	RungConnector     = "connector_running" // 1: the connector exists and is RUNNING
	RungTasks         = "tasks_running"     // 2: every task is RUNNING
	RungCanaryTopic   = "canary_topic"      // 3: the canary topic exists
	RungSnapshot      = "snapshot_complete" // 4: the initial snapshot finished
	RungCanaryEvent   = "canary_event"      // 5: the canary event with the nonce as key is on the topic
	RungCanarySubject = "canary_subject"    // 6: the canary subject is in the schema registry
	RungSinkRow       = "sink_row"          // 7: the canary row is in the sink
	RungPersonChange  = "person_change"     // 8: a real change the person made arrived
)

// ErrLadder is returned by Ladder for a setup it does not define a ladder
// for: an unknown kind, or a target whose ladder is its own driver's row.
var ErrLadder = errors.New("plan: no verification ladder for this setup")

// Ladder sources and targets, as events.SetupRequest names them. The targets
// here reach a Kafka Connect REST endpoint, so rungs 1 and 2 are its status.
// A target whose status is not a Connect REST endpoint — embedded (a log file
// the person names, U6b), dbzserver (server_status, U11b) and the managed
// services (describe-connector, U13b2/U13c/U13d) — defines its own ladder in
// its own row; Ladder refuses it rather than guess its reads.
var (
	ladderSources = []string{"postgres", "mysql", "sqlserver", "mongodb", "oracle"}
	ladderTargets = []string{"compose", "host", "strimzi"}
)

// The room reads a rung issues, by internal/tools tool name (Q wave). Rung 4
// reads Debezium's metrics first and falls back to the topic's record count
// when the metrics endpoint is not configured; rung 8's wait (10 minutes) is
// the ladder runner's, not the predicate's.
const (
	readConnectStatus = "connect_status"
	readTopicOffsets  = "kafka_topic_offsets"
	readMetrics       = "debezium_metrics"
	readFindByKey     = "kafka_find_by_key"
	readSubjects      = "schema_registry_subjects"
	readWarehouseRow  = "warehouse_row"
)

// Predicate judges one read's structured Data. It never receives Summary, so
// no rung can be satisfied by prose: a Summary that claims success while the
// Data says FAILED leaves the rung red (design §3.4).
type Predicate func(data json.RawMessage) (ok bool, note string)

// Rung is one rung of the verification ladder (design §3.4). Reads are issued
// by the ladder runner through the room's clients, in order, until a
// predicate decides; Name, StepID and ReceiptID are what setup.verified
// records. NotApplicable is set for a rung this setup does not have (rung 7
// without a sink): the ladder runner reports it green with that note and
// issues no read.
type Rung struct {
	Index         int
	Name          string
	Reads         []string
	Predicate     Predicate
	NotApplicable string
}

// Ladder returns the eight rungs of design §3.4 for a (source, target, sink)
// setup: source and target as events.SetupRequest names them, sink the
// warehouse kind the setup fills (SinkNone or "" when it sets no sink up).
// Rungs 1 to 7 are the ladder's; rung 8 waits for a change the person makes.
// An unknown source, target or sink is an error, never an empty ladder.
func Ladder(source, target, sink string) ([]Rung, error) {
	if !slices.Contains(ladderSources, source) {
		return nil, fmt.Errorf("%w: unknown source %q", ErrLadder, cleanText(source, 32))
	}
	if !slices.Contains(ladderTargets, target) {
		return nil, fmt.Errorf("%w: target %q has no Connect status in this ladder (embedded, dbzserver and the managed services define their own)", ErrLadder, cleanText(target, 32))
	}
	noSink := sink == "" || sink == SinkNone
	if !noSink && !validSinkKind(sink) {
		return nil, fmt.Errorf("%w: unknown sink %q", ErrLadder, cleanText(sink, 32))
	}
	rungs := []Rung{
		{Name: RungConnector, Reads: []string{readConnectStatus}, Predicate: connectorRunning},
		{Name: RungTasks, Reads: []string{readConnectStatus}, Predicate: tasksRunning},
		{Name: RungCanaryTopic, Reads: []string{readTopicOffsets}, Predicate: canaryTopic},
		{Name: RungSnapshot, Reads: []string{readMetrics, readTopicOffsets}, Predicate: snapshotComplete},
		{Name: RungCanaryEvent, Reads: []string{readFindByKey}, Predicate: canaryEvent},
		{Name: RungCanarySubject, Reads: []string{readSubjects}, Predicate: canarySubject},
		{Name: RungSinkRow, Reads: []string{readWarehouseRow}, Predicate: sinkRow},
		{Name: RungPersonChange, Reads: []string{readFindByKey}, Predicate: personChange},
	}
	if noSink {
		rungs[6].NotApplicable = "no sink was set up"
	}
	for i := range rungs {
		rungs[i].Index = i + 1
	}
	return rungs, nil
}

// validSinkKind accepts a warehouse kind ("postgres", "clickhouse",
// "snowflake", ...) by shape only: the drivers own the list.
func validSinkKind(sink string) bool {
	if sink == "" || len(sink) > 32 || sink[0] < 'a' || sink[0] > 'z' {
		return false
	}
	for _, r := range sink {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

// connectorRunning reads connect_status Data: the connector's own state.
func connectorRunning(data json.RawMessage) (bool, string) {
	var st struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return false, "the read did not return a connector status"
	}
	if st.State == "" {
		return false, "the connector status names no state"
	}
	if !strings.EqualFold(st.State, "RUNNING") {
		return false, "the connector is " + note(st.State)
	}
	return true, "the connector is RUNNING"
}

// tasksRunning reads connect_status Data: every task RUNNING and at least one
// task. A connector with no tasks is not green: nothing flows.
func tasksRunning(data json.RawMessage) (bool, string) {
	var st struct {
		Tasks []struct {
			ID    int    `json:"id"`
			State string `json:"state"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return false, "the read did not return a connector status"
	}
	if len(st.Tasks) == 0 {
		return false, "the connector reports no tasks"
	}
	for _, task := range st.Tasks {
		if !strings.EqualFold(task.State, "RUNNING") {
			return false, fmt.Sprintf("task %d is %s", task.ID, note(task.State))
		}
	}
	return true, fmt.Sprintf("%d task(s) RUNNING", len(st.Tasks))
}

// canaryTopic reads kafka_topic_offsets Data for the canary topic: the read
// succeeded and the topic has partitions.
func canaryTopic(data json.RawMessage) (bool, string) {
	var st struct {
		Topic      string            `json:"topic"`
		Partitions []json.RawMessage `json:"partitions"`
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return false, "the read did not return topic offsets"
	}
	if st.Topic == "" {
		return false, "the read names no topic"
	}
	if len(st.Partitions) == 0 {
		return false, "the topic has no partitions"
	}
	return true, fmt.Sprintf("the topic exists with %d partition(s)", len(st.Partitions))
}

// snapshotComplete reads debezium_metrics Data (SnapshotCompleted) and, when
// the metrics endpoint is not configured, the topic offsets' record count of
// the canary topic (design §3.4 rung 4).
func snapshotComplete(data json.RawMessage) (bool, string) {
	var st struct {
		Snapshot     map[string]float64 `json:"snapshot"`
		TotalRecords *int64             `json:"total_records"`
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return false, "the read returned neither Debezium metrics nor topic offsets"
	}
	if st.Snapshot != nil {
		if st.Snapshot["SnapshotCompleted"] >= 1 {
			return true, "SnapshotCompleted"
		}
		if st.Snapshot["SnapshotRunning"] >= 1 {
			return false, "the snapshot is still running"
		}
		return false, "the snapshot has not completed"
	}
	if st.TotalRecords != nil {
		if *st.TotalRecords >= 1 {
			return true, fmt.Sprintf("%d canary record(s) on the topic (no metrics endpoint)", *st.TotalRecords)
		}
		return false, "the topic holds no canary records yet"
	}
	return false, "the read returned neither Debezium metrics nor topic offsets"
}

// canaryEvent reads kafka_find_by_key Data: the canary nonce is on the topic
// as a key. The scan is the ladder runner's (topic and since), the verdict is
// the Data's found flag.
func canaryEvent(data json.RawMessage) (bool, string) {
	var st struct {
		Found *bool `json:"found"`
	}
	if err := json.Unmarshal(data, &st); err != nil || st.Found == nil {
		return false, "the read did not return a key scan"
	}
	if !*st.Found {
		return false, "no record with the canary key yet"
	}
	return true, "the canary event with the nonce as key is on the topic"
}

// canarySubject reads schema_registry_subjects Data for the canary topic's
// prefix: a non-empty subject list. With no schema registry configured the
// tool answers {"configured": false}, and the rung is green: the design's
// "when one is configured".
func canarySubject(data json.RawMessage) (bool, string) {
	var obj struct {
		Configured *bool    `json:"configured"`
		Subjects   []string `json:"subjects"`
	}
	if err := json.Unmarshal(data, &obj); err == nil {
		if obj.Configured != nil && !*obj.Configured {
			return true, "no schema registry is configured"
		}
		if obj.Subjects != nil {
			if len(obj.Subjects) == 0 {
				return false, "no subject under the canary topic's prefix yet"
			}
			return true, fmt.Sprintf("%d subject(s) under the canary topic's prefix", len(obj.Subjects))
		}
	}
	var subjects []string
	if err := json.Unmarshal(data, &subjects); err != nil {
		return false, "the read did not return a subject list"
	}
	if len(subjects) == 0 {
		return false, "no subject under the canary topic's prefix yet"
	}
	return true, fmt.Sprintf("%d subject(s) under the canary topic's prefix", len(subjects))
}

// sinkRow reads warehouse_row Data for the canary row: found.
func sinkRow(data json.RawMessage) (bool, string) {
	var st struct {
		Found *bool `json:"found"`
	}
	if err := json.Unmarshal(data, &st); err != nil || st.Found == nil {
		return false, "the read did not return a warehouse row"
	}
	if !*st.Found {
		return false, "the canary row is not in the sink yet"
	}
	return true, "the canary row is in the sink"
}

// personChange reads kafka_find_by_key Data for the change the person was
// asked to make (rung 8); the ladder runner waits up to 10 minutes and times
// out as a note, not a failure.
func personChange(data json.RawMessage) (bool, string) {
	var st struct {
		Found *bool `json:"found"`
	}
	if err := json.Unmarshal(data, &st); err != nil || st.Found == nil {
		return false, "the read did not return a key scan"
	}
	if !*st.Found {
		return false, "the change the person made has not arrived yet"
	}
	return true, "the change the person made arrived"
}

// note makes endpoint text safe for a rung note an operator reads: valid
// UTF-8, no control characters, at most 32 runes.
func note(s string) string { return cleanText(s, 32) }

// cleanText is note with the cap as a parameter; clip uses it for model text
// in an error.
func cleanText(s string, maxRunes int) string {
	s = strings.ToValidUTF8(s, "?")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s)
	runes := []rune(s)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes]) + "…"
	}
	return s
}
