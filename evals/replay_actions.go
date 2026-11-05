package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/actions/pgactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/wiring"
)

// The P3 actions on replay clients (P3g-2).
//
// pg_add_publication_table, pg_snapshot_signal, pg_drop_replication_slot,
// kafka_reset_consumer_offsets, dlq_replay and connect_snapshot_table are
// registered through wiring.RegisterP3Actions (production's order: Kafka,
// snapshot, then pg) after actions.Register's seven, so a scenario can name them
// in expect.fix, expect.forbidden_actions and inputs[].action, and the agent
// can propose them. Their Validate reads the recorded stack: the reads below
// answer from the scenario's recordings and resources, the first recording
// first, as connectStatuses does for connect_status.
//
// Every write a replay client has (the source writer, the Kafka writer, the
// Connect and Schema Registry writes) is a no-op that only records the call
// in a writeLog: an eval never writes anywhere, and the log says what the
// action would have done.

// ReplayWrite is one write a replay client took and did not perform.
type ReplayWrite struct {
	// Method is the client method, e.g. "AddPublicationTables".
	Method string `json:"method"`
	// Args are its arguments by name, without the context.
	Args map[string]any `json:"args"`
}

// WriteNote is a replay write as a report shows it: the action or client
// method and the object it would have touched. It never carries a record
// value, a config value or a SQL text.
type WriteNote struct {
	Action string `json:"action"`
	Target string `json:"target,omitempty"`
}

// maxWriteNotes bounds the notes one run shows.
const maxWriteNotes = 20

// writeTargetKeys are the argument names that name a write's object, in the
// order the first present one is shown.
var writeTargetKeys = []string{"connector", "group", "topic", "slot", "publication", "subject", "signal_table", "table", "role"}

// writeNotes reduces a write log to what a report shows: the method and one
// target name each, at most maxWriteNotes of them.
func writeNotes(ws []ReplayWrite) []WriteNote {
	var out []WriteNote
	for _, w := range ws {
		if len(out) == maxWriteNotes {
			break
		}
		n := WriteNote{Action: w.Method}
		for _, k := range writeTargetKeys {
			if v, ok := w.Args[k].(string); ok && v != "" {
				n.Target = v
				break
			}
		}
		out = append(out, n)
	}
	return out
}

// writeLog records the writes of one set of replay clients, and the proposal
// steps the run's scripted executor ran with success (stepRan), which the
// recovery actions' Verify counts against (X2e).
type writeLog struct {
	mu     sync.Mutex
	writes []ReplayWrite
	ran    []ranStep
}

// ranStep is one proposal step that the run executed with success.
type ranStep struct {
	action string
	params json.RawMessage
}

func (l *writeLog) record(method string, args map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writes = append(l.writes, ReplayWrite{Method: method, Args: args})
}

// stepRan notes a step the scripted executor ran with success.
func (l *writeLog) stepRan(action string, params json.RawMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ran = append(l.ran, ranStep{action: action, params: append(json.RawMessage(nil), params...)})
}

// stepsRan returns the params of every step of action that ran with success.
func (l *writeLog) stepsRan(action string) []json.RawMessage {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []json.RawMessage
	for _, s := range l.ran {
		if s.action == action {
			out = append(out, s.params)
		}
	}
	return out
}

func (l *writeLog) snapshot() []ReplayWrite {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ReplayWrite(nil), l.writes...)
}

// registerP3 adds the P3 actions to reg through wiring.RegisterP3Actions, the
// one registration production uses, over replay clients that read res and
// recs and record every write in wl. The replay writer is never a typed nil.
func registerP3(reg *actions.Registry, res Resources, recs []Recording, wl *writeLog) error {
	kr := replayKafkaReads{recs: recs, res: res, log: wl}
	w := replayWriter{log: wl}
	if err := wiring.RegisterP3Actions(reg, wiring.P3ActionDeps{
		Connect:      replayConfig{res: res, recs: recs},
		Source:       replaySource{res: res, recs: recs},
		Planner:      kr,
		Records:      kr,
		SourceWriter: w,
		KafkaWriter:  w,
		Sink:         replaySink{res: res, log: wl},
	}); err != nil {
		return fmt.Errorf("evals: P3 action definitions: %w", err)
	}
	return nil
}

// firstData returns the data of the first recording of tool that keep
// accepts, preferring one without a phase (the start state) over a phased
// one. Its data is round-tripped through JSON into a value of the caller's
// choosing by keep.
func firstData(recs []Recording, tool string, keep func(r Recording, data []byte) bool) bool {
	for _, phased := range []bool{false, true} {
		for _, r := range recs {
			if r.Tool != tool || (r.Phase != "") != phased {
				continue
			}
			d := recordingData(r)
			if d == nil {
				continue
			}
			b, err := json.Marshal(d)
			if err != nil {
				continue
			}
			if keep(r, b) {
				return true
			}
		}
	}
	return false
}

// replayConfig answers Connect.Config for the P3 actions: the connector's
// connect_config recording with resources.connector_configs merged over it.
// actions.Register's seven keep replayConnect, whose Config is not replayed,
// so their Validate behaves as before P3g-2.
type replayConfig struct {
	res  Resources
	recs []Recording
}

func (c replayConfig) Config(_ context.Context, name string) (map[string]string, error) {
	if !slices.Contains(c.res.Connectors, name) {
		return nil, fmt.Errorf("evals: connector %q: %w", name, clients.ErrNotFound)
	}
	out := map[string]string{}
	found := firstData(c.recs, "connect_config", func(r Recording, b []byte) bool {
		if n, _ := r.Input["connector"].(string); n != name {
			return false
		}
		var m map[string]any
		if json.Unmarshal(b, &m) != nil {
			return false
		}
		for k, v := range m {
			if s, ok := v.(string); ok {
				out[k] = s
			} else {
				out[k] = fmt.Sprint(v)
			}
		}
		return true
	})
	extra, declared := c.res.ConnectorConfigs[name]
	if !found && !declared {
		return nil, fmt.Errorf("evals: config of connector %q: %w (record connect_config or declare resources.connector_configs)", name, errNotReplayed)
	}
	for k, v := range extra {
		out[k] = v
	}
	return out, nil
}

func (c replayConfig) ListConnectors(context.Context) ([]string, error) {
	return append([]string(nil), c.res.Connectors...), nil
}

func (c replayConfig) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	st, err := connectStatuses(c.recs)
	if err != nil {
		return clients.ConnectorStatus{}, err
	}
	if s, ok := st[name]; ok {
		return s, nil
	}
	return clients.ConnectorStatus{}, errNotReplayed
}

// replaySource is the read-only source Postgres of the pg actions: slots
// from the pg_replication_slots recording, tables and publications from
// resources.
type replaySource struct {
	res  Resources
	recs []Recording
}

var (
	_ clients.SourceDB               = replaySource{}
	_ pgactions.PublicationAddReader = replaySource{}
	_ pgactions.SnapshotSignalReader = replaySource{}
)

func (s replaySource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	var slots []clients.ReplicationSlot
	found := firstData(s.recs, "pg_replication_slots", func(_ Recording, b []byte) bool {
		if json.Unmarshal(b, &slots) == nil {
			return true
		}
		var wrapped struct {
			Slots []clients.ReplicationSlot `json:"slots"`
		}
		if json.Unmarshal(b, &wrapped) == nil && wrapped.Slots != nil {
			slots = wrapped.Slots
			return true
		}
		return false
	})
	if !found {
		return nil, fmt.Errorf("evals: replication slots: %w (record pg_replication_slots with data)", errNotReplayed)
	}
	return slots, nil
}

func (replaySource) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return nil, errNotReplayed
}

func (replaySource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return nil, errNotReplayed
}

func (replaySource) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return nil, false, errNotReplayed
}

// tables refuses a scenario that declares no source tables: without them
// every table would read as missing, and the refusal would blame the agent.
func (s replaySource) tables() error {
	if len(s.res.Tables) == 0 || len(s.res.Publications) == 0 {
		return fmt.Errorf("evals: source tables and publications: %w (declare resources.tables and resources.publications)", errNotReplayed)
	}
	return nil
}

func estimate(t TableState) int64 {
	if t.EstimatedRows == nil {
		return -1
	}
	return *t.EstimatedRows
}

func (s replaySource) PublicationAddState(_ context.Context, publication, table string, visible []string) (postgres.PublicationAddState, error) {
	if err := s.tables(); err != nil {
		return postgres.PublicationAddState{}, err
	}
	members, ok := s.res.Publications[publication]
	if !ok {
		return postgres.PublicationAddState{}, fmt.Errorf("evals: publication %q: %w", publication, clients.ErrNotFound)
	}
	var st postgres.PublicationAddState
	for _, m := range members {
		if slices.Contains(visible, m) {
			st.Members = append(st.Members, m)
		} else {
			st.OtherMembers++
		}
	}
	sort.Strings(st.Members)
	t, exists := s.res.Tables[table]
	if !exists {
		return st, nil
	}
	st.TableExists = true
	st.RelKind = orStr(t.RelKind, "r")
	st.ReplicaIdentity = orStr(t.ReplicaIdentity, "d")
	st.HasPrimaryKey = t.PrimaryKey
	st.InPublication = slices.Contains(members, table)
	st.EstimatedRows = estimate(t)
	return st, nil
}

func (s replaySource) SnapshotSignalState(_ context.Context, publication, signalTable string, tables []string) (postgres.SnapshotSignalState, error) {
	if err := s.tables(); err != nil {
		return postgres.SnapshotSignalState{}, err
	}
	members, pubOK := s.res.Publications[publication]
	sig, sigOK := s.res.Tables[signalTable]
	st := postgres.SnapshotSignalState{
		PublicationExists: pubOK,
		SignalExists:      sigOK,
		SignalColumns:     sigOK && sig.SignalColumns,
		SignalPublished:   slices.Contains(members, signalTable),
	}
	for _, name := range tables {
		t, ok := s.res.Tables[name]
		ts := postgres.SnapshotTableState{Table: name, Exists: ok, EstimatedRows: -1}
		if ok {
			ts.EstimatedRows = estimate(t)
		}
		st.Tables = append(st.Tables, ts)
	}
	return st, nil
}

func orStr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// replayKafkaReads is the read-only Kafka of the P3 Kafka actions: the
// group-reset planner reads the kafka_consumer_lag recording, the DLQ reader
// the kafka_topic_offsets and dlq_peek recordings. kafka_replay_range (X2e)
// also reads resources.topics and, in Verify, the steps the run executed.
type replayKafkaReads struct {
	recs []Recording
	res  Resources
	log  *writeLog
}

var (
	_ clients.GroupResetPlanner = replayKafkaReads{}
	_ actions.DLQRecordReader   = replayKafkaReads{}
)

// lagData is the part of kafka_consumer_lag's Data the planner reads. The
// tool lists the 5 worst partitions only, so the plan covers at most those:
// enough for the recorded groups, and a plan that misses a topic makes the
// action refuse rather than pass.
type lagData struct {
	Group   string `json:"group"`
	State   string `json:"state"`
	Members int    `json:"members"`
	Worst   []struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Committed int64  `json:"committed"`
		End       int64  `json:"end"`
	} `json:"worst_partitions"`
}

func (k replayKafkaReads) lag(group string) (lagData, error) {
	var out lagData
	found := firstData(k.recs, "kafka_consumer_lag", func(_ Recording, b []byte) bool {
		var d lagData
		if json.Unmarshal(b, &d) != nil || d.Group != group {
			return false
		}
		out = d
		return true
	})
	if !found || out.State == "Dead" {
		return out, fmt.Errorf("evals: consumer group %q: %w", group, clients.ErrNotFound)
	}
	return out, nil
}

func (k replayKafkaReads) CheckGroupInactive(_ context.Context, group string) error {
	d, err := k.lag(group)
	if err != nil {
		return err
	}
	if d.Members > 0 {
		return fmt.Errorf("evals: consumer group %q has %d members: %w", group, d.Members, clients.ErrGroupActive)
	}
	return nil
}

// PlanGroupReset plans each recorded partition back to its log start (the
// kafka_topic_offsets recording, else 0). A timestamp target is planned to
// the log start too: replay has no record timestamps to look it up in.
func (k replayKafkaReads) PlanGroupReset(_ context.Context, group string, _ clients.OffsetTarget) ([]clients.OffsetMove, error) {
	d, err := k.lag(group)
	if err != nil {
		return nil, err
	}
	if len(d.Worst) == 0 {
		return nil, fmt.Errorf("evals: consumer group %q has no committed offsets: %w", group, clients.ErrNotFound)
	}
	starts := map[string]map[int32]int64{}
	moves := make([]clients.OffsetMove, 0, len(d.Worst))
	for _, p := range d.Worst {
		if _, ok := starts[p.Topic]; !ok {
			starts[p.Topic] = map[int32]int64{}
			if offs, ok := k.topicOffsets(p.Topic); ok {
				for _, o := range offs {
					starts[p.Topic][o.Partition] = o.Start
				}
			}
		}
		moves = append(moves, clients.OffsetMove{Topic: p.Topic, Partition: p.Partition, Committed: p.Committed, Target: starts[p.Topic][p.Partition], End: p.End})
	}
	return moves, nil
}

// topicOffsets reads a kafka_topic_offsets recording of topic.
func (k replayKafkaReads) topicOffsets(topic string) ([]clients.PartitionOffsets, bool) {
	var out []clients.PartitionOffsets
	found := firstData(k.recs, "kafka_topic_offsets", func(_ Recording, b []byte) bool {
		var d struct {
			Topic      string                     `json:"topic"`
			Partitions []clients.PartitionOffsets `json:"partitions"`
		}
		if json.Unmarshal(b, &d) != nil || d.Topic != topic || len(d.Partitions) == 0 {
			return false
		}
		out = d.Partitions
		return true
	})
	return out, found
}

// dlqOffsets reads a dlq_peek recording of an existing DLQ topic.
func (k replayKafkaReads) dlqOffsets(topic string) ([]clients.PartitionOffsets, bool) {
	var out []clients.PartitionOffsets
	found := firstData(k.recs, "dlq_peek", func(_ Recording, b []byte) bool {
		var d struct {
			Topic  string           `json:"topic"`
			Exists bool             `json:"exists"`
			Start  map[string]int64 `json:"start_offsets"`
			End    map[string]int64 `json:"end_offsets"`
		}
		if json.Unmarshal(b, &d) != nil || d.Topic != topic || !d.Exists || len(d.End) == 0 {
			return false
		}
		out = nil
		for p, end := range d.End {
			n, err := strconv.ParseInt(p, 10, 32)
			if err != nil {
				return false
			}
			out = append(out, clients.PartitionOffsets{Partition: int32(n), Start: d.Start[p], End: end})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
		return true
	})
	return out, found
}

// DLQOffsets answers every topic it has a recording for and leaves out the
// others, as the Kafka client leaves out a topic that does not exist.
func (k replayKafkaReads) DLQOffsets(_ context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	out := map[string][]clients.PartitionOffsets{}
	for _, t := range topics {
		if offs, ok := k.topicOffsets(t); ok {
			out[t] = offs
		} else if offs, ok := k.dlqOffsets(t); ok {
			out[t] = offs
		} else if offs, ok := k.declaredTopic(t); ok {
			out[t] = offs
		}
	}
	return out, nil
}

// ReadRecords is not replayed: recordings hold no record values. dlq_replay's
// Preview and Execute say so; its Validate needs only the offsets.
func (replayKafkaReads) ReadRecords(context.Context, string, int32, int64, int) ([]clients.Record, error) {
	return nil, errNotReplayed
}

// replayWriter is the source and Kafka writer of the P3 actions: every write
// is recorded and succeeds without doing anything.
type replayWriter struct{ log *writeLog }

var (
	_ clients.SourceWriter = replayWriter{}
	_ clients.KafkaWriter  = replayWriter{}
)

func (w replayWriter) AddPublicationTables(_ context.Context, publication string, tables []string) error {
	w.log.record("AddPublicationTables", map[string]any{"publication": publication, "tables": append([]string(nil), tables...)})
	return nil
}

func (w replayWriter) DropReplicationSlot(_ context.Context, slot string) error {
	w.log.record("DropReplicationSlot", map[string]any{"slot": slot})
	return nil
}

func (w replayWriter) InsertSnapshotSignal(_ context.Context, signalTable, connectorID string, tables []string) (string, error) {
	w.log.record("InsertSnapshotSignal", map[string]any{"signal_table": signalTable, "connector": connectorID, "tables": append([]string(nil), tables...)})
	return "replay-signal", nil
}

func (w replayWriter) ResetGroupOffsets(_ context.Context, group string, to clients.OffsetTarget) (int, error) {
	w.log.record("ResetGroupOffsets", map[string]any{"group": group, "to": to})
	return 0, nil
}

func (w replayWriter) Produce(_ context.Context, topic string, recs []clients.Record) (int, error) {
	w.log.record("Produce", map[string]any{"topic": topic, "records": len(recs)})
	return len(recs), nil
}

// SendSnapshotSignal is the Kafka writer's narrow signal produce that
// connect_snapshot_table asserts for.
func (w replayWriter) SendSnapshotSignal(_ context.Context, signalTopic, connector, logicalName string, tables []string) (kafka.SnapshotSignal, error) {
	w.log.record("SendSnapshotSignal", map[string]any{"topic": signalTopic, "connector": connector, "logical_name": logicalName, "tables": append([]string(nil), tables...)})
	return kafka.SnapshotSignal{ID: "replay-signal", Topic: signalTopic}, nil
}

// ---- the recovery actions (X2e) ----
//
// pg_resnapshot_rows and kafka_replay_range (actions.ClassRecovery) count
// what their step repaired in Verify, which the agent calls after the step
// ran (X2c). Replay answers those counts as follows:
//
//   - Source key counts (RangeColumn, CountRange) come from
//     resources.tables: key_column and the key_ranges that hold rows. No read
//     tool counts a key range.
//   - The sink (read by Verify only) holds the source rows of every
//     pg_resnapshot_rows step the run executed with success, and nothing
//     else of the step's range: a recovery is counted against what the run
//     actually ran.
//   - A reprocess topic holds, since the step, the offsets of every
//     kafka_replay_range step the run executed into it (all on its first
//     partition: Verify sums the partitions). Offsets by time are not
//     replayed, so a time-window replay is refused at Validate.
//   - The reprocess topic itself is declared in resources.topics (empty),
//     since kafka_topic_offsets refuses any topic but the CDC topics.

var (
	_ pgactions.RowRangeCounter = replaySource{}
	_ pgactions.SinkCounter     = replaySink{}
	_ clients.Warehouse         = replaySink{}
	_ clients.RateReader        = replayKafkaReads{}
)

// rangeKind is the column every replayed key is: a 64-bit integer key.
func rangeKind(primary bool) postgres.RangeColumn {
	return postgres.RangeColumn{Type: "int8", Kind: pgconv.RangeKindInteger, PrimaryKey: primary}
}

func (s replaySource) RangeColumn(_ context.Context, table, column string) (postgres.RangeColumn, error) {
	t, ok := s.res.Tables[table]
	if !ok {
		return postgres.RangeColumn{}, fmt.Errorf("evals: table %s: %w", table, clients.ErrNotFound)
	}
	if t.KeyColumn == "" || column != t.KeyColumn {
		return postgres.RangeColumn{}, fmt.Errorf("evals: column %s of %s: %w (replay counts only the key_column of resources.tables)", column, table, errNotReplayed)
	}
	return rangeKind(t.PrimaryKey), nil
}

func (s replaySource) CountRange(_ context.Context, table, column, from, to string) (int64, error) {
	t, ok := s.res.Tables[table]
	if !ok {
		return 0, fmt.Errorf("evals: table %s: %w", table, clients.ErrNotFound)
	}
	if t.KeyColumn == "" || column != t.KeyColumn {
		return 0, fmt.Errorf("evals: column %s of %s: %w", column, table, errNotReplayed)
	}
	lo, hi, err := keyBounds(from, to)
	if err != nil {
		return 0, err
	}
	return rowsIn(t.KeyRanges, lo, hi), nil
}

// keyBounds parses a range's canonical integer bounds; to "" is no upper
// bound.
func keyBounds(from, to string) (lo, hi int64, err error) {
	if lo, err = strconv.ParseInt(from, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("evals: key bound %q: %w", from, errNotReplayed)
	}
	hi = math.MaxInt64
	if to != "" {
		if hi, err = strconv.ParseInt(to, 10, 64); err != nil {
			return 0, 0, fmt.Errorf("evals: key bound %q: %w", to, errNotReplayed)
		}
	}
	return lo, hi, nil
}

// rowsIn counts the keys of ranges (each [from, to], disjoint) in [lo, hi].
func rowsIn(ranges [][]int64, lo, hi int64) int64 {
	var n int64
	for _, r := range ranges {
		if len(r) != 2 {
			continue
		}
		a, b := max(r[0], lo), min(r[1], hi)
		if a <= b {
			n += b - a + 1
		}
	}
	return n
}

// replaySink is the sink pg_resnapshot_rows' Verify counts (wiring hands
// the action only its range-count view). It holds what the run's executed
// steps re-emitted; it has no rows to read and no time windows.
type replaySink struct {
	res Resources
	log *writeLog
}

func (replaySink) Freshness(context.Context, string, string) (time.Time, error) {
	return time.Time{}, errNotReplayed
}

func (replaySink) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return nil, false, errNotReplayed
}

// RangeColumn answers a key column some source table declares; any other
// column is missing at the sink, which Verify reads as unverifiable.
func (k replaySink) RangeColumn(_ context.Context, table, column string) (postgres.RangeColumn, error) {
	for _, t := range k.res.Tables {
		if t.KeyColumn != "" && t.KeyColumn == column {
			return rangeKind(true), nil
		}
	}
	return postgres.RangeColumn{}, fmt.Errorf("evals: sink column %s of %s: %w", column, table, clients.ErrNotFound)
}

// resnapStep is the part of a pg_resnapshot_rows step the sink reads.
type resnapStep struct {
	Table    string `json:"table"`
	KeyRange *struct {
		Column string `json:"column"`
		From   string `json:"from"`
		To     string `json:"to"`
	} `json:"key_range"`
	Since *struct {
		Column string `json:"column"`
		At     string `json:"at"`
	} `json:"since"`
}

// CountRange counts the source rows in [from, to] that an executed
// pg_resnapshot_rows step on column re-emitted; overlapping steps count a
// row once.
func (k replaySink) CountRange(_ context.Context, _, column, from, to string) (int64, error) {
	lo, hi, err := keyBounds(from, to)
	if err != nil {
		return 0, err
	}
	if k.log == nil {
		return 0, nil
	}
	byTable := map[string][][]int64{}
	for _, raw := range k.log.stepsRan(pgactions.ResnapshotRowsName) {
		var p resnapStep
		if json.Unmarshal(raw, &p) != nil {
			continue
		}
		var c, f, t string
		switch {
		case p.KeyRange != nil:
			c, f, t = p.KeyRange.Column, p.KeyRange.From, p.KeyRange.To
		case p.Since != nil:
			c, f = p.Since.Column, p.Since.At
		}
		a, b, err := keyBounds(f, t)
		if c != column || err != nil {
			continue
		}
		byTable[p.Table] = append(byTable[p.Table], []int64{max(a, lo), min(b, hi)})
	}
	var n int64
	for table, steps := range byTable {
		for _, s := range mergeRanges(steps) {
			n += rowsIn(k.res.Tables[table].KeyRanges, s[0], s[1])
		}
	}
	return n, nil
}

func (replaySink) CountBetween(context.Context, string, string, time.Time, time.Time) (int64, error) {
	return 0, fmt.Errorf("evals: time windows: %w", clients.ErrNotSupported)
}

// mergeRanges returns rs (each [from, to]) sorted and merged, empty ones
// dropped, so overlapping steps count a key once.
func mergeRanges(rs [][]int64) [][]int64 {
	var in [][]int64
	for _, r := range rs {
		if r[0] <= r[1] {
			in = append(in, r)
		}
	}
	sort.Slice(in, func(i, j int) bool { return in[i][0] < in[j][0] })
	var out [][]int64
	for _, r := range in {
		if n := len(out); n > 0 && r[0] <= out[n-1][1] {
			out[n-1][1] = max(out[n-1][1], r[1])
			continue
		}
		out = append(out, []int64{r[0], r[1]})
	}
	return out
}

// replayRangeStep is the part of a kafka_replay_range step the reprocess
// topic's count reads.
type replayRangeStep struct {
	ToTopic string `json:"to_topic"`
	Ranges  []struct {
		FromOffset int64 `json:"from_offset"`
		ToOffset   int64 `json:"to_offset"`
	} `json:"ranges"`
}

// PartitionRates answers only a resources.topics topic or one that executed
// kafka_replay_range steps copied into: the offsets of those steps, as
// written since the step (Verify's count). Offsets by time of any other topic
// (a CDC topic's time window) are not replayed.
func (k replayKafkaReads) PartitionRates(_ context.Context, _ time.Time, topics ...string) ([]clients.PartitionRate, error) {
	var out []clients.PartitionRate
	for _, topic := range topics {
		var copied int64
		_, found := k.res.Topics[topic]
		if k.log != nil {
			for _, raw := range k.log.stepsRan("kafka_replay_range") {
				var p replayRangeStep
				if json.Unmarshal(raw, &p) != nil || p.ToTopic != topic {
					continue
				}
				found = true
				for _, r := range p.Ranges {
					copied += max(r.ToOffset-r.FromOffset+1, 0)
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("evals: offsets by time of %s: %w", topic, clients.ErrNotSupported)
		}
		for i, p := range k.partitionsOf(topic) {
			r := clients.PartitionRate{Topic: topic, Partition: p.Partition, End: p.End, OffsetAtStart: p.End}
			if i == 0 {
				r.End, r.Records = p.End+copied, copied
			}
			out = append(out, r)
		}
	}
	return out, nil
}

// partitionsOf is a topic's recorded offsets, else its resources.topics
// entry, else one empty partition.
func (k replayKafkaReads) partitionsOf(topic string) []clients.PartitionOffsets {
	if offs, ok := k.topicOffsets(topic); ok {
		return offs
	}
	if offs, ok := k.declaredTopic(topic); ok {
		return offs
	}
	return []clients.PartitionOffsets{{Partition: 0}}
}

// declaredTopic is a resources.topics entry: every partition empty.
func (k replayKafkaReads) declaredTopic(topic string) ([]clients.PartitionOffsets, bool) {
	t, ok := k.res.Topics[topic]
	if !ok || t.Partitions < 1 {
		return nil, false
	}
	out := make([]clients.PartitionOffsets, t.Partitions)
	for i := range out {
		out[i].Partition = int32(i)
	}
	return out, true
}
