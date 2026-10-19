package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/actions/pgactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/postgres"
)

// The P3 actions on replay clients (P3g-2).
//
// pg_add_publication_table, pg_snapshot_signal, pg_drop_replication_slot,
// kafka_reset_consumer_offsets, dlq_replay and connect_snapshot_table are
// registered next to actions.Register's seven, so a scenario can name them
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

// writeLog records the writes of one set of replay clients.
type writeLog struct {
	mu     sync.Mutex
	writes []ReplayWrite
}

func (l *writeLog) record(method string, args map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writes = append(l.writes, ReplayWrite{Method: method, Args: args})
}

func (l *writeLog) snapshot() []ReplayWrite {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ReplayWrite(nil), l.writes...)
}

// registerP3 adds the P3 actions to reg over replay clients that read res
// and recs and record every write in wl.
func registerP3(reg *actions.Registry, res Resources, recs []Recording, wl *writeLog) error {
	cfg := replayConfig{res: res, recs: recs}
	src := replaySource{res: res, recs: recs}
	w := replayWriter{log: wl}
	kr := replayKafkaReads{recs: recs}
	for _, a := range []actions.Action{
		pgactions.NewAddPublicationTable(pgactions.AddPublicationTableDeps{Connect: cfg, Source: src, Writer: w}),
		pgactions.NewSnapshotSignal(pgactions.SnapshotSignalDeps{Connect: cfg, Source: src, Writer: w}),
		pgactions.NewDropReplicationSlot(pgactions.DropSlotDeps{Connect: cfg, Source: src, Writer: w}),
	} {
		if err := reg.Register(a); err != nil {
			return fmt.Errorf("evals: pg action definitions: %w", err)
		}
	}
	if err := actions.RegisterKafka(reg, actions.KafkaDeps{Writer: w, Planner: kr, Records: kr}); err != nil {
		return fmt.Errorf("evals: kafka action definitions: %w", err)
	}
	if err := actions.RegisterSnapshot(reg, actions.SnapshotDeps{Connect: cfg, Source: src, Kafka: w}); err != nil {
		return fmt.Errorf("evals: snapshot action definition: %w", err)
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
// the kafka_topic_offsets and dlq_peek recordings.
type replayKafkaReads struct {
	recs []Recording
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
