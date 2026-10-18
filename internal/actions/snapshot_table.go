package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// SnapshotTableName is the registered name of connect_snapshot_table.
const SnapshotTableName = "connect_snapshot_table"

// The two ways a Debezium connector takes a snapshot signal.
const (
	ViaSignalTopic = "signal_topic" // a record on signal.kafka.topic (P3a)
	// ViaSignalTable is refused at every stage (errSignalTableLayer): a row
	// in signal.data.collection is the postgres-layer pg_snapshot_signal.
	ViaSignalTable = "signal_table"
)

// maxSnapshotTables caps the tables of one step: one signal, one approval.
const maxSnapshotTables = 50

// snapshotTableRe is a "schema.table" as owners.yaml lists source tables;
// AGENTS.md §4 rule 6 for each part.
var snapshotTableRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`)

var (
	// ErrSignalNotConfigured refuses a via the connector has no signal
	// channel for; the message names the keys to set.
	ErrSignalNotConfigured = errors.New("actions: the connector is not configured for this snapshot signal")
	// errSignalTableLayer refuses via signal_table in parseSnapshot, so
	// Validate, CheckScope, Preview and Execute all refuse it before any
	// I/O: a signal table row is a write on the source database whose owner
	// must approve it, and this action's layer is kafka-connect. That
	// variant is the postgres-layer action pg_snapshot_signal (pgactions,
	// P3b-4).
	errSignalTableLayer = errors.New("actions: connect_snapshot_table: via signal_table is the postgres-layer action pg_snapshot_signal (it writes a row to the source database, whose owner must approve it); use via signal_topic here")
	// ErrNoSignalWriter refuses a wired Kafka writer without the narrow
	// SendSnapshotSignal. Its Produce would not do: it checks the topic
	// against the CDC topics of the request's tables only (readSetup), not of
	// every source table as SendSnapshotSignal does, so a signal topic that
	// is another source table's CDC topic would get the signal record.
	ErrNoSignalWriter = errors.New("actions: connect_snapshot_table: the Kafka writer cannot send snapshot signals (no SendSnapshotSignal); nothing was sent")
)

// SnapshotDeps is what connect_snapshot_table reads and writes through.
type SnapshotDeps struct {
	// Connect reads the connector config (signal channels, topic.prefix).
	Connect clients.ConnectReader
	// Source is optional: when it can estimate a table's rows
	// (tableRowEstimator), Preview shows the estimate.
	Source clients.SourceReader
	// Kafka is nil unless write mode is on; only Execute writes through it.
	// It is the action's only writer: a signal table row, a source database
	// write, is pg_snapshot_signal's. It must also be a snapshotSignaler
	// (the real kafka.Writer is); Validate and Execute refuse one that is not
	// (ErrNoSignalWriter), and the action never falls back to Produce.
	Kafka clients.KafkaWriter

	// Timeout overrides the 10 s bound of each call; tests only.
	Timeout time.Duration
}

// RegisterSnapshot adds connect_snapshot_table to reg.
func RegisterSnapshot(reg *Registry, d SnapshotDeps) error {
	return reg.Register(&snapshotTable{d: d})
}

// snapshotSignaler is the Kafka Writer's narrow signal produce: Produce only
// writes the pipeline's CDC topics, and the signal topic is none of them.
// SendSnapshotSignal checks the topic against every source table's CDC topic.
type snapshotSignaler interface {
	SendSnapshotSignal(ctx context.Context, signalTopic, connector, logicalName string, tables []string) (kafka.SnapshotSignal, error)
}

var _ snapshotSignaler = (*kafka.Writer)(nil)

// tableRowEstimator is an optional read of the source client: the planner's
// row estimate of "schema.table" (pg_class.reltuples; negative = never
// analysed). Not on the contract SourceReader, so fakes and replay clients
// compile without it; Preview says "estimate unavailable" then.
type tableRowEstimator interface {
	EstimatedRows(ctx context.Context, table string) (int64, error)
}

type snapshotTable struct{ d SnapshotDeps }

var (
	_ Action  = (*snapshotTable)(nil)
	_ Scoped  = (*snapshotTable)(nil)
	_ Preview = (*snapshotTable)(nil)
)

type snapshotParams struct {
	Connector string   `json:"connector"`
	Tables    []string `json:"tables"`
	Via       string   `json:"via"`
}

func (a *snapshotTable) Name() string { return SnapshotTableName }

// Layer is kafka-connect: the signal topic variant changes nothing but what
// the connector does. The signal table variant is the postgres-layer action
// pg_snapshot_signal (pgactions, P3b-4).
func (a *snapshotTable) Layer() string { return "kafka-connect" }

func (a *snapshotTable) Describe() string {
	return "Asks a Debezium source connector for an incremental snapshot of some of the pipeline's source tables: " +
		"it re-reads them in chunks and emits every existing row again (op r) to their topics while streaming goes on. " +
		"For a table whose rows never reached Kafka (missed by the publication or include list) or reached it corrupted. " +
		"via signal_topic produces the execute-snapshot signal to the connector's signal.kafka.topic (needs kafka in " +
		"signal.enabled.channels). Downstream sinks see duplicates unless they upsert by key. " +
		"A signal row in the source database's signal table is not this action: propose pg_snapshot_signal (postgres layer)."
}

func (a *snapshotTable) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{
		"connector": strProp("the pipeline's Debezium source connector"),
		"tables": map[string]any{
			"type":        "array",
			"description": "source tables to snapshot, schema.table, each one of the pipeline's source_tables",
			"items":       map[string]any{"type": "string"},
			"minItems":    1,
			"maxItems":    maxSnapshotTables,
		},
		"via": map[string]any{
			"type":        "string",
			"enum":        []string{ViaSignalTopic},
			"description": "how the signal reaches the connector: signal_topic (its signal.kafka.topic); a signal table row is pg_snapshot_signal",
		},
	}, "connector", "tables", "via")
}

// parseSnapshot decodes params strictly and checks them without I/O. Tables
// come back deduplicated in order. via signal_table is errSignalTableLayer,
// so every stage that parses refuses it before any client call.
func parseSnapshot(raw json.RawMessage) (snapshotParams, error) {
	var p snapshotParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("actions: %s: bad params: %w", SnapshotTableName, err)
	}
	if strings.TrimSpace(p.Connector) == "" {
		return p, fmt.Errorf("actions: %s: connector is required", SnapshotTableName)
	}
	if p.Via == ViaSignalTable {
		return p, errSignalTableLayer
	}
	if p.Via != ViaSignalTopic {
		return p, fmt.Errorf("actions: %s: via must be %q, got %q", SnapshotTableName, ViaSignalTopic, capParam(p.Via))
	}
	if len(p.Tables) == 0 {
		return p, fmt.Errorf("actions: %s: tables needs at least one schema.table", SnapshotTableName)
	}
	seen := make(map[string]bool, len(p.Tables))
	tables := make([]string, 0, len(p.Tables))
	for _, t := range p.Tables {
		if !snapshotTableRe.MatchString(t) {
			return p, fmt.Errorf("actions: %s: table %q is not schema.table (letters, digits and _)", SnapshotTableName, capParam(t))
		}
		if !seen[t] {
			seen[t] = true
			tables = append(tables, t)
		}
	}
	if len(tables) > maxSnapshotTables {
		return p, fmt.Errorf("actions: %s: %d tables, at most %d per step", SnapshotTableName, len(tables), maxSnapshotTables)
	}
	p.Tables = tables
	return p, nil
}

// CheckScope refuses a connector that is not the pipeline's and a table that
// is not one of its source_tables (tools.ErrOutOfScope), without I/O.
func (a *snapshotTable) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseSnapshot(raw)
	if err != nil {
		return err
	}
	if !slices.Contains(pl.Connectors, p.Connector) {
		return fmt.Errorf("actions: %s: %w: connector %q is not part of pipeline %q", SnapshotTableName, tools.ErrOutOfScope, capParam(p.Connector), pl.ID)
	}
	for _, t := range p.Tables {
		if !slices.Contains(pl.SourceTables, t) {
			return fmt.Errorf("actions: %s: %w: table %q is not one of pipeline %q's source_tables (%s)", SnapshotTableName, tools.ErrOutOfScope, t, pl.ID, strings.Join(pl.SourceTables, ", "))
		}
	}
	return nil
}

// signalSetup is what the connector config says about its signals.
type signalSetup struct {
	class       string
	prefix      string // topic.prefix: the key of a Kafka signal
	channels    []string
	topic       string // signal.kafka.topic
	collection  string // signal.data.collection
	includeList string // table.include.list
}

// readSetup reads the connector's config and checks it takes a signal on its
// signal topic (the only via parseSnapshot lets through). Every
// refusal of a missing key wraps ErrSignalNotConfigured and says what to set.
func (a *snapshotTable) readSetup(ctx context.Context, p snapshotParams) (signalSetup, error) {
	if a.d.Connect == nil {
		return signalSetup{}, fmt.Errorf("actions: %s: no Connect client to read connector %q's config", SnapshotTableName, capParam(p.Connector))
	}
	cfg, err := a.d.Connect.Config(ctx, p.Connector)
	if err != nil {
		return signalSetup{}, fmt.Errorf("actions: %s: read connector %q config: %w", SnapshotTableName, capParam(p.Connector), err)
	}
	s := signalSetup{
		class:       cfg["connector.class"],
		prefix:      strings.TrimSpace(cfg["topic.prefix"]),
		topic:       strings.TrimSpace(cfg["signal.kafka.topic"]),
		collection:  strings.TrimSpace(cfg["signal.data.collection"]),
		includeList: cfg["table.include.list"],
	}
	// Debezium's default when the key is unset is the source channel only.
	ch := cfg["signal.enabled.channels"]
	if strings.TrimSpace(ch) == "" {
		ch = "source"
	}
	for _, c := range strings.Split(ch, ",") {
		if c = strings.ToLower(strings.TrimSpace(c)); c != "" {
			s.channels = append(s.channels, c)
		}
	}
	if !strings.HasPrefix(s.class, "io.debezium.connector.") {
		return s, fmt.Errorf("actions: %s: connector %q is not a Debezium source connector (connector.class %q); only Debezium takes snapshot signals", SnapshotTableName, capParam(p.Connector), capParam(s.class))
	}
	var missing []string
	if !slices.Contains(s.channels, "kafka") {
		missing = append(missing, fmt.Sprintf("signal.enabled.channels to include kafka (now %q, e.g. %q)", strings.Join(s.channels, ","), strings.Join(append(slices.Clone(s.channels), "kafka"), ",")))
	}
	if s.topic == "" {
		missing = append(missing, "signal.kafka.topic to a one-partition topic the connector reads signals from")
	}
	if strings.TrimSpace(cfg["signal.kafka.bootstrap.servers"]) == "" {
		missing = append(missing, "signal.kafka.bootstrap.servers to the brokers of that topic")
	}
	if s.prefix == "" {
		missing = append(missing, "topic.prefix (the key of every signal record)")
	}
	// An incremental snapshot that is not read-only writes its window
	// watermarks to the signal table, whichever channel carried the signal.
	if s.collection == "" && !strings.EqualFold(strings.TrimSpace(cfg["read.only"]), "true") {
		missing = append(missing, "signal.data.collection to the signal table (schema.table, in the publication): Debezium writes the snapshot's window watermarks there even when the signal comes over Kafka")
	}
	if len(missing) > 0 {
		return s, fmt.Errorf("actions: %s: connector %q cannot take a snapshot signal via %s; set %s (a connect_update_connector_config step), or propose pg_snapshot_signal (a signal table row, postgres layer) if the connector has the source channel: %w",
			SnapshotTableName, capParam(p.Connector), p.Via, strings.Join(missing, "; "), ErrSignalNotConfigured)
	}
	// A signal topic that is one of the snapshotted tables' CDC topics would
	// put a signal record among the data records consumers read.
	for _, t := range p.Tables {
		if s.topic == s.prefix+"."+t {
			return s, fmt.Errorf("actions: %s: connector %q's signal.kafka.topic %q is the CDC topic of %s; point it at a topic of its own: %w", SnapshotTableName, capParam(p.Connector), capParam(s.topic), t, ErrSignalNotConfigured)
		}
	}
	return s, nil
}

// Validate reads the connector config and refuses a connector that is not a
// Debezium source, and a via it is not configured for (ErrSignalNotConfigured,
// naming the keys to set). Tables outside source_tables are CheckScope's.
// It never writes, so it works with write mode off (no writer); a wired
// writer without SendSnapshotSignal is ErrNoSignalWriter, before any I/O.
func (a *snapshotTable) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseSnapshot(raw)
	if err != nil {
		return err
	}
	if a.d.Kafka != nil {
		if _, ok := a.d.Kafka.(snapshotSignaler); !ok {
			return ErrNoSignalWriter
		}
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	if _, err := a.readSetup(ctx, p); err != nil {
		return err
	}
	return nil
}

// Preview shows the tables (with a row estimate when the source client has
// one), what the snapshot re-emits and how to stop it. It reads the Connect
// config and the source client only.
func (a *snapshotTable) Preview(ctx context.Context, raw json.RawMessage) (PreviewResult, error) {
	p, err := parseSnapshot(raw)
	if err != nil {
		return PreviewResult{}, err
	}
	if err := a.ctxScope(ctx, raw); err != nil {
		return PreviewResult{}, err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	s, err := a.readSetup(ctx, p)
	if err != nil {
		return PreviewResult{}, previewErr(SnapshotTableName, err)
	}
	tables := strings.Join(p.Tables, ", ")
	target := fmt.Sprintf("incremental snapshot of %s by connector %s, signal on topic %s", tables, p.Connector, s.topic)
	how := fmt.Sprintf("one execute-snapshot record on %s keyed %q", s.topic, s.prefix)
	stopVal, _ := kafka.StopSnapshotSignalValue(p.Tables) // tables already checked by parseSnapshot
	stop := fmt.Sprintf("produce %s with key %q to %s", stopVal, s.prefix, s.topic)
	rows := a.estimates(ctx, s.prefix, p.Tables)
	downstream := []string{
		fmt.Sprintf("history is re-emitted: every existing row of %s reaches its topic again as a read event (op r), next to live changes. Sinks that upsert or merge by key converge; append-only or other non-idempotent consumers get duplicates", tables),
		"not undone by a later action: rows already emitted stay in the topics. To stop the snapshot before it ends, " + stop + " (not a registered action)",
	}
	if miss := notIncluded(s.includeList, p.Tables); len(miss) > 0 {
		downstream = append(downstream, fmt.Sprintf("table.include.list does not match %s: Debezium skips a table it does not capture, so add it there first", strings.Join(miss, ", ")))
	}
	return PreviewResult{
		Target:  target,
		Current: fmt.Sprintf("connector %s (%s, topic.prefix %s, signal channels %s); %s", p.Connector, s.class, s.prefix, strings.Join(s.channels, ","), rows),
		Expected: fmt.Sprintf("%s; the connector reads the tables in chunks (incremental.snapshot.chunk.size, default 1024 rows) while streaming continues, and logs the snapshot's start and end",
			how),
		Downstream: downstream,
		Reversible: false,
	}, nil
}

// estimates renders the row estimate of each table, or why there is none.
func (a *snapshotTable) estimates(ctx context.Context, prefix string, tables []string) string {
	re, ok := a.d.Source.(tableRowEstimator)
	if !ok {
		return "row estimate unavailable (the source client has no estimate)"
	}
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		n, err := re.EstimatedRows(ctx, t)
		switch {
		case err != nil:
			parts = append(parts, fmt.Sprintf("%s: estimate unavailable (%v)", t, err))
		case n < 0:
			parts = append(parts, fmt.Sprintf("%s: never analysed, no estimate", t))
		default:
			parts = append(parts, fmt.Sprintf("%s: ~%d rows to %s.%s", t, n, prefix, t))
		}
	}
	return strings.Join(parts, "; ")
}

// notIncluded lists the tables that table.include.list (comma-separated
// regular expressions matched against the whole name) does not match. An
// unset list captures every table; a pattern that does not compile is not
// judged.
func notIncluded(list string, tables []string) []string {
	if strings.TrimSpace(list) == "" {
		return nil
	}
	var pats []*regexp.Regexp
	for _, s := range strings.Split(list, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		re, err := regexp.Compile("^(?:" + s + ")$")
		if err != nil {
			return nil
		}
		pats = append(pats, re)
	}
	var out []string
	for _, t := range tables {
		if !slices.ContainsFunc(pats, func(re *regexp.Regexp) bool { return re.MatchString(t) }) {
			out = append(out, t)
		}
	}
	return out
}

// Execute re-reads the connector config (it may have changed since the
// approval) and sends the signal as a record on its signal topic through the
// Kafka writer's SendSnapshotSignal (ErrNoSignalWriter without it, before any
// I/O). via signal_table never gets here (parseSnapshot).
func (a *snapshotTable) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseSnapshot(raw)
	if err != nil {
		return "", err
	}
	if err := a.ctxScope(ctx, raw); err != nil {
		return "", err
	}
	if a.d.Kafka == nil {
		return "", fmt.Errorf("actions: %s: no Kafka write client (PIPEROOM_WRITE_MODE=on is needed); nothing was sent", SnapshotTableName)
	}
	ss, ok := a.d.Kafka.(snapshotSignaler)
	if !ok {
		return "", ErrNoSignalWriter
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	s, err := a.readSetup(ctx, p)
	if err != nil {
		return "", err
	}
	tables := strings.Join(p.Tables, ", ")
	sig, err := ss.SendSnapshotSignal(ctx, s.topic, p.Connector, s.prefix, p.Tables)
	if err != nil {
		return "", fmt.Errorf("actions: %s: signal to %s: %w", SnapshotTableName, s.topic, err)
	}
	id := fmt.Sprintf("%s (partition %d offset %d, the id Debezium logs)", sig.ID, sig.Partition, sig.Offset)
	return fmt.Sprintf("sent execute-snapshot signal %s (incremental) for %s to connector %s on topic %s (key %s); Debezium snapshots in the background and emits the rows to their topics", id, tables, p.Connector, s.topic, s.prefix), nil
}

// ctxScope is the fail-closed scope gate of Preview and Execute, before any
// I/O: the room's pipeline must be in ctx (tools.WithPipeline) and the params
// in it; tools.WithUnscoped (tests, CLI) passes; anything else is
// tools.ErrNoScope.
func (a *snapshotTable) ctxScope(ctx context.Context, raw json.RawMessage) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return fmt.Errorf("actions: %s: needs the room's pipeline in ctx: %w", SnapshotTableName, err)
	}
	if sc.Unscoped() {
		return nil
	}
	return a.CheckScope(sc.Pipeline(), raw)
}

func (a *snapshotTable) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	t := timeout
	if a.d.Timeout > 0 {
		t = a.d.Timeout
	}
	return context.WithTimeout(ctx, t)
}

// capParam caps a param echoed in an error.
func capParam(s string) string {
	const n = 80
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}
