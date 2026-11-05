package pgactions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// SnapshotSignalName is the registered name of the action.
const SnapshotSignalName = "pg_snapshot_signal"

const (
	// snapSigTimeout bounds the reads of Validate and Preview and the whole
	// of Execute (AGENTS.md §4 rule 4).
	snapSigTimeout = 10 * time.Second
	// snapSigMaxTables caps the tables of one step: one signal, one
	// approval. The signal table's data column (varchar(2048)) also caps it;
	// the writer refuses what does not fit.
	snapSigMaxTables = 50
	// snapSigDefaultChunk is Debezium's incremental.snapshot.chunk.size
	// default.
	snapSigDefaultChunk = 1024
	// snapSigLargeRows is the estimate above which the preview warns that
	// the snapshot is large: every row is emitted again to its topic.
	snapSigLargeRows = 10_000_000
	// snapSigMaxData is the signal table's data column as the Debezium docs
	// define it, varchar(2048); the writer refuses a longer value, so the
	// step is refused before anyone approves it.
	snapSigMaxData = 2048
)

// snapSigTableRe is a "schema.table" as owners.yaml lists source tables, each
// part AGENTS.md §4 rule 6 and at most NAMEDATALEN-1 bytes.
var snapSigTableRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}\.[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

var (
	// ErrSignalTableNotConfigured refuses a connector that cannot take a
	// signal-table snapshot signal; the message names the keys to set.
	ErrSignalTableNotConfigured = errors.New("pgactions: the connector is not configured for a signal-table snapshot signal")
	// ErrSignalTableChanged refuses a step whose signal_table is not the
	// connector's signal.data.collection any more: the row would land in a
	// table the approvers never saw.
	ErrSignalTableChanged = errors.New("pgactions: the connector's signal table is not the one in the step")
	// ErrSignalIsSourceTable refuses a signal table that is one of the
	// pipeline's source tables or a table being snapshotted: a signal row
	// must never be written into a captured data table.
	ErrSignalIsSourceTable = errors.New("pgactions: the signal table is a captured data table")
	// errSnapSigNoWriter is Execute's refusal when wiring handed the action
	// no SourceWriter (write mode off, or SOURCE_DB_WRITE_URL empty).
	errSnapSigNoWriter = errors.New("no write credentials: the source Postgres writer is not configured (PIPEROOM_WRITE_MODE=on and SOURCE_DB_WRITE_URL are both needed)")
)

// SnapshotSignalReader is the read the action needs from the source client:
// *postgres.DB implements it on the read-only pool. It is not on
// clients.SourceDB, so the action finds it with a type assertion.
type SnapshotSignalReader interface {
	SnapshotSignalState(ctx context.Context, publication, signalTable string, tables []string) (postgres.SnapshotSignalState, error)
}

var _ SnapshotSignalReader = (*postgres.DB)(nil)

// SnapshotSignalDeps are the clients of pg_snapshot_signal.
type SnapshotSignalDeps struct {
	// Connect reads the connector config (signal.data.collection,
	// signal.enabled.channels, publication.name). The read-only Connect
	// client is enough.
	Connect tools.ConfigReader
	// Source is the read-only source client; it must implement
	// SnapshotSignalReader (the real *postgres.DB does).
	Source clients.SourceDB
	// Writer is used by Execute only. nil when write mode is off or
	// SOURCE_DB_WRITE_URL is empty: Validate and Preview still work, so the
	// step can be proposed and reviewed, and Execute refuses.
	Writer clients.SourceWriter

	// timeout overrides snapSigTimeout; tests only.
	timeout time.Duration
}

// NewSnapshotSignal returns pg_snapshot_signal: one Debezium execute-snapshot
// row (an incremental snapshot) in the connector's signal table.
func NewSnapshotSignal(d SnapshotSignalDeps) actions.Action { return &snapshotSignal{d: d} }

type snapshotSignal struct{ d SnapshotSignalDeps }

var (
	_ actions.Scoped  = (*snapshotSignal)(nil)
	_ actions.Preview = (*snapshotSignal)(nil)
)

func (a *snapshotSignal) Name() string { return SnapshotSignalName }

// Layer is postgres: the signal is a row written to the source database, so
// its owner approves it (#620 ruling). The Kafka signal topic variant is
// connect_snapshot_table (kafka-connect).
func (a *snapshotSignal) Layer() string { return "postgres" }

func (a *snapshotSignal) Describe() string {
	return "Asks a Debezium Postgres source connector for an incremental snapshot of some of the pipeline's source tables by inserting one execute-snapshot row " +
		"(type INCREMENTAL) into the connector's signal table (its signal.data.collection, which must have source in signal.enabled.channels and be in the publication). " +
		"The connector re-reads the tables in chunks and emits every existing row again (op r) to their topics while streaming goes on: " +
		"for a table whose rows never reached Kafka or reached it corrupted. Downstream sinks see duplicates unless they upsert by key. " +
		"For a connector that reads signals from Kafka use connect_snapshot_table (via signal_topic)."
}

func (a *snapshotSignal) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "The pipeline's Debezium Postgres source connector; its signal.data.collection is the table written to."},
			"tables": map[string]any{
				"type":        "array",
				"description": "Source tables to snapshot, \"schema.table\", each one of the pipeline's source_tables.",
				"items":       map[string]any{"type": "string", "pattern": snapSigTableRe.String()},
				"minItems":    1,
				"maxItems":    snapSigMaxTables,
			},
			"signal_table": signalTableProp(),
		},
		"additionalProperties": false,
		"required":             []string{"connector", "tables", "signal_table"},
	}
}

// signalTableProp is the required signal_table param: the write target, named
// in the params (and so in params_hash) so approvers approve a named table and
// Execute can tell when the config moved since.
func signalTableProp() map[string]any {
	return map[string]any{
		"type":        "string",
		"description": "Required: the connector's signal.data.collection (\"schema.table\") exactly as read with connect_config; the signal row is written there. Validate refuses any other value, and Execute refuses when the config changed since approval.",
		"pattern":     snapSigTableRe.String(),
	}
}

type snapshotSignalParams struct {
	Connector string   `json:"connector"`
	Tables    []string `json:"tables"`
	// SignalTable is the signal table the step was proposed and approved
	// with: the write target, which must equal the connector's
	// signal.data.collection at Validate and at Execute.
	SignalTable string `json:"signal_table"`
}

// parseSnapshotSignal decodes params strictly and checks every identifier
// before anything reads or writes. Tables come back deduplicated in order.
func parseSnapshotSignal(raw json.RawMessage) (snapshotSignalParams, error) {
	var p snapshotSignalParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("pgactions: %s: bad params: %w", SnapshotSignalName, err)
	}
	if strings.TrimSpace(p.Connector) == "" || len(p.Connector) > 255 {
		return p, fmt.Errorf("pgactions: %s: connector is required (at most 255 bytes)", SnapshotSignalName)
	}
	if len(p.Tables) == 0 {
		return p, fmt.Errorf("pgactions: %s: tables is required (at least one \"schema.table\")", SnapshotSignalName)
	}
	if len(p.Tables) > snapSigMaxTables {
		return p, fmt.Errorf("pgactions: %s: %d tables, at most %d per signal", SnapshotSignalName, len(p.Tables), snapSigMaxTables)
	}
	if strings.TrimSpace(p.SignalTable) == "" {
		return p, fmt.Errorf("pgactions: %s: bad params: signal_table is required: the connector's signal.data.collection (\"schema.table\") as read with connect_config, so the approvers approve a named write target", SnapshotSignalName)
	}
	if !snapSigTableRe.MatchString(p.SignalTable) {
		return p, fmt.Errorf("pgactions: %s: invalid signal_table %q: want \"schema.table\"", SnapshotSignalName, addPubCut(p.SignalTable))
	}
	uniq := make([]string, 0, len(p.Tables))
	for _, t := range p.Tables {
		if !snapSigTableRe.MatchString(t) {
			return p, fmt.Errorf("pgactions: %s: invalid table %q: want \"schema.table\", each part letters, digits and _, not starting with a digit, at most 63", SnapshotSignalName, addPubCut(t))
		}
		if !slices.Contains(uniq, t) {
			uniq = append(uniq, t)
		}
	}
	p.Tables = uniq
	if n := len(snapSigData(uniq)); n > snapSigMaxData {
		return p, fmt.Errorf("pgactions: %s: the signal data for %d tables is %d bytes, over the %d of the signal table's data column: split the tables over several signals", SnapshotSignalName, len(uniq), n, snapSigMaxData)
	}
	return p, nil
}

func (a *snapshotSignal) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	t := snapSigTimeout
	if a.d.timeout > 0 {
		t = a.d.timeout
	}
	return context.WithTimeout(ctx, t)
}

func (a *snapshotSignal) reader() (SnapshotSignalReader, error) {
	r, ok := a.d.Source.(SnapshotSignalReader)
	if !ok {
		return nil, fmt.Errorf("pgactions: %s: the source client cannot read the signal table (no SnapshotSignalState)", SnapshotSignalName)
	}
	return r, nil
}

// signalConfig is what the connector config says about its signal table.
type signalConfig struct {
	cfg         map[string]string
	table       string // signal.data.collection, "schema.table"
	publication string // publication.name, default dbz_publication
	chunk       int    // incremental.snapshot.chunk.size
}

// connectorSignal reads the connector config and refuses a connector that is
// not a Debezium Postgres source or cannot take a signal-table signal: no
// signal.data.collection, a collection that is not "schema.table", or source
// missing from signal.enabled.channels (Debezium's default is source).
func (a *snapshotSignal) connectorSignal(ctx context.Context, connector string) (signalConfig, error) {
	if a.d.Connect == nil {
		return signalConfig{}, fmt.Errorf("pgactions: %s: no Connect client to read connector %q's signal.data.collection", SnapshotSignalName, addPubCut(connector))
	}
	cfg, err := a.d.Connect.Config(ctx, connector)
	if err != nil {
		return signalConfig{}, fmt.Errorf("pgactions: %s: read connector %q config: %w", SnapshotSignalName, addPubCut(connector), err)
	}
	if !strings.Contains(cfg["connector.class"], "PostgresConnector") {
		return signalConfig{}, fmt.Errorf("pgactions: %s: connector %q is not a Debezium Postgres source connector (connector.class %q)", SnapshotSignalName, addPubCut(connector), addPubCut(cfg["connector.class"]))
	}
	sc := signalConfig{cfg: cfg, table: strings.TrimSpace(cfg["signal.data.collection"]), publication: strings.TrimSpace(cfg["publication.name"]), chunk: snapSigDefaultChunk}
	if sc.publication == "" {
		sc.publication = addPubDefaultPublication
	}
	if n, err := strconv.Atoi(strings.TrimSpace(cfg["incremental.snapshot.chunk.size"])); err == nil && n > 0 {
		sc.chunk = n
	}
	var missing []string
	if channels := strings.TrimSpace(cfg["signal.enabled.channels"]); channels != "" {
		on := false
		for _, c := range strings.Split(channels, ",") {
			if strings.EqualFold(strings.TrimSpace(c), "source") {
				on = true
			}
		}
		if !on {
			missing = append(missing, fmt.Sprintf("signal.enabled.channels to include source (now %q)", addPubCut(channels)))
		}
	}
	if sc.table == "" {
		missing = append(missing, "signal.data.collection to the signal table (schema.table with columns id, type, data, in the publication)")
	}
	if len(missing) > 0 {
		return sc, fmt.Errorf("pgactions: %s: connector %q needs %s (or use connect_snapshot_table via signal_topic if it reads signals from Kafka): %w",
			SnapshotSignalName, addPubCut(connector), strings.Join(missing, " and "), ErrSignalTableNotConfigured)
	}
	if !snapSigTableRe.MatchString(sc.table) {
		return sc, fmt.Errorf("pgactions: %s: connector %q's signal.data.collection %q is not schema.table: %w", SnapshotSignalName, addPubCut(connector), addPubCut(sc.table), ErrSignalTableNotConfigured)
	}
	if !addPubIdentRe.MatchString(sc.publication) {
		return sc, fmt.Errorf("pgactions: %s: connector %q's publication.name %q is not a plain identifier", SnapshotSignalName, addPubCut(connector), addPubCut(sc.publication))
	}
	return sc, nil
}

// checkSignalTarget refuses a write target the approvers did not see or must
// never get a signal row: a signal_table in the step that is not the
// connector's signal.data.collection now (the config changed), and a signal
// table that is one of the tables being snapshotted or, with the room's
// pipeline known, one of its source_tables.
func checkSignalTarget(p snapshotSignalParams, sc signalConfig, pl owners.Pipeline, scoped bool) error {
	if p.SignalTable != sc.table {
		return fmt.Errorf("pgactions: %s: the step names signal table %s but connector %q's signal.data.collection is now %s; the connector config changed since the step was proposed: propose it again: %w",
			SnapshotSignalName, p.SignalTable, addPubCut(p.Connector), sc.table, ErrSignalTableChanged)
	}
	if slices.Contains(p.Tables, sc.table) || (scoped && slices.Contains(pl.SourceTables, sc.table)) {
		return fmt.Errorf("pgactions: %s: connector %q's signal.data.collection %s is one of the pipeline's source tables; a signal row is never written into a captured data table: %w",
			SnapshotSignalName, addPubCut(p.Connector), sc.table, ErrSignalIsSourceTable)
	}
	return nil
}

// checkSignalState refuses a signal the connector would never see or a table
// that does not exist.
func checkSignalState(sc signalConfig, st postgres.SnapshotSignalState) error {
	if !st.SignalExists {
		return fmt.Errorf("pgactions: %s: signal table %s (signal.data.collection) does not exist: %w", SnapshotSignalName, sc.table, clients.ErrNotFound)
	}
	if !st.SignalColumns {
		return fmt.Errorf("pgactions: %s: signal table %s lacks the id, type and data columns Debezium reads: %w", SnapshotSignalName, sc.table, ErrSignalTableNotConfigured)
	}
	if !st.PublicationExists {
		return fmt.Errorf("pgactions: %s: publication %s (publication.name) does not exist: %w", SnapshotSignalName, sc.publication, clients.ErrNotFound)
	}
	if !st.SignalPublished {
		return fmt.Errorf("pgactions: %s: signal table %s is not in publication %s, so the connector never sees the signal row; add it first (pg_add_publication_table): %w", SnapshotSignalName, sc.table, sc.publication, ErrSignalTableNotConfigured)
	}
	var gone []string
	for _, t := range st.Tables {
		if !t.Exists {
			gone = append(gone, t.Table)
		}
	}
	if len(gone) > 0 {
		return fmt.Errorf("pgactions: %s: table %s does not exist: %w", SnapshotSignalName, strings.Join(gone, ", "), clients.ErrNotFound)
	}
	return nil
}

// Validate reads the connector config and the source on the read-only
// clients and refuses a connector without a usable signal table, a signal
// table other than the step's signal_table or among the source tables (with
// the room's pipeline in ctx), and a table that does not exist. It does not
// depend on the scope in ctx (CheckScope is the scope gate) nor on the
// writer (Execute refuses without one).
func (a *snapshotSignal) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseSnapshotSignal(raw)
	if err != nil {
		return err
	}
	r, err := a.reader()
	if err != nil {
		return err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	sc, err := a.connectorSignal(ctx, p.Connector)
	if err != nil {
		return err
	}
	pl, scoped := tools.PipelineFrom(ctx)
	if err := checkSignalTarget(p, sc, pl, scoped); err != nil {
		return err
	}
	st, err := r.SnapshotSignalState(ctx, sc.publication, sc.table, p.Tables)
	if err != nil {
		return fmt.Errorf("pgactions: %s: %w", SnapshotSignalName, err)
	}
	return checkSignalState(sc, st)
}

// CheckScope refuses, without I/O, a connector or table outside p and a
// pipeline that is not a CDC one.
func (a *snapshotSignal) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseSnapshotSignal(raw)
	if err != nil {
		return err
	}
	return snapSigInScope(pl, p)
}

func snapSigInScope(pl owners.Pipeline, p snapshotSignalParams) error {
	if err := refuseNonPostgres(SnapshotSignalName, pl); err != nil {
		return err
	}
	if pl.KindOrDefault() != events.PipelineKindCDC {
		return fmt.Errorf("pgactions: %s: %w: pipeline %q is not a CDC pipeline", SnapshotSignalName, tools.ErrOutOfScope, pl.ID)
	}
	if !slices.Contains(pl.Connectors, p.Connector) {
		return fmt.Errorf("pgactions: %s: %w: connector %q is not one of pipeline %q's connectors", SnapshotSignalName, tools.ErrOutOfScope, addPubCut(p.Connector), pl.ID)
	}
	for _, t := range p.Tables {
		if !slices.Contains(pl.SourceTables, t) {
			return fmt.Errorf("pgactions: %s: %w: table %s is not one of pipeline %q's source_tables", SnapshotSignalName, tools.ErrOutOfScope, t, pl.ID)
		}
	}
	return nil
}

// snapSigScope is the fail-closed scope gate of Preview and Execute, before
// any I/O: the room's whole pipeline (targets checked), or the explicit
// tools.WithUnscoped opt-out; anything else is tools.ErrNoScope.
func snapSigScope(ctx context.Context, p snapshotSignalParams) (owners.Pipeline, bool, error) {
	if pl, ok := tools.PipelineFrom(ctx); ok {
		return pl, true, snapSigInScope(pl, p)
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err == nil && sc.Unscoped() {
		return owners.Pipeline{}, false, nil
	}
	if err == nil || !errors.Is(err, tools.ErrNoScope) {
		err = fmt.Errorf("%w: %v", tools.ErrNoScope, err)
	}
	return owners.Pipeline{}, false, fmt.Errorf("pgactions: %s: it needs the room's whole pipeline in ctx: %w", SnapshotSignalName, err)
}

// Preview reads the connector config and, on the read-only source client,
// the signal table and each table's row estimate (pg_class.reltuples). It
// never holds the writer.
func (a *snapshotSignal) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseSnapshotSignal(raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	pl, scoped, err := snapSigScope(ctx, p)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	r, err := a.reader()
	if err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	sc, err := a.connectorSignal(ctx, p.Connector)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	st, err := r.SnapshotSignalState(ctx, sc.publication, sc.table, p.Tables)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("pgactions: %s: preview: %w", SnapshotSignalName, err)
	}
	return snapSigPreview(p, pl, scoped, sc, st, checkSignalTarget(p, sc, pl, scoped)), nil
}

// snapSigData is the data column the writer inserts, rendered for the
// preview the same way (postgres.Writer.InsertSnapshotSignal).
func snapSigData(tables []string) string {
	b, err := json.Marshal(struct {
		DataCollections []string `json:"data-collections"`
		Type            string   `json:"type"`
	}{tables, "INCREMENTAL"})
	if err != nil {
		return ""
	}
	return string(b)
}

// snapSigPreview renders the preview from what was read.
func snapSigPreview(p snapshotSignalParams, pl owners.Pipeline, scoped bool, sc signalConfig, st postgres.SnapshotSignalState, refused error) actions.PreviewResult {
	res := actions.PreviewResult{
		Target:     fmt.Sprintf("signal table %s of connector %s on the source Postgres", sc.table, p.Connector),
		Current:    snapSigCurrent(sc, st),
		Reversible: false,
		Rollback: fmt.Sprintf("a running incremental snapshot stops on a stop-snapshot signal: INSERT INTO %s (id, type, data) VALUES ('<new id>', 'stop-snapshot', '{\"data-collections\":[..],\"type\":\"INCREMENTAL\"}') (by a DBA; no registered action). Rows already re-emitted stay in Kafka.",
			addPubQuoteTable(sc.table)),
	}
	if refused == nil {
		refused = checkSignalState(sc, st)
	}
	if err := refused; err != nil {
		res.Expected = "no change: this step is refused (" + strings.TrimPrefix(err.Error(), "pgactions: "+SnapshotSignalName+": ") + ")"
		res.Rollback = ""
		return res
	}
	var total int64
	unknown := 0
	for _, t := range st.Tables {
		if t.EstimatedRows < 0 {
			unknown++
			continue
		}
		total += t.EstimatedRows
	}
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (id, type, data) VALUES ('<new uuid>', 'execute-snapshot', '%s'). ", addPubQuoteTable(sc.table), snapSigData(p.Tables))
	fmt.Fprintf(&b, "Connector %s re-reads %s in chunks of %d rows (incremental.snapshot.chunk.size) and emits every row again as a read event (op r) while streaming goes on; ", p.Connector, strings.Join(p.Tables, ", "), sc.chunk)
	fmt.Fprintf(&b, "~%d rows in all (pg_class.reltuples", total)
	if unknown > 0 {
		fmt.Fprintf(&b, "; %d table(s) never analyzed, not counted", unknown)
	}
	b.WriteString(").")
	if total >= snapSigLargeRows {
		fmt.Fprintf(&b, " Warning: a large snapshot, ~%d rows in ~%d chunks; it adds load on the source and lag downstream until done.", total, (total+int64(sc.chunk)-1)/int64(sc.chunk))
	}
	for _, c := range addPubDebeziumCaveats(sc.table, sc.cfg) {
		b.WriteString(" Signal table: ")
		b.WriteString(c)
	}
	res.Expected = b.String()
	for _, t := range p.Tables {
		if scoped && pl.TopicPrefix != "" {
			res.Downstream = append(res.Downstream, fmt.Sprintf("topic %s.%s receives every row of %s again (op r); sinks that do not upsert by key see duplicates", pl.TopicPrefix, t, t))
		} else {
			res.Downstream = append(res.Downstream, fmt.Sprintf("the topic of %s receives every row again (op r); sinks that do not upsert by key see duplicates", t))
		}
	}
	return res
}

func snapSigCurrent(sc signalConfig, st postgres.SnapshotSignalState) string {
	var b strings.Builder
	switch {
	case !st.SignalExists:
		fmt.Fprintf(&b, "signal table %s does not exist", sc.table)
	case !st.SignalColumns:
		fmt.Fprintf(&b, "signal table %s exists but lacks the id, type, data columns", sc.table)
	case st.SignalPublished:
		fmt.Fprintf(&b, "signal table %s exists and is in publication %s", sc.table, sc.publication)
	default:
		fmt.Fprintf(&b, "signal table %s exists but is NOT in publication %s", sc.table, sc.publication)
	}
	parts := make([]string, len(st.Tables))
	for i, t := range st.Tables {
		switch {
		case !t.Exists:
			parts[i] = t.Table + " does not exist"
		case t.EstimatedRows < 0:
			parts[i] = t.Table + " (never analyzed)"
		default:
			parts[i] = fmt.Sprintf("%s ~%d rows", t.Table, t.EstimatedRows)
		}
	}
	b.WriteString("; ")
	b.WriteString(strings.Join(parts, ", "))
	return b.String()
}

// Execute re-reads the connector's signal table from its config (the agent
// never chooses where the row goes; the required signal_table pins what was
// approved and must still equal it), re-runs the target and signal-state
// checks of Validate, and only then inserts the signal through the
// SourceWriter.
func (a *snapshotSignal) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseSnapshotSignal(raw)
	if err != nil {
		return "", err
	}
	pl, scoped, err := snapSigScope(ctx, p)
	if err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", fmt.Errorf("pgactions: %s: %w", SnapshotSignalName, errSnapSigNoWriter)
	}
	r, err := a.reader()
	if err != nil {
		return "", err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	sc, err := a.connectorSignal(ctx, p.Connector)
	if err != nil {
		return "", err
	}
	// The connector config may have changed since approval: the row goes
	// only into the table the step names (when it names one), never into a
	// source table, and only into a signal table that passes the same checks
	// as at Validate, read again now.
	if err := checkSignalTarget(p, sc, pl, scoped); err != nil {
		return "", err
	}
	st, err := r.SnapshotSignalState(ctx, sc.publication, sc.table, p.Tables)
	if err != nil {
		return "", fmt.Errorf("pgactions: %s: re-check before the write: %w", SnapshotSignalName, err)
	}
	if err := checkSignalState(sc, st); err != nil {
		return "", fmt.Errorf("pgactions: %s: refused at execute, nothing written: %w", SnapshotSignalName, err)
	}
	id, err := a.d.Writer.InsertSnapshotSignal(ctx, sc.table, p.Connector, p.Tables)
	if err != nil {
		return "", fmt.Errorf("pgactions: %s: %w", SnapshotSignalName, err)
	}
	return fmt.Sprintf("inserted execute-snapshot signal %s (INCREMENTAL) into %s for connector %s: %s. The connector re-emits the tables' rows (op r) as it reads the signal; its log shows the incremental snapshot starting.",
		id, sc.table, p.Connector, strings.Join(p.Tables, ", ")), nil
}
