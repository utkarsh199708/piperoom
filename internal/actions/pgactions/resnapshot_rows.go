package pgactions

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ResnapshotRowsName is the registered name of the action.
const ResnapshotRowsName = "pg_resnapshot_rows"

const (
	// resnapTimeout bounds Validate, Preview and Verify and the whole of
	// Execute (AGENTS.md §4 rule 4).
	resnapTimeout = 10 * time.Second
	// resnapMaxValue caps one bound as the model writes it: a key or a time
	// is far shorter, and the condition must stay under
	// clients.MaxSnapshotConditionBytes.
	resnapMaxValue = 256
	// resnapMaxDetail is VerifyResult.Detail's cap (actions.VerifyResult).
	resnapMaxDetail = 200
)

// The two ways the signal reaches the connector, chosen from its config.
const (
	resnapViaTable = "signal_table" // a row in signal.data.collection
	resnapViaTopic = "signal_topic" // a record on signal.kafka.topic
)

var (
	// ErrResnapshotTooLarge refuses a condition that matches more source
	// rows than the pipeline's max_rows (owners.yaml recovery.max_rows,
	// default 100 000): that is a whole-table snapshot, pg_snapshot_signal.
	ErrResnapshotTooLarge = errors.New("pgactions: the condition matches more rows than max_rows")
	// errResnapNoWriter is Execute's refusal before any I/O when wiring
	// handed the action neither writer (write mode off).
	errResnapNoWriter = errors.New("no write credentials: neither the source Postgres writer nor the Kafka writer is configured (PIPEROOM_WRITE_MODE=on is needed, and SOURCE_DB_WRITE_URL for a signal-table connector)")
)

// resnapIdentRe is one column name, AGENTS.md §4 rule 6, at most
// NAMEDATALEN-1 bytes.
var resnapIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// resnapUUIDRe is a UUID in its canonical text form.
var resnapUUIDRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// RowRangeCounter is the read pg_resnapshot_rows needs on the source and,
// for Verify, on the sink: a column's kind and a range count, on the
// read-only pool. *postgres.DB and the dev Postgres *warehouse.DB implement
// it; it is not on clients.SourceDB or clients.Warehouse, so the action
// finds it with a type assertion.
type RowRangeCounter interface {
	RangeColumn(ctx context.Context, table, column string) (postgres.RangeColumn, error)
	CountRange(ctx context.Context, table, column, from, to string) (int64, error)
}

var _ RowRangeCounter = (*postgres.DB)(nil)

// SinkCounter is the sink side of Verify: the range reads and the X1 window
// count (clients.Reconciler.CountBetween) on the sink table, read-only. The
// dev Postgres *warehouse.DB has all three; wiring hands the action a view
// with these methods only, never the whole warehouse client.
type SinkCounter interface {
	RowRangeCounter
	CountBetween(ctx context.Context, table, tsColumn string, from, to time.Time) (int64, error)
}

// ResnapshotRowsDeps are the clients of pg_resnapshot_rows.
type ResnapshotRowsDeps struct {
	// Connect reads the connector config (signal channels, signal table or
	// topic, topic.prefix). The read-only Connect client is enough.
	Connect tools.ConfigReader
	// Source is the read-only source client; it must implement
	// RowRangeCounter and SnapshotSignalReader, and clients.Reconciler for
	// Verify of a time window (the real *postgres.DB does).
	Source clients.SourceDB
	// Sink is read by Verify only. nil, or answering clients.ErrNotSupported,
	// makes the outcome unverifiable.
	Sink SinkCounter
	// Writer (signal table) and Kafka (signal topic) are used by Execute
	// only, asserted as clients.SnapshotConditionWriter and
	// clients.SnapshotConditionSignaler. Both are nil with write mode off.
	Writer clients.SourceWriter
	Kafka  clients.KafkaWriter

	// timeout overrides resnapTimeout; tests only.
	timeout time.Duration
}

// NewResnapshotRows returns pg_resnapshot_rows: a Debezium incremental
// snapshot of one source table limited to a key range or a time window, so
// the connector re-emits only the rows a fault lost (X2a).
func NewResnapshotRows(d ResnapshotRowsDeps) actions.Action { return &resnapshotRows{d: d} }

type resnapshotRows struct{ d ResnapshotRowsDeps }

var (
	_ actions.Scoped   = (*resnapshotRows)(nil)
	_ actions.Preview  = (*resnapshotRows)(nil)
	_ actions.Classed  = (*resnapshotRows)(nil)
	_ actions.Verifier = (*resnapshotRows)(nil)
)

func (a *resnapshotRows) Name() string { return ResnapshotRowsName }

// Layer is postgres, like pg_snapshot_signal: the condition runs on the
// source database, so its owner approves it, whichever channel carries
// the signal.
func (a *resnapshotRows) Layer() string { return "postgres" }

// Class is recovery: the step re-emits lost rows and is verified by counts.
func (a *resnapshotRows) Class() string { return actions.ClassRecovery }

func (a *resnapshotRows) Describe() string {
	return "Recovery: asks the pipeline's Debezium Postgres source connector to re-emit only some rows of one source table: an incremental snapshot (type INCREMENTAL) " +
		"with Debezium's additional-conditions, built by the action from key_range {column, from, to} (inclusive) or since {column, at}, never from free SQL. " +
		"The column must be the table's whole primary key or a date/timestamp column; integer, text, uuid and time values are bound as quoted literals. " +
		"The signal goes where the connector config takes it: a row in its signal table (source in signal.enabled.channels) or a record on its signal.kafka.topic. " +
		"The preview counts the matching rows (\"will re-emit N rows\") and the step is refused above max_rows (owners.yaml recovery.max_rows, default 100000). " +
		"After it ran, the source and sink counts over the same range are compared until the sink has them all. Sinks that do not upsert by key see duplicates."
}

func (a *resnapshotRows) ParamsSchema() map[string]any {
	str := func(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
	ident := map[string]any{"type": "string", "pattern": resnapIdentRe.String(),
		"description": "the table's primary-key column (a single-column key) or a date/timestamp column"}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": str("The pipeline's Debezium Postgres source connector."),
			"table": map[string]any{"type": "string", "pattern": snapSigTableRe.String(),
				"description": "The source table, \"schema.table\", one of the pipeline's source_tables."},
			"key_range": map[string]any{
				"type":        "object",
				"description": "Re-emit the rows whose column is between from and to, both inclusive. Exactly one of key_range or since.",
				"properties": map[string]any{
					"column": ident,
					"from":   str("lower bound as text: an integer, a text key, a uuid, or an RFC 3339 time for a date/timestamp column"),
					"to":     str("upper bound, same form as from, not below it"),
				},
				"required":             []string{"column", "from", "to"},
				"additionalProperties": false,
			},
			"since": map[string]any{
				"type":        "object",
				"description": "Re-emit the rows whose column is at or after at. Exactly one of key_range or since.",
				"properties": map[string]any{
					"column": ident,
					"at":     str("the bound: an RFC 3339 time (e.g. 2026-10-22T09:00:00Z) for a date/timestamp column, or a key value for the primary key"),
				},
				"required":             []string{"column", "at"},
				"additionalProperties": false,
			},
			"sink_table": map[string]any{"type": "string", "pattern": snapSigTableRe.String(),
				"description": "Optional: the sink table the rows reach (one of the pipeline's sink_tables), counted after the step to verify it; by default the sink table named like the source table."},
		},
		"additionalProperties": false,
		"required":             []string{"connector", "table"},
	}
}

type resnapRange struct {
	Column string `json:"column"`
	From   string `json:"from"`
	To     string `json:"to"`
}

type resnapSince struct {
	Column string `json:"column"`
	At     string `json:"at"`
}

type resnapParams struct {
	Connector string       `json:"connector"`
	Table     string       `json:"table"`
	KeyRange  *resnapRange `json:"key_range,omitempty"`
	Since     *resnapSince `json:"since,omitempty"`
	SinkTable string       `json:"sink_table,omitempty"`
}

// column and bounds read whichever of key_range and since the step has; to
// is "" for since (no upper bound).
func (p resnapParams) column() string {
	if p.KeyRange != nil {
		return p.KeyRange.Column
	}
	return p.Since.Column
}

func (p resnapParams) bounds() (from, to string) {
	if p.KeyRange != nil {
		return p.KeyRange.From, p.KeyRange.To
	}
	return p.Since.At, ""
}

// which names the step's condition for messages, never its values.
func (p resnapParams) which() string {
	if p.KeyRange != nil {
		return "key_range"
	}
	return "since"
}

func resnapBad(format string, args ...any) error {
	return fmt.Errorf("pgactions: %s: %w: %s", ResnapshotRowsName, tools.ErrBadInput, fmt.Sprintf(format, args...))
}

// parseResnapshot decodes params strictly and checks every identifier and
// raw value before anything reads or writes. Values are checked again by
// the column's kind once it is known (resnapBound).
func parseResnapshot(raw json.RawMessage) (resnapParams, error) {
	var p resnapParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, resnapBad("bad params: %v", err)
	}
	if strings.TrimSpace(p.Connector) == "" || len(p.Connector) > 255 {
		return p, resnapBad("connector is required (at most 255 bytes)")
	}
	if !snapSigTableRe.MatchString(p.Table) {
		return p, resnapBad("invalid table %q: want \"schema.table\", each part letters, digits and _, not starting with a digit, at most 63", addPubCut(p.Table))
	}
	if (p.KeyRange == nil) == (p.Since == nil) {
		return p, resnapBad("give exactly one of key_range {column, from, to} or since {column, at}")
	}
	if c := p.column(); !resnapIdentRe.MatchString(c) {
		return p, resnapBad("invalid column %q: want letters, digits and _, not starting with a digit, at most 63", addPubCut(c))
	}
	from, to := p.bounds()
	vals := []struct{ name, v string }{{"from", from}, {"to", to}}
	if p.Since != nil {
		vals = vals[:1]
		vals[0].name = "at"
	}
	for _, v := range vals {
		if err := resnapRawValue(v.name, v.v); err != nil {
			return p, err
		}
	}
	if p.SinkTable != "" && !snapSigTableRe.MatchString(p.SinkTable) {
		return p, resnapBad("invalid sink_table %q: want \"schema.table\"", addPubCut(p.SinkTable))
	}
	return p, nil
}

// resnapRawValue refuses what no bound of any kind may hold: nothing, too
// long, invalid UTF-8, a control character or a backslash (which the
// condition grammar refuses anyway). Errors never echo the value.
func resnapRawValue(name, v string) error {
	if v == "" {
		return resnapBad("%s is required", name)
	}
	if len(v) > resnapMaxValue {
		return resnapBad("%s is %d bytes, at most %d", name, len(v), resnapMaxValue)
	}
	if !utf8.ValidString(v) {
		return resnapBad("%s is not valid UTF-8", name)
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f || r == '\\' {
			return resnapBad("%s holds a control character or a backslash, which a condition literal cannot carry", name)
		}
	}
	return nil
}

// CheckScope refuses, without I/O, a connector, table or sink table outside
// p and a pipeline that is not a CDC one.
func (a *resnapshotRows) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseResnapshot(raw)
	if err != nil {
		return err
	}
	return resnapInScope(pl, p)
}

func resnapInScope(pl owners.Pipeline, p resnapParams) error {
	if err := refuseMySQL(ResnapshotRowsName, pl); err != nil {
		return err
	}
	if pl.KindOrDefault() != events.PipelineKindCDC {
		return fmt.Errorf("pgactions: %s: %w: pipeline %q is not a CDC pipeline", ResnapshotRowsName, tools.ErrOutOfScope, pl.ID)
	}
	if !slices.Contains(pl.Connectors, p.Connector) {
		return fmt.Errorf("pgactions: %s: %w: connector %q is not one of pipeline %q's connectors", ResnapshotRowsName, tools.ErrOutOfScope, addPubCut(p.Connector), pl.ID)
	}
	if !slices.Contains(pl.SourceTables, p.Table) {
		return fmt.Errorf("pgactions: %s: %w: table %s is not one of pipeline %q's source_tables", ResnapshotRowsName, tools.ErrOutOfScope, p.Table, pl.ID)
	}
	if p.SinkTable != "" && !slices.Contains(pl.SinkTables, p.SinkTable) {
		return fmt.Errorf("pgactions: %s: %w: sink_table %s is not one of pipeline %q's sink_tables", ResnapshotRowsName, tools.ErrOutOfScope, p.SinkTable, pl.ID)
	}
	return nil
}

// resnapScope is the fail-closed scope gate of every stage, before any I/O:
// the room's whole pipeline (targets checked), or the explicit
// tools.WithUnscoped opt-out; anything else is tools.ErrNoScope.
func resnapScope(ctx context.Context, p resnapParams) (owners.Pipeline, bool, error) {
	if pl, ok := tools.PipelineFrom(ctx); ok {
		return pl, true, resnapInScope(pl, p)
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err == nil && sc.Unscoped() {
		return owners.Pipeline{}, false, nil
	}
	if err == nil || !errors.Is(err, tools.ErrNoScope) {
		err = fmt.Errorf("%w: %v", tools.ErrNoScope, err)
	}
	return owners.Pipeline{}, false, fmt.Errorf("pgactions: %s: it needs the room's whole pipeline in ctx: %w", ResnapshotRowsName, err)
}

func (a *resnapshotRows) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	t := resnapTimeout
	if a.d.timeout > 0 {
		t = a.d.timeout
	}
	return context.WithTimeout(ctx, t)
}

func (a *resnapshotRows) counter() (RowRangeCounter, error) {
	c, ok := a.d.Source.(RowRangeCounter)
	if !ok {
		return nil, fmt.Errorf("pgactions: %s: the source client cannot count rows by range (no RangeColumn/CountRange)", ResnapshotRowsName)
	}
	return c, nil
}

// resnapCond is a step's condition, built from typed params and checked.
type resnapCond struct {
	column   string
	col      postgres.RangeColumn
	from, to string // canonical text of each bound; to "" = no upper bound
	filter   string // the Debezium additional-conditions filter
}

// resnapBound parses v by the column's kind and returns its canonical
// text: what the condition quotes and the count casts. Errors never echo v.
func resnapBound(kind, name, v string) (string, error) {
	switch kind {
	case pgconv.RangeKindInteger:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return "", resnapBad("%s is not a 64-bit integer, as the column's type needs", name)
		}
		return strconv.FormatInt(n, 10), nil
	case pgconv.RangeKindTime:
		t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(v))
		if err != nil {
			if t, err = time.Parse(time.DateOnly, strings.TrimSpace(v)); err != nil {
				return "", resnapBad("%s is not an RFC 3339 time (2026-10-22T09:00:00Z) or a date (2026-10-22), as the column's type needs", name)
			}
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	case pgconv.RangeKindUUID:
		if !resnapUUIDRe.MatchString(v) {
			return "", resnapBad("%s is not a uuid, as the column's type needs", name)
		}
		return strings.ToLower(v), nil
	case pgconv.RangeKindText:
		return v, nil
	}
	return "", resnapBad("no condition is built on this column type")
}

// resnapCompare orders two canonical bounds of kind. Text and uuid compare
// by bytes, which may differ from the column's collation for text: a range
// refused here can be given the other way round.
func resnapCompare(kind, a, b string) int {
	switch kind {
	case pgconv.RangeKindInteger:
		x, _ := strconv.ParseInt(a, 10, 64)
		y, _ := strconv.ParseInt(b, 10, 64)
		return cmp.Compare(x, y)
	case pgconv.RangeKindTime:
		x, _ := time.Parse(time.RFC3339Nano, a)
		y, _ := time.Parse(time.RFC3339Nano, b)
		return x.Compare(y)
	}
	return strings.Compare(a, b)
}

// resnapFilter builds the Debezium filter from a checked column name and
// canonical bounds: the column quoted as an identifier, each bound a quoted
// literal with ' doubled, the operators fixed. Postgres reads the literal as
// a value of the column's type, as the count's cast does. It is the only
// place a condition is made; the model never supplies SQL.
func resnapFilter(column, from, to string) string {
	c := pgx.Identifier{column}.Sanitize()
	f := c + " >= " + resnapLiteral(from)
	if to != "" {
		f += " AND " + c + " <= " + resnapLiteral(to)
	}
	return f
}

func resnapLiteral(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

// condition reads the column's kind on the source and builds the checked
// condition: the column must be the table's whole primary key or a date or
// timestamp column, the bounds values of its type with from <= to.
func (a *resnapshotRows) condition(ctx context.Context, c RowRangeCounter, p resnapParams) (resnapCond, error) {
	col := p.column()
	rc, err := c.RangeColumn(ctx, p.Table, col)
	if err != nil {
		return resnapCond{}, fmt.Errorf("pgactions: %s: column %s of %s: %w", ResnapshotRowsName, col, p.Table, err)
	}
	if !rc.PrimaryKey && rc.Kind != pgconv.RangeKindTime {
		return resnapCond{}, resnapBad("column %s of %s is neither the table's whole primary key nor a date or timestamp column (it is %s); a re-snapshot condition is built only on those", col, p.Table, addPubCut(rc.Type))
	}
	if rc.Kind == "" {
		return resnapCond{}, resnapBad("column %s of %s is %s: a condition is built only on an integer, text, uuid, date or timestamp column", col, p.Table, addPubCut(rc.Type))
	}
	rawFrom, rawTo := p.bounds()
	fromName := "key_range.from"
	if p.Since != nil {
		fromName = "since.at"
	}
	cond := resnapCond{column: col, col: rc}
	if cond.from, err = resnapBound(rc.Kind, fromName, rawFrom); err != nil {
		return resnapCond{}, err
	}
	if rawTo != "" {
		if cond.to, err = resnapBound(rc.Kind, "key_range.to", rawTo); err != nil {
			return resnapCond{}, err
		}
		if resnapCompare(rc.Kind, cond.from, cond.to) > 0 {
			return resnapCond{}, resnapBad("key_range.from is after key_range.to")
		}
	}
	cond.filter = resnapFilter(col, cond.from, cond.to)
	if err := clients.ValidateSnapshotCondition(cond.filter); err != nil {
		return resnapCond{}, fmt.Errorf("pgactions: %s: %w: %w", ResnapshotRowsName, tools.ErrBadInput, err)
	}
	return cond, nil
}

// resnapRoute is where the connector config takes the signal.
type resnapRoute struct {
	via         string // resnapViaTable | resnapViaTopic
	signalTable string // signal.data.collection
	publication string // publication.name, default dbz_publication
	topic       string // signal.kafka.topic
	prefix      string // topic.prefix, the Kafka signal's key
	chunk       int    // incremental.snapshot.chunk.size
}

// route reads the connector config and picks the signal channel: the
// signal table when source is in signal.enabled.channels (Debezium's
// default) and signal.data.collection is set, else the Kafka signal topic
// when it is fully configured; else ErrSignalTableNotConfigured, naming
// what to set.
func (a *resnapshotRows) route(ctx context.Context, p resnapParams) (resnapRoute, error) {
	if a.d.Connect == nil {
		return resnapRoute{}, fmt.Errorf("pgactions: %s: no Connect client to read connector %q's signal settings", ResnapshotRowsName, addPubCut(p.Connector))
	}
	cfg, err := a.d.Connect.Config(ctx, p.Connector)
	if err != nil {
		return resnapRoute{}, fmt.Errorf("pgactions: %s: read connector %q config: %w", ResnapshotRowsName, addPubCut(p.Connector), err)
	}
	if !strings.Contains(cfg["connector.class"], "PostgresConnector") {
		return resnapRoute{}, fmt.Errorf("pgactions: %s: connector %q is not a Debezium Postgres source connector (connector.class %q)", ResnapshotRowsName, addPubCut(p.Connector), addPubCut(cfg["connector.class"]))
	}
	r := resnapRoute{
		signalTable: strings.TrimSpace(cfg["signal.data.collection"]),
		publication: strings.TrimSpace(cfg["publication.name"]),
		topic:       strings.TrimSpace(cfg["signal.kafka.topic"]),
		prefix:      strings.TrimSpace(cfg["topic.prefix"]),
		chunk:       snapSigDefaultChunk,
	}
	if r.publication == "" {
		r.publication = addPubDefaultPublication
	}
	if n, err := strconv.Atoi(strings.TrimSpace(cfg["incremental.snapshot.chunk.size"])); err == nil && n > 0 {
		r.chunk = n
	}
	channels := strings.TrimSpace(cfg["signal.enabled.channels"])
	if channels == "" {
		channels = "source"
	}
	has := func(ch string) bool {
		return slices.ContainsFunc(strings.Split(channels, ","), func(c string) bool { return strings.EqualFold(strings.TrimSpace(c), ch) })
	}
	readOnly := strings.EqualFold(strings.TrimSpace(cfg["read.only"]), "true")
	switch {
	case has("source") && r.signalTable != "":
		if !snapSigTableRe.MatchString(r.signalTable) {
			return r, fmt.Errorf("pgactions: %s: connector %q's signal.data.collection %q is not schema.table: %w", ResnapshotRowsName, addPubCut(p.Connector), addPubCut(r.signalTable), ErrSignalTableNotConfigured)
		}
		if !addPubIdentRe.MatchString(r.publication) {
			return r, fmt.Errorf("pgactions: %s: connector %q's publication.name %q is not a plain identifier", ResnapshotRowsName, addPubCut(p.Connector), addPubCut(r.publication))
		}
		r.via = resnapViaTable
	case has("kafka") && r.topic != "" && r.prefix != "" && strings.TrimSpace(cfg["signal.kafka.bootstrap.servers"]) != "" && (r.signalTable != "" || readOnly):
		// A signal topic that is the table's CDC topic would put a signal
		// record among the data records consumers read.
		if r.topic == r.prefix+"."+p.Table {
			return r, fmt.Errorf("pgactions: %s: connector %q's signal.kafka.topic %q is the CDC topic of %s; point it at a topic of its own: %w", ResnapshotRowsName, addPubCut(p.Connector), addPubCut(r.topic), p.Table, ErrSignalTableNotConfigured)
		}
		r.via = resnapViaTopic
	default:
		return r, fmt.Errorf("pgactions: %s: connector %q takes no snapshot signal (signal.enabled.channels %q): set signal.data.collection to a signal table (schema.table with id, type, data, in the publication) with source in signal.enabled.channels, "+
			"or signal.kafka.topic, signal.kafka.bootstrap.servers and topic.prefix with kafka in signal.enabled.channels (Debezium still needs signal.data.collection for the snapshot's watermarks unless read.only=true): %w",
			ResnapshotRowsName, addPubCut(p.Connector), addPubCut(channels), ErrSignalTableNotConfigured)
	}
	return r, nil
}

// checkSignalTable refuses a signal-table route whose table is a captured
// data table, does not exist, lacks Debezium's columns or is not published
// (the connector would never see the row). Read on the read-only client.
func (a *resnapshotRows) checkSignalTable(ctx context.Context, p resnapParams, r resnapRoute, pl owners.Pipeline, scoped bool) error {
	if r.signalTable == p.Table || (scoped && slices.Contains(pl.SourceTables, r.signalTable)) {
		return fmt.Errorf("pgactions: %s: connector %q's signal.data.collection %s is one of the pipeline's source tables; a signal row is never written into a captured data table: %w",
			ResnapshotRowsName, addPubCut(p.Connector), r.signalTable, ErrSignalIsSourceTable)
	}
	sr, ok := a.d.Source.(SnapshotSignalReader)
	if !ok {
		return fmt.Errorf("pgactions: %s: the source client cannot read the signal table (no SnapshotSignalState)", ResnapshotRowsName)
	}
	st, err := sr.SnapshotSignalState(ctx, r.publication, r.signalTable, []string{p.Table})
	if err != nil {
		return fmt.Errorf("pgactions: %s: signal table: %w", ResnapshotRowsName, err)
	}
	switch {
	case !st.SignalExists:
		return fmt.Errorf("pgactions: %s: signal table %s (signal.data.collection) does not exist: %w", ResnapshotRowsName, r.signalTable, clients.ErrNotFound)
	case !st.SignalColumns:
		return fmt.Errorf("pgactions: %s: signal table %s lacks the id, type and data columns Debezium reads: %w", ResnapshotRowsName, r.signalTable, ErrSignalTableNotConfigured)
	case !st.PublicationExists:
		return fmt.Errorf("pgactions: %s: publication %s (publication.name) does not exist: %w", ResnapshotRowsName, r.publication, clients.ErrNotFound)
	case !st.SignalPublished:
		return fmt.Errorf("pgactions: %s: signal table %s is not in publication %s, so the connector never sees the signal row; add it first (pg_add_publication_table): %w", ResnapshotRowsName, r.signalTable, r.publication, ErrSignalTableNotConfigured)
	}
	return nil
}

// resnapMaxRows is the room pipeline's recovery.max_rows, or the default.
func resnapMaxRows(pl owners.Pipeline, scoped bool) int64 {
	if scoped {
		return pl.RecoveryMaxRows()
	}
	return owners.DefaultRecoveryMaxRows
}

// resnapPrep is what every stage reads before it decides.
type resnapPrep struct {
	cond  resnapCond
	route resnapRoute
	count int64
	max   int64
}

// prepare reads, on the read-only clients only: the condition's column,
// the connector's signal route (and the signal table's state on that
// route) and the count of matching source rows, refusing no rows and more
// than max_rows.
func (a *resnapshotRows) prepare(ctx context.Context, p resnapParams, pl owners.Pipeline, scoped bool) (resnapPrep, error) {
	c, err := a.counter()
	if err != nil {
		return resnapPrep{}, err
	}
	cond, err := a.condition(ctx, c, p)
	if err != nil {
		return resnapPrep{}, err
	}
	r, err := a.route(ctx, p)
	if err != nil {
		return resnapPrep{}, err
	}
	if r.via == resnapViaTable {
		if err := a.checkSignalTable(ctx, p, r, pl, scoped); err != nil {
			return resnapPrep{}, err
		}
	}
	n, err := c.CountRange(ctx, p.Table, cond.column, cond.from, cond.to)
	if err != nil {
		return resnapPrep{}, fmt.Errorf("pgactions: %s: count the matching rows: %w", ResnapshotRowsName, err)
	}
	prep := resnapPrep{cond: cond, route: r, count: n, max: resnapMaxRows(pl, scoped)}
	if n == 0 {
		return prep, resnapBad("the step's %s on %s matches no rows of %s: nothing to re-emit", p.which(), cond.column, p.Table)
	}
	if n > prep.max {
		return prep, fmt.Errorf("pgactions: %s: the step would re-emit %d rows of %s, over max_rows %d (owners.yaml recovery.max_rows): narrow the %s, or snapshot the whole table with pg_snapshot_signal: %w",
			ResnapshotRowsName, n, p.Table, prep.max, p.which(), ErrResnapshotTooLarge)
	}
	return prep, nil
}

// Validate refuses, before any I/O, bad params and a target outside the
// room's pipeline (or no scope at all: every read here is scoped); then it
// reads the column, the connector config, the signal table and the count
// on the read-only clients. It does not need a writer.
func (a *resnapshotRows) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseResnapshot(raw)
	if err != nil {
		return err
	}
	pl, scoped, err := resnapScope(ctx, p)
	if err != nil {
		return err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	_, err = a.prepare(ctx, p, pl, scoped)
	return err
}

// Preview counts the rows the condition matches on the read-only source
// ("will re-emit N rows") and says where the signal goes. It refuses above
// max_rows with the count in the message. It never holds a writer and never
// shows the condition's values.
func (a *resnapshotRows) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseResnapshot(raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	pl, scoped, err := resnapScope(ctx, p)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	prep, err := a.prepare(ctx, p, pl, scoped)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	return resnapPreview(p, pl, scoped, prep), nil
}

func resnapPreview(p resnapParams, pl owners.Pipeline, scoped bool, prep resnapPrep) actions.PreviewResult {
	n := prep.count
	how := fmt.Sprintf("one execute-snapshot row (INCREMENTAL, additional-conditions on %s) in signal table %s", prep.cond.column, prep.route.signalTable)
	target := fmt.Sprintf("signal table %s of connector %s on the source Postgres", prep.route.signalTable, p.Connector)
	if prep.route.via == resnapViaTopic {
		how = fmt.Sprintf("one execute-snapshot record (INCREMENTAL, additional-conditions on %s) on topic %s keyed %q", prep.cond.column, prep.route.topic, prep.route.prefix)
		target = fmt.Sprintf("signal topic %s of connector %s", prep.route.topic, p.Connector)
	}
	kind := "date/timestamp column"
	if prep.cond.col.PrimaryKey {
		kind = "primary key"
	}
	topic := "the topic of " + p.Table
	if scoped && pl.TopicPrefix != "" {
		topic = "topic " + pl.TopicPrefix + "." + p.Table
	}
	return actions.PreviewResult{
		Target: target + ": incremental snapshot of " + p.Table + " limited to the step's " + p.which(),
		Current: fmt.Sprintf("%d rows of %s match the step's %s on %s (%s, %s); max_rows %d",
			n, p.Table, p.which(), prep.cond.column, kind, prep.cond.col.Type, prep.max),
		Expected: fmt.Sprintf("will re-emit %d rows: %s; connector %s re-reads only the matching rows of %s in chunks of %d and emits each again as a read event (op r) while streaming goes on",
			n, how, p.Connector, p.Table, prep.route.chunk),
		Downstream: []string{
			fmt.Sprintf("%s receives these %d rows again (op r); sinks that do not upsert by key see duplicates", topic, n),
			"after the step, the source and sink counts over the same range are compared until the sink has every row (recovered) or 10 minutes pass",
		},
		Reversible: false,
		Rollback:   "a running incremental snapshot stops on a stop-snapshot signal on the same channel (by a DBA; no registered action). Rows already re-emitted stay in Kafka.",
		Count:      &n,
		Unit:       events.RecoveryUnitRows,
	}
}

// Execute re-runs every check of Validate on the read-only clients (the
// connector config may have moved since the approval, so the route is read
// again), and only then sends the signal through the writer of that route.
// The condition is rebuilt from the params, never stored or taken as text.
func (a *resnapshotRows) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseResnapshot(raw)
	if err != nil {
		return "", err
	}
	pl, scoped, err := resnapScope(ctx, p)
	if err != nil {
		return "", err
	}
	if a.d.Writer == nil && a.d.Kafka == nil {
		return "", fmt.Errorf("pgactions: %s: %w", ResnapshotRowsName, errResnapNoWriter)
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	prep, err := a.prepare(ctx, p, pl, scoped)
	if err != nil {
		return "", fmt.Errorf("pgactions: %s: refused at execute, nothing sent: %w", ResnapshotRowsName, err)
	}
	tables := []string{p.Table}
	if prep.route.via == resnapViaTopic {
		if a.d.Kafka == nil {
			return "", fmt.Errorf("pgactions: %s: connector %s takes signals on Kafka, and there is no Kafka write client (PIPEROOM_WRITE_MODE=on is needed); nothing was sent", ResnapshotRowsName, p.Connector)
		}
		ks, ok := a.d.Kafka.(clients.SnapshotConditionSignaler)
		if !ok {
			return "", fmt.Errorf("pgactions: %s: the Kafka writer cannot send a conditional snapshot signal (no SendSnapshotSignalWhere); nothing was sent", ResnapshotRowsName)
		}
		sig, err := ks.SendSnapshotSignalWhere(ctx, prep.route.topic, p.Connector, prep.route.prefix, tables, prep.cond.filter)
		if err != nil {
			return "", fmt.Errorf("pgactions: %s: signal to %s: %w", ResnapshotRowsName, prep.route.topic, err)
		}
		return fmt.Sprintf("sent execute-snapshot signal %s (INCREMENTAL, rows of %s whose %s is in the step's %s; partition %d offset %d) to connector %s on topic %s (key %s): it re-emits %d rows (op r) as the connector reads the signal",
			sig.ID, p.Table, prep.cond.column, p.which(), sig.Partition, sig.Offset, p.Connector, prep.route.topic, prep.route.prefix, prep.count), nil
	}
	if a.d.Writer == nil {
		return "", fmt.Errorf("pgactions: %s: %w", ResnapshotRowsName, errSnapSigNoWriter)
	}
	cw, ok := a.d.Writer.(clients.SnapshotConditionWriter)
	if !ok {
		return "", fmt.Errorf("pgactions: %s: the source writer cannot write a conditional snapshot signal (no InsertSnapshotSignalWhere); nothing was written", ResnapshotRowsName)
	}
	id, err := cw.InsertSnapshotSignalWhere(ctx, prep.route.signalTable, p.Connector, tables, prep.cond.filter)
	if err != nil {
		return "", fmt.Errorf("pgactions: %s: %w", ResnapshotRowsName, err)
	}
	return fmt.Sprintf("inserted execute-snapshot signal %s (INCREMENTAL, rows of %s whose %s is in the step's %s) into %s for connector %s: it re-emits %d rows (op r) as the connector reads the signal; its log shows the incremental snapshot starting",
		id, p.Table, prep.cond.column, p.which(), prep.route.signalTable, p.Connector, prep.count), nil
}

// resnapSinkTable picks the sink table Verify counts: the step's sink_table,
// else the pipeline's sink table with the source table's name (schema and
// table, then table alone), else its only sink table. None is
// clients.ErrNotSupported: the outcome is unverifiable, not a failure.
func resnapSinkTable(p resnapParams, pl owners.Pipeline, scoped bool) (string, error) {
	if p.SinkTable != "" {
		return p.SinkTable, nil
	}
	if !scoped {
		return "", fmt.Errorf("pgactions: %s: no pipeline to find the sink table of %s in; name sink_table: %w", ResnapshotRowsName, p.Table, clients.ErrNotSupported)
	}
	if slices.Contains(pl.SinkTables, p.Table) {
		return p.Table, nil
	}
	_, name, _ := strings.Cut(p.Table, ".")
	var same []string
	for _, s := range pl.SinkTables {
		if _, n, _ := strings.Cut(s, "."); n == name {
			same = append(same, s)
		}
	}
	switch {
	case len(same) == 1:
		return same[0], nil
	case len(same) == 0 && len(pl.SinkTables) == 1:
		return pl.SinkTables[0], nil
	}
	return "", fmt.Errorf("pgactions: %s: pipeline %q has %d sink tables and none is the one sink named like %s; name sink_table: %w", ResnapshotRowsName, pl.ID, len(pl.SinkTables), p.Table, clients.ErrNotSupported)
}

// unsupportedSink turns a sink that cannot answer (no such table or column)
// into clients.ErrNotSupported: the recovery is unverifiable there, which
// polling again would not change.
func unsupportedSink(err error) error {
	if errors.Is(err, clients.ErrNotFound) && !errors.Is(err, clients.ErrNotSupported) {
		return fmt.Errorf("%w: %w", clients.ErrNotSupported, err)
	}
	return err
}

// Verify counts, on the read-only source and sink, the rows of the step's
// range: Before is the source count (for since on a time column, the rows
// in [at, executedAt), so rows written after the step do not move it) and
// After the part of it not yet at the sink, 0 <= After <= Before. It calls
// only read methods, never a writer, and returns counts and names only. A
// sink without the readers, the sink table or the column answers
// clients.ErrNotSupported (unverifiable).
func (a *resnapshotRows) Verify(ctx context.Context, raw json.RawMessage, executedAt time.Time) (actions.VerifyResult, error) {
	p, err := parseResnapshot(raw)
	if err != nil {
		return actions.VerifyResult{}, err
	}
	pl, scoped, err := resnapScope(ctx, p)
	if err != nil {
		return actions.VerifyResult{}, err
	}
	src, err := a.counter()
	if err != nil {
		return actions.VerifyResult{}, fmt.Errorf("%w: %w", clients.ErrNotSupported, err)
	}
	sink := a.d.Sink
	if sink == nil {
		return actions.VerifyResult{}, fmt.Errorf("pgactions: %s: no sink client to count the rows at the sink: %w", ResnapshotRowsName, clients.ErrNotSupported)
	}
	sinkTable, err := resnapSinkTable(p, pl, scoped)
	if err != nil {
		return actions.VerifyResult{}, err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	cond, err := a.condition(ctx, src, p)
	if err != nil {
		return actions.VerifyResult{}, err
	}
	sc, err := sink.RangeColumn(ctx, sinkTable, cond.column)
	if err != nil {
		return actions.VerifyResult{}, fmt.Errorf("pgactions: %s: sink column %s of %s: %w", ResnapshotRowsName, cond.column, sinkTable, unsupportedSink(err))
	}
	if sc.Kind != cond.col.Kind {
		return actions.VerifyResult{}, fmt.Errorf("pgactions: %s: sink column %s of %s is %s, not comparable with the source's %s: %w", ResnapshotRowsName, cond.column, sinkTable, addPubCut(sc.Type), cond.col.Type, clients.ErrNotSupported)
	}

	var before, atSink int64
	if p.Since != nil && cond.col.Kind == pgconv.RangeKindTime {
		before, atSink, err = a.countWindow(ctx, p.Table, sinkTable, cond, executedAt)
	} else {
		if before, err = src.CountRange(ctx, p.Table, cond.column, cond.from, cond.to); err == nil {
			atSink, err = sink.CountRange(ctx, sinkTable, cond.column, cond.from, cond.to)
			err = unsupportedSink(err)
		}
	}
	if err != nil {
		return actions.VerifyResult{}, fmt.Errorf("pgactions: %s: verify: %w", ResnapshotRowsName, err)
	}
	after := min(max(before-atSink, 0), before)
	detail := fmt.Sprintf("%s.%s range: %d source rows, %d at sink %s", p.Table, cond.column, before, atSink, sinkTable)
	if len(detail) > resnapMaxDetail {
		detail = fmt.Sprintf("%d source rows, %d at the sink", before, atSink)
	}
	return actions.VerifyResult{Before: before, After: after, Unit: events.RecoveryUnitRows, Detail: detail}, nil
}

// countWindow counts both sides over [at, executedAt) with the X1
// Reconciler's CountBetween on each side.
func (a *resnapshotRows) countWindow(ctx context.Context, table, sinkTable string, cond resnapCond, executedAt time.Time) (src, snk int64, err error) {
	sr, ok := a.d.Source.(clients.Reconciler)
	if !ok {
		return 0, 0, fmt.Errorf("the source cannot count a time window (no clients.Reconciler): %w", clients.ErrNotSupported)
	}
	at, err := time.Parse(time.RFC3339Nano, cond.from)
	if err != nil {
		return 0, 0, fmt.Errorf("since.at: %w", tools.ErrBadInput)
	}
	if !executedAt.After(at) {
		return 0, 0, nil
	}
	if src, err = sr.CountBetween(ctx, table, cond.column, at, executedAt); err != nil {
		return 0, 0, err
	}
	snk, err = a.d.Sink.CountBetween(ctx, sinkTable, cond.column, at, executedAt)
	return src, snk, unsupportedSink(err)
}
