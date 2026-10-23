package sourceactions

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// SetReplicaIdentityName is the registered name of the action.
const SetReplicaIdentityName = "pg_set_replica_identity"

// setReplicaIdentityParams is the strict params shape of
// pg_set_replica_identity.
type setReplicaIdentityParams struct {
	Connector string `json:"connector"`
	Table     string `json:"table"`
	Mode      string `json:"mode"`
	Index     string `json:"index,omitempty"`
}

// SetReplicaIdentityDeps are the clients of pg_set_replica_identity.
type SetReplicaIdentityDeps struct {
	// Connect reads the connector config (publication.name, connector.class).
	// The read-only client is enough.
	Connect tools.ConfigReader
	// Source is the read-only source client: Validate and Preview read the
	// table, the table's size and the row estimate through it. It must
	// implement PublicationReader and TableSizeReader (the real *postgres.DB
	// does).
	Source clients.SourceDB
	// Writer is the admin write contract (Q2b) on SOURCE_DB_WRITE_URL. It is
	// nil when write mode is off or SOURCE_DB_WRITE_URL is empty: Validate
	// refuses, and Execute refuses, so the action cannot run without it.
	// Preview never touches it.
	Writer AdminWriter

	// timeout overrides defaultTimeout; tests only.
	timeout time.Duration
}

// NewSetReplicaIdentity returns pg_set_replica_identity: ALTER TABLE ...
// REPLICA IDENTITY FULL | DEFAULT | USING INDEX <index>.
func NewSetReplicaIdentity(d SetReplicaIdentityDeps) actions.Action {
	return &setReplicaIdentity{d: d}
}

type setReplicaIdentity struct{ d SetReplicaIdentityDeps }

var (
	_ actions.Action  = (*setReplicaIdentity)(nil)
	_ actions.Scoped  = (*setReplicaIdentity)(nil)
	_ actions.Preview = (*setReplicaIdentity)(nil)
)

func (a *setReplicaIdentity) Name() string  { return SetReplicaIdentityName }
func (a *setReplicaIdentity) Layer() string { return sourceLayer }

func (a *setReplicaIdentity) Describe() string {
	return "Sets ALTER TABLE <table> REPLICA IDENTITY FULL, DEFAULT or USING INDEX <index> on one of the pipeline's source_tables. " +
		"FULL makes every UPDATE and DELETE replicate the whole old row (needed for a table without a primary key) and adds WAL; DEFAULT goes back to the key columns. " +
		"It takes one brief ACCESS EXCLUSIVE lock, does not rewrite the table and needs no re-snapshot. " +
		"Validate requires write credentials and the table to be a source table of the room's pipeline."
}

func (a *setReplicaIdentity) ParamsSchema() map[string]any {
	ident := `^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "The pipeline's Debezium Postgres source connector."},
			"table":     map[string]any{"type": "string", "description": "A source table of the pipeline, \"schema.table\".", "pattern": ident + `\.` + ident},
			"mode":      map[string]any{"type": "string", "enum": []string{"full", "default", "index"}, "description": "full writes the whole old row to WAL on UPDATE/DELETE; default goes back to the key; index uses the named unique index's columns as the key."},
			"index":     map[string]any{"type": "string", "description": "The unique index name; required with mode index and refused with full or default.", "pattern": ident},
		},
		"additionalProperties": false,
		"required":             []string{"connector", "table", "mode"},
	}
}

// parse checks every param before any I/O.
func parseSetReplicaIdentity(raw json.RawMessage) (setReplicaIdentityParams, error) {
	var p setReplicaIdentityParams
	if err := decodeParams(SetReplicaIdentityName, raw, &p); err != nil {
		return p, err
	}
	if err := connectorName(SetReplicaIdentityName, p.Connector); err != nil {
		return p, err
	}
	if _, _, err := splitTable(p.Table); err != nil {
		return p, fmt.Errorf("sourceactions: %s: %w", SetReplicaIdentityName, err)
	}
	switch p.Mode {
	case "full", "default":
		if p.Index != "" {
			return p, fmt.Errorf("sourceactions: %s: mode %q takes no index", SetReplicaIdentityName, p.Mode)
		}
	case "index":
		if !identRe.MatchString(p.Index) {
			return p, fmt.Errorf("sourceactions: %s: mode index needs a valid index name (letters, digits and _, not starting with a digit, at most 63), got %q", SetReplicaIdentityName, cut(p.Index))
		}
	default:
		return p, fmt.Errorf("sourceactions: %s: unknown mode %q (full, default or index)", SetReplicaIdentityName, cut(p.Mode))
	}
	return p, nil
}

func (a *setReplicaIdentity) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, actionTimeout(a.d.timeout))
}

// CheckScope refuses, without I/O, a connector or table outside p.
func (a *setReplicaIdentity) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseSetReplicaIdentity(raw)
	if err != nil {
		return err
	}
	return a.checkPipeline(pl, p)
}

func (a *setReplicaIdentity) checkPipeline(pl owners.Pipeline, p setReplicaIdentityParams) error {
	if err := cdcScope(pl, p.Connector); err != nil {
		return err
	}
	for _, t := range pl.SourceTables {
		if t == p.Table {
			return nil
		}
	}
	return fmt.Errorf("%w: table %s is not one of pipeline %q's source_tables", tools.ErrOutOfScope, p.Table, pl.ID)
}

// read reads the connector config, the publication and the table's state.
func (a *setReplicaIdentity) read(ctx context.Context, p setReplicaIdentityParams) (map[string]string, postgres.PublicationAddState, error) {
	cfg, pub, err := readConnectorConfig(ctx, SetReplicaIdentityName, a.d.Connect, p.Connector)
	if err != nil {
		return nil, postgres.PublicationAddState{}, err
	}
	st, err := publicationState(ctx, SetReplicaIdentityName, a.d.Source, pub, p.Table)
	if err != nil {
		return nil, postgres.PublicationAddState{}, err
	}
	return cfg, st, nil
}

// Validate checks the params, the write credentials, the pipeline scope in ctx
// (when present) and the table, all before Preview. It refuses a table that
// does not exist or is not a table, and DEFAULT on a table without a primary
// key (which would leave no replica identity at all).
func (a *setReplicaIdentity) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseSetReplicaIdentity(raw)
	if err != nil {
		return err
	}
	if err := checkWriteCredentials(SetReplicaIdentityName, a.d.Writer); err != nil {
		return err
	}
	if err := validateScope(ctx, func(pl owners.Pipeline) error { return a.checkPipeline(pl, p) }); err != nil {
		return err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	_, st, err := a.read(ctx, p)
	if err != nil {
		return err
	}
	return checkIdentityState(p, st)
}

// checkIdentityState is the table rule shared by Validate and Preview.
func checkIdentityState(p setReplicaIdentityParams, st postgres.PublicationAddState) error {
	if !st.TableExists {
		return fmt.Errorf("sourceactions: %s: table %s does not exist: %w", SetReplicaIdentityName, p.Table, clients.ErrNotFound)
	}
	switch st.RelKind {
	case "r":
	case "p":
	default:
		return fmt.Errorf("sourceactions: %s: %s is not an ordinary or partitioned table (relkind %q)", SetReplicaIdentityName, p.Table, cut(st.RelKind))
	}
	if p.Mode == "default" && !st.HasPrimaryKey {
		return fmt.Errorf("sourceactions: %s: %s has no primary key, so REPLICA IDENTITY DEFAULT leaves no identity at all and every UPDATE and DELETE on it would fail once published; use FULL or add a primary key first", SetReplicaIdentityName, p.Table)
	}
	return nil
}

// Preview reads the table and its size on the read-only clients and renders
// the lock, the WAL cost and the rollback. It never touches the writer.
func (a *setReplicaIdentity) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseSetReplicaIdentity(raw)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("sourceactions: %s: preview: %w", SetReplicaIdentityName, err)
	}
	if err := ctxScope(ctx, SetReplicaIdentityName, func(pl owners.Pipeline) error { return a.checkPipeline(pl, p) }); err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	_, st, err := a.read(ctx, p)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("sourceactions: %s: preview: %w", SetReplicaIdentityName, err)
	}
	ts, sizeErr := tableSize(ctx, SetReplicaIdentityName, a.d.Source, p.Table)
	return renderReplicaIdentityPreview(p, st, ts, sizeErr), nil
}

// renderReplicaIdentityPreview builds the preview from what was read.
func renderReplicaIdentityPreview(p setReplicaIdentityParams, st postgres.PublicationAddState, ts postgres.TableSize, sizeErr error) actions.PreviewResult {
	res := actions.PreviewResult{
		Target:  fmt.Sprintf("table %s (connector %s) on the source Postgres", p.Table, p.Connector),
		Current: currentIdentityText(p, st, ts, sizeErr),
	}
	if err := checkIdentityState(p, st); err != nil {
		res.Expected = "no change: this step is refused (" + err.Error() + ")"
		res.Reversible, res.Rollback = false, ""
		return res
	}
	res.Expected = expectedIdentityText(p, st, ts, sizeErr)
	res.Downstream = identityDownstream(p, st)
	res.Reversible = true
	res.Rollback = rollbackIdentity(p, st)
	return res
}

// identityWords spells pg_class.relreplident out.
var identityWords = map[string]string{"d": "DEFAULT (the primary key)", "f": "FULL (every column)", "i": "USING INDEX", "n": "NOTHING"}

// identityWord spells one relreplident code, including an unknown one.
func identityWord(code string) string {
	if w := identityWords[code]; w != "" {
		return w
	}
	return "unknown (relreplident " + cut(code) + ")"
}

func currentIdentityText(p setReplicaIdentityParams, st postgres.PublicationAddState, ts postgres.TableSize, sizeErr error) string {
	kind := "table"
	if st.RelKind == "p" {
		kind = "partitioned table"
	}
	key := "no primary key"
	if st.HasPrimaryKey {
		key = "primary key"
	}
	rows := "row count unknown (pg_class.reltuples is -1: never analysed)"
	if st.EstimatedRows >= 0 {
		rows = fmt.Sprintf("about %d rows (pg_class.reltuples)", st.EstimatedRows)
	}
	size := "size unknown"
	if sizeErr == nil {
		size = topology.HumanBytes(ts.TotalBytes) + " on disk (pg_total_relation_size)"
	}
	pub := "not in the publication"
	if st.InPublication {
		pub = "in the publication"
	}
	return fmt.Sprintf("%s %s: %s, %s, %s, REPLICA IDENTITY %s",
		p.Table, kind, key, rows, size, identityWord(st.ReplicaIdentity)) + ", " + pub
}

func expectedIdentityText(p setReplicaIdentityParams, st postgres.PublicationAddState, ts postgres.TableSize, sizeErr error) string {
	if identityAlreadySet(p, st) {
		return fmt.Sprintf("no change: REPLICA IDENTITY is already %s", identityWord(st.ReplicaIdentity))
	}
	stmt := identityStatement(p)
	switch p.Mode {
	case "full":
		return fmt.Sprintf("%s: the catalog update takes one brief ACCESS EXCLUSIVE lock (the table is not rewritten and its rows are not scanned). "+
			"From then on every UPDATE and DELETE writes the whole old row to WAL instead of only the key: %s.", stmt, walEstimate(ts, st.EstimatedRows, sizeErr))
	case "default":
		return fmt.Sprintf("%s: the catalog update takes one brief ACCESS EXCLUSIVE lock (no rewrite, no scan). "+
			"From then on UPDATE and DELETE log only the key columns again, saving %s for rows that are changed.", stmt, walEstimate(ts, st.EstimatedRows, sizeErr))
	default: // index
		return fmt.Sprintf("%s: the catalog update takes one brief ACCESS EXCLUSIVE lock (no rewrite, no scan). "+
			"From then on UPDATE and DELETE log the index's columns; Postgres refuses a non-unique, partial or non-immediate index, or one with a nullable column.", stmt)
	}
}

// walEstimate phrases the extra (or saved) WAL from the table's size.
func walEstimate(ts postgres.TableSize, rows int64, sizeErr error) string {
	if sizeErr != nil {
		return "the extra WAL cannot be estimated: " + sizeErr.Error()
	}
	if ts.TableBytes <= 0 {
		return "no extra WAL from the table's current contents (pg_table_size is 0: it is empty)"
	}
	if rows > 0 {
		return fmt.Sprintf("about %s of WAL per full pass over every row (%s across about %d rows, ~%s per row)",
			topology.HumanBytes(ts.TableBytes), topology.HumanBytes(ts.TableBytes), rows, topology.HumanBytes(ts.TableBytes/rows))
	}
	return fmt.Sprintf("about %s of WAL per full pass over every row (the row estimate is unknown: pg_class.reltuples has never been analysed)", topology.HumanBytes(ts.TableBytes))
}

// identityAlreadySet reports whether the table already has the target mode.
// pg_class does not name the index of mode "i", so an index target is never
// reported as already set.
func identityAlreadySet(p setReplicaIdentityParams, st postgres.PublicationAddState) bool {
	switch p.Mode {
	case "full":
		return st.ReplicaIdentity == "f"
	case "default":
		return st.ReplicaIdentity == "d"
	}
	return false
}

// identityStatement renders the ALTER for text only; the writer builds and
// runs the real statement with the same quoting.
func identityStatement(p setReplicaIdentityParams) string {
	table := quoteTable(p.Table)
	switch p.Mode {
	case "full":
		return "ALTER TABLE " + table + " REPLICA IDENTITY FULL"
	case "default":
		return "ALTER TABLE " + table + " REPLICA IDENTITY DEFAULT"
	default:
		return "ALTER TABLE " + table + " REPLICA IDENTITY USING INDEX " + pgx.Identifier{p.Index}.Sanitize()
	}
}

// rollbackIdentity is the text the approval card offers: the previous mode
// when it is known, or what to look up when it is an index whose name
// pg_class does not carry.
func rollbackIdentity(p setReplicaIdentityParams, st postgres.PublicationAddState) string {
	table := quoteTable(p.Table)
	if identityAlreadySet(p, st) {
		return fmt.Sprintf("none needed: REPLICA IDENTITY is already %s", identityWord(st.ReplicaIdentity))
	}
	switch st.ReplicaIdentity {
	case "f":
		return "ALTER TABLE " + table + " REPLICA IDENTITY FULL"
	case "d":
		return "ALTER TABLE " + table + " REPLICA IDENTITY DEFAULT"
	case "n":
		return "ALTER TABLE " + table + " REPLICA IDENTITY NOTHING"
	case "i":
		return "ALTER TABLE " + table + " REPLICA IDENTITY USING INDEX <the index it had before this step, read from pg_index> (the catalog does not name it here)"
	}
	return "ALTER TABLE " + table + " REPLICA IDENTITY DEFAULT (the previous identity could not be read)"
}

// identityDownstream says what the change does to the connector and the rows
// already sent.
func identityDownstream(p setReplicaIdentityParams, st postgres.PublicationAddState) []string {
	out := []string{
		"Debezium reads the new replica identity on the next change: no re-snapshot, no reconnect, and rows already in Kafka keep their old before image.",
	}
	if p.Mode == "full" && !st.HasPrimaryKey {
		out = append(out, "FULL is what lets Debezium identify rows of a table without a primary key; without it UPDATE and DELETE cannot be replicated.")
	}
	if p.Mode == "full" && st.HasPrimaryKey {
		out = append(out, "The primary key already identified rows, so FULL only adds WAL and larger 'before' images; prefer DEFAULT unless the key is not enough (for example a key whose columns change).")
	}
	if !st.InPublication {
		out = append(out, "The table is not in the connector's publication, so this changes nothing downstream until pg_add_publication_table adds it.")
	}
	return out
}

// Execute sets the identity through the admin writer. It re-checks the scope
// in ctx and refuses without a writer before any I/O; the writer re-checks the
// scope and quotes the identifiers itself.
func (a *setReplicaIdentity) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseSetReplicaIdentity(raw)
	if err != nil {
		return "", err
	}
	if err := ctxScope(ctx, SetReplicaIdentityName, func(pl owners.Pipeline) error { return a.checkPipeline(pl, p) }); err != nil {
		return "", err
	}
	if err := checkWriteCredentials(SetReplicaIdentityName, a.d.Writer); err != nil {
		return "", err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()

	// Best effort: read the state the preview showed, for the result's
	// rollback text. A read failure never stops the approved write.
	_, st, rerr := a.read(ctx, p)
	if err := a.d.Writer.SetReplicaIdentity(ctx, p.Table, p.Mode, p.Index); err != nil {
		return "", fmt.Errorf("sourceactions: %s: %w", SetReplicaIdentityName, err)
	}
	rollback := "ALTER TABLE " + quoteTable(p.Table) + " REPLICA IDENTITY DEFAULT (the previous identity could not be read)"
	if rerr == nil {
		rollback = rollbackIdentity(p, st)
	}
	return fmt.Sprintf("set REPLICA IDENTITY %s on %s (one brief ACCESS EXCLUSIVE lock, no table rewrite, no re-snapshot). Rollback: %s.",
		modeWords(p), p.Table, rollback), nil
}

// modeWords renders the target mode for a result.
func modeWords(p setReplicaIdentityParams) string {
	if p.Mode == "index" {
		return "USING INDEX " + p.Index
	}
	return strings.ToUpper(p.Mode)
}
