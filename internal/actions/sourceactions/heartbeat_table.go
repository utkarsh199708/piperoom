package sourceactions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// CreateHeartbeatTableName is the registered name of the action.
const CreateHeartbeatTableName = "source_create_heartbeat_table"

// heartbeatTableParams is the strict params shape of
// source_create_heartbeat_table: the table to create, not a source table.
type heartbeatTableParams struct {
	Connector string `json:"connector"`
	Table     string `json:"table"`
}

// CreateHeartbeatTableDeps are the clients of source_create_heartbeat_table.
type CreateHeartbeatTableDeps struct {
	// Connect reads the connector config: connector.class, publication.name
	// and publication.autocreate.mode decide the follow-up steps.
	Connect tools.ConfigReader
	// Source is the read-only source client: Validate and Preview read the
	// publication and whether the table already exists through it. It must
	// implement PublicationReader (the real *postgres.DB does).
	Source clients.SourceDB
	// Writer is the admin write contract (Q2b) on SOURCE_DB_WRITE_URL. nil
	// when write mode is off or SOURCE_DB_WRITE_URL is empty: Validate and
	// Execute refuse. Preview never touches it.
	Writer AdminWriter

	// timeout overrides defaultTimeout; tests only.
	timeout time.Duration
}

// NewCreateHeartbeatTable returns source_create_heartbeat_table: CREATE TABLE
// IF NOT EXISTS <schema>.<table> (id integer PRIMARY KEY, ts timestamptz NOT
// NULL DEFAULT now()).
func NewCreateHeartbeatTable(d CreateHeartbeatTableDeps) actions.Action {
	return &heartbeatTable{d: d}
}

type heartbeatTable struct{ d CreateHeartbeatTableDeps }

var (
	_ actions.Action  = (*heartbeatTable)(nil)
	_ actions.Scoped  = (*heartbeatTable)(nil)
	_ actions.Preview = (*heartbeatTable)(nil)
)

func (a *heartbeatTable) Name() string  { return CreateHeartbeatTableName }
func (a *heartbeatTable) Layer() string { return sourceLayer }

func (a *heartbeatTable) Describe() string {
	return "Creates Debezium's heartbeat table (id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now()) in a schema that holds one of the pipeline's source_tables, if it does not exist. " +
		"Debezium's heartbeat.action.query inserts one row per heartbeat, which advances the replication slot's confirmed position on an idle database. " +
		"Propose the follow-up steps the preview lists in the same proposal: connect_update_connector_config setting heartbeat.action.query (N keys), and pg_add_publication_table when the publication.autocreate.mode is filtered. " +
		"Validate requires write credentials; the table must not be a captured source table."
}

func (a *heartbeatTable) ParamsSchema() map[string]any {
	ident := `^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "The pipeline's Debezium Postgres source connector."},
			"table":     map[string]any{"type": "string", "description": "The heartbeat table to create, \"schema.table\"; its schema must hold one of the pipeline's source_tables, and it must not be a captured table.", "pattern": ident + `\.` + ident},
		},
		"additionalProperties": false,
		"required":             []string{"connector", "table"},
	}
}

// parseHeartbeatTable checks every param before any I/O.
func parseHeartbeatTable(raw json.RawMessage) (heartbeatTableParams, error) {
	var p heartbeatTableParams
	if err := decodeParams(CreateHeartbeatTableName, raw, &p); err != nil {
		return p, err
	}
	if err := connectorName(CreateHeartbeatTableName, p.Connector); err != nil {
		return p, err
	}
	if _, _, err := splitTable(p.Table); err != nil {
		return p, fmt.Errorf("sourceactions: %s: %w", CreateHeartbeatTableName, err)
	}
	return p, nil
}

func (a *heartbeatTable) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, actionTimeout(a.d.timeout))
}

// CheckScope refuses, without I/O, a connector outside p, a table that is one
// of p's source tables, and a schema that holds none of them (the writer's
// fail-closed rule for a table it cannot know yet).
func (a *heartbeatTable) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseHeartbeatTable(raw)
	if err != nil {
		return err
	}
	return a.checkPipeline(pl, p)
}

func (a *heartbeatTable) checkPipeline(pl owners.Pipeline, p heartbeatTableParams) error {
	if err := cdcScope(pl, p.Connector); err != nil {
		return err
	}
	for _, t := range pl.SourceTables {
		if t == p.Table {
			return fmt.Errorf("%w: table %s is one of pipeline %q's source_tables; a heartbeat table must never be a captured table", tools.ErrOutOfScope, p.Table, pl.ID)
		}
	}
	schema, _, err := splitTable(p.Table)
	if err != nil {
		return err
	}
	if !holdsSourceTable(pl, schema) {
		return fmt.Errorf("%w: schema %q holds none of pipeline %q's source_tables", tools.ErrOutOfScope, schema, pl.ID)
	}
	return nil
}

// read reads the connector config and, when it exists, the publication and
// the heartbeat table. known is false when the publication could not be read
// (Debezium has not created it yet, or autocreate is disabled and a DBA owns
// it); that is not an error here.
func (a *heartbeatTable) read(ctx context.Context, p heartbeatTableParams) (cfg map[string]string, pub string, st postgres.PublicationAddState, known bool, err error) {
	cfg, pub, err = readConnectorConfig(ctx, CreateHeartbeatTableName, a.d.Connect, p.Connector)
	if err != nil {
		return nil, "", postgres.PublicationAddState{}, false, err
	}
	st, err = publicationState(ctx, CreateHeartbeatTableName, a.d.Source, pub, p.Table)
	if errors.Is(err, clients.ErrNotFound) {
		return cfg, pub, postgres.PublicationAddState{}, false, nil
	}
	if err != nil {
		return nil, "", postgres.PublicationAddState{}, false, err
	}
	return cfg, pub, st, true, nil
}

// checkExistingType refuses an existing relation of another kind: CREATE TABLE
// IF NOT EXISTS would skip it and the heartbeat query would then fail.
func checkExistingType(p heartbeatTableParams, st postgres.PublicationAddState) error {
	if st.TableExists && st.RelKind != "r" {
		return fmt.Errorf("sourceactions: %s: %s exists but is not an ordinary table (relkind %q); CREATE TABLE IF NOT EXISTS would skip it and the heartbeat query would fail", CreateHeartbeatTableName, p.Table, cut(st.RelKind))
	}
	return nil
}

// Validate checks the params, the write credentials, the pipeline scope in ctx
// (when present), the connector config and the existing relation type.
func (a *heartbeatTable) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseHeartbeatTable(raw)
	if err != nil {
		return err
	}
	if err := checkWriteCredentials(CreateHeartbeatTableName, a.d.Writer); err != nil {
		return err
	}
	if err := validateScope(ctx, func(pl owners.Pipeline) error { return a.checkPipeline(pl, p) }); err != nil {
		return err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	_, _, st, _, err := a.read(ctx, p)
	if err != nil {
		return err
	}
	return checkExistingType(p, st)
}

// Preview reads the config and the publication and renders the table, the
// cost and the follow-up steps. It never touches the writer.
func (a *heartbeatTable) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseHeartbeatTable(raw)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("sourceactions: %s: preview: %w", CreateHeartbeatTableName, err)
	}
	if err := ctxScope(ctx, CreateHeartbeatTableName, func(pl owners.Pipeline) error { return a.checkPipeline(pl, p) }); err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	cfg, pub, st, known, err := a.read(ctx, p)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("sourceactions: %s: preview: %w", CreateHeartbeatTableName, err)
	}
	return renderHeartbeatPreview(cfg, pub, p, st, known), nil
}

// renderHeartbeatPreview builds the preview from what was read.
func renderHeartbeatPreview(cfg map[string]string, pub string, p heartbeatTableParams, st postgres.PublicationAddState, known bool) actions.PreviewResult {
	res := actions.PreviewResult{
		Target:  fmt.Sprintf("heartbeat table %s (connector %s) on the source Postgres", p.Table, p.Connector),
		Current: heartbeatCurrentText(pub, p, st, known),
	}
	if err := checkExistingType(p, st); err != nil {
		res.Expected = "no change: this step is refused (" + err.Error() + ")"
		return res
	}
	res.Expected = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now()): a one-row table, a catalog change only, with no lock on the captured tables. "+
		"Debezium runs heartbeat.action.query on every heartbeat and the inserted row advances the replication slot's confirmed position on an idle database; the table itself is tiny.", quoteTable(p.Table))
	res.Reversible = true
	res.Rollback = fmt.Sprintf("DROP TABLE %s (by a DBA; no registered action), and unset heartbeat.action.query (the N-key connect_update_connector_config sets keys, it cannot unset one).", quoteTable(p.Table))
	if st.TableExists {
		res.Current += "; it already exists (CREATE TABLE IF NOT EXISTS is a no-op)"
	}
	mode := strings.TrimSpace(cfg["publication.autocreate.mode"])
	if mode == "" {
		mode = "all_tables (the default)"
	}
	res.Downstream = []string{
		fmt.Sprintf("publication %s: %s", pub, publicationShape(st, known)),
		fmt.Sprintf("publication.autocreate.mode is %s", mode),
	}
	if q := strings.TrimSpace(cfg["heartbeat.action.query"]); q != "" {
		res.Downstream = append(res.Downstream, "the connector already sets heartbeat.action.query: the follow-up step replaces it; read the current query before approving.")
	}
	if strings.TrimSpace(cfg["heartbeat.interval.ms"]) == "" || strings.TrimSpace(cfg["heartbeat.interval.ms"]) == "0" {
		res.Downstream = append(res.Downstream, "heartbeat.interval.ms is unset or 0 in the connector config: Debezium sends no heartbeats until it is set, so the query above would never run.")
	}
	res.Downstream = append(res.Downstream, "include these follow-up steps in the same proposal, after this one (the publication step validates once this table exists):")
	for _, fu := range HeartbeatFollowUps(cfg, p.Connector, pub, p.Table) {
		res.Downstream = append(res.Downstream, fmt.Sprintf("step %s (layer %s) %s — %s", fu.Action, fu.Layer, string(fu.Params), fu.Why))
	}
	return res
}

// heartbeatCurrentText describes the table and the publication.
func heartbeatCurrentText(pub string, p heartbeatTableParams, st postgres.PublicationAddState, known bool) string {
	state := "existence not checked"
	if known {
		state = "does not exist"
		if st.TableExists {
			state = "exists"
		}
	}
	return fmt.Sprintf("%s %s; publication %s: %s", p.Table, state, pub, publicationShape(st, known))
}

// publicationShape phrases what was read of the publication. known is false
// when it could not be read at all.
func publicationShape(st postgres.PublicationAddState, known bool) string {
	if !known {
		return "not readable (the connector may not have created it yet; with publication.autocreate.mode disabled a DBA creates it)"
	}
	if st.AllTables {
		return "FOR ALL TABLES (any new table is published automatically)"
	}
	return fmt.Sprintf("has %d tables", len(st.Members)+st.OtherMembers)
}

// Execute creates the table through the admin writer. It re-checks the scope
// in ctx and refuses without a writer before any I/O; the writer re-checks the
// scope, quotes the identifiers and is idempotent (CREATE TABLE IF NOT
// EXISTS).
func (a *heartbeatTable) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseHeartbeatTable(raw)
	if err != nil {
		return "", err
	}
	if err := ctxScope(ctx, CreateHeartbeatTableName, func(pl owners.Pipeline) error { return a.checkPipeline(pl, p) }); err != nil {
		return "", err
	}
	if err := checkWriteCredentials(CreateHeartbeatTableName, a.d.Writer); err != nil {
		return "", err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()

	// Best effort: read the config for the follow-up list in the result. A
	// read failure never stops the approved write.
	cfg, pub, _, _, rerr := a.read(ctx, p)
	if err := a.d.Writer.CreateHeartbeatTable(ctx, p.Table); err != nil {
		return "", fmt.Errorf("sourceactions: %s: %w", CreateHeartbeatTableName, err)
	}
	follow := "connect_update_connector_config with heartbeat.action.query"
	if rerr == nil {
		names := make([]string, 0, 2)
		for _, fu := range HeartbeatFollowUps(cfg, p.Connector, pub, p.Table) {
			names = append(names, fu.Action)
		}
		if len(names) > 0 {
			follow = strings.Join(names, ", ")
		}
	}
	return fmt.Sprintf("created heartbeat table %s if it did not exist (id integer PRIMARY KEY, ts timestamptz); one row per heartbeat advances the slot on an idle database. Follow-up steps in the same proposal: %s. Rollback: DROP TABLE %s; unset heartbeat.action.query.",
		p.Table, follow, quoteTable(p.Table)), nil
}
