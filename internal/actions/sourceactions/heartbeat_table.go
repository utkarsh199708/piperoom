package sourceactions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

// HeartbeatWriter is the admin write of source_create_heartbeat_table
// (postgres.SourceAdminWriter.CreateHeartbeatTable, Q2b).
type HeartbeatWriter interface {
	CreateHeartbeatTable(ctx context.Context, table string) error
}

var _ HeartbeatWriter = (*postgres.Writer)(nil)

// PublicationStateReader reads a publication and one table
// (*postgres.DB.PublicationAddState, the read of pg_add_publication_table).
// Optional on the source client: without it the preview says the publication
// was not read.
type PublicationStateReader interface {
	PublicationAddState(ctx context.Context, publication, table string, visible []string) (postgres.PublicationAddState, error)
}

var _ PublicationStateReader = (*postgres.DB)(nil)

// defaultPublication is Debezium's publication when publication.name is
// unset.
const defaultPublication = "dbz_publication"

// defaultHeartbeatIntervalMS is the heartbeat.interval.ms the preview
// suggests when the connector sends no heartbeats (Debezium's default is 0,
// off).
const defaultHeartbeatIntervalMS = "10000"

// HeartbeatDeps are the clients of source_create_heartbeat_table.
type HeartbeatDeps struct {
	// Connect reads the connector config (connector.class, publication.name,
	// heartbeat settings). The read-only Connect client is enough.
	Connect tools.ConfigReader
	// Source is the read-only source client; PublicationStateReader is
	// optional and only informs the preview's follow-up steps.
	Source clients.SourceDB
	// Writer is used by Execute only; nil (untyped) when write mode is off
	// or SOURCE_DB_WRITE_URL is empty, and then Validate refuses.
	Writer HeartbeatWriter

	// timeout overrides opTimeout; tests only.
	timeout time.Duration
}

// NewCreateHeartbeatTable returns source_create_heartbeat_table: CREATE TABLE
// IF NOT EXISTS <schema.table> (id integer PRIMARY KEY, ts timestamptz NOT
// NULL DEFAULT now()), the table Debezium's heartbeat.action.query writes to
// so a connector on a quiet database can confirm a newer LSN and its slot
// stops holding WAL.
func NewCreateHeartbeatTable(d HeartbeatDeps) actions.Action {
	return &createHeartbeatTable{d: d}
}

type createHeartbeatTable struct{ d HeartbeatDeps }

var (
	_ actions.Scoped  = (*createHeartbeatTable)(nil)
	_ actions.Preview = (*createHeartbeatTable)(nil)
)

func (a *createHeartbeatTable) Name() string  { return CreateHeartbeatTableName }
func (a *createHeartbeatTable) Layer() string { return layerSource }

func (a *createHeartbeatTable) Describe() string {
	return "Create the Debezium heartbeat table on the source database (CREATE TABLE IF NOT EXISTS <schema.table> (id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now())). " +
		"Use it when a replication slot keeps WAL because the captured tables are quiet: with heartbeat.interval.ms and heartbeat.action.query writing to this table, the connector confirms a newer LSN. " +
		"The table must be in a schema that holds one of the pipeline's source_tables and must not be a source table. The preview lists the follow-up steps: adding the table to a filtered publication (pg_add_publication_table) and setting heartbeat.action.query on the connector."
}

func (a *createHeartbeatTable) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "The pipeline's Debezium source connector that will write heartbeats."},
			"table":     map[string]any{"type": "string", "description": "The heartbeat table to create, \"schema.table\" (e.g. public.debezium_heartbeat), in a schema of the pipeline's source_tables.", "pattern": tablePattern},
		},
		"additionalProperties": false,
		"required":             []string{"connector", "table"},
	}
}

type heartbeatParams struct {
	Connector string `json:"connector"`
	Table     string `json:"table"`
}

func parseHeartbeat(raw json.RawMessage) (heartbeatParams, error) {
	var p heartbeatParams
	if err := decodeStrict(CreateHeartbeatTableName, raw, &p); err != nil {
		return p, err
	}
	if err := checkConnectorName(CreateHeartbeatTableName, p.Connector); err != nil {
		return p, err
	}
	if err := checkTable(CreateHeartbeatTableName, p.Table); err != nil {
		return p, err
	}
	return p, nil
}

// CheckScope refuses, without I/O, a connector outside pl, a table that is
// one of pl's source tables, and a schema that holds none of them (the
// writer's own rule, checked here first).
func (a *createHeartbeatTable) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseHeartbeat(raw)
	if err != nil {
		return err
	}
	return heartbeatInScope(pl, p)
}

func heartbeatInScope(pl owners.Pipeline, p heartbeatParams) error {
	if err := pipelineScope(CreateHeartbeatTableName, pl, p.Connector); err != nil {
		return err
	}
	if slices.Contains(pl.SourceTables, p.Table) {
		return fmt.Errorf("sourceactions: %s: %s is one of pipeline %q's source_tables, not a heartbeat table", CreateHeartbeatTableName, p.Table, pl.ID)
	}
	schema, _, _ := strings.Cut(p.Table, ".")
	for _, t := range pl.SourceTables {
		if s, _, ok := strings.Cut(t, "."); ok && s == schema {
			return nil
		}
	}
	return fmt.Errorf("sourceactions: %s: %w: schema %q holds none of pipeline %q's source_tables", CreateHeartbeatTableName, tools.ErrOutOfScope, schema, pl.ID)
}

// connectorConfig reads the connector's config and refuses one that is not a
// Debezium source connector.
func (a *createHeartbeatTable) connectorConfig(ctx context.Context, connector string) (map[string]string, error) {
	if a.d.Connect == nil {
		return nil, fmt.Errorf("sourceactions: %s: no Connect client to read connector %q", CreateHeartbeatTableName, connector)
	}
	cfg, err := a.d.Connect.Config(ctx, connector)
	if err != nil {
		return nil, fmt.Errorf("sourceactions: %s: read connector %q config: %w", CreateHeartbeatTableName, connector, err)
	}
	if !isDebeziumSource(cfg["connector.class"]) {
		return nil, fmt.Errorf("sourceactions: %s: connector %q is not a Debezium source connector (connector.class %q)", CreateHeartbeatTableName, connector, cut(cfg["connector.class"]))
	}
	return cfg, nil
}

// Validate refuses bad params, a target outside the room's pipeline (when
// ctx carries it), a missing writer and a connector that is not a Debezium
// source.
func (a *createHeartbeatTable) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseHeartbeat(raw)
	if err != nil {
		return err
	}
	if err := validateScope(ctx, func(pl owners.Pipeline) error { return heartbeatInScope(pl, p) }); err != nil {
		return err
	}
	if a.d.Writer == nil {
		return fmt.Errorf("sourceactions: %s: %w", CreateHeartbeatTableName, errNoWriter)
	}
	ctx, cancel := withTimeout(ctx, a.d.timeout)
	defer cancel()
	_, err = a.connectorConfig(ctx, p.Connector)
	return err
}

// pubState is what the preview learnt about the publication; read is false
// when it was not read (note says why).
type pubState struct {
	name string
	read bool
	note string
	st   postgres.PublicationAddState
}

// readPublication reads the connector's publication and the table on the
// read-only client. Only a Postgres connector has one.
func (a *createHeartbeatTable) readPublication(ctx context.Context, cfg map[string]string, table string) pubState {
	ps := pubState{name: strings.TrimSpace(cfg["publication.name"])}
	if ps.name == "" {
		ps.name = defaultPublication
	}
	r, ok := a.d.Source.(PublicationStateReader)
	if !ok {
		ps.note = "the source client cannot read publications"
		return ps
	}
	st, err := r.PublicationAddState(ctx, ps.name, table, []string{table})
	if errors.Is(err, clients.ErrNotFound) {
		ps.note = fmt.Sprintf("publication %s does not exist yet (Debezium creates it on start unless publication.autocreate.mode is disabled)", ps.name)
		return ps
	}
	if err != nil {
		ps.note = "publication not read (" + errText(err) + ")"
		return ps
	}
	ps.read, ps.st = true, st
	return ps
}

// Preview reads the connector config and, for a Postgres connector, its
// publication and the table, on read-only clients. It never holds the writer.
func (a *createHeartbeatTable) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseHeartbeat(raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	if _, _, err := ctxScope(ctx, CreateHeartbeatTableName, func(pl owners.Pipeline) error { return heartbeatInScope(pl, p) }); err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := withTimeout(ctx, a.d.timeout)
	defer cancel()
	cfg, err := a.connectorConfig(ctx, p.Connector)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	postgresSource := strings.Contains(cfg["connector.class"], "PostgresConnector")
	var ps pubState
	if postgresSource {
		ps = a.readPublication(ctx, cfg, p.Table)
	}
	return heartbeatPreview(p, cfg, postgresSource, ps), nil
}

// heartbeatQuery is the heartbeat.action.query for table: one row, updated
// in place, so the table never grows.
func heartbeatQuery(table string) string {
	return fmt.Sprintf("INSERT INTO %s (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts", quoteTable(table))
}

// heartbeatPreview renders the preview and its follow-up steps.
func heartbeatPreview(p heartbeatParams, cfg map[string]string, postgresSource bool, ps pubState) actions.PreviewResult {
	res := actions.PreviewResult{
		Target:     fmt.Sprintf("heartbeat table %s on the source database (connector %s)", p.Table, p.Connector),
		Reversible: true,
		Rollback:   fmt.Sprintf("DROP TABLE %s (by a DBA; no registered action), after removing heartbeat.action.query from connector %s", quoteTable(p.Table), p.Connector),
	}
	var cur []string
	exists := ps.read && ps.st.TableExists
	switch {
	case exists:
		cur = append(cur, p.Table+" already exists")
	case ps.read:
		cur = append(cur, p.Table+" does not exist")
	}
	interval := strings.TrimSpace(cfg["heartbeat.interval.ms"])
	if interval == "" || interval == "0" {
		cur = append(cur, "the connector sends no heartbeats (heartbeat.interval.ms unset or 0)")
	} else {
		cur = append(cur, "heartbeat.interval.ms is "+oneLine(interval, 20))
	}
	if q := strings.TrimSpace(cfg["heartbeat.action.query"]); q != "" {
		cur = append(cur, fmt.Sprintf("heartbeat.action.query is set (%q)", oneLine(q, 120)))
	}
	if postgresSource {
		cur = append(cur, publicationText(ps))
	}
	res.Current = strings.Join(cur, "; ")

	var b strings.Builder
	if exists {
		fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s changes nothing: the table exists and its columns are not checked (heartbeat.action.query must match them). ", quoteTable(p.Table))
		res.Reversible, res.Rollback = false, ""
	} else {
		fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now()): a new empty table, no lock on any existing table. ", quoteTable(p.Table))
	}
	b.WriteString("It does nothing until the connector writes to it. Follow-up steps, proposed once this step has run (each step's Validate needs the table to exist):")
	n := 1
	if postgresSource {
		if f := publicationFollowUp(p, cfg, ps); f != "" {
			fmt.Fprintf(&b, " (%d) %s", n, f)
			n++
		}
	}
	changes := map[string]string{"heartbeat.action.query": heartbeatQuery(p.Table)}
	if interval == "" || interval == "0" {
		changes["heartbeat.interval.ms"] = defaultHeartbeatIntervalMS
	}
	cj, _ := json.Marshal(map[string]any{"connector": p.Connector, "changes": changes})
	fmt.Fprintf(&b, " (%d) connect_update_connector_config %s, when its allow-list admits these keys; otherwise a Connect admin sets them. Connect restarts the tasks once.", n, cj)
	res.Expected = b.String()
	return res
}

// publicationText is the publication part of Current.
func publicationText(ps pubState) string {
	switch {
	case !ps.read:
		return ps.note
	case ps.st.AllTables:
		return fmt.Sprintf("publication %s is FOR ALL TABLES", ps.name)
	case ps.st.InPublication:
		return fmt.Sprintf("publication %s is filtered and already has the table", ps.name)
	}
	return fmt.Sprintf("publication %s is filtered (%d tables) and does not have the table", ps.name, len(ps.st.Members)+ps.st.OtherMembers)
}

// publicationFollowUp is the pg_add_publication_table step a filtered
// publication needs: Debezium only sees changes of published tables, so an
// unpublished heartbeat table moves no LSN for the connector.
func publicationFollowUp(p heartbeatParams, cfg map[string]string, ps pubState) string {
	if ps.read && (ps.st.AllTables || ps.st.InPublication) {
		return ""
	}
	pj, _ := json.Marshal(map[string]string{"connector": p.Connector, "publication": ps.name, "table": p.Table})
	s := fmt.Sprintf("pg_add_publication_table %s", pj)
	if !ps.read {
		s += " if the publication is filtered (it was not read)"
	}
	s += "; that action accepts only the pipeline's source_tables, so the table is added to source_tables in owners.yaml first."
	if strings.EqualFold(strings.TrimSpace(cfg["publication.autocreate.mode"]), "filtered") {
		s += " publication.autocreate.mode is filtered: the table must also be in table.include.list, or the connector drops it from the publication on restart."
	}
	return s
}

// Execute creates the table through the admin writer, after the scope gate.
func (a *createHeartbeatTable) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseHeartbeat(raw)
	if err != nil {
		return "", err
	}
	if _, _, err := ctxScope(ctx, CreateHeartbeatTableName, func(pl owners.Pipeline) error { return heartbeatInScope(pl, p) }); err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", fmt.Errorf("sourceactions: %s: %w", CreateHeartbeatTableName, errNoWriter)
	}
	if err := a.d.Writer.CreateHeartbeatTable(ctx, p.Table); err != nil {
		return "", fmt.Errorf("sourceactions: %s: %w", CreateHeartbeatTableName, err)
	}
	return fmt.Sprintf("heartbeat table %s exists. Next: publish it if the publication is filtered, and set heartbeat.action.query (%s) on connector %s.", p.Table, heartbeatQuery(p.Table), p.Connector), nil
}
