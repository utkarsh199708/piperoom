package pgactions

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

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// AddPublicationTableName is the registered name of the action.
const AddPublicationTableName = "pg_add_publication_table"

// addPubTimeout bounds the reads of Validate and Preview (AGENTS.md rule 4).
const addPubTimeout = 10 * time.Second

// addPubDefaultPublication is the publication Debezium's Postgres connector
// uses when its config sets no publication.name.
const addPubDefaultPublication = "dbz_publication"

// addPubMaxListed caps the member names a preview prints.
const addPubMaxListed = 20

// addPubIdentRe is AGENTS.md rule 6's identifier check; NAMEDATALEN caps the
// length at 63 bytes.
var addPubIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// errAddPubNoWriter is Execute's refusal when wiring handed the action no
// SourceWriter (write mode off, or SOURCE_DB_WRITE_URL empty).
var errAddPubNoWriter = errors.New("no write credentials: the source Postgres writer is not configured (PIPEROOM_WRITE_MODE=on and SOURCE_DB_WRITE_URL are both needed)")

// PublicationAddReader is the read the action needs from the source client:
// *postgres.DB implements it on the read-only pool. It is not on
// clients.SourceDB, so the action finds it with a type assertion.
type PublicationAddReader interface {
	PublicationAddState(ctx context.Context, publication, table string, visible []string) (postgres.PublicationAddState, error)
}

var _ PublicationAddReader = (*postgres.DB)(nil)

// AddPublicationTableDeps are the clients of pg_add_publication_table.
type AddPublicationTableDeps struct {
	// Connect reads the connector config (publication.name, table filters).
	// The read-only Connect client is enough.
	Connect tools.ConfigReader
	// Source is the read-only source client; it must implement
	// PublicationAddReader (the real *postgres.DB does).
	Source clients.SourceDB
	// Writer is used by Execute only. nil when write mode is off or
	// SOURCE_DB_WRITE_URL is empty: Validate and Preview still work, so the
	// step can be proposed and reviewed, and Execute refuses.
	Writer clients.SourceWriter

	// timeout overrides addPubTimeout; tests only.
	timeout time.Duration
}

// NewAddPublicationTable returns pg_add_publication_table: ALTER PUBLICATION
// <the connector's publication> ADD TABLE <schema.table>.
func NewAddPublicationTable(d AddPublicationTableDeps) actions.Action {
	return &addPublicationTable{d: d}
}

type addPublicationTable struct{ d AddPublicationTableDeps }

var (
	_ actions.Scoped  = (*addPublicationTable)(nil)
	_ actions.Preview = (*addPublicationTable)(nil)
)

func (a *addPublicationTable) Name() string  { return AddPublicationTableName }
func (a *addPublicationTable) Layer() string { return "postgres" }

func (a *addPublicationTable) Describe() string {
	return "Add one source table of the pipeline to its Debezium connector's publication (ALTER PUBLICATION <publication.name> ADD TABLE <schema.table>) so its changes start replicating. " +
		"Refused unless the publication is the connector's, the table is one of the pipeline's source_tables, exists, has a primary key or REPLICA IDENTITY FULL/INDEX, and is not already published. " +
		"Debezium picks the table up on the next change; existing rows need connect_snapshot_table, and a connector with table.include.list must list the table too."
}

func (a *addPublicationTable) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector":   map[string]any{"type": "string", "description": "The pipeline's Debezium Postgres source connector."},
			"publication": map[string]any{"type": "string", "description": "That connector's publication (its publication.name, default dbz_publication).", "pattern": addPubIdentRe.String()},
			"table":       map[string]any{"type": "string", "description": "The source table to add, \"schema.table\"; one of the pipeline's source_tables.", "pattern": `^[a-zA-Z_][a-zA-Z0-9_]{0,62}\.[a-zA-Z_][a-zA-Z0-9_]{0,62}$`},
		},
		"additionalProperties": false,
		"required":             []string{"connector", "publication", "table"},
	}
}

type addPublicationTableParams struct {
	Connector   string `json:"connector"`
	Publication string `json:"publication"`
	Table       string `json:"table"`
}

// parseAddPublicationTable decodes params strictly and checks every
// identifier before anything reads or writes.
func parseAddPublicationTable(raw json.RawMessage) (addPublicationTableParams, error) {
	var p addPublicationTableParams
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("pgactions: %s: bad params: %w", AddPublicationTableName, err)
	}
	if strings.TrimSpace(p.Connector) == "" || len(p.Connector) > 255 {
		return p, fmt.Errorf("pgactions: %s: connector is required (at most 255 bytes)", AddPublicationTableName)
	}
	if !addPubIdentRe.MatchString(p.Publication) {
		return p, fmt.Errorf("pgactions: %s: invalid publication name %q (letters, digits and _, not starting with a digit, at most 63)", AddPublicationTableName, addPubCut(p.Publication))
	}
	schema, table, ok := strings.Cut(p.Table, ".")
	if !ok || !addPubIdentRe.MatchString(schema) || !addPubIdentRe.MatchString(table) {
		return p, fmt.Errorf("pgactions: %s: invalid table %q: want \"schema.table\", each part letters, digits and _, not starting with a digit, at most 63", AddPublicationTableName, addPubCut(p.Table))
	}
	return p, nil
}

func (a *addPublicationTable) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	t := addPubTimeout
	if a.d.timeout > 0 {
		t = a.d.timeout
	}
	return context.WithTimeout(ctx, t)
}

func (a *addPublicationTable) reader() (PublicationAddReader, error) {
	r, ok := a.d.Source.(PublicationAddReader)
	if !ok {
		return nil, fmt.Errorf("pgactions: %s: the source client cannot read publications (no PublicationAddState)", AddPublicationTableName)
	}
	return r, nil
}

// connectorPublication reads the connector's config and returns it with the
// publication it publishes to. A connector that is not a Debezium Postgres
// source is refused: only its publication is this action's to change.
func (a *addPublicationTable) connectorPublication(ctx context.Context, connector string) (map[string]string, string, error) {
	if a.d.Connect == nil {
		return nil, "", fmt.Errorf("pgactions: %s: no Connect client to read connector %q's publication.name", AddPublicationTableName, connector)
	}
	cfg, err := a.d.Connect.Config(ctx, connector)
	if err != nil {
		return nil, "", fmt.Errorf("pgactions: %s: read connector %q config: %w", AddPublicationTableName, connector, err)
	}
	if !strings.Contains(cfg["connector.class"], "PostgresConnector") {
		return nil, "", fmt.Errorf("pgactions: %s: connector %q is not a Debezium Postgres source connector (connector.class %q)", AddPublicationTableName, connector, addPubCut(cfg["connector.class"]))
	}
	pub := strings.TrimSpace(cfg["publication.name"])
	if pub == "" {
		pub = addPubDefaultPublication
	}
	return cfg, pub, nil
}

// checkAddState refuses a table the ALTER must not or cannot add. It returns
// clients.ErrAlreadyPublished (wrapped) for a table already published.
func checkAddState(p addPublicationTableParams, st postgres.PublicationAddState) error {
	if !st.TableExists {
		return fmt.Errorf("pgactions: %s: table %s does not exist: %w", AddPublicationTableName, p.Table, clients.ErrNotFound)
	}
	if st.AllTables {
		return fmt.Errorf("pgactions: %s: publication %s is FOR ALL TABLES, so %s is already in it: %w", AddPublicationTableName, p.Publication, p.Table, clients.ErrAlreadyPublished)
	}
	if st.InPublication {
		return fmt.Errorf("pgactions: %s: %s is already in publication %s; nothing to do: %w", AddPublicationTableName, p.Table, p.Publication, clients.ErrAlreadyPublished)
	}
	switch st.RelKind {
	case "r":
	case "p":
		return fmt.Errorf("pgactions: %s: %s is a partitioned table; adding one is not supported by this action (its partitions' replica identities and publish_via_partition_root decide what is published): ask a DBA", AddPublicationTableName, p.Table)
	default:
		return fmt.Errorf("pgactions: %s: %s is not an ordinary table (relkind %q); only tables can be published", AddPublicationTableName, p.Table, st.RelKind)
	}
	// Once a table without a replica identity is in a publication that
	// publishes updates and deletes, Postgres refuses every UPDATE and DELETE
	// on it: the fix would break the application writing to it.
	switch {
	case st.ReplicaIdentity == "f", st.ReplicaIdentity == "i":
	case st.ReplicaIdentity == "d" && st.HasPrimaryKey:
	case st.ReplicaIdentity == "n":
		return fmt.Errorf("pgactions: %s: %s has REPLICA IDENTITY NOTHING: once published, every UPDATE and DELETE on it would fail; set REPLICA IDENTITY FULL or add a primary key first", AddPublicationTableName, p.Table)
	default:
		return fmt.Errorf("pgactions: %s: %s has no primary key and REPLICA IDENTITY DEFAULT: once published, every UPDATE and DELETE on it would fail; add a primary key or set REPLICA IDENTITY FULL first", AddPublicationTableName, p.Table)
	}
	return nil
}

// Validate reads the connector config and the publication on the read-only
// clients and refuses a wrong publication, a missing or ineligible table and
// one already published. It does not depend on the scope in ctx (CheckScope
// is the scope gate) nor on the writer (Execute refuses without one).
func (a *addPublicationTable) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseAddPublicationTable(raw)
	if err != nil {
		return err
	}
	r, err := a.reader()
	if err != nil {
		return err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	_, pub, err := a.connectorPublication(ctx, p.Connector)
	if err != nil {
		return err
	}
	if pub != p.Publication {
		return fmt.Errorf("pgactions: %s: wrong publication %q: connector %q publishes to %q (its publication.name); only that publication may be changed", AddPublicationTableName, p.Publication, p.Connector, pub)
	}
	st, err := r.PublicationAddState(ctx, p.Publication, p.Table, []string{p.Table})
	if err != nil {
		return fmt.Errorf("pgactions: %s: %w", AddPublicationTableName, err)
	}
	return checkAddState(p, st)
}

// CheckScope refuses, without I/O, a connector or table outside p and a
// pipeline that is not a CDC one.
func (a *addPublicationTable) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseAddPublicationTable(raw)
	if err != nil {
		return err
	}
	return addPubInScope(pl, p)
}

func addPubInScope(pl owners.Pipeline, p addPublicationTableParams) error {
	if err := refuseMySQL(AddPublicationTableName, pl); err != nil {
		return err
	}
	if pl.KindOrDefault() != events.PipelineKindCDC {
		return fmt.Errorf("pgactions: %s: %w: pipeline %q is not a CDC pipeline", AddPublicationTableName, tools.ErrOutOfScope, pl.ID)
	}
	if !slices.Contains(pl.Connectors, p.Connector) {
		return fmt.Errorf("pgactions: %s: %w: connector %q is not one of pipeline %q's connectors", AddPublicationTableName, tools.ErrOutOfScope, addPubCut(p.Connector), pl.ID)
	}
	if !slices.Contains(pl.SourceTables, p.Table) {
		return fmt.Errorf("pgactions: %s: %w: table %s is not one of pipeline %q's source_tables", AddPublicationTableName, tools.ErrOutOfScope, p.Table, pl.ID)
	}
	return nil
}

// addPubScope is the fail-closed scope of Preview and Execute, in the writer's
// order: the room's whole pipeline (targets checked), or the explicit
// tools.WithUnscoped opt-out; anything else is tools.ErrNoScope.
func addPubScope(ctx context.Context, p addPublicationTableParams) (owners.Pipeline, bool, error) {
	if pl, ok := tools.PipelineFrom(ctx); ok {
		return pl, true, addPubInScope(pl, p)
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err == nil && sc.Unscoped() {
		return owners.Pipeline{}, false, nil
	}
	if err == nil || !errors.Is(err, tools.ErrNoScope) {
		err = fmt.Errorf("%w: %v", tools.ErrNoScope, err)
	}
	return owners.Pipeline{}, false, fmt.Errorf("pgactions: %s: it needs the room's whole pipeline in ctx: %w", AddPublicationTableName, err)
}

// Preview reads the publication's current tables (the pipeline's by name, the
// rest counted) and the table on the read-only source client. It never holds
// the writer.
func (a *addPublicationTable) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseAddPublicationTable(raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	pl, scoped, err := addPubScope(ctx, p)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	r, err := a.reader()
	if err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	cfg, pub, err := a.connectorPublication(ctx, p.Connector)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	if pub != p.Publication {
		return actions.PreviewResult{}, fmt.Errorf("pgactions: %s: wrong publication %q: connector %q publishes to %q", AddPublicationTableName, p.Publication, p.Connector, pub)
	}
	visible := []string{p.Table}
	if scoped {
		// CheckScope put p.Table among them; a repeat would list a member twice.
		visible = pl.SourceTables
	}
	st, err := r.PublicationAddState(ctx, p.Publication, p.Table, visible)
	if err != nil {
		return actions.PreviewResult{}, fmt.Errorf("pgactions: %s: preview: %w", AddPublicationTableName, err)
	}
	return addPubPreview(p, pl, scoped, cfg, st), nil
}

// addPubPreview renders the preview from what was read.
func addPubPreview(p addPublicationTableParams, pl owners.Pipeline, scoped bool, cfg map[string]string, st postgres.PublicationAddState) actions.PreviewResult {
	res := actions.PreviewResult{
		Target:     fmt.Sprintf("publication %s (connector %s) on the source Postgres", p.Publication, p.Connector),
		Current:    addPubMembersText(p, st) + "; " + addPubTableText(p, st),
		Reversible: true,
		Rollback:   fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s (by a DBA; no registered action). Change events already sent stay in Kafka.", addPubQuoteIdent(p.Publication), addPubQuoteTable(p.Table)),
	}
	if err := checkAddState(p, st); err != nil {
		res.Expected = "no change: this step is refused (" + strings.TrimPrefix(err.Error(), "pgactions: "+AddPublicationTableName+": ") + ")"
		res.Reversible, res.Rollback = false, ""
		return res
	}
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER PUBLICATION %s ADD TABLE %s: publication has %d tables after. ", addPubQuoteIdent(p.Publication), addPubQuoteTable(p.Table), len(st.Members)+st.OtherMembers+1)
	b.WriteString("Debezium picks the table up on the next change; use connect_snapshot_table for history")
	if st.EstimatedRows >= 0 {
		fmt.Fprintf(&b, " (~%d existing rows, pg_class.reltuples, are not sent otherwise)", st.EstimatedRows)
	}
	b.WriteString(".")
	for _, c := range addPubDebeziumCaveats(p.Table, cfg) {
		b.WriteString(" ")
		b.WriteString(c)
	}
	res.Expected = b.String()
	if scoped && pl.TopicPrefix != "" {
		res.Downstream = []string{fmt.Sprintf("topic %s.%s receives the table's change events (created on the first event if topic auto-creation is on)", pl.TopicPrefix, p.Table)}
	}
	return res
}

func addPubMembersText(p addPublicationTableParams, st postgres.PublicationAddState) string {
	if st.AllTables {
		return fmt.Sprintf("publication %s is FOR ALL TABLES", p.Publication)
	}
	listed := st.Members
	more := 0
	if len(listed) > addPubMaxListed {
		listed, more = listed[:addPubMaxListed], len(listed)-addPubMaxListed
	}
	var b strings.Builder
	fmt.Fprintf(&b, "publication %s has %d tables", p.Publication, len(st.Members)+st.OtherMembers)
	if len(listed) > 0 {
		b.WriteString(": " + strings.Join(listed, ", "))
	}
	if more > 0 {
		fmt.Fprintf(&b, " and %d more of this pipeline", more)
	}
	if st.OtherMembers > 0 {
		fmt.Fprintf(&b, " (+%d not of this pipeline, not shown)", st.OtherMembers)
	}
	return b.String()
}

func addPubTableText(p addPublicationTableParams, st postgres.PublicationAddState) string {
	if !st.TableExists {
		return p.Table + " does not exist"
	}
	state := "not in it"
	if st.InPublication {
		state = "already in it"
	}
	ident := map[string]string{"d": "DEFAULT", "n": "NOTHING", "f": "FULL", "i": "USING INDEX"}[st.ReplicaIdentity]
	if ident == "" {
		ident = "unknown"
	}
	pk := "no primary key"
	if st.HasPrimaryKey {
		pk = "primary key"
	}
	return fmt.Sprintf("%s is %s (%s, REPLICA IDENTITY %s)", p.Table, state, pk, ident)
}

// addPubDebeziumCaveats says what the connector itself needs for the table to flow:
// its table/schema filters, matched as Debezium does (whole-name regular
// expressions on "schema.table" and "schema"), and the filtered autocreate
// mode, under which the connector may rewrite the publication from its
// filters when it restarts.
func addPubDebeziumCaveats(table string, cfg map[string]string) []string {
	var out []string
	schema, _, _ := strings.Cut(table, ".")
	check := func(key, name string, include bool) {
		list := strings.TrimSpace(cfg[key])
		if list == "" {
			return
		}
		m, ok := addPubMatchesAny(list, name)
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("The connector's %s could not be read as patterns here: check that it lets %s through.", key, name))
		case include && !m:
			out = append(out, fmt.Sprintf("Follow-up needed: the connector's %s does not list %s, so Debezium ignores it until connect_update_connector_config adds it.", key, name))
		case !include && m:
			out = append(out, fmt.Sprintf("Follow-up needed: the connector's %s matches %s, so Debezium ignores it until that is changed.", key, name))
		}
	}
	check("table.include.list", table, true)
	check("table.exclude.list", table, false)
	check("schema.include.list", schema, true)
	check("schema.exclude.list", schema, false)
	if strings.EqualFold(strings.TrimSpace(cfg["publication.autocreate.mode"]), "filtered") {
		out = append(out, "publication.autocreate.mode is filtered: the connector may reset the publication to its table filters on restart, so the table must be in them to stay published.")
	}
	return out
}

// addPubMatchesAny reports whether name fully matches one of the comma-separated
// regular expressions of list; ok is false when one does not compile.
func addPubMatchesAny(list, name string) (match, ok bool) {
	for _, pat := range strings.Split(list, ",") {
		pat = strings.TrimSpace(pat)
		if pat == "" {
			continue
		}
		re, err := regexp.Compile(`^(?:` + pat + `)$`)
		if err != nil {
			return false, false
		}
		if re.MatchString(name) {
			return true, true
		}
	}
	return false, true
}

// Execute adds the table through the SourceWriter. A table published since
// Validate is a no-op with a clear result, not an error: the state asked for
// holds.
func (a *addPublicationTable) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseAddPublicationTable(raw)
	if err != nil {
		return "", err
	}
	if _, _, err := addPubScope(ctx, p); err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", fmt.Errorf("pgactions: %s: %w", AddPublicationTableName, errAddPubNoWriter)
	}
	err = a.d.Writer.AddPublicationTables(ctx, p.Publication, []string{p.Table})
	if errors.Is(err, clients.ErrAlreadyPublished) {
		return fmt.Sprintf("%s is already in publication %s; no change made", p.Table, p.Publication), nil
	}
	if err != nil {
		return "", fmt.Errorf("pgactions: %s: %w", AddPublicationTableName, err)
	}
	return fmt.Sprintf("added %s to publication %s. Debezium picks it up on the next change; existing rows need connect_snapshot_table. Rollback: ALTER PUBLICATION %s DROP TABLE %s.",
		p.Table, p.Publication, addPubQuoteIdent(p.Publication), addPubQuoteTable(p.Table)), nil
}

// addPubQuoteIdent and addPubQuoteTable render identifiers for the preview and
// result text only; no statement is built from them here. The identifiers were
// already checked against addPubIdentRe, which admits no double quote, but the
// rendering still escapes one the Postgres way (doubling it), so the printed
// ALTER stays a safe statement even if copied and run verbatim.
func addPubQuoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func addPubQuoteTable(t string) string {
	s, n, _ := strings.Cut(t, ".")
	return addPubQuoteIdent(s) + "." + addPubQuoteIdent(n)
}

// addPubCut caps an echoed input in an error.
func addPubCut(s string) string {
	const n = 64
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}
