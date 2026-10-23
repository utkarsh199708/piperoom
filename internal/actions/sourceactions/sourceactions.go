// Package sourceactions holds the approved source-Postgres admin actions of
// the Q board (row Q2c): pg_set_replica_identity and
// source_create_heartbeat_table. They change the source database itself, so
// they are proposals like every other action: Validate and Preview read only
// (the read-only source pool and the Connect config), and Execute alone calls
// postgres.SourceAdminWriter (Q2b), which exists only when write mode is on
// and SOURCE_DB_WRITE_URL is set.
//
// Both actions refuse a connector or table outside the room's pipeline before
// any I/O (actions.Scoped), and every identifier travels after the
// ^[a-zA-Z_][a-zA-Z0-9_]*$ check and through pgx.Identifier.Sanitize() when it
// is rendered into SQL text (AGENTS.md rule 6).
package sourceactions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/actions/pgactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Layers of the actions and of their follow-up steps, plus the read deadline
// and the connector-name cap. Layer values are the pipeline layers of
// events.Layers.
const (
	sourceLayer      = "postgres"
	connectLayer     = "kafka-connect"
	defaultTimeout   = 10 * time.Second
	maxConnectorName = 255
)

// Action names. The publication follow-up reuses pgactions' action; the
// config follow-up reuses the N-key connect_update_connector_config of
// package actions. Neither is reimplemented here.
const (
	updateConnectorConfigAction = "connect_update_connector_config"
	defaultPublication          = "dbz_publication"
)

// identRe is AGENTS.md rule 6's identifier check, capped at NAMEDATALEN - 1:
// a longer name cannot exist in Postgres.
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// AdminWriter is the Q2b admin write contract of the source Postgres.
//
// It is the postgres package's local copy of the Q0 gap (it is not in
// internal/clients/clients.go): this alias keeps the wiring able to pass a
// *postgres.Writer and the tests able to fake it, without a second shape.
type AdminWriter = postgres.SourceAdminWriter

// PublicationReader is the read pg_add_publication_table already uses for the
// same facts: the publication's shape and one table's existence, kind, replica
// identity, primary key and row estimate. *postgres.DB implements it on the
// read-only pool.
type PublicationReader interface {
	PublicationAddState(ctx context.Context, publication, table string, visible []string) (postgres.PublicationAddState, error)
}

var _ PublicationReader = (*postgres.DB)(nil)

// TableSizeReader is the catalog read of the table's on-disk size, used for
// the FULL extra-WAL estimate. *postgres.DB implements it on the read-only
// pool.
type TableSizeReader interface {
	TableSize(ctx context.Context, table string) (postgres.TableSize, error)
}

var _ TableSizeReader = (*postgres.DB)(nil)

// errNoWriteCredentials is the refusal of Validate and Execute when wiring
// handed the action no admin writer: write mode is off, or SOURCE_DB_WRITE_URL
// is empty. The message names both variables and never a value.
var errNoWriteCredentials = errors.New("no write credentials: the source Postgres admin writer is not configured (PIPEROOM_WRITE_MODE=on and SOURCE_DB_WRITE_URL are both needed)")

// splitTable checks "schema.table" against identRe and returns its parts. It
// does no I/O; nothing is quoted here (quoteTable does that).
func splitTable(s string) (schema, table string, err error) {
	schema, table, ok := strings.Cut(s, ".")
	if !ok || !identRe.MatchString(schema) || !identRe.MatchString(table) {
		return "", "", fmt.Errorf("invalid table %q: want \"schema.table\", each part letters, digits and _, not starting with a digit, at most 63", cut(s))
	}
	return schema, table, nil
}

// quoteTable renders a validated "schema.table" for SQL text (a rollback, a
// follow-up step, the heartbeat query). It is the only place this package
// quotes an identifier: pgx.Identifier.Sanitize double-quotes and escapes it.
func quoteTable(s string) string {
	schema, table, err := splitTable(s)
	if err != nil {
		// Callers pass a table splitTable already accepted; a bad one can only
		// come from a programming error, and an unquoted value must never reach
		// SQL text, so the empty string is safer than the input.
		return ""
	}
	return pgx.Identifier{schema, table}.Sanitize()
}

// cut caps echoed input in an error.
func cut(s string) string {
	const n = 80
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}

// decodeParams decodes raw strictly into dst, so a misspelt key is refused
// rather than ignored.
func decodeParams(action string, raw json.RawMessage, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("sourceactions: %s: bad params: %w", action, err)
	}
	return nil
}

// connectorName checks the connector parameter before any I/O.
func connectorName(action, name string) error {
	if strings.TrimSpace(name) == "" || len(name) > maxConnectorName {
		return fmt.Errorf("sourceactions: %s: connector is required (at most %d bytes)", action, maxConnectorName)
	}
	return nil
}

// checkWriteCredentials is Validate's and Execute's write-mode gate.
func checkWriteCredentials(action string, w AdminWriter) error {
	if w == nil {
		return fmt.Errorf("sourceactions: %s: %w", action, errNoWriteCredentials)
	}
	return nil
}

// readConnectorConfig reads one connector's config through the read-only
// Connect client and refuses anything that is not a Debezium Postgres source:
// only such a connector has a publication and a replica identity to change.
// It returns the config and the publication it publishes to.
func readConnectorConfig(ctx context.Context, action string, c tools.ConfigReader, connector string) (map[string]string, string, error) {
	if c == nil {
		return nil, "", fmt.Errorf("sourceactions: %s: no Connect client to read connector %q's config", action, cut(connector))
	}
	cfg, err := c.Config(ctx, connector)
	if err != nil {
		return nil, "", fmt.Errorf("sourceactions: %s: read connector %q config: %w", action, cut(connector), err)
	}
	if !strings.Contains(cfg["connector.class"], "PostgresConnector") {
		return nil, "", fmt.Errorf("sourceactions: %s: connector %q is not a Debezium Postgres source connector (connector.class %q)", action, cut(connector), cut(cfg["connector.class"]))
	}
	pub := strings.TrimSpace(cfg["publication.name"])
	if pub == "" {
		pub = defaultPublication
	}
	return cfg, pub, nil
}

// publicationState reads publication and table through the read-only source
// client. A missing reader is a clear error, never a silent "unknown". A
// missing publication is clients.ErrNotFound from the client; the heartbeat
// action tolerates it because Debezium may not have created it yet.
func publicationState(ctx context.Context, action string, src clients.SourceDB, publication, table string) (postgres.PublicationAddState, error) {
	if src == nil {
		return postgres.PublicationAddState{}, fmt.Errorf("sourceactions: %s: no source database client", action)
	}
	r, ok := src.(PublicationReader)
	if !ok {
		return postgres.PublicationAddState{}, fmt.Errorf("sourceactions: %s: the source client cannot read publications (no PublicationAddState)", action)
	}
	st, err := r.PublicationAddState(ctx, publication, table, []string{table})
	if err != nil {
		return postgres.PublicationAddState{}, fmt.Errorf("sourceactions: %s: %w", action, err)
	}
	return st, nil
}

// tableSize reads the table's catalog size. A source client without the read
// is a clear error naming the gap, not a made-up estimate.
func tableSize(ctx context.Context, action string, src clients.SourceDB, table string) (postgres.TableSize, error) {
	if src == nil {
		return postgres.TableSize{}, fmt.Errorf("sourceactions: %s: no source database client", action)
	}
	r, ok := src.(TableSizeReader)
	if !ok {
		return postgres.TableSize{}, fmt.Errorf("sourceactions: %s: the source client cannot read a table's size (no TableSize)", action)
	}
	ts, err := r.TableSize(ctx, table)
	if err != nil {
		return postgres.TableSize{}, fmt.Errorf("sourceactions: %s: read table size: %w", action, err)
	}
	return ts, nil
}

// actionTimeout is the deadline of one Validate, Preview or Execute.
func actionTimeout(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return defaultTimeout
}

// ctxScope is the fail-closed scope gate of Preview and Execute, before any
// I/O: the room's whole pipeline (tools.WithPipeline) or the explicit
// tools.WithUnscoped opt-out. Anything else is tools.ErrNoScope. check is the
// action's own actions.Scoped check on that pipeline.
func ctxScope(ctx context.Context, action string, check func(owners.Pipeline) error) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %v", tools.ErrNoScope, err)
		}
		return fmt.Errorf("sourceactions: %s: needs the room's pipeline in ctx: %w", action, err)
	}
	if sc.Unscoped() {
		return nil
	}
	if err := check(sc.Pipeline()); err != nil {
		return fmt.Errorf("sourceactions: %s: %w", action, err)
	}
	return nil
}

// validateScope checks the params against the pipeline in ctx when the room
// service has already put one there, before Validate does any I/O. A ctx with
// no whole pipeline is not refused: the room service calls CheckScope itself
// right after Validate (action.go), and dev/CLI callers opt out explicitly.
func validateScope(ctx context.Context, check func(owners.Pipeline) error) error {
	if pl, ok := tools.PipelineFrom(ctx); ok {
		return check(pl)
	}
	return nil
}

// cdcScope refuses a pipeline that is not a CDC one and a connector that is
// not its own. It does no I/O.
func cdcScope(pl owners.Pipeline, connector string) error {
	if pl.KindOrDefault() != events.PipelineKindCDC {
		return fmt.Errorf("%w: pipeline %q is not a CDC pipeline", tools.ErrOutOfScope, pl.ID)
	}
	for _, c := range pl.Connectors {
		if c == connector {
			return nil
		}
	}
	return fmt.Errorf("%w: connector %q is not one of pipeline %q's connectors", tools.ErrOutOfScope, cut(connector), pl.ID)
}

// holdsSourceTable reports whether one of the pipeline's source_tables lives
// in schema. The heartbeat table is new, so it cannot be a source table; its
// schema must hold one (the writer's checkHeartbeatScope rule).
func holdsSourceTable(pl owners.Pipeline, schema string) bool {
	for _, t := range pl.SourceTables {
		if s, _, err := splitTable(t); err == nil && s == schema {
			return true
		}
	}
	return false
}

// FollowUp is one step that belongs beside an action in the same proposal:
// the existing action to run, its layer, its exact params and why. The
// follow-ups reuse connect_update_connector_config and
// pg_add_publication_table; this package implements neither.
type FollowUp struct {
	Action string          `json:"action"`
	Layer  string          `json:"layer"`
	Params json.RawMessage `json:"params"`
	Why    string          `json:"why"`
}

// HeartbeatFollowUps returns the steps source_create_heartbeat_table needs
// beside itself, as the Q2c row requires:
//
//   - connect_update_connector_config with the N-key params {connector,
//     changes: {heartbeat.action.query: ...}} setting the query Debezium runs
//     on every heartbeat;
//   - pg_add_publication_table with {connector, publication, table} when the
//     connector's publication.autocreate.mode is filtered, because that mode
//     makes Debezium reset the publication to its table filters on restart.
//
// It does no I/O and reads only the connector config it is given (as read by
// Validate or Preview). The heartbeat table must already exist for the
// publication step to validate (pg_add_publication_table refuses a table that
// does not exist), which is what this action's step creates first.
func HeartbeatFollowUps(cfg map[string]string, connector, publication, table string) []FollowUp {
	q, _ := json.Marshal(struct {
		Connector string            `json:"connector"`
		Changes   map[string]string `json:"changes"`
	}{Connector: connector, Changes: map[string]string{"heartbeat.action.query": heartbeatQuery(table)}})
	out := []FollowUp{{
		Action: updateConnectorConfigAction,
		Layer:  connectLayer,
		Params: q,
		Why: "sets the query Debezium runs on every heartbeat: heartbeat.action.query = " + heartbeatQuery(table) +
			". The N-key connect_update_connector_config action must allow heartbeat.action.query (its allow-list is a separate change); heartbeat.interval.ms must also be non-zero or Debezium sends no heartbeats.",
	}}
	if strings.EqualFold(strings.TrimSpace(cfg["publication.autocreate.mode"]), "filtered") {
		p, _ := json.Marshal(struct {
			Connector   string `json:"connector"`
			Publication string `json:"publication"`
			Table       string `json:"table"`
		}{Connector: connector, Publication: publication, Table: table})
		out = append(out, FollowUp{
			Action: pgactions.AddPublicationTableName,
			Layer:  sourceLayer,
			Params: p,
			Why:    "the connector's publication is filtered, so Debezium may reset it to its table filters on restart; the heartbeat table must be a member to stay published once this step has created it",
		})
	}
	return out
}

// heartbeatQuery is the heartbeat.action.query for a table created by
// postgres.SourceAdminWriter.CreateHeartbeatTable (id integer PRIMARY KEY, ts
// timestamptz NOT NULL DEFAULT now()). table is validated and quoted through
// pgx.Identifier.Sanitize.
func heartbeatQuery(table string) string {
	q := quoteTable(table)
	if q == "" {
		return ""
	}
	return "INSERT INTO " + q + " (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = now()"
}
