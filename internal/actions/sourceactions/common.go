package sourceactions

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
	"unicode"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// The Q2a read-only source settings surface (Q board Q0 item 2,
// clients.SourceSettingsReader). Neither clients.go nor the postgres client on
// main carries it yet: Q2a (#859, internal/clients/postgres/settings.go)
// defines it with these exact DTO shapes and JSON tags. The method sets below
// are the subsets each action reads; once #859 (or Q0) lands, the DTOs here
// become aliases of those types and *postgres.DB satisfies the interfaces
// with no other change. See the contract change request of the Q2c/Q2d PR.

// TableReplication is a table's primary key presence and replica identity
// (postgres.TableReplication of Q2a).
type TableReplication struct {
	Table           string `json:"table"`
	HasPrimaryKey   bool   `json:"has_primary_key"`
	ReplicaIdentity string `json:"replica_identity,omitempty"` // default | full | index | nothing; "" when the source has none
}

// RoleInfo is a source role's login and replication rights and its SELECT
// privilege per table (postgres.RoleInfo of Q2a).
type RoleInfo struct {
	Role        string          `json:"role"`
	Exists      bool            `json:"exists"`
	CanLogin    bool            `json:"can_login"`
	Replication bool            `json:"replication"`
	Selectable  map[string]bool `json:"selectable"` // table -> SELECT
}

// DBSession is one session that can hold back logical replication
// (postgres.DBSession of Q2a). Query is untrusted text.
type DBSession struct {
	PID        int64     `json:"pid"`
	StartedAt  time.Time `json:"started_at"`
	XactStart  time.Time `json:"xact_start"`
	User       string    `json:"user"`
	App        string    `json:"application"`
	State      string    `json:"state"`
	Query      string    `json:"query"`
	BlocksSlot bool      `json:"blocks_slot"`
}

// TableReplicationReader is SourceSettingsReader.TableReplication: one entry
// per table, in order; a missing table is clients.ErrNotFound.
type TableReplicationReader interface {
	TableReplication(ctx context.Context, tables []string) ([]TableReplication, error)
}

// RoleInfoReader is SourceSettingsReader.RoleInfo: a missing role is not an
// error (Exists false).
type RoleInfoReader interface {
	RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error)
}

// BlockerReader is SourceSettingsReader.Blockers, the read behind the
// source_blocking_sessions tool.
type BlockerReader interface {
	Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error)
}

// opTimeout bounds every read of Validate and Preview and Execute's re-checks
// (AGENTS.md §4 rule 4); the admin writer applies its own deadline.
const opTimeout = 10 * time.Second

// layerSource is the approving layer of every action here: the source
// database's owner.
const layerSource = "postgres"

// identRe is AGENTS.md §4 rule 6; NAMEDATALEN caps a Postgres name at 63
// bytes, and MySQL's limit (64) is wider.
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// tablePattern is the JSON Schema pattern of a "schema.table" parameter.
const tablePattern = `^[a-zA-Z_][a-zA-Z0-9_]{0,62}\.[a-zA-Z_][a-zA-Z0-9_]{0,62}$`

// errNoWriter is the refusal when wiring handed the action no admin writer.
var errNoWriter = errors.New("no write credentials: the source database admin writer is not configured (PIPEROOM_WRITE_MODE=on and SOURCE_DB_WRITE_URL are both needed)")

// decodeStrict decodes params, refusing unknown fields and trailing data.
func decodeStrict(name string, raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("sourceactions: %s: bad params: %w", name, err)
	}
	if dec.More() {
		return fmt.Errorf("sourceactions: %s: bad params: trailing data after the object", name)
	}
	return nil
}

// checkTable validates a "schema.table" identifier.
func checkTable(name, table string) error {
	schema, tbl, ok := strings.Cut(table, ".")
	if !ok || !identRe.MatchString(schema) || !identRe.MatchString(tbl) {
		return fmt.Errorf("sourceactions: %s: invalid table %q: want \"schema.table\", each part letters, digits and _, not starting with a digit, at most 63", name, cut(table))
	}
	return nil
}

// checkConnectorName refuses an empty or oversized connector name.
func checkConnectorName(name, connector string) error {
	if strings.TrimSpace(connector) == "" || len(connector) > 255 {
		return fmt.Errorf("sourceactions: %s: connector is required (at most 255 bytes)", name)
	}
	return nil
}

// pipelineScope is the CheckScope base of every action: a CDC pipeline whose
// connectors include connector.
func pipelineScope(name string, pl owners.Pipeline, connector string) error {
	if err := pipelineKindCDC(name, pl); err != nil {
		return err
	}
	if !slices.Contains(pl.Connectors, connector) {
		return fmt.Errorf("sourceactions: %s: %w: connector %q is not one of pipeline %q's connectors", name, tools.ErrOutOfScope, cut(connector), pl.ID)
	}
	return nil
}

// pipelineKindCDC refuses a pipeline that is not a CDC one: only a CDC
// pipeline has a source database.
func pipelineKindCDC(name string, pl owners.Pipeline) error {
	if pl.KindOrDefault() != events.PipelineKindCDC {
		return fmt.Errorf("sourceactions: %s: %w: pipeline %q is not a CDC pipeline", name, tools.ErrOutOfScope, pl.ID)
	}
	return nil
}

// sourceTablesScope refuses a table that is not one of pl's source_tables.
func sourceTablesScope(name string, pl owners.Pipeline, tables ...string) error {
	for _, t := range tables {
		if !slices.Contains(pl.SourceTables, t) {
			return fmt.Errorf("sourceactions: %s: %w: table %s is not one of pipeline %q's source_tables", name, tools.ErrOutOfScope, t, pl.ID)
		}
	}
	return nil
}

// ctxScope is the fail-closed scope of Preview and Execute, in the writer's
// order: the room's whole pipeline (check runs on it), or the explicit
// tools.WithUnscoped opt-out; anything else is tools.ErrNoScope.
func ctxScope(ctx context.Context, name string, check func(owners.Pipeline) error) (owners.Pipeline, bool, error) {
	if pl, ok := tools.PipelineFrom(ctx); ok {
		return pl, true, check(pl)
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err == nil && sc.Unscoped() {
		return owners.Pipeline{}, false, nil
	}
	if err == nil || !errors.Is(err, tools.ErrNoScope) {
		err = fmt.Errorf("%w: %v", tools.ErrNoScope, err)
	}
	return owners.Pipeline{}, false, fmt.Errorf("sourceactions: %s: it needs the room's whole pipeline in ctx: %w", name, err)
}

// validateScope runs check when ctx carries the room's pipeline. Validate is
// also called without one (CheckScope is the room service's gate), so a
// missing pipeline is not an error here.
func validateScope(ctx context.Context, check func(owners.Pipeline) error) error {
	if pl, ok := tools.PipelineFrom(ctx); ok {
		return check(pl)
	}
	return nil
}

// configRef reports whether a connector config value is a config provider
// reference (${file:...}, ${env:...}, ${secret:...}): its real value is not
// in the config Connect returns, so it cannot be compared.
func configRef(v string) bool {
	v = strings.TrimSpace(v)
	return strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}")
}

// isDebeziumSource reports whether a connector class is one of Debezium's
// source connectors, the only connectors whose database.user and source
// database these actions act on.
func isDebeziumSource(class string) bool {
	return strings.HasPrefix(strings.TrimSpace(class), "io.debezium.connector.")
}

// quoteIdent and quoteTable render identifiers for preview and result text
// only; no statement is built from them here. The identifiers were checked
// against identRe, which admits no double quote, but the rendering still
// escapes one the Postgres way, so a copied statement stays safe.
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func quoteTable(t string) string {
	s, n, _ := strings.Cut(t, ".")
	return quoteIdent(s) + "." + quoteIdent(n)
}

func quoteTables(ts []string) string {
	q := make([]string, len(ts))
	for i, t := range ts {
		q[i] = quoteTable(t)
	}
	return strings.Join(q, ", ")
}

// cut caps an echoed input in an error.
func cut(s string) string {
	const n = 64
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}

// oneLine folds untrusted display text (a role, application or user name, a
// query) to one line: control characters and whitespace runs become single
// spaces, so the text cannot forge extra lines in a preview. It then cuts it
// to at most n bytes without splitting a UTF-8 sequence.
func oneLine(s string, n int) string {
	var b strings.Builder
	space := false
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == utf8.RuneError {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	out := b.String()
	if len(out) <= n {
		return out
	}
	c := n - len("…")
	for c > 0 && !utf8.RuneStart(out[c]) {
		c--
	}
	return out[:c] + "…"
}

// maxQueryBytes caps a session query shown in a preview (the Q2a client's
// MaxSessionQueryBytes).
const maxQueryBytes = 200

// safeQuery redacts (N5) and folds a session query again: the reader is
// expected to have done it, but a preview must not depend on that.
func safeQuery(q string) string {
	q, _ = redact.Text(q)
	return oneLine(q, maxQueryBytes)
}

// withTimeout bounds one read phase.
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = opTimeout
	}
	return context.WithTimeout(ctx, d)
}
