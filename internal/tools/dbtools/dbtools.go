// Package dbtools registers the read-only tools that inspect the source
// Postgres, the warehouse, and the pipeline topology.
package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// Deps are the collaborators the tools read from.
type Deps struct {
	Src clients.SourceDB
	Wh  clients.Warehouse
	// WarehouseKind is "postgres" (or empty) or "snowflake". It only decides
	// which table and column names the warehouse tools accept as input.
	WarehouseKind string
	Builder       topology.Builder
	// Connect reads live Kafka Connect connector configs. Only
	// pg_publication_tables uses it, so it is nil-able; the tool reports a
	// clear error when it needs Connect and the field is nil.
	Connect clients.ConnectClient
	// Kafka is read by reconcile for per-bucket topic counts (clients.RateReader,
	// asserted at use time). Nil-able: reconcile then leaves the topic column out.
	Kafka clients.KafkaReader
	// Owners is the loaded owners config, used by the row tools to scope reads
	// to the pipeline named by pipeline_id (security finding F4). It must be
	// non-nil; the row tools refuse to run without it.
	Owners *owners.Owners
}

// pipeline resolves the owners.Pipeline for id. A nil Owners is a wiring fault
// (not the agent's input); an unknown id is ErrBadInput so the agent reports it
// as an input problem.
func (d Deps) pipeline(id string) (owners.Pipeline, error) {
	if d.Owners == nil {
		return owners.Pipeline{}, fmt.Errorf("dbtools: owners not configured")
	}
	p, ok := d.Owners.Pipeline(id)
	if !ok {
		return owners.Pipeline{}, fmt.Errorf("%w: pipeline %q not found", tools.ErrBadInput, id)
	}
	return p, nil
}

// scope resolves the room's pipeline scope for one tool call (finding F11).
// A tool calls it first and checks every name it got against the result before
// it uses a client; a call with no room pipeline and no explicit opt-out is
// refused.
func (d Deps) scope(ctx context.Context) (tools.Scope, error) {
	return tools.ScopeFor(ctx, d.Owners)
}

// resolve returns the pipeline a tool works on. Scoped, it is the room's
// pipeline, and a pipeline_id in the input that differs from it is refused
// (the input is never trusted). Unscoped (explicit opt-out), it is the
// pipeline the input names.
func (d Deps) resolve(sc tools.Scope, inputID string) (owners.Pipeline, error) {
	if sc.Unscoped() {
		return d.pipeline(inputID)
	}
	if err := sc.CheckPipelineID(inputID); err != nil {
		return owners.Pipeline{}, err
	}
	return sc.Pipeline(), nil
}

// scopedTable returns an ErrBadInput error when table is not one of the
// pipeline's own tables (finding F4). tables is SourceTables for the source
// row tool and SinkTables for the warehouse row tool.
func scopedTable(pipelineID, table string, tables []string) error {
	if slices.Contains(tables, table) {
		return nil
	}
	return fmt.Errorf("%w: table %q is not in pipeline %q", tools.ErrBadInput, table, pipelineID)
}

// Register adds every db tool to reg in a stable order. It fails fast when
// Owners is nil, so a mis-wired server refuses to start instead of every row
// tool erroring at call time.
func Register(reg *tools.Registry, d Deps) error {
	if d.Owners == nil {
		return fmt.Errorf("dbtools: Owners is nil (wiring error)")
	}
	list := []tools.Tool{
		pgReplicationSlotsTool(d),
		pgRecentDDLTool(d),
		pgLongTransactionsTool(d),
		pgRowTool(d),
		pgPublicationTablesTool(d),
		warehouseFreshnessTool(d),
		warehouseRowTool(d),
		warehouseLoadErrorsTool(d),
		reconcileTool(d),
		pipelineMapTool(d),
		traceRecordTool(d),
	}
	for _, t := range list {
		if err := reg.Register(t); err != nil {
			return fmt.Errorf("dbtools: register %s: %w", t.Name(), err)
		}
	}
	return nil
}

// tool is a small Tool implementation backed by a run function.
type tool struct {
	name   string
	desc   string
	schema map[string]any
	run    func(ctx context.Context, input json.RawMessage) (tools.Result, error)
}

func (t *tool) Name() string                { return t.name }
func (t *tool) Description() string         { return t.desc }
func (t *tool) InputSchema() map[string]any { return t.schema }
func (t *tool) Kind() tools.Kind            { return tools.KindRead }
func (t *tool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	return t.run(ctx, in)
}

// objSchema builds a strict JSON Schema object.
func objSchema(props map[string]any, required ...string) map[string]any {
	s := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

func intProp(desc string) map[string]any {
	return map[string]any{"type": "integer", "description": desc}
}

// wrapErr maps a topology bad-input sentinel onto tools.ErrBadInput so the
// agent reports it as an input problem, not a target-system failure.
func wrapErr(toolName string, err error) error {
	if errors.Is(err, topology.ErrBadInput) {
		return fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	return fmt.Errorf("dbtools: %s: %w", toolName, err)
}

// decode unmarshals tool input, reporting bad input with tools.ErrBadInput.
func decode(input json.RawMessage, v any) error {
	if len(input) == 0 {
		return fmt.Errorf("%w: missing input", tools.ErrBadInput)
	}
	if err := json.Unmarshal(input, v); err != nil {
		return fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	return nil
}

var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validTable reports whether name is a "schema.table" identifier.
func validTable(name string) bool {
	parts := strings.Split(name, ".")
	return len(parts) == 2 && identRe.MatchString(parts[0]) && identRe.MatchString(parts[1])
}

// snowflakeIdentRe is identRe plus '$', which Snowflake allows in unquoted
// names and internal/clients/snowflake accepts, so both layers agree.
var snowflakeIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]*$`)

// isSnowflake reports whether the warehouse is Snowflake. Empty means the
// default, Postgres.
func (d Deps) isSnowflake() bool {
	return strings.EqualFold(strings.TrimSpace(d.WarehouseKind), "snowflake")
}

// whIdentRe is the identifier regex for the configured warehouse kind.
func (d Deps) whIdentRe() *regexp.Regexp {
	if d.isSnowflake() {
		return snowflakeIdentRe
	}
	return identRe
}

// validWarehouseTable reports whether name is a valid warehouse table. Postgres
// takes "schema.table". A Snowflake sink table can live in another database
// than the DSN's default one, so Snowflake also takes "db.schema.table". Each
// part has to match the kind's identifier regex (AGENTS.md section 4, rule 6);
// the client quotes it again.
func (d Deps) validWarehouseTable(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) != 2 && !(len(parts) == 3 && d.isSnowflake()) {
		return false
	}
	re := d.whIdentRe()
	for _, p := range parts {
		if !re.MatchString(p) {
			return false
		}
	}
	return true
}

// validWarehouseColumn is validColumn for the configured warehouse kind.
func (d Deps) validWarehouseColumn(name string) bool { return d.whIdentRe().MatchString(name) }

func validColumn(name string) bool { return identRe.MatchString(name) }

func parseRFC3339(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %q is not RFC 3339", tools.ErrBadInput, s)
	}
	return t, nil
}

// toolCtx caps the caller's ctx at 10 s even when the parent already has a
// deadline: context.WithTimeout keeps the earlier of the two.
func toolCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 10*time.Second)
}

// cutShort truncates s to at most n runes, never splitting a UTF-8 sequence.
func cutShort(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

func isTimestampColumn(name string) bool {
	return name == "at" || strings.HasSuffix(name, "_at")
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
