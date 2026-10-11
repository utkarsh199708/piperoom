// Package dbtools registers the read-only tools that inspect the source
// Postgres, the warehouse, and the pipeline topology.
package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// Deps are the collaborators the tools read from.
type Deps struct {
	Src     clients.SourceDB
	Wh      clients.Warehouse
	Builder topology.Builder
}

// Register adds every db tool to reg in a stable order.
func Register(reg *tools.Registry, d Deps) error {
	list := []tools.Tool{
		pgReplicationSlotsTool(d),
		pgRecentDDLTool(d),
		pgLongTransactionsTool(d),
		pgRowTool(d),
		warehouseFreshnessTool(d),
		warehouseRowTool(d),
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
