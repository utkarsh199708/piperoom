package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// ---- connect_list_connectors ----

type connectList struct{ d Deps }

func (t *connectList) Name() string     { return "connect_list_connectors" }
func (t *connectList) Kind() tools.Kind { return tools.KindRead }
func (t *connectList) InputSchema() map[string]any {
	return objectSchema(map[string]any{})
}
func (t *connectList) Description() string {
	return "Lists the pipeline's Kafka Connect connectors with their type, state and failed task count."
}

func (t *connectList) Run(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	names, err := t.d.Connect.ListConnectors(cctx)
	if err != nil {
		return tools.Result{}, err
	}
	// Only this room's connectors; the others get no Status call either.
	names = sc.FilterConnectors(names)
	type row struct {
		Name        string `json:"name"`
		Type        string `json:"type"`
		State       string `json:"state"`
		FailedTasks int    `json:"failed_tasks"`
	}
	var rows []row
	for _, n := range names {
		st, err := t.d.Connect.Status(cctx, n)
		if err != nil {
			rows = append(rows, row{Name: n, State: "UNKNOWN"})
			continue
		}
		failed := 0
		for _, ts := range st.Tasks {
			if ts.State == "FAILED" {
				failed++
			}
		}
		rows = append(rows, row{Name: n, Type: st.Type, State: st.State, FailedTasks: failed})
	}
	var sb strings.Builder
	for i, r := range rows {
		if i > 0 {
			sb.WriteString("\n")
		}
		fmt.Fprintf(&sb, "%s (%s): %s, %d failed tasks", r.Name, r.Type, r.State, r.FailedTasks)
	}
	return res(sb.String(), rows), nil
}

// ---- connect_status ----

type connectStatus struct{ d Deps }

func (t *connectStatus) Name() string     { return "connect_status" }
func (t *connectStatus) Kind() tools.Kind { return tools.KindRead }
func (t *connectStatus) InputSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}
func (t *connectStatus) Description() string {
	return "Returns a connector's state and each task's state; for FAILED tasks the exception class, message and every 'Caused by:' line (stack frames and schema blobs stripped)."
}

func (t *connectStatus) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Connector string `json:"connector"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Connector == "" {
		return tools.Result{}, fmt.Errorf("%w: connector is required", tools.ErrBadInput)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckConnector(p.Connector); err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	st, err := t.d.Connect.Status(cctx, p.Connector)
	if err != nil {
		return tools.Result{}, err
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s (%s): %s", st.Name, st.Type, st.State)
	if st.WorkerID != "" {
		fmt.Fprintf(&sb, " on %s", st.WorkerID)
	}
	sb.WriteString(".")
	type taskRow struct {
		ID    int    `json:"id"`
		State string `json:"state"`
		Error string `json:"error,omitempty"`
	}
	var tasks []taskRow
	for _, ts := range st.Tasks {
		sb.WriteString(fmt.Sprintf("\nTask %d: %s", ts.ID, ts.State))
		tr := taskRow{ID: ts.ID, State: ts.State}
		if ts.State == "FAILED" && ts.Trace != "" {
			cond := condenseTrace(ts.Trace)
			if t.d.Registry != nil {
				if line := rejectedSchemaLine(cctx, t.d.Registry, sc, ts.Trace); line != "" {
					cond += "\n" + line
				}
			}
			if len(cond) > 2000 {
				cond = truncateRunes(cond, 2000) + "\n…"
			}
			tr.Error = cond
			sb.WriteString(":\n" + cond)
		}
		tasks = append(tasks, tr)
	}
	return res(sb.String(), map[string]any{
		"name": st.Name, "state": st.State, "worker": st.WorkerID, "tasks": tasks,
	}), nil
}

// ---- connect_config ----

type connectConfig struct{ d Deps }

func (t *connectConfig) Name() string     { return "connect_config" }
func (t *connectConfig) Kind() tools.Kind { return tools.KindRead }
func (t *connectConfig) InputSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}
func (t *connectConfig) Description() string {
	return "Returns the key settings of a connector config, redacting any credential value (keys containing password, secret, token or .key) and every transforms.* value except each transform's type and field names."
}

func (t *connectConfig) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Connector string `json:"connector"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Connector == "" {
		return tools.Result{}, fmt.Errorf("%w: connector is required", tools.ErrBadInput)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckConnector(p.Connector); err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	cfg, err := t.d.Connect.Config(cctx, p.Connector)
	if err != nil {
		return tools.Result{}, err
	}

	important := map[string]bool{
		"connector.class": true, "table.include.list": true, "topic.prefix": true, "slot.name": true,
		"key.converter": true, "value.converter": true, "decimal.handling.mode": true,
		"database.hostname": true, "database.port": true, "database.dbname": true,
		"database.user": true, "database.server.name": true,
	}
	shown := map[string]string{}
	for k, v := range cfg {
		if sensitiveKey(k) {
			shown[k] = "***" // presence shown, value redacted
			continue
		}
		if strings.HasPrefix(k, "transforms") {
			// Transform settings can carry literal values (static fields,
			// regexes, replacements): show only the allow-listed keys.
			if transformKeyAllowed(k) {
				shown[k] = v
			} else {
				shown[k] = "***"
			}
			continue
		}
		if important[k] {
			shown[k] = v
		}
	}

	var sb strings.Builder
	for _, k := range sortedKeys(shown) {
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		fmt.Fprintf(&sb, "%s: %s", k, shown[k])
	}
	return res(sb.String(), shown), nil
}

// transformAllowed are the transforms.<name>.<setting> settings whose values are
// shown: the transform class and the names of the fields it works on. Every
// other transforms.* value is redacted.
var transformAllowed = map[string]bool{
	"type": true, "field": true, "fields": true, "field.name": true,
}

// transformKeyAllowed reports whether a transforms* key may show its value:
// "transforms" itself (the list of transform names) and the allow-listed
// settings of each transform.
func transformKeyAllowed(k string) bool {
	if k == "transforms" {
		return true
	}
	rest, ok := strings.CutPrefix(k, "transforms.")
	if !ok {
		return false
	}
	_, setting, ok := strings.Cut(rest, ".")
	return ok && transformAllowed[setting]
}

func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// sensitiveKey reports whether a config key holds a credential. ".key" targets
// fields like "ssl.key.password"/"api.key" but not "key.converter".
func sensitiveKey(k string) bool {
	lower := strings.ToLower(k)
	if strings.Contains(lower, "password") || strings.Contains(lower, "secret") || strings.Contains(lower, "token") {
		return true
	}
	if strings.Contains(lower, ".key") {
		return true
	}
	return false
}

// truncateRunes returns s truncated to n bytes on a UTF-8 rune boundary.
func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// condenseTrace keeps the top exception line and every "Caused by:" line,
// dropping stack frames and stripping embedded schema JSON blobs.
func condenseTrace(trace string) string {
	var out []string
	for _, line := range strings.Split(trace, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "at ") {
			continue
		}
		if strings.HasPrefix(trimmed, "Caused by:") || len(out) == 0 {
			out = append(out, sanitizeTrace(trimmed))
		}
	}
	return strings.Join(out, "\n")
}

// sanitizeTrace strips the embedded Avro schema JSON blobs from a trace line.
func sanitizeTrace(s string) string {
	s = stripSchemaAfter(s, "Error registering Avro schema")
	s = stripOldSchema(s)
	return s
}

// stripSchemaAfter replaces the balanced {…} JSON that follows marker with <schema>.
func stripSchemaAfter(s, marker string) string {
	idx := strings.Index(s, marker)
	if idx < 0 {
		return s
	}
	rest := s[idx+len(marker):]
	j := 0
	for j < len(rest) && (rest[j] == ' ' || rest[j] == '\t') {
		j++
	}
	if j >= len(rest) || rest[j] != '{' {
		return s
	}
	end := balancedBrace(rest, j)
	return s[:idx+len(marker)] + "<schema>" + rest[end:]
}

// stripOldSchema replaces oldSchema: '{…}' (single-quoted JSON) with oldSchema: <schema>.
func stripOldSchema(s string) string {
	idx := strings.Index(s, "oldSchema:")
	if idx < 0 {
		return s
	}
	rest := s[idx+len("oldSchema:"):]
	j := 0
	for j < len(rest) && (rest[j] == ' ' || rest[j] == '\t') {
		j++
	}
	if j >= len(rest) || rest[j] != '\'' {
		return s
	}
	k := closingQuote(rest, j)
	return s[:idx+len("oldSchema:")] + " <schema>" + rest[k+1:]
}

// balancedBrace returns the index just past the balanced {…} starting at start (points at '{').
func balancedBrace(s string, start int) int {
	depth, inStr, esc := 0, false, false
	for i := start; i < len(s); i++ {
		c := s[i]
		if inStr {
			if esc {
				esc = false
				continue
			}
			if c == '\\' {
				esc = true
				continue
			}
			if c == '"' {
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return len(s)
}

// closingQuote returns the index of the closing single quote after start (points at the opening ').
func closingQuote(s string, start int) int {
	inStr := false
	for i := start + 1; i < len(s); i++ {
		c := s[i]
		if inStr {
			if c == '\\' {
				i++
				continue
			}
			if c == '"' {
				inStr = false
			}
			continue
		}
		if c == '"' {
			inStr = true
			continue
		}
		if c == '\'' {
			return i
		}
	}
	return len(s) - 1
}

// extractRejectedSchema returns the Avro schema JSON from an "Error registering Avro schema{…}" trace.
func extractRejectedSchema(trace string) (string, bool) {
	idx := strings.Index(trace, "Error registering Avro schema")
	if idx < 0 {
		return "", false
	}
	rest := trace[idx+len("Error registering Avro schema"):]
	j := 0
	for j < len(rest) && (rest[j] == ' ' || rest[j] == '\t') {
		j++
	}
	if j >= len(rest) || rest[j] != '{' {
		return "", false
	}
	end := balancedBrace(rest, j)
	return rest[j:end], true
}

var subjectRe = regexp.MustCompile(`subject\s+"([^"]+)"`)

// extractSubject returns the subject name from a registry error trace.
func extractSubject(trace string) (string, bool) {
	m := subjectRe.FindStringSubmatch(trace)
	if len(m) < 2 {
		return "", false
	}
	return m[1], true
}

// rejectedSchemaLine diffs a rejected schema (from a trace) against the subject's latest version.
func rejectedSchemaLine(ctx context.Context, reg clients.SchemaRegistryClient, sc tools.Scope, trace string) string {
	schema, ok := extractRejectedSchema(trace)
	if !ok {
		return ""
	}
	subject, ok := extractSubject(trace)
	if !ok {
		return ""
	}
	// The subject comes from trace text, not from the pipeline's config: read
	// it only when it belongs to this room's pipeline.
	if !sc.SubjectInScope(subject) {
		return ""
	}
	latest, err := reg.Schema(ctx, subject, -1)
	if err != nil {
		return ""
	}
	changes, err := diffFields(latest.Schema, schema)
	if err != nil || len(changes) == 0 {
		return ""
	}
	return fmt.Sprintf("Rejected schema vs %s v%d:\n%s", subject, latest.Version, formatChanges(changes))
}
