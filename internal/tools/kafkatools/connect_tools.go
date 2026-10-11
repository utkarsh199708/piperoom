package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

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
	return "Lists all Kafka Connect connectors with their type, state and failed task count."
}

func (t *connectList) Run(_ context.Context, _ json.RawMessage) (tools.Result, error) {
	cctx, cancel := ctx()
	defer cancel()
	names, err := t.d.Connect.ListConnectors(cctx)
	if err != nil {
		return tools.Result{}, err
	}
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

func (t *connectStatus) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Connector string `json:"connector"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Connector == "" {
		return tools.Result{}, fmt.Errorf("%w: connector is required", tools.ErrBadInput)
	}
	cctx, cancel := ctx()
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
			if len(cond) > 2000 {
				cond = cond[:2000] + "\n…"
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
	return "Returns the key settings of a connector config, redacting any credential value (keys containing password, secret, token or .key)."
}

func (t *connectConfig) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Connector string `json:"connector"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Connector == "" {
		return tools.Result{}, fmt.Errorf("%w: connector is required", tools.ErrBadInput)
	}
	cctx, cancel := ctx()
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
		if important[k] || strings.HasPrefix(k, "transforms") {
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

// condenseTrace keeps the top exception line and every "Caused by:" line,
// dropping stack frames and stripping embedded schema JSON blobs.
var (
	schemaBlobRe = regexp.MustCompile(`\{\.\.\.[^}]*\.\.\.\}`)
	oldSchemaRe  = regexp.MustCompile(`oldSchema\s*:\s*'\{[^']*\}'`)
)

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

func sanitizeTrace(s string) string {
	s = schemaBlobRe.ReplaceAllString(s, "<schema>")
	s = oldSchemaRe.ReplaceAllString(s, "oldSchema: <schema>")
	return s
}
