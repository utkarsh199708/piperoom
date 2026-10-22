package agent

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/redact"
)

// The watcher (internal/watch) opens a room with a JSON body of the shape
//
//	{"source":"watcher","pipeline":"orders","severity":"sev2","at":"…",
//	 "signals":[{"signal":"slot_wal","severity":"sev2","slot":"…",
//	             "retained_bytes":…,"eta_hours":…,…}, …]}
//
// Its numbers (retained WAL, rates, ETA, lag) are what the agent needs first,
// but the body is still data from outside the agent: object names come from
// connector configs and task traces from Kafka Connect. So it is never handed
// to a prompt as is. watcherEvidence renders an allow-list of keys only:
// numbers, booleans, RFC 3339 times, a few fixed-vocabulary strings and
// object names that look like names. The only free text kept is the first
// line of a task trace, masked. Anything else in the body is dropped, so a
// new watcher key reaches the model only after it is added here.

const (
	// maxEvidenceSignals caps the signals rendered from one body.
	maxEvidenceSignals = 6
	// maxEvidenceLine caps one rendered signal line, in bytes.
	maxEvidenceLine = 400
	// maxEvidenceTrace caps the first trace line kept for one task.
	maxEvidenceTrace = 160
	// maxEvidenceListItems caps the items rendered from one nested list
	// (connectors, tasks, partitions, risks).
	maxEvidenceListItems = 3
)

// evidenceNumberKeys are watcher keys whose value is a number (or null).
var evidenceNumberKeys = map[string]bool{
	"retained_bytes": true, "fire_bytes": true, "clear_bytes": true, "safe_wal_bytes": true,
	"rate_1h_bytes_per_hour": true, "rate_6h_bytes_per_hour": true, "eta_hours": true,
	"records": true, "end_offset_sum": true, "new_in_window": true, "new_in_window_all": true,
	"older_than_watcher": true, "rate_per_hour": true, "partitions": true,
	"members": true, "total_lag": true, "lag_rate_per_min": true, "state_changes_15m": true,
	"partition": true, "committed": true, "log_start": true, "records_lost": true,
	"connectors_omitted": true, "tasks_omitted": true, "restarts": true,
	// X1 drift signals (events.DriftSignal): counts only.
	"source": true, "sink": true, "missing": true, "mismatched": true, "sample": true,
	// X3 forecast (events.ForecastSignal) and config_drift
	// (events.ConfigDriftSignal).
	"current": true, "limit": true, "slope_per_hour": true, "r2": true, "samples": true, "keys_omitted": true,
}

// evidenceBoolKeys are watcher keys whose value is a boolean.
var evidenceBoolKeys = map[string]bool{
	"active": true, "exists": true, "declared": true, "shared": true, "context_headers": true,
}

// evidenceTimeKeys are watcher keys whose value is an RFC 3339 time.
var evidenceTimeKeys = map[string]bool{"first_seen": true, "eta": true}

// evidenceWordKeys are watcher keys with a fixed vocabulary (a state, a
// wal_status, an ETA basis, a window like "15m"): one short word.
var evidenceWordKeys = map[string]bool{"state": true, "wal_status": true, "eta_basis": true, "window": true}

// evidenceNameKeys are watcher keys that name an object: a slot, a group, a
// topic, a table, a connector, a forecast's metric and object.
var evidenceNameKeys = map[string]bool{"slot": true, "group": true, "topic": true, "table": true,
	"connector": true, "metric": true, "object": true}

// maxEvidenceChangedKeys caps the config_drift key names rendered.
const maxEvidenceChangedKeys = 8

// driftSignals are the X1 signals whose "window" is an object {from, to}
// (events.DriftWindow), not a word as in dlq_growth.
var driftSignals = map[string]bool{"count_drift": true, "row_drift": true, "dup_keys": true, "order_skew": true}

// evidenceWord is the shape of a fixed-vocabulary value.
var evidenceWord = regexp.MustCompile(`^[A-Za-z0-9_]{1,32}$`)

// evidenceName is the shape of an object name: Kafka topic and group names,
// Postgres slot names and connector names all fit it. A value that does not
// is dropped, not escaped: it cannot be a name the tools would accept.
var evidenceName = regexp.MustCompile(`^[A-Za-z0-9._\-]{1,128}$`)

// AlertEvidenceText renders a watcher alert body (Alert.Raw) as a few short
// lines of allow-listed facts, one per signal. The room service sends it as
// the Text of an "alert_update" HumanInput when the alert fires again or the
// watcher joins a room; the agent's first message carries the same lines.
// A body that is not a watcher body gives a fixed line, never its text.
// The result is untrusted data: callers wrap it before it enters a prompt.
func AlertEvidenceText(raw json.RawMessage) string {
	if s := watcherEvidence(raw); s != "" {
		return s
	}
	return "the alert fired again; no watcher evidence"
}

// watcherBody is the part of a watcher body the agent reads.
type watcherBody struct {
	Source   string           `json:"source"`
	Severity string           `json:"severity"`
	At       string           `json:"at"`
	Signals  []map[string]any `json:"signals"`
	// ConfigRisks are the pipeline's config_risk findings when another
	// signal opened or joined the room (watch.compose): connector risks and
	// X7d retention findings, rendered like a config_risk signal's risks.
	ConfigRisks any `json:"config_risks"`
}

// parseWatcherBody decodes raw when it is a watcher body with at least one
// signal; ok is false otherwise.
func parseWatcherBody(raw json.RawMessage) (watcherBody, bool) {
	if len(raw) == 0 {
		return watcherBody{}, false
	}
	var b watcherBody
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&b); err != nil || b.Source != "watcher" || len(b.Signals) == 0 {
		return watcherBody{}, false
	}
	return b, true
}

// watcherEvidence renders raw when it is a watcher body; "" otherwise.
func watcherEvidence(raw json.RawMessage) string {
	b, ok := parseWatcherBody(raw)
	if !ok {
		return ""
	}
	var lines []string
	head := "source=watcher"
	if evidenceWord.MatchString(b.Severity) {
		head += " severity=" + b.Severity
	}
	if t, ok := evidenceTime(b.At); ok {
		head += " at=" + t
	}
	lines = append(lines, head)
	for i, s := range b.Signals {
		if i == maxEvidenceSignals {
			lines = append(lines, fmt.Sprintf("(%d more signals)", len(b.Signals)-maxEvidenceSignals))
			break
		}
		if line := signalLine(s); line != "" {
			lines = append(lines, line)
		}
	}
	if rs := riskFields(b.ConfigRisks); len(rs) > 0 {
		lines = append(lines, truncatePromptField("config_risks "+strings.Join(rs, " "), maxEvidenceLine))
	}
	return strings.Join(lines, "\n")
}

// signalLine renders one signal of a watcher body.
func signalLine(s map[string]any) string {
	sig, _ := s["signal"].(string)
	if !evidenceWord.MatchString(sig) {
		return ""
	}
	parts := []string{"signal=" + sig}
	if sev, _ := s["severity"].(string); evidenceWord.MatchString(sev) {
		parts = append(parts, "severity="+sev)
	}
	if t, ok := evidenceTime(s["since"]); ok {
		parts = append(parts, "since="+t)
	}
	if n, ok := evidenceNumber(s["firing_for_seconds"]); ok {
		parts = append(parts, "firing_for="+n+"s")
	}
	parts = append(parts, flatFields(s)...)
	if driftSignals[sig] {
		parts = append(parts, driftWindowFields(s["window"])...)
	}
	parts = append(parts, changedKeyFields(s["changed_keys"])...)
	parts = append(parts, connectorFields(s["connectors"])...)
	parts = append(parts, riskFields(s["risks"])...)
	parts = append(parts, partitionFields(s["worst_partitions"])...)
	return truncatePromptField(strings.Join(parts, " "), maxEvidenceLine)
}

// flatFields renders the allow-listed scalar keys of m in key order, so the
// same body always renders the same text.
func flatFields(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		if v, ok := evidenceValue(k, m[k]); ok {
			out = append(out, k+"="+v)
		}
	}
	return out
}

// evidenceValue renders one allow-listed scalar; ok is false for a key that
// is not on a list or a value of the wrong shape.
func evidenceValue(k string, v any) (string, bool) {
	switch {
	case evidenceNumberKeys[k]:
		if v == nil {
			return "unknown", true
		}
		return evidenceNumber(v)
	case evidenceBoolKeys[k]:
		b, ok := v.(bool)
		if !ok {
			return "", false
		}
		return strconv.FormatBool(b), true
	case evidenceTimeKeys[k]:
		return evidenceTime(v)
	case evidenceWordKeys[k]:
		w, _ := v.(string)
		return w, evidenceWord.MatchString(w)
	case evidenceNameKeys[k]:
		n, _ := v.(string)
		return n, evidenceName.MatchString(n)
	}
	return "", false
}

// evidenceNumber renders a JSON number (json.Number or float64) as text; a
// value that is not a finite number is refused.
func evidenceNumber(v any) (string, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return "", false
		}
		return n.String(), true
	case float64:
		if math.IsInf(n, 0) || math.IsNaN(n) {
			return "", false
		}
		return strconv.FormatFloat(n, 'f', -1, 64), true
	}
	return "", false
}

// evidenceTime accepts an RFC 3339 time and returns it in UTC.
func evidenceTime(v any) (string, bool) {
	s, _ := v.(string)
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return "", false
	}
	return t.UTC().Format(time.RFC3339), true
}

// driftWindowFields renders a drift signal's window {from, to} as two times.
func driftWindowFields(v any) []string {
	m, _ := v.(map[string]any)
	from, ok1 := evidenceTime(m["from"])
	to, ok2 := evidenceTime(m["to"])
	if !ok1 || !ok2 {
		return nil
	}
	return []string{"window_from=" + from, "window_to=" + to}
}

// changedKeyFields renders config_drift's changed key names (never values):
// the first maxEvidenceChangedKeys that look like names, comma-joined, the
// count of the rest, and whether a secret key changed (events.SecretKeyChanged
// stands for its name, so only the fact is rendered).
func changedKeyFields(v any) []string {
	items, _ := v.([]any)
	var names []string
	more, secret := 0, false
	for _, it := range items {
		k, _ := it.(string)
		switch {
		case k == events.SecretKeyChanged:
			secret = true
		case !evidenceName.MatchString(k):
		case len(names) == maxEvidenceChangedKeys:
			more++
		default:
			names = append(names, k)
		}
	}
	var out []string
	if len(names) > 0 {
		out = append(out, "changed_keys="+strings.Join(names, ","))
	}
	if more > 0 {
		out = append(out, "changed_keys_more="+strconv.Itoa(more))
	}
	if secret {
		out = append(out, "secret_key_changed=true")
	}
	return out
}

// connectorFields renders connector_down evidence: name, state, and per task
// its id, state and the masked first line of its trace.
func connectorFields(v any) []string {
	conns, _ := v.([]any)
	var out []string
	for i, c := range conns {
		if i == maxEvidenceListItems {
			break
		}
		m, _ := c.(map[string]any)
		name, _ := m["name"].(string)
		if !evidenceName.MatchString(name) {
			continue
		}
		part := "connector=" + name
		if st, _ := m["state"].(string); evidenceWord.MatchString(st) {
			part += " state=" + st
		}
		tasks, _ := m["tasks"].([]any)
		for j, t := range tasks {
			if j == maxEvidenceListItems {
				break
			}
			tm, _ := t.(map[string]any)
			id, ok := evidenceNumber(tm["id"])
			if !ok {
				continue
			}
			st, _ := tm["state"].(string)
			if !evidenceWord.MatchString(st) {
				continue
			}
			part += " task" + id + "=" + st
			if tr := traceLine(tm["trace"]); tr != "" {
				part += fmt.Sprintf(" task%s_trace=%q", id, tr)
			}
		}
		out = append(out, part)
	}
	return out
}

// traceLine keeps the masked first line of a task trace, capped.
func traceLine(v any) string {
	s, _ := v.(string)
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s, _ = redact.Text(s)
	return truncatePromptField(s, maxEvidenceTrace)
}

// riskFields renders config_risk findings: a connector risk as a
// connector:code pair, a retention finding (X7d, events.RetentionRisk) as
// retention_risk=<topic or table>:code with its durations, size cap and
// cleanup policy. Its text is never rendered.
func riskFields(v any) []string {
	risks, _ := v.([]any)
	var out []string
	for i, r := range risks {
		if i == maxEvidenceListItems {
			break
		}
		m, _ := r.(map[string]any)
		code, _ := m["code"].(string)
		if !evidenceWord.MatchString(code) {
			continue
		}
		if retentionCodes[code] {
			if part := retentionRiskField(m, code); part != "" {
				out = append(out, part)
			}
			continue
		}
		conn, _ := m["connector"].(string)
		if !evidenceName.MatchString(conn) {
			continue
		}
		out = append(out, "risk="+conn+":"+code)
	}
	return out
}

// retentionCodes are the RetentionRisk codes.
var retentionCodes = map[string]bool{
	events.RiskRetentionShorter: true, events.RiskRetentionLonger: true, events.RiskCompactCDCTopic: true,
}

// evidenceRetention is topology.FormatRetention's output ("7d", "36h",
// "infinite"); evidencePolicy is a cleanup.policy of the two Kafka words.
var (
	evidenceRetention = regexp.MustCompile(`^(infinite|[0-9]{1,12}[dhms])$`)
	evidencePolicy    = regexp.MustCompile(`^(compact|delete)(,(compact|delete))?$`)
)

// retentionRiskField renders one retention finding; "" when its topic or
// table does not look like a name.
func retentionRiskField(m map[string]any, code string) string {
	obj, _ := m["topic"].(string)
	if obj == "" {
		obj, _ = m["table"].(string)
	}
	if !evidenceName.MatchString(obj) {
		return ""
	}
	part := "retention_risk=" + obj + ":" + code
	for _, k := range []string{"configured", "declared"} {
		if d, _ := m[k].(string); evidenceRetention.MatchString(d) {
			part += " " + k + "=" + d
		}
	}
	if n, ok := evidenceNumber(m["retention_bytes"]); ok {
		part += " retention_bytes=" + n
	}
	if p, _ := m["cleanup_policy"].(string); evidencePolicy.MatchString(p) {
		part += " cleanup_policy=" + p
	}
	return part
}

// partitionFields renders a consumer lag signal's worst partitions as
// topic/partition:lag.
func partitionFields(v any) []string {
	parts, _ := v.([]any)
	var out []string
	for i, p := range parts {
		if i == maxEvidenceListItems {
			break
		}
		m, _ := p.(map[string]any)
		topic, _ := m["topic"].(string)
		part, ok1 := evidenceNumber(m["partition"])
		lag, ok2 := evidenceNumber(m["lag"])
		if !evidenceName.MatchString(topic) || !ok1 || !ok2 {
			continue
		}
		out = append(out, "worst="+topic+"/"+part+":"+lag)
	}
	return out
}
