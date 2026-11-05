package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/dlq"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// defaultConnectorConfigAllowList returns the only connector settings this
// action may change, with the exact values each accepts. Each one changes how a
// connector encodes its output (schema or precision) for NEW events; events
// already emitted keep their old form, so "reversible" applies to the setting,
// not to past events. Two errors.* switches are listed with the single value
// "true": they only add visibility (worker log lines, Connect error headers on
// DLQ records) and change nothing about which records are kept. Turning them
// off is not offered. errors.tolerance is deliberately absent: on a source
// connector "all" drops CDC events for good and setting it back does not
// restore them. errors.log.include.messages is absent too: it writes record
// values into the worker log. The two Debezium heartbeat keys have an empty
// value set: their values are not a fixed list, and heartbeatValueCheck
// (heartbeat_keys.go) checks them instead; they apply to Debezium source
// connectors only. Anything not listed is refused: the action must never become a
// general "edit any connector config" tool. A function, not a package variable,
// so nothing can mutate the list at run time.
func defaultConnectorConfigAllowList() map[string]map[string]bool {
	return map[string]map[string]bool{
		"decimal.handling.mode": {"precise": true, "string": true},
		"time.precision.mode":   {"adaptive": true, "adaptive_time_microseconds": true, "connect": true},
		"tombstones.on.delete":  {"true": true, "false": true},
		// Visibility only; "true" is the only value offered (see above).
		"errors.log.enable":                             {"true": true},
		"errors.deadletterqueue.context.headers.enable": {"true": true},
		// Checked by heartbeatValueCheck, not by value (see above).
		keyHeartbeatInterval: {},
		keyHeartbeatQuery:    {},
	}
}

// sortedKeys returns the keys of an allow-list, sorted.
func sortedKeys(allow map[string]map[string]bool) []string {
	keys := make([]string, 0, len(allow))
	for k := range allow {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---- connect_update_connector_config ----

type updateConnectorConfig struct {
	d Deps
	// allow replaces the default allow-list; tests only. Nil means the default.
	allow map[string]map[string]bool
}

var _ Scoped = (*updateConnectorConfig)(nil)

// allowList returns the allow-list in force.
func (a *updateConnectorConfig) allowList() map[string]map[string]bool {
	if a.allow != nil {
		return a.allow
	}
	allow := defaultConnectorConfigAllowList()
	if a.d.mysqlRoom() {
		// Debezium MySQL source connectors only; checked by codeValueCheck
		// (mysql_config_keys.go). Offered only where a binlog reader is wired
		// (a MySQL room), so a Postgres room's action list does not change.
		allow[keyServerID] = map[string]bool{}
		allow[keySnapshotMode] = map[string]bool{}
	}
	return allow
}

func (a *updateConnectorConfig) Name() string  { return "connect_update_connector_config" }
func (a *updateConnectorConfig) Layer() string { return "kafka-connect" }
func (a *updateConnectorConfig) Describe() string {
	return "Changes one or more settings that set the connector's output schema or precision, in one request and one approval: params {connector, changes: {key: value, ...}} (the old one-key form {connector, key, value} is still accepted). Allowed: decimal.handling.mode precise|string (double is not offered: it loses precision); time.precision.mode adaptive|adaptive_time_microseconds|connect; tombstones.on.delete true|false; and, errors.log.enable true (refused unless errors.log.include.messages is unset or false) and, on a sink connector only, errors.deadletterqueue.context.headers.enable true (they add worker-log lines and Connect error headers on DLQ records; they never change which records are kept). On a Debezium source connector only: heartbeat.interval.ms 0 (off) or 1000..3600000, and heartbeat.action.query, one INSERT INTO or UPDATE of a schema-qualified heartbeat table (its name contains heartbeat; in a schema of the pipeline's source_tables), with literal values, NOW(), CLOCK_TIMESTAMP() or CURRENT_TIMESTAMP only (e.g. the query source_create_heartbeat_table names). Output settings take effect for new events only; events already emitted keep the old form. Connect restarts the tasks once. To undo an output setting, set the old value." + a.d.mysqlDescribe()
}

func (a *updateConnectorConfig) ParamsSchema() map[string]any {
	// propose_fix no longer inlines this schema into the strict request (the
	// agent checks params against it in code and lists it in the tool
	// description), so the enums cost no strict-schema bytes. changes lists
	// every allowed key as a property with its allowed values, so the agent's
	// schema check and the room's exact-key check (param_keys.go) refuse any
	// other key. key/value are the old one-key form, kept for one wave so
	// recorded rooms and evals still parse; Validate refuses a mix of both.
	allow := a.allowList()
	keys := sortedKeys(allow)
	changeProps := make(map[string]any, len(keys))
	for _, k := range keys {
		vals := make([]string, 0, len(allow[k]))
		for v := range allow[k] {
			vals = append(vals, v)
		}
		sort.Strings(vals)
		if len(vals) == 0 {
			// A heartbeat or MySQL key: checked in code (codeValueCheck), not by enum.
			changeProps[k] = map[string]any{"type": "string", "description": a.d.codeKeySchemaText(k)}
			continue
		}
		changeProps[k] = map[string]any{"type": "string", "enum": vals}
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": strProp("connector name"),
			"changes": map[string]any{
				"type":                 "object",
				"description":          "config keys to set, with their new values; all change in one request",
				"properties":           changeProps,
				"additionalProperties": false,
			},
			"key":   map[string]any{"type": "string", "enum": keys, "description": "old one-key form; use changes"},
			"value": map[string]any{"type": "string", "description": "old one-key form; use changes"},
		},
		"additionalProperties": false,
		"required":             []string{"connector"},
	}
}

// updateConnectorConfigParams is the old one-key params shape, still accepted.
type updateConnectorConfigParams struct {
	Connector string `json:"connector"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// Caps on a change set. The allow-list is far smaller; these bound what an
// error message can echo and what a lookalike key can carry.
const (
	maxConfigChanges  = 8
	maxConfigKeyLen   = 128
	maxConfigValueLen = 256
)

// configKeyRe is the shape of a Connect config key name.
var configKeyRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// configChangeSet is parsed params: the connector and every key to set.
type configChangeSet struct {
	connector string
	changes   map[string]string
	// legacy is true when the proposal used the old {key, value} form; the
	// rollback is then written in that form too.
	legacy bool
}

// keys returns the keys of the change set, sorted.
func (s configChangeSet) keys() []string { return sortedStringKeys(s.changes) }

// parseParams decodes and checks params without I/O. Keys and values must
// match the allow-list exactly: no trimming, no case folding, so a lookalike
// ("Decimal.Handling.Mode", a trailing U+200B, a Cyrillic letter) is refused.
func (a *updateConnectorConfig) parseParams(raw json.RawMessage) (configChangeSet, error) {
	var in struct {
		Connector string            `json:"connector"`
		Changes   map[string]string `json:"changes"`
		Key       *string           `json:"key"`
		Value     *string           `json:"value"`
	}
	if err := json.Unmarshal(raw, &in); err != nil || in.Connector == "" {
		return configChangeSet{}, fmt.Errorf("actions: connector and changes (an object of string values) are required, or the one-key form: connector, key and value")
	}
	set := configChangeSet{connector: in.Connector}
	switch {
	case in.Changes != nil && (in.Key != nil || in.Value != nil):
		return set, fmt.Errorf("actions: give either changes or key and value, not both")
	case in.Changes != nil:
		set.changes = in.Changes
	default:
		if in.Key == nil || *in.Key == "" {
			return set, fmt.Errorf("actions: connector, key and value are required (or connector and changes)")
		}
		v := ""
		if in.Value != nil {
			v = *in.Value
		}
		set.changes, set.legacy = map[string]string{*in.Key: v}, true
	}
	if len(set.changes) == 0 {
		return set, fmt.Errorf("actions: changes must name at least one key; one is required")
	}
	if len(set.changes) > maxConfigChanges {
		return set, fmt.Errorf("actions: changes has %d keys; at most %d", len(set.changes), maxConfigChanges)
	}
	allow := a.allowList()
	for _, k := range set.keys() {
		if err := checkConfigChange(allow, k, set.changes[k]); err != nil {
			return set, err
		}
	}
	return set, nil
}

// checkConfigChange refuses one key/value pair: a key that is not a config key
// name, a credential-looking key or value (secrets never travel through a
// proposal), a key off the allow-list and a value the key does not accept.
func checkConfigChange(allow map[string]map[string]bool, key, value string) error {
	if len(key) > maxConfigKeyLen || !configKeyRe.MatchString(key) {
		return fmt.Errorf("actions: key %q is not a config key name (letters, digits, '.', '_', '-'; at most %d bytes)", shortText(key), maxConfigKeyLen)
	}
	if LooksSecret(key) {
		return fmt.Errorf("actions: key %q looks like a credential and cannot be changed", key)
	}
	values, ok := allow[key]
	if !ok {
		return fmt.Errorf("actions: key %q is not allowed; allowed keys: %s", key, strings.Join(sortedKeys(allow), ", "))
	}
	if len(value) > maxConfigValueLen {
		return fmt.Errorf("actions: value for %s is longer than %d bytes", key, maxConfigValueLen)
	}
	if isCodeChecked(key) && len(values) == 0 {
		if err := codeValueCheck(key, value); err != nil {
			return err
		}
		if isHeartbeatKey(key) && heartbeatValueLooksSecret(key, value) {
			return fmt.Errorf("actions: the value for %s looks like a credential; secrets never travel through a proposal", key)
		}
		return nil
	}
	if valueLooksSecret(value) {
		// The value is not echoed: it may be the secret.
		return fmt.Errorf("actions: the value for %s looks like a credential; secrets never travel through a proposal", key)
	}
	if !values[value] {
		if key == "decimal.handling.mode" && value == "double" {
			return fmt.Errorf("actions: value %q is not allowed for %s: double loses precision for money values; allowed values: precise, string", value, key)
		}
		return fmt.Errorf("actions: value %q is not allowed for %s", value, key)
	}
	return nil
}

// valueLooksSecret reports whether a proposed value looks like a credential:
// a secret-looking word, or text the redactor would mask.
func valueLooksSecret(v string) bool {
	if LooksSecret(v) {
		return true
	}
	_, st := redact.Text(v)
	return st.Total > 0
}

// shortText caps untrusted text before it is echoed in an error.
func shortText(s string) string {
	const max = 80
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max]) + "…"
}

// ctxConnectorScope refuses a connector outside the pipeline ctx carries
// (tools.WithPipeline), as tools.ErrOutOfScope. Without a whole pipeline in
// ctx it passes: Validate also runs before the proposal path scopes ctx (and
// CheckScope is the scope check there), and the write client refuses an
// unscoped write itself (tools.ErrNoScope).
func ctxConnectorScope(ctx context.Context, name string) error {
	if _, ok := tools.PipelineFrom(ctx); !ok {
		return nil
	}
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		return fmt.Errorf("actions: %w", err)
	}
	if err := sc.CheckConnector(name); err != nil {
		return fmt.Errorf("actions: %w", err)
	}
	return nil
}

func (a *updateConnectorConfig) Validate(ctx context.Context, raw json.RawMessage) error {
	set, err := a.parseParams(raw)
	if err != nil {
		return err
	}
	if err := ctxConnectorScope(ctx, set.connector); err != nil {
		return err
	}
	if err := ctxHeartbeatScope(ctx, set, false); err != nil {
		return err
	}
	ctx, cancel := a.d.validateContext(ctx)
	defer cancel()
	if err := a.d.connectorExists(ctx, set.connector); err != nil {
		return err
	}
	if !set.hasGuarded() {
		return nil
	}
	cfg, err := a.d.Connect.Config(ctx, set.connector)
	if err != nil {
		return fmt.Errorf("actions: read config of %s: %w", set.connector, err)
	}
	if err := set.checkGuards(cfg); err != nil {
		return err
	}
	if err := a.d.checkServerID(ctx, set, false); err != nil {
		return err
	}
	_, err = a.d.checkRecoveryName(ctx, set, cfg)
	return err
}

// hasGuarded reports whether the set holds a key checkVisibilityGuard checks.
func (s configChangeSet) hasGuarded() bool {
	_, log := s.changes[dlq.KeyLogEnable]
	_, hdr := s.changes[dlq.KeyContextHeaders]
	_, hbi := s.changes[keyHeartbeatInterval]
	_, hbq := s.changes[keyHeartbeatQuery]
	_, sid := s.changes[keyServerID]
	_, snap := s.changes[keySnapshotMode]
	return log || hdr || hbi || hbq || sid || snap
}

// checkGuards runs checkVisibilityGuard on every key of the set, then the
// checks that need the new values (checkValueGuards).
func (s configChangeSet) checkGuards(cfg map[string]string) error {
	for _, k := range s.keys() {
		if err := checkVisibilityGuard(s.connector, k, cfg); err != nil {
			return err
		}
	}
	return s.checkValueGuards(cfg)
}

// checkVisibilityGuard refuses the two errors.* switches where they would not
// be "visibility only": errors.log.enable=true on a connector whose
// errors.log.include.messages is not false starts writing record keys and values into
// the worker log, and the DLQ context headers exist only on sink connectors.
func checkVisibilityGuard(connector, key string, cfg map[string]string) error {
	switch key {
	case dlq.KeyLogEnable:
		// Fail closed: only an unset or literal "false" passes. A config-provider
		// reference such as ${file:/x:incl} comes back unresolved from Connect
		// and may resolve to true.
		if v, set := cfg[dlq.KeyLogMessages]; set && !strings.EqualFold(strings.TrimSpace(v), "false") {
			return fmt.Errorf("actions: %s=true is refused on %s: errors.log.include.messages is not false (it is true or cannot be read as false), so enabling the error log would write failed record values into the worker log; turn errors.log.include.messages off first", key, connector)
		}
	case dlq.KeyContextHeaders:
		if !dlq.Parse(cfg).IsSink {
			return fmt.Errorf("actions: %s only applies to a sink connector; %s is not one", key, connector)
		}
	case keyHeartbeatInterval, keyHeartbeatQuery:
		return checkHeartbeatGuard(connector, key, cfg)
	case keyServerID, keySnapshotMode:
		return checkMySQLKeyGuard(connector, key, cfg)
	}
	return nil
}

func (a *updateConnectorConfig) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params struct {
		Connector string `json:"connector"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	if err := inScopeConnector(p, params.Connector); err != nil {
		return err
	}
	// A heartbeat query names a table on the source database: it must be the
	// pipeline's too. Other keys need no more than the connector.
	set, err := a.parseParams(raw)
	if err != nil {
		if _, hb := set.changes[keyHeartbeatQuery]; hb {
			return err
		}
		return nil
	}
	return set.checkScope(p)
}

// configText renders one config value for a result or preview: "unset" when
// the key is absent, redacted, and capped so an odd stored value cannot flood
// the text. It reads only key: the rest of the config (which can hold
// passwords in plaintext) never leaves this function.
func configText(cfg map[string]string, key string) string {
	v, ok := cfg[key]
	if !ok {
		return "unset"
	}
	v, _ = redact.Text(v)
	maxLen := 80
	if key == keyHeartbeatQuery {
		// The whole statement, so the preview shows what will run; it is
		// capped at write time to maxConfigValueLen, a stored one here.
		maxLen = maxConfigValueLen
	}
	if len(v) > maxLen {
		return strings.ToValidUTF8(v[:maxLen], "") + "..."
	}
	return v
}

// plan splits the change set against the current config: the keys to write
// (value differs or is unset) and the keys already at their value, sorted.
func (s configChangeSet) plan(cfg map[string]string) (todo map[string]string, same []string) {
	todo = map[string]string{}
	for _, k := range s.keys() {
		v := s.changes[k]
		if cur, set := cfg[k]; set && cur == v {
			same = append(same, k)
			continue
		}
		todo[k] = v
	}
	return todo, same
}

func (a *updateConnectorConfig) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	set, err := a.parseParams(raw)
	if err != nil {
		return "", err
	}
	if err := ctxConnectorScope(ctx, set.connector); err != nil {
		return "", err
	}
	if err := ctxHeartbeatScope(ctx, set, true); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// Asserted here only: with write mode off the client is read-only, and the
	// agent must still be able to propose (Validate) and preview.
	fw, ok := a.d.Connect.(clients.ConnectFixWriter)
	if !ok {
		return "", fmt.Errorf("actions: update config %s: connect client does not support UpdateConfig of N keys (clients.ConnectFixWriter); is write mode on?", set.connector)
	}
	before, err := a.d.Connect.Config(ctx, set.connector)
	if err != nil {
		return "", fmt.Errorf("actions: update config %s: %w", set.connector, err)
	}
	// The state may have changed since Validate: check again on what is there now.
	if err := set.checkGuards(before); err != nil {
		return "", err
	}
	if err := a.d.checkServerID(ctx, set, true); err != nil {
		return "", err
	}
	todo, same := set.plan(before)
	sameText := make([]string, 0, len(same))
	for _, k := range same {
		sameText = append(sameText, fmt.Sprintf("%s: already %s", k, set.changes[k]))
	}
	if len(todo) == 0 {
		// A write would restart the tasks for nothing.
		return fmt.Sprintf("%s %s; no change made", set.connector, strings.Join(sameText, ", ")), nil
	}
	todoKeys := sortedStringKeys(todo)
	moves := make([]string, 0, len(todo))
	for _, k := range todoKeys {
		moves = append(moves, fmt.Sprintf("%s: %s → %s", k, configText(before, k), todo[k]))
	}
	if err := fw.UpdateConnectorConfigKeys(ctx, set.connector, todo); err != nil {
		return "", fmt.Errorf("actions: update config %s: %w", set.connector, err)
	}
	// The write is done. Every later problem is a non-error result: an error
	// would tell the agent the change failed and invite a retry.
	head := set.connector + " " + strings.Join(moves, ", ")
	if len(sameText) > 0 {
		head += " (unchanged: " + strings.Join(sameText, ", ") + ")"
	}
	after, err := a.d.Connect.Config(ctx, set.connector)
	if err != nil {
		return fmt.Sprintf("%s (written; re-read of the config failed: %v)", head, err), nil
	}
	var off []string
	for _, k := range todoKeys {
		if got := configText(after, k); got != todo[k] {
			off = append(off, fmt.Sprintf("%s = %s", k, got))
		}
	}
	if len(off) > 0 {
		return fmt.Sprintf("%s (written, but the re-read shows %s; check the connector config)", head, strings.Join(off, ", ")), nil
	}
	// One status read, not a poll: right after the PATCH the first read often
	// still shows the old RUNNING tasks, so waiting for RUNNING proves nothing.
	// The wording says exactly that; the agent re-checks with connect_status.
	st, err := a.d.Connect.Status(ctx, set.connector)
	if err != nil {
		return fmt.Sprintf("%s (written; status read failed: %v)", head, err), nil
	}
	desc, _, running := connectorAndTasks(st)
	if running {
		desc = "RUNNING"
	}
	return fmt.Sprintf("%s; status %s at first read (Connect restarts tasks asynchronously)", head, desc), nil
}

// sortedStringKeys returns the keys of m, sorted.
func sortedStringKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// undoValues returns the values that undo writing todoKeys, or nil when no
// full undo exists. Undo needs, for every key written, a stored value the
// action accepts again; an unset key (or a value we truncated or do not
// offer) cannot be restored by an action, and a partial undo is not offered.
// Two heartbeat cases are restorable all the same: an unset
// heartbeat.interval.ms is Debezium's default 0, and an unset
// heartbeat.action.query never runs while the interval is 0, so the undo sets
// the interval to 0 (note says so) when that is where it goes back to.
func (a *updateConnectorConfig) undoValues(ctx context.Context, cfg map[string]string, todoKeys []string) (map[string]string, string) {
	var pl *owners.Pipeline
	if p, ok := tools.PipelineFrom(ctx); ok {
		pl = &p
	}
	allow := a.allowList()
	prev := make(map[string]string, len(todoKeys))
	queryUnset := false
	for _, k := range todoKeys {
		v, set := cfg[k]
		switch {
		case isMySQLKey(k) && len(allow[k]) == 0:
			undo, ok := mysqlKeyUndo(k, cfg)
			if !ok {
				return nil, ""
			}
			v = undo
		case isHeartbeatKey(k) && len(allow[k]) == 0:
			switch {
			case k == keyHeartbeatInterval && !set:
				v = "0"
			case k == keyHeartbeatQuery && !set:
				queryUnset = true
				continue
			case !heartbeatRestorable(k, v, pl):
				return nil, ""
			}
		case !set || !allow[k][v]:
			return nil, ""
		}
		prev[k] = v
	}
	if !queryUnset {
		return prev, ""
	}
	iv, written := prev[keyHeartbeatInterval]
	if !written {
		// The interval is not changed by this proposal: the undo can stop the
		// query only if heartbeats are off now, and writes that 0 explicitly.
		if cur, set := cfg[keyHeartbeatInterval]; set && cur != "0" {
			return nil, ""
		}
		iv = "0"
	}
	if iv != "0" {
		return nil, ""
	}
	prev[keyHeartbeatInterval] = "0"
	return prev, "heartbeat.action.query was unset and this action cannot remove a key; the undo sets heartbeat.interval.ms to 0, so the query never runs"
}
