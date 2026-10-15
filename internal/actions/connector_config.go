package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/dlq"
	"piperoom/internal/owners"
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
// values into the worker log. Anything not listed is refused: the action must never become a
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

// secretSubstrings are fragments that mark a config key as a credential.
var secretSubstrings = []string{"password", "passwd", "secret", "token", "credential", "jaas", "apikey", "private"}

// looksSecret reports whether a config key probably holds a credential. It is
// a second line of defence behind the allow-list: if someone later adds a key
// to the list by mistake, a secret-looking one is still refused. A bare "key"
// segment counts too (key.converter credentials, api.key, ssl.key.password).
func looksSecret(key string) bool {
	k := strings.ToLower(key)
	for _, s := range secretSubstrings {
		if strings.Contains(k, s) {
			return true
		}
	}
	for _, seg := range strings.FieldsFunc(k, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		if seg == "key" {
			return true
		}
	}
	return false
}

// connectorConfigUpdater is the local adapter for the client method that
// changes a connector's config until it lands on clients.ConnectClient
// (contract change request in the PR). The concrete connect client implements
// it; Execute reports a clear error when the injected client does not.
type connectorConfigUpdater interface {
	UpdateConfig(ctx context.Context, name string, key, value string) error
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
	return defaultConnectorConfigAllowList()
}

func (a *updateConnectorConfig) Name() string  { return "connect_update_connector_config" }
func (a *updateConnectorConfig) Layer() string { return "kafka-connect" }
func (a *updateConnectorConfig) Describe() string {
	return "Changes one setting that sets the connector's output schema or precision: decimal.handling.mode precise|string (double is not offered: it loses precision); time.precision.mode adaptive|adaptive_time_microseconds|connect; tombstones.on.delete true|false; and, errors.log.enable true (refused unless errors.log.include.messages is unset or false) and, on a sink connector only, errors.deadletterqueue.context.headers.enable true (they add worker-log lines and Connect error headers on DLQ records; they never change which records are kept). Output settings take effect for new events only; events already emitted keep the old form. Connect restarts the tasks. To undo an output setting, set the old value."
}
func (a *updateConnectorConfig) ParamsSchema() map[string]any {
	// propose_fix no longer inlines this schema into the strict request (the
	// agent checks params against it in code and lists it in the tool
	// description), so an enum costs no strict-schema bytes. key is an enum of
	// the allow-list so a wrong key is refused with the allowed names; the
	// allowed values depend on the key, so value stays a string and Validate
	// enforces the pair.
	return objectSchema(map[string]any{
		"connector": map[string]any{"type": "string"},
		"key":       map[string]any{"type": "string", "enum": sortedKeys(a.allowList())},
		"value":     map[string]any{"type": "string"},
	}, "connector", "key", "value")
}

type updateConnectorConfigParams struct {
	Connector string `json:"connector"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// parseParams decodes and checks params without I/O. Keys and values must
// match the allow-list exactly: no trimming, no case folding, so a lookalike
// ("Decimal.Handling.Mode", a trailing U+200B, a Cyrillic letter) is refused.
func (a *updateConnectorConfig) parseParams(raw json.RawMessage) (updateConnectorConfigParams, error) {
	var p updateConnectorConfigParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" || p.Key == "" {
		return p, fmt.Errorf("actions: connector, key and value are required")
	}
	if looksSecret(p.Key) {
		return p, fmt.Errorf("actions: key %q looks like a credential and cannot be changed", p.Key)
	}
	allow := a.allowList()
	values, ok := allow[p.Key]
	if !ok {
		return p, fmt.Errorf("actions: key %q is not allowed; allowed keys: %s", p.Key, strings.Join(sortedKeys(allow), ", "))
	}
	if !values[p.Value] {
		if p.Key == "decimal.handling.mode" && p.Value == "double" {
			return p, fmt.Errorf("actions: value %q is not allowed for %s: double loses precision for money values; allowed values: precise, string", p.Value, p.Key)
		}
		return p, fmt.Errorf("actions: value %q is not allowed for %s", p.Value, p.Key)
	}
	return p, nil
}

func (a *updateConnectorConfig) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := a.parseParams(raw)
	if err != nil {
		return err
	}
	ctx, cancel := a.d.validateContext(ctx)
	defer cancel()
	if err := a.d.connectorExists(ctx, p.Connector); err != nil {
		return err
	}
	if p.Key != dlq.KeyLogEnable && p.Key != dlq.KeyContextHeaders {
		return nil
	}
	cfg, err := a.d.Connect.Config(ctx, p.Connector)
	if err != nil {
		return fmt.Errorf("actions: read config of %s: %w", p.Connector, err)
	}
	return checkVisibilityGuard(p, cfg)
}

// checkVisibilityGuard refuses the two errors.* switches where they would not
// be "visibility only": errors.log.enable=true on a connector whose
// errors.log.include.messages is not false starts writing record keys and values into
// the worker log, and the DLQ context headers exist only on sink connectors.
func checkVisibilityGuard(p updateConnectorConfigParams, cfg map[string]string) error {
	switch p.Key {
	case dlq.KeyLogEnable:
		// Fail closed: only an unset or literal "false" passes. A config-provider
		// reference such as ${file:/x:incl} comes back unresolved from Connect
		// and may resolve to true.
		if v, set := cfg[dlq.KeyLogMessages]; set && !strings.EqualFold(strings.TrimSpace(v), "false") {
			return fmt.Errorf("actions: %s=true is refused on %s: errors.log.include.messages is not false (it is true or cannot be read as false), so enabling the error log would write failed record values into the worker log; turn errors.log.include.messages off first", p.Key, p.Connector)
		}
	case dlq.KeyContextHeaders:
		if !dlq.Parse(cfg).IsSink {
			return fmt.Errorf("actions: %s only applies to a sink connector; %s is not one", p.Key, p.Connector)
		}
	}
	return nil
}

func (a *updateConnectorConfig) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params updateConnectorConfigParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeConnector(p, params.Connector)
}

// configText renders one config value for the result: "unset" when the key is
// absent, and capped so an odd stored value cannot flood the result. It reads
// only key: the rest of the config (which can hold passwords in plaintext)
// never leaves this function.
func configText(cfg map[string]string, key string) string {
	v, ok := cfg[key]
	if !ok {
		return "unset"
	}
	const maxLen = 80
	if len(v) > maxLen {
		return v[:maxLen] + "..."
	}
	return v
}

func (a *updateConnectorConfig) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := a.parseParams(raw)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	up, ok := a.d.Connect.(connectorConfigUpdater)
	if !ok {
		return "", fmt.Errorf("actions: update config %s: connect client does not support UpdateConfig", p.Connector)
	}
	before, err := a.d.Connect.Config(ctx, p.Connector)
	if err != nil {
		return "", fmt.Errorf("actions: update config %s: %w", p.Connector, err)
	}
	// The state may have changed since Validate: check again on what is there now.
	if p.Key == dlq.KeyLogEnable || p.Key == dlq.KeyContextHeaders {
		if err := checkVisibilityGuard(p, before); err != nil {
			return "", err
		}
	}
	old := configText(before, p.Key)
	if cur, set := before[p.Key]; set && cur == p.Value {
		// A write would restart the tasks for nothing.
		return fmt.Sprintf("%s %s: already %s; no change made", p.Connector, p.Key, p.Value), nil
	}
	if err := up.UpdateConfig(ctx, p.Connector, p.Key, p.Value); err != nil {
		return "", fmt.Errorf("actions: update config %s: %w", p.Connector, err)
	}
	// The write is done. Every later problem is a non-error result: an error
	// would tell the agent the change failed and invite a retry.
	head := fmt.Sprintf("%s %s: %s → %s", p.Connector, p.Key, old, p.Value)
	after, err := a.d.Connect.Config(ctx, p.Connector)
	if err != nil {
		return fmt.Sprintf("%s (written; re-read of the config failed: %v)", head, err), nil
	}
	if got := configText(after, p.Key); got != p.Value {
		return fmt.Sprintf("%s (written, but the re-read shows %s = %s; check the connector config)", head, p.Key, got), nil
	}
	// One status read, not a poll: right after the PATCH the first read often
	// still shows the old RUNNING tasks, so waiting for RUNNING proves nothing.
	// The wording says exactly that; the agent re-checks with connect_status.
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return fmt.Sprintf("%s (written; status read failed: %v)", head, err), nil
	}
	desc, _, running := connectorAndTasks(st)
	if running {
		desc = "RUNNING"
	}
	return fmt.Sprintf("%s; status %s at first read (Connect restarts tasks asynchronously)", head, desc), nil
}
