package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"piperoom/internal/owners"
)

// connectorConfigAllowList names the only connector settings this action may
// change, with the values each accepts. Every entry is a low-risk Debezium or
// Connect setting that changes how data is encoded or how errors are handled
// and is undone by writing the old value back. Anything not listed is refused:
// the action must never become a general "edit any connector config" tool.
var connectorConfigAllowList = map[string]map[string]bool{
	"decimal.handling.mode": {"precise": true, "double": true, "string": true},
	"time.precision.mode":   {"adaptive": true, "adaptive_time_microseconds": true, "connect": true},
	"tombstones.on.delete":  {"true": true, "false": true},
	"errors.tolerance":      {"none": true, "all": true},
}

// connectorConfigKeys returns the allow-listed keys, sorted, for the schema.
func connectorConfigKeys() []string {
	keys := make([]string, 0, len(connectorConfigAllowList))
	for k := range connectorConfigAllowList {
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

// connectorConfigUpdater is the local adapter for the client methods that
// change a connector's config until they land on clients.ConnectClient
// (contract change request in the PR). The concrete connect client implements
// it; Execute reports a clear error when the injected client does not.
type connectorConfigUpdater interface {
	UpdateConfig(ctx context.Context, name string, key, value string) error
}

// ---- connect_update_connector_config ----

type updateConnectorConfig struct {
	d Deps
	// pollInterval overrides resumePollInterval in tests; zero means the default.
	pollInterval time.Duration
}

var _ Scoped = (*updateConnectorConfig)(nil)

func (a *updateConnectorConfig) Name() string  { return "connect_update_connector_config" }
func (a *updateConnectorConfig) Layer() string { return "kafka-connect" }
func (a *updateConnectorConfig) Describe() string {
	return "Sets one connector setting: decimal.handling.mode precise|double|string; time.precision.mode adaptive|adaptive_time_microseconds|connect; tombstones.on.delete true|false; errors.tolerance none|all. Connect restarts the tasks. Reversible: set the old value."
}
func (a *updateConnectorConfig) ParamsSchema() map[string]any {
	// No property descriptions: propose_fix nests every action's schema, and
	// the strict-schema byte budget in internal/agent is tight. The allowed
	// values per key are in Describe and in Validate's error message.
	return objectSchema(map[string]any{
		"connector": map[string]any{"type": "string"},
		"key":       map[string]any{"type": "string", "enum": connectorConfigKeys()},
		"value":     map[string]any{"type": "string"},
	}, "connector", "key", "value")
}

type updateConnectorConfigParams struct {
	Connector string `json:"connector"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// parseConfigParams decodes and checks params without I/O.
func parseConfigParams(raw json.RawMessage) (updateConnectorConfigParams, error) {
	var p updateConnectorConfigParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" || p.Key == "" {
		return p, fmt.Errorf("actions: connector, key and value are required")
	}
	if looksSecret(p.Key) {
		return p, fmt.Errorf("actions: key %q looks like a credential and cannot be changed", p.Key)
	}
	values, ok := connectorConfigAllowList[p.Key]
	if !ok {
		return p, fmt.Errorf("actions: key %q is not allowed; allowed keys: %s", p.Key, strings.Join(connectorConfigKeys(), ", "))
	}
	if !values[p.Value] {
		return p, fmt.Errorf("actions: value %q is not allowed for %s", p.Value, p.Key)
	}
	return p, nil
}

func (a *updateConnectorConfig) Validate(raw json.RawMessage) error {
	p, err := parseConfigParams(raw)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.d.connectorExists(ctx, p.Connector)
}

func (a *updateConnectorConfig) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params updateConnectorConfigParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeConnector(p, params.Connector)
}

// configText renders a config value for the result: "unset" when the key is
// absent, and capped so an odd stored value cannot flood the result.
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
	p, err := parseConfigParams(raw)
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
	// Connect restarts the tasks after a config change; success needs the
	// connector and every listed task RUNNING again.
	taskCount := 0
	state, running, err := awaitRunning(ctx, pollEvery(a.pollInterval), func(ctx context.Context) (string, error) {
		st, err := a.d.Connect.Status(ctx, p.Connector)
		if err != nil {
			return "", err
		}
		desc, n, ok := connectorAndTasks(st)
		taskCount = n
		if ok {
			return "RUNNING", nil
		}
		return desc, nil
	})
	switch {
	case err != nil:
		return fmt.Sprintf("%s (written; status read failed: %v)", head, err), nil
	case running:
		return fmt.Sprintf("%s; connector and %d tasks RUNNING", head, taskCount), nil
	}
	return fmt.Sprintf("%s; %s (Connect applies restarts asynchronously)", head, orUnknown(state)), nil
}
