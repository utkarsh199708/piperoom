package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// ConfigValidator is the Connect pair connect_validate_config reads through.
// clients.ConfigValidator is proposed in the Q0 contract change request; until
// it lands this local interface uses the connect package's result types, so the
// real client implements it without an adapter.
type ConfigValidator interface {
	// ValidateConfig asks Connect to validate cfg for class and creates nothing
	// (PUT /connector-plugins/{class}/config/validate).
	ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error)
	// PluginConfigDef lists the class's known configuration keys
	// (GET /connector-plugins/{class}/config, Kafka Connect 3.2+).
	PluginConfigDef(ctx context.Context, class string) ([]connect.ConfigKeyDef, error)
}

// ---- connect_validate_config ----

type connectValidateConfig struct{ d Deps }

func (t *connectValidateConfig) Name() string     { return "connect_validate_config" }
func (t *connectValidateConfig) Kind() tools.Kind { return tools.KindRead }
func (t *connectValidateConfig) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"class": strProp("connector plugin class, e.g. io.debezium.connector.postgresql.PostgresConnector"),
		"config": map[string]any{
			"type":        "object",
			"description": "connector configuration key -> value. Credential-looking keys are refused in plain text; use a ${secret:name}, ${file:path:key} or ${env:NAME} reference, or omit them and pass base_connector so the server fills them in.",
			// A connector config is a free-form map. Strict tool use prefers a
			// closed object, but a config has no fixed key list; the agent's
			// schema check accepts this form.
			"additionalProperties": map[string]any{"type": "string"},
		},
		"base_connector": strProp("optional connector of this pipeline whose config supplies defaults; its credential values are used on the server and never sent back"),
	}, "class", "config")
}

func (t *connectValidateConfig) Description() string {
	return "Asks Kafka Connect to validate a connector configuration without creating a connector (PUT /connector-plugins/{class}/config/validate). Returns the error count and each key's messages. Credential values are never taken from the model: a plain-text value under a credential-looking key is refused, and with base_connector the server fills those keys from that connector's config. A host key (database.hostname, a JDBC/connection URL) must name the pipeline's source host or the base connector's host, so Connect is never asked to reach another host. An unknown key gets the nearest known key of the plugin; a key known to have been renamed also names the known-issue id. Read-only: it creates nothing."
}

func (t *connectValidateConfig) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Class         string            `json:"class"`
		Config        map[string]string `json:"config"`
		BaseConnector string            `json:"base_connector"`
	}
	if err := json.Unmarshal(input, &p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	class := strings.TrimSpace(p.Class)
	if err := checkValidateInput(class, p.Config); err != nil {
		return tools.Result{}, err
	}

	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	val, ok := t.d.Connect.(ConfigValidator)
	if !ok {
		return tools.Result{}, fmt.Errorf("connect_validate_config: the Connect client has no config validator: %w", clients.ErrNotSupported)
	}
	if p.BaseConnector != "" {
		if err := sc.CheckConnector(p.BaseConnector); err != nil {
			return tools.Result{}, err
		}
	}

	cctx, cancel := withTimeout(ctx)
	defer cancel()

	// The model's config is checked before any network call (checkValidateInput
	// above); this copy is what may be sent.
	merged := make(map[string]string, len(p.Config))
	for k, v := range p.Config {
		merged[k] = v
	}
	var secretValues []string
	var allowedHosts = map[string]bool{}
	if p.BaseConnector != "" {
		base, err := t.d.Connect.Config(cctx, p.BaseConnector)
		if err != nil {
			return tools.Result{}, fmt.Errorf("connect_validate_config: reading base connector %q: %w", p.BaseConnector, err)
		}
		secretValues = mergeBaseConfig(merged, base)
		addConfigHosts(allowedHosts, base)
	}
	// The pipeline's source host: a literal connections.source_db_url (a
	// provisional pipeline) and the hosts of the pipeline's own source
	// connectors. A sink connector's host (the warehouse) is deliberately not
	// a source host, so a model cannot point a source config at it.
	if h := connectionSourceHost(sc, "source_db_url"); h != "" {
		allowedHosts[h] = true
	}
	t.addSourceConnectorHosts(cctx, sc, p.BaseConnector, allowedHosts)

	if err := checkConfigHosts(p.Config, merged, allowedHosts); err != nil {
		return tools.Result{}, err
	}

	defs, defsErr := t.pluginDefs(cctx, val, class)
	unknown := unknownConfigKeys(p.Config, defs, t.d.RenamedKey)

	vr, err := val.ValidateConfig(cctx, class, merged)
	if err != nil {
		return tools.Result{}, fmt.Errorf("connect_validate_config: %s: %w", class, err)
	}

	data, summary, capped := renderValidation(class, p.BaseConnector, merged, vr, unknown, defsErr, secretValues, t.d.RenamedKey)
	s, cut := tools.Truncate(summary)
	return tools.Result{Summary: s, Data: data, Truncated: cut || capped}, nil
}

// checkValidateInput refuses bad input before any I/O: an empty or absurd
// class, a key that is not a config key name, an oversized key or value, and a
// plain-text credential value. A credential-looking key accepts only a config
// provider reference, so a secret never travels from the model.
func checkValidateInput(class string, cfg map[string]string) error {
	if class == "" {
		return fmt.Errorf("%w: class is required", tools.ErrBadInput)
	}
	if len(class) > maxValidateClassBytes {
		return fmt.Errorf("%w: class is longer than %d bytes", tools.ErrBadInput, maxValidateClassBytes)
	}
	if strings.ContainsAny(class, "\x00\r\n") {
		return fmt.Errorf("%w: class contains a control character", tools.ErrBadInput)
	}
	if len(cfg) > maxValidateConfigKeys {
		return fmt.Errorf("%w: config has %d keys; at most %d are accepted", tools.ErrBadInput, len(cfg), maxValidateConfigKeys)
	}
	for k, v := range cfg {
		if len(k) > maxValidateKeyBytes || !validateConfigKeyRe.MatchString(k) {
			return fmt.Errorf("%w: %q is not a connector config key name (letters, digits, '.', '_', '-', '$'; at most %d bytes)", tools.ErrBadInput, shortenText(k), maxValidateKeyBytes)
		}
		if len(v) > maxValidateValueBytes {
			return fmt.Errorf("%w: the value of %s is longer than %d bytes", tools.ErrBadInput, k, maxValidateValueBytes)
		}
		if plaintextSecretKey(k) && v != "" && !isConfigReference(v) {
			return fmt.Errorf("%w: %s looks like a credential; pass a ${secret:...}, ${file:...} or ${env:...} reference, or omit it and pass base_connector so the server fills it in", tools.ErrBadInput, k)
		}
	}
	return nil
}

const (
	maxValidateClassBytes  = 300
	maxValidateConfigKeys  = 256
	maxValidateKeyBytes    = 128
	maxValidateValueBytes  = 8192
	maxValidateErrorKeys   = 40
	maxValidateKeyMessages = 6
	maxValidateMessageSize = 300
	maxValidateMessageSum  = 12000
	maxUnknownKeys         = 20
)

var validateConfigKeyRe = regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9._$-]*$`)

// plaintextSecretKey reports whether a config key names a credential under the
// shared actions.LooksSecret rule. looksSecret also flags the converter class
// keys, because they contain a bare "key" segment; a converter class is not a
// credential, so those stay usable (LooksSecret's doc expects each caller's
// allow-list to do exactly this).
func plaintextSecretKey(k string) bool {
	if !actions.LooksSecret(k) {
		return false
	}
	lk := strings.ToLower(k)
	for _, prefix := range []string{"key.converter", "value.converter", "header.converter"} {
		if lk == prefix || strings.HasPrefix(lk, prefix+".") {
			return false
		}
	}
	return true
}

// isConfigReference reports whether v is a Connect config provider reference.
// ${secret:...} is accepted as a non-secret value (U0); ${file:...} and
// ${env:...} are the other documented providers (design, Q5c).
func isConfigReference(v string) bool {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "${") || !strings.HasSuffix(v, "}") || len(v) < 4 {
		return false
	}
	inner := v[2 : len(v)-1]
	for _, p := range []string{"secret:", "file:", "env:"} {
		if strings.HasPrefix(inner, p) && len(inner) > len(p) {
			return true
		}
	}
	return false
}

// mergeBaseConfig lays the base connector's config under the model's and
// returns the credential values that must never be echoed. A model value for a
// credential-looking key never wins over the server's value: the row is "secret
// keys come from base_connector, never from the model".
func mergeBaseConfig(merged, base map[string]string) []string {
	for k, v := range base {
		// The base connector supplies defaults; the model may change a
		// non-secret setting for the configuration it wants validated.
		if _, set := merged[k]; !set {
			merged[k] = v
		}
	}
	var secrets []string
	for k, v := range base {
		if !plaintextSecretKey(k) {
			continue
		}
		if v != "" {
			secrets = append(secrets, v)
		}
		// The base value wins even when the model sent a reference.
		merged[k] = v
	}
	return secrets
}

// addConfigHosts adds the hosts named by a (server-side) config's host keys.
func addConfigHosts(set map[string]bool, cfg map[string]string) {
	for k, v := range cfg {
		if !isHostConfigKey(k) {
			continue
		}
		if h := configHost(v); h != "" {
			set[h] = true
		}
	}
}

// connectionSourceHost returns the host of the pipeline's literal
// connections.source_db_url. A {env:} or {file:} reference is not resolved
// here: the tool has no config and a reference is not a host. A provisional
// pipeline writes a {value:} DSN, which is enough for the setup path.
func connectionSourceHost(sc tools.Scope, key string) string {
	p := sc.Pipeline()
	r, ok := p.Connections[key]
	if !ok {
		return ""
	}
	return configHost(r.Value)
}

// addSourceConnectorHosts adds the hosts of the pipeline's own source
// connectors. Sink connectors are skipped: their connection.url names the
// warehouse, not the source. Read failures are skipped; the host guard then
// fails closed unless the base connector or a connection named the host.
func (t *connectValidateConfig) addSourceConnectorHosts(ctx context.Context, sc tools.Scope, base string, set map[string]bool) {
	for _, name := range sc.Pipeline().Connectors {
		if name == base {
			continue
		}
		st, err := t.d.Connect.Status(ctx, name)
		if err != nil || st.Type != "source" {
			continue
		}
		cfg, err := t.d.Connect.Config(ctx, name)
		if err != nil {
			continue
		}
		addConfigHosts(set, cfg)
	}
}

// isHostConfigKey reports whether a config key names a host Connect may be
// asked to reach. "hostname" and "host" cover Debezium and JDBC connectors;
// the URL and connection-string forms cover JDBC and MongoDB.
func isHostConfigKey(k string) bool {
	lk := strings.ToLower(k)
	switch {
	case strings.Contains(lk, "hostname"):
		return true
	case lk == "host" || strings.HasSuffix(lk, ".host"):
		return true
	case strings.Contains(lk, "jdbc"):
		return true
	case strings.Contains(lk, "connection.string"):
		return true
	case lk == "connection.url" || strings.HasSuffix(lk, ".connection.url"):
		return true
	case lk == "database.url" || strings.HasSuffix(lk, ".database.url"):
		return true
	}
	return false
}

// configHost reduces a host value or a DSN/JDBC URL to a normalized host name.
// Ports are dropped: the guard is "never reach another host", and the same host
// on another port is still the same host. A config provider reference returns
// "" (the caller treats it as unknown, never as a host).
func configHost(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || isConfigReference(v) {
		return ""
	}
	if i := strings.Index(v, "://"); i >= 0 {
		v = v[i+3:]
	}
	if i := strings.LastIndex(v, "@"); i >= 0 {
		v = v[i+1:]
	}
	if i := strings.IndexAny(v, "/?#,; \t"); i >= 0 {
		v = v[:i]
	}
	if h, _, err := net.SplitHostPort(v); err == nil {
		v = h
	}
	v = strings.ToLower(strings.Trim(v, "[]"))
	return strings.TrimSuffix(v, ".")
}

var validHostRe = regexp.MustCompile(`^[a-z0-9._:\[\]-]+$`)

// checkConfigHosts refuses a host key that is not the pipeline's source host or
// the base connector's host, and a host key whose value is not a literal host.
// Only the model's keys are checked (their effective value may come from the
// base connector); a value inherited unchanged from the server-side base config
// is trusted. It runs before the validate request, so Connect is never asked to
// reach an unapproved host.
func checkConfigHosts(model, merged map[string]string, allowed map[string]bool) error {
	keys := make([]string, 0, len(model))
	for k := range model {
		if isHostConfigKey(k) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := strings.TrimSpace(merged[k])
		if v == "" {
			continue // Connect reports the missing required value
		}
		if isConfigReference(v) {
			return fmt.Errorf("%w: host key %s must be a literal host, not a config reference", tools.ErrBadInput, k)
		}
		h := configHost(v)
		if h == "" || !validHostRe.MatchString(h) {
			return fmt.Errorf("%w: host key %s does not name a host", tools.ErrBadInput, k)
		}
		if len(allowed) == 0 {
			return fmt.Errorf("%w: host key %s points at %s, but the pipeline has no known source host; pass base_connector so its host can be checked", tools.ErrOutOfScope, k, h)
		}
		if !allowed[h] {
			return fmt.Errorf("%w: host key %s points at %s, which is neither the pipeline's source host nor the base connector's (allowed: %s)", tools.ErrOutOfScope, k, h, strings.Join(sortedHosts(allowed), ", "))
		}
	}
	return nil
}

func sortedHosts(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for h := range set {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

// pluginDefs reads the plugin's known keys. An older Connect without the
// endpoint (404/405) must not fail the validation itself: the tool then has no
// suggestions and says so.
func (t *connectValidateConfig) pluginDefs(ctx context.Context, val ConfigValidator, class string) (map[string]bool, error) {
	defs, err := val.PluginConfigDef(ctx, class)
	if err != nil {
		if errors.Is(err, clients.ErrNotFound) {
			return nil, fmt.Errorf("Connect has no plugin config endpoint (needs Kafka Connect 3.2+)")
		}
		return nil, err
	}
	set := make(map[string]bool, len(defs))
	for _, d := range defs {
		if d.Name != "" {
			set[d.Name] = true
		}
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("Connect returned no config definitions")
	}
	return set, nil
}

// unknownConfigKey is one model key the plugin does not define. Suggestion is
// the nearest defined key; NewKey and IssueID come from the Q1 known-issues
// index when it is wired (Deps.RenamedKey).
type unknownConfigKey struct {
	Key        string `json:"key"`
	Suggestion string `json:"suggestion,omitempty"`
	NewKey     string `json:"new_key,omitempty"`
	IssueID    string `json:"issue_id,omitempty"`
}

// unknownConfigKeys compares the model's keys with the plugin's definitions and
// returns at most maxUnknownKeys, in key order. A known renamed key is resolved
// through lookup first; otherwise the nearest defined key is suggested.
func unknownConfigKeys(cfg map[string]string, defs map[string]bool, lookup func(string) (string, string, bool)) []unknownConfigKey {
	if len(defs) == 0 {
		return nil
	}
	keys := sortedKeys(cfg)
	var out []unknownConfigKey
	for _, k := range keys {
		if defs[k] {
			continue
		}
		if len(out) == maxUnknownKeys {
			break
		}
		u := unknownConfigKey{Key: k}
		if lookup != nil {
			if newKey, issueID, ok := lookup(k); ok {
				u.NewKey, u.IssueID = newKey, issueID
				out = append(out, u)
				continue
			}
		}
		u.Suggestion = nearestConfigKey(k, defs)
		out = append(out, u)
	}
	return out
}

// nearestConfigKey returns the defined key closest to key, or "" when none is
// close enough. The threshold keeps a wholly new key from getting a misleading
// suggestion: distance <= 3 and at most 40% of the key's length.
func nearestConfigKey(key string, defs map[string]bool) string {
	best, bestDist := "", 1<<30
	for _, d := range sortedKeySet(defs) {
		if dist := editDistance(strings.ToLower(key), strings.ToLower(d)); dist < bestDist {
			best, bestDist = d, dist
		}
	}
	if best == "" || bestDist > 3 || bestDist*5 > 2*len(key) {
		return ""
	}
	return best
}

// sortedKeySet returns a set's keys, sorted.
func sortedKeySet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// editDistance is Levenshtein distance over runes, bounded by the key length
// (keys are at most maxValidateKeyBytes, so this stays cheap).
func editDistance(a, b string) int {
	ar, br := []rune(a), []rune(b)
	if len(ar) == 0 {
		return len(br)
	}
	prev := make([]int, len(br)+1)
	cur := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ar); i++ {
		cur[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			cur[j] = min3(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(br)]
}

func min3(a, b, c int) int {
	if b < a {
		a = b
	}
	if c < a {
		a = c
	}
	return a
}

// validateData is the structured result. It holds no config value: only keys,
// redacted messages and suggestions.
type validateData struct {
	Class           string              `json:"class"`
	BaseConnector   string              `json:"base_connector,omitempty"`
	ErrorCount      int                 `json:"error_count"`
	Errors          map[string][]string `json:"errors,omitempty"`
	UnknownKeys     []unknownConfigKey  `json:"unknown_keys,omitempty"`
	ConfigKeys      int                 `json:"config_keys"`
	DefsUnavailable string              `json:"defs_unavailable,omitempty"`
}

// renderValidation redacts and caps Connect's messages and builds the summary.
// Connect can echo a value in a message, so every message is redacted, a
// message that contains a base connector credential is dropped whole (a short
// credential cannot be safely masked by substring), and each message and the
// total are capped.
func renderValidation(class, base string, cfg map[string]string, vr connect.ValidateResult, unknown []unknownConfigKey, defsErr error, secrets []string, lookup func(string) (string, string, bool)) (validateData, string, bool) {
	data := validateData{
		Class:         class,
		BaseConnector: base,
		ErrorCount:    vr.ErrorCount,
		ConfigKeys:    len(cfg),
		UnknownKeys:   unknown,
	}
	if defsErr != nil {
		data.DefsUnavailable = redactMessage(defsErr.Error(), nil)
	}

	keys := sortedKeys2(vr.Errors)
	budget := maxValidateMessageSum
	shownKeys := 0
	capped := false
	for _, k := range keys {
		msgs := vr.Errors[k]
		if len(msgs) == 0 {
			continue
		}
		if shownKeys == maxValidateErrorKeys || budget <= 0 {
			capped = true
			break
		}
		if len(msgs) > maxValidateKeyMessages {
			capped = true
		}
		out := make([]string, 0, min(len(msgs), maxValidateKeyMessages))
		for _, m := range msgs {
			if len(out) == maxValidateKeyMessages || budget <= 0 {
				capped = true
				break
			}
			m = redactMessage(m, secrets)
			if len(m) > maxValidateMessageSize {
				capped = true
				m = truncateRunes(m, maxValidateMessageSize)
			}
			if len(m) > budget {
				capped = true
				m = truncateRunes(m, budget)
			}
			if m == "" {
				continue
			}
			out = append(out, m)
			budget -= len(m)
		}
		if len(out) == 0 {
			continue
		}
		if data.Errors == nil {
			data.Errors = map[string][]string{}
		}
		data.Errors[k] = out
		shownKeys++
	}

	// A renamed key that Connect reported an error on names the known issue too
	// (the row's "an error on a renamed key names the Q1 known-issue id").
	if lookup != nil && data.Errors != nil {
		for k := range data.Errors {
			if len(data.UnknownKeys) == maxUnknownKeys {
				break
			}
			if newKey, issueID, ok := lookup(k); ok {
				data.UnknownKeys = append(data.UnknownKeys, unknownConfigKey{Key: k, NewKey: newKey, IssueID: issueID})
			}
		}
	}

	var sb strings.Builder
	if vr.ErrorCount == 0 {
		fmt.Fprintf(&sb, "0 errors in %s.", class)
	} else {
		fmt.Fprintf(&sb, "%d error", vr.ErrorCount)
		if vr.ErrorCount != 1 {
			sb.WriteString("s")
		}
		fmt.Fprintf(&sb, " in %s", class)
		if shownKeys < len(keys) {
			fmt.Fprintf(&sb, " (showing %d of %d keys)", shownKeys, len(keys))
		}
		sb.WriteString(":")
	}
	for _, k := range sortedKeys2(data.Errors) {
		appendLine(&sb, "  "+k+": "+strings.Join(data.Errors[k], "; "))
	}
	for _, u := range data.UnknownKeys {
		line := "unknown key " + quoteKey(u.Key)
		switch {
		case u.NewKey != "" && u.IssueID != "":
			line += ": renamed to " + quoteKey(u.NewKey) + " (known issue " + u.IssueID + ")"
		case u.NewKey != "":
			line += ": renamed to " + quoteKey(u.NewKey)
		case u.IssueID != "":
			line += " (known issue " + u.IssueID + ")"
		case u.Suggestion != "":
			line += ": did you mean " + quoteKey(u.Suggestion) + "?"
		default:
			line += ": not a known key of this plugin"
		}
		appendLine(&sb, line)
	}
	if data.DefsUnavailable != "" {
		appendLine(&sb, "plugin config definitions unavailable: "+data.DefsUnavailable)
	}
	return data, sb.String(), capped
}

// redactMessage masks credentials in one Connect message and drops the message
// whole when it literally contains a known credential (a short one cannot be
// substring-masked without garbling the rest).
func redactMessage(m string, secrets []string) string {
	for _, s := range secrets {
		if s != "" && strings.Contains(m, s) {
			return "invalid value (details redacted)"
		}
	}
	m, _ = redact.Text(m)
	return m
}

func quoteKey(k string) string { return "\"" + truncateRunes(k, 80) + "\"" }

// shortenText caps untrusted text before it is echoed in an error.
func shortenText(s string) string {
	const max = 80
	return truncateRunes(s, max)
}

// sortedKeys2 sorts a map's keys; kept local to avoid changing the package's
// sortedKeys signature for map[string][]string.
func sortedKeys2(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
