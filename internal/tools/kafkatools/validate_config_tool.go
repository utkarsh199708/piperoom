package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"

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
			"description": "connector configuration key -> value. Credential-looking keys are refused in plain text; use a ${secret:name} reference, or omit them and pass base_connector so the server fills them in. ${file:...} and ${env:...} references are refused: Connect could resolve one and echo a server file or environment value back.",
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
	addConnectionSourceHosts(allowedHosts, sc, "source_db_url")
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
			return fmt.Errorf("%w: %s looks like a credential; pass a ${secret:...} reference, or omit it and pass base_connector so the server fills it in", tools.ErrBadInput, k)
		}
		// Only the named-secret-store form is accepted from the model.
		// Connect may resolve a provider reference server-side and echo the
		// resolved value in a validation message, so a model-chosen
		// ${file:path} or ${env:NAME} would read an arbitrary server file or
		// environment variable back to the model, whatever key it sits under.
		// Server-side configs (base_connector) may hold any provider form.
		if isConfigReference(v) && !isSecretReference(v) {
			return fmt.Errorf("%w: %s uses a ${file:...} or ${env:...} reference, which could echo a server-side file or environment value in a validation message; only ${secret:...} is accepted from the model", tools.ErrBadInput, k)
		}
		// A reference is only accepted where a credential belongs, for the
		// same echo reason. A host key's reference is refused by the host
		// guard with its own message.
		if isConfigReference(v) && !referenceAllowedKey(k) {
			return fmt.Errorf("%w: %s is not a credential key; a ${...} config reference is only accepted on a credential key, so a validation message cannot echo a server-side value", tools.ErrBadInput, k)
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

// looksSecretKey is the rule of actions.LooksSecret, mirrored here because
// internal/actions imports kafkatools (config versions), so importing it here
// would be a cycle. The two must stay in step until a leaf package owns the
// rule (Q0/U1-23); a test in this file pins the fragments.
func looksSecretKey(key string) bool {
	k := strings.ToLower(key)
	for _, s := range secretKeySubstrings {
		if strings.Contains(k, s) {
			return true
		}
	}
	segs := strings.FieldsFunc(k, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	})
	for _, seg := range segs {
		if seg == "key" {
			return true
		}
	}
	return len(segs) > 0 && slices.Contains(secretKeyLastSegments, segs[len(segs)-1])
}

var secretKeySubstrings = []string{"password", "passwd", "secret", "token", "credential", "jaas", "apikey", "private", "connectionstring"}

// secretKeyLastSegments mirrors actions.secretKeyLastSegments.
var secretKeyLastSegments = []string{"authorization", "seed", "jwt"}

// plaintextSecretKey reports whether a config key names a credential under the
// shared looksSecret rule. looksSecret also flags the converter class
// keys, because they contain a bare "key" segment; a converter class is not a
// credential, so those stay usable (the rule's doc expects each caller's
// allow-list to do exactly this).
func plaintextSecretKey(k string) bool {
	if !looksSecretKey(k) {
		return false
	}
	lk := strings.ToLower(k)
	for _, prefix := range []string{"key.converter", "value.converter", "header.converter"} {
		if lk == prefix || strings.HasPrefix(lk, prefix+".") {
			return false
		}
	}
	// Common connector keys whose bare "key" segment means the record key,
	// not a credential: Elasticsearch's key.ignore, Debezium's
	// message.key.columns, the *.key.format / *.key.fields family and the JDBC
	// sink's primary.key.mode / primary.key.fields. Their
	// values are booleans and column lists; refusing them would make the tool
	// unusable for ordinary configs.
	for _, suffix := range []string{"key.ignore", "message.key.columns", "key.format", "key.fields", "primary.key.mode", "primary.key.fields"} {
		if lk == suffix || strings.HasSuffix(lk, "."+suffix) {
			return false
		}
	}
	return true
}

// referenceAllowedKey reports whether a ${...} config reference may be passed
// under key k: a credential-looking key (where a reference is the only
// accepted form), a converter key (key./value./header.converter.* carry
// schema-registry credentials such as basic.auth.user.info), or a host key
// (the host guard then refuses it with its literal-host message).
func referenceAllowedKey(k string) bool {
	if looksSecretKey(k) || isHostConfigKey(k) {
		return true
	}
	lk := strings.ToLower(k)
	for _, prefix := range []string{"key.converter", "value.converter", "header.converter"} {
		if lk == prefix || strings.HasPrefix(lk, prefix+".") {
			return true
		}
	}
	return false
}

// isConfigReference reports whether v is a Connect config provider reference.
// ${secret:...} is accepted as a non-secret value (U0); ${file:...} and
// ${env:...} are the other documented providers (design, Q5c), recognized so
// they can be refused from the model (see checkValidateInput).
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

// isSecretReference reports whether v is the named-secret-store form, the only
// provider reference the model may send.
func isSecretReference(v string) bool {
	v = strings.TrimSpace(v)
	return strings.HasPrefix(v, "${secret:") && strings.HasSuffix(v, "}") && len(v) > len("${secret:}")
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
		// The base value wins even when the model sent a reference — but an
		// EMPTY base value must not clobber the model's ${secret:...}
		// reference with "", which would make Connect report a spurious
		// missing credential.
		if v != "" {
			secrets = append(secrets, v)
			merged[k] = v
		}
	}
	return secrets
}

// addConfigHosts adds the hosts named by a (server-side) config's host keys.
// Every host of a multi-host value (a failover JDBC URL, a bootstrap server
// list) is added; an entry that is not a host is skipped. A credential-named
// key is skipped even when it matches a host fragment (jdbc.password): its
// value is a secret, and nothing a refusal message might name may hold one.
func addConfigHosts(set map[string]bool, cfg map[string]string) {
	for k, v := range cfg {
		if !isHostConfigKey(k) || plaintextSecretKey(k) {
			continue
		}
		for _, h := range configHosts(v) {
			if h != "" && validHostRe.MatchString(h) {
				set[h] = true
			}
		}
	}
}

// addConnectionSourceHosts adds the hosts of the pipeline's literal
// connections.source_db_url. A {env:} or {file:} reference is not resolved
// here: the tool has no config and a reference is not a host. A provisional
// pipeline writes a {value:} DSN, which is enough for the setup path.
func addConnectionSourceHosts(set map[string]bool, sc tools.Scope, key string) {
	r, ok := sc.Pipeline().Connections[key]
	if !ok {
		return
	}
	for _, h := range configHosts(r.Value) {
		if h != "" && validHostRe.MatchString(h) {
			set[h] = true
		}
	}
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

// isHostConfigKey reports whether a config key names a host (or host list)
// Connect may be asked to reach. It errs towards true: "host" covers
// hostname/host/hosts (Debezium, MongoDB), "jdbc" and "connection.string" the
// JDBC and MongoDB DSNs, "bootstrap.servers" the Kafka endpoints a connector
// reaches itself (database.history.kafka.bootstrap.servers), and the suffix
// forms cover connection.url/urls, connection.uri (the official MongoDB
// connector), schema.registry.url, http.api.url, endpoints and addresses. The
// guard is symmetric: addConfigHosts uses the same rule to collect the
// allowed hosts from the server-side configs, so a key this flags is only
// usable with a value whose hosts the pipeline already reaches.
func isHostConfigKey(k string) bool {
	lk := strings.ToLower(k)
	// ssl.endpoint.identification.algorithm (and its Debezium producer/
	// consumer override forms) holds "https"/"" — a TLS mode, not a host.
	// Without this exemption every TLS-hardened config would be refused with
	// "points at https".
	if strings.HasSuffix(lk, "ssl.endpoint.identification.algorithm") {
		return false
	}
	switch {
	case strings.Contains(lk, "host"):
		return true
	case strings.Contains(lk, "jdbc"):
		return true
	case strings.Contains(lk, "connection.string"):
		return true
	case strings.Contains(lk, "bootstrap.servers"):
		return true
	case strings.Contains(lk, "endpoint"):
		return true
	}
	for _, s := range []string{"url", "urls", "uri", "uris", "address", "addresses", "servers", "dsn", "proxy"} {
		if lk == s || strings.HasSuffix(lk, "."+s) {
			return true
		}
	}
	return false
}

// configHosts reduces a host value, a DSN/JDBC URL or a host list to its
// normalized host names, one per list entry. Ports are dropped: the guard is
// "never reach another host", and the same host on another port is still the
// same host. The authority is isolated (everything after the first '/', '?'
// or '#' is cut) BEFORE list splitting and userinfo stripping, so a path or
// query that contains '@' cannot pose as the host
// (jdbc:postgresql://evil/x@allowed must reduce to "evil"). Every ','- or
// ';'-separated entry is returned, so a failover/load-balance URL or a
// bootstrap list cannot hide a second host behind an allowed first one; an
// entry that is not a host (jdbc:sqlserver's serverName=... property) stays in
// the result unnormalized and fails the caller's validHostRe check, which
// fails closed. A config provider reference returns nil (the caller treats it
// as unknown, never as a host).
func configHosts(v string) []string {
	v = strings.TrimSpace(v)
	if v == "" || isConfigReference(v) {
		return nil
	}
	if i := strings.Index(v, "://"); i >= 0 {
		v = v[i+3:]
	}
	if i := strings.IndexAny(v, "/?#"); i >= 0 {
		v = v[:i]
	}
	var hosts []string
	for _, e := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ';' }) {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		// userinfo, within this authority entry only
		if i := strings.LastIndex(e, "@"); i >= 0 {
			e = e[i+1:]
		}
		if h, _, err := net.SplitHostPort(e); err == nil {
			e = h
		}
		e = strings.ToLower(strings.Trim(e, "[]"))
		e = strings.TrimSuffix(e, ".")
		if e == "" {
			continue
		}
		hosts = append(hosts, e)
	}
	return hosts
}

var validHostRe = regexp.MustCompile(`^[a-z0-9._:\[\]-]+$`)

// checkConfigHosts refuses a host key whose value names any host that is not
// the pipeline's source host or the base connector's, and a host key whose
// value is not a literal host. Every host of a multi-host value must be
// allowed, so a failover URL cannot hide a second host behind an allowed
// first one. Only the model's keys are checked; a value inherited unchanged
// from the server-side base config is trusted, and a model key whose
// effective value the base connector overrode (a credential-looking host key)
// is server-side too, so it is skipped and no fragment of it is ever echoed
// in an error. It runs before the validate request, so Connect is never asked
// to reach an unapproved host.
func checkConfigHosts(model, merged map[string]string, allowed map[string]bool) error {
	keys := make([]string, 0, len(model))
	for k := range model {
		if isHostConfigKey(k) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if merged[k] != model[k] {
			continue // overridden server-side by the base connector
		}
		v := strings.TrimSpace(merged[k])
		if v == "" {
			continue // Connect reports the missing required value
		}
		if isConfigReference(v) {
			return fmt.Errorf("%w: host key %s must be a literal host, not a config reference", tools.ErrBadInput, k)
		}
		hosts := configHosts(v)
		if len(hosts) == 0 {
			return fmt.Errorf("%w: host key %s does not name a host", tools.ErrBadInput, k)
		}
		for _, h := range hosts {
			if !validHostRe.MatchString(h) {
				return fmt.Errorf("%w: host key %s does not reduce to host names (every host of a multi-host value must be a plain host[:port])", tools.ErrBadInput, k)
			}
			if len(allowed) == 0 {
				return fmt.Errorf("%w: host key %s points at %s, but the pipeline has no known source host; pass base_connector so its host can be checked", tools.ErrOutOfScope, k, h)
			}
			// The refusal names only the model's own key and host: listing the
			// allowed set would hand out the pipeline's internal hostnames
			// (and whatever a misnamed server-side key holds) in one call.
			if !allowed[h] {
				return fmt.Errorf("%w: host key %s points at %s, which is neither the pipeline's source host nor the base connector's", tools.ErrOutOfScope, k, h)
			}
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
		// An endpoint-less Connect answers 404 or 405 (see PluginConfigDef).
		if errors.Is(err, clients.ErrNotFound) || errors.Is(err, clients.ErrNotSupported) {
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
// through lookup first; otherwise the nearest defined key is suggested. With no
// definitions (an older Connect) rename lookups still run — they need no defs —
// but unknown-key detection cannot.
func unknownConfigKeys(cfg map[string]string, defs map[string]bool, lookup func(string) (string, string, bool)) []unknownConfigKey {
	keys := sortedKeys(cfg)
	var out []unknownConfigKey
	if len(defs) == 0 {
		if lookup == nil {
			return nil
		}
		for _, k := range keys {
			if len(out) == maxUnknownKeys {
				break
			}
			if newKey, issueID, ok := lookup(k); ok {
				out = append(out, unknownConfigKey{Key: k, NewKey: newKey, IssueID: issueID})
			}
		}
		return out
	}
	sortedDefs := sortedKeySet(defs)
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
		u.Suggestion = nearestConfigKey(k, sortedDefs)
		out = append(out, u)
	}
	return out
}

// maxSuggestionDefs caps the definitions a suggestion scan considers: the def
// list is server data, and a hostile or buggy Connect must not be able to buy
// unbounded CPU with a huge one. Real plugins define a few hundred keys.
const maxSuggestionDefs = 2048

// nearestConfigKey returns the defined key closest to key, or "" when none is
// close enough. The threshold keeps a wholly new key from getting a misleading
// suggestion: distance <= 3 and at most 40% of the key's length. Definitions
// whose length differs from the key's by more than the threshold are skipped
// before the quadratic distance runs (|len difference| is a lower bound on the
// distance), so a server-supplied def list cannot make this scan expensive.
func nearestConfigKey(key string, sortedDefs []string) string {
	key = strings.ToLower(key)
	best, bestDist := "", 1<<30
	defs := sortedDefs
	if len(defs) > maxSuggestionDefs {
		defs = defs[:maxSuggestionDefs]
	}
	for _, d := range defs {
		if diff := len(d) - len(key); diff > 3 || diff < -3 {
			continue
		}
		if dist := editDistance(key, strings.ToLower(d)); dist < bestDist {
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
		data.DefsUnavailable = redactMessage(defsErr.Error(), secrets)
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
	// Keys already listed as unknown are not listed twice, and the error keys
	// are walked in order so the output is deterministic.
	if lookup != nil && data.Errors != nil {
		listed := make(map[string]bool, len(data.UnknownKeys))
		for _, u := range data.UnknownKeys {
			listed[u.Key] = true
		}
		for _, k := range sortedKeys2(data.Errors) {
			if len(data.UnknownKeys) == maxUnknownKeys {
				break
			}
			if listed[k] {
				continue
			}
			if newKey, issueID, ok := lookup(k); ok {
				data.UnknownKeys = append(data.UnknownKeys, unknownConfigKey{Key: k, NewKey: newKey, IssueID: issueID})
				listed[k] = true
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
// whole when it contains a known credential (a short one cannot be
// substring-masked without garbling the rest), literally or in the URL-encoded
// form a message quoting a connection URL would carry.
func redactMessage(m string, secrets []string) string {
	for _, s := range secrets {
		if s == "" {
			continue
		}
		for _, enc := range []string{s, url.QueryEscape(s), url.PathEscape(s)} {
			if strings.Contains(m, enc) {
				return "invalid value (details redacted)"
			}
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
