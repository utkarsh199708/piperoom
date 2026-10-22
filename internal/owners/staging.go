package owners

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// X8 staging (docs/api.md "X8 staging"): a pipeline's optional staging:
// {connections: ...} block and its require_staging: list. Everything here is
// pure; package wiring resolves the references, runs CheckStagingOverlap on
// the resolved values and builds the staging clients (actions.Staging) only
// when write mode is on.

// StagingSpec is a pipeline's staging: block.
type StagingSpec struct {
	// Connections holds the connect_*, schema_registry_* and kafka_* keys of
	// a connections: block (ConnectionKeys), with the same reference rules;
	// connect_url and schema_registry_url may also be a plain {value: URL}
	// without a login in it (StagingKeys).
	Connections Connections `yaml:"connections"`
}

// UnmarshalYAML refuses unknown keys, like the other blocks: a typo such as
// `connection:` would otherwise leave a staging: block that stages nothing.
func (s *StagingSpec) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: staging: want a mapping with a connections: block", node.Line)
	}
	if err := checkKeys(node, "staging", "connections"); err != nil {
		return err
	}
	type plain StagingSpec // no methods, so Decode does not recurse
	return node.Decode((*plain)(s))
}

// ErrStagingIsProduction wraps the refusal of a staging connect_url or
// schema_registry_url that names a production endpoint.
var ErrStagingIsProduction = errors.New("staging endpoint is a production endpoint")

// stagingURLKeys are the staging keys that must never name production.
// kafka_brokers is not one: staging Connect may share the production Kafka
// cluster with its own internal topics, and the staging Kafka client only
// reads.
var stagingURLKeys = []string{"connect_url", "schema_registry_url"}

// processVar is the process-wide variable of a staging URL key.
var processVar = map[string]string{"connect_url": "CONNECT_URL", "schema_registry_url": "SCHEMA_REGISTRY_URL"}

// isStagingKey reports whether a connections: key may appear in staging:.
// No source_db_* or warehouse_* key: no X8 action stages a database write.
func isStagingKey(k string) bool {
	if _, ok := connKeys[k]; !ok {
		return false
	}
	return strings.HasPrefix(k, "connect_") || strings.HasPrefix(k, "schema_registry_") || strings.HasPrefix(k, "kafka_")
}

// StagingKeys returns every key a staging: connections: block may hold,
// sorted.
func StagingKeys() []string {
	var out []string
	for _, k := range ConnectionKeys() {
		if isStagingKey(k) {
			out = append(out, k)
		}
	}
	return out
}

// HasStaging reports whether the pipeline declares a staging: block.
func (p Pipeline) HasStaging() bool { return p.Staging != nil && len(p.Staging.Connections) > 0 }

// RequireStagingList returns a copy of the pipeline's require_staging:
// actions in file order, never nil.
func (p *Pipeline) RequireStagingList() []string {
	if p == nil {
		return []string{}
	}
	return append([]string{}, p.RequireStaging...)
}

// stagingRefs are the canonical env: and file: references of the staging
// block (CA bundles left out, as in ConnRefs), for CheckSharedRefs.
func (p Pipeline) stagingRefs() []string {
	if p.Staging == nil {
		return nil
	}
	var out []string
	for k, r := range p.Staging.Connections {
		if IsCAKey(k) {
			continue
		}
		if c := canonRef(r); c != "" {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

// validateStaging checks every pipeline's staging: block and
// require_staging: list (shape only), then refuses a staging URL reference
// that is a production one: the same env: or file: reference as a
// connect_url or schema_registry_url of any pipeline's connections:, or the
// process-wide CONNECT_URL / SCHEMA_REGISTRY_URL variable. Two different
// references that hold the same URL are caught by wiring once resolved
// (CheckStagingOverlap).
func (o *Owners) validateStaging() error {
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		if err := p.validateStagingBlock(); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		if !p.HasStaging() {
			continue
		}
		for _, k := range stagingURLKeys {
			ref := canonRef(p.Staging.Connections[k])
			if ref == "" {
				continue
			}
			if other := o.productionRefOwner(ref); other != "" {
				return fmt.Errorf("pipeline %q: staging.connections.%s is the same reference as %s: staging must never be production: %w",
					p.ID, k, other, ErrStagingIsProduction)
			}
		}
	}
	return nil
}

// productionRefOwner names the production URL setting that uses ref, or ""
// when none does.
func (o *Owners) productionRefOwner(ref string) string {
	for _, k := range stagingURLKeys {
		if ref == refEnv(processVar[k]) {
			return "the process-wide " + processVar[k]
		}
	}
	for _, q := range o.Pipelines {
		for _, k := range stagingURLKeys {
			if r, ok := q.Connections[k]; ok && canonRef(r) == ref {
				return fmt.Sprintf("pipeline %q connections.%s", q.ID, k)
			}
		}
	}
	return ""
}

// validateStagingBlock checks the shape of one pipeline's staging: block and
// require_staging: list. Values are never read here and never quoted.
func (p *Pipeline) validateStagingBlock() error {
	if err := checkActionList("require_staging", p.RequireStaging); err != nil {
		return err
	}
	if p.Staging == nil {
		if len(p.RequireStaging) > 0 {
			return fmt.Errorf("require_staging: needs a staging: block (nothing could ever be staged, so no step could be approved)")
		}
		return nil
	}
	if p.isBatch() {
		return fmt.Errorf("staging: is not supported for kind %s (no X8 action stages a batch system); remove the block", p.KindOrDefault())
	}
	c := p.Staging.Connections
	if len(c) == 0 {
		return fmt.Errorf("staging: needs a connections: block with connect_url or schema_registry_url")
	}
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !isStagingKey(k) {
			return fmt.Errorf("staging.connections: key %s is not allowed (allowed: %s)", quoteName(k), strings.Join(StagingKeys(), ", "))
		}
		info := connKeys[k]
		r := c[k]
		if r.Value != "" && (k == "connect_url" || k == "schema_registry_url") {
			// A URL is not a secret unless it carries a login; the docs'
			// example writes the staging URLs as values.
			if err := checkStagingURLValue(k, r); err != nil {
				return err
			}
			info.literal = true
		}
		if err := checkRef("staging.connections", k, info, r); err != nil {
			return err
		}
	}
	_, conn := c["connect_url"]
	_, reg := c["schema_registry_url"]
	if !conn && !reg {
		return fmt.Errorf("staging.connections: set connect_url or schema_registry_url (or both); kafka_brokers alone stages nothing")
	}
	// A login or TLS setting without its system's address would be dropped
	// without a word: refuse it so the block says what it does.
	for _, k := range keys {
		for _, sys := range []struct{ prefix, addr string }{
			{"connect_", "connect_url"}, {"schema_registry_", "schema_registry_url"}, {"kafka_", "kafka_brokers"},
		} {
			if strings.HasPrefix(k, sys.prefix) && k != sys.addr {
				if _, ok := c[sys.addr]; !ok {
					return fmt.Errorf("staging.connections.%s needs staging.connections.%s", k, sys.addr)
				}
			}
		}
	}
	return nil
}

// checkStagingURLValue accepts a {value: URL} for a staging URL key only
// when it is an http or https URL with a host and without a user or
// password. The value is never quoted.
func checkStagingURLValue(k string, r ConnRef) error {
	if r.Env != "" || r.File != "" {
		return nil // checkRef refuses more than one source
	}
	u, err := url.Parse(strings.TrimSpace(r.Value))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("staging.connections.%s: value: must be an http or https URL with a host (value not shown)", k)
	}
	if u.User != nil {
		return fmt.Errorf("staging.connections.%s: value: must not carry a login; put the password in %s and the URL without it", k, strings.TrimSuffix(k, "_url")+"_password")
	}
	return nil
}

// checkActionList checks a list of action names (require_staging:) like
// auto_approve: every entry is a snake_case action name and none repeats.
func checkActionList(field string, entries []string) error {
	for _, a := range entries {
		if !validActionName(a) {
			return fmt.Errorf("%s: %s is not an action name (want lowercase letters, digits and underscores, starting with a letter)", field, quoteName(a))
		}
	}
	if d := firstDuplicate(entries); d != "" {
		return fmt.Errorf("%s lists %q twice", field, d)
	}
	return nil
}

// ValidateRequireStaging refuses a require_staging: entry that is not in
// stagerNames: the names of the registered actions that implement
// actions.Stager for the pipeline's client set. As for ValidateAutoApprove,
// a nil or empty set refuses every entry, and the caller passes the
// proposal registry (every action, whatever the write mode), never an
// executor registry.
func (o *Owners) ValidateRequireStaging(stagerNames []string) error {
	if o == nil {
		return nil
	}
	known := make(map[string]bool, len(stagerNames))
	for _, n := range stagerNames {
		known[n] = true
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		for _, a := range p.RequireStaging {
			if !known[a] {
				return fmt.Errorf("owners: pipeline %q: require_staging: %q is not an action that can be staged (stageable: %s)", p.ID, a, knownList(known))
			}
		}
	}
	return nil
}

// StagingEndpoints are resolved URL values for CheckStagingOverlap, keyed by
// connection key (connect_url, schema_registry_url). Values are compared
// after normalisation and never returned or quoted.
type StagingEndpoints struct {
	// Process is the process-wide CONNECT_URL and SCHEMA_REGISTRY_URL.
	Process map[string]string
	// Production holds, per pipeline id, the resolved connections: values of
	// the pipelines with a connections: block. A pipeline left out (its
	// references could not be resolved here) is compared by reference only
	// (Load).
	Production map[string]map[string]string
	// Staging holds, per pipeline id, the resolved staging.connections:
	// values. A pipeline left out falls back to its {value:} literals.
	Staging map[string]map[string]string
}

// CheckStagingOverlap refuses a staging connect_url or schema_registry_url
// that resolves to the same endpoint as a production connect_url or
// schema_registry_url: the process-wide settings, or any pipeline's
// connections:. Endpoints are compared as normalised host:port (scheme
// default ports, lower-case host, no path or login), so http://connect and
// http://connect:80 are one endpoint, and so are two paths on one host. The
// error names both settings by pipeline id and key, never a value, and wraps
// ErrStagingIsProduction.
func (o *Owners) CheckStagingOverlap(ep StagingEndpoints) error {
	if o == nil {
		return nil
	}
	for _, p := range o.Pipelines {
		if !p.HasStaging() {
			continue
		}
		for _, k := range stagingURLKeys {
			v, ok := ep.Staging[p.ID][k]
			if !ok {
				v = p.Staging.Connections[k].Value
			}
			hp := hostPort(v, defaultPort(k))
			if hp == "" {
				continue
			}
			if other := o.productionEndpointOwner(hp, ep); other != "" {
				return fmt.Errorf("pipeline %q: staging.connections.%s resolves to the same endpoint as %s (values not shown): staging must never be production: %w",
					p.ID, k, other, ErrStagingIsProduction)
			}
		}
	}
	return nil
}

// productionEndpointOwner names the production URL setting whose endpoint is
// hp, or "" when none is.
func (o *Owners) productionEndpointOwner(hp string, ep StagingEndpoints) string {
	for _, k := range stagingURLKeys {
		if hostPort(ep.Process[k], defaultPort(k)) == hp {
			return "the process-wide " + processVar[k]
		}
	}
	for _, q := range o.Pipelines {
		vals, ok := ep.Production[q.ID]
		if !ok {
			continue
		}
		for _, k := range stagingURLKeys {
			if hostPort(vals[k], defaultPort(k)) == hp {
				return fmt.Sprintf("pipeline %q connections.%s", q.ID, k)
			}
		}
	}
	return ""
}
