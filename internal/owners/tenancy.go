package owners

import (
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
)

// Tenancy (docs/design/tenancy.md): the tenants: block, a pipeline's tenant
// and its connections: block. Everything here is pure; package wiring resolves
// the connection references at start.

// DefaultTenant is the tenant of a pipeline that names none.
const DefaultTenant = "default"

// tenantNameRE is the pipeline-id charset: a tenant name ends up in logs and
// error texts next to pipeline ids.
var tenantNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// repoIDRE is the repo-id charset (a slug, like the pipeline ids it sits next
// to in errors and the PR tool's repo: field).
var repoIDRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// envNameRE is a portable environment variable name.
var envNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// shownNameRE is what an error may quote from owners.yaml: a key, a tenant or
// a setting name. Anything else (a DSN or password pasted in the wrong place,
// as a key or as a value) is not echoed into the start-up log.
var shownNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// quoteName returns s quoted when it looks like a name, else a placeholder.
func quoteName(s string) string {
	if shownNameRE.MatchString(s) {
		return strconv.Quote(s)
	}
	return "(a value that is not a plain name; not shown)"
}

// Tenant is one entry of the top-level tenants: block.
type Tenant struct {
	// Members see every room of the tenant's pipelines. The owners of those
	// pipelines' layers are members too, without being listed (InTenant).
	Members Principals `yaml:"members"`
	// Admins approve the T2/T3 steps of a setup room whose provisional
	// pipeline nobody owns yet, and a pipeline_register proposal on a team
	// server, with grant "admin" (events.GrantAdmin; docs/design/setup-agent.md
	// §3.6). Principals, exactly like members:. May be empty: a tenant with no
	// admins relies on an overlapping pipeline's owners, or solo mode.
	Admins Principals `yaml:"admins"`
	// Repos are the repo ids of the tenant's default repos: the setup form's
	// repo list falls back to them when the person names none (§3.10). A repo
	// id is a plain slug; the repo's url and globs live in the pipeline's
	// repos: block (Q7a).
	Repos []string `yaml:"repos"`
}

// UnmarshalYAML rejects unknown keys, like the other blocks: a typo such as
// `member:` would otherwise leave the tenant with owners only.
func (t *Tenant) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "tenants", "members", "admins", "repos"); err != nil {
		return err
	}
	type plain Tenant // no methods, so Decode does not recurse
	return node.Decode((*plain)(t))
}

// ConnRef names where one connection setting comes from. Exactly one field is
// set. owners.yaml never holds a secret: Env and File name where it is, and
// Value is allowed only for the few non-secret keys (connKeys).
type ConnRef struct {
	// Env is the name of an environment variable that holds the value.
	Env string `yaml:"env"`
	// File is an absolute path. For a path key (the TLS *_file keys) it is the
	// file itself; otherwise the value is the file's trimmed content.
	File string `yaml:"file"`
	// Value is a literal, for non-secret keys only.
	Value string `yaml:"value"`
}

// UnmarshalYAML accepts only the {env|file|value: ...} mapping. A plain string
// is refused without quoting it: it is most likely a DSN or password pasted
// into owners.yaml, and the decoder's own error would echo it into the log.
func (r *ConnRef) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: connections: want {env: NAME}, {file: /path} or {value: X}; a plain value is refused (never put a secret in owners.yaml)", node.Line)
	}
	if err := checkKeys(node, "connections", "env", "file", "value"); err != nil {
		return err
	}
	for i := 1; i < len(node.Content); i += 2 {
		if v := node.Content[i]; v.Kind != yaml.ScalarNode {
			return fmt.Errorf("line %d: connections: %s must be a string", v.Line, quoteName(node.Content[i-1].Value))
		}
	}
	type plain ConnRef
	return node.Decode((*plain)(r))
}

// Connections is a pipeline's connections: block: setting key -> reference.
type Connections map[string]ConnRef

// UnmarshalYAML requires a mapping and does not echo a scalar (see ConnRef).
func (c *Connections) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: connections: want a mapping of setting names to {env|file|value} references", node.Line)
	}
	m := map[string]ConnRef{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		k := node.Content[i]
		if k.Kind != yaml.ScalarNode {
			return fmt.Errorf("line %d: connections: a key must be a setting name", k.Line)
		}
		if _, dup := m[k.Value]; dup {
			return fmt.Errorf("line %d: connections: %s is set twice", k.Line, quoteName(k.Value))
		}
		var r ConnRef
		if err := r.UnmarshalYAML(node.Content[i+1]); err != nil {
			return err
		}
		m[k.Value] = r
	}
	*c = m
	return nil
}

// connKey describes one key of a connections: block.
type connKey struct {
	// kind is the only pipeline kind whose block may set the key (a cdc
	// system a batch pipeline does not have, or the reverse); "" = every kind.
	kind string
	// required: every connections: block of a pipeline the key belongs to
	// (kind) must set it, so a batch block needs only the batch keys.
	required bool
	literal  bool // {value: X} allowed: the setting is never a secret
	path     bool // the value is a file path (TLS material)
	// ca: a CA bundle path. It is public material and a shared corporate CA
	// is normal, so it is not a connection reference (CheckSharedRefs).
	ca bool
}

// connKeys are the lower-case names of the process-wide settings a pipeline
// can replace (internal/config/config.go). wiring maps each to its field; a
// test there keeps the two lists equal.
//
// The warehouse keys belong to both kinds; every other key today is a cdc
// one. The Airflow and dbt keys of a batch pipeline join when wiring maps
// them (P4b-1), so the two lists stay equal.
var connKeys = map[string]connKey{
	// source_db_write_url is the source Postgres as its separate write role
	// (SOURCE_DB_WRITE_URL), for the pg write actions. Optional: without it
	// the pipeline gets no writer and those actions refuse at Execute. Never
	// a literal (a DSN with a password), and never the same reference as
	// source_db_url (checkWriteRole).
	"source_db_url":                 {kind: cdc, required: true},
	"source_db_write_url":           {kind: cdc},
	"warehouse_db_url":              {required: true},
	"warehouse_kind":                {literal: true},
	"kafka_brokers":                 {kind: cdc, required: true},
	"kafka_security_protocol":       {kind: cdc, literal: true},
	"kafka_sasl_mechanism":          {kind: cdc, literal: true},
	"kafka_sasl_username":           {kind: cdc},
	"kafka_sasl_password":           {kind: cdc},
	"kafka_tls_ca_file":             {kind: cdc, path: true, ca: true},
	"kafka_tls_cert_file":           {kind: cdc, path: true},
	"kafka_tls_key_file":            {kind: cdc, path: true},
	"kafka_tls_server_name":         {kind: cdc, literal: true},
	"connect_url":                   {kind: cdc, required: true},
	"connect_username":              {kind: cdc},
	"connect_password":              {kind: cdc},
	"connect_tls_ca_file":           {kind: cdc, path: true, ca: true},
	"connect_tls_cert_file":         {kind: cdc, path: true},
	"connect_tls_key_file":          {kind: cdc, path: true},
	"schema_registry_url":           {kind: cdc, required: true},
	"schema_registry_username":      {kind: cdc},
	"schema_registry_password":      {kind: cdc},
	"schema_registry_tls_ca_file":   {kind: cdc, path: true, ca: true},
	"schema_registry_tls_cert_file": {kind: cdc, path: true},
	"schema_registry_tls_key_file":  {kind: cdc, path: true},
}

// cdc is events.PipelineKindCDC, short for the table above.
const cdc = events.PipelineKindCDC

// belongsTo reports whether a pipeline of kind may set the key.
func (k connKey) belongsTo(kind string) bool { return k.kind == "" || k.kind == kind }

// ConnectionKeys returns every key a connections: block may hold, sorted.
func ConnectionKeys() []string {
	out := make([]string, 0, len(connKeys))
	for k := range connKeys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// IsPathKey reports whether the key's value is a file path (the TLS *_file
// keys): a {file: p} reference then means the path p itself.
func IsPathKey(key string) bool { return connKeys[key].path }

// IsCAKey reports whether the key is a TLS CA bundle (*_tls_ca_file). A CA is
// public and often shared by every team, so it never counts as a shared
// connection reference.
func IsCAKey(key string) bool { return connKeys[key].ca }

// IsLiteralKey reports whether the key is one of the non-secret settings that
// may be written as {value: X}.
func IsLiteralKey(key string) bool { return connKeys[key].literal }

// FileRef is the canonical connection reference of a file path (see
// ConnRefs): cleaned, lower case, and with forward slashes, so the same file
// gives the same reference on every OS (Windows paths ignore case, and an
// owners.yaml written on one OS must compare equal on another).
func FileRef(path string) string {
	return "file:" + strings.ToLower(filepath.ToSlash(filepath.Clean(path)))
}

// TenantName returns the pipeline's tenant: its tenant: value, or DefaultTenant.
func (p Pipeline) TenantName() string {
	if p.Tenant == "" {
		return DefaultTenant
	}
	return p.Tenant
}

// HasConnections reports whether the pipeline has its own connections: block.
// Without one it uses the process-wide settings.
func (p Pipeline) HasConnections() bool { return len(p.Connections) > 0 }

// kafkaClusterKey names the Kafka cluster the pipeline reaches, as far as
// owners.yaml can tell without reading a value: "process" for the
// process-wide KAFKA_BROKERS, else the pipeline's kafka_brokers reference.
// Topic names are unique per cluster, so checks on topic names (validateDLQ)
// compare them within one key. Two references that hold the same brokers look
// like two clusters here; that check is by name, like CheckSharedRefs.
func (p Pipeline) kafkaClusterKey() string {
	if !p.HasConnections() {
		return "process"
	}
	r := p.Connections["kafka_brokers"]
	switch {
	case r.Env != "":
		return refEnv(r.Env)
	case r.File != "":
		return FileRef(r.File)
	}
	return "pipeline:" + p.ID
}

// TenancyOn reports whether owners.yaml has a tenants: block, i.e. whether the
// tenant filter is on. Without one every signed-in user sees every room.
func (o *Owners) TenancyOn() bool { return o != nil && len(o.Tenants) > 0 }

// TenantNames returns the declared tenants, sorted.
func (o *Owners) TenantNames() []string {
	if o == nil {
		return nil
	}
	out := make([]string, 0, len(o.Tenants))
	for t := range o.Tenants {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// TenantOf returns the tenant of a known pipeline. ok is false for a pipeline
// that owners.yaml does not define: such a room belongs to no tenant.
func (o *Owners) TenantOf(pipelineID string) (string, bool) {
	if o == nil {
		return "", false
	}
	p, ok := o.Pipeline(pipelineID)
	if !ok {
		return "", false
	}
	return p.TenantName(), true
}

// InTenant reports whether the session belongs to the tenant: it matches a
// principal of the tenant's members:, or it may approve (as approver or
// backup) any layer of one of the tenant's pipelines. users.yaml, auditors:
// and viewer groups give no membership.
func (o *Owners) InTenant(tenant, userID string, groups []string) bool {
	if o == nil {
		return false
	}
	if t, ok := o.Tenants[tenant]; ok {
		if _, ok := t.Members.Matches(userID, groups); ok {
			return true
		}
	}
	for i := range o.Pipelines {
		if o.Pipelines[i].TenantName() != tenant {
			continue
		}
		for _, layer := range events.Layers {
			if _, ok := o.CanApprove(o.Pipelines[i].ID, layer, userID, groups); ok {
				return true
			}
		}
	}
	return false
}

// IsTenantAdmin reports whether the session is named in tenants.<t>.admins:.
// A tenant admin may approve a setup room's T2/T3 steps and its
// pipeline_register with grant "admin" (docs/design/setup-agent.md §3.6).
func (o *Owners) IsTenantAdmin(tenant, userID string, groups []string) bool {
	if o == nil {
		return false
	}
	t, ok := o.Tenants[tenant]
	if !ok {
		return false
	}
	_, ok = t.Admins.Matches(userID, groups)
	return ok
}

// CanSeePipeline reports whether the session may see the rooms of the
// pipeline. With the filter off (no tenants: block) the answer is always yes.
// With it on, a pipeline owners.yaml does not define is in no tenant and is
// seen by nobody.
func (o *Owners) CanSeePipeline(pipelineID, userID string, groups []string) bool {
	if !o.TenancyOn() {
		return true
	}
	t, ok := o.TenantOf(pipelineID)
	if !ok {
		return false
	}
	return o.InTenant(t, userID, groups)
}

// VisiblePipelines returns the ids of the pipelines whose rooms the session
// may see, for filtering a list in one pass. all is true when the filter is
// off; the set is then nil.
func (o *Owners) VisiblePipelines(userID string, groups []string) (set map[string]bool, all bool) {
	if !o.TenancyOn() {
		return nil, true
	}
	in := map[string]bool{}
	set = map[string]bool{}
	for _, p := range o.Pipelines {
		t := p.TenantName()
		member, seen := in[t]
		if !seen {
			member = o.InTenant(t, userID, groups)
			in[t] = member
		}
		if member {
			set[p.ID] = true
		}
	}
	return set, false
}

// InheritedLayers returns the layers of a pipeline whose owners come from the
// top-level layers: block because the pipeline's owners: block does not set
// them, in events.Layers order. With the tenant filter on, those owners are in
// this pipeline's tenant (InTenant), so the server warns about each.
func (o *Owners) InheritedLayers(pipelineID string) []string {
	p, ok := o.Pipeline(pipelineID)
	if !ok {
		return nil
	}
	var out []string
	for _, l := range events.Layers {
		if _, over := p.Owners[l]; over {
			continue
		}
		if len(o.Layers[l]) > 0 {
			out = append(out, l)
		}
	}
	return out
}

// Only returns a copy of o that holds only the named pipelines (unknown ids
// are skipped). Layers, auditors and tenants are shared with o and must not be
// changed. wiring gives each client set such a view, so a tool built over one
// pipeline's clients cannot resolve another pipeline's id.
func (o *Owners) Only(ids ...string) *Owners {
	if o == nil {
		return nil
	}
	view := *o
	view.Pipelines = nil
	for _, p := range o.Pipelines {
		if slices.Contains(ids, p.ID) {
			view.Pipelines = append(view.Pipelines, p)
		}
	}
	return &view
}

// ProcessRefs are the references of a pipeline with no connections: block: it
// uses the process-wide variables, one per connection key except the CA
// bundles (IsCAKey).
func ProcessRefs() []string {
	var out []string
	for _, k := range ConnectionKeys() {
		if !IsCAKey(k) {
			out = append(out, refEnv(strings.ToUpper(k)))
		}
	}
	return out
}

// ConnRefs returns the canonical references the pipeline uses, sorted:
// "env:NAME" (upper case: Windows variable names ignore case) and
// "file:<path>" (cleaned, lower case, for the same reason on Windows paths).
// A literal is not a reference, and neither is a CA bundle (IsCAKey). A
// pipeline with no connections: block uses ProcessRefs. wiring adds the
// resolved paths of the TLS key and certificate files, wherever they came
// from, once the references are read.
func (p Pipeline) ConnRefs() []string {
	if !p.HasConnections() {
		return ProcessRefs()
	}
	set := map[string]bool{}
	for k, r := range p.Connections {
		if IsCAKey(k) {
			continue
		}
		switch {
		case r.Env != "":
			set[refEnv(r.Env)] = true
		case r.File != "":
			set[FileRef(r.File)] = true
		}
	}
	out := make([]string, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

func refEnv(name string) string { return "env:" + strings.ToUpper(name) }

// CheckSharedRefs fails when one reference is used by pipelines of two or more
// tenants, unless every pipeline that uses it sets shared_connections: true.
// extra adds references found only when the connections are resolved
// (pipeline id -> references), such as the process AWS identity of MSK IAM.
func (o *Owners) CheckSharedRefs(extra map[string][]string) error {
	type use struct{ tenant, pipeline string }
	users := map[string][]use{}
	shared := map[string]bool{}
	for _, p := range o.Pipelines {
		refs := append(p.ConnRefs(), extra[p.ID]...)
		shared[p.ID] = p.SharedConnections
		for _, r := range refs {
			users[r] = append(users[r], use{p.TenantName(), p.ID})
		}
	}
	refs := make([]string, 0, len(users))
	for r := range users {
		refs = append(refs, r)
	}
	sort.Strings(refs) // same error for the same file
	for _, r := range refs {
		tenants := map[string]bool{}
		allShared := true
		var pipelines []string
		for _, u := range users[r] {
			tenants[u.tenant] = true
			allShared = allShared && shared[u.pipeline]
			pipelines = append(pipelines, u.pipeline)
		}
		if len(tenants) < 2 || allShared {
			continue
		}
		names := make([]string, 0, len(tenants))
		for t := range tenants {
			names = append(names, t)
		}
		sort.Strings(names)
		return fmt.Errorf("connection reference %s is used by tenants %s (pipelines %s); give each tenant its own, or set shared_connections: true on every one of those pipelines",
			r, strings.Join(names, ", "), strings.Join(pipelines, ", "))
	}
	return nil
}

// validateTenancy checks the tenants: block, every pipeline's tenant: and
// connections: (shape only, no I/O) and the shared-reference rule.
func (o *Owners) validateTenancy() error {
	if o.Tenants != nil && len(o.Tenants) == 0 {
		return fmt.Errorf("tenants: is empty; declare at least one tenant or remove the block")
	}
	for _, name := range o.TenantNames() {
		if !tenantNameRE.MatchString(name) {
			return fmt.Errorf("tenants: tenant name %s must match %s", quoteName(name), tenantNameRE)
		}
		repos, err := cleanRepos(o.Tenants[name].Repos)
		if err != nil {
			return fmt.Errorf("tenants.%s: %w", name, err)
		}
		t := o.Tenants[name]
		t.Repos = repos
		o.Tenants[name] = t
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		if p.Tenant != "" && !tenantNameRE.MatchString(p.Tenant) {
			return fmt.Errorf("pipeline %q: tenant %s must match %s", p.ID, quoteName(p.Tenant), tenantNameRE)
		}
		t := p.TenantName()
		if o.TenancyOn() {
			if _, ok := o.Tenants[t]; !ok && t != DefaultTenant {
				return fmt.Errorf("pipeline %q: tenant %s is not declared in tenants: (declared: %s)", p.ID, quoteName(t), strings.Join(o.TenantNames(), ", "))
			}
		} else if t != DefaultTenant {
			// Without the block the filter is off, so naming a tenant would
			// look like isolation while every user still sees the rooms.
			return fmt.Errorf("pipeline %q: tenant %s needs a tenants: block that declares it (members may be empty)", p.ID, quoteName(t))
		}
		if p.Connections != nil && len(p.Connections) == 0 {
			return fmt.Errorf("pipeline %q: connections: is empty; remove it to use the process-wide settings", p.ID)
		}
		if err := validateConnections(p.Connections, p.KindOrDefault()); err != nil {
			return fmt.Errorf("pipeline %q: %w", p.ID, err)
		}
	}
	return o.CheckSharedRefs(nil)
}

// validateConnections checks the shape of a connections: block of a pipeline
// of the given kind: a key of the other kind is refused, as a field of the
// other kind is (kind.go), and only the required keys of this kind must be
// set. Values are never read here and never quoted in an error.
func validateConnections(c Connections, kind string) error {
	if len(c) == 0 {
		return nil
	}
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		info, ok := connKeys[k]
		if !ok {
			return fmt.Errorf("connections: unknown key %s (allowed: %s)", quoteName(k), strings.Join(ConnectionKeys(), ", "))
		}
		if !info.belongsTo(kind) {
			// A batch pipeline has no Kafka or Connect: a key for one would
			// open a client to a system the pipeline does not have.
			return fmt.Errorf("connections.%s is a %s setting, but the pipeline kind is %s", k, info.kind, kind)
		}
		r := c[k]
		n := 0
		for _, s := range []string{r.Env, r.File, r.Value} {
			if s != "" {
				n++
			}
		}
		if n != 1 {
			return fmt.Errorf("connections.%s: set exactly one of env, file or value", k)
		}
		// The bad text is not quoted: a misplaced DSN or password must not
		// reach the start-up log.
		switch {
		case r.Value != "" && !info.literal:
			return fmt.Errorf("connections.%s: value: is allowed only for non-secret settings; use env: or file:", k)
		case r.Env != "" && !envNameRE.MatchString(r.Env):
			return fmt.Errorf("connections.%s: env: is not a variable name (want %s)", k, envNameRE)
		case r.File != "" && !isAbsPath(r.File):
			return fmt.Errorf("connections.%s: file: must be an absolute path", k)
		}
	}
	if err := checkWriteRole(c); err != nil {
		return err
	}
	for _, k := range ConnectionKeys() {
		if info := connKeys[k]; info.required && info.belongsTo(kind) {
			if _, ok := c[k]; !ok {
				return fmt.Errorf("connections: %s is required for kind %s", k, kind)
			}
		}
	}
	return nil
}

// checkWriteRole refuses a source_db_write_url that names the same reference
// as source_db_url: the write actions would then run as the read-only role
// (or the read tools as the write role), which defeats the separate role.
// Only two references of the same form can be compared here; an env: and a
// file: that hold the same DSN are caught by wiring when it resolves them.
func checkWriteRole(c Connections) error {
	w, okW := c["source_db_write_url"]
	r, okR := c["source_db_url"]
	if !okW || !okR {
		return nil
	}
	if cw, cr := canonRef(w), canonRef(r); cw != "" && cw == cr {
		return fmt.Errorf("connections.source_db_write_url: is the same reference as source_db_url; the write role needs its own credentials")
	}
	return nil
}

// canonRef is the canonical form of an env: or file: reference (see
// ConnRefs), or "" for a literal.
func canonRef(r ConnRef) string {
	switch {
	case r.Env != "":
		return refEnv(r.Env)
	case r.File != "":
		return FileRef(r.File)
	}
	return ""
}

// isAbsPath accepts an absolute path on this OS, and also a slash-rooted one
// so a file written on Linux checks the same way on a Windows dev machine.
func isAbsPath(p string) bool {
	return filepath.IsAbs(p) || strings.HasPrefix(p, "/")
}

// cleanRepos trims and checks a tenant's repos: list, like cleanNames for the
// pipeline name lists. It returns the trimmed list and leaves its input alone.
func cleanRepos(repos []string) ([]string, error) {
	if repos == nil {
		return nil, nil
	}
	out := make([]string, 0, len(repos))
	for i, raw := range repos {
		name := strings.TrimSpace(raw)
		switch {
		case name == "":
			return nil, fmt.Errorf("repos[%d]: empty entry", i)
		case !repoIDRE.MatchString(name):
			return nil, fmt.Errorf("repos: %s is not a repo id (want %s)", quoteName(name), repoIDRE)
		}
		out = append(out, name)
	}
	if d := firstDuplicate(out); d != "" {
		return nil, fmt.Errorf("repos lists %q twice", d)
	}
	return out, nil
}

// checkNullBlocks refuses a tenants:, connections: or tenant: key whose value
// is null: written empty, as ~, or with every entry commented out. yaml.v3
// never calls UnmarshalYAML for a null value, so after decoding such a key
// looks absent, and a lost connections: block would silently fall back to the
// process-wide credentials (a lost tenants: block would turn the filter
// off). The key's presence is read from the node tree. root is the decoded
// document; anything that is not a mapping is left to the normal decoder.
//
// It also refuses a null entry inside an auto_approve: list. yaml.v3 drops a
// null list item when it decodes into []string, so `- ` left behind by an edit
// would vanish without a word and the policy the admin meant would not run. A
// null auto_approve: key itself is left alone: it reads as no list, which is
// no auto-approval, the safe meaning (checkAutoApproveSyntax). A null entry in
// a dags: or models: list is refused the same way. Because only literal keys
// are read, a `<<` merge key in a pipeline is refused too.
func checkNullBlocks(root *yaml.Node) error {
	doc := root
	if doc.Kind == yaml.DocumentNode {
		if len(doc.Content) == 0 {
			return nil
		}
		doc = doc.Content[0]
	}
	if doc.Kind != yaml.MappingNode {
		return nil
	}
	if v, ok := mapValue(doc, "tenants"); ok && isNull(v) {
		return fmt.Errorf("line %d: tenants: is empty (null); declare at least one tenant, or remove the key to keep one tenant", v.Line)
	}
	pipes, ok := mapValue(doc, "pipelines")
	if !ok || pipes.Kind != yaml.SequenceNode {
		return nil
	}
	for i, p := range pipes.Content {
		if p.Kind != yaml.MappingNode {
			continue
		}
		name := fmt.Sprintf("pipelines[%d]", i)
		if id, ok := mapValue(p, "id"); ok && id.Kind == yaml.ScalarNode && !isNull(id) {
			name = "pipeline " + quoteName(id.Value)
		}
		// This function reads literal keys only. A `<<` merge key would bring in
		// fields it never sees (a null dags: entry, say), so it is refused.
		for j := 0; j+1 < len(p.Content); j += 2 {
			if k := p.Content[j]; k.Kind == yaml.ScalarNode && k.ShortTag() == "!!merge" {
				return fmt.Errorf("line %d: %s: merge key << is not supported in a pipeline; write the fields out", k.Line, name)
			}
		}
		if v, ok := mapValue(p, "connections"); ok && isNull(v) {
			return fmt.Errorf("line %d: %s: connections: is empty (null); list the connections, or remove the key to use the process-wide settings", v.Line, name)
		}
		if v, ok := mapValue(p, "tenant"); ok && isNull(v) {
			return fmt.Errorf("line %d: %s: tenant: is empty (null); name the tenant, or remove the key for tenant %q", v.Line, name, DefaultTenant)
		}
		if v, ok := mapValue(p, "auto_approve"); ok && v.Kind == yaml.SequenceNode {
			for _, item := range v.Content {
				if isNull(item) {
					return fmt.Errorf("line %d: %s: auto_approve: has an empty (null) entry; name an action or remove the entry", item.Line, name)
				}
			}
		}
		// The same for dags: and models: of a batch pipeline: a dropped null
		// entry would shrink the scope the file seems to give.
		for _, key := range []string{"dags", "models"} {
			if v, ok := mapValue(p, key); ok && v.Kind == yaml.SequenceNode {
				for _, item := range v.Content {
					if isNull(item) {
						return fmt.Errorf("line %d: %s: %s: has an empty (null) entry; name one or remove the entry", item.Line, name, key)
					}
				}
			}
		}
	}
	return nil
}

// mapValue returns the value node of key in a mapping node.
func mapValue(m *yaml.Node, key string) (*yaml.Node, bool) {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1], true
		}
	}
	return nil, false
}

func isNull(n *yaml.Node) bool {
	return n.Kind == yaml.ScalarNode && n.ShortTag() == "!!null"
}
