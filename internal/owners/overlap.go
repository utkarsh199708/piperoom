package owners

import (
	"sort"
	"strings"
)

// Overlap kinds: what a pipeline being registered shares with an already
// loaded pipeline (docs/design/setup-agent.md §3.6).
const (
	// OverlapTopicPrefix: two pipelines use the same topic prefix.
	OverlapTopicPrefix = "topic_prefix"
	// OverlapConnector: two pipelines use the same connector name.
	OverlapConnector = "connector"
	// OverlapTable: two pipelines read the same source table on the same
	// source host.
	OverlapTable = "source_table"
	// OverlapHost: two pipelines reach the same host (the same connection
	// reference).
	OverlapHost = "host"
)

// Overlap is one shared thing between the pipeline being registered and an
// already-loaded pipeline. The caller (pipeline_register) turns the list into
// a refusal; the rules of §3.6 are honoured by construction: a pipeline of
// another tenant is never named.
type Overlap struct {
	// Kind is OverlapTopicPrefix, OverlapConnector, OverlapTable or
	// OverlapHost.
	Kind string
	// Value is the shared name: the topic prefix, the connector, the source
	// table, or the host reference. It is always the requester's own name,
	// never another tenant's.
	Value string
	// PipelineID is the other pipeline's id when it is in the requester's own
	// tenant. It is "" when the other pipeline is in another tenant: the
	// refusal must then say "a pipeline in another tenant", never the id.
	PipelineID string
	// Contact holds the other tenant's admins when the other pipeline is in
	// another tenant, so the refusal can name who to talk to without naming
	// the tenant. nil for the requester's own tenant.
	Contact Principals
}

// Overlap compares p against every loaded pipeline (p itself excluded) and
// returns one Overlap per shared thing, in a stable order: kind first, then
// the shared value. It never names a pipeline or tenant of another tenant:
// such an Overlap has an empty PipelineID and the tenant's admins in Contact.
func (o *Owners) Overlap(p Pipeline) []Overlap {
	if o == nil {
		return nil
	}
	reqTenant := p.TenantName()
	var out []Overlap
	for i := range o.Pipelines {
		q := &o.Pipelines[i]
		if q.ID == p.ID {
			continue
		}
		out = append(out, overlapBetween(p, *q, reqTenant, o)...)
	}
	return out
}

// overlapBetween returns the overlaps of p (the requester) with one loaded
// pipeline q. reqTenant is p's tenant.
func overlapBetween(p, q Pipeline, reqTenant string, o *Owners) []Overlap {
	var other string
	var contact Principals
	if reqTenant != q.TenantName() {
		if t, ok := o.Tenants[q.TenantName()]; ok {
			contact = t.Admins
		}
	} else {
		other = q.ID
	}
	var out []Overlap
	add := func(kind, value string) {
		out = append(out, Overlap{Kind: kind, Value: value, PipelineID: other, Contact: contact})
	}
	if p.TopicPrefix != "" && p.TopicPrefix == q.TopicPrefix {
		add(OverlapTopicPrefix, p.TopicPrefix)
	}
	for _, c := range sortedIntersect(p.Connectors, q.Connectors) {
		add(OverlapConnector, c)
	}
	if sameSourceHost(p, q) {
		for _, t := range sortedIntersect(p.SourceTables, q.SourceTables) {
			add(OverlapTable, t)
		}
	}
	for _, h := range sortedIntersect(p.hostRefs(), q.hostRefs()) {
		add(OverlapHost, h)
	}
	return out
}

// hostConnKeys are the connections: keys whose value names a host the pipeline
// reaches. The TLS material and the literal settings (warehouse_kind, the
// kafka security/sasl/tls-server-name knobs) do not name a host.
var hostConnKeys = []string{"source_db_url", "source_db_write_url", "warehouse_db_url", "kafka_brokers", "connect_url", "schema_registry_url"}

// hostRef returns the canonical form of one connection reference, for the
// overlap check: env:NAME (upper), file:<path> (FileRef) or, for a literal
// host address, the trimmed lower-case value. A zero reference answers "".
func hostRef(r ConnRef) string {
	switch {
	case r.Env != "":
		return refEnv(r.Env)
	case r.File != "":
		return FileRef(r.File)
	case r.Value != "":
		return strings.ToLower(strings.TrimSpace(r.Value))
	}
	return ""
}

// sourceHostRef is the pipeline's source host reference: the source_db_url
// connection, canonical. "" when the pipeline has no connections: block (it
// then uses the process-wide settings, which are not "its" host here).
func (p Pipeline) sourceHostRef() string {
	if !p.HasConnections() {
		return ""
	}
	return hostRef(p.Connections["source_db_url"])
}

// sameSourceHost reports whether p and q read their source tables from the
// same host, by their source_db_url reference.
func sameSourceHost(p, q Pipeline) bool {
	ph, qh := p.sourceHostRef(), q.sourceHostRef()
	return ph != "" && ph == qh
}

// hostRefs returns the pipeline's host references, sorted and without
// duplicates, for the overlap check. A pipeline with no connections: block has
// none (process-wide settings are not compared here; CheckSharedRefs covers
// them).
func (p Pipeline) hostRefs() []string {
	if !p.HasConnections() {
		return nil
	}
	set := map[string]bool{}
	for _, k := range hostConnKeys {
		if r := hostRef(p.Connections[k]); r != "" {
			set[r] = true
		}
	}
	out := make([]string, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// sortedIntersect returns the values present in both slices, sorted and
// without duplicates.
func sortedIntersect(a, b []string) []string {
	set := make(map[string]bool, len(b))
	for _, x := range b {
		set[x] = true
	}
	seen := map[string]bool{}
	var out []string
	for _, x := range a {
		if set[x] && !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}
