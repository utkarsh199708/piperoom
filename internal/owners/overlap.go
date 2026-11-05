package owners

import (
	"net/url"
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
	// OverlapHost: two pipelines reach the same host (the same resolved
	// endpoint's host:port).
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
	// table, or the host. It is always the requester's own name, never another
	// tenant's.
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
//
// Hosts are compared by the resolved endpoints, not the raw references: a
// loaded pipeline holds env:/file: references and a provisional pipeline holds
// value: literals, so only the caller can know they name the same host.
// endpoints resolves a pipeline's connection references to the endpoint each
// connection key reaches (e.g. "source_db_url" -> "postgres://…:5432/db"); a
// nil resolver, or a key it does not resolve, falls back to the pipeline's own
// value: literal. Every endpoint is reduced to a normalized host:port before it
// is compared.
func (o *Owners) Overlap(p Pipeline, endpoints func(Pipeline) map[string]string) []Overlap {
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
		out = append(out, overlapBetween(p, *q, reqTenant, o, endpoints)...)
	}
	return out
}

// overlapBetween returns the overlaps of p (the requester) with one loaded
// pipeline q. reqTenant is p's tenant.
func overlapBetween(p, q Pipeline, reqTenant string, o *Owners, endpoints func(Pipeline) map[string]string) []Overlap {
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
	if sameSourceHost(p, q, endpoints) {
		for _, t := range sortedIntersect(p.SourceTables, q.SourceTables) {
			add(OverlapTable, t)
		}
	}
	for _, h := range sortedIntersect(p.hostPorts(endpoints), q.hostPorts(endpoints)) {
		add(OverlapHost, h)
	}
	return out
}

// hostConnKeys are the connections: keys whose value names a host the pipeline
// reaches. The TLS material and the literal settings (warehouse_kind, the
// kafka security/sasl/tls-server-name knobs) do not name a host.
var hostConnKeys = []string{"source_db_url", "source_db_write_url", "warehouse_db_url", "kafka_brokers", "connect_url", "schema_registry_url"}

// resolvedEndpoint returns the resolved endpoint of pipeline p for connection
// key k: endpoints(p)[k] when the caller provides a resolver, else the literal
// value:, trimmed. "" when the pipeline has neither.
func resolvedEndpoint(p Pipeline, k string, endpoints func(Pipeline) map[string]string) string {
	if endpoints != nil {
		if m := endpoints(p); m != nil {
			if v := strings.TrimSpace(m[k]); v != "" {
				return v
			}
		}
	}
	return strings.TrimSpace(p.Connections[k].Value)
}

// sourceHostPort is the pipeline's source host as a normalized host:port, from
// its resolved source_db_url endpoint. "" when the pipeline has no connections:
// block (it then uses the process-wide settings, which are not "its" host here).
func (p Pipeline) sourceHostPort(endpoints func(Pipeline) map[string]string) string {
	if !p.HasConnections() {
		return ""
	}
	return hostPort(resolvedEndpoint(p, "source_db_url", endpoints), p.defaultPortFor("source_db_url"))
}

// sameSourceHost reports whether p and q read their source tables from the
// same host, by their resolved source_db_url endpoint's normalized host:port.
func sameSourceHost(p, q Pipeline, endpoints func(Pipeline) map[string]string) bool {
	ph, qh := p.sourceHostPort(endpoints), q.sourceHostPort(endpoints)
	return ph != "" && ph == qh
}

// hostPorts returns the pipeline's resolved host endpoints as normalized
// host:port values, sorted and without duplicates, for the overlap check. A
// pipeline with no connections: block has none (process-wide settings are not
// compared here; CheckSharedRefs covers them).
func (p Pipeline) hostPorts(endpoints func(Pipeline) map[string]string) []string {
	if !p.HasConnections() {
		return nil
	}
	set := map[string]bool{}
	for _, k := range hostConnKeys {
		if k == "kafka_brokers" {
			// kafka_brokers is a comma-separated list of broker addresses; a
			// pipeline reaches every broker it names, so each entry is a host.
			for _, h := range kafkaHostPorts(resolvedEndpoint(p, k, endpoints)) {
				set[h] = true
			}
			continue
		}
		if v := hostPort(resolvedEndpoint(p, k, endpoints), p.defaultPortFor(k)); v != "" {
			set[v] = true
		}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// defaultPort is the standard port for a connection key, used when an endpoint
// names a host without a port, so a DSN without a port matches a typed
// host:port.
func defaultPort(key string) string {
	switch key {
	case "source_db_url", "source_db_write_url", "warehouse_db_url":
		return "5432"
	case "connect_url":
		return "8083"
	case "schema_registry_url":
		return "8081"
	case "kafka_brokers":
		return "9092"
	}
	return ""
}

// defaultPortFor is defaultPort for one pipeline: the source database keys of
// a MySQL pipeline default to 3306, so its bare "db.acme" and a typed
// "db.acme:3306" are one host (Q8d).
func (p Pipeline) defaultPortFor(key string) string {
	if key == "source_db_url" || key == "source_db_write_url" {
		if port := sourceDefaultPort(p); port != "" {
			return port
		}
	}
	return defaultPort(key)
}

// sourceDefaultPort is the standard port of a non-Postgres cdc source kind, ""
// for Postgres, a batch pipeline or an unknown kind (defaultPort then applies).
func sourceDefaultPort(p Pipeline) string {
	if p.isBatch() {
		return ""
	}
	switch p.SourceKind {
	case SourceKindMySQL:
		return "3306"
	case SourceKindSQLServer:
		return "1433"
	case SourceKindMongoDB:
		return "27017"
	case SourceKindOracle:
		return "1521"
	}
	return ""
}

// schemeDefaultPort is the standard port for a URL scheme, used when a URL
// endpoint names a host without a port; "" for a scheme with no known default.
func schemeDefaultPort(scheme string) string {
	switch strings.ToLower(scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	case "postgres", "postgresql":
		return "5432"
	case "mysql":
		return "3306"
	case "sqlserver", "mssql":
		return "1433"
	case "mongodb":
		return "27017"
	case "oracle":
		return "1521"
	}
	return ""
}

// kafkaHostPorts reduces a kafka_brokers value to its host:port values. The
// value is a comma-separated list (e.g. "b1:9092,b2:9092"); each entry may
// carry a Kafka protocol prefix ("SASL_SSL://b1:9092"), which is stripped before
// the usual endpoint normalization. Overlap on any one broker is a host overlap.
func kafkaHostPorts(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var out []string
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if i := strings.Index(entry, "://"); i >= 0 {
			entry = entry[i+3:]
		}
		if h := hostPort(entry, defaultPort("kafka_brokers")); h != "" {
			out = append(out, h)
		}
	}
	return out
}

// hostPort reduces an endpoint to its normalized host:port: scheme, userinfo,
// path, query and fragment are removed and the host is lower-cased, so a full
// DSN (postgres://user:pass@DB.Acme:5432/db?x=y) and a bare host:port
// (DB.Acme:5432) both reduce to "db.acme:5432". When the endpoint names a host
// without a port, the missing port is filled in: a URL endpoint uses its
// scheme's standard port (https -> 443, http -> 80, postgres -> 5432, …), and a
// bare host uses defaultPort (the connection key's standard port). A scheme
// without a known default falls back to defaultPort; an empty defaultPort keeps
// the host as written.
func hostPort(s, defaultPort string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if i := strings.Index(s, "://"); i >= 0 {
		if u, err := url.Parse(s); err == nil {
			host := strings.ToLower(u.Hostname())
			if host == "" {
				return ""
			}
			if port := u.Port(); port != "" {
				return host + ":" + port
			}
			def := schemeDefaultPort(u.Scheme)
			if def == "" {
				def = defaultPort
			}
			return withDefaultPort(host, def)
		}
	}
	// A bare host[:port], possibly with userinfo and a path/query/fragment.
	if i := strings.LastIndex(s, "@"); i >= 0 {
		s = s[i+1:]
	}
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	return withDefaultPort(strings.ToLower(strings.TrimSpace(s)), defaultPort)
}

// withDefaultPort returns host with defaultPort appended when the host carries
// no port of its own; a bracketed IPv6 literal counts as having no port unless
// a port follows the closing bracket.
func withDefaultPort(host, defaultPort string) string {
	if host == "" || defaultPort == "" || hasPort(host) {
		return host
	}
	return host + ":" + defaultPort
}

// hasPort reports whether host carries a :port.
func hasPort(host string) bool {
	colon := strings.LastIndex(host, ":")
	if colon < 0 {
		return false
	}
	if i := strings.Index(host, "]"); i >= 0 {
		// A bracketed IPv6 literal: only a colon after the bracket is a port.
		return colon > i
	}
	return true
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
