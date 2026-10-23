package nettools

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools/kafkatools"
)

// The labels the model may name. A label is the only thing the model supplies:
// every address behind it comes from config, the pipeline's connections: or
// its connectors, never from the tool input.
const (
	LabelSource    = "source"          // the source database
	LabelKafka     = "kafka"           // the brokers
	LabelConnect   = "connect"         // the Kafka Connect REST API
	LabelRegistry  = "schema-registry" // the Schema Registry
	LabelWarehouse = "warehouse"       // the warehouse
)

// Target kinds. They select the extra checks: advertised listeners for Kafka,
// Connect's own view for the source database.
const (
	KindSource    = "source_db"
	KindKafka     = "kafka"
	KindConnect   = "connect"
	KindRegistry  = "schema_registry"
	KindWarehouse = "warehouse"
)

// maxEndpoints caps the endpoints of one target (a long broker list), so one
// probe cannot walk a whole fleet.
const maxEndpoints = 16

// Endpoint is one address of a target and the TLS policy configured for it.
type Endpoint struct {
	// Addr is host:port, normalized (lower-case host, numeric port).
	Addr string `json:"addr"`
	// TLS is true when the endpoint is configured with TLS, so the probe runs
	// its certificate step.
	TLS bool `json:"tls"`
	// Connector names the connector whose database.hostname named Addr; "" when
	// the address came from a setting. The Connect check validates that
	// connector's running config.
	Connector string `json:"connector,omitempty"`
	// Config carries the roots and server name for the chain verdict; nil =
	// system roots and the dialed host's name. It is never marshalled: it can
	// hold a client certificate, which the probe never presents.
	Config *tls.Config `json:"-"`
}

// Target is one named thing the probe can reach: a label the model may name
// and the endpoints behind it.
type Target struct {
	Label     string     `json:"label"`
	Kind      string     `json:"kind"`
	Endpoints []Endpoint `json:"endpoints"`
	// Note is a short, non-secret remark about a part of the target that could
	// not be read, e.g. a connector whose config Connect refused.
	Note string `json:"note,omitempty"`
	// Connectors lists the pipeline's connectors whose running config the
	// Connect check may validate, in pipeline order.
	Connectors []string `json:"connectors,omitempty"`
}

// TargetSource is where a probe's targets come from. The setup runner passes
// the provisional pipeline's hosts (TargetsFromSetup); the room tool passes the
// pipeline's configured targets (RoomTargets). Labels opens no connection and
// reads no connector config, so an unknown label is refused before anything is
// read; Target may read a connector config, so it takes ctx and must honor it.
type TargetSource interface {
	// Labels lists the labels the source can resolve for this pipeline.
	Labels(p owners.Pipeline) []string
	// Target resolves one label to its endpoints. A label Labels does not
	// return is refused by the caller.
	Target(ctx context.Context, p owners.Pipeline, label string) (Target, error)
}

// RoomTargets lists the targets of the room's pipeline from the client set's
// own settings (the process-wide settings, or one pipeline's resolved
// connections: block), the pipeline's literal connections: values (a
// provisional pipeline) and the pipeline's own connectors'
// database.hostname/database.port. cfg must be the settings of the same client
// set: a set built for one tenant's pipeline never names another tenant's host.
func RoomTargets(cfg config.Config, connect clients.ConnectClient) TargetSource {
	return roomTargets{cfg: cfg, connect: connect}
}

type roomTargets struct {
	cfg     config.Config
	connect clients.ConnectClient
}

func (r roomTargets) Labels(p owners.Pipeline) []string {
	cfg := overlayLiteral(r.cfg, p)
	b := newTargetSet()
	for _, ct := range kafkatools.CertTargets(cfg) {
		b.add(kindOfCertLabel(ct.Label), labelOf(ct.Label), ct.Addr, ct.TLS, ct.Config, "")
	}
	if strings.EqualFold(warehouseKind(cfg.WarehouseKind), "postgres") {
		if addr := hostPortOf(cfg.WarehouseDBURL, "5432"); addr != "" {
			b.add(KindWarehouse, LabelWarehouse, addr, false, nil, "")
		}
	}
	if addr := hostPortOf(cfg.SourceDBURL, "5432"); addr != "" {
		b.add(KindSource, LabelSource, addr, false, nil, "")
	}
	// A declared connector may name the source host; that is only known after
	// Connect answers, so the label is offered and Target reads the configs.
	if r.connect != nil && len(p.Connectors) > 0 {
		b.addLabel(KindSource, LabelSource)
	}
	return b.labels()
}

func (r roomTargets) Target(ctx context.Context, p owners.Pipeline, label string) (Target, error) {
	label = strings.ToLower(strings.TrimSpace(label))
	cfg := overlayLiteral(r.cfg, p)
	b := newTargetSet()
	switch kindOfLabel(label) {
	case KindKafka, KindConnect, KindRegistry, KindWarehouse:
		for _, ct := range kafkatools.CertTargets(cfg) {
			if labelOf(ct.Label) != label {
				continue
			}
			b.add(kindOfCertLabel(ct.Label), labelOf(ct.Label), ct.Addr, ct.TLS, ct.Config, "")
		}
		if kindOfLabel(label) == KindWarehouse && strings.EqualFold(warehouseKind(cfg.WarehouseKind), "postgres") {
			if addr := hostPortOf(cfg.WarehouseDBURL, "5432"); addr != "" {
				b.add(KindWarehouse, LabelWarehouse, addr, false, nil, "")
			}
		}
	case KindSource:
		if addr := hostPortOf(cfg.SourceDBURL, "5432"); addr != "" {
			b.add(KindSource, LabelSource, addr, false, nil, "")
		}
	default:
		return Target{}, fmt.Errorf("net_probe: label %q has no target kind", clip(label))
	}
	if kindOfLabel(label) == KindSource {
		notes := b.addConnectors(ctx, r.connect, p)
		targets := b.list()
		if len(targets) == 0 {
			return Target{Label: label, Kind: KindSource, Note: strings.Join(notes, "; ")}, nil
		}
		targets[0].Connectors = append([]string(nil), p.Connectors...)
		targets[0].Note = strings.Join(notes, "; ")
		return targets[0], nil
	}
	targets := b.list()
	if len(targets) == 0 {
		return Target{Label: label, Kind: kindOfLabel(label)}, nil
	}
	return targets[0], nil
}

// addConnectors adds the source host each of the pipeline's connectors names.
// Only database.hostname/database.port are read: a credential value never is.
// A connector Connect cannot describe is a note, not a failed probe: the
// settings may still name the host.
func (b *targetSet) addConnectors(ctx context.Context, connect clients.ConnectClient, p owners.Pipeline) []string {
	if connect == nil || len(p.Connectors) == 0 {
		return nil
	}
	var notes []string
	for _, name := range p.Connectors {
		cctx, cancel := context.WithTimeout(ctx, stepTimeout)
		cfg, err := connect.Config(cctx, name)
		cancel()
		if err != nil {
			notes = append(notes, "connector "+clip(name)+": config unreadable")
			continue
		}
		host := strings.TrimSpace(cfg["database.hostname"])
		if host == "" {
			continue // a sink, or a source that names no JDBC host
		}
		port := strings.TrimSpace(cfg["database.port"])
		if port == "" {
			port = "5432"
		}
		addr := canonicalAddr(host, port)
		if addr == "" {
			notes = append(notes, "connector "+clip(name)+": database.hostname is not a host")
			continue
		}
		b.add(KindSource, LabelSource, addr, false, nil, name)
	}
	return notes
}

// TargetsFromSetup adapts the setup runner's registered targets to a
// TargetSource, so U1-2 runs the same probe from the runner's position over the
// provisional pipeline's hosts. Each comma-separated address is one endpoint.
// TLS is off: a setup target carries no scheme, and an https endpoint's
// certificate is http_get's and tls_cert_expiry's business.
func TargetsFromSetup(ts []events.SetupTarget) TargetSource {
	return setupTargets(append([]events.SetupTarget(nil), ts...))
}

type setupTargets []events.SetupTarget

func (s setupTargets) Labels(_ owners.Pipeline) []string {
	b := newTargetSet()
	for _, t := range s {
		if label := strings.ToLower(strings.TrimSpace(t.Kind)); label != "" {
			b.addLabel(kindOfLabel(label), label)
		}
	}
	return b.labels()
}

func (s setupTargets) Target(_ context.Context, _ owners.Pipeline, label string) (Target, error) {
	b := newTargetSet()
	for _, t := range s {
		kind := strings.ToLower(strings.TrimSpace(t.Kind))
		if kind == "" || !strings.EqualFold(kind, label) {
			continue
		}
		for _, raw := range strings.Split(t.Address, ",") {
			if addr := hostPortOf(strings.TrimSpace(raw), defaultPort(kindOfLabel(kind))); addr != "" {
				b.add(kindOfLabel(kind), kind, addr, false, nil, "")
			}
		}
	}
	targets := b.list()
	if len(targets) == 0 {
		return Target{Label: label, Kind: kindOfLabel(label)}, nil
	}
	return targets[0], nil
}

// targetSet builds the target list of one call, preserving labels' first-seen
// order and dropping a duplicate address of the same label.
type targetSet struct {
	order   []string
	byLabel map[string]*Target
}

func newTargetSet() *targetSet { return &targetSet{byLabel: map[string]*Target{}} }

func (b *targetSet) add(kind, label, addr string, tlsOn bool, cfg *tls.Config, connector string) {
	if addr == "" || label == "" {
		return
	}
	b.addLabel(kind, label)
	t := b.byLabel[label]
	for i, e := range t.Endpoints {
		if strings.EqualFold(e.Addr, addr) {
			if e.Connector == "" && connector != "" {
				t.Endpoints[i].Connector = connector
			}
			return
		}
	}
	if len(t.Endpoints) >= maxEndpoints {
		return
	}
	t.Endpoints = append(t.Endpoints, Endpoint{Addr: addr, TLS: tlsOn, Connector: connector, Config: cfg})
}

// addLabel lists a label with no endpoint yet (a target whose address is only
// known after a client answers).
func (b *targetSet) addLabel(kind, label string) {
	if label == "" {
		return
	}
	if _, ok := b.byLabel[label]; ok {
		return
	}
	b.byLabel[label] = &Target{Label: label, Kind: kind}
	b.order = append(b.order, label)
}

func (b *targetSet) labels() []string { return append([]string(nil), b.order...) }

func (b *targetSet) list() []Target {
	out := make([]Target, 0, len(b.order))
	for _, label := range b.order {
		out = append(out, *b.byLabel[label])
	}
	return out
}

// labelOf maps a kafkatools.CertTarget label to the label the model names.
func labelOf(certLabel string) string {
	switch certLabel {
	case "broker":
		return LabelKafka
	case "schema registry":
		return LabelRegistry
	case "connect":
		return LabelConnect
	case "warehouse":
		return LabelWarehouse
	}
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(certLabel)), " ", "-")
}

func kindOfCertLabel(certLabel string) string { return kindOfLabel(labelOf(certLabel)) }

// kindOfLabel normalizes a target label (or a setup target's kind) to a kind.
func kindOfLabel(label string) string {
	switch label {
	case "kafka", "broker", "brokers":
		return KindKafka
	case "connect":
		return KindConnect
	case "schema-registry", "schema_registry", "schema registry", "registry":
		return KindRegistry
	case "warehouse":
		return KindWarehouse
	case "source", "source-db", "source_db", "postgres", "postgresql", "mysql", "sqlserver":
		return KindSource
	}
	return label
}

// defaultPort is the standard port of a kind, used when a target names a host
// without a port.
func defaultPort(kind string) string {
	switch kind {
	case KindKafka:
		return "9092"
	case KindConnect:
		return "8083"
	case KindRegistry:
		return "8081"
	case KindSource, KindWarehouse:
		return "5432"
	}
	return ""
}

// overlayLiteral fills the host-bearing settings of cfg from the pipeline's
// literal connections: values (warehouse_kind is read too, because it chooses
// the warehouse's host form). A resolved setting always wins: wiring hands a
// set its pipeline's own resolved values, and only a provisional (setup)
// pipeline writes a literal. env: and file: references are never resolved here:
// they are not hosts.
func overlayLiteral(cfg config.Config, p owners.Pipeline) config.Config {
	set := func(key string, field *string) {
		if r, ok := p.Connections[key]; ok && r.Value != "" && strings.TrimSpace(*field) == "" {
			*field = r.Value
		}
	}
	set("source_db_url", &cfg.SourceDBURL)
	set("warehouse_db_url", &cfg.WarehouseDBURL)
	set("warehouse_kind", &cfg.WarehouseKind)
	set("kafka_brokers", &cfg.KafkaBrokers)
	set("kafka_security_protocol", &cfg.KafkaSecurityProtocol)
	set("connect_url", &cfg.ConnectURL)
	set("schema_registry_url", &cfg.SchemaRegistryURL)
	return cfg
}

// warehouseKind is the normalized WAREHOUSE_KIND; "" means postgres, the
// default of the rest of Piperoom.
func warehouseKind(k string) string {
	if strings.TrimSpace(k) == "" {
		return "postgres"
	}
	return strings.ToLower(strings.TrimSpace(k))
}

var hostRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`)

// validHost reports whether host is an IP literal or a plausible host name. It
// errs towards refusing: a DSN fragment that is not a host must not become a
// target.
func validHost(host string) bool {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if host == "" || len(host) > 253 {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	return hostRE.MatchString(host)
}

// canonicalAddr normalizes host and port to host:port (lower-case host, a
// numeric port in 1..65535), or "" when either is not usable.
func canonicalAddr(host, port string) string {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if !validHost(host) {
		return ""
	}
	if port == "" {
		return ""
	}
	n, err := strconv.Atoi(strings.TrimSpace(port))
	if err != nil || n < 1 || n > 65535 {
		return ""
	}
	return net.JoinHostPort(strings.ToLower(host), strconv.Itoa(n))
}

// schemePort is a URL scheme's default port, else fallback.
func schemePort(scheme, fallback string) string {
	switch strings.ToLower(scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	case "postgres", "postgresql":
		return "5432"
	case "clickhouse":
		return "9000"
	}
	return fallback
}

// hostPortOf reads the host and port out of a DSN, URL or bare host[:port],
// dropping scheme, userinfo, path and query: a credential never leaves this
// function. "" when the value names no usable host, so a bad DSN yields no
// target and no error text that could carry it. defaultPort fills a missing
// port.
func hostPortOf(raw, defaultPort string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if i := strings.Index(raw, "://"); i >= 0 {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return ""
		}
		port := u.Port()
		if port == "" {
			port = schemePort(u.Scheme, defaultPort)
		}
		return canonicalAddr(u.Hostname(), port)
	}
	// A bare host[:port], possibly with userinfo and a path/query/fragment.
	if i := strings.LastIndex(raw, "@"); i >= 0 {
		raw = raw[i+1:]
	}
	if i := strings.IndexAny(raw, "/?#"); i >= 0 {
		raw = raw[:i]
	}
	host, port := raw, ""
	if h, p, err := net.SplitHostPort(raw); err == nil {
		host, port = h, p
	} else if strings.Count(raw, ":") > 1 {
		host = strings.Trim(raw, "[]") // an IPv6 literal without a port
	}
	if port == "" {
		port = defaultPort
	}
	return canonicalAddr(host, port)
}

// clip bounds text that came from input or a server before it is echoed.
func clip(s string) string {
	const max = 120
	s = strings.TrimSpace(strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		return r
	}, s))
	if r := []rune(s); len(r) > max {
		return string(r[:max]) + "…"
	}
	return s
}

// stepTimeout is the bound of one DNS, TCP, TLS or Connect step.
const stepTimeout = 3 * time.Second
