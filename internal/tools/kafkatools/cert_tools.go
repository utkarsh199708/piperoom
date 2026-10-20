package kafkatools

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients/databricks"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

// CertTarget is one configured endpoint tls_cert_expiry may dial. Targets are
// built from the process configuration only (CertTargets); the tool takes no
// address from the model.
type CertTarget struct {
	Label string // broker | connect | schema registry | warehouse
	Addr  string // host:port
	TLS   bool   // false: the endpoint is configured as plain text
	// Config carries the roots and server name for the chain check; nil = system roots.
	Config *tls.Config
}

// CertTargets lists the endpoints in cfg that the tool may probe: the Kafka
// brokers, Connect, the Schema Registry and the warehouse host (Snowflake,
// ClickHouse or Databricks). A root-CA file that cannot be read is skipped for
// that endpoint's chain check only (system roots are then used), never a
// failure.
func CertTargets(cfg config.Config) []CertTarget {
	var out []CertTarget
	kafkaTLS := false
	switch strings.ToUpper(cfg.KafkaSecurityProtocol) {
	case "SSL", "SASL_SSL":
		kafkaTLS = true
	}
	kc := tlsRoots(cfg.KafkaTLSCAFile, cfg.KafkaTLSServerName)
	for _, b := range strings.Split(cfg.KafkaBrokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, CertTarget{Label: "broker", Addr: b, TLS: kafkaTLS, Config: kc})
		}
	}
	if t, ok := urlTarget("connect", cfg.ConnectURL, cfg.ConnectTLSCAFile); ok {
		out = append(out, t)
	}
	if t, ok := urlTarget("schema registry", cfg.SchemaRegistryURL, cfg.SchemaRegistryTLSCAFile); ok {
		out = append(out, t)
	}
	if t, ok := warehouseTarget(cfg.WarehouseKind, cfg.WarehouseDBURL); ok {
		out = append(out, t)
	}
	return out
}

func tlsRoots(caFile, serverName string) *tls.Config {
	cfg, err := netsec.TLS{CAFile: caFile, ServerName: serverName}.Config()
	if err != nil {
		return nil
	}
	return cfg
}

func urlTarget(label, raw, caFile string) (CertTarget, bool) {
	if raw == "" {
		return CertTarget{}, false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return CertTarget{}, false
	}
	port := u.Port()
	if u.Scheme == "https" && port == "" {
		port = "443"
	}
	if port == "" {
		port = "80"
	}
	return CertTarget{Label: label, Addr: net.JoinHostPort(u.Hostname(), port), TLS: u.Scheme == "https", Config: tlsRoots(caFile, "")}, true
}

// snowflakeHost reads only the account of a Snowflake DSN
// ("user:password@account/db/schema?..."); the credentials are dropped.
func snowflakeHost(dsn string) string {
	i := strings.LastIndex(dsn, "@")
	if i < 0 {
		return ""
	}
	acct := dsn[i+1:]
	if j := strings.IndexAny(acct, "/?"); j >= 0 {
		acct = acct[:j]
	}
	if acct == "" {
		return ""
	}
	if !strings.Contains(acct, ".") {
		acct += ".snowflakecomputing.com"
	}
	return acct + ":443"
}

// warehouseTarget builds the warehouse endpoint tls_cert_expiry probes for the
// given kind, from the DSN host only; DSN credentials are never parsed or
// returned. ok is false when the kind has no per-customer host to check —
// BigQuery talks to Google's shared endpoints, which hold no customer
// certificate, and Postgres has no TLS-expiry check here — or when the DSN
// does not name a usable host, so a bad DSN yields no target and no error text
// that could carry the DSN.
func warehouseTarget(kind, dsn string) (CertTarget, bool) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "snowflake":
		if h := snowflakeHost(dsn); h != "" {
			return CertTarget{Label: "warehouse", Addr: h, TLS: true}, true
		}
	case "clickhouse":
		return clickhouseTarget(dsn)
	case "databricks":
		if h := databricksHost(dsn); h != "" {
			return CertTarget{Label: "warehouse", Addr: h, TLS: true}, true
		}
	}
	return CertTarget{}, false
}

// clickhouseTarget reads the first host:port of a clickhouse-go DSN
// ("clickhouse://user:password@host:port/database?...", or an http(s) URL
// form); the credentials and any further HA hosts are dropped. TLS is on only
// when clickhouse-go would use it: an https:// scheme, or a secure query
// parameter that is empty or parses to true. ok is false when the DSN does not
// name a host with a port, so a bad DSN yields no target and no error text.
func clickhouseTarget(dsn string) (CertTarget, bool) {
	u, err := url.Parse(dsn)
	if err != nil {
		return CertTarget{}, false
	}
	if strings.Contains(dsn, "@") && u.User == nil {
		// The credentials hold an unescaped "/": url.Parse ended the authority
		// before the "@", so u.Host would be a fragment of the credentials.
		return CertTarget{}, false
	}
	authority := strings.Split(u.Host, ",")[0]
	host, port, err := net.SplitHostPort(authority)
	if err != nil || host == "" || port == "" {
		return CertTarget{}, false
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return CertTarget{}, false
	}
	tlsOn := u.Scheme == "https"
	if v, ok := q["secure"]; ok {
		switch {
		case len(v) == 0 || v[0] == "":
			tlsOn = true
		default:
			if b, err := strconv.ParseBool(v[0]); err == nil && b {
				tlsOn = true
			}
		}
	}
	t := CertTarget{Label: "warehouse", Addr: net.JoinHostPort(host, port), TLS: tlsOn}
	if tlsOn {
		t.Config = tlsRoots("", q.Get("tls_server_name"))
	}
	return t, true
}

// databricksHost reads only the workspace host (and port) of a Databricks DSN
// via the client's own parser, so a host or port the client refuses (a port
// outside 1..65535, a host that is not a host name) yields no target; the
// token is dropped. It returns "" when the DSN does not parse, so a bad DSN
// yields no target and no error text.
func databricksHost(dsn string) string {
	if dsn == "" {
		return ""
	}
	// The DSN form token:<pat>@<host>/... may also carry an https:// prefix. An
	// unescaped "/" in the token ends url.Parse's authority before the "@", so
	// the parsed host would be a fragment of the token.
	raw := dsn
	if !strings.Contains(dsn, "://") {
		raw = "https://" + dsn
	}
	if u, err := url.Parse(raw); err == nil && strings.Contains(dsn, "@") && u.User == nil {
		return ""
	}
	cfg, err := databricks.ParseDSN(dsn)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
}

// ---- tls_cert_expiry ----

type certExpiry struct{ d Deps }

func (t *certExpiry) Name() string     { return "tls_cert_expiry" }
func (t *certExpiry) Kind() tools.Kind { return tools.KindRead }
func (t *certExpiry) InputSchema() map[string]any {
	return objectSchema(map[string]any{})
}
func (t *certExpiry) Description() string {
	return "Days until the TLS certificates of the configured endpoints (Kafka brokers, Connect, Schema Registry, warehouse) expire, " +
		"and whether the chain is trusted. Use it for sudden handshake or certificate errors. It takes no address: only configured endpoints are read."
}

// certResult is one endpoint's outcome in Result.Data.
type certResult struct {
	Label string           `json:"label"`
	Addr  string           `json:"addr"`
	TLS   bool             `json:"tls"`
	Cert  *netsec.CertInfo `json:"cert,omitempty"`
	Err   string           `json:"error,omitempty"`
}

func (t *certExpiry) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if _, err := parse[struct{}](input); err != nil {
		return tools.Result{}, err
	}
	if _, err := t.d.scope(ctx); err != nil {
		return tools.Result{}, err
	}
	probe := t.d.CertProbe
	if probe == nil {
		probe = netsec.CertExpiry
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	results := make([]certResult, len(t.d.CertTargets))
	var wg sync.WaitGroup
	for i, tg := range t.d.CertTargets {
		results[i] = certResult{Label: tg.Label, Addr: tg.Addr, TLS: tg.TLS}
		if !tg.TLS {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := probe(cctx, tg.Addr, tg.Config)
			if err != nil {
				results[i].Err = err.Error()
				return
			}
			results[i].Cert = &info
		}()
	}
	wg.Wait()
	var sb strings.Builder
	for _, r := range results {
		appendLine(&sb, certLine(r))
	}
	if sb.Len() == 0 {
		sb.WriteString("no endpoints configured")
	}
	return res(sb.String(), results), nil
}

func certLine(r certResult) string {
	head := r.Label + " " + r.Addr
	switch {
	case !r.TLS:
		return head + ": no TLS configured"
	case r.Err != "":
		return head + ": cannot read the certificate: " + r.Err
	}
	c := r.Cert
	var when string
	switch {
	case c.DaysLeft < 0:
		when = fmt.Sprintf("EXPIRED %d d ago (%s)", -c.DaysLeft, c.NotAfter.UTC().Format(time.DateOnly))
	default:
		when = fmt.Sprintf("expires in %d d (%s)", c.DaysLeft, c.NotAfter.UTC().Format(time.DateOnly))
	}
	chain := "✔ chain ok"
	if !c.Verified {
		chain = "✘ chain not trusted by the configured CAs (or the name does not match)"
	}
	return fmt.Sprintf("%s %s issuer %s %s", head, when, c.Issuer, chain)
}
