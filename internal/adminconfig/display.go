package adminconfig

import (
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strings"

	"piperoom/internal/config"
	"piperoom/internal/connprobe"
	"piperoom/internal/owners"
)

// keyInfo says, for one connections: key, which connection kind it belongs
// to, which process-wide variable sets it and which config.Config field it
// fills. It mirrors wiring's connFields (a test checks both agree through
// wiring.PipelineConfig) so the view and the probes read the same field.
type keyInfo struct {
	kind  connprobe.Kind
	env   string
	field func(*config.Config) *string
}

// keys is every owners.ConnectionKeys key (a test keeps the two lists equal).
var keys = map[string]keyInfo{
	"source_db_url":                 {connprobe.KindSourceDB, "SOURCE_DB_URL", func(c *config.Config) *string { return &c.SourceDBURL }},
	"source_db_write_url":           {connprobe.KindSourceDB, "SOURCE_DB_WRITE_URL", func(c *config.Config) *string { return &c.SourceDBWriteURL }},
	"warehouse_db_url":              {connprobe.KindWarehouse, "WAREHOUSE_DB_URL", func(c *config.Config) *string { return &c.WarehouseDBURL }},
	"warehouse_kind":                {connprobe.KindWarehouse, "WAREHOUSE_KIND", func(c *config.Config) *string { return &c.WarehouseKind }},
	"kafka_brokers":                 {connprobe.KindKafka, "KAFKA_BROKERS", func(c *config.Config) *string { return &c.KafkaBrokers }},
	"kafka_security_protocol":       {connprobe.KindKafka, "KAFKA_SECURITY_PROTOCOL", func(c *config.Config) *string { return &c.KafkaSecurityProtocol }},
	"kafka_sasl_mechanism":          {connprobe.KindKafka, "KAFKA_SASL_MECHANISM", func(c *config.Config) *string { return &c.KafkaSASLMechanism }},
	"kafka_sasl_username":           {connprobe.KindKafka, "KAFKA_SASL_USERNAME", func(c *config.Config) *string { return &c.KafkaSASLUsername }},
	"kafka_sasl_password":           {connprobe.KindKafka, "KAFKA_SASL_PASSWORD", func(c *config.Config) *string { return &c.KafkaSASLPassword }},
	"kafka_tls_ca_file":             {connprobe.KindKafka, "KAFKA_TLS_CA_FILE", func(c *config.Config) *string { return &c.KafkaTLSCAFile }},
	"kafka_tls_cert_file":           {connprobe.KindKafka, "KAFKA_TLS_CERT_FILE", func(c *config.Config) *string { return &c.KafkaTLSCertFile }},
	"kafka_tls_key_file":            {connprobe.KindKafka, "KAFKA_TLS_KEY_FILE", func(c *config.Config) *string { return &c.KafkaTLSKeyFile }},
	"kafka_tls_server_name":         {connprobe.KindKafka, "KAFKA_TLS_SERVER_NAME", func(c *config.Config) *string { return &c.KafkaTLSServerName }},
	"connect_url":                   {connprobe.KindConnect, "CONNECT_URL", func(c *config.Config) *string { return &c.ConnectURL }},
	"connect_username":              {connprobe.KindConnect, "CONNECT_USERNAME", func(c *config.Config) *string { return &c.ConnectUsername }},
	"connect_password":              {connprobe.KindConnect, "CONNECT_PASSWORD", func(c *config.Config) *string { return &c.ConnectPassword }},
	"connect_tls_ca_file":           {connprobe.KindConnect, "CONNECT_TLS_CA_FILE", func(c *config.Config) *string { return &c.ConnectTLSCAFile }},
	"connect_tls_cert_file":         {connprobe.KindConnect, "CONNECT_TLS_CERT_FILE", func(c *config.Config) *string { return &c.ConnectTLSCertFile }},
	"connect_tls_key_file":          {connprobe.KindConnect, "CONNECT_TLS_KEY_FILE", func(c *config.Config) *string { return &c.ConnectTLSKeyFile }},
	"schema_registry_url":           {connprobe.KindSchemaRegistry, "SCHEMA_REGISTRY_URL", func(c *config.Config) *string { return &c.SchemaRegistryURL }},
	"schema_registry_username":      {connprobe.KindSchemaRegistry, "SCHEMA_REGISTRY_USERNAME", func(c *config.Config) *string { return &c.SchemaRegistryUsername }},
	"schema_registry_password":      {connprobe.KindSchemaRegistry, "SCHEMA_REGISTRY_PASSWORD", func(c *config.Config) *string { return &c.SchemaRegistryPassword }},
	"schema_registry_tls_ca_file":   {connprobe.KindSchemaRegistry, "SCHEMA_REGISTRY_TLS_CA_FILE", func(c *config.Config) *string { return &c.SchemaRegistryTLSCAFile }},
	"schema_registry_tls_cert_file": {connprobe.KindSchemaRegistry, "SCHEMA_REGISTRY_TLS_CERT_FILE", func(c *config.Config) *string { return &c.SchemaRegistryTLSCertFile }},
	"schema_registry_tls_key_file":  {connprobe.KindSchemaRegistry, "SCHEMA_REGISTRY_TLS_KEY_FILE", func(c *config.Config) *string { return &c.SchemaRegistryTLSKeyFile }},
}

// requiredKey is the setting without which a kind has no connection: the DSN,
// the brokers or the URL. A kind whose required setting is empty is left out
// of the view.
var requiredKey = map[connprobe.Kind]string{
	connprobe.KindSourceDB:       "source_db_url",
	connprobe.KindWarehouse:      "warehouse_db_url",
	connprobe.KindKafka:          "kafka_brokers",
	connprobe.KindConnect:        "connect_url",
	connprobe.KindSchemaRegistry: "schema_registry_url",
}

// keysOf returns the keys of a kind, sorted (owners.ConnectionKeys order).
func keysOf(k connprobe.Kind) []string {
	var out []string
	for _, key := range owners.ConnectionKeys() {
		if keys[key].kind == k {
			out = append(out, key)
		}
	}
	return out
}

// isURLKey reports whether the key holds a URL shown without user info.
func isURLKey(key string) bool { return key == "connect_url" || key == "schema_registry_url" }

// isSecretKey reports whether the key may hold a credential (DSNs, user names,
// passwords): everything that is not a literal, a TLS path, a URL or the
// brokers. Deciding by exclusion means a key added to owners later is secret
// until someone says otherwise.
func isSecretKey(key string) bool {
	return !owners.IsLiteralKey(key) && !owners.IsPathKey(key) && !isURLKey(key) && key != "kafka_brokers"
}

// plainTokenRE is what a literal setting may look like to be shown: the
// protocol, mechanism, warehouse kind and TLS server name are all plain
// tokens. Anything else (a DSN or password in the wrong variable) is hidden.
var plainTokenRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$`)

// hostRE is an RFC 1123 host name, plus '_' (Docker service names use it).
var hostRE = regexp.MustCompile(`^[A-Za-z0-9_]([A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?(\.[A-Za-z0-9_]([A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?)*$`)

// portRE is a TCP port.
var portRE = regexp.MustCompile(`^[0-9]{1,5}$`)

// validHostPort reports whether host and port look like a host (a name or an
// IP address) and a port, nothing else.
func validHostPort(host, port string) bool {
	if !portRE.MatchString(port) {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return true
	}
	return len(host) <= 253 && hostRE.MatchString(host)
}

// displayURL returns scheme://host:port/path of a URL, without user info,
// query or fragment. ok is false when the value does not parse as such a URL;
// the caller then shows nothing and marks the row secret, because a value that
// is not a URL may be anything, a password included.
func displayURL(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || u.Opaque != "" {
		return "", false
	}
	// An '@' the parser did not take as the end of the user info sits in the
	// path, query or fragment: a password with an unescaped '/', '?' or '#'
	// can push part of itself there (http://u:1234/x@host). Show nothing.
	if strings.Contains(raw, "@") && u.User == nil {
		return "", false
	}
	if !validHostPort(strings.Trim(u.Hostname(), "[]"), portOr(u.Port())) {
		return "", false
	}
	out := u.Scheme + "://" + u.Host + u.EscapedPath()
	if strings.Contains(out, "@") {
		return "", false
	}
	return out, true
}

// portOr returns p, or a placeholder port so a URL without one still passes
// validHostPort (the scheme's default applies).
func portOr(p string) string {
	if p == "" {
		return "0"
	}
	return p
}

// displayBrokers returns the brokers as a comma list of host:port, with any
// scheme prefix and user info dropped. ok is false when an entry is not a
// host:port after that, or when only some entries carried user info: a
// password with a comma in it would otherwise split into an entry that looks
// like a broker ("user:1234").
func displayBrokers(raw string) (string, bool) {
	var out []string
	withUser, entries := 0, 0
	for _, e := range strings.Split(raw, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		entries++
		if i := strings.Index(e, "://"); i >= 0 {
			e = e[i+3:]
		}
		if i := strings.LastIndex(e, "@"); i >= 0 {
			withUser++
			e = e[i+1:]
		}
		host, port, err := net.SplitHostPort(e)
		if err != nil || !validHostPort(host, port) {
			return "", false
		}
		out = append(out, net.JoinHostPort(host, port))
	}
	if entries == 0 || (withUser > 0 && withUser != entries) {
		return "", false
	}
	return strings.Join(out, ","), true
}

// displayValue applies the display rule of key to a resolved, non-secret
// value. secret is true when the value must not be shown after all.
func displayValue(key, v string) (display string, secret bool) {
	v = strings.TrimSpace(v)
	switch {
	case isURLKey(key):
		d, ok := displayURL(v)
		return d, !ok
	case key == "kafka_brokers":
		d, ok := displayBrokers(v)
		return d, !ok
	case owners.IsLiteralKey(key):
		if v == "" || plainTokenRE.MatchString(v) {
			return v, false
		}
		return "", true
	case owners.IsPathKey(key):
		return v, false
	}
	return "", true
}

// refFrom is the `from` text of a pipeline reference: never its value.
func refFrom(r owners.ConnRef) string {
	switch {
	case r.Env != "":
		return "env " + r.Env
	case r.File != "":
		return "file " + r.File
	}
	return "value"
}
