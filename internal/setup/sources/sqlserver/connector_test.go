package sqlserver

import (
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/actions"
)

func testConnectorOptions() ConnectorOptions {
	return ConnectorOptions{
		Host: "mssql", Port: 1433, Database: "orders", User: "piperoom_cdc",
		PasswordSecret: "mssql_piperoom_cdc", TopicPrefix: "mssql",
		Tables:           []string{"dbo.orders", DefaultCanaryTable},
		HistoryBootstrap: "kafka:9092",
	}
}

func TestConnectorConfig(t *testing.T) {
	cfg, err := ConnectorConfig(testConnectorOptions())
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	want := map[string]string{
		"connector.class":    ConnectorClass,
		"database.hostname":  "mssql",
		"database.port":      "1433",
		"database.names":     "orders",
		"database.user":      "piperoom_cdc",
		"database.password":  "${secret:mssql_piperoom_cdc}",
		"database.encrypt":   "false",
		"topic.prefix":       "mssql",
		"table.include.list": "dbo.orders,dbo.piperoom_canary",
		"schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
		"schema.history.internal.kafka.topic":             "schema-changes.mssql",
	}
	if len(cfg) != len(want) {
		t.Fatalf("config has %d keys: %v", len(cfg), cfg)
	}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("%s = %q, want %q", k, cfg[k], v)
		}
	}

	o := testConnectorOptions()
	o.Encrypt, o.TrustServerCertificate = true, true
	o.HistoryBootstrap = "b1:9092, 10.0.0.2:9093"
	o.HistoryTopic = "schema-changes.orders-mssql"
	cfg, err = ConnectorConfig(o)
	if err != nil {
		t.Fatalf("ConnectorConfig with TLS: %v", err)
	}
	if cfg["database.encrypt"] != "true" || cfg["database.trustServerCertificate"] != "true" ||
		cfg["schema.history.internal.kafka.bootstrap.servers"] != "b1:9092,10.0.0.2:9093" ||
		cfg["schema.history.internal.kafka.topic"] != "schema-changes.orders-mssql" {
		t.Fatalf("TLS config: %v", cfg)
	}
}

// TestConnectorConfigPassesQ4aShapes: key names, literal hosts, and only
// ${secret:...} references on credential keys.
func TestConnectorConfigPassesQ4aShapes(t *testing.T) {
	cfg, err := ConnectorConfig(testConnectorOptions())
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	keyRe := regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9._$-]*$`)
	for k, v := range cfg {
		if !keyRe.MatchString(k) {
			t.Errorf("key %q is not a Connect config key", k)
		}
		if strings.Contains(v, "${") && (!strings.HasPrefix(v, "${secret:") || !actions.LooksSecret(k)) {
			t.Errorf("%s holds a reference %q outside a credential key", k, v)
		}
		if actions.LooksSecret(k) && !strings.HasPrefix(v, "${secret:") {
			t.Errorf("%s is a credential key with a plaintext value", k)
		}
	}
}

func TestConnectorConfigRefusals(t *testing.T) {
	tests := []struct {
		name string
		mod  func(*ConnectorOptions)
	}{
		{"empty host", func(o *ConnectorOptions) { o.Host = "" }},
		{"host with a scheme", func(o *ConnectorOptions) { o.Host = "sqlserver://db" }},
		{"host a reference", func(o *ConnectorOptions) { o.Host = "${env:DB_HOST}" }},
		{"port zero", func(o *ConnectorOptions) { o.Port = 0 }},
		{"port too high", func(o *ConnectorOptions) { o.Port = 70000 }},
		{"system database", func(o *ConnectorOptions) { o.Database = "master" }},
		{"database with a space", func(o *ConnectorOptions) { o.Database = "or ders" }},
		{"sa user", func(o *ConnectorOptions) { o.User = "sa" }},
		{"guest user", func(o *ConnectorOptions) { o.User = "guest" }},
		{"cdc user", func(o *ConnectorOptions) { o.User = "cdc" }},
		{"history broker ipv6 without brackets", func(o *ConnectorOptions) { o.HistoryBootstrap = "::1:9092" }},
		{"user with a bracket", func(o *ConnectorOptions) { o.User = "ro]" }},
		{"no secret name", func(o *ConnectorOptions) { o.PasswordSecret = "" }},
		{"secret name with a slash", func(o *ConnectorOptions) { o.PasswordSecret = "../../etc/passwd" }},
		{"empty topic prefix", func(o *ConnectorOptions) { o.TopicPrefix = "" }},
		{"no tables", func(o *ConnectorOptions) { o.Tables = nil }},
		{"table injection", func(o *ConnectorOptions) { o.Tables = []string{"dbo.orders]"} }},
		{"no history bootstrap", func(o *ConnectorOptions) { o.HistoryBootstrap = "" }},
		{"history broker without port", func(o *ConnectorOptions) { o.HistoryBootstrap = "kafka" }},
		{"history broker bad port", func(o *ConnectorOptions) { o.HistoryBootstrap = "kafka:0" }},
		{"history broker reference", func(o *ConnectorOptions) { o.HistoryBootstrap = "${env:K}:9092" }},
		{"history topic with a space", func(o *ConnectorOptions) { o.HistoryTopic = "schema changes" }},
		{"history topic dot", func(o *ConnectorOptions) { o.HistoryTopic = "." }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := testConnectorOptions()
			tc.mod(&o)
			if cfg, err := ConnectorConfig(o); err == nil {
				t.Fatalf("accepted: %v", cfg)
			}
		})
	}
}

// TestConnectorConfigTLSKeysPassQ4aShapes: the keys written only with Encrypt
// on are Connect config keys and not credential keys (their values are plain
// booleans, so LooksSecret must not claim them), trustServerCertificate is
// absent with encryption off, and an IPv6 broker keeps its brackets.
func TestConnectorConfigTLSKeysPassQ4aShapes(t *testing.T) {
	o := testConnectorOptions()
	o.Encrypt, o.TrustServerCertificate = true, true
	o.HistoryBootstrap = "[::1]:9092,kafka:9092"
	cfg, err := ConnectorConfig(o)
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	keyRe := regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9._$-]*$`)
	for k, v := range cfg {
		if !keyRe.MatchString(k) {
			t.Errorf("key %q is not a Connect config key", k)
		}
		if strings.Contains(v, "${") && (!strings.HasPrefix(v, "${secret:") || !actions.LooksSecret(k)) {
			t.Errorf("%s holds a reference %q outside a credential key", k, v)
		}
		if actions.LooksSecret(k) && !strings.HasPrefix(v, "${secret:") {
			t.Errorf("%s is a credential key with a plaintext value", k)
		}
	}
	if cfg["database.encrypt"] != "true" || cfg["database.trustServerCertificate"] != "true" {
		t.Fatalf("TLS keys: %v", cfg)
	}
	if got := cfg["schema.history.internal.kafka.bootstrap.servers"]; got != "[::1]:9092,kafka:9092" {
		t.Fatalf("bootstrap servers = %q", got)
	}
	o.Encrypt = false
	cfg, err = ConnectorConfig(o)
	if err != nil {
		t.Fatalf("ConnectorConfig without TLS: %v", err)
	}
	if _, ok := cfg["database.trustServerCertificate"]; ok || cfg["database.encrypt"] != "false" {
		t.Fatalf("trustServerCertificate written with encryption off: %v", cfg)
	}
}
