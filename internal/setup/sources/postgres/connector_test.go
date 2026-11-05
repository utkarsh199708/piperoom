package postgres

import (
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/actions"
)

func testConnectorOptions() ConnectorOptions {
	return ConnectorOptions{
		Host:           "db-1.acme.com",
		Port:           5432,
		Database:       "orders",
		User:           "piperoom_ro",
		PasswordSecret: "orders_read_password",
		TopicPrefix:    "orders",
		Tables:         []string{"public.orders", "public.customers"},
	}
}

func TestConnectorConfig(t *testing.T) {
	cfg, err := ConnectorConfig(testConnectorOptions())
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	want := map[string]string{
		"connector.class":             ConnectorClass,
		"database.hostname":           "db-1.acme.com",
		"database.port":               "5432",
		"database.dbname":             "orders",
		"database.user":               "piperoom_ro",
		"database.password":           "${secret:orders_read_password}",
		"plugin.name":                 "pgoutput",
		"slot.name":                   "debezium",
		"publication.name":            "dbz_publication",
		"publication.autocreate.mode": "disabled",
		"topic.prefix":                "orders",
		"table.include.list":          "public.orders,public.customers",
	}
	if len(cfg) != len(want) {
		t.Fatalf("config has %d keys: %v", len(cfg), cfg)
	}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("%s = %q, want %q", k, cfg[k], v)
		}
	}
}

// TestConnectorConfigDisablesPublicationAutocreate pins the one mode a
// setup-created publication can use: the read role owns neither the
// publication nor the tables, and Debezium 2.x's "filtered" runs ALTER
// PUBLICATION ... SET TABLE, which needs that ownership.
func TestConnectorConfigDisablesPublicationAutocreate(t *testing.T) {
	cfg, err := ConnectorConfig(testConnectorOptions())
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	if cfg["publication.autocreate.mode"] != "disabled" {
		t.Fatalf("publication.autocreate.mode = %q, want disabled", cfg["publication.autocreate.mode"])
	}
}

// TestConnectorConfigPassesQ4aShapes checks the config against the rules
// connect_validate_config and connect_create_connector apply before Connect
// sees it: key names, value sizes, a literal host and only ${secret:...}
// references, and no plaintext credential.
func TestConnectorConfigPassesQ4aShapes(t *testing.T) {
	cfg, err := ConnectorConfig(testConnectorOptions())
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	keyRe := regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9._$-]*$`)
	refRe := regexp.MustCompile(`^\$\{[a-z]+:[^}]+\}$`)
	for k, v := range cfg {
		if !keyRe.MatchString(k) {
			t.Errorf("key %q is not a Connect config key", k)
		}
		if len(v) > 8192 {
			t.Errorf("%s is over 8192 bytes", k)
		}
		if strings.Contains(v, "${") {
			if !refRe.MatchString(v) || !strings.HasPrefix(v, "${secret:") {
				t.Errorf("%s holds a non-secret reference %q", k, v)
			}
			if !actions.LooksSecret(k) {
				t.Errorf("%s is not a credential key but holds a reference", k)
			}
		}
		if actions.LooksSecret(k) && !strings.HasPrefix(v, "${secret:") {
			t.Errorf("%s is a credential key with a plaintext value", k)
		}
	}
	if host := cfg["database.hostname"]; strings.ContainsAny(host, "/:@ ") {
		t.Errorf("database.hostname %q is not a literal host", host)
	}
}

func TestConnectorConfigRefusals(t *testing.T) {
	tests := []struct {
		name string
		mod  func(*ConnectorOptions)
	}{
		{"empty host", func(o *ConnectorOptions) { o.Host = "" }},
		{"host with a scheme", func(o *ConnectorOptions) { o.Host = "postgres://db" }},
		{"host a reference", func(o *ConnectorOptions) { o.Host = "${env:DB_HOST}" }},
		{"port zero", func(o *ConnectorOptions) { o.Port = 0 }},
		{"port too high", func(o *ConnectorOptions) { o.Port = 70000 }},
		{"empty database", func(o *ConnectorOptions) { o.Database = "" }},
		{"database with a space", func(o *ConnectorOptions) { o.Database = "or ders" }},
		{"empty user", func(o *ConnectorOptions) { o.User = "" }},
		{"public user", func(o *ConnectorOptions) { o.User = "public" }},
		{"user with a quote", func(o *ConnectorOptions) { o.User = `ro"` }},
		{"no secret name", func(o *ConnectorOptions) { o.PasswordSecret = "" }},
		{"secret name with a slash", func(o *ConnectorOptions) { o.PasswordSecret = "../../etc/passwd" }},
		{"empty topic prefix", func(o *ConnectorOptions) { o.TopicPrefix = "" }},
		{"topic prefix with a space", func(o *ConnectorOptions) { o.TopicPrefix = "or ders" }},
		{"no tables", func(o *ConnectorOptions) { o.Tables = nil }},
		{"table without schema", func(o *ConnectorOptions) { o.Tables = []string{"orders"} }},
		{"table injection", func(o *ConnectorOptions) { o.Tables = []string{`public.orders"`} }},
		{"bad publication", func(o *ConnectorOptions) { o.Publication = "1pub" }},
		{"upper-case slot", func(o *ConnectorOptions) { o.Slot = "Debezium" }},
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
