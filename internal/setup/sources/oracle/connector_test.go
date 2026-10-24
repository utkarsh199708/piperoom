package oracle

import (
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/actions"
)

func testConnectorOptions() ConnectorOptions {
	return ConnectorOptions{
		Host:                 "oracle-1.acme.com",
		Port:                 1521,
		Database:             "FREE",
		PDB:                  "FREEPDB1",
		User:                 "C##DBZUSER",
		PasswordSecret:       "source_read_password",
		TopicPrefix:          "orders",
		Tables:               []string{"PIPEROOM.ORDERS", "PIPEROOM.PIPEROOM_CANARY"},
		SchemaHistoryServers: []string{"kafka:9092"},
	}
}

func TestConnectorConfig(t *testing.T) {
	cfg, err := ConnectorConfig(testConnectorOptions())
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	want := map[string]string{
		"connector.class":             ConnectorClass,
		"database.hostname":           "oracle-1.acme.com",
		"database.port":               "1521",
		"database.dbname":             "FREE",
		"database.pdb.name":           "FREEPDB1",
		"database.user":               "C##DBZUSER",
		"database.password":           "${secret:source_read_password}",
		"database.connection.adapter": "logminer",
		"log.mining.strategy":         "online_catalog",
		"topic.prefix":                "orders",
		"table.include.list":          "PIPEROOM.ORDERS,PIPEROOM.PIPEROOM_CANARY",
		"schema.history.internal.kafka.bootstrap.servers":        "kafka:9092",
		"schema.history.internal.kafka.topic":                    "schema-history.orders",
		"schema.history.internal.store.only.captured.tables.ddl": "true",
	}
	if len(cfg) != len(want) {
		t.Fatalf("config has %d keys, want %d: %v", len(cfg), len(want), cfg)
	}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("%s = %q, want %q", k, cfg[k], v)
		}
	}
}

func TestConnectorConfigNonCDB(t *testing.T) {
	o := testConnectorOptions()
	o.PDB, o.User, o.Database, o.Port = "", "DBZUSER", "ORCL", 0
	o.SchemaHistoryTopic = "orcl.history"
	cfg, err := ConnectorConfig(o)
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	if _, ok := cfg["database.pdb.name"]; ok {
		t.Fatalf("a non-CDB config names a PDB: %v", cfg)
	}
	if cfg["database.port"] != "1521" || cfg["schema.history.internal.kafka.topic"] != "orcl.history" {
		t.Fatalf("port/topic = %q/%q", cfg["database.port"], cfg["schema.history.internal.kafka.topic"])
	}
}

// TestConnectorConfigEscapesDollar: '$' is legal in an Oracle name and a regex
// anchor in Debezium's include list, so it is escaped.
func TestConnectorConfigEscapesDollar(t *testing.T) {
	o := testConnectorOptions()
	o.Tables = []string{"APP$.ORD$ITEMS", "APP.T#1"}
	cfg, err := ConnectorConfig(o)
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	if got := cfg["table.include.list"]; got != `APP\$.ORD\$ITEMS,APP.T#1` {
		t.Fatalf("table.include.list = %q", got)
	}
	re := regexp.MustCompile("^(?:" + strings.Split(cfg["table.include.list"], ",")[0] + ")$")
	if !re.MatchString("APP$.ORD$ITEMS") {
		t.Fatal("the escaped pattern does not match its table")
	}
}

// TestConnectorConfigPassesQ4aShapes checks the config against the rules
// connect_validate_config and connect_create_connector apply before Connect
// sees it: key names, value sizes, only ${secret:...} references, and no
// plaintext credential.
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
}

func TestConnectorConfigRefusals(t *testing.T) {
	tests := []struct {
		name string
		mod  func(*ConnectorOptions)
	}{
		{"empty host", func(o *ConnectorOptions) { o.Host = "" }},
		{"host with a scheme", func(o *ConnectorOptions) { o.Host = "oracle://db" }},
		{"host a reference", func(o *ConnectorOptions) { o.Host = "${env:DB_HOST}" }},
		{"host list", func(o *ConnectorOptions) { o.Host = "a,b" }},
		{"port negative", func(o *ConnectorOptions) { o.Port = -1 }},
		{"port too high", func(o *ConnectorOptions) { o.Port = 70000 }},
		{"empty database", func(o *ConnectorOptions) { o.Database = "" }},
		{"lower-case database", func(o *ConnectorOptions) { o.Database = "free" }},
		{"database with a space", func(o *ConnectorOptions) { o.Database = "FR EE" }},
		{"bad PDB", func(o *ConnectorOptions) { o.PDB = "FREE;PDB" }},
		{"empty user", func(o *ConnectorOptions) { o.User = "" }},
		{"SYSTEM user", func(o *ConnectorOptions) { o.User = "SYSTEM" }},
		{"local user with a PDB", func(o *ConnectorOptions) { o.User = "DBZUSER" }},
		{"user with a quote", func(o *ConnectorOptions) { o.User = `C##DBZ"` }},
		{"no secret name", func(o *ConnectorOptions) { o.PasswordSecret = "" }},
		{"secret name with a slash", func(o *ConnectorOptions) { o.PasswordSecret = "../../etc/passwd" }},
		{"empty topic prefix", func(o *ConnectorOptions) { o.TopicPrefix = "" }},
		{"no tables", func(o *ConnectorOptions) { o.Tables = nil }},
		{"table without owner", func(o *ConnectorOptions) { o.Tables = []string{"ORDERS"} }},
		{"lower-case table", func(o *ConnectorOptions) { o.Tables = []string{"piperoom.orders"} }},
		{"no history servers", func(o *ConnectorOptions) { o.SchemaHistoryServers = nil }},
		{"history server without port", func(o *ConnectorOptions) { o.SchemaHistoryServers = []string{"kafka"} }},
		{"history server URL", func(o *ConnectorOptions) { o.SchemaHistoryServers = []string{"PLAINTEXT://kafka:9092"} }},
		{"history topic with a space", func(o *ConnectorOptions) { o.SchemaHistoryTopic = "a b" }},
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
