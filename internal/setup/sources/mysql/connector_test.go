package mysql

import (
	"errors"
	"strings"
	"testing"
)

func goodOptions() ConnectorOptions {
	return ConnectorOptions{
		Host: "mysql", Port: 3306, User: "debezium", PasswordSecret: "source_read_password",
		ServerID: 5401, TopicPrefix: "shop", Tables: []string{"shop.orders", "shop.order_items", "audit.log"},
		HistoryBootstrap: "kafka:9092",
	}
}

func TestConnectorConfig(t *testing.T) {
	cfg, err := ConnectorConfig(goodOptions())
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	want := map[string]string{
		"connector.class":       ConnectorClass,
		"database.hostname":     "mysql",
		"database.port":         "3306",
		"database.user":         "debezium",
		"database.password":     "${secret:source_read_password}",
		"database.server.id":    "5401",
		"topic.prefix":          "shop",
		"database.include.list": "shop,audit",
		"table.include.list":    "shop.orders,shop.order_items,audit.log",
		"schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
		"schema.history.internal.kafka.topic":             "schema-history.shop",
		"tasks.max":                                       "1",
	}
	if len(cfg) != len(want) {
		t.Errorf("config has %d keys, want %d: %v", len(cfg), len(want), cfg)
	}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("%s = %q, want %q", k, cfg[k], v)
		}
	}
	for k, v := range cfg {
		if strings.Contains(strings.ToLower(k), "password") && !strings.HasPrefix(v, "${secret:") {
			t.Errorf("%s carries a plaintext value %q", k, v)
		}
	}
}

func TestConnectorConfigCustomHistoryTopic(t *testing.T) {
	o := goodOptions()
	o.HistoryTopic = "history.custom"
	o.HistoryBootstrap = "k1:9092,k2:9092"
	cfg, err := ConnectorConfig(o)
	if err != nil {
		t.Fatal(err)
	}
	if cfg["schema.history.internal.kafka.topic"] != "history.custom" || cfg["schema.history.internal.kafka.bootstrap.servers"] != "k1:9092,k2:9092" {
		t.Fatalf("history keys: %v", cfg)
	}
}

func TestConnectorConfigRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ConnectorOptions)
		want   error
	}{
		{"empty host", func(o *ConnectorOptions) { o.Host = "" }, ErrParams},
		{"secret placeholder as host", func(o *ConnectorOptions) { o.Host = "${secret:x}" }, ErrParams},
		{"host with a slash", func(o *ConnectorOptions) { o.Host = "a/b" }, ErrParams},
		{"port zero", func(o *ConnectorOptions) { o.Port = 0 }, ErrParams},
		{"port too big", func(o *ConnectorOptions) { o.Port = 70000 }, ErrParams},
		{"quote in user", func(o *ConnectorOptions) { o.User = "a'b" }, ErrIdentifier},
		{"reserved user", func(o *ConnectorOptions) { o.User = "root" }, ErrIdentifier},
		{"bad secret name", func(o *ConnectorOptions) { o.PasswordSecret = "a b" }, ErrParams},
		{"server id zero", func(o *ConnectorOptions) { o.ServerID = 0 }, ErrParams},
		{"server id too big", func(o *ConnectorOptions) { o.ServerID = 4294967296 }, ErrParams},
		{"bad topic prefix", func(o *ConnectorOptions) { o.TopicPrefix = "a b" }, ErrParams},
		{"no tables", func(o *ConnectorOptions) { o.Tables = nil }, ErrNoTables},
		{"bad table", func(o *ConnectorOptions) { o.Tables = []string{"shop"} }, ErrIdentifier},
		{"no bootstrap", func(o *ConnectorOptions) { o.HistoryBootstrap = "" }, ErrParams},
		{"bootstrap without port", func(o *ConnectorOptions) { o.HistoryBootstrap = "kafka" }, ErrParams},
		{"bootstrap port too big", func(o *ConnectorOptions) { o.HistoryBootstrap = "kafka:99999" }, ErrParams},
		{"bootstrap list with a bad entry", func(o *ConnectorOptions) { o.HistoryBootstrap = "k1:9092,;x" }, ErrParams},
		{"bad history topic", func(o *ConnectorOptions) { o.HistoryTopic = "a b" }, ErrParams},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := goodOptions()
			tc.mutate(&o)
			if _, err := ConnectorConfig(o); !errors.Is(err, tc.want) {
				t.Fatalf("ConnectorConfig = %v, want %v", err, tc.want)
			}
		})
	}
}
