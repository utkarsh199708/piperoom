package oracle

import (
	"strings"
	"testing"
)

func validConnectorOptions() ConnectorOptions {
	return ConnectorOptions{
		Host: "oracle", Port: 1521, Database: "FREE", PDB: "FREEPDB1",
		User: "C##DBZUSER", PasswordSecret: "source_read_password",
		TopicPrefix: "orders", Tables: []string{"PIPEROOM.ORDERS"},
	}
}

func TestConnectorConfigUsesSecretAndCanary(t *testing.T) {
	cfg, err := ConnectorConfig(validConnectorOptions())
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	if cfg["connector.class"] != ConnectorClass {
		t.Errorf("connector.class = %q", cfg["connector.class"])
	}
	if cfg["database.connection.adapter"] != "logminer" {
		t.Errorf("database.connection.adapter = %q", cfg["database.connection.adapter"])
	}
	if cfg["database.password"] != "$"+"{secret:source_read_password}" {
		t.Errorf("database.password = %q, want secret reference", cfg["database.password"])
	}
	if strings.Contains(cfg["database.password"], "piperoom-dev") {
		t.Fatal("connector config contains a plaintext password")
	}
	if cfg["database.dbname"] != "FREE" || cfg["database.pdb.name"] != "FREEPDB1" {
		t.Errorf("database identity fields = %v", cfg)
	}
	if cfg["table.include.list"] != "PIPEROOM.ORDERS,"+DefaultCanaryTable {
		t.Errorf("table.include.list = %q", cfg["table.include.list"])
	}
}

func TestConnectorConfigRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		edit func(*ConnectorOptions)
	}{
		{"bad host", func(o *ConnectorOptions) { o.Host = "localhost;drop" }},
		{"bad port", func(o *ConnectorOptions) { o.Port = 70000 }},
		{"bad database", func(o *ConnectorOptions) { o.Database = "FREE/OTHER" }},
		{"bad PDB", func(o *ConnectorOptions) { o.PDB = "FREEPDB1;DROP" }},
		{"bad user", func(o *ConnectorOptions) { o.User = "DBZ USER" }},
		{"bad password secret", func(o *ConnectorOptions) { o.PasswordSecret = "bad secret" }},
		{"bad topic prefix", func(o *ConnectorOptions) { o.TopicPrefix = "bad prefix" }},
		{"no source tables", func(o *ConnectorOptions) { o.Tables = nil }},
		{"bad source table", func(o *ConnectorOptions) { o.Tables = []string{"PIPEROOM.ORDERS;DROP"} }},
		{"bad canary", func(o *ConnectorOptions) { o.CanaryTable = "X.Y.Z" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := validConnectorOptions()
			tt.edit(&opts)
			if _, err := ConnectorConfig(opts); err == nil {
				t.Fatal("ConnectorConfig succeeded with bad input")
			}
		})
	}
}

func TestConnectorConfigUsesConfiguredCanary(t *testing.T) {
	opts := validConnectorOptions()
	opts.CanaryTable = "PIPEROOM.IT_CANARY"
	cfg, err := ConnectorConfig(opts)
	if err != nil {
		t.Fatalf("ConnectorConfig: %v", err)
	}
	if cfg["table.include.list"] != "PIPEROOM.ORDERS,PIPEROOM.IT_CANARY" {
		t.Fatalf("table.include.list = %q", cfg["table.include.list"])
	}
}
