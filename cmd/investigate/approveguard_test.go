package main

import (
	"errors"
	"strings"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// devCfg is the dev-stack configuration (the config.Load defaults).
func devCfg() config.Config {
	return config.Config{
		RoomDBURL:         "postgres://piperoom:piperoom-dev@localhost:5432/piperoom?sslmode=disable",
		SourceDBURL:       "postgres://piperoom:piperoom-dev@localhost:5432/orders?sslmode=disable",
		WarehouseDBURL:    "postgres://piperoom:piperoom-dev@127.0.0.1:5432/warehouse?sslmode=disable",
		KafkaBrokers:      "localhost:29092",
		ConnectURL:        "http://localhost:8083",
		SchemaRegistryURL: "http://localhost:8081",
		WarehouseKind:     "postgres",
	}
}

func noEnv(string) string { return "" }

func envConfirmed(k string) string {
	if k == approveConfirmEnv {
		return approveConfirmValue
	}
	return ""
}

func TestApproveAllAllowsDevStack(t *testing.T) {
	cases := map[string]struct {
		mutate  func(*config.Config)
		flag    bool
		getenv  func(string) string
		pipe    owners.Pipeline
		havePip bool
	}{
		"flag":                  {flag: true, getenv: noEnv},
		"env":                   {getenv: envConfirmed},
		"schema registry off":   {flag: true, getenv: noEnv, mutate: func(c *config.Config) { c.SchemaRegistryURL = "" }},
		"ipv6 loopback":         {flag: true, getenv: noEnv, mutate: func(c *config.Config) { c.ConnectURL = "http://[::1]:8083" }},
		"several local brokers": {flag: true, getenv: noEnv, mutate: func(c *config.Config) { c.KafkaBrokers = "localhost:29092, 127.0.0.2:29093" }},
		"pipeline w/o conns":    {flag: true, getenv: noEnv, pipe: owners.Pipeline{ID: "orders"}, havePip: true},
		"plaintext explicit":    {flag: true, getenv: noEnv, mutate: func(c *config.Config) { c.KafkaSecurityProtocol = "plaintext" }},
		"keyword dsn":           {flag: true, getenv: noEnv, mutate: func(c *config.Config) { c.SourceDBURL = "host=localhost port=5432 dbname=orders user=u" }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := devCfg()
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			if err := checkApproveAll(cfg, tc.pipe, tc.havePip, tc.flag, tc.getenv); err != nil {
				t.Fatalf("checkApproveAll = %v, want nil", err)
			}
		})
	}
}

func TestApproveAllRefusals(t *testing.T) {
	const secret = "s3cr3t-pw"
	cases := []struct {
		name    string
		mutate  func(*config.Config)
		flag    bool
		getenv  func(string) string
		pipe    owners.Pipeline
		havePip bool
		want    string // substring of the error
	}{
		{name: "no confirmation", getenv: noEnv, want: "confirmation missing"},
		{name: "wrong env value", getenv: func(string) string { return "1" }, want: "confirmation missing"},
		{name: "confirmation does not excuse a remote source", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "postgres://u:" + secret + "@db.prod.example.com:5432/orders" }, want: "SOURCE_DB_URL"},
		{name: "remote connect", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectURL = "https://connect.example.com" }, want: "CONNECT_URL"},
		{name: "remote registry", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SchemaRegistryURL = "https://sr.example.com" }, want: "SCHEMA_REGISTRY_URL"},
		{name: "remote warehouse", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.WarehouseDBURL = "postgres://u:p@10.0.0.5/wh" }, want: "WAREHOUSE_DB_URL"},
		{name: "remote room db", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.RoomDBURL = "postgres://u:p@rooms.internal/rooms" }, want: "PIPEROOM_DB_URL"},
		{name: "mixed brokers", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.KafkaBrokers = "localhost:29092,broker-1.example.com:9092" }, want: "KAFKA_BROKERS"},
		{name: "mixed hosts in one DSN", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "postgres://u:p@localhost:5432,db.example.com:5432/orders" }, want: "SOURCE_DB_URL"},
		{name: "query host override", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "postgres://u:p@localhost/orders?host=db.example.com" }, want: "SOURCE_DB_URL"},
		{name: "keyword host override", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "host=db.example.com dbname=orders" }, want: "SOURCE_DB_URL"},
		{name: "keyword spaces around =, last host wins", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "host=localhost dbname=orders host = db.example.internal" }, want: "SOURCE_DB_URL"},
		{name: "keyword duplicate host", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.WarehouseDBURL = "host=localhost host=db.example.internal dbname=wh" }, want: "WAREHOUSE_DB_URL"},
		{name: "keyword hostaddr", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "host=localhost hostaddr=10.0.0.5 dbname=orders" }, want: "SOURCE_DB_URL"},
		{name: "keyword multi-host", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "host=localhost,db.remote dbname=orders" }, want: "SOURCE_DB_URL"},
		{name: "keyword unix socket", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "host=/var/run/postgresql dbname=orders" }, want: "SOURCE_DB_URL"},
		{name: "dsn that does not parse", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "host=localhost port=notaport password=" + secret }, want: "SOURCE_DB_URL"},
		{name: "https connect on localhost", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectURL = "https://localhost:8443" }, want: "CONNECT_URL is https"},
		{name: "https registry on localhost", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SchemaRegistryURL = "https://127.0.0.1:8443" }, want: "SCHEMA_REGISTRY_URL is https"},
		{name: "connect tls ca file", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectTLSCAFile = "/ca.pem" }, want: "CONNECT_TLS_CA_FILE"},
		{name: "connect tls cert file", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectTLSCertFile = "/c.pem" }, want: "CONNECT_TLS_CERT_FILE"},
		{name: "connect tls key file", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectTLSKeyFile = "/k.pem" }, want: "CONNECT_TLS_KEY_FILE"},
		{name: "registry tls ca file", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SchemaRegistryTLSCAFile = "/ca.pem" }, want: "SCHEMA_REGISTRY_TLS_CA_FILE"},
		{name: "registry tls cert file", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SchemaRegistryTLSCertFile = "/c.pem" }, want: "SCHEMA_REGISTRY_TLS_CERT_FILE"},
		{name: "registry tls key file", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SchemaRegistryTLSKeyFile = "/k.pem" }, want: "SCHEMA_REGISTRY_TLS_KEY_FILE"},
		{name: "userinfo trick", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectURL = "http://localhost@evil.example.com:8083" }, want: "CONNECT_URL"},
		{name: "localhost lookalike", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectURL = "http://localhost.evil.example.com:8083" }, want: "CONNECT_URL"},
		{name: "loopback lookalike", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectURL = "http://127.0.0.1.evil.example.com:8083" }, want: "CONNECT_URL"},
		{name: "name that resolves locally is not trusted", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectURL = "http://kafka-connect:8083" }, want: "CONNECT_URL"},
		{name: "unix socket / empty host", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.SourceDBURL = "postgres:///orders" }, want: "SOURCE_DB_URL"},
		{name: "unparseable url", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectURL = "http://[::1" }, want: "CONNECT_URL"},
		{name: "empty required address", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.KafkaBrokers = "" }, want: "KAFKA_BROKERS is empty"},
		{name: "snowflake warehouse", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.WarehouseKind = "snowflake" }, want: "WAREHOUSE_KIND"},
		{name: "tls kafka", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.KafkaSecurityProtocol = "SASL_SSL" }, want: "KAFKA_SECURITY_PROTOCOL"},
		{name: "kafka login via tunnel", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.KafkaSASLUsername = "svc" }, want: "KAFKA_SASL_USERNAME"},
		{name: "connect password via tunnel", flag: true, getenv: noEnv,
			mutate: func(c *config.Config) { c.ConnectPassword = "x" }, want: "CONNECT_PASSWORD"},
		{name: "pipeline connections block", flag: true, getenv: noEnv, havePip: true,
			pipe: owners.Pipeline{ID: "orders", Connections: owners.Connections{"SOURCE_DB_URL": owners.ConnRef{}}}, want: "connections:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := devCfg()
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			err := checkApproveAll(cfg, tc.pipe, tc.havePip, tc.flag, tc.getenv)
			if err == nil {
				t.Fatal("checkApproveAll = nil, want refusal")
			}
			if !errors.Is(err, errApproveAllRefused) {
				t.Errorf("err %v does not wrap errApproveAllRefused", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %q, want it to contain %q", err, tc.want)
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("err leaks the password: %q", err)
			}
		})
	}
}

// Every bad address is reported, not only the first.
func TestApproveAllReportsAllProblems(t *testing.T) {
	cfg := devCfg()
	cfg.SourceDBURL = "postgres://u:p@a.example.com/x"
	cfg.ConnectURL = "http://b.example.com"
	err := checkApproveAll(cfg, owners.Pipeline{}, false, true, noEnv)
	if err == nil || !strings.Contains(err.Error(), "SOURCE_DB_URL") || !strings.Contains(err.Error(), "CONNECT_URL") {
		t.Fatalf("err = %v, want both settings named", err)
	}
}

// A DSN with no host takes it from PGHOST. The guard checks the parsed result,
// not the string, so the environment cannot smuggle a remote host in.
func TestApproveAllChecksPGHOSTFallback(t *testing.T) {
	cfg := devCfg()
	cfg.SourceDBURL = "dbname=orders user=u"

	t.Setenv("PGHOST", "db.example.internal")
	err := checkApproveAll(cfg, owners.Pipeline{}, false, true, noEnv)
	if err == nil || !strings.Contains(err.Error(), "SOURCE_DB_URL") {
		t.Fatalf("PGHOST=remote: err = %v, want SOURCE_DB_URL refused", err)
	}

	t.Setenv("PGHOST", "localhost")
	if err := checkApproveAll(cfg, owners.Pipeline{}, false, true, noEnv); err != nil {
		t.Fatalf("PGHOST=localhost: err = %v, want nil", err)
	}
}
