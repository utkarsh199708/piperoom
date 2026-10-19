package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestClickHouseInitStartsProfileAndCreatesOrdersSink(t *testing.T) {
	var composeCalls int
	var gotSQL string
	st := &stack{
		composeFn: func(_ context.Context, _ map[string]string, args ...string) error {
			composeCalls++
			want := []string{"--profile", "clickhouse", "up", "-d", "--wait", "clickhouse"}
			if strings.Join(args, " ") != strings.Join(want, " ") {
				t.Fatalf("compose args = %v, want %v", args, want)
			}
			return nil
		},
		clickhouseExecFn: func(_ context.Context, query string) error {
			gotSQL = query
			return nil
		},
	}

	if err := st.clickhouseInit(context.Background()); err != nil {
		t.Fatalf("clickhouseInit: %v", err)
	}
	if composeCalls != 1 {
		t.Fatalf("compose calls = %d, want 1", composeCalls)
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS analytics.orders",
		"ENGINE = ReplacingMergeTree(version)",
		"CREATE TABLE IF NOT EXISTS analytics.orders_kafka",
		"kafka_topic_list = 'pg.public.orders'",
		"kafka_format = 'AvroConfluent'",
		"format_avro_schema_registry_url = 'http://schema-registry:8081'",
		"CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_mv",
		"TO analytics.orders",
	} {
		if !strings.Contains(gotSQL, want) {
			t.Errorf("ClickHouse init SQL missing %q", want)
		}
	}
}

func TestClickHouseComposeProfileAndReaderGrants(t *testing.T) {
	st := repoStack(t)
	raw, err := os.ReadFile(filepath.Join(st.root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]struct {
			Profiles    []string          `yaml:"profiles"`
			Image       string            `yaml:"image"`
			Ports       []string          `yaml:"ports"`
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	svc, ok := doc.Services["clickhouse"]
	if !ok {
		t.Fatal("clickhouse service missing")
	}
	if strings.Join(svc.Profiles, ",") != "clickhouse" {
		t.Fatalf("profiles = %v, want [clickhouse]", svc.Profiles)
	}
	if svc.Image != "clickhouse/clickhouse-server:25.8" {
		t.Fatalf("image = %q, want a pinned ClickHouse server release tag", svc.Image)
	}
	if len(svc.Ports) != 1 || svc.Ports[0] != "127.0.0.1:9001:9000" {
		t.Fatalf("ports = %v, want native port bound to host loopback", svc.Ports)
	}
	if svc.Environment["CLICKHOUSE_DB"] != "analytics" ||
		svc.Environment["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"] != "1" {
		t.Fatalf("ClickHouse environment does not initialize analytics and access management: %v", svc.Environment)
	}

	initSQL, err := os.ReadFile(filepath.Join(st.root, "dev", "clickhouse", "init", "01-reader.sql"))
	if err != nil {
		t.Fatal(err)
	}
	normalized := strings.ToLower(string(initSQL))
	for _, want := range []string{
		"create settings profile if not exists piperoom_reader",
		"settings readonly = 2",
		"create user if not exists piperoom_reader",
		"grant select on analytics.* to piperoom_reader",
		"grant select on system.query_log to piperoom_reader",
	} {
		if !strings.Contains(normalized, want) {
			t.Errorf("reader initialization SQL missing %q", want)
		}
	}
	if strings.Contains(normalized, "readonly = 1") {
		t.Fatal("reader initialization must use readonly=2, not readonly=1")
	}
	if got := strings.Count(normalized, "grant "); got != 2 {
		t.Fatalf("reader has %d grants, want exactly the two SELECT grants", got)
	}
}
