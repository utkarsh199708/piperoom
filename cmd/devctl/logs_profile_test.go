package main

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Q3d structure (the live check, `docker compose --profile logs up` healthy and
// both tools returning data, needs Docker and is not run here).

type q3Service struct {
	Profiles    []string          `yaml:"profiles"`
	Image       string            `yaml:"image"`
	Command     []string          `yaml:"command"`
	Ports       []string          `yaml:"ports"`
	Volumes     []string          `yaml:"volumes"`
	DependsOn   map[string]any    `yaml:"depends_on"`
	Environment map[string]string `yaml:"environment"`
	Healthcheck struct {
		Test []string `yaml:"test"`
	} `yaml:"healthcheck"`
}

func readQ3Compose(t *testing.T) (root string, svcs map[string]q3Service) {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]q3Service `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse compose file: %v", err)
	}
	return root, doc.Services
}

// pinned reports whether image names an exact version: a tag with a digit,
// never latest.
func pinned(image string) bool {
	_, tag, ok := strings.Cut(image, ":")
	return ok && tag != "latest" && strings.ContainsAny(tag, "0123456789")
}

func TestComposeLogsProfile(t *testing.T) {
	root, svcs := readQ3Compose(t)
	loki, promtail := svcs["loki"], svcs["promtail"]
	for name, s := range map[string]q3Service{"loki": loki, "promtail": promtail} {
		if !slices.Equal(s.Profiles, []string{"logs"}) {
			t.Errorf("%s profiles = %v, want [logs]: `devctl up` must not start it", name, s.Profiles)
		}
		if !pinned(s.Image) {
			t.Errorf("%s image %q: want a pinned version", name, s.Image)
		}
		if len(s.Healthcheck.Test) == 0 {
			t.Errorf("%s has no healthcheck: `up --wait` could not tell it is ready", name)
		}
		for _, v := range s.Volumes {
			src, _, _ := strings.Cut(v, ":")
			if strings.HasPrefix(src, "./") {
				if _, err := os.Stat(filepath.Join(root, "dev", src)); err != nil {
					t.Errorf("%s mounts %s: %v", name, src, err)
				}
			}
		}
	}
	if !slices.Equal(loki.Ports, []string{"127.0.0.1:3100:3100"}) {
		t.Errorf("loki ports = %v, want 127.0.0.1:3100:3100 (no auth)", loki.Ports)
	}
	if len(promtail.Ports) != 0 {
		t.Errorf("promtail publishes %v: nothing outside needs it", promtail.Ports)
	}
	if !slices.Contains(promtail.Volumes, "/var/run/docker.sock:/var/run/docker.sock:ro") {
		t.Errorf("promtail volumes = %v: want the Docker socket read-only", promtail.Volumes)
	}
	if _, ok := promtail.DependsOn["loki"]; !ok {
		t.Error("promtail does not wait for loki")
	}

	// Promtail tails the production worker only, and labels the level.
	raw, err := os.ReadFile(filepath.Join(root, "dev", "logs", "promtail.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var pc struct {
		Clients []struct {
			URL string `yaml:"url"`
		} `yaml:"clients"`
		ScrapeConfigs []struct {
			DockerSD []struct {
				Host    string `yaml:"host"`
				Filters []struct {
					Name   string   `yaml:"name"`
					Values []string `yaml:"values"`
				} `yaml:"filters"`
			} `yaml:"docker_sd_configs"`
			PipelineStages []map[string]map[string]any `yaml:"pipeline_stages"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal(raw, &pc); err != nil {
		t.Fatalf("parse promtail.yml: %v", err)
	}
	if len(pc.Clients) != 1 || pc.Clients[0].URL != "http://loki:3100/loki/api/v1/push" {
		t.Errorf("promtail clients = %+v", pc.Clients)
	}
	if len(pc.ScrapeConfigs) != 1 || len(pc.ScrapeConfigs[0].DockerSD) != 1 {
		t.Fatalf("promtail scrape configs = %+v, want one docker_sd job", pc.ScrapeConfigs)
	}
	f := pc.ScrapeConfigs[0].DockerSD[0].Filters
	if len(f) != 1 || f[0].Name != "label" || !slices.Equal(f[0].Values, []string{"com.docker.compose.project=piperoom-dev", "com.docker.compose.service=connect"}) {
		t.Errorf("promtail docker filters = %+v, want project piperoom-dev and service connect", f)
	}
	var firstline, levelRE string
	for _, st := range pc.ScrapeConfigs[0].PipelineStages {
		if m, ok := st["multiline"]; ok {
			firstline, _ = m["firstline"].(string)
		}
		if r, ok := st["regex"]; ok {
			levelRE, _ = r["expression"].(string)
		}
	}
	// Promtail's regexes are Go's: the lines below are what the worker prints.
	first := regexp.MustCompile(firstline)
	level := regexp.MustCompile(levelRE)
	lines := []struct {
		line, level string
		first       bool
	}{
		{"2026-10-23 10:00:00,123 ERROR  Postgres|pg|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]", "ERROR", true},
		{"[2026-10-23 10:00:01,456] WARN [pg-orders-connector|task-0] Task threw an uncaught exception (org.apache.kafka.connect.runtime.WorkerTask:234)", "WARN", true},
		{"2026-10-23T10:00:02.789 INFO  Postgres|pg|streaming  Processing messages   [io.debezium.connector.postgresql.PostgresStreamingChangeEventSource]", "INFO", true},
		{"\tat io.debezium.connector.postgresql.PostgresConnectorTask.start(PostgresConnectorTask.java:140)", "", false},
		{"org.postgresql.util.PSQLException: ERROR: permission denied for table orders", "", false},
	}
	for _, l := range lines {
		if got := first.MatchString(l.line); got != l.first {
			t.Errorf("multiline firstline on %q = %v, want %v", l.line, got, l.first)
		}
		got := ""
		if m := level.FindStringSubmatch(l.line); m != nil {
			got = m[level.SubexpIndex("level")]
		}
		if got != l.level {
			t.Errorf("level of %q = %q, want %q", l.line, got, l.level)
		}
	}

	raw, err = os.ReadFile(filepath.Join(root, "dev", "logs", "loki.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var lc struct {
		AuthEnabled bool `yaml:"auth_enabled"`
		Server      struct {
			Port int `yaml:"http_listen_port"`
		} `yaml:"server"`
		Analytics struct {
			ReportingEnabled bool `yaml:"reporting_enabled"`
		} `yaml:"analytics"`
	}
	if err := yaml.Unmarshal(raw, &lc); err != nil {
		t.Fatalf("parse loki.yml: %v", err)
	}
	if lc.AuthEnabled || lc.Server.Port != 3100 || lc.Analytics.ReportingEnabled {
		t.Errorf("loki.yml = %+v, want no auth, port 3100, no usage reporting", lc)
	}
}

// The connect service loads the JMX exporter the image ships, on loopback.
func TestComposeConnectJMXExporter(t *testing.T) {
	root, svcs := readQ3Compose(t)
	connect := svcs["connect"]
	opts := connect.Environment["KAFKA_OPTS"]
	m := regexp.MustCompile(`^-javaagent:(\S+\.jar)=(\d+):(\S+)$`).FindStringSubmatch(opts)
	if m == nil {
		t.Fatalf("connect KAFKA_OPTS = %q, want -javaagent:<jar>=<port>:<rules>", opts)
	}
	if m[2] != "9404" || !slices.Contains(connect.Ports, "127.0.0.1:9404:9404") {
		t.Errorf("exporter port %s, ports %v: want 9404 on host loopback", m[2], connect.Ports)
	}
	df, err := os.ReadFile(filepath.Join(root, "dev", "connect", "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	d := string(df)
	if !strings.Contains(d, "-o "+m[1]) || !strings.Contains(d, "sha256sum -c") {
		t.Errorf("dev/connect/Dockerfile does not download %s with a checksum check", m[1])
	}
	if !strings.Contains(d, "COPY jmx-exporter.yml "+m[3]) {
		t.Errorf("dev/connect/Dockerfile does not put the rules at %s", m[3])
	}
	if !regexp.MustCompile(`ARG JMX_EXPORTER_VERSION=\d+\.\d+\.\d+\n`).MatchString(d) {
		t.Error("JMX exporter version is not pinned")
	}
	if _, ok := svcs["connect-staging"].Environment["KAFKA_OPTS"]; ok {
		t.Error("connect-staging sets KAFKA_OPTS: its port 9404 is not published and production already uses it")
	}
}
