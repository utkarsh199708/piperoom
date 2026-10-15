package config

import (
	"strings"
	"testing"
	"time"
)

func TestFWaveDefaults(t *testing.T) {
	for _, k := range []string{
		"PIPEROOM_WATCH_MAX_OPEN_ROOMS", "PIPEROOM_TRIAGE_PASS", "PIPEROOM_TRIAGE_PASS_MODEL",
		"PIPEROOM_TRIAGE_PASS_TIMEOUT", "PIPEROOM_POSTMORTEM_MODEL", "PIPEROOM_CHANGES_RETENTION_DAYS",
		"DEBEZIUM_METRICS_URL", "DEBEZIUM_METRICS_PREFIX",
	} {
		t.Setenv(k, "")
	}
	c := Load()
	if n, err := ParseWatchMaxOpenRooms(c); err != nil || n != 6 {
		t.Errorf("ParseWatchMaxOpenRooms = %d, %v; want 6", n, err)
	}
	tp, err := ParseTriagePass(c)
	if want := (TriagePass{On: true, Model: "claude-haiku-5-5", Timeout: 40 * time.Second}); err != nil || tp != want {
		t.Errorf("ParseTriagePass = %+v, %v; want %+v", tp, err, want)
	}
	if m, err := ParsePostmortemModel(c); err != nil || m != PostmortemModelAuto {
		t.Errorf("ParsePostmortemModel = %q, %v; want auto", m, err)
	}
	if d, err := ParseChangesRetentionDays(c); err != nil || d != 30 {
		t.Errorf("ParseChangesRetentionDays = %d, %v; want 30", d, err)
	}
	dm, err := ParseDebeziumMetrics(c)
	if want := (DebeziumMetrics{Prefix: "debezium_metrics_"}); err != nil || dm != want {
		t.Errorf("ParseDebeziumMetrics = %+v, %v; want %+v", dm, err, want)
	}
}

// TestFWaveEnvNames pins every new variable name: each one must reach its field.
func TestFWaveEnvNames(t *testing.T) {
	t.Setenv("PIPEROOM_WATCH_MAX_OPEN_ROOMS", "9")
	t.Setenv("PIPEROOM_TRIAGE_PASS", "OFF")
	t.Setenv("PIPEROOM_TRIAGE_PASS_MODEL", "claude-sonnet-5-5")
	t.Setenv("PIPEROOM_TRIAGE_PASS_TIMEOUT", "90s")
	t.Setenv("PIPEROOM_POSTMORTEM_MODEL", "Off")
	t.Setenv("PIPEROOM_CHANGES_RETENTION_DAYS", "0")
	t.Setenv("DEBEZIUM_METRICS_URL", "http://connect:9404/metrics")
	t.Setenv("DEBEZIUM_METRICS_PREFIX", "dbz_")
	c := Load()
	if n, err := ParseWatchMaxOpenRooms(c); err != nil || n != 9 {
		t.Errorf("max open rooms = %d, %v", n, err)
	}
	tp, err := ParseTriagePass(c)
	if want := (TriagePass{On: false, Model: "claude-sonnet-5-5", Timeout: 90 * time.Second}); err != nil || tp != want {
		t.Errorf("triage pass = %+v, %v; want %+v", tp, err, want)
	}
	if m, err := ParsePostmortemModel(c); err != nil || m != PostmortemModelOff {
		t.Errorf("postmortem model = %q, %v", m, err)
	}
	if d, err := ParseChangesRetentionDays(c); err != nil || d != 0 {
		t.Errorf("changes retention = %d, %v", d, err)
	}
	dm, err := ParseDebeziumMetrics(c)
	if want := (DebeziumMetrics{URL: "http://connect:9404/metrics", Prefix: "dbz_"}); err != nil || dm != want {
		t.Errorf("debezium metrics = %+v, %v; want %+v", dm, err, want)
	}
}

func TestFWaveParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		c       Config
		parse   func(Config) error
		wantEnv string
	}{
		{"max open rooms zero", Config{WatchMaxOpenRooms: "0"}, func(c Config) error { _, err := ParseWatchMaxOpenRooms(c); return err }, "PIPEROOM_WATCH_MAX_OPEN_ROOMS"},
		{"max open rooms word", Config{WatchMaxOpenRooms: "six"}, func(c Config) error { _, err := ParseWatchMaxOpenRooms(c); return err }, "PIPEROOM_WATCH_MAX_OPEN_ROOMS"},
		{"max open rooms huge", Config{WatchMaxOpenRooms: "1001"}, func(c Config) error { _, err := ParseWatchMaxOpenRooms(c); return err }, "PIPEROOM_WATCH_MAX_OPEN_ROOMS"},
		{"triage pass yes", Config{TriagePass: "yes"}, func(c Config) error { _, err := ParseTriagePass(c); return err }, "PIPEROOM_TRIAGE_PASS"},
		{"triage model spaces", Config{TriagePassModel: "claude haiku"}, func(c Config) error { _, err := ParseTriagePass(c); return err }, "PIPEROOM_TRIAGE_PASS_MODEL"},
		{"triage timeout bad", Config{TriagePassTimeout: "40"}, func(c Config) error { _, err := ParseTriagePass(c); return err }, "PIPEROOM_TRIAGE_PASS_TIMEOUT"},
		{"triage timeout zero", Config{TriagePassTimeout: "0s"}, func(c Config) error { _, err := ParseTriagePass(c); return err }, "PIPEROOM_TRIAGE_PASS_TIMEOUT"},
		{"triage timeout over max", Config{TriagePassTimeout: "91s"}, func(c Config) error { _, err := ParseTriagePass(c); return err }, "PIPEROOM_TRIAGE_PASS_TIMEOUT"},
		{"postmortem model unknown", Config{PostmortemModel: "always"}, func(c Config) error { _, err := ParsePostmortemModel(c); return err }, "PIPEROOM_POSTMORTEM_MODEL"},
		{"changes retention negative", Config{ChangesRetentionDays: "-1"}, func(c Config) error { _, err := ParseChangesRetentionDays(c); return err }, "PIPEROOM_CHANGES_RETENTION_DAYS"},
		{"changes retention huge", Config{ChangesRetentionDays: "3651"}, func(c Config) error { _, err := ParseChangesRetentionDays(c); return err }, "PIPEROOM_CHANGES_RETENTION_DAYS"},
		{"debezium url scheme", Config{DebeziumMetricsURL: "ftp://x/metrics"}, func(c Config) error { _, err := ParseDebeziumMetrics(c); return err }, "DEBEZIUM_METRICS_URL"},
		{"debezium url no host", Config{DebeziumMetricsURL: "http:///metrics"}, func(c Config) error { _, err := ParseDebeziumMetrics(c); return err }, "DEBEZIUM_METRICS_URL"},
		{"debezium url user info", Config{DebeziumMetricsURL: "http://u:secret@x/metrics"}, func(c Config) error { _, err := ParseDebeziumMetrics(c); return err }, "DEBEZIUM_METRICS_URL"},
		{"debezium prefix digit", Config{DebeziumMetricsPrefix: "1dbz"}, func(c Config) error { _, err := ParseDebeziumMetrics(c); return err }, "DEBEZIUM_METRICS_PREFIX"},
		{"debezium prefix dash", Config{DebeziumMetricsPrefix: "dbz-"}, func(c Config) error { _, err := ParseDebeziumMetrics(c); return err }, "DEBEZIUM_METRICS_PREFIX"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.parse(tc.c)
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tc.wantEnv) {
				t.Fatalf("error %q does not name %s", err, tc.wantEnv)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error %q echoes user info", err)
			}
		})
	}
}
