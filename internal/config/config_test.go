package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// SCHEMA_REGISTRY_URL set to "" or "off" means no registry; unset keeps the
// dev default.
func TestSchemaRegistryURLOff(t *testing.T) {
	for _, tc := range []struct {
		val, want string
		set       bool
	}{
		{"", "http://localhost:8081", false},
		{"", "", true},
		{"off", "", true},
		{" OFF ", "", true},
		{"https://sr.example:8081", "https://sr.example:8081", true},
	} {
		t.Setenv("SCHEMA_REGISTRY_URL", tc.val) // also restores it after the test
		if !tc.set {
			if err := os.Unsetenv("SCHEMA_REGISTRY_URL"); err != nil {
				t.Fatal(err)
			}
		}
		if got := Load().SchemaRegistryURL; got != tc.want {
			t.Errorf("SCHEMA_REGISTRY_URL=%q (set=%v): got %q, want %q", tc.val, tc.set, got, tc.want)
		}
	}
}

func TestWatchDefaults(t *testing.T) {
	for _, k := range []string{"PIPEROOM_WATCH_MODE", "PIPEROOM_WATCH_INTERVAL", "PIPEROOM_WATCH_CHECK_TIMEOUT", "PIPEROOM_WATCH_COOLDOWN"} {
		t.Setenv(k, "")
	}
	c := Load()
	if c.WatchMode != "log" || c.WatchInterval != "60s" || c.WatchCheckTimeout != "10s" || c.WatchCooldown != "60m" {
		t.Fatalf("defaults = %q %q %q %q", c.WatchMode, c.WatchInterval, c.WatchCheckTimeout, c.WatchCooldown)
	}
	w, err := ParseWatch(c)
	if err != nil {
		t.Fatalf("ParseWatch(defaults): %v", err)
	}
	want := Watch{Mode: WatchLog, Interval: time.Minute, CheckTimeout: 10 * time.Second, Cooldown: time.Hour}
	if w != want {
		t.Fatalf("ParseWatch(defaults) = %+v, want %+v", w, want)
	}
}

// TestWatchEnvNames pins the four variable names: each one must reach its field.
func TestWatchEnvNames(t *testing.T) {
	t.Setenv("PIPEROOM_WATCH_MODE", "rooms")
	t.Setenv("PIPEROOM_WATCH_INTERVAL", "15s")
	t.Setenv("PIPEROOM_WATCH_CHECK_TIMEOUT", "5s")
	t.Setenv("PIPEROOM_WATCH_COOLDOWN", "2h")
	c := Load()
	if c.WatchMode != "rooms" || c.WatchInterval != "15s" || c.WatchCheckTimeout != "5s" || c.WatchCooldown != "2h" {
		t.Fatalf("env not read: %+v", c)
	}
	w, err := ParseWatch(c)
	if err != nil {
		t.Fatalf("ParseWatch: %v", err)
	}
	want := Watch{Mode: WatchRooms, Interval: 15 * time.Second, CheckTimeout: 5 * time.Second, Cooldown: 2 * time.Hour}
	if w != want {
		t.Fatalf("ParseWatch = %+v, want %+v", w, want)
	}
}

func TestParseWatchErrors(t *testing.T) {
	ok := Config{WatchMode: "log", WatchInterval: "60s", WatchCheckTimeout: "10s", WatchCooldown: "60m"}
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantEnv string
	}{
		{"mode unknown", func(c *Config) { c.WatchMode = "loud" }, "PIPEROOM_WATCH_MODE"},
		{"interval not a duration", func(c *Config) { c.WatchInterval = "soon" }, "PIPEROOM_WATCH_INTERVAL"},
		{"interval bare number", func(c *Config) { c.WatchInterval = "60" }, "PIPEROOM_WATCH_INTERVAL"},
		{"interval below minimum", func(c *Config) { c.WatchInterval = "14s" }, "PIPEROOM_WATCH_INTERVAL"},
		{"interval negative", func(c *Config) { c.WatchInterval = "-1m" }, "PIPEROOM_WATCH_INTERVAL"},
		{"timeout zero", func(c *Config) { c.WatchCheckTimeout = "0s" }, "PIPEROOM_WATCH_CHECK_TIMEOUT"},
		{"timeout above statement_timeout", func(c *Config) { c.WatchCheckTimeout = "11s" }, "PIPEROOM_WATCH_CHECK_TIMEOUT"},
		{"cooldown bad", func(c *Config) { c.WatchCooldown = "1 hour" }, "PIPEROOM_WATCH_COOLDOWN"},
		{"cooldown zero", func(c *Config) { c.WatchCooldown = "0" }, "PIPEROOM_WATCH_COOLDOWN"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := ok
			tc.mutate(&c)
			_, err := ParseWatch(c)
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tc.wantEnv) {
				t.Fatalf("error %q does not name %s", err, tc.wantEnv)
			}
		})
	}
}

func TestParseWatchModeCaseAndEmpty(t *testing.T) {
	w, err := ParseWatch(Config{WatchMode: " OFF "})
	if err != nil {
		t.Fatalf("ParseWatch: %v", err)
	}
	if w.Mode != WatchOff || w.Interval != time.Minute {
		t.Fatalf("got %+v, want mode off and default interval", w)
	}
}
