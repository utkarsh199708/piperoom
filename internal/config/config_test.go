package config

import (
	"strings"
	"testing"
	"time"
)

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

func TestParseTenancyMode(t *testing.T) {
	t.Setenv("PIPEROOM_TENANCY_MODE", "")
	if got := Load().TenancyMode; got != TenancyAuto {
		t.Fatalf("default TenancyMode = %q, want %q", got, TenancyAuto)
	}
	t.Setenv("PIPEROOM_TENANCY_MODE", "teams")
	if got := Load().TenancyMode; got != "teams" {
		t.Fatalf("PIPEROOM_TENANCY_MODE not read: %q", got)
	}
	for in, want := range map[string]string{"": TenancyAuto, "auto": TenancyAuto, " Single ": TenancySingle, "TEAMS": TenancyTeams} {
		got, err := ParseTenancyMode(Config{TenancyMode: in})
		if err != nil || got != want {
			t.Fatalf("ParseTenancyMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"team", "multi", "on"} {
		if _, err := ParseTenancyMode(Config{TenancyMode: bad}); err == nil || !strings.Contains(err.Error(), "PIPEROOM_TENANCY_MODE") {
			t.Fatalf("ParseTenancyMode(%q) err = %v, want one naming PIPEROOM_TENANCY_MODE", bad, err)
		}
	}
}
