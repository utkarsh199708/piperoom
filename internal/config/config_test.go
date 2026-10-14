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
	for _, k := range []string{"PIPEROOM_WATCH_MODE", "PIPEROOM_WATCH_INTERVAL", "PIPEROOM_WATCH_CHECK_TIMEOUT", "PIPEROOM_WATCH_COOLDOWN", "PIPEROOM_WATCH_JOIN_MAX_AGE", "PIPEROOM_WATCH_ESCALATE_AFTER"} {
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
	want := Watch{Mode: WatchLog, Interval: time.Minute, CheckTimeout: 10 * time.Second, Cooldown: time.Hour,
		JoinMaxAge: 2 * time.Hour, EscalateAfter: 10 * time.Minute}
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
	t.Setenv("PIPEROOM_WATCH_JOIN_MAX_AGE", "30m")
	t.Setenv("PIPEROOM_WATCH_ESCALATE_AFTER", "0")
	c := Load()
	if c.WatchMode != "rooms" || c.WatchInterval != "15s" || c.WatchCheckTimeout != "5s" || c.WatchCooldown != "2h" {
		t.Fatalf("env not read: %+v", c)
	}
	w, err := ParseWatch(c)
	if err != nil {
		t.Fatalf("ParseWatch: %v", err)
	}
	want := Watch{Mode: WatchRooms, Interval: 15 * time.Second, CheckTimeout: 5 * time.Second, Cooldown: 2 * time.Hour,
		JoinMaxAge: 30 * time.Minute}
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

func TestLimitsDefaults(t *testing.T) {
	for _, k := range []string{"PIPEROOM_MAX_OPEN_DAYS", "PIPEROOM_ROOM_MAX_EVENTS", "PIPEROOM_ROOM_MAX_BYTES", "PIPEROOM_EVENT_MAX_DATA_BYTES", "PIPEROOM_ARCHIVE_DIR"} {
		t.Setenv(k, "")
	}
	l, err := ParseLimits(Load())
	if err != nil {
		t.Fatalf("ParseLimits(defaults): %v", err)
	}
	want := Limits{MaxOpenDays: 14, RoomMaxEvents: 20000, RoomMaxBytes: 256 << 20, EventMaxDataBytes: 64 << 10}
	if l != want {
		t.Fatalf("ParseLimits(defaults) = %+v, want %+v", l, want)
	}
}

// TestLimitsEnvNames pins the five variable names: each one must reach its field.
func TestLimitsEnvNames(t *testing.T) {
	t.Setenv("PIPEROOM_MAX_OPEN_DAYS", "0")
	t.Setenv("PIPEROOM_ROOM_MAX_EVENTS", "5000")
	t.Setenv("PIPEROOM_ROOM_MAX_BYTES", "1GiB")
	t.Setenv("PIPEROOM_EVENT_MAX_DATA_BYTES", "131072")
	t.Setenv("PIPEROOM_ARCHIVE_DIR", "/var/lib/piperoom/archive/")
	l, err := ParseLimits(Load())
	if err != nil {
		t.Fatalf("ParseLimits: %v", err)
	}
	want := Limits{MaxOpenDays: 0, RoomMaxEvents: 5000, RoomMaxBytes: 1 << 30, EventMaxDataBytes: 128 << 10, ArchiveDir: "/var/lib/piperoom/archive"}
	if l != want {
		t.Fatalf("ParseLimits = %+v, want %+v", l, want)
	}
}

func TestParseLimitsErrors(t *testing.T) {
	tests := []struct {
		name    string
		c       Config
		wantEnv string
	}{
		{"open days negative", Config{MaxOpenDays: "-1"}, "PIPEROOM_MAX_OPEN_DAYS"},
		{"open days decimal", Config{MaxOpenDays: "1.5"}, "PIPEROOM_MAX_OPEN_DAYS"},
		{"open days unit", Config{MaxOpenDays: "14d"}, "PIPEROOM_MAX_OPEN_DAYS"},
		{"open days too many", Config{MaxOpenDays: "3651"}, "PIPEROOM_MAX_OPEN_DAYS"},
		{"events zero", Config{RoomMaxEvents: "0"}, "PIPEROOM_ROOM_MAX_EVENTS"},
		{"events below minimum", Config{RoomMaxEvents: "99"}, "PIPEROOM_ROOM_MAX_EVENTS"},
		{"events above export limit", Config{RoomMaxEvents: "490001"}, "PIPEROOM_ROOM_MAX_EVENTS"},
		{"events word", Config{RoomMaxEvents: "lots"}, "PIPEROOM_ROOM_MAX_EVENTS"},
		{"bytes lower-case unit", Config{RoomMaxBytes: "256mib"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"bytes SI unit", Config{RoomMaxBytes: "256MB"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"bytes inner space", Config{RoomMaxBytes: "256 MiB"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"bytes unit only", Config{RoomMaxBytes: "MiB"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"bytes zero", Config{RoomMaxBytes: "0"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"bytes too small", Config{RoomMaxBytes: "1023KiB"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"bytes too large", Config{RoomMaxBytes: "17GiB"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"bytes overflow", Config{RoomMaxBytes: "9999999999999GiB"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"bytes overflow just past int64/GiB", Config{RoomMaxBytes: "17179869185GiB"}, "PIPEROOM_ROOM_MAX_BYTES"},
		{"data too small", Config{EventMaxDataBytes: "1023"}, "PIPEROOM_EVENT_MAX_DATA_BYTES"},
		{"data too large", Config{EventMaxDataBytes: "17MiB"}, "PIPEROOM_EVENT_MAX_DATA_BYTES"},
		{"data negative", Config{EventMaxDataBytes: "-64KiB"}, "PIPEROOM_EVENT_MAX_DATA_BYTES"},
		{"data over room cap", Config{RoomMaxBytes: "1MiB", EventMaxDataBytes: "2MiB"}, "PIPEROOM_EVENT_MAX_DATA_BYTES"},
		{"archive relative", Config{ArchiveDir: "archive"}, "PIPEROOM_ARCHIVE_DIR"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseLimits(tc.c)
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tc.wantEnv) {
				t.Fatalf("error %q does not name %s", err, tc.wantEnv)
			}
		})
	}
}

func TestParseLimitsBounds(t *testing.T) {
	l, err := ParseLimits(Config{MaxOpenDays: "3650", RoomMaxEvents: "490000", RoomMaxBytes: "16GiB", EventMaxDataBytes: "16MiB"})
	if err != nil {
		t.Fatalf("upper bounds: %v", err)
	}
	if l.MaxOpenDays != 3650 || l.RoomMaxEvents != 490000 || l.RoomMaxBytes != 16<<30 || l.EventMaxDataBytes != 16<<20 {
		t.Fatalf("upper bounds = %+v", l)
	}
	l, err = ParseLimits(Config{MaxOpenDays: " 1 ", RoomMaxEvents: "100", RoomMaxBytes: "1MiB", EventMaxDataBytes: "1KiB"})
	if err != nil {
		t.Fatalf("lower bounds: %v", err)
	}
	if l.MaxOpenDays != 1 || l.RoomMaxEvents != 100 || l.RoomMaxBytes != 1<<20 || l.EventMaxDataBytes != 1<<10 {
		t.Fatalf("lower bounds = %+v", l)
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

// PIPEROOM_SLACK_PAGE_NO_DRIVER: Load splits and trims; ParseSlackMemberIDs
// checks the Slack member ID shape (C12 #130 paging allow-list).
func TestSlackPageNoDriver(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		want    []string
		wantErr bool
	}{
		{"empty", "", nil, false},
		{"spaces only", " , ", nil, false},
		{"one", "U012ABC", []string{"U012ABC"}, false},
		{"several, trimmed, deduplicated", " U012ABC, W0XYZ99 ,,U012ABC", []string{"U012ABC", "W0XYZ99"}, false},
		{"invalid prefix", "C012ABC", nil, true},
		{"invalid lowercase", "u012abc", nil, true},
		{"invalid too short", "U1", nil, true},
		{"invalid mention syntax", "<@U012ABC>", nil, true},
		{"one invalid among valid", "U012ABC,bob", nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PIPEROOM_SLACK_PAGE_NO_DRIVER", tc.env)
			c := Load()
			got, err := ParseSlackMemberIDs("PIPEROOM_SLACK_PAGE_NO_DRIVER", c.SlackPageNoDriver)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if !strings.Contains(err.Error(), "PIPEROOM_SLACK_PAGE_NO_DRIVER") {
					t.Fatalf("error does not name the variable: %v", err)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}
