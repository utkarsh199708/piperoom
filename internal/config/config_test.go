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
