package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseCostBuiltinDefault(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", BuiltinModelPrices},
		{"  ", BuiltinModelPrices},
		{"off", ""},
		{"OFF", ""},
		{"/etc/prices.json", "/etc/prices.json"},
	}
	for _, tc := range cases {
		got, err := ParseCost(Config{ModelPrices: tc.in})
		if err != nil {
			t.Fatalf("ParseCost(%q): %v", tc.in, err)
		}
		if got.PricesFile != tc.want {
			t.Errorf("ParseCost(%q).PricesFile = %q, want %q", tc.in, got.PricesFile, tc.want)
		}
	}
	// A cost cap needs a table: "off" refuses it, the default accepts it.
	if _, err := ParseCost(Config{ModelPrices: "off", PipelineDailyCostUSD: "5"}); err == nil {
		t.Error("ParseCost(off, cap) = nil error, want refusal")
	}
	if _, err := ParseCost(Config{PipelineDailyCostUSD: "5"}); err != nil {
		t.Errorf("ParseCost(default table, cap): %v", err)
	}
}

func TestParseWatchEscalate(t *testing.T) {
	base := Config{WatchInterval: "60s"}
	for _, tc := range []struct {
		v       string
		want    time.Duration
		wantErr string
	}{
		{"", 10 * time.Minute, ""},
		{"0", 0, ""},
		{"15m", 15 * time.Minute, ""},
		{"30s", 0, "at least PIPEROOM_WATCH_INTERVAL"},
		{"-1m", 0, "must not be negative"},
		{"soon", 0, "not a duration"},
	} {
		c := base
		c.WatchEscalateAfter = tc.v
		w, err := ParseWatch(c)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "PIPEROOM_WATCH_ESCALATE_AFTER") {
				t.Errorf("ParseWatch(escalate=%q) err = %v, want %q naming the var", tc.v, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseWatch(escalate=%q): %v", tc.v, err)
		}
		if w.EscalateAfter != tc.want {
			t.Errorf("ParseWatch(escalate=%q).EscalateAfter = %v, want %v", tc.v, w.EscalateAfter, tc.want)
		}
	}
	if _, err := ParseWatch(Config{WatchJoinMaxAge: "0"}); err == nil {
		t.Error("ParseWatch(join max age 0) = nil error, want refusal (must be > 0)")
	}
}

func TestParseNotify(t *testing.T) {
	for _, tc := range []struct {
		url, after string
		wantAfter  time.Duration
		wantErr    string
	}{
		{"", "", 5 * time.Minute, ""},
		{"", "0", 0, ""},
		{"https://hooks.example.com/piperoom", "2m", 2 * time.Minute, ""},
		{"http://127.0.0.1:9999/hook", "", 5 * time.Minute, ""},
		{"http://localhost:9999/hook", "", 5 * time.Minute, ""},
		{"http://hooks.example.com/hook", "", 0, "want https"},
		{"https://user:pw@hooks.example.com/", "", 0, "must not carry a user"},
		{"hooks.example.com", "", 0, "not an absolute URL"},
		{"", "later", 0, "PIPEROOM_PAGE_UNDRIVEN_AFTER"},
	} {
		n, err := ParseNotify(Config{NotifyWebhookURL: tc.url, PageUndrivenAfter: tc.after})
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ParseNotify(%q, %q) err = %v, want %q", tc.url, tc.after, err, tc.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), "pw") {
				t.Errorf("error echoes the URL password: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseNotify(%q, %q): %v", tc.url, tc.after, err)
		}
		if n.PageUndrivenAfter != tc.wantAfter {
			t.Errorf("ParseNotify(%q, %q).PageUndrivenAfter = %v, want %v", tc.url, tc.after, n.PageUndrivenAfter, tc.wantAfter)
		}
	}
}

func TestQ0Defaults(t *testing.T) {
	for _, k := range []string{"PIPEROOM_DB_URL", "CONNECT_WORKER_LOG_FILE", "PIPEROOM_PAGE_UNDRIVEN_AFTER"} {
		t.Setenv(k, "")
	}
	c := Load()
	if !strings.Contains(c.RoomDBURL, "localhost:5433/piperoom") {
		t.Errorf("RoomDBURL default = %q, want the roomdb service on :5433", c.RoomDBURL)
	}
	if c.ConnectWorkerLogFile != DefaultConnectWorkerLogFile {
		t.Errorf("ConnectWorkerLogFile default = %q", c.ConnectWorkerLogFile)
	}
	t.Setenv("CONNECT_WORKER_LOG_FILE", "Off")
	if got := Load().ConnectWorkerLogFile; got != "" {
		t.Errorf("CONNECT_WORKER_LOG_FILE=Off -> %q, want \"\"", got)
	}
}
