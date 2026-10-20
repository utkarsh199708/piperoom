package config

import (
	"log/slog"
	"strings"
	"testing"
)

func TestParseLog(t *testing.T) {
	tests := []struct {
		name, level, format string
		want                Log
		wantErr             string // substring; "" = no error
	}{
		{name: "defaults", want: Log{Level: slog.LevelInfo, Format: LogFormatText}},
		{name: "debug json", level: "debug", format: "json", want: Log{Level: slog.LevelDebug, Format: LogFormatJSON}},
		{name: "case and spaces", level: " WARN ", format: "Text", want: Log{Level: slog.LevelWarn, Format: LogFormatText}},
		{name: "error", level: "error", want: Log{Level: slog.LevelError, Format: LogFormatText}},
		{name: "offset refused", level: "info+2", wantErr: "PIPEROOM_LOG_LEVEL"},
		{name: "numeric refused", level: "-4", wantErr: "PIPEROOM_LOG_LEVEL"},
		{name: "unknown format", format: "logfmt", wantErr: "PIPEROOM_LOG_FORMAT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseLog(Config{LogLevel: tt.level, LogFormat: tt.format})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ParseLog = %+v, %v; want %+v", got, err, tt.want)
			}
		})
	}
}

func TestLoadLogDefaults(t *testing.T) {
	t.Setenv("PIPEROOM_LOG_LEVEL", "")
	t.Setenv("PIPEROOM_LOG_FORMAT", "")
	t.Setenv("PIPEROOM_DEV_ACK", "")
	c := Load()
	if c.LogLevel != DefaultLogLevel || c.LogFormat != DefaultLogFormat || c.DevAck != "" {
		t.Fatalf("Load: level %q format %q ack %q", c.LogLevel, c.LogFormat, c.DevAck)
	}
}

func TestParseDevAck(t *testing.T) {
	tests := []struct {
		in      string
		want    bool
		wantErr bool
	}{
		{in: "", want: false},
		{in: "  ", want: false},
		{in: DevAckValue, want: true},
		{in: " " + DevAckValue + " ", want: true},
		{in: "1", wantErr: true},
		{in: "true", wantErr: true},
		{in: strings.ToUpper(DevAckValue), wantErr: true},
	}
	for _, tt := range tests {
		got, err := ParseDevAck(Config{DevAck: tt.in})
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("ParseDevAck(%q) = %v, %v; want %v, err %v", tt.in, got, err, tt.want, tt.wantErr)
		}
		if err != nil && !strings.Contains(err.Error(), "PIPEROOM_DEV_ACK") {
			t.Errorf("ParseDevAck(%q) error does not name the variable: %v", tt.in, err)
		}
	}
}

func TestParseFakeAgent(t *testing.T) {
	ok := Config{FakeAgent: FakeAgentScripted, AuthMode: DefaultAuthMode, DevAck: DevAckValue}
	with := func(f func(*Config)) Config { c := ok; f(&c); return c }
	tests := []struct {
		name    string
		c       Config
		want    bool
		wantErr string
	}{
		{name: "unset", c: Config{}, want: false},
		{name: "unset ignores the rest", c: Config{AuthMode: "oidc", WriteMode: "on"}, want: false},
		{name: "scripted in dev", c: ok, want: true},
		{name: "spaces and default auth", c: with(func(c *Config) { c.FakeAgent = " scripted "; c.AuthMode = "" }), want: true},
		{name: "write mode off spelled out", c: with(func(c *Config) { c.WriteMode = "off" }), want: true},
		{name: "typo", c: with(func(c *Config) { c.FakeAgent = "Scripted" }), wantErr: "want"},
		{name: "true", c: with(func(c *Config) { c.FakeAgent = "true" }), wantErr: "want"},
		{name: "oidc", c: with(func(c *Config) { c.AuthMode = "oidc" }), wantErr: "PIPEROOM_AUTH_MODE"},
		{name: "proxy", c: with(func(c *Config) { c.AuthMode = "proxy" }), wantErr: "PIPEROOM_AUTH_MODE"},
		{name: "no ack", c: with(func(c *Config) { c.DevAck = "" }), wantErr: "PIPEROOM_DEV_ACK"},
		{name: "bad ack", c: with(func(c *Config) { c.DevAck = "1" }), wantErr: "PIPEROOM_DEV_ACK"},
		{name: "write mode on", c: with(func(c *Config) { c.WriteMode = "on" }), wantErr: "PIPEROOM_WRITE_MODE"},
		{name: "bad write mode", c: with(func(c *Config) { c.WriteMode = "yes" }), wantErr: "PIPEROOM_WRITE_MODE"},
	}
	for _, tt := range tests {
		got, err := ParseFakeAgent(tt.c)
		if tt.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) || got {
				t.Errorf("%s: ParseFakeAgent = %v, %v; want an error naming %s", tt.name, got, err, tt.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "PIPEROOM_") {
				t.Errorf("%s: error does not name a variable: %v", tt.name, err)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("%s: ParseFakeAgent = %v, %v; want %v", tt.name, got, err, tt.want)
		}
	}
}

func TestLoadFakeAgent(t *testing.T) {
	t.Setenv("PIPEROOM_FAKE_AGENT", FakeAgentScripted)
	if got := Load().FakeAgent; got != FakeAgentScripted {
		t.Errorf("Load().FakeAgent = %q, want %q", got, FakeAgentScripted)
	}
}
