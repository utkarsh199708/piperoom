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
