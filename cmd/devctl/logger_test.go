package main

import (
	"bytes"
	"strings"
	"testing"

	"piperoom/internal/config"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name     string
		cfg      config.Config
		wantHas  []string
		wantNone []string
	}{
		{"defaults are text at info", config.Config{}, []string{"level=INFO msg=hi"}, []string{"level=DEBUG"}},
		{"json", config.Config{LogFormat: "json"}, []string{`"msg":"hi"`}, nil},
		{"debug shows debug", config.Config{LogLevel: "debug"}, []string{"level=DEBUG msg=dbg"}, nil},
		{"warn hides info", config.Config{LogLevel: "warn"}, nil, []string{"msg=hi"}},
		{"bad value keeps defaults and says so", config.Config{LogLevel: "loud"}, []string{"ignoring log settings", "PIPEROOM_LOG_LEVEL", "msg=hi"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b bytes.Buffer
			log := newLogger(&b, tt.cfg)
			log.Debug("dbg")
			log.Info("hi")
			for _, w := range tt.wantHas {
				if !strings.Contains(b.String(), w) {
					t.Errorf("output lacks %q:\n%s", w, &b)
				}
			}
			for _, w := range tt.wantNone {
				if strings.Contains(b.String(), w) {
					t.Errorf("output has %q:\n%s", w, &b)
				}
			}
		})
	}
}
