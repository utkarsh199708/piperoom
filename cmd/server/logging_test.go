package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/config"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		level, format string
		wantJSON      bool
		wantDebug     bool
		wantInfo      bool
	}{
		{level: "", format: "", wantInfo: true},
		{level: "debug", format: "text", wantDebug: true, wantInfo: true},
		{level: "WARN", format: "JSON", wantJSON: true},
		{level: " error ", format: "json", wantJSON: true},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		log, err := newLogger(config.Config{LogLevel: tt.level, LogFormat: tt.format}, &buf)
		if err != nil {
			t.Fatalf("%q/%q: %v", tt.level, tt.format, err)
		}
		log.Debug("dbg-line")
		log.Info("info-line")
		out := buf.String()
		if got := strings.Contains(out, "dbg-line"); got != tt.wantDebug {
			t.Errorf("%q/%q: debug logged = %v, want %v\n%s", tt.level, tt.format, got, tt.wantDebug, out)
		}
		if got := strings.Contains(out, "info-line"); got != tt.wantInfo {
			t.Errorf("%q/%q: info logged = %v, want %v\n%s", tt.level, tt.format, got, tt.wantInfo, out)
		}
		log.Error("err-line", "k", "v")
		last := strings.TrimSpace(buf.String()[strings.LastIndex(strings.TrimSpace(buf.String()), "\n")+1:])
		var m map[string]any
		isJSON := json.Unmarshal([]byte(last), &m) == nil
		if isJSON != tt.wantJSON {
			t.Errorf("%q/%q: JSON = %v, want %v: %s", tt.level, tt.format, isJSON, tt.wantJSON, last)
		}
	}
}

func TestNewLoggerRefusesBadValues(t *testing.T) {
	for _, c := range []config.Config{
		{LogLevel: "verbose"},
		{LogLevel: "info+2"},
		{LogFormat: "logfmt"},
	} {
		if _, err := newLogger(c, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "PIPEROOM_LOG_") {
			t.Errorf("%+v: err = %v, want an error naming the variable", c, err)
		}
	}
}

// -version prints what -ldflags -X linked in, and "dev" for a plain build.
// It builds the real binary, so it checks the flag and the linker path end to
// end without a network or a database.
func TestVersionFlag(t *testing.T) {
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go tool not found: %v", err)
	}
	dir := t.TempDir()
	for _, tc := range []struct{ ldflags, want string }{
		{"", "dev"},
		{"-X piperoom/internal/version.Version=v1.2.3-test", "v1.2.3-test"},
	} {
		bin := filepath.Join(dir, "server")
		args := []string{"build", "-o", bin}
		if tc.ldflags != "" {
			args = append(args, "-ldflags", tc.ldflags)
		}
		build := exec.Command(gobin, append(args, ".")...)
		build.Env = os.Environ()
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("go build %v: %v\n%s", args, err, out)
		}
		out, err := exec.Command(bin, "-version").Output()
		if err != nil {
			t.Fatalf("server -version: %v", err)
		}
		if got := strings.TrimSpace(string(out)); got != tc.want {
			t.Fatalf("server -version = %q, want %q", got, tc.want)
		}
	}
}
