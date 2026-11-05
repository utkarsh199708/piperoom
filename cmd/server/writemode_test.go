package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
)

func TestWriteModeSetup(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		demo        bool
		wantOn      bool
		wantLog     string // a line the start-up log must hold
	}{
		{"unset is off", "", false, false, `level=WARN msg="server: write mode off: fixes will be approved but not executed`},
		{"off", "off", false, false, "level=WARN"},
		{"on", "on", false, true, "write mode on: fully approved fixes run"},
		{"demo forces on over off", "off", true, true, "--demo runs fixes on fake clients only"},
		{"demo with on", "on", true, true, "--demo runs fixes on fake clients only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PIPEROOM_WRITE_MODE", tc.value)
			var buf bytes.Buffer
			on, err := writeModeSetup(config.Load(), tc.demo, slog.New(slog.NewTextHandler(&buf, nil)))
			if err != nil {
				t.Fatal(err)
			}
			if on != tc.wantOn || !strings.Contains(buf.String(), tc.wantLog) {
				t.Fatalf("on = %v, log %q; want %v and %q", on, buf.String(), tc.wantOn, tc.wantLog)
			}
			if tc.wantOn && strings.Contains(buf.String(), "level=WARN") {
				t.Fatalf("write mode on logged a warning: %s", buf.String())
			}
		})
	}
}

// Any value but off or on refuses the start, --demo included, and the error
// names the variable.
func TestWriteModeSetupRefusesBadValue(t *testing.T) {
	for _, v := range []string{"true", "1", "ON", "yes", "of"} {
		for _, demo := range []bool{false, true} {
			cfg := config.Load()
			cfg.WriteMode = v
			if _, err := writeModeSetup(cfg, demo, discardLog()); err == nil || !strings.Contains(err.Error(), "PIPEROOM_WRITE_MODE") {
				t.Errorf("%q demo=%v: err = %v, want a refusal naming PIPEROOM_WRITE_MODE", v, demo, err)
			}
		}
	}
}

// The whole start stops on a bad value, --demo included (TestRunHelper runs
// run() with --demo in its own process), before anything listens.
func TestStartRefusesBadWriteMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRunHelper$")
	cmd.Env = append(os.Environ(),
		"PIPEROOM_TEST_RUN=1",
		"PIPEROOM_WRITE_MODE=true",
		"PIPEROOM_HTTP_ADDR=127.0.0.1:0",
		"OWNERS_FILE=../../dev/owners.yaml",
		"USERS_FILE=../../dev/users.yaml",
	)
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "RUN ERROR") || !strings.Contains(string(out), "PIPEROOM_WRITE_MODE") {
		t.Fatalf("PIPEROOM_WRITE_MODE=true: err %v, want a start-up error naming the variable\n%s", err, out)
	}
}
