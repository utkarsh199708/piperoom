package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"piperoom/internal/config"
)

// checkSetupSecretsDir is the start-up check behind U1-5d's "a server with
// setup enabled that cannot write the directory refuses to start".

func TestCheckSetupSecretsDirCreatesAndProbes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "secrets")
	if err := checkSetupSecretsDir(dir); err != nil {
		t.Fatalf("check: %v", err)
	}
	fi, err := os.Stat(dir)
	if err != nil || !fi.IsDir() {
		t.Fatalf("the directory was not created: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the probe was not removed: %v", entries)
	}
	// A second start over the existing directory works too.
	if err := checkSetupSecretsDir(dir); err != nil {
		t.Fatalf("check again: %v", err)
	}
}

func TestCheckSetupSecretsDirRefusals(t *testing.T) {
	// A regular file where the directory belongs.
	file := filepath.Join(t.TempDir(), "secrets")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkSetupSecretsDir(file); err == nil {
		t.Fatal("a regular file was accepted as the secrets directory")
	} else if !strings.Contains(err.Error(), "PIPEROOM_SECRETS_DIR") {
		t.Fatalf("the error does not name the variable: %v", err)
	}

	// A path under a regular file can never be created.
	if err := checkSetupSecretsDir(filepath.Join(file, "secrets")); err == nil {
		t.Fatal("a path under a file was accepted")
	}

	if err := checkSetupSecretsDir(""); err == nil {
		t.Fatal("an empty directory was accepted")
	}

	// A directory the process may not write, where the OS enforces it.
	if runtime.GOOS != "windows" && os.Geteuid() != 0 {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		if err := checkSetupSecretsDir(dir); err == nil {
			t.Fatal("a read-only directory was accepted")
		}
	}
}

// TestParseServerRefusesUnwritableSecretsDir: a server started with setup on
// whose directory cannot be written refuses to start, before anything listens.
func TestParseServerRefusesUnwritableSecretsDir(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		HTTPAddr:     "127.0.0.1:8090",
		SetupEnabled: "on",
		WriteMode:    "on",
		SecretsDir:   file,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := parseServer(cfg, serveMode{log: log})
	if err == nil {
		t.Fatal("a server with setup on started with an unwritable secrets directory")
	}
	if !strings.Contains(err.Error(), "PIPEROOM_SECRETS_DIR") {
		t.Fatalf("the error does not name the variable: %v", err)
	}
}
