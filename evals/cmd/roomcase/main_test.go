package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixtures = "../../testdata/rooms/"

func TestRunWritesPendingFile(t *testing.T) {
	out := t.TempDir()
	var stdout, stderr bytes.Buffer
	err := run([]string{"-export", fixtures + "orders-confirmed.json", "-owners", "../../../dev/owners.yaml", "-out", out}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, stderr.String())
	}
	files, _ := filepath.Glob(filepath.Join(out, "pending", "orders-*.yaml"))
	if len(files) != 1 || !strings.Contains(stdout.String(), files[0]) {
		t.Fatalf("files %v, stdout %q", files, stdout.String())
	}
	// A second export of the same incident is refused; -force writes it.
	if err := run([]string{"-export", fixtures + "orders-confirmed.json", "-owners", "../../../dev/owners.yaml", "-out", out}, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("second export: %v", err)
	}
	if err := run([]string{"-force", "-export", fixtures + "orders-confirmed.json", "-owners", "../../../dev/owners.yaml", "-out", out}, &stdout, &stderr); err != nil {
		t.Fatalf("-force: %v", err)
	}
}

func TestRunRefusals(t *testing.T) {
	out := t.TempDir()
	own := "../../../dev/owners.yaml"
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"no flags":      {nil, "-export and -owners are required"},
		"extra args":    {[]string{"-export", "x", "-owners", own, "stray"}, "unexpected arguments"},
		"missing file":  {[]string{"-export", filepath.Join(out, "nope.json"), "-owners", own, "-out", out}, "open export"},
		"shadow room":   {[]string{"-export", fixtures + "orders-shadow.json", "-owners", own, "-out", out}, "shadow rooms"},
		"unlabelled":    {[]string{"-export", fixtures + "orders-unlabelled.json", "-owners", own, "-out", out}, "no human label"},
		"bad owners":    {[]string{"-export", fixtures + "orders-confirmed.json", "-owners", filepath.Join(out, "none.yaml"), "-out", out}, "load owners"},
		"hash mismatch": {[]string{"-export", tamper(t, out), "-owners", own, "-out", out}, "hash mismatch"},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := run(tc.args, &buf, &buf)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	if files, _ := filepath.Glob(filepath.Join(out, "pending", "*.yaml")); len(files) != 0 {
		t.Errorf("a refused export wrote %v", files)
	}
}

// tamper writes a copy of the confirmed export with one word changed.
func tamper(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(fixtures + "orders-confirmed.json")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "tampered.json")
	if err := os.WriteFile(p, bytes.Replace(b, []byte("orders stale"), []byte("orders stalf"), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
