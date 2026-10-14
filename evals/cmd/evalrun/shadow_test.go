package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/evals"
)

const (
	shadowSample = "../../testdata/shadow-run.json"
	devOwners    = "../../../dev/owners.yaml"
)

func TestRunFromShadowStdout(t *testing.T) {
	var out, errOut bytes.Buffer
	code, err := run(flags{fromShadow: shadowSample, owners: devOwners}, &out, &errOut)
	if err != nil || code != 0 {
		t.Fatalf("code %d err %v stderr %q", code, err, errOut.String())
	}
	// stdout is the scenario and nothing else, so it can be piped to a file.
	sc, err := evals.ParseScenario(out.Bytes())
	if err != nil || sc.Name != "shadow-room_8f3a1c7e" {
		t.Fatalf("stdout is not the scenario: %v %q", err, sc.Name)
	}
}

// The written file passes the same check as `evalrun -check` on a directory.
func TestRunFromShadowOutPassesCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "new.yaml")
	var out bytes.Buffer
	if code, err := run(flags{fromShadow: shadowSample, owners: devOwners, out: path}, &out, &out); err != nil || code != 0 {
		t.Fatalf("code %d err %v out %q", code, err, out.String())
	}
	out.Reset()
	code, err := run(flags{scenarios: dir, check: true}, &out, &out)
	if err != nil || code != 0 || !strings.Contains(out.String(), "1 scenario(s) OK") {
		t.Fatalf("-check on the output: code %d err %v out %q", code, err, out.String())
	}
}

func TestRunFromShadowDoesNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.yaml")
	want := []byte("keep this file")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	code, err := run(flags{fromShadow: shadowSample, owners: devOwners, out: path}, &out, &out)
	if err == nil || code == 0 || !strings.Contains(err.Error(), path) {
		t.Fatalf("code %d err %v, want refusal naming %s", code, err, path)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || !bytes.Equal(got, want) {
		t.Fatalf("existing file changed: got %q err %v", got, readErr)
	}
}

func TestRunFromShadowErrors(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"run":`), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]flags{
		"missing file":    {fromShadow: filepath.Join(dir, "none.json"), owners: devOwners},
		"malformed JSON":  {fromShadow: bad, owners: devOwners},
		"missing owners":  {fromShadow: shadowSample, owners: filepath.Join(dir, "none.yaml")},
		"unwritable path": {fromShadow: shadowSample, owners: devOwners, out: filepath.Join(dir, "no", "such", "dir.yaml")},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			code, err := run(f, &out, &out)
			if err == nil || code == 0 {
				t.Fatalf("code %d err %v", code, err)
			}
			if strings.Contains(out.String(), "name:") {
				t.Errorf("wrote a scenario on failure: %q", out.String())
			}
		})
	}
}

func TestParseFlagsFromShadow(t *testing.T) {
	f, err := parseFlags([]string{"-from-shadow", "r.json", "-o", "x.yaml", "-name", "n"}, io.Discard)
	if err != nil || f.fromShadow != "r.json" || f.out != "x.yaml" || f.name != "n" {
		t.Fatalf("%+v %v", f, err)
	}
}
