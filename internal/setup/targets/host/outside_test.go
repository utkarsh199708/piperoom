package host

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// realTempDir is a temporary directory with its symlinks resolved: on macOS
// t.TempDir is under /var, a symlink to /private/var, which the outside-file
// walk refuses as it should.
func realTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestPropertiesWriteTier is the ruling's tier per case: a worker file at the
// project root is T1, an absolute path outside the project is T2, on the
// params alone (the server computes it without the runner's OS).
func TestPropertiesWriteTier(t *testing.T) {
	cases := []struct {
		file string
		want string
	}{
		{"connect-distributed.properties", protocol.TierT1},
		{"connect-standalone.properties", protocol.TierT1},
		{"/etc/kafka/connect-distributed.properties", protocol.TierT2},
		{"/opt/kafka/config/connect-standalone.properties", protocol.TierT2},
		{`C:\kafka\config\connect-distributed.properties`, protocol.TierT2},
		{"D:/kafka/connect-standalone.properties", protocol.TierT2},
	}
	for _, c := range cases {
		raw, _ := json.Marshal(PropertiesParams{File: c.file, Entries: distributedEntries()})
		got, err := PropertiesWriteTier(raw)
		if err != nil || got != c.want {
			t.Errorf("PropertiesWriteTier(%q) = %q, %v; want %q", c.file, got, err, c.want)
		}
	}
	for _, bad := range []string{"", "worker.properties", "kafka/config/connect-distributed.properties", "../connect-distributed.properties", "C:connect-distributed.properties"} {
		raw, _ := json.Marshal(PropertiesParams{File: bad, Entries: distributedEntries()})
		if got, err := PropertiesWriteTier(raw); err == nil {
			t.Errorf("PropertiesWriteTier(%q) = %q, want an error", bad, got)
		}
	}
	if _, err := PropertiesWriteTier(json.RawMessage(`{"file":"connect-distributed.properties","entries":{},"tier":"T1"}`)); !errors.Is(err, ErrParams) {
		t.Errorf("a tier param: %v, want ErrParams", err)
	}

	// The op carries the same answer, and its floor stays T1.
	f := newFixture(t, Options{})
	op, ok := f.reg.Get("properties_write")
	if !ok {
		t.Fatal("properties_write is not registered")
	}
	if op.Tier() != protocol.TierT1 {
		t.Errorf("floor %q, want T1", op.Tier())
	}
	tiered, ok := op.(interface {
		TierFor(json.RawMessage) (string, error)
	})
	if !ok {
		t.Fatal("properties_write has no TierFor")
	}
	for _, c := range cases {
		raw, _ := json.Marshal(PropertiesParams{File: c.file, Entries: distributedEntries()})
		if got, err := tiered.TierFor(raw); err != nil || got != c.want {
			t.Errorf("TierFor(%q) = %q, %v; want %q", c.file, got, err, c.want)
		}
	}
}

func TestPropertiesWriteOutsideProject(t *testing.T) {
	skipOnWindows(t)
	f := newFixture(t, Options{TierRouted: true})
	out := realTempDir(t)
	file := out + "/connect-distributed.properties"
	res, err := f.run("properties_write", PropertiesParams{File: file, Entries: distributedEntries()})
	if err != nil {
		t.Fatalf("properties_write outside: %v (%s)", err, res.Summary)
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "group.id=piperoom-connect\n") ||
		!strings.Contains(string(b), "plugin.path="+slashed(f.dir)+"/connect-plugins\n") {
		t.Errorf("file:\n%s", b)
	}
	if fi, _ := os.Stat(file); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v, want 0600", fi.Mode().Perm())
	}
	var data map[string]any
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["outside_project"] != true || data["tier"] != protocol.TierT2 || data["file"] != file || res.Receipt.Target != file {
		t.Errorf("data %v receipt %+v", data, res.Receipt)
	}
	if _, err := os.Stat(f.dir + "/connect-distributed.properties"); err == nil {
		t.Error("the project's worker file was written too")
	}
	res, err = f.run("properties_write", PropertiesParams{File: file, Entries: distributedEntries()})
	if err != nil || !strings.HasPrefix(res.Summary, "unchanged") {
		t.Errorf("second write: %v %q", err, res.Summary)
	}

	// The preview is a diff of the outside file.
	raw, _ := json.Marshal(PropertiesParams{File: file, Entries: with(distributedEntries(), "group.id", "other")})
	op, _ := f.reg.Get("properties_write")
	pv, err := op.(propertiesWriteOp).Preview(t.Context(), raw, f.env())
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if !strings.Contains(pv.Expected, "- group.id=piperoom-connect") || !strings.Contains(pv.Expected, "+ group.id=other") || !strings.Contains(pv.Expected, "T2") {
		t.Errorf("preview %+v", pv)
	}
}

func TestPropertiesWriteOutsideNotRouted(t *testing.T) {
	skipOnWindows(t)
	f := newFixture(t, Options{})
	file := realTempDir(t) + "/connect-distributed.properties"
	if _, err := f.run("properties_write", PropertiesParams{File: file, Entries: distributedEntries()}); !errors.Is(err, ErrTierRouting) {
		t.Fatalf("not routed: %v, want ErrTierRouting", err)
	}
	if _, err := os.Stat(file); err == nil {
		t.Error("an outside file was written without tier routing")
	}
	// The project file is still T1 and writes.
	if _, err := f.run("properties_write", PropertiesParams{File: "connect-distributed.properties", Entries: distributedEntries()}); err != nil {
		t.Errorf("project file: %v", err)
	}
}

func TestPropertiesWriteOutsideRefusals(t *testing.T) {
	skipOnWindows(t)
	f := newFixture(t, Options{TierRouted: true})
	out := realTempDir(t)
	elsewhere := realTempDir(t)
	keep := elsewhere + "/real.properties"
	if err := os.WriteFile(keep, []byte("keep\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A symlink at the leaf: refused, never followed.
	leafLink := out + "/leaf"
	if err := os.Mkdir(leafLink, 0o755); err != nil {
		t.Fatal(err)
	}
	mustSymlink(t, keep, leafLink+"/connect-distributed.properties")
	// A symlinked parent directory.
	mustSymlink(t, elsewhere, out+"/linkdir")

	d := distributedEntries()
	cases := []struct {
		name    string
		file    string
		entries map[string]string
		want    error
	}{
		{"unknown key", out + "/connect-distributed.properties", with(d, "config.providers", "file"), ErrProperties},
		{"code-loading key", out + "/connect-distributed.properties", with(d, "config.providers.file.class", "a.B"), ErrProperties},
		{"other mode key", out + "/connect-distributed.properties", with(d, "offset.storage.file.filename", "x.offsets"), ErrProperties},
		{"plugin.path outside the project", out + "/connect-distributed.properties", with(d, "plugin.path", out), ErrProperties},
		{"credential-looking value", out + "/connect-distributed.properties", with(d, "group.id", "AKIAIOSFODNN7EXAMPLE"), ErrSecretValue},
		{"symlinked leaf", leafLink + "/connect-distributed.properties", d, ops.ErrNotRegular},
		{"symlinked parent", out + "/linkdir/connect-distributed.properties", d, ErrPath},
		{"other base name", out + "/worker.properties", d, ErrProperties},
		{"env file", out + "/.env", d, ErrProperties},
		{"missing parent", out + "/nope/connect-distributed.properties", d, ErrPath},
		{"not clean", out + "/x/../connect-distributed.properties", d, ErrPath},
		{"ssh dir", out + "/.ssh/connect-distributed.properties", d, ErrPath},
		{"space", out + "/a b/connect-distributed.properties", d, ErrPath},
		{"under the project", f.dir + "/connect-distributed.properties", d, ErrProperties},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := f.run("properties_write", PropertiesParams{File: c.file, Entries: c.entries})
			if !errors.Is(err, c.want) {
				t.Fatalf("err %v, want %v", err, c.want)
			}
		})
	}
	if b, _ := os.ReadFile(keep); string(b) != "keep\n" {
		t.Error("a symlink target was written")
	}
	if _, err := os.Stat(out + "/nope"); err == nil {
		t.Error("a directory was created outside the project")
	}
	if _, err := os.Stat(out + "/connect-distributed.properties"); err == nil {
		t.Error("a refused step wrote the outside file")
	}
}
