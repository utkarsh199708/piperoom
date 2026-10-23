package compose

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Argv must refuse a traversal in the compose or env file name: the joined
// path would point compose at a file outside the project, bypassing the
// confiner, and feed it an arbitrary host .env for interpolation.
func TestArgvRefusesTraversal(t *testing.T) {
	dir := t.TempDir()
	for _, bad := range [][2]string{
		{"../../etc/docker-compose.yml", EnvFileName},
		{"/etc/docker-compose.yml", EnvFileName},
		{DefaultFile, "../../home/victim/.env"},
		{DefaultFile, "sub/.env"},
	} {
		argv, err := Argv(dir, bad[0], bad[1], "up", nil)
		if err == nil {
			t.Errorf("Argv(%q, %q) = %q, want a refusal", bad[0], bad[1], argv)
		}
	}
}

// properties_write accepts the three exact root-level names only: a file in a
// subdirectory or a case-folded name would be written where the pinned argv
// and the worker never read.
func TestPropertiesWriteExactNames(t *testing.T) {
	h := newHarness(t, protocol.ClassLocal)
	for _, bad := range []string{"sub/.env", "conf/worker.properties", "WORKER.PROPERTIES", ".ENV"} {
		_, err := h.run(t, "properties_write", PropertiesParams{File: bad, Entries: map[string]string{"POSTGRES_DB": "orders"}})
		if err == nil {
			t.Errorf("properties_write(%q) passed, want a refusal", bad)
		}
	}
}

// writeText refuses a path under a registered mount directory, like file_write
// does: the plugin directory must take writes from plugin_install only.
func TestWriteTextRefusesMountDirs(t *testing.T) {
	h := newHarness(t, protocol.ClassLocal)
	if _, err := h.run(t, "compose_write", WriteParams{Templates: []string{TemplateKafka, TemplateConnect}}); err != nil {
		t.Fatalf("compose_write: %v", err)
	}
	// The plugin dir is registered now; properties_write into it is refused.
	_, err := h.run(t, "properties_write", PropertiesParams{File: PluginDir + "/worker.properties", Entries: map[string]string{"group.id": "g"}})
	if err == nil {
		t.Fatal("properties_write into the plugin directory passed")
	}
}

// An unchanged properties_write still fixes the file's mode: a pre-existing
// world-readable .env must not stay 0644 because its bytes already matched.
func TestPropertiesWriteFixesMode(t *testing.T) {
	h := newHarness(t, protocol.ClassLocal)
	entries := map[string]string{"POSTGRES_DB": "orders"}
	if _, err := h.run(t, "properties_write", PropertiesParams{File: EnvFileName, Entries: entries}); err != nil {
		t.Fatalf("properties_write: %v", err)
	}
	p := filepath.Join(h.dir, EnvFileName)
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.run(t, "properties_write", PropertiesParams{File: EnvFileName, Entries: entries}); err != nil {
		t.Fatalf("second properties_write: %v", err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o after an unchanged write, want 600", fi.Mode().Perm())
	}
}

// file_write refuses .properties files wholesale, so it cannot bypass
// properties_write's key allow-list for files a Connect worker loads.
func TestFileWriteRefusesProperties(t *testing.T) {
	h := newHarness(t, protocol.ClassLocal)
	_, err := h.run(t, "file_write", map[string]any{"path": "worker.properties", "content": "plugin.path=/anywhere\n"})
	if !errors.Is(err, ops.ErrExecutedName) {
		t.Fatalf("file_write(worker.properties) = %v, want ops.ErrExecutedName", err)
	}
}

// The rendered templates still pass the tightened validator (interpolation
// allow-list, read-only binds, entrypoint denial).
func TestTemplatesPassTightenedValidator(t *testing.T) {
	h := newHarness(t, protocol.ClassLocal)
	res, err := h.run(t, "compose_write", WriteParams{Templates: []string{TemplateKafka, TemplatePostgres, TemplateConnect, TemplateRegistry}, Registry: true})
	if err != nil {
		t.Fatalf("compose_write: %v (summary %q)", err, res.Summary)
	}
	doc, err := os.ReadFile(filepath.Join(h.dir, DefaultFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateYAML(doc, protocol.ClassLocal); err != nil {
		t.Fatalf("the rendered stack fails the validator: %v", err)
	}
	if strings.Contains(string(doc), "entrypoint") {
		t.Fatal("a template renders an entrypoint")
	}
}

// Params whose keys differ only in letter case are refused: the decoder folds
// them and the last one wins, so the value checked by one reader and the value
// run by another could differ.
func TestDecodeParamsRefusesCaseCollidingKeys(t *testing.T) {
	var p PropertiesParams
	for _, raw := range []string{
		`{"file":"a.properties","File":"../../etc/passwd"}`,
		`{"file":"a.properties","FILE":"b.properties"}`,
	} {
		if err := decodeParams([]byte(raw), &p); !errors.Is(err, ErrParams) || !strings.Contains(err.Error(), "letter case") {
			t.Fatalf("decodeParams(%s) = %v, want ErrParams for colliding keys", raw, err)
		}
	}
	if err := decodeParams([]byte(`{"file":"a.properties"}`), &p); err != nil {
		t.Fatalf("decodeParams of clean params = %v", err)
	}
}
