package owners

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dirPipeline is a complete per-pipeline owners document for OWNERS_DIR.
func dirPipeline(id string) string {
	return "pipelines:\n  - {id: " + id + ", topic_prefix: " + id + ", source_tables: [public." + id + "], connectors: [" + id + "-c]}\n"
}

func writeDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const mainYAML = "pipelines:\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1]}\nlayers: {}\n"

func TestLoadDirMerges(t *testing.T) {
	dir := writeDir(t, map[string]string{
		"refunds.yaml": dirPipeline("refunds"),
		"ledger.yaml":  dirPipeline("ledger"),
	})
	o, err := LoadDir(writeTemp(t, mainYAML), dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	for _, id := range []string{"orders", "refunds", "ledger"} {
		if _, ok := o.Pipeline(id); !ok {
			t.Fatalf("pipeline %q missing", id)
		}
	}
	if len(o.Pipelines) != 3 {
		t.Fatalf("pipelines = %d, want 3", len(o.Pipelines))
	}
	if len(o.DirSkips()) != 0 {
		t.Fatalf("skips = %+v", o.DirSkips())
	}
}

func TestLoadDirSkipsBadFile(t *testing.T) {
	dir := writeDir(t, map[string]string{
		"bad.yaml":    "pipelines:\n  - {id: bad}\n", // no topic_prefix/source_tables/connectors
		"good.yaml":   dirPipeline("good"),
		"two.yaml":    "pipelines:\n  - {id: two, topic_prefix: two, source_tables: [public.two], connectors: [two-c]}\n  - {id: three, topic_prefix: three, source_tables: [public.three], connectors: [three-c]}\n", // two pipelines
		"layers.yaml": "pipelines:\n  - {id: withlayers, topic_prefix: wl, source_tables: [public.wl], connectors: [wl-c]}\nlayers:\n  postgres: priya\n",
	})
	o, err := LoadDir(writeTemp(t, mainYAML), dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if _, ok := o.Pipeline("good"); !ok {
		t.Fatal("the valid dir file must load")
	}
	for _, absent := range []string{"bad", "two", "three", "withlayers"} {
		if _, ok := o.Pipeline(absent); ok {
			t.Fatalf("pipeline %q loaded from a skipped file", absent)
		}
	}
	skips := o.DirSkips()
	if len(skips) != 3 {
		t.Fatalf("skips = %+v, want 3", skips)
	}
	byFile := map[string]string{}
	for _, s := range skips {
		if s.Reason == "" {
			t.Fatalf("skip %+v has no reason", s)
		}
		byFile[filepath.Base(s.File)] = s.Reason
	}
	if _, ok := byFile["bad.yaml"]; !ok {
		t.Fatalf("skips = %+v, missing bad.yaml", skips)
	}
	if _, ok := byFile["two.yaml"]; !ok || !strings.Contains(byFile["two.yaml"], "exactly one pipeline") {
		t.Fatalf("skips = %+v, want two.yaml with a shape reason", skips)
	}
	if _, ok := byFile["layers.yaml"]; !ok || !strings.Contains(byFile["layers.yaml"], "must not set layers") {
		t.Fatalf("skips = %+v, want layers.yaml with a top-level-block reason", skips)
	}
}

func TestLoadDirRefusesDuplicateID(t *testing.T) {
	// Same id in the main file and a dir file.
	dir := writeDir(t, map[string]string{"orders.yaml": dirPipeline("orders")})
	_, err := LoadDir(writeTemp(t, mainYAML), dir)
	if err == nil || !strings.Contains(err.Error(), "orders") || !strings.Contains(err.Error(), "both") {
		t.Fatalf("err = %v, want an id-in-both refusal", err)
	}
	// Same id in two dir files.
	dir2 := writeDir(t, map[string]string{"a.yaml": dirPipeline("dup"), "b.yaml": dirPipeline("dup")})
	if _, err := LoadDir(writeTemp(t, mainYAML), dir2); err == nil || !strings.Contains(err.Error(), "dup") {
		t.Fatalf("err = %v, want a dir-vs-dir duplicate refusal", err)
	}
}

func TestLoadDirEmptyDirIsLoad(t *testing.T) {
	o, err := LoadDir(writeTemp(t, mainYAML), "")
	if err != nil {
		t.Fatalf("LoadDir with empty dir: %v", err)
	}
	if _, ok := o.Pipeline("orders"); !ok {
		t.Fatal("pipeline orders missing")
	}
}

func TestReloadSwapsAtomically(t *testing.T) {
	dir := writeDir(t, map[string]string{"a.yaml": dirPipeline("a")})
	o, err := LoadDir(writeTemp(t, mainYAML), dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if _, ok := o.Pipeline("a"); !ok {
		t.Fatal("a must load")
	}
	// Swap the directory contents: a goes, b arrives, and one bad file is
	// skipped without stopping the swap.
	if err := os.Remove(filepath.Join(dir, "a.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.yaml"), []byte(dirPipeline("b")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("pipelines:\n  - {id: bad}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := o.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if _, ok := o.Pipeline("a"); ok {
		t.Fatal("a must be gone after the swap")
	}
	if _, ok := o.Pipeline("b"); !ok {
		t.Fatal("b must be present after the swap")
	}
	if _, ok := o.Pipeline("orders"); !ok {
		t.Fatal("the main file's pipeline must survive the swap")
	}
	if got := o.DirSkips(); len(got) != 1 || filepath.Base(got[0].File) != "bad.yaml" {
		t.Fatalf("skips = %+v", got)
	}

	// A refused reload leaves the set unchanged.
	if err := os.WriteFile(filepath.Join(dir, "dup.yaml"), []byte(dirPipeline("orders")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := o.Reload(); err == nil || !strings.Contains(err.Error(), "orders") {
		t.Fatalf("Reload with duplicate = %v, want a refusal", err)
	}
	if _, ok := o.Pipeline("b"); !ok {
		t.Fatal("a refused reload must not change the set (b is gone)")
	}
}

func TestReloadNeedsLoadDir(t *testing.T) {
	o, err := Load(writeTemp(t, mainYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := o.Reload(); err == nil || !strings.Contains(err.Error(), "OWNERS_DIR") {
		t.Fatalf("Reload without LoadDir = %v, want an OWNERS_DIR error", err)
	}
}
