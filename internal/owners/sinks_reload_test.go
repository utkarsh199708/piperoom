package owners

import (
	"os"
	"path/filepath"
	"testing"
)

// LoadDir re-validates already-validated pipelines; a sinks: pipeline must
// survive that second validation (KAN-481 reload fix).
func TestLoadDirWithSinksRepro(t *testing.T) {
	dir := t.TempDir()
	dirY := `
pipelines:
  - id: other
    title: Other CDC
    topic_prefix: pg
    source_tables: [public.other]
    connectors: [pg-other-connector]
    sink_tables: [analytics.other]
    consumer_groups: [other-sync]
    sink_ts_column: updated_at
`
	if err := os.WriteFile(filepath.Join(dir, "other.yaml"), []byte(dirY), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDir(writeTemp(t, twoSinksYAML), dir); err != nil {
		t.Fatalf("LoadDir with a sinks: pipeline: %v", err)
	}
}
