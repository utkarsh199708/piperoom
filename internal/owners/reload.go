package owners

import (
	"fmt"
	"log/slog"
	"path/filepath"
)

// DirSkip records one OWNERS_DIR file LoadDir or Reload skipped, with the
// reason. A skipped file is a file that failed validation or had the wrong
// shape; the rest of the directory still loads.
type DirSkip struct {
	File   string
	Reason string
}

// LoadDir loads the main owners file at path and then merges every
// per-pipeline file in dir (OWNERS_DIR). Each per-pipeline file is a complete
// owners document with exactly one pipeline and no top-level layers:, tenants:
// or auditors: block. A file that fails validation or has the wrong shape is
// skipped — logged with the reason and recorded in DirSkips — and the rest
// still load. A pipeline id that appears in both the main file and a dir file,
// or in two dir files, is refused. With dir empty, LoadDir is Load. The
// returned Owners remembers path and dir, so Reload can re-read them.
func LoadDir(path, dir string) (*Owners, error) {
	if dir == "" {
		o, err := Load(path)
		if err != nil {
			return nil, err
		}
		o.path = path
		return o, nil
	}
	return loadDir(path, dir)
}

func loadDir(path, dir string) (*Owners, error) {
	o, err := Load(path)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(o.Pipelines))
	for _, p := range o.Pipelines {
		seen[p.ID] = true
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, fmt.Errorf("owners: list %s: %w", dir, err)
	}
	var dirPipes []Pipeline
	var skips []DirSkip
	for _, f := range files {
		d, err := Load(f)
		if err != nil {
			skips = append(skips, DirSkip{File: f, Reason: err.Error()})
			slog.Warn("owners: skipped a per-pipeline file in OWNERS_DIR", "file", f, "reason", err)
			continue
		}
		if err := dirFileOK(d); err != nil {
			skips = append(skips, DirSkip{File: f, Reason: err.Error()})
			slog.Warn("owners: skipped a per-pipeline file in OWNERS_DIR", "file", f, "reason", err)
			continue
		}
		p := d.Pipelines[0]
		if seen[p.ID] {
			return nil, fmt.Errorf("owners: pipeline %q is defined in both %s and a per-pipeline file in %s (%s)", p.ID, path, dir, f)
		}
		seen[p.ID] = true
		dirPipes = append(dirPipes, p)
	}
	o.Pipelines = append(o.Pipelines, dirPipes...)
	o.path = path
	o.dir = dir
	o.skips = skips
	return o, nil
}

// dirFileOK checks the shape of a per-pipeline file: exactly one pipeline, and
// no top-level layers:, tenants: or auditors: (the directory adds pipelines
// only; the shared top-level blocks live in the main file).
func dirFileOK(d *Owners) error {
	if len(d.Pipelines) != 1 {
		return fmt.Errorf("want exactly one pipeline, got %d", len(d.Pipelines))
	}
	if len(d.Layers) > 0 || len(d.Tenants) > 0 || len(d.Auditors) > 0 {
		return fmt.Errorf("a per-pipeline file must not set layers:, tenants: or auditors: (they come from the main file)")
	}
	return nil
}

// Reload re-reads the main file and OWNERS_DIR and swaps the whole config in
// atomically: the new set is built and validated completely before it replaces
// the old one, and a refused load (a duplicate id) leaves o unchanged. Skipped
// per-pipeline files are logged and recorded in DirSkips. It errors when o was
// not loaded with LoadDir (no OWNERS_DIR to re-read).
func (o *Owners) Reload() error {
	if o == nil || o.dir == "" {
		return fmt.Errorf("owners: Reload: no OWNERS_DIR was loaded (call LoadDir)")
	}
	fresh, err := loadDir(o.path, o.dir)
	if err != nil {
		return err
	}
	*o = *fresh
	return nil
}

// DirSkips returns the per-pipeline files the last LoadDir/Reload skipped and
// why, in directory order. It is a copy; the caller may keep or change it.
func (o *Owners) DirSkips() []DirSkip {
	if o == nil {
		return nil
	}
	out := make([]DirSkip, len(o.skips))
	copy(out, o.skips)
	return out
}
