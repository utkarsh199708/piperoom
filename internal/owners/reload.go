package owners

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// DirSkip records one OWNERS_DIR file LoadDir or a Live.Reload skipped, with
// the reason. A skipped file is a file that failed validation or had the wrong
// shape; the rest of the directory still loads.
type DirSkip struct {
	File   string
	Reason string
}

// Live publishes the current owners config behind an atomic pointer. Readers
// take a snapshot once per request with Load; Reload builds a complete new set
// and publishes it only on success, so a reader never sees a half-updated
// config and a reload never mutates the snapshot a reader already holds.
// Concurrent Reloads are serialized by mu so two reloads cannot interleave and
// lose an update (the last one to load the directory wins, whole).
type Live struct {
	mu sync.Mutex
	p  atomic.Pointer[Owners]
}

// NewLive loads the owners config at path with OWNERS_DIR dir (dir may be "")
// and returns it published in a Live holder.
func NewLive(path, dir string) (*Live, error) {
	o, err := LoadDir(path, dir)
	if err != nil {
		return nil, err
	}
	l := &Live{}
	l.p.Store(o)
	return l, nil
}

// Load returns the current owners config. The caller keeps this snapshot for
// the whole request; Reload replaces the pointer rather than mutating it.
func (l *Live) Load() *Owners {
	if l == nil {
		return nil
	}
	return l.p.Load()
}

// Reload re-reads the main file and OWNERS_DIR and publishes the new set only
// on success; a refused load leaves the published set unchanged. It errors when
// the Live was created without an OWNERS_DIR (NewLive with dir ""). Reloads are
// serialized: the mutex is held across the whole load-and-publish so concurrent
// reloads cannot interleave and publish a stale set.
func (l *Live) Reload() error {
	if l == nil {
		return fmt.Errorf("owners: Reload: nil Live")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	cur := l.p.Load()
	if cur == nil || cur.dir == "" {
		return fmt.Errorf("owners: Reload: no OWNERS_DIR was loaded (call NewLive)")
	}
	fresh, err := loadDir(cur.path, cur.dir)
	if err != nil {
		return err
	}
	l.p.Store(fresh)
	return nil
}

// LoadDir loads the main owners file at path and then merges every
// per-pipeline file in dir (OWNERS_DIR). Each per-pipeline file is a complete
// owners document with exactly one pipeline and no top-level layers:, tenants:
// or auditors: block. A file that fails validation or has the wrong shape is
// skipped — logged with the reason and recorded in DirSkips — and the rest
// still load. A pipeline id that appears in both the main file and a dir file,
// or in two dir files, is refused. With dir empty, LoadDir is Load. The
// returned Owners remembers path and dir, so a Live built over it can Reload.
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
		d, err := parse(f)
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
		// Validate this pipeline against the main file's top-level blocks and
		// every already-accepted dir pipeline, not alone: a tenant: name is only
		// valid when the main file declares it, and a shared connection
		// reference only when the whole set is known.
		candPipes := make([]Pipeline, 0, len(o.Pipelines)+len(dirPipes)+1)
		candPipes = append(candPipes, o.Pipelines...)
		candPipes = append(candPipes, dirPipes...)
		candPipes = append(candPipes, p)
		cand := Owners{
			Layers:    o.Layers,
			Auditors:  o.Auditors,
			Tenants:   o.Tenants,
			Teams:     o.Teams,
			Pipelines: candPipes,
		}
		if err := cand.validate(); err != nil {
			skips = append(skips, DirSkip{File: f, Reason: err.Error()})
			slog.Warn("owners: skipped a per-pipeline file in OWNERS_DIR", "file", f, "reason", err)
			continue
		}
		seen[p.ID] = true
		dirPipes = append(dirPipes, p)
	}
	// Validate the final merged set once more before it is published: this is
	// the copy that carries the validated pipelines (trimmed lists, resolved
	// watch), and the last check catches anything only the full set reveals.
	merged := Owners{
		Layers:    o.Layers,
		Auditors:  o.Auditors,
		Tenants:   o.Tenants,
		Teams:     o.Teams,
		Pipelines: append(append([]Pipeline{}, o.Pipelines...), dirPipes...),
		derived:   &derivedGroups{},
	}
	if err := merged.validate(); err != nil {
		return nil, err
	}
	merged.path = path
	merged.dir = dir
	merged.skips = skips
	return &merged, nil
}

// dirFileOK checks the shape of a per-pipeline file: exactly one pipeline, and
// no top-level layers:, tenants:, auditors: or teams: (the directory adds
// pipelines only; the shared top-level blocks live in the main file).
func dirFileOK(d *Owners) error {
	if len(d.Pipelines) != 1 {
		return fmt.Errorf("want exactly one pipeline, got %d", len(d.Pipelines))
	}
	if len(d.Layers) > 0 || len(d.Tenants) > 0 || len(d.Auditors) > 0 || len(d.Teams) > 0 {
		return fmt.Errorf("a per-pipeline file must not set layers:, tenants:, auditors: or teams: (they come from the main file)")
	}
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
