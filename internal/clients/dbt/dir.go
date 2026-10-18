package dbt

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Dir reads the artifacts dbt writes to its target directory
// (DBT_ARTIFACTS_DIR). Each call opens the file again, so the newest dbt run
// is read without a restart. It is safe for concurrent use.
type Dir struct {
	dir string
}

// NewDir returns a reader for the artifacts in dir, an absolute path.
// Whether the directory and files exist is checked at each read: dbt may not
// have run yet when the server starts.
func NewDir(dir string) (*Dir, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("dbt: artifacts dir %q: want an absolute path", dir)
	}
	return &Dir{dir: filepath.Clean(dir)}, nil
}

// RunResults reads and decodes run_results.json.
func (d *Dir) RunResults(ctx context.Context) (RunResults, error) {
	b, err := d.read(ctx, RunResultsFile)
	if err != nil {
		return RunResults{}, err
	}
	return parseRunResults(b)
}

// ModelLineage reads manifest.json and returns model's direct parents and
// children. model is a unique_id or a model's short name; an unknown one is
// clients.ErrNotFound.
func (d *Dir) ModelLineage(ctx context.Context, model string) (Lineage, error) {
	b, err := d.read(ctx, ManifestFile)
	if err != nil {
		return Lineage{}, err
	}
	m, err := parseManifest(b)
	if err != nil {
		return Lineage{}, err
	}
	return m.lineage(model)
}

// read returns one artifact, at most MaxArtifactBytes. A local file read
// does not take a context, so ctx is checked before opening and between
// reads: a slow network share stops at the caller's deadline.
func (d *Dir) read(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("dbt: read %s: %w", name, err)
	}
	path := filepath.Join(d.dir, name)
	f, err := os.Open(path)
	if err != nil {
		// The *PathError names the configured path, never file content.
		return nil, fmt.Errorf("dbt: %w: %w", ErrNoArtifact, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("dbt: stat %s: %w", path, err)
	}
	// A FIFO or device would block or never end; dbt writes regular files.
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("dbt: %s: %w: not a regular file", path, ErrMalformed)
	}
	if fi.Size() > MaxArtifactBytes {
		return nil, fmt.Errorf("dbt: %s (%d bytes): %w", name, fi.Size(), ErrTooLarge)
	}
	// The size may change between Stat and the read (dbt rewriting the
	// file), so readCapped enforces the cap again.
	return readCapped(ctxReader{ctx: ctx, r: f}, name)
}

// ctxReader stops a read once ctx is done.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
