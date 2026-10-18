package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"piperoom/internal/owners"
)

// ownersOutcome says where installOwners left the owners file.
type ownersOutcome struct {
	path   string // the file written
	draft  bool   // path is "<wanted>.draft": the file failed owners.Load
	reason string // the owners.Load error, when draft
}

// installOwners puts data at path only if the real loader accepts it. The file
// is first written next to path (same directory, so the final rename stays on
// one file system), loaded with owners.Load, and then moved into place. A file
// that does not load is written to "<path>.draft" instead, with the error as a
// comment block on top, and path is left as it was: a failed run never
// replaces a working owners file, not even with force.
//
// Without force an existing path (or an existing .draft) is refused, also when
// it appears while the run is going (fs.ErrExist). clean is applied to the
// loader's text before it is written into the draft.
func installOwners(path string, data []byte, force bool, clean func(string) string) (ownersOutcome, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".owners-*.tmp")
	if err != nil {
		return ownersOutcome{}, fmt.Errorf("discover: create a temporary file next to %s: %w", path, err)
	}
	tmpName := tmp.Name()
	// After a successful rename this is a no-op; after a link it removes the
	// second name; on every failure it removes the half-written file.
	defer func() { _ = os.Remove(tmpName) }()
	_, werr := tmp.Write(data)
	// CreateTemp makes the file 0600; the owners file holds no secret and a
	// teammate or the server's user may need to read it, as with writeFile.
	merr := tmp.Chmod(0o644)
	cerr := tmp.Close()
	if err := errors.Join(werr, merr, cerr); err != nil {
		return ownersOutcome{}, fmt.Errorf("discover: write %s: %w", tmpName, err)
	}

	if _, lerr := owners.Load(tmpName); lerr != nil {
		draftPath := path + ".draft"
		// Name the file the reader will open, not the temporary one.
		reason := clean(strings.ReplaceAll(lerr.Error(), tmpName, draftPath))
		content := append([]byte(draftHeader(path, draftPath, reason)), data...)
		if err := writeFile(draftPath, content, force); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return ownersOutcome{}, fmt.Errorf("discover: the owners file failed owners.Load (%s) and %s already exists (use -force to overwrite it)", reason, draftPath)
			}
			return ownersOutcome{}, err
		}
		return ownersOutcome{path: draftPath, draft: true, reason: reason}, nil
	}

	if err := place(tmpName, path, data, force); err != nil {
		return ownersOutcome{}, err
	}
	return ownersOutcome{path: path}, nil
}

// place moves the loaded temporary file to path. With force the rename
// replaces path atomically. Without it a hard link fails when path exists, so
// a file that appeared meanwhile is never overwritten; if the file system has
// no hard links, an exclusive create does the same job.
func place(tmp, path string, data []byte, force bool) error {
	if force {
		if err := os.Rename(tmp, path); err != nil {
			return fmt.Errorf("discover: replace %s: %w", path, err)
		}
		return nil
	}
	err := os.Link(tmp, path)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		err = writeFile(path, data, false)
	}
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("discover: %s already exists (use -force to overwrite): %w", path, fs.ErrExist)
	}
	return err
}

// draftHeader is the comment block on top of a .draft file. Each line of the
// error is its own comment line, and control characters are removed, so the
// error text cannot end the comment.
func draftHeader(path, draftPath, reason string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# DRAFT: this file did not pass owners.Load, so %s was not written.\n", commentText(path))
	b.WriteString("# The loader said:\n")
	for _, l := range strings.Split(reason, "\n") {
		b.WriteString("#   " + commentText(l) + "\n")
	}
	fmt.Fprintf(&b, "# Fix the problem, then rename %s to %s.\n\n", commentText(draftPath), commentText(path))
	return b.String()
}

// commentText replaces control characters with spaces so s fits on one
// comment line.
func commentText(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
}
