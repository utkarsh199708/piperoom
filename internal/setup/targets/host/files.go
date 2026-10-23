package host

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"unicode/utf8"

	"piperoom/internal/setup/ops"
)

// writeConfined writes name under the project directory through the §2.3
// rule (ops.Confiner: the lexical check and the Lstat walk refusing any
// symlink or reparse point) and an os.Root, atomically: an O_EXCL temporary
// file renamed over the target, so a planted file or hard link is replaced,
// never written through. It reports whether the bytes changed. unitFile is
// set only for the unit file, the one file this driver writes inside a
// directory it registered as a mount; every other write refuses mounted
// paths the way file_write does, and every write refuses a materialised
// secret file.
func writeConfined(env ops.Env, name string, data []byte, mode fs.FileMode, unitFile bool) (string, bool, error) {
	if env.Confine == nil {
		return "", false, errors.New("host: the project root is not open")
	}
	clean, err := env.Confine.Check(name)
	if err != nil {
		return "", false, err
	}
	switch {
	case unitFile && path.Dir(clean) != UnitDir:
		return "", false, fmt.Errorf("%w: %s is not in %s/", ErrPath, q(name), UnitDir)
	case !unitFile && ops.MountedPath(clean, env.MountDirs):
		return "", false, fmt.Errorf("%w: %s is under a directory the worker executes or loads from", ops.ErrExecutedName, q(name))
	}
	// The refusal file_write applies to a file the runner materialised from
	// the secret store: no driver write replaces one either.
	if err := ops.MaterialisedRefusal(clean, env); err != nil {
		return "", false, err
	}
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return "", false, fmt.Errorf("host: opening the project root: %w", err)
	}
	defer root.Close()
	if fi, lerr := root.Lstat(clean); lerr == nil {
		if !fi.Mode().IsRegular() {
			return "", false, fmt.Errorf("%w: %s exists and is not a regular file", ops.ErrNotRegular, q(name))
		}
		if old, rerr := root.ReadFile(clean); rerr == nil && bytes.Equal(old, data) && fi.Mode().Perm() == mode.Perm() {
			return clean, false, nil
		}
	} else if !errors.Is(lerr, fs.ErrNotExist) {
		return "", false, fmt.Errorf("host: stat %s: %w", q(name), lerr)
	}
	if dir := path.Dir(clean); dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return "", false, fmt.Errorf("host: creating %s: %w", q(dir), err)
		}
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", false, fmt.Errorf("host: generating a temp name: %w", err)
	}
	tmp := path.Join(path.Dir(clean), "."+path.Base(clean)+".piperoom-tmp-"+hex.EncodeToString(rnd[:]))
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return "", false, fmt.Errorf("host: creating a temp file for %s: %w", q(name), err)
	}
	_, werr := f.Write(data)
	if werr == nil {
		werr = f.Chmod(mode) // the umask must not widen or narrow the mode
	}
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = root.Rename(tmp, clean)
	}
	if werr != nil {
		_ = root.Remove(tmp)
		return "", false, fmt.Errorf("host: writing %s: %w", q(name), werr)
	}
	return clean, true, nil
}

// readCurrent reads a project file through the catalogue's own file_read op,
// so a preview or properties_read gets every read rule (the path rule, the
// secret-file and materialised-path refusals, the size cap, the redaction)
// without a second copy of them. A missing file is ("", false, nil).
func (d *Driver) readCurrent(ctx context.Context, env ops.Env, name string) (string, bool, error) {
	op, ok := d.reg.Get("file_read")
	if !ok {
		return "", false, errors.New("host: file_read is not in the catalogue")
	}
	raw, err := json.Marshal(map[string]string{"path": name})
	if err != nil {
		return "", false, fmt.Errorf("host: encoding file_read params: %w", err)
	}
	res, err := op.Execute(ctx, raw, env)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	m, _ := res.Data.(map[string]any)
	content, _ := m["content"].(string)
	if cut, _ := m["truncated"].(bool); cut {
		return "", true, fmt.Errorf("%w: %s is larger than file_read returns", ops.ErrTooLarge, q(name))
	}
	return content, true, nil
}

// Diff limits: worker and unit files are a few dozen lines.
const (
	maxDiffLines = 400
	maxDiffBytes = 4 << 10
)

// lineDiff is a full-context line diff of before → after: "  " kept, "- "
// removed, "+ " added. It is what a preview shows an approver, so it is
// complete rather than clever: an LCS over at most maxDiffLines lines a side.
func lineDiff(before, after string) string {
	a, b := splitLines(before), splitLines(after)
	if len(a) > maxDiffLines || len(b) > maxDiffLines {
		return fmt.Sprintf("(too large to diff: %d lines now, %d after)", len(a), len(b))
	}
	lcs := make([][]int, len(a)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(b)+1)
	}
	for i := len(a) - 1; i >= 0; i-- {
		for j := len(b) - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	var out strings.Builder
	line := func(mark, text string) { out.WriteString(mark + printable(text) + "\n") }
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		switch {
		case i < len(a) && j < len(b) && a[i] == b[j]:
			line("  ", a[i])
			i, j = i+1, j+1
		case i < len(a) && (j == len(b) || lcs[i+1][j] >= lcs[i][j+1]):
			line("- ", a[i])
			i++
		default:
			line("+ ", b[j])
			j++
		}
	}
	s := out.String()
	if len(s) > maxDiffBytes {
		cut := maxDiffBytes
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		return s[:cut] + "\n… (diff cut)"
	}
	return s
}

// printable replaces control characters other than tab, so a line from a file
// on disk cannot carry a terminal escape onto an approver's screen.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if (r < 0x20 && r != '\t') || r == 0x7f {
			return '?'
		}
		return r
	}, strings.ToValidUTF8(s, "?"))
}

// splitLines splits text into lines without a trailing empty one.
func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}
