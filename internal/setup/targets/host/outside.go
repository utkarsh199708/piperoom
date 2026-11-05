package host

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"piperoom/internal/redact"
	"piperoom/internal/setup/ops"
)

// Files outside the project directory (lead ruling on U8a, KAN-533): a worker
// properties file the person's own Kafka install reads (properties_write at
// T2), and a system unit under /etc/systemd/system (service_install on a
// system scope). Neither can go through ops.Confiner, which is rooted at the
// project, so this file applies the same §2.3 rule to one fixed parent
// directory: every component from the volume root down to the parent is
// Lstat'ed and refused if it is a symlink, junction or other reparse point,
// the parent is then opened as an os.Root and compared by identity with the
// directory the walk saw (a swap between the walk and the open is caught),
// and the leaf is opened through that root, refused unless it is a regular
// file or missing. Writes are atomic (an O_EXCL temporary file renamed over
// the leaf), so a planted hard link or symlink is replaced, never written
// through. No directory is created outside the project.

// maxOutsideRead caps a file read outside the project for a preview diff:
// worker and unit files are a few dozen lines.
const maxOutsideRead = 64 << 10

// drivePathRE is a drive-absolute Windows path in either separator.
var drivePathRE = regexp.MustCompile(`^[A-Za-z]:[\\/]`)

// absoluteShape reports whether p has the shape of an absolute path on either
// OS, without I/O and without consulting the runtime's OS: the server, which
// may run on another OS than the runner, decides a step's tier from it.
func absoluteShape(p string) bool {
	return strings.HasPrefix(p, "/") || strings.HasPrefix(p, `\`) || drivePathRE.MatchString(p)
}

// checkOutsidePath checks an absolute path outside the project lexically and
// returns its parent directory and base name in the runner's native form. The
// shape rule is CheckServicePath's (a POSIX path of safe characters, or a
// drive-absolute Windows path of plain components; UNC, device and rooted
// paths are refused), and the part below the volume root then passes the
// project's own lexical rule (ops.CheckName: no dot-dot, reserved device,
// 8.3 alias, .git, .ssh or .piperoom component), so an outside file is held to
// the rule a project file is.
func checkOutsidePath(p string) (dir, base string, err error) {
	if len(p) > maxTemplatePath {
		return "", "", fmt.Errorf("%w: %s is longer than %d bytes", ErrPath, q(p), maxTemplatePath)
	}
	if !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return "", "", fmt.Errorf("%w: %s is not an absolute clean path on this host", ErrPath, q(p))
	}
	if err := CheckServicePath(p); err != nil {
		return "", "", fmt.Errorf("%w: %w", ErrPath, err)
	}
	vol := filepath.VolumeName(p)
	rest := strings.TrimLeft(p[len(vol):], `/\`)
	if rest == "" {
		return "", "", fmt.Errorf("%w: %s is a volume root", ErrPath, q(p))
	}
	if err := ops.CheckName(filepath.ToSlash(rest)); err != nil {
		return "", "", err
	}
	return filepath.Dir(p), filepath.Base(p), nil
}

// ancestors lists dir and every directory above it, the volume root first.
func ancestors(dir string) []string {
	var out []string
	for d := dir; ; d = filepath.Dir(d) {
		out = append(out, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// openOutsideDir walks dir from the volume root, refusing a symlink, junction
// or reparse point and any directory that is the project directory (a file
// under the project is named by its project-relative name and goes through
// the project's own rule), then opens dir as an os.Root and checks that the
// opened directory is the one the walk saw. project may be "" (a system unit
// directory is never under a project, but the check is cheap and kept).
func openOutsideDir(dir, project string) (*os.Root, error) {
	var projectInfo fs.FileInfo
	if project != "" {
		if fi, err := os.Stat(project); err == nil {
			projectInfo = fi
		} else {
			return nil, fmt.Errorf("%w: the project directory: %v", ErrDetect, err)
		}
	}
	var last fs.FileInfo
	for _, d := range ancestors(dir) {
		fi, err := os.Lstat(d)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s does not exist (no directory is created outside the project)", ErrPath, q(d))
		}
		if err != nil {
			return nil, fmt.Errorf("host: lstat %s: %w", q(d), err)
		}
		// The volume root itself is never a link; on Windows Lstat of "C:\"
		// reports a directory.
		if fi.Mode()&(fs.ModeSymlink|fs.ModeIrregular) != 0 {
			return nil, fmt.Errorf("%w: %s is a symlink, junction or reparse point", ErrPath, q(d))
		}
		if !fi.IsDir() {
			return nil, fmt.Errorf("%w: %s is not a directory", ErrPath, q(d))
		}
		if projectInfo != nil && os.SameFile(fi, projectInfo) {
			return nil, fmt.Errorf("%w: %s is under the project directory; name the file by its project-relative name", ErrPath, q(dir))
		}
		last = fi
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("host: opening %s: %w", q(dir), err)
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(opened, last) {
		_ = root.Close()
		return nil, fmt.Errorf("%w: %s changed while it was opened", ErrPath, q(dir))
	}
	return root, nil
}

// readOutside reads dir/base for a preview: the leaf must be a regular file
// (a symlink is refused, not followed), at most maxOutsideRead bytes, and the
// text is redacted before it reaches a diff. A missing file is ("", false, nil).
func readOutside(dir, base, project string) (string, bool, error) {
	root, err := openOutsideDir(dir, project)
	if err != nil {
		return "", false, err
	}
	defer root.Close()
	fi, err := root.Lstat(base)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("host: stat %s: %w", q(base), err)
	case !fi.Mode().IsRegular():
		return "", false, fmt.Errorf("%w: %s exists and is not a regular file (a symlink is refused)", ops.ErrNotRegular, q(filepath.Join(dir, base)))
	}
	f, err := root.OpenFile(base, os.O_RDONLY, 0)
	if err != nil {
		return "", false, fmt.Errorf("host: opening %s: %w", q(base), err)
	}
	defer f.Close()
	if after, err := f.Stat(); err != nil || !os.SameFile(after, fi) {
		return "", false, fmt.Errorf("%w: %s changed while it was opened", ErrPath, q(base))
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxOutsideRead+1))
	if err != nil {
		return "", false, fmt.Errorf("host: reading %s: %w", q(base), err)
	}
	if len(raw) > maxOutsideRead {
		return "", true, fmt.Errorf("%w: %s is larger than %d bytes", ops.ErrTooLarge, q(base), maxOutsideRead)
	}
	text, _ := redact.Text(strings.ToValidUTF8(string(raw), "?"))
	return text, true, nil
}

// writeOutside writes dir/base atomically through an os.Root on dir and
// reports whether the bytes or the mode changed. The leaf must be a regular
// file or missing: a symlink at the leaf is refused rather than replaced, so
// the step's preview and its effect name the same file.
func writeOutside(dir, base, project string, data []byte, mode fs.FileMode) (bool, error) {
	root, err := openOutsideDir(dir, project)
	if err != nil {
		return false, err
	}
	defer root.Close()
	if fi, lerr := root.Lstat(base); lerr == nil {
		if !fi.Mode().IsRegular() {
			return false, fmt.Errorf("%w: %s exists and is not a regular file (a symlink is refused)", ops.ErrNotRegular, q(filepath.Join(dir, base)))
		}
		sameMode := runtime.GOOS == "windows" || fi.Mode().Perm() == mode.Perm()
		if old, rerr := root.ReadFile(base); rerr == nil && bytes.Equal(old, data) && sameMode {
			return false, nil
		}
	} else if !errors.Is(lerr, fs.ErrNotExist) {
		return false, fmt.Errorf("host: stat %s: %w", q(base), lerr)
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return false, fmt.Errorf("host: generating a temp name: %w", err)
	}
	tmp := "." + base + ".piperoom-tmp-" + hex.EncodeToString(rnd[:])
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return false, fmt.Errorf("host: creating a temp file for %s: %w", q(base), err)
	}
	_, werr := f.Write(data)
	if werr == nil {
		werr = f.Chmod(mode)
	}
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = root.Rename(tmp, base)
	}
	if werr != nil {
		_ = root.Remove(tmp)
		return false, fmt.Errorf("host: writing %s: %w", q(filepath.Join(dir, base)), werr)
	}
	return true, nil
}

// removeOutside removes dir/base, best effort: a unit file this install
// created and could not enable. The caller already failed and reports that.
func removeOutside(dir, base string) {
	root, err := openOutsideDir(dir, "")
	if err != nil {
		return
	}
	defer root.Close()
	if fi, err := root.Lstat(base); err == nil && fi.Mode().IsRegular() {
		_ = root.Remove(base)
	}
}
