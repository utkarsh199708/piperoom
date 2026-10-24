package server

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"

	"piperoom/internal/setup/ops"
)

// driverFiles are the only paths this driver writes; both are under a
// directory registered as a mount (the distribution the unit executes, the
// systemd directory), so the exemption below is exactly two files wide.
func driverFile(clean string) bool {
	return clean == ConfigFile || clean == UnitDir+"/"+UnitName
}

// writeFile writes name under the project directory through the U1-2 path
// rule (ops.Confiner) and an os.Root, atomically: an O_EXCL temporary file
// renamed over the target. It reports whether the bytes changed. A path under
// a registered mount directory is refused unless it is one of the two files
// this driver owns, and a materialised secret file is never replaced.
func writeFile(env ops.Env, name string, data []byte, mode fs.FileMode) (string, bool, error) {
	if env.Confine == nil {
		return "", false, errors.New("server: the project root is not open")
	}
	clean, err := env.Confine.Check(name)
	if err != nil {
		return "", false, err
	}
	if ops.MountedPath(clean, env.MountDirs) && !driverFile(clean) {
		return "", false, fmt.Errorf("%w: %s is under a directory the server executes or loads from", ops.ErrExecutedName, q(name))
	}
	if err := ops.MaterialisedRefusal(clean, env); err != nil {
		return "", false, err
	}
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return "", false, fmt.Errorf("server: opening the project root: %w", err)
	}
	defer root.Close()
	if fi, lerr := root.Lstat(clean); lerr == nil {
		if !fi.Mode().IsRegular() {
			return "", false, fmt.Errorf("%w: %s exists and is not a regular file", ops.ErrNotRegular, q(name))
		}
		if old, rerr := root.ReadFile(clean); rerr == nil && bytes.Equal(old, data) {
			// Unchanged bytes are a no-op write, but the mode still applies: a
			// pre-existing file must not keep a wider mode because its content
			// already matched.
			if fi.Mode().Perm() != mode.Perm() {
				if cerr := root.Chmod(clean, mode); cerr != nil {
					return "", false, fmt.Errorf("server: fixing the mode of %s: %w", q(name), cerr)
				}
			}
			return clean, false, nil
		}
	} else if !errors.Is(lerr, fs.ErrNotExist) {
		return "", false, fmt.Errorf("server: stat %s: %w", q(name), lerr)
	}
	if dir := path.Dir(clean); dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return "", false, fmt.Errorf("server: creating %s: %w", q(path.Dir(name)), err)
		}
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", false, fmt.Errorf("server: generating a temp name: %w", err)
	}
	tmp := path.Join(path.Dir(clean), "."+path.Base(clean)+".piperoom-tmp-"+hex.EncodeToString(rnd[:]))
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return "", false, fmt.Errorf("server: creating a temp file for %s: %w", q(name), err)
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
		return "", false, fmt.Errorf("server: writing %s: %w", q(name), werr)
	}
	return clean, true, nil
}
