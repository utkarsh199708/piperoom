package host

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"

	"piperoom/internal/setup/protocol"
)

// UnitScope is the systemd manager a unit is installed into (lead ruling on
// U8a, KAN-533). It is never an op param: the driver selects it from what it
// detects (the runner's effective user) and from the target class, so the
// model cannot move a worker into the system manager by naming a field.
type UnitScope int

const (
	// ScopeUser is the default: a user unit (`systemctl --user`), its file
	// in the project's systemd/ and linked into ~/.config/systemd/user by
	// `systemctl --user enable <path>`.
	ScopeUser UnitScope = iota
	// ScopeSystem is a system unit (`systemctl` without --user), its file
	// in /etc/systemd/system, written by the runner running as root. A
	// system unit runs as root (the template has no User= line), so it is
	// installed only over a tree only root can change (checkRootOnly).
	ScopeSystem
)

// String is the scope as Data and summaries show it.
func (s UnitScope) String() string {
	if s == ScopeSystem {
		return "system"
	}
	return "user"
}

// SystemUnitDir is where a system unit file is written: the administrator's
// unit directory, root-owned, outside every user-writable directory. Linking a
// project file into the system manager instead would let the project's owner
// edit a root service.
const SystemUnitDir = "/etc/systemd/system"

// maxTrustEntries bounds the walk checkRootOnly does over the trees a system
// unit executes or loads (the distribution, the plugins, the JDK).
const maxTrustEntries = 100000

// selectScope is the scope rule: a system unit when the runner runs as root
// (root has no login session whose user manager would keep a user unit
// running, and `systemctl --user` under sudo has no bus) or when the target
// class is production (a production worker must start at boot, not at a
// login); a user unit otherwise. Only Linux has systemd scopes.
func selectScope(goos string, euid int, class string) UnitScope {
	if goos != "linux" {
		return ScopeUser
	}
	if euid == 0 || class == protocol.ClassProduction {
		return ScopeSystem
	}
	return ScopeUser
}

// fileOwner returns the owning uid of a file from its Lstat result. It reads
// the Uid field of the platform's stat structure by reflection, so this
// package imports no syscall (the AST test keeps it free of raw syscalls); a
// platform whose FileInfo carries no such field reports false, and every
// caller fails closed on it.
func fileOwner(fi fs.FileInfo) (uint64, bool) {
	v := reflect.ValueOf(fi.Sys())
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0, false
	}
	f := v.FieldByName("Uid")
	if !f.IsValid() {
		return 0, false
	}
	switch f.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return f.Uint(), true
	}
	return 0, false
}

// rootOnly refuses fi unless root owns it and no group or other user can
// write it. A sticky directory (the 1777 of /tmp) may be world-writable: only
// an entry's owner can rename or remove it there, and the entries under it
// are checked themselves.
func rootOnly(path string, fi fs.FileInfo, owner func(fs.FileInfo) (uint64, bool)) error {
	uid, ok := owner(fi)
	if !ok {
		return fmt.Errorf("%w: cannot read the owner of %s", ErrDetect, q(path))
	}
	if uid != 0 {
		return fmt.Errorf("%w: %s is owned by uid %d, not root; a system unit runs as root, so everything it executes or loads must be root's (chown -R root the project, or run as a user unit)", ErrDetect, q(path), uid)
	}
	perm := fi.Mode().Perm()
	if fi.Mode()&fs.ModeSymlink != 0 {
		return nil // a symlink's own bits mean nothing; its owner was checked
	}
	if perm&0o022 != 0 && !(fi.IsDir() && fi.Mode()&fs.ModeSticky != 0) {
		return fmt.Errorf("%w: %s is writable by group or others (mode %04o); a system unit runs as root", ErrDetect, q(path), perm)
	}
	return nil
}

// checkRootOnly is the precondition of a system unit: the project directory
// and every directory above it, and every entry of the trees the unit
// executes or loads (the distribution, the plugin directory, the worker file,
// the JDK), are root-owned and not writable by group or others. Otherwise a
// user who can write any of them could change what the root service runs.
// WalkDir does not follow symlinks; a symlink is checked as an entry (its
// owner), not followed. required trees (project-relative) must exist;
// optional ones are checked where they exist.
func checkRootOnly(project, javaHome string, required, optional []string, owner func(fs.FileInfo) (uint64, bool)) error {
	if javaHome == "" {
		return fmt.Errorf("%w: no JAVA_HOME to check", ErrDetect)
	}
	// The directories above the project and above the JDK: whoever can
	// write one of them can rename the tree under it and put another in
	// its place.
	for _, top := range []string{project, filepath.Dir(javaHome)} {
		for _, d := range ancestors(top) {
			fi, err := os.Lstat(d)
			if err != nil {
				return fmt.Errorf("%w: %s: %v", ErrDetect, q(d), err)
			}
			if err := rootOnly(d, fi, owner); err != nil {
				return err
			}
		}
	}
	seen := 0
	walk := func(top string, required bool) error {
		return filepath.WalkDir(top, func(p string, de fs.DirEntry, err error) error {
			if err != nil {
				if !required && p == top && errors.Is(err, fs.ErrNotExist) {
					return filepath.SkipDir
				}
				return fmt.Errorf("%w: %s: %v", ErrDetect, q(p), err)
			}
			seen++
			if seen > maxTrustEntries {
				return fmt.Errorf("%w: more than %d files under the trees a system unit loads", ErrDetect, maxTrustEntries)
			}
			fi, err := de.Info()
			if err != nil {
				return fmt.Errorf("%w: %s: %v", ErrDetect, q(p), err)
			}
			return rootOnly(p, fi, owner)
		})
	}
	for _, t := range required {
		if err := walk(filepath.Join(project, filepath.FromSlash(t)), true); err != nil {
			return err
		}
	}
	for _, t := range optional {
		if err := walk(filepath.Join(project, filepath.FromSlash(t)), false); err != nil {
			return err
		}
	}
	return walk(javaHome, true)
}
