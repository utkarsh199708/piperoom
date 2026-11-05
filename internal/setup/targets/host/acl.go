package host

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

// The DACL half of the service home (row U8b; servicehome.go). The service
// runs as its own virtual account from a release staged into an admin-only
// home under Program Files; install gives that home, and everything in it, a
// protected DACL owned by Administrators: Administrators and SYSTEM full
// control, the service's account and Users read and execute, and the service's
// account modify on the home's logs and data directories, the only places it
// writes. The project directory is never re-permissioned: it stays the
// person's workspace, and nothing the service runs or reads is in it.
//
// The Windows build applies this (acl_windows.go); every other build refuses
// it, the way the SCM binding does, and the manager's own logic is tested with
// a fake.

// aclTarget is one path install hardens: a directory the service executes or
// loads from, or one it writes.
type aclTarget struct {
	// Path is the absolute path under the service home.
	Path string `json:"path"`
	// Write is whether the service's account may change it: the home's logs
	// and data directories, and nothing else.
	Write bool `json:"write"`
}

// aclAPI sets the DACLs that keep the service's home admin-only. The Windows build implements it (acl_windows.go); the tests
// record the calls.
type aclAPI interface {
	// Harden gives every target, and everything under it, a protected DACL and
	// takes its ownership (hardenOrder):
	// Administrators and SYSTEM full control, the account named by account read
	// and execute (read, write, execute and delete where the target is
	// writable), and Users read and execute, each inherited by everything the
	// directory holds.
	Harden(account string, targets []aclTarget) error
}

// hardenOrder lists root and everything under it in the order Harden sets
// them: every entry before the directory that holds it. Each entry gets its own
// protected DACL and owner, not only the directory: SetNamedSecurityInfo passes
// inherited ACEs down but never the owner, so a jar the person's account
// extracted would otherwise stay theirs, and an owner can always rewrite a
// file's DACL and then the file. Children go first because only their old,
// inherited DACL still lets a non-elevated caller set them, and a protected
// child is skipped by the directory's own propagation afterwards. A symlink,
// junction or other reparse point is refused rather than followed: setting its
// security would change the file it points at, outside the project.
func hardenOrder(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && !d.Type().IsRegular() {
			return fmt.Errorf("%w: %s is a link or another reparse point; the service's files must be plain files and directories", ErrDetect, q(path))
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("host: listing %s to harden: %w", q(root), err)
	}
	for i, j := 0, len(paths)-1; i < j; i, j = i+1, j-1 {
		paths[i], paths[j] = paths[j], paths[i]
	}
	return paths, nil
}

// lastComponent is the final element of a path of either shape ("\" or "/"),
// for a summary; filepath.Base splits by the build's separator only.
func lastComponent(p string) string {
	if i := strings.LastIndexAny(p, `/\`); i >= 0 {
		return p[i+1:]
	}
	return p
}
