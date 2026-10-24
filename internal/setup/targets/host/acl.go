package host

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"piperoom/internal/setup/ops"
)

// The DACL half of the service-account fix (row U8b). The service runs as its
// own virtual account (winsvc.go, serviceAccountPrefix), but the files it
// executes live under the project directory, which the person's own account
// writes. Without an ACL, a medium-integrity process of that account can
// replace bin\piperoom-setup.exe, a jar under kafka\libs, a connector jar under
// connect-plugins\ or a recorded definition under services\, and the service
// runs it at its next (automatic) start. install therefore sets a protected
// DACL and takes ownership of every directory the service executes or loads
// from, so only Administrators and SYSTEM can write them and the service's
// account and Users can read and run what is in them. logs\ and a standalone
// worker's offset file are the two places the service writes, so they grant the
// service's account modify.
//
// The Windows build applies this (acl_windows.go); every other build refuses
// it, the way the SCM binding does, and the manager's own logic is tested with
// a fake.
//
// What this leaves in place, and why: the worker properties file sits at the
// project root, which stays writable by the person's account, because
// properties_write (T1) writes it through an unelevated run and an elevated T1
// path would be a design change. That file names plugin.path and the
// connectors' classes, so a file the person's account can change still decides
// what the service loads — including a plugin directory outside the project.
// The ACLs close the planted exe, jar, plugin and service record; the worker
// file needs either an elevated properties_write on Windows or a ruling, and
// the PR records that as the remaining risk.

// aclTarget is one path install hardens: a directory the service executes or
// loads from, or one it writes.
type aclTarget struct {
	// Path is the absolute path under the project directory.
	Path string `json:"path"`
	// Write is whether the service's account may change it: the log directory
	// and a standalone worker's offset file, and nothing else.
	Write bool `json:"write"`
}

// aclAPI sets the DACLs that keep a user-writable file from becoming code the
// service runs. The Windows build implements it (acl_windows.go); the tests
// record the calls.
type aclAPI interface {
	// Harden gives every target a protected DACL and takes its ownership:
	// Administrators and SYSTEM full control, the account named by account read
	// and execute (read, write, execute and delete where the target is
	// writable), and Users read and execute, each inherited by everything the
	// directory holds.
	Harden(account string, targets []aclTarget) error
}

// serviceACLTargets is what install hardens for s: the directories the SCM runs
// and the JVM loads from, the log directory the service writes, and the
// offsets file a standalone worker writes.
func serviceACLTargets(s serviceSpec) []aclTarget {
	dir := func(name string, write bool) aclTarget {
		return aclTarget{Path: nativeJoin(s.ProjectDir, name), Write: write}
	}
	targets := []aclTarget{
		dir(BinDir, false),
		dir(DistDir, false),
		dir(PluginDir, false),
		dir(ServiceDir, false),
		dir(LogDir, true),
	}
	if s.Offsets != "" {
		targets = append(targets, aclTarget{Path: s.Offsets, Write: true})
	}
	return targets
}

// ensureServiceFiles creates the directories install hardens and, for a
// standalone worker, the offset file, before any DACL is set. Both matter for
// the service to start: the account has no write access to the project root, so
// a directory or offset file it must write has to exist first, and a directory
// created later by the person's account would carry that account's inherited
// ACEs instead of the ones install sets.
func ensureServiceFiles(env ops.Env, s serviceSpec) error {
	if env.Confine == nil {
		return errors.New("host: the project root is not open")
	}
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return fmt.Errorf("host: opening the project root: %w", err)
	}
	defer root.Close()
	for _, dir := range []string{BinDir, DistDir, PluginDir, ServiceDir, LogDir} {
		clean, err := env.Confine.Check(dir)
		if err != nil {
			return err
		}
		if err := root.MkdirAll(clean, 0o755); err != nil {
			return fmt.Errorf("host: creating %s: %w", q(dir), err)
		}
	}
	if s.Offsets == "" {
		return nil
	}
	f, err := root.OpenFile(OffsetsFile, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("host: creating the offset file %s: %w", q(OffsetsFile), err)
	}
	return f.Close()
}

// lastComponent is the final element of a path of either shape ("\" or "/"),
// for a summary; filepath.Base splits by the build's separator only.
func lastComponent(p string) string {
	if i := strings.LastIndexAny(p, `/\`); i >= 0 {
		return p[i+1:]
	}
	return p
}
