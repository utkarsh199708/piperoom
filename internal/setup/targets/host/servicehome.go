package host

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"

	"piperoom/internal/setup/ops"
)

// The Windows service home (row U8b). The project directory is the person's
// workspace: their account writes it, and so does every unelevated op
// (properties_write, plugin_install, a re-extracted distribution). A service
// that ran from it would run whatever that account, or any process running as
// it, puts there — and no DACL on a folder inside the project closes that,
// because the project directory and its parents stay the person's, and a
// rename needs only delete-child on the parent.
//
// So the service never runs from the project. service_install (T2, elevated,
// approved) stages a release into an admin-only home:
//
//	%ProgramFiles%\Piperoom\piperoom-<name>\
//	    releases\<id>\bin\piperoom-setup.exe     the runner copy the SCM starts
//	    releases\<id>\kafka\libs\...             the distribution's classpath
//	    releases\<id>\kafka\config\...           its log4j configuration
//	    releases\<id>\connect-plugins\...        the connector plugins
//	    releases\<id>\config\connect-<mode>.properties
//	    logs\                                    the service writes
//	    data\                                    a standalone worker's offsets
//	    service.txt                              the definition install recorded
//
// %ProgramFiles% comes from the known-folder API, never from the environment,
// and only Administrators can create a folder in it. The id is the content
// digest of the release, so an approver's preview names exactly what runs. A
// release is never changed after it is staged: a new install stages a new one,
// and the service's definition switches to it in one CreateService. The worker
// file in the release is validated again by the properties allow-list and its
// paths are rewritten into the release, so nothing the service reads points
// back into the project.
const (
	homeVendorDir    = "Piperoom"
	releasesDir      = "releases"
	homeLogDir       = "logs"
	homeDataDir      = "data"
	homeRecordName   = "service.txt"
	releaseConfigDir = "config"
	// stagePrefix names a release being copied; a leftover one (an install
	// that died mid-copy) is removed by the next install.
	stagePrefix = ".stage-"

	// Caps of one release: a distribution is about 150 files and 120 MB, a
	// plugin a few hundred files; the caps refuse a project that is not one.
	maxReleaseFiles = 20000
	maxReleaseBytes = 4 << 30
	maxWorkerBytes  = 64 << 10
)

// releaseIDRE is a release directory name: the first 16 hex digits of the
// release's content digest.
var releaseIDRE = regexp.MustCompile(`^[0-9a-f]{16}$`)

// ErrUntrusted: a path the service would execute or load can be changed by an
// account that is not SYSTEM, Administrators or TrustedInstaller.
var ErrUntrusted = errors.New("host: a path the service runs from is writable by a non-administrator")

// trustAPI checks that only the machine's administrators can change a path.
// The Windows build reads the owner and DACL of the path, of every directory
// above it and, for a tree, of every entry under it (servicehome_windows.go);
// the tests record the calls.
type trustAPI interface {
	// CheckTrusted returns ErrUntrusted unless path and every directory above
	// it are owned by SYSTEM, Administrators or TrustedInstaller and grant no
	// other account the right to write, delete, rename or re-permission them
	// (and, with tree, the same of everything under path). A link or other
	// reparse point anywhere on the way is refused, never followed.
	CheckTrusted(path string, tree bool) error
}

// serviceHome is the admin-only home of one service.
func serviceHome(programFiles, unit string) string {
	return nativeJoin(programFiles, homeVendorDir, unit)
}

// releasePath is one release directory under home.
func releasePath(home, id string) string { return nativeJoin(home, releasesDir, id) }

// releaseFile is one file of a release.
type releaseFile struct {
	rel  string // slash path inside the release
	src  string // project-relative source, or "" for the runner
	abs  string // absolute source outside the project (the runner binary)
	sum  string // hex SHA-256
	size int64
}

// releasePlan is what install stages: every file with its digest, the worker
// file to render, and the release id the digests make.
type releasePlan struct {
	files  []releaseFile
	dirs   []string // slash paths inside the release, parents first
	worker workerProps
	// workerName is the worker file's name under config\.
	workerName string
	digest     string // hex SHA-256 of the manifest
	id         string
}

// releaseSources are the project directories a release copies, and whether
// the project must have them.
var releaseSources = []struct {
	dir      string
	required bool
}{
	{DistDir + "/libs", true},
	{DistDir + "/config", false},
	{PluginDir, false},
}

// planRelease reads what a release of this project would hold: the runner
// binary, the distribution's libs and config, the plugins, and the worker file
// for mode, validated again by the properties allow-list (the file under the
// project is the person's to edit by hand, so what properties_write wrote is
// not assumed). Every file is hashed; stage copies it again and refuses one
// whose bytes changed in between.
func planRelease(env ops.Env, runner, mode, class, home string) (releasePlan, error) {
	if env.Confine == nil {
		return releasePlan{}, errors.New("host: the project root is not open")
	}
	dir := env.Confine.Dir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return releasePlan{}, fmt.Errorf("host: opening the project root: %w", err)
	}
	defer root.Close()

	p := releasePlan{dirs: []string{BinDir, releaseConfigDir}}
	var total int64
	add := func(f releaseFile) error {
		if len(p.files) >= maxReleaseFiles {
			return fmt.Errorf("%w: the release would hold more than %d files", ErrDetect, maxReleaseFiles)
		}
		total += f.size
		if total > maxReleaseBytes {
			return fmt.Errorf("%w: the release would hold more than %d bytes", ErrDetect, int64(maxReleaseBytes))
		}
		p.files = append(p.files, f)
		return nil
	}

	sum, size, err := hashFile(func() (*os.File, error) { return os.Open(runner) })
	if err != nil {
		return releasePlan{}, fmt.Errorf("%w: reading the piperoom-setup binary %s: %w", ErrDetect, q(runner), err)
	}
	if err := add(releaseFile{rel: BinDir + "/" + RunnerExe, abs: runner, sum: sum, size: size}); err != nil {
		return releasePlan{}, err
	}

	for _, src := range releaseSources {
		fi, err := root.Lstat(src.dir)
		switch {
		case errors.Is(err, fs.ErrNotExist) && !src.required:
			continue
		case err != nil:
			return releasePlan{}, fmt.Errorf("%w: %s under the project directory: %w", ErrDetect, q(src.dir), err)
		case !fi.IsDir():
			return releasePlan{}, fmt.Errorf("%w: %s under the project directory is not a plain directory", ErrDetect, q(src.dir))
		}
		werr := fs.WalkDir(root.FS(), src.dir, func(name string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				p.dirs = append(p.dirs, name)
				return nil
			}
			if !d.Type().IsRegular() {
				return fmt.Errorf("%w: %s is a link or another reparse point; a release holds plain files only", ErrDetect, q(name))
			}
			sum, size, err := hashFile(func() (*os.File, error) { return root.Open(name) })
			if err != nil {
				return fmt.Errorf("host: reading %s: %w", q(name), err)
			}
			return add(releaseFile{rel: name, src: name, sum: sum, size: size})
		})
		if werr != nil {
			return releasePlan{}, werr
		}
	}

	files, ok := modeFiles[mode]
	if !ok {
		return releasePlan{}, fmt.Errorf("%w: mode %s is not distributed or standalone", ErrParams, q(mode))
	}
	p.workerName = files[1]
	text, err := readCapped(root, files[1], maxWorkerBytes)
	if err != nil {
		return releasePlan{}, fmt.Errorf("%w: the worker file %s (properties_write writes it): %w", ErrDetect, q(files[1]), err)
	}
	entries, _ := parseProperties(string(text))
	p.worker, err = prepareProps(mode, entries, dir, class)
	if err != nil {
		return releasePlan{}, fmt.Errorf("the worker file %s: %w", q(files[1]), err)
	}
	// The worker's own bytes depend on the release directory, which depends on
	// the digest; the manifest therefore names the worker rendered against a
	// fixed placeholder, which is the same text for the same entries.
	tmpl, err := p.renderWorker(dir, "<release>", home)
	if err != nil {
		return releasePlan{}, err
	}

	sort.Strings(p.dirs)
	sort.Slice(p.files, func(i, j int) bool { return p.files[i].rel < p.files[j].rel })
	h := sha256.New()
	for _, d := range p.dirs {
		fmt.Fprintf(h, "d %s\n", d)
	}
	for _, f := range p.files {
		fmt.Fprintf(h, "f %s %d %s\n", f.sum, f.size, f.rel)
	}
	fmt.Fprintf(h, "w %x %s/%s\n", sha256.Sum256(tmpl), releaseConfigDir, p.workerName)
	p.digest = hex.EncodeToString(h.Sum(nil))
	p.id = p.digest[:16]
	return p, nil
}

// renderWorker is the worker file the release holds: the validated entries,
// with plugin.path rewritten from the project's plugin directory to the
// release's and a standalone offset file moved to the home's data directory.
// No other key holds a path (the allow-list has these two), so nothing the
// worker reads points back into the project.
func (p releasePlan) renderWorker(project, release, home string) ([]byte, error) {
	base := strings.ReplaceAll(project, "\\", "/")
	out := workerProps{mode: p.worker.mode, entries: make(map[string]string, len(p.worker.entries)),
		keys: p.worker.keys, defaults: p.worker.defaults}
	for key, value := range p.worker.entries {
		out.entries[key] = value
	}
	if v, ok := p.worker.entries["plugin.path"]; ok {
		parts := strings.Split(v, ",")
		for i, part := range parts {
			rel, err := relUnder(base, part)
			if err != nil || (rel != PluginDir && !strings.HasPrefix(rel, PluginDir+"/")) {
				return nil, fmt.Errorf("%w: plugin.path %s is not the project's %s/", ErrProperties, q(part), PluginDir)
			}
			parts[i] = projectValue(release, rel)
		}
		out.entries["plugin.path"] = strings.Join(parts, ",")
	}
	if v, ok := p.worker.entries["offset.storage.file.filename"]; ok {
		rel, err := relUnder(base, v)
		if err != nil || !offsetsRE.MatchString(rel) {
			return nil, fmt.Errorf("%w: offset.storage.file.filename %s is not a *.offsets file at the project root", ErrProperties, q(v))
		}
		out.entries["offset.storage.file.filename"] = projectValue(home, homeDataDir, rel)
	}
	return out.render(), nil
}

// offsetsFile is where a standalone worker of this plan keeps its offsets
// (under the home's data directory), or "" for a distributed worker.
func (p releasePlan) offsetsFile(project, home string) string {
	v, ok := p.worker.entries["offset.storage.file.filename"]
	if !ok {
		return ""
	}
	rel, err := relUnder(strings.ReplaceAll(project, "\\", "/"), v)
	if err != nil {
		return ""
	}
	return nativeJoin(home, homeDataDir, rel)
}

// stage copies the plan into home\releases\<id> and returns that directory.
// It copies into a fresh .stage- directory first and renames it into place,
// so a release directory is either complete or absent; a release of the same
// id (an install of unchanged content) is replaced by the fresh copy. Every
// file is hashed again while it is copied, and one whose bytes differ from the
// plan is refused: the release holds what the plan named, or nothing.
func (p releasePlan) stage(env ops.Env, home string) (string, error) {
	if err := os.MkdirAll(nativeJoin(home, releasesDir), 0o755); err != nil {
		return "", fmt.Errorf("host: creating the service home %s: %w", q(home), err)
	}
	for _, d := range []string{homeLogDir, homeDataDir} {
		if err := os.MkdirAll(nativeJoin(home, d), 0o755); err != nil {
			return "", fmt.Errorf("host: creating %s: %w", q(nativeJoin(home, d)), err)
		}
	}
	hroot, err := os.OpenRoot(home)
	if err != nil {
		return "", fmt.Errorf("host: opening the service home %s: %w", q(home), err)
	}
	defer hroot.Close()
	proot, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return "", fmt.Errorf("host: opening the project root: %w", err)
	}
	defer proot.Close()

	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("host: naming the staging directory: %w", err)
	}
	tmp := releasesDir + "/" + stagePrefix + hex.EncodeToString(nonce[:])
	if err := hroot.Mkdir(tmp, 0o755); err != nil {
		return "", fmt.Errorf("host: creating %s: %w", q(tmp), err)
	}
	fail := func(err error) (string, error) {
		_ = hroot.RemoveAll(tmp)
		return "", err
	}
	for _, d := range p.dirs {
		if err := hroot.MkdirAll(tmp+"/"+d, 0o755); err != nil {
			return fail(fmt.Errorf("host: creating %s in the release: %w", q(d), err))
		}
	}
	for _, f := range p.files {
		open := func() (*os.File, error) { return proot.Open(f.src) }
		if f.src == "" {
			open = func() (*os.File, error) { return os.Open(f.abs) }
		}
		if err := copyVerified(hroot, tmp+"/"+f.rel, open, f.sum, f.size); err != nil {
			return fail(err)
		}
	}
	release := releasePath(home, p.id)
	worker, err := p.renderWorker(env.Confine.Dir(), release, home)
	if err != nil {
		return fail(err)
	}
	if err := hroot.WriteFile(tmp+"/"+releaseConfigDir+"/"+p.workerName, worker, 0o644); err != nil {
		return fail(fmt.Errorf("host: writing the release's worker file: %w", err))
	}
	final := releasesDir + "/" + p.id
	if err := hroot.RemoveAll(final); err != nil {
		return fail(fmt.Errorf("host: replacing the release %s: %w", q(p.id), err))
	}
	if err := hroot.Rename(tmp, final); err != nil {
		return fail(fmt.Errorf("host: moving the release %s into place: %w", q(p.id), err))
	}
	return release, nil
}

// copyVerified copies one file into the release with O_EXCL and checks its
// digest and size against the plan.
func copyVerified(hroot *os.Root, dst string, open func() (*os.File, error), sum string, size int64) error {
	src, err := open()
	if err != nil {
		return fmt.Errorf("host: opening %s to stage it: %w", q(dst), err)
	}
	defer src.Close()
	if fi, err := src.Stat(); err != nil || !fi.Mode().IsRegular() {
		return fmt.Errorf("%w: the source of %s is no longer a regular file", ErrDetect, q(dst))
	}
	out, err := hroot.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("host: creating %s in the release: %w", q(dst), err)
	}
	h := sha256.New()
	n, cerr := io.Copy(io.MultiWriter(out, h), io.LimitReader(src, size+1))
	if err := out.Close(); cerr == nil {
		cerr = err
	}
	if cerr != nil {
		return fmt.Errorf("host: copying %s into the release: %w", q(dst), cerr)
	}
	if n != size || hex.EncodeToString(h.Sum(nil)) != sum {
		return fmt.Errorf("%w: %s changed while the release was staged; run service_install again", ErrDetect, q(dst))
	}
	return nil
}

// hashFile returns the hex SHA-256 and the size of the file open returns.
func hashFile(open func() (*os.File, error)) (string, int64, error) {
	f, err := open()
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", 0, err
	}
	if !fi.Mode().IsRegular() {
		return "", 0, fmt.Errorf("%w: not a regular file", ErrDetect)
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// readCapped reads a project file through root, refusing one over limit bytes
// or one that is not a regular file.
func readCapped(root *os.Root, name string, limit int64) ([]byte, error) {
	fi, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: not a regular file", ops.ErrNotRegular)
	}
	if fi.Size() > limit {
		return nil, fmt.Errorf("larger than %d bytes", limit)
	}
	return root.ReadFile(name)
}

// pruneReleases removes every release under home except keep, and any staging
// directory an earlier install left behind. It runs after the service runs from
// keep, so a release still named by the previous definition is never removed
// while a rollback could need it. Failures are returned joined: an old release
// that cannot be removed now is removed by the next install.
func pruneReleases(home, keep string) error {
	hroot, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer hroot.Close()
	entries, err := fs.ReadDir(hroot.FS(), releasesDir)
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if e.Name() == keep {
			continue
		}
		if !releaseIDRE.MatchString(e.Name()) && !strings.HasPrefix(e.Name(), stagePrefix) {
			continue
		}
		if err := hroot.RemoveAll(path.Join(releasesDir, e.Name())); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// readHomeRecord returns the definition install recorded in the home, and
// whether it exists.
func readHomeRecord(home string) ([]byte, bool) {
	hroot, err := os.OpenRoot(home)
	if err != nil {
		return nil, false
	}
	defer hroot.Close()
	b, err := readCapped(hroot, homeRecordName, maxWorkerBytes)
	if err != nil {
		return nil, false
	}
	return b, true
}

// writeHomeRecord replaces the home's record atomically (a temporary file
// renamed over it), or removes it when data is nil.
func writeHomeRecord(home string, data []byte) error {
	hroot, err := os.OpenRoot(home)
	if err != nil {
		return fmt.Errorf("host: opening the service home: %w", err)
	}
	defer hroot.Close()
	if data == nil {
		if err := hroot.Remove(homeRecordName); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("host: removing the service record: %w", err)
		}
		return nil
	}
	tmp := homeRecordName + ".tmp"
	_ = hroot.Remove(tmp)
	if err := hroot.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("host: writing the service record: %w", err)
	}
	if err := hroot.Rename(tmp, homeRecordName); err != nil {
		_ = hroot.Remove(tmp)
		return fmt.Errorf("host: writing the service record: %w", err)
	}
	return nil
}

// homeACLTargets is what install hardens in the home: everything read and
// execute for the service and Users, the logs and data directories writable
// by the service.
func homeACLTargets(home string) []aclTarget {
	return []aclTarget{
		{Path: home},
		{Path: nativeJoin(home, homeLogDir), Write: true},
		{Path: nativeJoin(home, homeDataDir), Write: true},
	}
}

// nativeDir is the directory of a path of either shape ("\\" or "/");
// filepath.Dir splits by the build's separator only.
func nativeDir(p string) string {
	p = strings.TrimRight(p, `/\`)
	if i := strings.LastIndexAny(p, `/\`); i > 0 {
		return p[:i]
	}
	return p
}

// nativeBase is the final element of a path of either shape.
func nativeBase(p string) string { return lastComponent(strings.TrimRight(p, `/\`)) }

// samePath compares two paths of the same shape: a drive path
// case-insensitively, as Windows does, a POSIX path exactly.
func samePath(a, b string) bool {
	a, b = strings.TrimRight(a, `/\`), strings.TrimRight(b, `/\`)
	if a == "" || b == "" {
		return false
	}
	if isDrivePath(a) {
		return strings.EqualFold(a, b)
	}
	return a == b
}
