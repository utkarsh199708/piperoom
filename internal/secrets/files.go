package secrets

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// DefaultFilePoll is how often mounted files are checked for changes (§3).
const DefaultFilePoll = 30 * time.Second

// FilesProvider reads secrets from files mounted by a secret manager's
// agent (CSI driver, External Secrets, Vault Agent, ...). It is the
// universal provider, id spec.ProviderFiles: secret://files/run/secrets/x
// reads /run/secrets/x. Only files under the allowed roots are read, and the
// root check runs on the path after symlinks are resolved. Files that were
// read are polled for changes; a change is reported through SetOnChange.
type FilesProvider struct {
	roots []string // cleaned, absolute, symlinks resolved when they exist
	poll  time.Duration

	mu       sync.Mutex
	onChange func(path string)
	watched  map[string]fileStamp // Ref.Path → last stamp

	cancel context.CancelFunc
	done   chan struct{}
}

type fileStamp struct {
	real  string
	size  int64
	mtime time.Time
}

// FilesOptions configure NewFilesProvider.
type FilesOptions struct {
	// Roots are the directories references may read under
	// (config.SecretFileRoots). Must not be empty.
	Roots []string
	// Poll is the change-check interval; 0 = DefaultFilePoll.
	Poll time.Duration
}

// ParseRoots splits a comma list (PIPEROOM_SECRET_FILE_ROOTS).
func ParseRoots(s string) []string {
	var out []string
	for _, r := range strings.Split(s, ",") {
		if r = strings.TrimSpace(r); r != "" {
			out = append(out, r)
		}
	}
	return out
}

// NewFilesProvider checks the roots and starts the change poller; Close
// stops it.
func NewFilesProvider(opts FilesOptions) (*FilesProvider, error) {
	if len(opts.Roots) == 0 {
		return nil, fmt.Errorf("%w: files provider needs at least one root", ErrBadProviderConfig)
	}
	p := &FilesProvider{poll: opts.Poll, watched: map[string]fileStamp{}, done: make(chan struct{})}
	if p.poll <= 0 {
		p.poll = DefaultFilePoll
	}
	for _, r := range opts.Roots {
		if !filepath.IsAbs(r) {
			return nil, fmt.Errorf("%w: secret file root %q is not absolute", ErrBadProviderConfig, r)
		}
		r = filepath.Clean(r)
		if r == filepath.Dir(r) {
			return nil, fmt.Errorf("%w: secret file root %q is the file system root", ErrBadProviderConfig, r)
		}
		p.roots = append(p.roots, r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go p.loop(ctx)
	return p, nil
}

// GoString keeps %#v to the String form.
func (p *FilesProvider) GoString() string { return p.String() }

// String names the provider.
func (p *FilesProvider) String() string { return "files provider " + strings.Join(p.roots, ",") }

// SetOnChange implements ChangeNotifier.
func (p *FilesProvider) SetOnChange(fn func(path string)) {
	p.mu.Lock()
	p.onChange = fn
	p.mu.Unlock()
}

// Close stops the poller.
func (p *FilesProvider) Close() {
	p.cancel()
	<-p.done
}

// filePath maps a Ref.Path to a file system path.
func filePath(refPath string) string {
	if runtime.GOOS == "windows" && filepath.VolumeName(refPath) != "" {
		return filepath.Clean(filepath.FromSlash(refPath))
	}
	return filepath.Clean(filepath.FromSlash("/" + refPath))
}

// errWorkloadIdentity refuses Piperoom's own workload-identity tokens.
var errWorkloadIdentity = fmt.Errorf("%w: Piperoom's own workload-identity token cannot be read as a secret", ErrAccessDenied)

// isWorkloadIdentityPath reports whether p lies in a directory that holds the
// pod's own identity: the Kubernetes service-account token (also used by the
// kubernetes provider and Vault kubernetes auth), the EKS IRSA token, or the
// Azure workload-identity token. The default roots include /var/run/secrets,
// so without this a files reference could hand that identity to a target.
func isWorkloadIdentityPath(p string) bool {
	segs := strings.Split(filepath.ToSlash(p), "/")
	for i, s := range segs {
		if s == "serviceaccount" {
			return true
		}
		if s == "azure" && i+1 < len(segs) && segs[i+1] == "tokens" {
			return true
		}
	}
	return false
}

// realPath resolves symlinks and checks the result is under a root.
func (p *FilesProvider) realPath(refPath string) (string, error) {
	if isWorkloadIdentityPath(filePath(refPath)) {
		return "", errWorkloadIdentity
	}
	real, err := filepath.EvalSymlinks(filePath(refPath))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("%w: no file at %s", ErrNotFound, refPath)
		}
		return "", fmt.Errorf("secrets: files: resolving %s: %w", refPath, err)
	}
	if !p.underRoot(real) {
		return "", fmt.Errorf("%w: %s", ErrOutsideRoots, refPath)
	}
	if isWorkloadIdentityPath(real) {
		return "", errWorkloadIdentity
	}
	return real, nil
}

func (p *FilesProvider) underRoot(real string) bool {
	for _, root := range p.roots {
		for _, r := range rootForms(root) {
			if real != r && strings.HasPrefix(real, r+string(filepath.Separator)) {
				return true
			}
		}
	}
	return false
}

// rootForms is the root as given and with its symlinks resolved, so a root
// that is itself a symlink (/var/run → /run) still matches resolved paths.
func rootForms(root string) []string {
	out := []string{root}
	if rr, err := filepath.EvalSymlinks(root); err == nil && rr != root {
		out = append(out, rr)
	}
	return out
}

// Resolve reads the file. Version is its size and modification time.
func (p *FilesProvider) Resolve(ctx context.Context, ref Ref) (Value, error) {
	if err := ctx.Err(); err != nil {
		return Value{}, err
	}
	if ref.Version != "" {
		return Value{}, fmt.Errorf("%w: mounted files have no versions", ErrInvalidRef)
	}
	real, err := p.realPath(ref.Path)
	if err != nil {
		return Value{}, err
	}
	f, err := os.Open(real)
	if err != nil {
		if errors.Is(err, fs.ErrPermission) {
			return Value{}, fmt.Errorf("%w: cannot open %s", ErrAccessDenied, ref.Path)
		}
		return Value{}, fmt.Errorf("secrets: files: opening %s: %w", ref.Path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return Value{}, fmt.Errorf("secrets: files: stat %s: %w", ref.Path, err)
	}
	if !st.Mode().IsRegular() {
		return Value{}, fmt.Errorf("%w: %s is not a regular file", ErrNotFound, ref.Path)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxSecretBytes+1))
	if err != nil {
		return Value{}, fmt.Errorf("secrets: files: reading %s: %w", ref.Path, err)
	}
	if len(data) > maxSecretBytes {
		return Value{}, fmt.Errorf("secrets: files: %s is larger than %d bytes", ref.Path, maxSecretBytes)
	}
	stamp := fileStamp{real: real, size: st.Size(), mtime: st.ModTime()}
	p.mu.Lock()
	p.watched[ref.Path] = stamp
	p.mu.Unlock()
	// Mounted secrets usually end with a newline the manager added.
	text := strings.TrimRight(string(data), "\r\n")
	return Value{Data: netsec.Secret(text), Version: stamp.version()}, nil
}

func (s fileStamp) version() string {
	return strconv.FormatInt(s.size, 10) + "-" + strconv.FormatInt(s.mtime.UnixNano(), 36)
}

// List returns the files directly in the directory part of prefix whose
// names start with the rest, as reference paths. Only files under the roots.
func (p *FilesProvider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix = strings.TrimPrefix(prefix, "/")
	if strings.Contains(prefix, "..") {
		return nil, fmt.Errorf("%w: '..' in prefix", ErrInvalidRef)
	}
	dir, namePrefix := "", prefix
	if i := strings.LastIndexByte(prefix, '/'); i >= 0 {
		dir, namePrefix = prefix[:i], prefix[i+1:]
	}
	out := []string{}
	if dir == "" {
		// No directory yet: offer the roots themselves.
		for _, r := range p.roots {
			rel := strings.TrimPrefix(filepath.ToSlash(r), "/")
			if strings.HasPrefix(rel, namePrefix) {
				out = append(out, rel+"/")
			}
		}
		return capList(out, limit), nil
	}
	if isWorkloadIdentityPath(filePath(dir)) {
		return nil, errWorkloadIdentity
	}
	real, err := filepath.EvalSymlinks(filePath(dir))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return out, nil
		}
		return nil, fmt.Errorf("secrets: files: listing %s: %w", dir, err)
	}
	if !p.underRoot(real) && !p.isRoot(real) {
		return nil, fmt.Errorf("%w: %s", ErrOutsideRoots, dir)
	}
	if isWorkloadIdentityPath(real) {
		return nil, errWorkloadIdentity
	}
	ents, err := os.ReadDir(real)
	if err != nil {
		return nil, fmt.Errorf("secrets: files: listing %s: %w", dir, err)
	}
	for _, e := range ents {
		name := e.Name()
		// Kubernetes atomic-writer internals (..data, ..2026_10_14...).
		if strings.HasPrefix(name, "..") || !strings.HasPrefix(name, namePrefix) ||
			isWorkloadIdentityPath(dir+"/"+name) {
			continue
		}
		full := dir + "/" + name
		if e.IsDir() {
			full += "/"
		}
		out = append(out, full)
	}
	sort.Strings(out)
	return capList(out, limit), nil
}

func (p *FilesProvider) isRoot(real string) bool {
	for _, root := range p.roots {
		for _, r := range rootForms(root) {
			if real == r {
				return true
			}
		}
	}
	return false
}

func capList(s []string, limit int) []string {
	if limit > 0 && len(s) > limit {
		return s[:limit]
	}
	return s
}

// Keys reads the file and returns its JSON keys.
func (p *FilesProvider) Keys(ctx context.Context, ref Ref) ([]string, error) {
	v, err := p.Resolve(ctx, ref.secretID())
	if err != nil {
		return nil, err
	}
	return KeysOf(v.Data), nil
}

// Check reports the roots and reads the sample.
func (p *FilesProvider) Check(ctx context.Context, sample *Ref) []spec.Check {
	var checks []spec.Check
	var found []string
	for _, r := range p.roots {
		if st, err := os.Stat(r); err == nil && st.IsDir() {
			found = append(found, r)
		}
	}
	const title = "Secret file roots are mounted"
	if len(found) == 0 {
		checks = append(checks, warn(CheckReach, title, "none of "+strings.Join(p.roots, ", ")+" exists",
			"mount the secret manager's volume under one of the roots, or set PIPEROOM_SECRET_FILE_ROOTS"))
	} else {
		checks = append(checks, pass(CheckReach, title, strings.Join(found, ", ")))
	}
	c, _, _ := sampleCheck(ctx, p, sample)
	checks = append(checks, c)
	checks = append(checks, skipped(CheckTooMuch, "Piperoom cannot read outside the roots",
		"file permissions are set by the mount; Piperoom itself reads only under the roots"))
	return checks
}

func (p *FilesProvider) loop(ctx context.Context) {
	defer close(p.done)
	t := time.NewTicker(p.poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.pollOnce()
		}
	}
}

// pollOnce compares each watched file with its last stamp. A changed real
// path catches the Kubernetes atomic symlink swap; size and mtime catch an
// in-place rewrite. A changed file is forgotten until it is read again.
func (p *FilesProvider) pollOnce() {
	p.mu.Lock()
	paths := make(map[string]fileStamp, len(p.watched))
	for k, v := range p.watched {
		paths[k] = v
	}
	fn := p.onChange
	p.mu.Unlock()

	var changed []string
	for refPath, old := range paths {
		cur, err := p.stamp(refPath)
		if err != nil || cur != old {
			changed = append(changed, refPath)
		}
	}
	if len(changed) == 0 {
		return
	}
	p.mu.Lock()
	for _, c := range changed {
		if p.watched[c] == paths[c] {
			delete(p.watched, c)
		}
	}
	p.mu.Unlock()
	if fn != nil {
		for _, c := range changed {
			fn(c)
		}
	}
}

func (p *FilesProvider) stamp(refPath string) (fileStamp, error) {
	real, err := p.realPath(refPath)
	if err != nil {
		return fileStamp{}, err
	}
	st, err := os.Stat(real)
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{real: real, size: st.Size(), mtime: st.ModTime()}, nil
}
