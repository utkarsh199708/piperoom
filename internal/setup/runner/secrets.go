package runner

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// The runner's secret store (docs/design/setup-agent.md §2.4). It is one
// file under the project's .piperoom directory, created 0600 in a 0700
// directory and listed in a generated .gitignore. The file also holds the
// per-runner HMAC receipt key: no op can read the file (the ops package
// refuses .piperoom/**), and the runner never logs a value or the key.
//
// Values are never written back into an op result, a summary or a receipt:
// before a result leaves the runner every stored value is replaced literally
// by ${secret:NAME} (SanitizeResult), and OutMAC is an HMAC-SHA256 over the
// redacted result under the receipt key (ResultMAC).
//
// Invariants, each a test in secrets_test.go and secret_frames_test.go:
//   - the store file is 0600, its directory 0700, and .gitignore names it;
//   - a value under MinSecretBytes is refused, so literal redaction cannot
//     mangle ordinary text; a name must match SecretNamePattern;
//   - the receipt key survives a restart and is in no log line;
//   - no op result, summary, Data or receipt carries a stored value.

const (
	// secretsDirName is the project directory the runner owns: the store, the
	// op log and the materialised-path list live here.
	secretsDirName = ".piperoom"
	// defaultSecretsFileName is the store's name inside .piperoom.
	defaultSecretsFileName = "secrets.env"
	// materialisedFileName lists project-relative paths whose content came
	// from the store; every file op refuses them.
	materialisedFileName = "materialised"
	// ReceiptKeyName is the line in the store that holds the per-runner
	// HMAC key. It is reserved: secret_set refuses it and materialisation
	// never resolves it.
	ReceiptKeyName = "PIPEROOM_RECEIPT_KEY"
	// MinSecretBytes is the shortest value secret_set accepts: literal
	// redaction replaces the value wherever it appears, and a shorter value
	// would mangle ordinary text.
	MinSecretBytes = 6
	// MaxSecretNameBytes and MaxSecretValueBytes bound one entry so a store
	// stays a store and not a data dump.
	MaxSecretNameBytes  = 64
	MaxSecretValueBytes = 8 << 10
	// receiptKeyBytes is the HMAC-SHA256 key length.
	receiptKeyBytes = 32
)

// SecretNamePattern is the name rule of protocol.Secret: an identifier that
// is never a path segment ("/", "\" and "." are impossible), so a name can be
// a file name on the server side too.
var SecretNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Errors callers branch on with errors.Is.
var (
	// ErrSecretName: the name is empty, too long, not an identifier, or the
	// reserved receipt key.
	ErrSecretName = errors.New("setup: invalid secret name")
	// ErrSecretShort: the value is shorter than MinSecretBytes.
	ErrSecretShort = errors.New("setup: secret value is too short")
	// ErrSecretLong: the value exceeds MaxSecretValueBytes.
	ErrSecretLong = errors.New("setup: secret value is too long")
	// ErrSecretMissing: the store has no value under that name.
	ErrSecretMissing = errors.New("setup: no such secret")
	// ErrStoreUnsafe: the store path or a materialised path is a symlink, not
	// a regular file, or otherwise not something the runner may write.
	ErrStoreUnsafe = errors.New("setup: secret store is not a regular file")
	// ErrStorePath: a custom store path (PIPEROOM_SETUP_SECRETS_FILE) resolves
	// inside the project directory but outside .piperoom/, where neither the
	// generated .gitignore entry nor the ops package's deny-list protects it.
	ErrStorePath = errors.New("setup: secret store path is unprotected")
	// ErrStoreCorrupt: the store file does not parse, or carries a duplicate
	// name.
	ErrStoreCorrupt = errors.New("setup: secret store is corrupt")
)

// Store is the runner's secret store: named values plus the receipt key and
// the list of materialised paths. It is safe for concurrent use.
type Store struct {
	mu   sync.Mutex
	dir  string
	file string

	values map[string]string
	// key is the per-runner receipt key, fixed for the life of the store and
	// persisted in the store file. It is never logged and never returned.
	key          []byte
	materialised []string
}

// OpenStore opens (and creates on first use) the secret store for a project
// directory. file is PIPEROOM_SETUP_SECRETS_FILE: "" means
// <projectDir>/.piperoom/secrets.env; a relative path is taken under
// projectDir and must stay under .piperoom/. A custom path inside the project
// but outside .piperoom/ is refused (ErrStorePath): only .piperoom/ is covered
// by the generated .gitignore entry and the ops package's read/write/delete
// deny-list, so a store anywhere else in the project could be committed or
// read back by a file op. A path outside the project is the operator's own
// choice and is allowed.
//
// The store refuses to follow a symlink or a reparse point at the store file
// or its directory, generates the receipt key on first use, and ensures the
// project's .gitignore lists .piperoom/. Every error names the file or the
// name involved, never a value.
func OpenStore(projectDir, file string) (*Store, error) {
	dir, err := filepath.Abs(orDefault(strings.TrimSpace(projectDir), "."))
	if err != nil {
		return nil, fmt.Errorf("setup: resolving the project directory: %w", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("setup: project directory %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("setup: project directory %s is not a directory", dir)
	}

	if file = strings.TrimSpace(file); file == "" {
		file = filepath.Join(dir, secretsDirName, defaultSecretsFileName)
	} else {
		if !filepath.IsAbs(file) {
			// A relative path is taken under the project, so it must be local:
			// a ".." that climbs out of the project is refused outright rather
			// than joined and resolved to wherever it lands.
			if !filepath.IsLocal(file) {
				return nil, fmt.Errorf("%w: %q is relative but not local to the project", ErrStorePath, scrub(file, maxPrintedFieldLen))
			}
			file = filepath.Join(dir, file)
		}
		if err := checkStorePath(dir, file); err != nil {
			return nil, err
		}
	}
	storeDir := filepath.Dir(file)
	if err := ensureSecretDir(storeDir); err != nil {
		return nil, err
	}
	// .piperoom is the runner's own directory: keep it 0700 even when a
	// previous run or a person created it with a wider mode. A caller-chosen
	// directory (PIPEROOM_SETUP_SECRETS_FILE) is left as it is.
	if filepath.Base(storeDir) == secretsDirName {
		if err := os.Chmod(storeDir, 0o700); err != nil {
			return nil, fmt.Errorf("setup: setting the secret store directory mode: %w", err)
		}
	}
	if err := ensureRegularOrMissing(file); err != nil {
		return nil, err
	}

	s := &Store{dir: storeDir, file: file, values: make(map[string]string)}
	raw, err := os.ReadFile(file)
	switch {
	case err == nil:
		if err := s.load(raw); err != nil {
			return nil, err
		}
	case errors.Is(err, fs.ErrNotExist):
	default:
		return nil, fmt.Errorf("setup: reading the secret store: %w", err)
	}
	if s.key == nil {
		key := make([]byte, receiptKeyBytes)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("setup: generating the receipt key: %w", err)
		}
		s.key = key
		if err := s.saveLocked(); err != nil {
			return nil, err
		}
	}
	if err := s.loadMaterialised(); err != nil {
		return nil, err
	}
	if err := EnsureGitignore(dir); err != nil {
		return nil, err
	}
	return s, nil
}

// checkStorePath refuses a custom store file that resolves inside the project
// directory but not under .piperoom/. Such a file escapes both protections the
// store relies on — the generated .gitignore entry names only .piperoom/, and
// the ops package's deny-list refuses only .piperoom/** — so a commit or a
// file op could expose it. A file outside the project is allowed: where to
// keep an external store is the operator's choice.
func checkStorePath(projectDir, file string) error {
	rel, err := filepath.Rel(projectDir, file)
	if err != nil || !filepath.IsLocal(rel) {
		return nil // outside the project: the operator's choice
	}
	first := rel
	if i := strings.IndexByte(filepath.ToSlash(rel), '/'); i >= 0 {
		first = filepath.ToSlash(rel)[:i]
	} else {
		return fmt.Errorf("%w: %s is at the project root, outside %s/", ErrStorePath, file, secretsDirName)
	}
	if first != secretsDirName {
		return fmt.Errorf("%w: %s is inside the project but outside %s/", ErrStorePath, file, secretsDirName)
	}
	return nil
}

// ensureSecretDir creates the store's directory 0700 or checks an existing
// one. A symlink, junction or non-directory is refused: the runner writes the
// store through an ordinary directory, never through a link.
func ensureSecretDir(dir string) error {
	fi, err := os.Lstat(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("setup: creating the secret store directory: %w", err)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return fmt.Errorf("setup: setting the secret store directory mode: %w", err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("setup: checking the secret store directory: %w", err)
	}
	if fi.Mode()&(fs.ModeSymlink|fs.ModeIrregular) != 0 {
		return fmt.Errorf("%w: %s is a symlink or a reparse point", ErrStoreUnsafe, dir)
	}
	if !fi.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrStoreUnsafe, dir)
	}
	return nil
}

// ensureRegularOrMissing refuses a store file that exists but is a symlink,
// a directory, a device or any other non-regular file.
func ensureRegularOrMissing(file string) error {
	fi, err := os.Lstat(file)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("setup: checking the secret store: %w", err)
	}
	if fi.Mode()&fs.ModeSymlink != 0 || !fi.Mode().IsRegular() {
		return fmt.Errorf("%w: %s", ErrStoreUnsafe, file)
	}
	return nil
}

// EnsureGitignore appends .piperoom/ to the project's .gitignore, creating it
// when missing. It never follows a symlinked .gitignore (the entry would be
// written into a file outside the project) and never rewrites the file. The
// standalone server (internal/setup/standalone) creates the same directory
// for the registered credentials, so it makes the same entry.
func EnsureGitignore(projectDir string) error {
	p := filepath.Join(projectDir, ".gitignore")
	if fi, err := os.Lstat(p); err == nil {
		if fi.Mode()&fs.ModeSymlink != 0 || !fi.Mode().IsRegular() {
			return nil // do not follow it; the person keeps their own file
		}
	}
	raw, err := os.ReadFile(p)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("setup: reading .gitignore: %w", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		switch strings.TrimSpace(line) {
		case ".piperoom", ".piperoom/", "/.piperoom", "/.piperoom/":
			return nil
		}
	}
	entry := ".piperoom/\n"
	if len(raw) > 0 && !bytes.HasSuffix(raw, []byte("\n")) {
		entry = "\n" + entry
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("setup: writing .gitignore: %w", err)
	}
	if _, err := f.WriteString(entry); err != nil {
		_ = f.Close()
		return fmt.Errorf("setup: writing .gitignore: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("setup: writing .gitignore: %w", err)
	}
	return nil
}

// load parses the store file. It refuses a duplicate name (an ambiguous store
// would make redaction and materialisation pick one of two values) and a
// stored user value shorter than MinSecretBytes.
func (s *Store) load(raw []byte) error {
	for i, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSuffix(line, "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		name, value, ok := strings.Cut(trimmed, "=")
		if !ok {
			return fmt.Errorf("%w: line %d has no =", ErrStoreCorrupt, i+1)
		}
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if name == ReceiptKeyName {
			if s.key != nil {
				return fmt.Errorf("%w: duplicate %s", ErrStoreCorrupt, ReceiptKeyName)
			}
			key, err := hex.DecodeString(strings.Trim(value, `"`))
			if err != nil || len(key) != receiptKeyBytes {
				return fmt.Errorf("%w: line %d: the receipt key is not %d hex-encoded bytes", ErrStoreCorrupt, i+1, receiptKeyBytes)
			}
			s.key = key
			continue
		}
		if err := ValidateSecretName(name); err != nil {
			return fmt.Errorf("%w: line %d: %v", ErrStoreCorrupt, i+1, err)
		}
		decoded, err := unquoteSecretValue(value)
		if err != nil {
			return fmt.Errorf("%w: line %d: %v", ErrStoreCorrupt, i+1, err)
		}
		if _, dup := s.values[name]; dup {
			return fmt.Errorf("%w: duplicate secret %q", ErrStoreCorrupt, scrub(name, MaxSecretNameBytes))
		}
		if len(decoded) < MinSecretBytes {
			return fmt.Errorf("%w: %q has %d bytes", ErrSecretShort, scrub(name, MaxSecretNameBytes), len(decoded))
		}
		s.values[name] = decoded
	}
	return nil
}

// unquoteSecretValue reads a value written by saveLocked (a Go-quoted string)
// and accepts an unquoted value from a hand-edited store.
func unquoteSecretValue(v string) (string, error) {
	if !strings.HasPrefix(v, `"`) {
		return v, nil
	}
	out, err := strconv.Unquote(v)
	if err != nil {
		return "", fmt.Errorf("quoted value: %w", err)
	}
	return out, nil
}

// saveLocked rewrites the store atomically: a temporary file in the same
// directory, mode 0600, fsynced and renamed over the old one. s.mu is held by
// the caller (or the store is not shared yet, as in OpenStore).
func (s *Store) saveLocked() error {
	names := make([]string, 0, len(s.values))
	for name := range s.values {
		names = append(names, name)
	}
	sort.Strings(names)

	var b bytes.Buffer
	b.WriteString("# Piperoom runner secret store. Mode 0600; never commit. Referenced as ${secret:NAME}.\n")
	for _, name := range names {
		fmt.Fprintf(&b, "%s=%s\n", name, strconv.Quote(s.values[name]))
	}
	fmt.Fprintf(&b, "%s=%s\n", ReceiptKeyName, hex.EncodeToString(s.key))
	return writeFileAtomic(s.file, b.Bytes(), 0o600)
}

// writeFileAtomic writes data to a temporary file next to file and renames it
// into place. O_EXCL makes a name collision an error, so the writer never
// writes through a planted file.
func writeFileAtomic(file string, data []byte, mode fs.FileMode) error {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Errorf("setup: generating a temporary name: %w", err)
	}
	tmp := filepath.Join(filepath.Dir(file), "."+filepath.Base(file)+".piperoom-tmp-"+hex.EncodeToString(suffix[:]))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("setup: creating the temporary file: %w", err)
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}
	if _, err := f.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("setup: writing the temporary file: %w", err)
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("setup: syncing the temporary file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("setup: closing the temporary file: %w", err)
	}
	if err := os.Chmod(tmp, mode); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("setup: setting the file mode: %w", err)
	}
	if err := os.Rename(tmp, file); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("setup: replacing the file: %w", err)
	}
	return nil
}

// loadMaterialised reads the materialised-path list. Each line is one
// project-relative slash path.
func (s *Store) loadMaterialised() error {
	p := s.materialisedPath()
	fi, err := os.Lstat(p)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("setup: checking the materialised-path list: %w", err)
	}
	if fi.Mode()&fs.ModeSymlink != 0 || !fi.Mode().IsRegular() {
		return fmt.Errorf("%w: %s", ErrStoreUnsafe, p)
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("setup: reading the materialised-path list: %w", err)
	}
	for i, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		clean, err := CleanMaterialisedPath(line)
		if err != nil {
			return fmt.Errorf("%w: line %d: %v", ErrStoreCorrupt, i+1, err)
		}
		if !containsString(s.materialised, clean) {
			s.materialised = append(s.materialised, clean)
		}
	}
	sort.Strings(s.materialised)
	return nil
}

// materialisedPath is the file that persists the materialised-path list.
func (s *Store) materialisedPath() string {
	return filepath.Join(s.dir, materialisedFileName)
}

// Set stores (or replaces) one named value. A replacement is a normal upsert:
// rotating a generated password must not need a delete. The file is rewritten
// atomically before Set returns; a write failure leaves the old value in
// place.
func (s *Store) Set(name, value string) error {
	if err := ValidateSecretName(name); err != nil {
		return err
	}
	if err := ValidateSecretValue(value); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.values[name]
	s.values[name] = value
	if err := s.saveLocked(); err != nil {
		if had {
			s.values[name] = old
		} else {
			delete(s.values, name)
		}
		return err
	}
	return nil
}

// Store stores one named value and satisfies the SecretStore interface of the
// setup source writers (internal/clients/oracle, internal/clients/sqlserver):
// CreateReadRole generates a password, stores it and returns only its name.
func (s *Store) Store(ctx context.Context, name, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Set(name, value)
}

// Get returns the value stored under name.
func (s *Store) Get(name string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[name]
	return v, ok
}

// Has reports whether name has a value.
func (s *Store) Has(name string) bool {
	_, ok := s.Get(name)
	return ok
}

// Names lists the stored user secret names, sorted. The receipt key is not a
// secret a person may use: it is never listed.
func (s *Store) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.values))
	for name := range s.values {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// ValidateSecretName checks protocol.Secret's name rule and refuses the
// reserved receipt key. The error quotes the name (never a value).
func ValidateSecretName(name string) error {
	if name == "" || len(name) > MaxSecretNameBytes || !SecretNamePattern.MatchString(name) {
		return fmt.Errorf("%w: %q: want ^[a-zA-Z_][a-zA-Z0-9_]*$ of at most %d bytes", ErrSecretName, scrub(name, MaxSecretNameBytes), MaxSecretNameBytes)
	}
	if name == ReceiptKeyName {
		return fmt.Errorf("%w: %q is reserved for the runner's receipt key", ErrSecretName, name)
	}
	return nil
}

// ValidateSecretValue checks the value bounds. The error never quotes the
// value.
func ValidateSecretValue(value string) error {
	if len(value) < MinSecretBytes {
		return fmt.Errorf("%w: at least %d bytes, so literal redaction cannot mangle ordinary text", ErrSecretShort, MinSecretBytes)
	}
	if len(value) > MaxSecretValueBytes {
		return fmt.Errorf("%w: at most %d bytes", ErrSecretLong, MaxSecretValueBytes)
	}
	return nil
}

// Materialise resolves every ${secret:NAME} reference in text to its stored
// value. The result is for memory or a child process environment only: it is
// never written into a compose file or a summary. A missing or reserved name
// fails the whole call, so a half-materialised string is never used.
func (s *Store) Materialise(text string) (string, error) {
	if !strings.Contains(text, "${secret:") {
		return text, nil
	}
	var missing string
	out := secretRefPattern.ReplaceAllStringFunc(text, func(ref string) string {
		name := ref[len("${secret:") : len(ref)-1]
		if name == ReceiptKeyName {
			if missing == "" {
				missing = name
			}
			return ref
		}
		v, ok := s.Get(name)
		if !ok {
			if missing == "" {
				missing = name
			}
			return ref
		}
		return v
	})
	if missing != "" {
		return "", fmt.Errorf("%w: %q is not in the store", ErrSecretMissing, scrub(missing, MaxSecretNameBytes))
	}
	return out, nil
}

// secretRefPattern is one ${secret:NAME} reference with the protocol name
// rule. The reference is resolved in one pass; a stored value that itself
// contains a reference is not expanded again.
var secretRefPattern = regexp.MustCompile(`\$\{secret:[a-zA-Z_][a-zA-Z0-9_]*\}`)

// MaterialiseEnv resolves ${secret:NAME} in the values of a child process
// environment ("KEY=value" entries). The returned slice is the only place
// materialised values may appear; the stored file, summaries and Data keep
// the reference.
func (s *Store) MaterialiseEnv(env []string) ([]string, error) {
	out := make([]string, len(env))
	for i, kv := range env {
		key, value, ok := strings.Cut(kv, "=")
		if !ok {
			out[i] = kv
			continue
		}
		resolved, err := s.Materialise(value)
		if err != nil {
			return nil, fmt.Errorf("setup: materialising %s: %w", scrub(key, MaxSecretNameBytes), err)
		}
		out[i] = key + "=" + resolved
	}
	return out, nil
}

// RegisterMaterialised records a project-relative path whose content came
// from the store. Every file op refuses it from then on, including after a
// runner restart (the list is persisted). The path is never a value.
func (s *Store) RegisterMaterialised(p string) error {
	clean, err := CleanMaterialisedPath(p)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if containsString(s.materialised, clean) {
		return nil
	}
	next := append(append([]string(nil), s.materialised...), clean)
	sort.Strings(next)
	var b bytes.Buffer
	b.WriteString("# Paths the runner materialised from the secret store; no file op may read them.\n")
	for _, m := range next {
		b.WriteString(m + "\n")
	}
	if err := writeFileAtomic(s.materialisedPath(), b.Bytes(), 0o600); err != nil {
		return err
	}
	s.materialised = next
	return nil
}

// MaterialisedPaths lists the registered materialised paths, sorted.
func (s *Store) MaterialisedPaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.materialised...)
}

// IsMaterialised reports whether p is (or is under) a materialised path.
func (s *Store) IsMaterialised(p string) bool {
	clean, err := CleanMaterialisedPath(p)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return underPath(clean, s.materialised)
}

// CleanMaterialisedPath normalises a project-relative materialised path and
// refuses anything that is not one: absolute, outside the project, a denied
// component, or empty.
func CleanMaterialisedPath(p string) (string, error) {
	clean := strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	if clean == "" || clean == "." || strings.HasPrefix(clean, "/") {
		return "", fmt.Errorf("%w: materialised path %q is not project-relative", ErrStoreUnsafe, scrub(p, maxPrintedFieldLen))
	}
	if !filepath.IsLocal(filepath.FromSlash(clean)) {
		return "", fmt.Errorf("%w: materialised path %q is outside the project", ErrStoreUnsafe, scrub(p, maxPrintedFieldLen))
	}
	for _, part := range strings.Split(clean, "/") {
		switch {
		case part == "" || part == "." || part == "..":
			return "", fmt.Errorf("%w: materialised path %q is not clean", ErrStoreUnsafe, scrub(p, maxPrintedFieldLen))
		case foldEqual(part, ".piperoom"), foldEqual(part, ".git"):
			return "", fmt.Errorf("%w: materialised path %q names a denied directory", ErrStoreUnsafe, scrub(p, maxPrintedFieldLen))
		}
	}
	return clean, nil
}

// foldEqual compares two path components case-insensitively, by simple fold
// and by upper case: strings.EqualFold alone misses the dotless ı (U+0131),
// whose upper case is "I", so "confıg" opens "config" on a case-folding file
// system while EqualFold says they differ.
func foldEqual(a, b string) bool {
	return strings.EqualFold(a, b) || strings.ToUpper(a) == strings.ToUpper(b)
}

// underPath reports whether clean is one of dirs or below one of them,
// comparing components case-insensitively (Windows file systems fold case).
func underPath(clean string, dirs []string) bool {
	parts := strings.Split(clean, "/")
	for _, dir := range dirs {
		dp := strings.Split(strings.Trim(dir, "/"), "/")
		if len(dp) == 0 || len(dp) > len(parts) {
			continue
		}
		ok := true
		for i := range dp {
			if !foldEqual(dp[i], parts[i]) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// redaction is one stored value and the placeholder that replaces it.
type redaction struct {
	name  string
	value string
}

// redactions returns every stored value (the receipt key included), longest
// first so a value that contains another value is replaced whole.
func (s *Store) redactions() []redaction {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]redaction, 0, len(s.values)+1)
	for name, value := range s.values {
		out = append(out, redaction{name: name, value: value})
	}
	out = append(out, redaction{name: ReceiptKeyName, value: hex.EncodeToString(s.key)})
	sort.Slice(out, func(i, j int) bool {
		if len(out[i].value) != len(out[j].value) {
			return len(out[i].value) > len(out[j].value)
		}
		return out[i].name < out[j].name
	})
	return out
}

// redactedRefPattern is exactly the span Redact may copy untouched: one
// well-formed ${secret:NAME} reference under ValidateSecretName's rule (an
// identifier of at most MaxSecretNameBytes bytes), anchored at the start. A
// looser skip — anything from "${secret:" to the first "}" — would let a raw
// value wrapped as "${secret:<value>}" pass through unredacted.
var redactedRefPattern = regexp.MustCompile(`^\$\{secret:[a-zA-Z_][a-zA-Z0-9_]{0,63}\}`)

// containsStoredValue reports whether any stored value appears inside span.
// A reference whose text carries a stored value is not a placeholder this
// store emitted: it must be redacted, never copied through.
func containsStoredValue(span string, rs []redaction) bool {
	for _, r := range rs {
		if strings.Contains(span, r.value) {
			return true
		}
	}
	return false
}

// Redact replaces every stored value in text literally by ${secret:NAME}, in
// one pass. An existing well-formed ${secret:NAME} reference that carries no
// stored value is copied untouched, so redaction is idempotent; anything else
// that merely starts with "${secret:" is ordinary text and is matched byte by
// byte, so wrapping a raw value in a fake reference does not smuggle it out.
func (s *Store) Redact(text string) string {
	if text == "" {
		return text
	}
	rs := s.redactions()
	if len(rs) == 0 {
		return text
	}
	var b strings.Builder
	b.Grow(len(text))
	for i := 0; i < len(text); {
		if strings.HasPrefix(text[i:], "${secret:") {
			if ref := redactedRefPattern.FindString(text[i:]); ref != "" && !containsStoredValue(ref, rs) {
				b.WriteString(ref)
				i += len(ref)
				continue
			}
		}
		matched := false
		for _, r := range rs {
			if strings.HasPrefix(text[i:], r.value) {
				b.WriteString("${secret:" + r.name + "}")
				i += len(r.value)
				matched = true
				break
			}
		}
		if !matched {
			b.WriteByte(text[i])
			i++
		}
	}
	return b.String()
}

// redactJSON walks every string (keys included) of a JSON value and redacts
// it. Data that does not parse is withheld rather than passed through: an
// unredactable frame must not carry a secret.
func (s *Store) redactJSON(raw []byte) (json.RawMessage, bool) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return raw, false
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return json.RawMessage(`{"truncated":true,"note":"data withheld: it was not valid JSON and could not be redacted"}`), true
	}
	out, err := json.Marshal(s.redactValue(v))
	if err != nil {
		return json.RawMessage(`{"truncated":true,"note":"data withheld: it could not be re-encoded after redaction"}`), true
	}
	return out, false
}

func (s *Store) redactValue(v any) any {
	switch x := v.(type) {
	case string:
		return s.Redact(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = s.redactValue(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			rk := s.Redact(k)
			if _, dup := out[rk]; dup {
				continue
			}
			out[rk] = s.redactValue(e)
		}
		return out
	default:
		return v
	}
}

// SanitizeResult is the literal-redaction pass every OpResult goes through
// before it leaves the runner: Summary, Data and the receipt's Argv and
// Target, then the caps. OutMAC is cleared; the runner sets it after this
// pass (ResultMAC), over exactly the bytes the server receives.
func (s *Store) SanitizeResult(res protocol.OpResult) protocol.OpResult {
	summary, cut := tools.Truncate(s.Redact(res.Summary))
	res.Summary = summary
	res.Truncated = res.Truncated || cut

	if len(res.Data) > 0 {
		data, withheld := s.redactJSON(res.Data)
		if len(data) > protocol.MaxDataBytes {
			data = json.RawMessage(`{"truncated":true,"note":"data exceeds the protocol cap after redaction"}`)
			withheld = true
		}
		res.Data = data
		res.Truncated = res.Truncated || withheld
	}

	if len(res.Receipt.Argv) > 0 {
		argv := make([]string, len(res.Receipt.Argv))
		for i, a := range res.Receipt.Argv {
			argv[i] = s.Redact(a)
		}
		res.Receipt.Argv = argv
	}
	res.Receipt.Target = s.Redact(res.Receipt.Target)
	res.Receipt.OutMAC = ""
	return res
}

// ResultMAC is the receipt's OutMAC: HMAC-SHA256 under the per-runner key
// over the result's OK flag, redacted Summary and redacted Data. A verifier
// holding the frame and the runner's store recomputes it (VerifyResult); a
// short output is therefore not a guessing oracle for someone who only holds
// the frame.
func (s *Store) ResultMAC(ok bool, summary string, data []byte) string {
	s.mu.Lock()
	key := append([]byte(nil), s.key...)
	s.mu.Unlock()

	m := hmac.New(sha256.New, key)
	m.Write([]byte("piperoom.setup.receipt.v1\nok="))
	if ok {
		m.Write([]byte("1"))
	} else {
		m.Write([]byte("0"))
	}
	fmt.Fprintf(m, "\nsummary=%d:", len(summary))
	m.Write([]byte(summary))
	fmt.Fprintf(m, "\ndata=%d:", len(data))
	m.Write(data)
	return hex.EncodeToString(m.Sum(nil))
}

// VerifyResult recomputes the result's OutMAC under this store's key and
// compares it in constant time. A tampered Summary, Data or OutMAC fails.
func (s *Store) VerifyResult(res protocol.OpResult) bool {
	want := res.Receipt.OutMAC
	if want == "" {
		return false
	}
	got := s.ResultMAC(res.OK, res.Summary, res.Data)
	return hmac.Equal([]byte(got), []byte(want))
}

// secretStore opens the runner's store on first use: the first op or secret
// frame creates .piperoom/secrets.env (0600) and the .gitignore entry; a
// runner that only pairs creates nothing.
func (r *Runner) secretStore() (*Store, error) {
	r.storeMu.Lock()
	defer r.storeMu.Unlock()
	if r.store != nil {
		return r.store, nil
	}
	s, err := OpenStore(r.projectDir, r.secretsF)
	if err != nil {
		return nil, fmt.Errorf("setup: opening the secret store: %w", err)
	}
	r.store = s
	return s, nil
}
