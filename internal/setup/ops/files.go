package ops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/setup/protocol"
)

const (
	// maxReadBytes caps what file_read reads from disk; the emitted content is
	// capped further so Data stays under protocol.MaxDataBytes.
	maxReadBytes = 256 << 10
	// maxReadContent is what file_read puts in Data: small enough that the
	// JSON envelope stays under 64 KiB.
	maxReadContent = 56 << 10
	// maxWriteBytes caps file_write content (design §2.5: 256 KiB).
	maxWriteBytes = 256 << 10
	// maxDirEntries caps dir_list.
	maxDirEntries = 500
)

// fileReadOp reads one UTF-8 text file under the project dir.
type fileReadOp struct{}

type fileReadParams struct {
	Path string `json:"path"`
}

func (fileReadOp) Name() string   { return "file_read" }
func (fileReadOp) Target() string { return "" }
func (fileReadOp) Tier() string   { return protocol.TierT0 }
func (fileReadOp) Layer() string  { return "environment" }
func (fileReadOp) Describe() string {
	return "read a UTF-8 text file under the project directory"
}

func (fileReadOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required":   []string{"path"},
		"properties": map[string]any{"path": map[string]any{"type": "string", "minLength": 1}},
	}
}

func (fileReadOp) Validate(params json.RawMessage, env Env) error {
	var p fileReadParams
	if err := decodeParams(params, &p); err != nil {
		return err
	}
	if p.Path == "" {
		return fmt.Errorf("%w: path is required", ErrParams)
	}
	if why := lexicalRefusal(p.Path); why != "" {
		return fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
	}
	if err := secretReadPath(p.Path); err != nil {
		return err
	}
	return nil
}

func (fileReadOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	var p fileReadParams
	if err := decodeParams(params, &p); err != nil {
		return Result{}, err
	}
	if why := lexicalRefusal(p.Path); why != "" {
		return Result{}, fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
	}
	if err := secretReadPath(p.Path); err != nil {
		return Result{}, err
	}
	clean, denied, err := env.Confine.prepare(p.Path)
	if err != nil {
		return Result{}, err
	}
	f, err := env.Confine.root.Open(clean)
	if err != nil {
		return Result{}, fmt.Errorf("ops: opening %s: %w", clip(p.Path), err)
	}
	defer f.Close()
	if err := env.Confine.verifyHandle(f, denied); err != nil {
		return Result{}, fmt.Errorf("ops: reading %s: %w", clip(p.Path), err)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxReadBytes+1))
	if err != nil {
		return Result{}, fmt.Errorf("ops: reading %s: %w", clip(p.Path), err)
	}
	truncated := len(data) > maxReadBytes
	if truncated {
		data = data[:maxReadBytes]
	}
	if !utf8.Valid(data) {
		return Result{}, fmt.Errorf("%w: %s is not UTF-8 text", ErrNotText, clip(p.Path))
	}
	content := string(data)
	if len(content) > maxReadContent {
		content = content[:maxReadContent]
		truncated = true
	}
	// A config file can carry credentials (database.password=..., tokens), and
	// §2.4 of this package's contract says no op reads a secret back; the path
	// deny-list alone only catches secret-named files. Redact the content like
	// http_get redacts its body, before it reaches the server or the model.
	content, _ = redact.Text(content)
	summary := fmt.Sprintf("read %s (%d bytes)", p.Path, len(data))
	if truncated {
		summary += ", truncated"
	}
	return Result{
		Summary:   summary,
		Data:      map[string]any{"path": clean, "content": content, "bytes": len(data), "truncated": truncated},
		Truncated: truncated,
		Receipt:   events.SetupReceipt{Kind: "file", Target: clean, At: env.Now()},
	}, nil
}

// dirListOp lists one directory under the project dir.
type dirListOp struct{}

type dirListParams struct {
	Path string `json:"path,omitempty"`
}

type dirEntry struct {
	Name string `json:"name"`
	Kind string `json:"kind"` // "dir" | "file"
	Size int64  `json:"size,omitempty"`
}

func (dirListOp) Name() string     { return "dir_list" }
func (dirListOp) Target() string   { return "" }
func (dirListOp) Tier() string     { return protocol.TierT0 }
func (dirListOp) Layer() string    { return "environment" }
func (dirListOp) Describe() string { return "list one directory under the project directory" }

func (dirListOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"path": map[string]any{"type": "string"}},
	}
}

func (dirListOp) Validate(params json.RawMessage, env Env) error {
	var p dirListParams
	if err := decodeParams(params, &p); err != nil {
		return err
	}
	if p.Path != "" {
		if why := lexicalRefusal(p.Path); why != "" {
			return fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
		}
	}
	return nil
}

func (dirListOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	var p dirListParams
	if err := decodeParams(params, &p); err != nil {
		return Result{}, err
	}
	if p.Path == "" {
		p.Path = "."
	}
	clean, denied, err := env.Confine.prepare(p.Path)
	if err != nil {
		return Result{}, err
	}
	f, err := env.Confine.root.Open(clean)
	if err != nil {
		return Result{}, fmt.Errorf("ops: opening %s: %w", clip(p.Path), err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return Result{}, fmt.Errorf("ops: stat %s: %w", clip(p.Path), err)
	}
	if !fi.IsDir() {
		return Result{}, fmt.Errorf("%w: %s is not a directory", ErrParams, clip(p.Path))
	}
	entries, err := f.ReadDir(-1)
	if err != nil {
		return Result{}, fmt.Errorf("ops: listing %s: %w", clip(p.Path), err)
	}
	out := make([]dirEntry, 0, len(entries))
	for _, e := range entries {
		info, ierr := e.Info()
		if ierr != nil {
			continue // vanished between readdir and stat
		}
		// Skip non-regular files and anything that aliases .git/.piperoom.
		switch {
		case info.Mode()&fs.ModeSymlink != 0, info.Mode()&fs.ModeIrregular != 0:
			continue
		case !info.IsDir() && !info.Mode().IsRegular():
			continue
		case deniedDir(e.Name()):
			continue
		}
		aliased := false
		for _, d := range denied {
			if os.SameFile(info, d) {
				aliased = true
				break
			}
		}
		if aliased {
			continue
		}
		kind := "file"
		if info.IsDir() {
			kind = "dir"
		}
		out = append(out, dirEntry{Name: e.Name(), Kind: kind, Size: info.Size()})
	}
	truncated := len(out) > maxDirEntries
	if truncated {
		out = out[:maxDirEntries]
	}
	summary := fmt.Sprintf("%d entries under %s", len(out), clean)
	if truncated {
		summary += ", truncated"
	}
	return Result{
		Summary:   summary,
		Data:      map[string]any{"path": clean, "entries": out, "truncated": truncated},
		Truncated: truncated,
		Receipt:   events.SetupReceipt{Kind: "file", Target: clean, At: env.Now()},
	}, nil
}

// fileWriteOp writes one UTF-8 text file that nothing executes or loads.
type fileWriteOp struct{}

type fileWriteParams struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func (fileWriteOp) Name() string   { return "file_write" }
func (fileWriteOp) Target() string { return "" }
func (fileWriteOp) Tier() string   { return protocol.TierT1 }
func (fileWriteOp) Layer() string  { return "environment" }
func (fileWriteOp) Describe() string {
	return "write a non-executed UTF-8 text file under the project directory"
}

func (fileWriteOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"path", "content"},
		"properties": map[string]any{
			"path":    map[string]any{"type": "string", "minLength": 1},
			"content": map[string]any{"type": "string", "maxLength": maxWriteBytes},
		},
	}
}

func (fileWriteOp) Validate(params json.RawMessage, env Env) error {
	p, err := decodeWrite(params)
	if err != nil {
		return err
	}
	if why := lexicalRefusal(p.Path); why != "" {
		return fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
	}
	if err := writablePath(p.Path, env.MountDirs); err != nil {
		return err
	}
	if err := checkWriteContent(p.Content); err != nil {
		return err
	}
	return nil
}

func (fileWriteOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := decodeWrite(params)
	if err != nil {
		return Result{}, err
	}
	if why := lexicalRefusal(p.Path); why != "" {
		return Result{}, fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
	}
	if err := writablePath(p.Path, env.MountDirs); err != nil {
		return Result{}, err
	}
	if err := checkWriteContent(p.Content); err != nil {
		return Result{}, err
	}
	clean, denied, err := env.Confine.prepare(p.Path)
	if err != nil {
		return Result{}, err
	}

	// A destination that exists must be a regular file: os.Root alone would
	// open a device file, FIFO or socket.
	if fi, lerr := env.Confine.root.Lstat(clean); lerr == nil {
		if !fi.Mode().IsRegular() {
			return Result{}, fmt.Errorf("%w: %s exists and is not a regular file", ErrNotRegular, clip(p.Path))
		}
	} else if !errors.Is(lerr, fs.ErrNotExist) {
		return Result{}, fmt.Errorf("ops: stat %s: %w", clip(p.Path), lerr)
	}

	// Remember which directories this write creates, deepest last, so a
	// refusal below can take them back out instead of leaving a model-chosen
	// directory tree behind.
	var createdDirs []string
	dir := path.Dir(clean)
	if dir != "." {
		for d := dir; d != "." && d != "/"; d = path.Dir(d) {
			if _, lerr := env.Confine.root.Lstat(d); errors.Is(lerr, fs.ErrNotExist) {
				createdDirs = append([]string{d}, createdDirs...)
			}
		}
		if err := env.Confine.root.MkdirAll(dir, 0o755); err != nil {
			return Result{}, fmt.Errorf("ops: creating %s: %w", clip(p.Path), err)
		}
	}
	removeCreated := func() {
		for i := len(createdDirs) - 1; i >= 0; i-- {
			_ = env.Confine.root.Remove(createdDirs[i])
		}
	}
	tmp, err := tempName(clean)
	if err != nil {
		return Result{}, err
	}
	// O_CREATE|O_EXCL means a planted hard link at the temporary name is not
	// written through; the rename then replaces the destination, so a hard
	// link at the destination is replaced, never written through.
	f, err := env.Confine.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return Result{}, fmt.Errorf("ops: creating a temp file for %s: %w", clip(p.Path), err)
	}
	_, werr := io.WriteString(f, p.Content)
	if werr == nil {
		werr = f.Sync()
	}
	cerr := f.Close()
	if werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = env.Confine.root.Rename(tmp, clean)
	}
	if werr != nil {
		_ = env.Confine.root.Remove(tmp)
		removeCreated()
		return Result{}, fmt.Errorf("ops: writing %s: %w", clip(p.Path), werr)
	}
	// Re-run the walk after the rename: a component swapped to a symlink
	// between prepare and the writes (a race only a concurrent local process
	// can stage; these ops cannot create symlinks) is caught here, and the
	// written file is removed rather than left behind the alias. The window
	// is narrowed, not closed — closing it needs an fd-resolved path check,
	// which the standard library does not expose portably.
	if why := walkRefusal(clean, env.Confine.root.Lstat, denied, os.SameFile); why != "" {
		_ = env.Confine.root.Remove(clean)
		removeCreated()
		return Result{}, fmt.Errorf("%w: %s: %s (path changed during the write; the file was removed)", ErrPath, why, clip(p.Path))
	}
	return Result{
		Summary: fmt.Sprintf("wrote %s (%d bytes)", p.Path, len(p.Content)),
		Data:    map[string]any{"path": clean, "bytes": len(p.Content)},
		Receipt: events.SetupReceipt{Kind: "file", Target: clean, At: env.Now()},
	}, nil
}

// fileDeleteOp deletes one regular file under the project dir.
type fileDeleteOp struct{}

type fileDeleteParams struct {
	Path string `json:"path"`
}

func (fileDeleteOp) Name() string     { return "file_delete" }
func (fileDeleteOp) Target() string   { return "" }
func (fileDeleteOp) Tier() string     { return protocol.TierT1 }
func (fileDeleteOp) Layer() string    { return "environment" }
func (fileDeleteOp) Describe() string { return "delete one regular file under the project directory" }

func (fileDeleteOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required":   []string{"path"},
		"properties": map[string]any{"path": map[string]any{"type": "string", "minLength": 1}},
	}
}

func (fileDeleteOp) Validate(params json.RawMessage, env Env) error {
	var p fileDeleteParams
	if err := decodeParams(params, &p); err != nil {
		return err
	}
	if p.Path == "" {
		return fmt.Errorf("%w: path is required", ErrParams)
	}
	if why := lexicalRefusal(p.Path); why != "" {
		return fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
	}
	return deletablePath(p.Path, env.MountDirs)
}

func (fileDeleteOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	var p fileDeleteParams
	if err := decodeParams(params, &p); err != nil {
		return Result{}, err
	}
	if why := lexicalRefusal(p.Path); why != "" {
		return Result{}, fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
	}
	if err := deletablePath(p.Path, env.MountDirs); err != nil {
		return Result{}, err
	}
	clean, _, err := env.Confine.prepare(p.Path)
	if err != nil {
		return Result{}, err
	}
	fi, err := env.Confine.root.Lstat(clean)
	if err != nil {
		return Result{}, fmt.Errorf("ops: stat %s: %w", clip(p.Path), err)
	}
	if !fi.Mode().IsRegular() {
		return Result{}, fmt.Errorf("%w: %s is not a regular file", ErrNotRegular, clip(p.Path))
	}
	if err := env.Confine.root.Remove(clean); err != nil {
		return Result{}, fmt.Errorf("ops: deleting %s: %w", clip(p.Path), err)
	}
	return Result{
		Summary: fmt.Sprintf("deleted %s", p.Path),
		Data:    map[string]any{"path": clean},
		Receipt: events.SetupReceipt{Kind: "file", Target: clean, At: env.Now()},
	}, nil
}

// secretReadPath refuses a read of the secret store's default file, the
// materialised .env files (design §2.4: no op can read a secret back), and the
// well-known credential files a project directory can hold whose formats the
// redact pass does not parse (.netrc's "password <token>", .pgpass's
// colon-separated lines, private key material). A materialised properties file
// registered by a template driver is refused by the Confiner's path list in
// the row that adds it.
func secretReadPath(p string) error {
	base := strings.ToLower(path.Base(strings.ReplaceAll(p, "\\", "/")))
	switch {
	case strings.HasPrefix(base, ".env"),
		base == "secrets.env", base == "secrets.json",
		base == ".netrc", base == "_netrc", base == ".pgpass", base == "pgpass.conf",
		base == ".my.cnf", base == ".htpasswd", base == "credentials",
		strings.HasPrefix(base, "id_rsa"), strings.HasPrefix(base, "id_ed25519"),
		strings.HasPrefix(base, "id_ecdsa"), strings.HasPrefix(base, "id_dsa"),
		strings.HasSuffix(base, ".ppk"), strings.HasSuffix(base, ".pem"),
		strings.HasSuffix(base, ".key"), strings.HasSuffix(base, ".p12"),
		strings.HasSuffix(base, ".pfx"), strings.HasSuffix(base, ".jks"),
		strings.HasSuffix(base, ".keystore"), strings.HasSuffix(base, ".truststore"),
		strings.HasSuffix(base, ".kdbx"):
		return fmt.Errorf("%w: %s names a secret file", ErrSecretPath, clip(p))
	}
	return nil
}

// deletablePath refuses the paths file_delete must not remove: everything
// file_write may not touch (deleting a compose file or a mounted plugin jar
// breaks the stack the same way overwriting it does) and the secret files
// (deleting the secret store destroys the room's credentials).
func deletablePath(p string, mounts []string) error {
	if err := writablePath(p, mounts); err != nil {
		return err
	}
	return secretReadPath(p)
}

// decodeWrite decodes and bounds file_write params.
func decodeWrite(params json.RawMessage) (fileWriteParams, error) {
	var p fileWriteParams
	if err := decodeParams(params, &p); err != nil {
		return fileWriteParams{}, err
	}
	if p.Path == "" {
		return fileWriteParams{}, fmt.Errorf("%w: path is required", ErrParams)
	}
	if len(p.Content) > maxWriteBytes {
		return fileWriteParams{}, fmt.Errorf("%w: content is %d bytes, the cap is %d", ErrTooLarge, len(p.Content), maxWriteBytes)
	}
	return p, nil
}

// checkWriteContent refuses binary content and content that carries a
// plaintext secret: file_write is for files nothing executes or loads, and a
// secret belongs in the runner's secret store, referenced as ${secret:name}.
func checkWriteContent(s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("%w: content is not valid UTF-8", ErrNotText)
	}
	if len(s) > maxWriteBytes {
		return fmt.Errorf("%w: content is %d bytes, the cap is %d", ErrTooLarge, len(s), maxWriteBytes)
	}
	if key, ok := secretKeyIn(s); ok {
		return fmt.Errorf("%w: content sets %q, which names a credential; use a ${secret:name} reference", ErrExecutedName, clip(key))
	}
	return nil
}

// secretKeyIn looks for a key that actions.LooksSecret names, in the
// "key = value" (properties, .env, YAML) and `"key": value` (JSON) forms.
func secretKeyIn(content string) (string, bool) {
	for _, line := range strings.Split(content, "\n") {
		l := strings.TrimSpace(line)
		if l == "" || strings.HasPrefix(l, "#") || strings.HasPrefix(l, "//") {
			continue
		}
		if k, _, ok := strings.Cut(l, "="); ok && actions.LooksSecret(strings.Trim(strings.TrimSpace(k), `"'`)) {
			return strings.TrimSpace(k), true
		}
		if k, _, ok := strings.Cut(l, ":"); ok && actions.LooksSecret(strings.Trim(strings.TrimSpace(k), `"'`)) {
			return strings.TrimSpace(k), true
		}
	}
	return "", false
}

// writablePath refuses the names and directories file_write must not write
// (design §2.5): everything that executes or loads, the compose names, and
// every directory a template mounts or builds from.
func writablePath(p string, mounts []string) error {
	base := strings.ToLower(path.Base(strings.ReplaceAll(p, "\\", "/")))
	refuse := func(why string) error {
		return fmt.Errorf("%w: %s names %s", ErrExecutedName, clip(p), why)
	}
	switch {
	case strings.HasPrefix(base, ".env"):
		return refuse("an environment file")
	case strings.HasPrefix(base, "dockerfile"), strings.HasPrefix(base, "containerfile"):
		return refuse("a Dockerfile")
	case base == "makefile", base == "gnumakefile", base == "justfile",
		strings.HasSuffix(base, ".mk"):
		return refuse("a build file")
	case base == ".pre-commit-config.yaml", base == ".pre-commit-config.yml",
		base == "lefthook.yml", base == "lefthook.yaml":
		return refuse("a hook configuration file")
	case strings.HasSuffix(base, ".jar"), strings.HasSuffix(base, ".sh"),
		strings.HasSuffix(base, ".bash"), strings.HasSuffix(base, ".zsh"),
		strings.HasSuffix(base, ".ps1"), strings.HasSuffix(base, ".psm1"),
		strings.HasSuffix(base, ".psd1"), strings.HasSuffix(base, ".bat"),
		strings.HasSuffix(base, ".cmd"), strings.HasSuffix(base, ".py"),
		strings.HasSuffix(base, ".rb"), strings.HasSuffix(base, ".pl"),
		strings.HasSuffix(base, ".js"), strings.HasSuffix(base, ".mjs"),
		strings.HasSuffix(base, ".cjs"),
		strings.HasSuffix(base, ".service"), strings.HasSuffix(base, ".timer"),
		strings.HasSuffix(base, ".socket"), strings.HasSuffix(base, ".path"),
		strings.HasSuffix(base, ".mount"):
		return refuse("an executable, script, archive or unit file")
	case base == "compose.yaml", base == "compose.yml",
		base == "compose.override.yaml", base == "compose.override.yml":
		return refuse("a compose file")
	case strings.HasPrefix(base, "docker-compose") &&
		(strings.HasSuffix(base, ".yml") || strings.HasSuffix(base, ".yaml")):
		return refuse("a compose file")
	}
	for _, part := range pathParts(p) {
		// .husky and .githooks hold scripts git runs whatever their names.
		if strings.EqualFold(part, ".husky") || strings.EqualFold(part, ".githooks") {
			return refuse("a git hook")
		}
	}
	if strings.HasSuffix(base, ".yaml") || strings.HasSuffix(base, ".yml") {
		for _, part := range pathParts(p) {
			if strings.EqualFold(part, "k8s") || strings.EqualFold(part, "manifests") {
				return refuse("a Kubernetes manifest")
			}
		}
	}
	if underAny(strings.ReplaceAll(p, "\\", "/"), mounts) {
		return fmt.Errorf("%w: %s is under a directory a template mounts or builds from", ErrExecutedName, clip(p))
	}
	return nil
}

// tempName builds a temporary name next to name. The random suffix keeps two
// concurrent ops apart; O_EXCL makes a collision an error, never a write
// through a planted file.
func tempName(name string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("ops: generating a temp name: %w", err)
	}
	dir, base := path.Dir(name), path.Base(name)
	tmp := "." + base + ".piperoom-tmp-" + hex.EncodeToString(b[:])
	if dir == "." {
		return tmp, nil
	}
	return dir + "/" + tmp, nil
}
