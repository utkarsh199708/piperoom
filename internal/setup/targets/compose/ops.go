package compose

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// PropertiesParams are properties_write's typed params: an allow-listed file
// and allow-listed keys. No field carries free-form content.
type PropertiesParams struct {
	// File is ".env", "worker.properties" or "connect-distributed.properties",
	// under the project directory.
	File string `json:"file"`
	// Entries are the key/value pairs; the allow-list depends on the file
	// kind, and a credential-named key takes a ${secret:name} reference only.
	Entries map[string]string `json:"entries"`
}

// compose_write (T1) renders one stack and writes it.
type composeWriteOp struct{ d *Driver }

func (composeWriteOp) Name() string   { return "compose_write" }
func (composeWriteOp) Target() string { return targetKind }
func (composeWriteOp) Tier() string   { return protocol.TierT1 }
func (composeWriteOp) Layer() string  { return layerName }
func (composeWriteOp) Describe() string {
	return "render a Compose stack from a typed template and write it under the project directory"
}

func (composeWriteOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"templates"},
		"properties": map[string]any{
			"templates": map[string]any{
				"type": "array", "minItems": 1, "maxItems": 4,
				"items": map[string]any{"type": "string", "enum": []string{
					TemplateKafka, TemplateConnect, TemplateRegistry, TemplatePostgres,
				}},
			},
			"project":  map[string]any{"type": "string", "pattern": `^piperoom-[a-z0-9_-]{1,54}$`},
			"ports":    map[string]any{"type": "object", "maxProperties": maxPorts, "additionalProperties": map[string]any{"type": "integer", "minimum": minPort, "maximum": maxPort}},
			"registry": map[string]any{"type": "boolean"},
			"file":     map[string]any{"type": "string"},
		},
	}
}

func (o composeWriteOp) Validate(params json.RawMessage, env ops.Env) error {
	var p WriteParams
	if err := decodeParams(params, &p); err != nil {
		return err
	}
	r, err := o.d.Render(p)
	if err != nil {
		return err
	}
	return ValidateYAML(r.Doc, o.d.classOrDefault())
}

func (o composeWriteOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	var p WriteParams
	if err := decodeParams(params, &p); err != nil {
		return ops.Result{}, err
	}
	r, err := o.d.Render(p)
	if err != nil {
		return ops.Result{}, err
	}
	if err := ValidateYAML(r.Doc, o.d.classOrDefault()); err != nil {
		return ops.Result{}, err
	}
	// Every directory the file mounts is registered with the ops package, so
	// file_write refuses it from now on (design §2.5). Registration comes
	// BEFORE the write: a registration failure must not leave the file on
	// disk with its mounts unprotected.
	for _, dir := range r.Mounts {
		if err := o.d.reg.RegisterMountDir(dir); err != nil {
			return ops.Result{}, fmt.Errorf("compose: registering mount %s: %w", q(dir), err)
		}
	}
	clean, changed, err := writeText(env, r.File, r.Doc, 0o644)
	if err != nil {
		return ops.Result{}, err
	}
	summary := fmt.Sprintf("wrote %s (%d bytes), services: %s", clean, len(r.Doc), strings.Join(r.Services, ", "))
	if !changed {
		summary = fmt.Sprintf("%s is unchanged (%d bytes), services: %s", clean, len(r.Doc), strings.Join(r.Services, ", "))
	}
	return ops.Result{
		Summary: summary,
		Data: map[string]any{
			"file":     clean,
			"bytes":    len(r.Doc),
			"changed":  changed,
			"services": r.Services,
			"sha256":   fmt.Sprintf("%x", sha256.Sum256(r.Doc)),
			"env_file": EnvFileName,
			"env_keys": r.EnvKeys,
		},
		Receipt: events.SetupReceipt{Kind: "file", Target: clean, At: now(env)},
	}, nil
}

// properties_write (T1) writes one allow-listed properties file.
type propertiesWriteOp struct{ d *Driver }

func (propertiesWriteOp) Name() string   { return "properties_write" }
func (propertiesWriteOp) Target() string { return targetKind }
func (propertiesWriteOp) Tier() string   { return protocol.TierT1 }
func (propertiesWriteOp) Layer() string  { return layerName }
func (propertiesWriteOp) Describe() string {
	return "write an allow-listed properties file (.env or Kafka Connect worker properties)"
}

func (propertiesWriteOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"file", "entries"},
		"properties": map[string]any{
			"file": map[string]any{"type": "string", "minLength": 1},
			"entries": map[string]any{
				"type": "object", "minProperties": 1, "maxProperties": maxPropertiesEntries,
				"additionalProperties": map[string]any{"type": "string"},
			},
		},
	}
}

func (o propertiesWriteOp) Validate(params json.RawMessage, env ops.Env) error {
	_, _, _, err := o.prepare(params)
	return err
}

func (o propertiesWriteOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, kind, keys, err := o.prepare(params)
	if err != nil {
		return ops.Result{}, err
	}
	text := renderProperties(keys, p.Entries)
	clean, changed, err := writeText(env, p.File, text, 0o600)
	if err != nil {
		return ops.Result{}, err
	}
	summary := fmt.Sprintf("wrote %s (%d keys): %s", clean, len(keys), strings.Join(keys, ", "))
	if !changed {
		summary = fmt.Sprintf("%s is unchanged (%d keys): %s", clean, len(keys), strings.Join(keys, ", "))
	}
	return ops.Result{
		Summary: summary,
		Data: map[string]any{
			"file":    clean,
			"kind":    string(kind),
			"bytes":   len(text),
			"changed": changed,
			"keys":    keys,
		},
		Receipt: events.SetupReceipt{Kind: "file", Target: clean, At: now(env)},
	}, nil
}

// prepare validates the properties params without I/O and returns them with
// the file kind and the sorted key list.
func (o propertiesWriteOp) prepare(params json.RawMessage) (PropertiesParams, propsKind, []string, error) {
	var p PropertiesParams
	if err := decodeParams(params, &p); err != nil {
		return p, "", nil, err
	}
	if p.File == "" {
		return p, "", nil, fmt.Errorf("%w: file is required", ErrParams)
	}
	if err := ops.CheckName(p.File); err != nil {
		return p, "", nil, fmt.Errorf("%w: file: %v", ErrPath, err)
	}
	kind, _, err := kindOf(p.File)
	if err != nil {
		return p, "", nil, err
	}
	keys, err := validateEntries(kind, p.Entries)
	if err != nil {
		return p, "", nil, err
	}
	return p, kind, keys, nil
}

// writeText writes name under the project directory through the U1-2 path
// rule and an os.Root, and reports whether the bytes changed. A second write
// of the same bytes is a no-op, so re-running a step is cheap and a diff
// preview stays empty.
func writeText(env ops.Env, name string, data []byte, mode fs.FileMode) (string, bool, error) {
	if env.Confine == nil {
		return "", false, errors.New("compose: the project root is not open")
	}
	clean, err := env.Confine.Check(name)
	if err != nil {
		return "", false, err
	}
	// The same refusal file_write enforces: a directory a rendered template
	// mounts or builds from takes no writes, from this writer either.
	if ops.MountedPath(clean, env.MountDirs) {
		return "", false, fmt.Errorf("%w: %s is under a directory a template mounts or builds from", ops.ErrExecutedName, q(name))
	}
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return "", false, fmt.Errorf("compose: opening the project root: %w", err)
	}
	defer root.Close()
	if fi, lerr := root.Lstat(clean); lerr == nil {
		if !fi.Mode().IsRegular() {
			return "", false, fmt.Errorf("%w: %s exists and is not a regular file", ops.ErrNotRegular, q(name))
		}
		if old, rerr := root.ReadFile(clean); rerr == nil && bytes.Equal(old, data) {
			// Unchanged bytes are a no-op write, but the mode still applies:
			// a pre-existing world-readable .env must not stay 0644 because
			// its content already matched.
			if fi.Mode().Perm() != mode.Perm() {
				if cerr := root.Chmod(clean, mode); cerr != nil {
					return "", false, fmt.Errorf("compose: fixing the mode of %s: %w", q(name), cerr)
				}
			}
			return clean, false, nil
		}
	} else if !errors.Is(lerr, fs.ErrNotExist) {
		return "", false, fmt.Errorf("compose: stat %s: %w", q(name), lerr)
	}
	if dir := path.Dir(clean); dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return "", false, fmt.Errorf("compose: creating %s: %w", q(path.Dir(name)), err)
		}
	}
	tmp, err := tempName(clean)
	if err != nil {
		return "", false, err
	}
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return "", false, fmt.Errorf("compose: creating a temp file for %s: %w", q(name), err)
	}
	_, werr := f.Write(data)
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
		return "", false, fmt.Errorf("compose: writing %s: %w", q(name), werr)
	}
	return clean, true, nil
}

// tempName builds a temporary name next to name; O_EXCL turns a collision into
// an error rather than a write through a planted file.
func tempName(name string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("compose: generating a temp name: %w", err)
	}
	dir, base := path.Dir(name), path.Base(name)
	tmp := "." + base + ".piperoom-tmp-" + hex.EncodeToString(b[:])
	if dir == "." {
		return tmp, nil
	}
	return dir + "/" + tmp, nil
}

// composeSubcommands are the compose subcommands Argv accepts. Every one of
// them reads the same pinned file and env file; none of them deletes data
// (there is no `down -v` form).
var composeSubcommands = map[string]bool{
	"up": true, "down": true, "ps": true, "config": true,
	"restart": true, "stop": true, "start": true, "logs": true,
}

// Argv builds the one argv shape every compose op runs (design §2.5):
//
//	docker compose --project-directory <dir> -f <file> --env-file <env> <sub> [services]
//
// and nothing else. U1-3b's ops call this; the test asserts the shape.
func Argv(projectDir, file, envFile, sub string, services []string) ([]string, error) {
	if projectDir == "" || !filepath.IsAbs(projectDir) {
		return nil, fmt.Errorf("%w: the project directory must be absolute", ErrParams)
	}
	if !isComposeName(file) {
		return nil, fmt.Errorf("%w: file %s is not a compose file name", ErrParams, q(file))
	}
	// The file is joined under the project directory below, so it must pass
	// the same path rule every file op enforces: ".." or an absolute path
	// would point compose at a file outside the project.
	if err := ops.CheckName(file); err != nil {
		return nil, fmt.Errorf("%w: file: %v", ErrParams, err)
	}
	if envFile == "" {
		envFile = EnvFileName
	}
	if envFile != EnvFileName {
		return nil, fmt.Errorf("%w: the env file must be %s at the project root", ErrParams, EnvFileName)
	}
	if !composeSubcommands[sub] {
		return nil, fmt.Errorf("%w: subcommand %s is not allowed", ErrParams, q(sub))
	}
	names := append([]string(nil), services...)
	sort.Strings(names)
	for _, name := range names {
		if !serviceNameRE.MatchString(name) {
			return nil, fmt.Errorf("%w: service %s is not a service name", ErrParams, q(name))
		}
	}
	argv := []string{
		"docker", "compose",
		"--project-directory", projectDir,
		"-f", filepath.Join(projectDir, filepath.FromSlash(file)),
		"--env-file", filepath.Join(projectDir, filepath.FromSlash(envFile)),
		sub,
	}
	return append(argv, names...), nil
}

// decodeParams fills dst from params, rejecting unknown fields, trailing JSON
// and keys that differ only in letter case, which the decoder would fold and
// let the last win (protocol.CheckParamKeys). This is the U1-2 rule; the
// server and the runner both validate the schema.
func decodeParams(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := protocol.CheckParamKeys(raw); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrParams, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing JSON", ErrParams)
	}
	return nil
}

// now is the injected clock, defaulted for an Env built by hand.
func now(env ops.Env) time.Time {
	if env.Now == nil {
		return time.Now()
	}
	return env.Now()
}
