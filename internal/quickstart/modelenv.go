package quickstart

// .piperoom/model.env (V7; docs/design/model-providers.md section 11): the
// model the standalone picker chose, and its key, kept for the next start.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

const (
	// ModelStateDir is the directory, under the project directory, that
	// holds model.env.
	ModelStateDir = ".piperoom"
	// ModelEnvFile is the picker's file inside ModelStateDir.
	ModelEnvFile = "model.env"
	// maxModelEnvBytes caps model.env: a dozen short lines.
	maxModelEnvBytes = 64 << 10
)

// keyVars are the variables model.env may hold a key in: the main key of
// the openai and gemini adapters, and Anthropic's.
var keyVars = map[string]bool{config.EnvModelAPIKey: true, "ANTHROPIC_API_KEY": true}

// modelVars are the config variables model.env may set, with their field.
var modelVars = map[string]func(*config.Config) *string{
	"PIPEROOM_MODEL_PROVIDER":       func(c *config.Config) *string { return &c.ModelProvider },
	"PIPEROOM_MODEL_FLAVOR":         func(c *config.Config) *string { return &c.ModelFlavor },
	"PIPEROOM_MODEL_BASE_URL":       func(c *config.Config) *string { return &c.ModelBaseURL },
	"PIPEROOM_MODEL":                func(c *config.Config) *string { return &c.Model },
	"PIPEROOM_TRIAGE_PASS_PROVIDER": func(c *config.Config) *string { return &c.TriagePassProvider },
	"PIPEROOM_TRIAGE_PASS_MODEL":    func(c *config.Config) *string { return &c.TriagePassModel },
	"PIPEROOM_TRIAGE_PASS_BASE_URL": func(c *config.Config) *string { return &c.TriagePassBaseURL },
	"PIPEROOM_SUMMARY_PROVIDER":     func(c *config.Config) *string { return &c.SummaryProvider },
	"PIPEROOM_SUMMARY_MODEL":        func(c *config.Config) *string { return &c.SummaryModel },
	"PIPEROOM_SUMMARY_BASE_URL":     func(c *config.Config) *string { return &c.SummaryBaseURL },
	"PIPEROOM_JUDGE_PROVIDER":       func(c *config.Config) *string { return &c.JudgeProvider },
	"PIPEROOM_JUDGE_MODEL":          func(c *config.Config) *string { return &c.JudgeModel },
	"PIPEROOM_JUDGE_BASE_URL":       func(c *config.Config) *string { return &c.JudgeBaseURL },
}

// ModelEnv is the content of model.env.
type ModelEnv struct {
	// Vars are model variables (modelVars) the picker set; a variable not
	// listed keeps the environment's value.
	Vars map[string]string
	// KeyVar names the variable Key is for; "" = no key stored.
	KeyVar string
	Key    netsec.Secret
}

// Apply returns cfg with e's variables written over it.
func (e ModelEnv) Apply(cfg config.Config) config.Config {
	for name, v := range e.Vars {
		*modelVars[name](&cfg) = v
	}
	return cfg
}

// modelEnvOf is the model.env content of cfg's model variables.
func modelEnvOf(cfg config.Config) map[string]string {
	out := map[string]string{}
	for name, f := range modelVars {
		out[name] = *f(&cfg)
	}
	return out
}

// errUnsafePath refuses a model.env path that is not a regular file or a
// directory that is a link: the file holds a key.
var errUnsafePath = errors.New("is a link or not a regular file")

// openStateDir opens projectDir/.piperoom through os.Root, creating it 0700.
// The directory itself must not be a link.
func openStateDir(projectDir string, create bool) (*os.Root, error) {
	if projectDir == "" {
		projectDir = "."
	}
	proj, err := os.OpenRoot(projectDir)
	if err != nil {
		return nil, fmt.Errorf("quickstart: model.env: open the project directory: %w", err)
	}
	defer proj.Close()
	fi, err := proj.Lstat(ModelStateDir)
	switch {
	case errors.Is(err, fs.ErrNotExist) && create:
		if err := proj.Mkdir(ModelStateDir, 0o700); err != nil {
			return nil, fmt.Errorf("quickstart: model.env: make %s: %w", ModelStateDir, err)
		}
	case err != nil:
		return nil, fmt.Errorf("quickstart: model.env: %s: %w", ModelStateDir, err)
	case !fi.IsDir():
		return nil, fmt.Errorf("quickstart: model.env: %s %w", ModelStateDir, errUnsafePath)
	}
	dir, err := proj.OpenRoot(ModelStateDir)
	if err != nil {
		return nil, fmt.Errorf("quickstart: model.env: open %s: %w", ModelStateDir, err)
	}
	return dir, nil
}

// ReadModelEnv reads projectDir/.piperoom/model.env; ok is false when there
// is none. A malformed file is an error that names the line, never a value.
func ReadModelEnv(projectDir string) (ModelEnv, bool, error) {
	dir, err := openStateDir(projectDir, false)
	if errors.Is(err, fs.ErrNotExist) {
		return ModelEnv{}, false, nil
	}
	if err != nil {
		return ModelEnv{}, false, err
	}
	defer dir.Close()
	fi, err := dir.Lstat(ModelEnvFile)
	if errors.Is(err, fs.ErrNotExist) {
		return ModelEnv{}, false, nil
	}
	if err != nil {
		return ModelEnv{}, false, fmt.Errorf("quickstart: model.env: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return ModelEnv{}, false, fmt.Errorf("quickstart: model.env %w", errUnsafePath)
	}
	if fi.Size() > maxModelEnvBytes {
		return ModelEnv{}, false, errors.New("quickstart: model.env: larger than 64 KiB")
	}
	b, err := dir.ReadFile(ModelEnvFile)
	if err != nil {
		return ModelEnv{}, false, fmt.Errorf("quickstart: model.env: %w", err)
	}
	e, err := parseModelEnv(b)
	if err != nil {
		return ModelEnv{}, false, err
	}
	return e, true, nil
}

func parseModelEnv(b []byte) (ModelEnv, error) {
	e := ModelEnv{Vars: map[string]string{}}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, v, ok := strings.Cut(line, "=")
		switch {
		case !ok:
			return ModelEnv{}, fmt.Errorf("quickstart: model.env line %d: want NAME=value", n)
		case keyVars[name]:
			if e.KeyVar != "" {
				return ModelEnv{}, fmt.Errorf("quickstart: model.env line %d: a second key", n)
			}
			e.KeyVar, e.Key = name, netsec.Secret(v)
		case modelVars[name] != nil:
			e.Vars[name] = v
		default:
			return ModelEnv{}, fmt.Errorf("quickstart: model.env line %d: %q is not a model variable", n, name)
		}
	}
	if err := sc.Err(); err != nil {
		return ModelEnv{}, fmt.Errorf("quickstart: model.env: %w", err)
	}
	return e, nil
}

// WriteModelEnv writes projectDir/.piperoom/model.env, mode 0600, through
// os.Root: a new temp file renamed over the old one, so a reader never sees
// half a file and a link planted at the name is replaced, not followed. A
// value with a line break or another control character is refused (the
// caller validates first; this is the file format's own guard).
func WriteModelEnv(projectDir string, e ModelEnv) error {
	var buf bytes.Buffer
	buf.WriteString("# Written by the Piperoom model picker (PUT /api/models). It holds a key: keep it private.\n")
	names := make([]string, 0, len(e.Vars))
	for name := range e.Vars {
		if modelVars[name] == nil {
			return fmt.Errorf("quickstart: model.env: %q is not a model variable", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !safeValue(e.Vars[name]) {
			return fmt.Errorf("quickstart: model.env: %s holds a control character", name)
		}
		fmt.Fprintf(&buf, "%s=%s\n", name, e.Vars[name])
	}
	if e.KeyVar != "" {
		if !keyVars[e.KeyVar] {
			return fmt.Errorf("quickstart: model.env: %q is not a key variable", e.KeyVar)
		}
		if !safeValue(e.Key.Reveal()) {
			return errors.New("quickstart: model.env: the key holds a control character")
		}
		buf.WriteString(e.KeyVar + "=" + e.Key.Reveal() + "\n")
	}
	dir, err := openStateDir(projectDir, true)
	if err != nil {
		return err
	}
	defer dir.Close()
	tmp := ModelEnvFile + ".tmp"
	_ = dir.Remove(tmp)
	f, err := dir.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("quickstart: model.env: create: %w", err)
	}
	_, werr := f.Write(buf.Bytes())
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		// A umask cannot widen 0600, but an O_EXCL file left by a crash
		// was removed above; this keeps the mode exact either way.
		werr = dir.Chmod(tmp, 0o600)
	}
	if werr == nil {
		werr = dir.Rename(tmp, ModelEnvFile)
	}
	if werr != nil {
		_ = dir.Remove(tmp)
		return fmt.Errorf("quickstart: model.env: write: %w", werr)
	}
	return nil
}

// ModelEnvPath is where model.env lives, for logs.
func ModelEnvPath(projectDir string) string {
	if projectDir == "" {
		projectDir = "."
	}
	return filepath.Join(projectDir, ModelStateDir, ModelEnvFile)
}

// safeValue refuses control characters (a line break would start a new
// variable) and leading or trailing space (it would not read back).
func safeValue(v string) bool {
	if v != strings.TrimSpace(v) {
		return false
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
