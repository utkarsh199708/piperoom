package quickstart

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
)

const testKey = "sk-picker-0123456789"

// mapEnv is a quickstart.Env over a map.
type mapEnv struct {
	mu sync.Mutex
	m  map[string]string
}

func (e *mapEnv) Lookup(n string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.m[n]
	return v, ok
}
func (e *mapEnv) Set(n, v string) error { e.mu.Lock(); defer e.mu.Unlock(); e.m[n] = v; return nil }
func (e *mapEnv) Unset(n string) error  { e.mu.Lock(); defer e.mu.Unlock(); delete(e.m, n); return nil }
func (e *mapEnv) get(n string) string   { v, _ := e.Lookup(n); return v }

type pickerEnv struct {
	m      *Models
	env    *mapEnv
	dir    string
	builds atomic.Int32
	// probe is set by the fake build: it sends one request to the chosen
	// base URL, as the start checks' probe does.
	installed atomic.Value
	buildErr  error
}

func groqSupported() []llm.SupportedModel {
	return []llm.SupportedModel{{Provider: "openai", Flavor: "vllm", Model: "qwen3:32b", Status: llm.GateSupported,
		Baseline: "b.json", GatedAt: "2026-10-01"}}
}

// newPicker starts on an openai vllm endpoint at base with a stored key.
func newPicker(t *testing.T, base string) *pickerEnv {
	t.Helper()
	p := &pickerEnv{env: &mapEnv{m: map[string]string{config.EnvModelAPIKey: testKey}}, dir: t.TempDir()}
	cfg := config.Config{ModelProvider: "openai", ModelFlavor: "vllm", ModelBaseURL: base, Model: "qwen3:32b"}
	p.m = NewModels(ModelsOptions{
		Mode: httpapi.ModelModeStandalone, ProjectDir: p.dir, Config: cfg, Ready: true, SoloUser: "solo",
		Stored: ModelEnv{KeyVar: config.EnvModelAPIKey, Key: testKey},
		Env:    p.env,
		Panel:  httpapi.PanelInput{Supported: groqSupported(), Getenv: p.env.get},
		Now:    func() time.Time { return time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC) },
		Build: func(ctx context.Context, cfg config.Config) (func(), func(), error) {
			p.builds.Add(1)
			if p.buildErr != nil {
				return nil, nil, p.buildErr
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.ModelBaseURL+"/models", nil)
			if err != nil {
				return nil, nil, err
			}
			req.Header.Set("Authorization", "Bearer "+p.env.get(config.EnvModelAPIKey))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, nil, err
			}
			resp.Body.Close()
			return func() { p.installed.Store(cfg.ModelBaseURL) }, func() {}, nil
		},
	})
	return p
}

// hostCounter is an httptest model host that counts requests and the key
// each one carried.
func hostCounter(t *testing.T) (*httptest.Server, *atomic.Int32, *atomic.Value) {
	t.Helper()
	var n atomic.Int32
	var auth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		auth.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, &n, &auth
}

func code(err error) string {
	var re *room.Error
	if errors.As(err, &re) {
		return re.Code
	}
	return ""
}

func ptr(s string) *string { return &s }

func TestPutWithNewHostAndNoKeyIsKeyRequiredAndSendsNothing(t *testing.T) {
	a, _, _ := hostCounter(t)
	b, hitsB, _ := hostCounter(t)
	p := newPicker(t, a.URL+"/v1")
	for _, c := range []httpapi.ModelChoice{
		{Provider: "openai", Flavor: "vllm", BaseURL: b.URL + "/v1", Model: "qwen3:32b"},       // new host
		{Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/other/v1", Model: "qwen3:32b"}, // same host, new path
		{Provider: "openai", Flavor: "ollama", BaseURL: a.URL + "/v1", Model: "qwen3:32b"},     // new flavor
		{Provider: "anthropic", Model: "claude-opus-5-5"},                                      // new provider
	} {
		_, err := p.m.Apply(context.Background(), c)
		if code(err) != "key_required" {
			t.Fatalf("Apply(%+v) = %v, want key_required", c, err)
		}
	}
	if p.builds.Load() != 0 || hitsB.Load() != 0 {
		t.Fatalf("builds = %d, requests to the new host = %d; want none", p.builds.Load(), hitsB.Load())
	}
	if _, err := os.Stat(ModelEnvPath(p.dir)); !os.IsNotExist(err) {
		t.Fatalf("model.env written on a refusal: %v", err)
	}
	if p.env.get(config.EnvModelAPIKey) != testKey {
		t.Fatal("the stored key changed on a refusal")
	}
}

func TestPutWithNewHostAndKeyRebuildsAndWritesModelEnv(t *testing.T) {
	a, _, _ := hostCounter(t)
	b, hitsB, authB := hostCounter(t)
	p := newPicker(t, a.URL+"/v1")
	const newKey = "sk-new-host-key-42"
	panel, err := p.m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: b.URL + "/v1", Model: "qwen3:32b", Key: ptr(newKey)})
	if err != nil {
		t.Fatal(err)
	}
	if hitsB.Load() != 1 || authB.Load() != "Bearer "+newKey || p.installed.Load() != b.URL+"/v1" {
		t.Fatalf("hits %d auth %v installed %v", hitsB.Load(), authB.Load(), p.installed.Load())
	}
	if panel.State != httpapi.ModelStateReady || panel.Roles[0].BaseURLHost != "127.0.0.1" || panel.Roles[0].Status != llm.GateSupported {
		t.Fatalf("panel = %+v", panel)
	}
	fi, err := os.Stat(ModelEnvPath(p.dir))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && fi.Mode().Perm() != 0o600 {
		t.Fatalf("model.env mode = %v, want 0600", fi.Mode().Perm())
	}
	e, ok, err := ReadModelEnv(p.dir)
	if err != nil || !ok || e.Key.Reveal() != newKey || e.KeyVar != config.EnvModelAPIKey || e.Vars["PIPEROOM_MODEL_BASE_URL"] != b.URL+"/v1" {
		t.Fatalf("model.env = %+v %v %v", e, ok, err)
	}
	// A change of model only keeps the key, which stays in model.env.
	if _, err := p.m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: b.URL + "/v1", Model: "qwen3:32b"}); err != nil {
		t.Fatal(err)
	}
	if e, _, _ := ReadModelEnv(p.dir); e.Key.Reveal() != newKey || authB.Load() != "Bearer "+newKey {
		t.Fatalf("the key did not stay with its host: %+v", e)
	}
	got := ModelEnv{Vars: map[string]string{}}.Apply(config.Config{})
	if got.Model != "" {
		t.Fatal("an empty model.env changes nothing")
	}
}

func TestPutValidation(t *testing.T) {
	a, hitsA, _ := hostCounter(t)
	p := newPicker(t, a.URL+"/v1")
	for name, c := range map[string]httpapi.ModelChoice{
		"metadata host":    {Provider: "openai", Flavor: "vllm", BaseURL: "http://169.254.169.254/v1", Model: "qwen3:32b", Key: ptr("k")},
		"user info":        {Provider: "openai", Flavor: "vllm", BaseURL: "https://user:pw@gw.example.com/v1", Model: "qwen3:32b", Key: ptr("k")},
		"plain http":       {Provider: "openai", Flavor: "vllm", BaseURL: "http://gw.example.com/acct-9/v1", Model: "qwen3:32b", Key: ptr("k")},
		"unsupported":      {Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/v1", Model: "not-gated-1"},
		"unknown provider": {Provider: "nope", Model: "m", Key: ptr("k")},
		"bad flavor":       {Provider: "openai", Flavor: "nope", Model: "m", Key: ptr("k")},
		"newline in model": {Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/v1", Model: "a\nPIPEROOM_MODEL_PROVIDER=x"},
		"key on bedrock":   {Provider: "bedrock", Model: "anthropic.claude", Key: ptr("k")},
		"no model":         {Provider: "openai"},
	} {
		_, err := p.m.Apply(context.Background(), c)
		if code(err) != "validation" {
			t.Fatalf("%s: Apply = %v, want validation", name, err)
		}
		if strings.Contains(err.Error(), "acct-9") || strings.Contains(err.Error(), "pw@") || strings.Contains(err.Error(), "169.254") {
			t.Fatalf("%s: the message echoes the base URL: %v", name, err)
		}
	}
	if p.builds.Load() != 0 || hitsA.Load() != 0 {
		t.Fatalf("a refused choice built an endpoint (%d) or sent a request (%d)", p.builds.Load(), hitsA.Load())
	}
	// PIPEROOM_MODEL_UNVERIFIED=on admits the unlisted model.
	p.m.cfg.ModelUnverified = "on"
	if _, err := p.m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/v1", Model: "not-gated-1"}); err != nil {
		t.Fatal(err)
	}
}

func TestPutBuildFailureRestoresKeyAndWritesNothing(t *testing.T) {
	a, _, _ := hostCounter(t)
	p := newPicker(t, a.URL+"/v1")
	p.buildErr = errors.New("agent: probe of " + a.URL + "/v1 failed for sk-bad")
	_, err := p.m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/v1", Model: "qwen3:32b", Key: ptr("sk-bad")})
	if code(err) != "validation" || strings.Contains(err.Error(), "sk-bad") || strings.Contains(err.Error(), a.URL) {
		t.Fatalf("Apply = %v", err)
	}
	if p.env.get(config.EnvModelAPIKey) != testKey {
		t.Fatal("the old key was not restored")
	}
	if _, err := os.Stat(ModelEnvPath(p.dir)); !os.IsNotExist(err) {
		t.Fatal("model.env written after a failed build")
	}
}

func TestEmptyKeyClearsTheOldOne(t *testing.T) {
	a, _, _ := hostCounter(t)
	p := newPicker(t, a.URL+"/v1")
	p.env.m[config.EnvModelAPIKeyFile] = "/tmp/key"
	if _, err := p.m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/x/v1", Model: "qwen3:32b", Key: ptr("")}); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.env.Lookup(config.EnvModelAPIKey); ok {
		t.Fatal("the old key would follow the new base URL")
	}
	if _, ok := p.env.Lookup(config.EnvModelAPIKeyFile); ok {
		t.Fatal("the key file would follow the new base URL")
	}
	if e, _, _ := ReadModelEnv(p.dir); e.KeyVar != "" {
		t.Fatalf("model.env kept a key: %+v", e)
	}
}

func TestPickerConcurrentApplyAndPanel(t *testing.T) {
	a, _, _ := hostCounter(t)
	p := newPicker(t, a.URL+"/v1")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = p.m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/v1", Model: "qwen3:32b"})
		}()
		go func() { defer wg.Done(); _ = p.m.Panel() }()
	}
	wg.Wait()
	if p.builds.Load() != 8 {
		t.Fatalf("builds = %d", p.builds.Load())
	}
}

func TestNoModelPanel(t *testing.T) {
	m := NewModels(ModelsOptions{Mode: httpapi.ModelModeQuickstart, Config: config.Config{}, Ready: true,
		Panel: httpapi.PanelInput{Supported: groqSupported(), Getenv: func(string) string { return "" }}})
	p := m.Panel()
	// The default Claude model without ANTHROPIC_API_KEY is no usable model.
	if p.State != httpapi.ModelStateNoModel || len(p.Supported) != 1 || p.Mode != "quickstart" {
		t.Fatalf("panel = %+v", p)
	}
}

func TestModelEnvFile(t *testing.T) {
	dir := t.TempDir()
	if _, ok, err := ReadModelEnv(dir); ok || err != nil {
		t.Fatalf("no file: %v %v", ok, err)
	}
	want := ModelEnv{Vars: map[string]string{"PIPEROOM_MODEL": "m1", "PIPEROOM_MODEL_PROVIDER": "openai"}, KeyVar: "ANTHROPIC_API_KEY", Key: "k1"}
	if err := WriteModelEnv(dir, want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadModelEnv(dir)
	if err != nil || !ok || got.Vars["PIPEROOM_MODEL"] != "m1" || got.Key != "k1" || got.KeyVar != "ANTHROPIC_API_KEY" {
		t.Fatalf("read back = %+v %v %v", got, ok, err)
	}
	if c := got.Apply(config.Config{Model: "old"}); c.Model != "m1" || c.ModelProvider != "openai" {
		t.Fatalf("Apply = %+v", c)
	}
	if err := WriteModelEnv(dir, ModelEnv{Vars: map[string]string{"PIPEROOM_MODEL": "a\nb"}}); err == nil {
		t.Fatal("a line break was written")
	}
	if err := WriteModelEnv(dir, ModelEnv{Vars: map[string]string{"HOME": "x"}}); err == nil {
		t.Fatal("a non-model variable was written")
	}
	path := ModelEnvPath(dir)
	if err := os.WriteFile(path, []byte("SHELL=/bin/sh\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadModelEnv(dir); err == nil || strings.Contains(err.Error(), "/bin/sh") {
		t.Fatalf("a foreign variable: %v", err)
	}
	if runtime.GOOS == "windows" {
		return
	}
	// A link planted at model.env is refused on read and replaced on write.
	target := filepath.Join(dir, "elsewhere")
	if err := os.WriteFile(target, []byte("PIPEROOM_MODEL=x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(path)
	if err := os.Symlink("../elsewhere", path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadModelEnv(dir); err == nil {
		t.Fatal("read through a link")
	}
	if err := WriteModelEnv(dir, want); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(target); !bytes.Equal(b, []byte("PIPEROOM_MODEL=x\n")) {
		t.Fatalf("the write followed the link: %q", b)
	}
}

func TestPrepareAppliesModelEnv(t *testing.T) {
	dir := t.TempDir()
	if err := WriteModelEnv(dir, ModelEnv{Vars: map[string]string{"PIPEROOM_MODEL": "picked"}, KeyVar: config.EnvModelAPIKey, Key: "k"}); err != nil {
		t.Fatal(err)
	}
	owners := filepath.Join(dir, "owners.yaml")
	src := "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\nlayers:\n  postgres: user:solo\n"
	if err := os.WriteFile(owners, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	s, cleanup, err := Prepare(Options{Config: config.Config{HTTPAddr: "127.0.0.1:0", UsersFile: defaultUsersFile, Model: "env-model"},
		OwnersPath: owners, ProjectDir: dir, TempDir: dir, Log: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if s.Config.Model != "picked" || s.ModelEnv.Key != "k" || s.ModelMode() != httpapi.ModelModeQuickstart {
		t.Fatalf("settings = %+v", s)
	}
	if strings.Contains(logs.String(), "k\n") {
		t.Fatal("the key is logged")
	}
}
