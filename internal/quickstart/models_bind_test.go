package quickstart

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent/llm/openai"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
)

const envKey = "sk-operator-env-key-777"

// adapterPicker starts the picker on cfg over env. Its build behaves as the
// adapters do for the key: each role with its own endpoint loads its key the
// way openai.LoadKey and the agent's loadKey do (the picker's variables for
// the main role, else the provider's standard variable) and sends one
// request with it to its base URL.
func adapterPicker(t *testing.T, cfg config.Config, env map[string]string, stored ModelEnv) (*Models, *mapEnv, *atomic.Int32) {
	t.Helper()
	e := &mapEnv{m: env}
	var builds atomic.Int32
	m := NewModels(ModelsOptions{
		Mode: httpapi.ModelModeStandalone, ProjectDir: t.TempDir(), Config: cfg, Stored: stored, Ready: true, SoloUser: "solo",
		Env: e, Panel: httpapi.PanelInput{Getenv: e.get},
		Now: func() time.Time { return time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC) },
		Build: func(ctx context.Context, cfg config.Config) (func(), func(), error) {
			builds.Add(1)
			k, err := config.ParseModelKnobs(cfg)
			if err != nil {
				return nil, nil, err
			}
			main := k.Roles.Main
			for i, r := range []config.ModelRole{main, k.Roles.FirstLook, k.Roles.Summary, k.Roles.Judge} {
				// A role on the main provider shares the main endpoint.
				isMain := r.Provider == main.Provider
				if r.BaseURL == nil || (isMain && i > 0) {
					continue
				}
				var key string
				switch r.Provider {
				case config.ModelProviderOpenAI:
					s, err := openai.LoadKey(r.Flavor, isMain, e.get, os.ReadFile)
					if err != nil {
						return nil, nil, err
					}
					key = s.Reveal()
				case config.ModelProviderGemini:
					if key = e.get(config.EnvModelAPIKey); !isMain || key == "" {
						key = e.get("GEMINI_API_KEY")
					}
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.BaseURL.String()+"/models", nil)
				if err != nil {
					return nil, nil, err
				}
				req.Header.Set("Authorization", "Bearer "+key)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return nil, nil, err
				}
				resp.Body.Close()
			}
			return func() {}, func() {}, nil
		},
	})
	return m, e, &builds
}

// B1 (a): the main role keeps its provider and flavor, moves to a new host
// and sends an empty key. The adapter would fall back to OPENAI_API_KEY (or
// GEMINI_API_KEY), so the change needs a key of its own.
func TestPutNewHostEmptyKeyWithEnvKeyIsKeyRequired(t *testing.T) {
	a, _, _ := hostCounter(t)
	evil, hitsEvil, _ := hostCounter(t)
	for _, tc := range []struct {
		name string
		cfg  config.Config
		env  string
		c    httpapi.ModelChoice
	}{
		{"openai", config.Config{ModelProvider: "openai", ModelFlavor: "openai", ModelBaseURL: a.URL + "/v1", Model: "gpt-5", ModelUnverified: "on"},
			"OPENAI_API_KEY", httpapi.ModelChoice{Provider: "openai", Flavor: "openai", BaseURL: evil.URL + "/v1", Model: "gpt-5", Key: ptr("")}},
		{"groq", config.Config{ModelProvider: "openai", ModelFlavor: "groq", Model: "openai/gpt-oss-120b", ModelUnverified: "on"},
			"GROQ_API_KEY", httpapi.ModelChoice{Provider: "openai", Flavor: "groq", BaseURL: evil.URL + "/v1", Model: "openai/gpt-oss-120b", Key: ptr("  ")}},
		{"gemini", config.Config{ModelProvider: "gemini", ModelBaseURL: a.URL + "/v1", Model: "gemini-3-pro", ModelUnverified: "on"},
			"GEMINI_API_KEY", httpapi.ModelChoice{Provider: "gemini", BaseURL: evil.URL + "/v1", Model: "gemini-3-pro", Key: ptr("")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, env, builds := adapterPicker(t, tc.cfg, map[string]string{tc.env: envKey}, ModelEnv{})
			for _, c := range []httpapi.ModelChoice{tc.c, func() httpapi.ModelChoice { c := tc.c; c.Key = nil; return c }()} {
				_, err := m.Apply(context.Background(), c)
				if code(err) != "key_required" {
					t.Fatalf("Apply = %v, want key_required", err)
				}
			}
			if builds.Load() != 0 || hitsEvil.Load() != 0 {
				t.Fatalf("builds %d, requests to the new host %d; want none", builds.Load(), hitsEvil.Load())
			}
			if env.get(tc.env) != envKey {
				t.Fatal("the operator's variable changed")
			}
		})
	}
}

// B1 (b): main unchanged, a single-shot role moves to another provider on a
// new host. That role reads only its standard variable, set by the operator,
// so the picker refuses instead of sending it there.
func TestPutRoleToNewHostWithEnvKeyIsRefused(t *testing.T) {
	evil, hitsEvil, _ := hostCounter(t)
	cfg := config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5", ModelUnverified: "on"}
	for _, role := range []string{"first_look", "summary", "judge"} {
		t.Run(role, func(t *testing.T) {
			m, env, builds := adapterPicker(t, cfg, map[string]string{"OPENAI_API_KEY": envKey, "ANTHROPIC_API_KEY": "sk-ant"}, ModelEnv{})
			rc := &httpapi.ModelRoleChoice{Provider: "openai", Model: "gpt-5", BaseURL: evil.URL + "/v1"}
			roles := &httpapi.ModelRoleChoices{}
			switch role {
			case "first_look":
				roles.FirstLook = rc
			case "summary":
				roles.Summary = rc
			default:
				roles.Judge = rc
			}
			_, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "anthropic", Model: "claude-opus-5-5", Roles: roles})
			if code(err) != "validation" || !strings.Contains(err.Error(), "roles."+role) || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
				t.Fatalf("Apply = %v, want validation naming roles.%s and OPENAI_API_KEY", err, role)
			}
			if strings.Contains(err.Error(), envKey) || strings.Contains(err.Error(), evil.URL) {
				t.Fatalf("the message leaks: %v", err)
			}
			if builds.Load() != 0 || hitsEvil.Load() != 0 {
				t.Fatalf("builds %d, requests to the new host %d; want none", builds.Load(), hitsEvil.Load())
			}
			if env.get("OPENAI_API_KEY") != envKey {
				t.Fatal("the operator's variable changed")
			}
		})
	}
}

// An unchanged binding with an absent key keeps working with the operator's
// key; a role on another provider at its default endpoint reads its
// standard variable as before.
func TestPutUnchangedBindingWithEnvKeyWorks(t *testing.T) {
	a, hitsA, authA := hostCounter(t)
	cfg := config.Config{ModelProvider: "openai", ModelFlavor: "openai", ModelBaseURL: a.URL + "/v1", Model: "gpt-5", ModelUnverified: "on"}
	m, _, _ := adapterPicker(t, cfg, map[string]string{"OPENAI_API_KEY": envKey}, ModelEnv{})
	if _, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "openai", BaseURL: a.URL + "/v1", Model: "gpt-5.1"}); err != nil {
		t.Fatal(err)
	}
	if hitsA.Load() != 1 || authA.Load() != "Bearer "+envKey {
		t.Fatalf("hits %d auth %v", hitsA.Load(), authA.Load())
	}

	cfg = config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5", ModelUnverified: "on", SummaryProvider: "openai", SummaryModel: "gpt-5"}
	m, _, builds := adapterPicker(t, cfg, map[string]string{"OPENAI_API_KEY": envKey, "ANTHROPIC_API_KEY": "sk-ant"}, ModelEnv{})
	if _, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "anthropic", Model: "claude-sonnet-5",
		Roles: &httpapi.ModelRoleChoices{Summary: &httpapi.ModelRoleChoice{Provider: "openai", Model: "gpt-5.1"}}}); err != nil || builds.Load() != 1 {
		t.Fatalf("Apply = %v, builds %d", err, builds.Load())
	}
}

// A standard key variable goes only to the provider's default endpoint: a
// role on another provider with a base URL is refused while that variable is
// set, even when the endpoint did not change.
func TestPutRoleWithBaseURLAndEnvKeyIsRefusedUnchanged(t *testing.T) {
	b, hitsB, _ := hostCounter(t)
	cfg := config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5", ModelUnverified: "on",
		SummaryProvider: "openai", SummaryModel: "gpt-5", SummaryBaseURL: b.URL + "/v1"}
	m, _, builds := adapterPicker(t, cfg, map[string]string{"OPENAI_API_KEY": envKey, "ANTHROPIC_API_KEY": "sk-ant"}, ModelEnv{})
	_, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "anthropic", Model: "claude-sonnet-5",
		Roles: &httpapi.ModelRoleChoices{Summary: &httpapi.ModelRoleChoice{Provider: "openai", Model: "gpt-5.1", BaseURL: b.URL + "/v1"}}})
	if code(err) != "validation" || !strings.Contains(err.Error(), "roles.summary.base_url") || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Fatalf("Apply = %v", err)
	}
	if builds.Load() != 0 || hitsB.Load() != 0 {
		t.Fatalf("builds %d, requests %d; want none", builds.Load(), hitsB.Load())
	}
}

// B1-1: a first PUT binds the new host with a throwaway key; a second PUT on
// the same binding with an empty key would clear it and let the adapter fall
// back to the operator's standard variable. The second PUT is key_required
// and the host receives nothing beyond the first PUT's probe, which carried
// the throwaway key.
func TestPutTwoStepEmptyKeyIsKeyRequired(t *testing.T) {
	for _, tc := range []struct {
		name, env string
		cfg       config.Config
		c         httpapi.ModelChoice
	}{
		{"openai", "OPENAI_API_KEY", config.Config{ModelProvider: "openai", ModelFlavor: "openai", Model: "gpt-5", ModelUnverified: "on"},
			httpapi.ModelChoice{Provider: "openai", Flavor: "openai", Model: "gpt-5"}},
		{"groq", "GROQ_API_KEY", config.Config{ModelProvider: "openai", ModelFlavor: "groq", Model: "openai/gpt-oss-120b", ModelUnverified: "on"},
			httpapi.ModelChoice{Provider: "openai", Flavor: "groq", Model: "openai/gpt-oss-120b"}},
		{"gemini", "GEMINI_API_KEY", config.Config{ModelProvider: "gemini", Model: "gemini-3-pro", ModelUnverified: "on"},
			httpapi.ModelChoice{Provider: "gemini", Model: "gemini-3-pro"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evil, hitsEvil, authEvil := hostCounter(t)
			m, env, _ := adapterPicker(t, tc.cfg, map[string]string{tc.env: envKey}, ModelEnv{})
			first := tc.c
			first.BaseURL, first.Key = evil.URL+"/v1", ptr("junk")
			if _, err := m.Apply(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			if hitsEvil.Load() != 1 || authEvil.Load() != "Bearer junk" {
				t.Fatalf("first PUT: hits %d auth %v", hitsEvil.Load(), authEvil.Load())
			}
			hitsEvil.Store(0)
			second := first
			second.Key = ptr(" ")
			if _, err := m.Apply(context.Background(), second); code(err) != "key_required" {
				t.Fatalf("second PUT = %v, want key_required", err)
			}
			if hitsEvil.Load() != 0 {
				t.Fatalf("the new host got %d requests after the empty key", hitsEvil.Load())
			}
			if env.get(tc.env) != envKey || env.get(config.EnvModelAPIKey) != "junk" {
				t.Fatal("the environment changed on a refusal")
			}
		})
	}
}

// B1-2: gemini on Vertex signs with Google application default credentials,
// so no base URL typed in the picker is accepted, on the main role or any
// other.
func TestPutGeminiVertexBaseURLIsRefused(t *testing.T) {
	evil, hitsEvil, _ := hostCounter(t)
	for name, tc := range map[string]struct {
		cfg   config.Config
		c     httpapi.ModelChoice
		field string
	}{
		"main": {config.Config{ModelProvider: "gemini", Model: "gemini-3-pro", GeminiBackend: "vertex", ModelUnverified: "on"},
			httpapi.ModelChoice{Provider: "gemini", Model: "gemini-3-pro", BaseURL: evil.URL + "/v1", Key: ptr("")}, "base_url"},
		"judge": {config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5", GeminiBackend: "vertex", ModelUnverified: "on"},
			httpapi.ModelChoice{Provider: "anthropic", Model: "claude-opus-5-5",
				Roles: &httpapi.ModelRoleChoices{Judge: &httpapi.ModelRoleChoice{Provider: "gemini", Model: "gemini-3-pro", BaseURL: evil.URL + "/v1"}}},
			"roles.judge.base_url"},
	} {
		t.Run(name, func(t *testing.T) {
			m, _, builds := adapterPicker(t, tc.cfg, map[string]string{"ANTHROPIC_API_KEY": "sk-ant"}, ModelEnv{})
			_, err := m.Apply(context.Background(), tc.c)
			want := tc.field + ": gemini on vertex signs with Google credentials; set it in the environment"
			if code(err) != "validation" || !strings.Contains(err.Error(), want) {
				t.Fatalf("Apply = %v, want validation %q", err, want)
			}
			if builds.Load() != 0 || hitsEvil.Load() != 0 {
				t.Fatalf("builds %d, requests %d; want none", builds.Load(), hitsEvil.Load())
			}
		})
	}
}

// A key the picker set for Anthropic does not stay in the environment after a
// switch to a provider no role of which reads it; an operator's does.
func TestPickerKeyLeftoverIsRemovedOperatorKeyKept(t *testing.T) {
	a, _, _ := hostCounter(t)
	cfg := config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5"}
	to := httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/v1", Model: "qwen3:32b", Key: ptr("sk-vllm"),
		Roles: &httpapi.ModelRoleChoices{}}

	// Picker-set (model.env's key at start): removed.
	m, env, _ := adapterPicker(t, cfg, map[string]string{"ANTHROPIC_API_KEY": "sk-picked"}, ModelEnv{KeyVar: "ANTHROPIC_API_KEY", Key: "sk-picked"})
	m.cfg.ModelUnverified = "on"
	if _, err := m.Apply(context.Background(), to); err != nil {
		t.Fatal(err)
	}
	if _, ok := env.Lookup("ANTHROPIC_API_KEY"); ok {
		t.Fatal("the picker's Anthropic key stayed after the switch")
	}
	if env.get(config.EnvModelAPIKey) != "sk-vllm" {
		t.Fatal("the new key is not set")
	}

	// Operator-set: kept.
	m, env, _ = adapterPicker(t, cfg, map[string]string{"ANTHROPIC_API_KEY": "sk-operator"}, ModelEnv{})
	m.cfg.ModelUnverified = "on"
	if _, err := m.Apply(context.Background(), to); err != nil {
		t.Fatal(err)
	}
	if env.get("ANTHROPIC_API_KEY") != "sk-operator" {
		t.Fatal("the operator's Anthropic key was removed")
	}

	// A picker key a role still reads stays: the summary role on Anthropic.
	m, env, _ = adapterPicker(t, cfg, map[string]string{"ANTHROPIC_API_KEY": "sk-picked"}, ModelEnv{KeyVar: "ANTHROPIC_API_KEY", Key: "sk-picked"})
	m.cfg.ModelUnverified = "on"
	keep := to
	keep.Roles = &httpapi.ModelRoleChoices{Summary: &httpapi.ModelRoleChoice{Provider: "anthropic", Model: "claude-haiku-5-5"}}
	if _, err := m.Apply(context.Background(), keep); err != nil {
		t.Fatal(err)
	}
	if env.get("ANTHROPIC_API_KEY") != "sk-picked" {
		t.Fatal("a key the summary role reads was removed")
	}
}

// A GET does not wait behind a PUT whose build is still running.
func TestPanelReturnsWhileApplyBuilds(t *testing.T) {
	a, _, _ := hostCounter(t)
	p := newPicker(t, a.URL+"/v1")
	entered, release := make(chan struct{}), make(chan struct{})
	build := p.m.o.Build
	p.m.o.Build = func(ctx context.Context, cfg config.Config) (func(), func(), error) {
		close(entered)
		<-release
		return build(ctx, cfg)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: a.URL + "/v1", Model: "qwen3:32b"})
		done <- err
	}()
	<-entered
	got := make(chan httpapi.ModelPanel, 1)
	go func() { got <- p.m.Panel() }()
	select {
	case panel := <-got:
		if len(panel.Roles) == 0 || panel.Roles[0].Model != "qwen3:32b" {
			t.Fatalf("panel = %+v", panel)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Panel waited behind Apply")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
