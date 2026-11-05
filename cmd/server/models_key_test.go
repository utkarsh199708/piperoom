package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/quickstart"
	"piperoom/internal/store/memstore"
	"piperoom/internal/wiring"
)

// keyVarsMP1 are the variables the picker's key used to be put in with
// os.Setenv (V18 MP1), and the ones an adapter could read it from.
var keyVarsMP1 = []string{config.EnvModelAPIKey, config.EnvModelAPIKeyFile, "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN",
	"OPENAI_API_KEY", "GEMINI_API_KEY"}

// clearKeyVars removes the key variables for the test (restored after it).
func clearKeyVars(t *testing.T) {
	t.Helper()
	for _, v := range keyVarsMP1 {
		t.Setenv(v, "")
		if err := os.Unsetenv(v); err != nil {
			t.Fatal(err)
		}
	}
}

// assertKeyVarsUnset fails when any key variable is in the process
// environment.
func assertKeyVarsUnset(t *testing.T, when string) {
	t.Helper()
	for _, v := range keyVarsMP1 {
		if val, ok := os.LookupEnv(v); ok {
			t.Fatalf("%s: %s is in the process environment (%d bytes)", when, v, len(val))
		}
	}
}

// probeHost is an httptest vllm host that answers the start probe and
// records the Authorization header of each request.
func probeHost(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"c","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"1","type":"function","function":{"name":"piperoom_start_probe","arguments":"{\"status\":\"ready\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":%d,"completion_tokens":3}}`, len(b)/3)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), auths...)
	}
}

// V18 MP1: the key model.env stores and the key of each PUT reach the
// adapters through the picker's overlay (wiring.WithGetenv), through the real
// wiring, and never enter the process environment.
func TestPickerKeyReachesAdapterNotEnvironment(t *testing.T) {
	clearKeyVars(t)
	a, authsA := probeHost(t)
	b, authsB := probeHost(t)
	const storedKey, putKey = "sk-stored-model-env-1111", "sk-put-key-2222"

	// A start with model.env: the stored key goes to the start endpoint.
	dir := t.TempDir()
	cfg := config.Config{ModelProvider: "openai", ModelFlavor: "vllm", ModelBaseURL: a.URL + "/v1", Model: "qwen3:32b", ModelUnverified: "on"}
	if err := quickstart.WriteModelEnv(dir, quickstart.ModelEnv{Vars: map[string]string{}, KeyVar: config.EnvModelAPIKey, Key: storedKey}); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := quickstart.ReadModelEnv(dir)
	if err != nil || !ok {
		t.Fatalf("ReadModelEnv = %v, %v", ok, err)
	}
	live := &liveModel{startKeys: quickstart.NewKeys(nil, stored)}
	in := wireAgentInput{cfg: cfg, log: discardLog(), spendStore: memstore.New(), usage: &usageRelay{log: discardLog()},
		hook: &roomClosedHook{}, live: live}
	if _, err := wireLive(context.Background(), live, in, nil, wiring.NewAgentEndpointWithEnv); err != nil {
		t.Fatal(err)
	}
	if !live.ready() {
		t.Fatal("the start endpoint did not build on the stored key")
	}
	if got := authsA(); len(got) != 1 || got[0] != "Bearer "+storedKey {
		t.Fatalf("start probe Authorization = %q, want the stored key", got)
	}
	assertKeyVarsUnset(t, "after a start with model.env")

	// A PUT with a new key on a new host: the adapter gets the PUT's key.
	m := quickstart.NewModels(quickstart.ModelsOptions{
		Mode: httpapi.ModelModeStandalone, ProjectDir: dir, Config: cfg, Stored: stored, Ready: true, SoloUser: "solo",
		Build: live.build,
	})
	panel, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: b.URL + "/v1", Model: "qwen3:32b", Key: ptrString(putKey)})
	if err != nil {
		t.Fatal(err)
	}
	if got := authsB(); len(got) != 1 || got[0] != "Bearer "+putKey {
		t.Fatalf("PUT probe Authorization = %q, want the PUT's key", got)
	}
	if panel.State != httpapi.ModelStateReady || !panel.Roles[0].KeySet {
		t.Fatalf("panel = %+v; want ready with the key set", panel)
	}
	assertKeyVarsUnset(t, "after a successful PUT")

	// A failed PUT (the host refuses) keeps the old key: a model change on
	// the same binding still sends the PUT's key.
	if _, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: "http://127.0.0.1:1/v1", Model: "qwen3:32b", Key: ptrString("sk-refused-3333")}); err == nil {
		t.Fatal("a PUT to a closed port built")
	}
	if _, err := m.Apply(context.Background(), httpapi.ModelChoice{Provider: "openai", Flavor: "vllm", BaseURL: b.URL + "/v1", Model: "qwen3:32b"}); err != nil {
		t.Fatal(err)
	}
	if got := authsB(); len(got) != 2 || got[1] != "Bearer "+putKey {
		t.Fatalf("after a failed PUT, Authorization = %q, want the last committed key", got)
	}
	assertKeyVarsUnset(t, "after a failed PUT")
}

// With the key lookup override the Anthropic client takes its key from the
// overlay only: a stored Anthropic key starts the endpoint with
// ANTHROPIC_API_KEY absent from the process environment.
func TestStoredAnthropicKeyStartsWithoutTheEnvironment(t *testing.T) {
	clearKeyVars(t)
	live := &liveModel{startKeys: quickstart.NewKeys(nil, quickstart.ModelEnv{KeyVar: "ANTHROPIC_API_KEY", Key: "sk-ant-stored-4444"})}
	in := wireAgentInput{cfg: config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5"}, log: discardLog(),
		spendStore: memstore.New(), usage: &usageRelay{log: discardLog()}, hook: &roomClosedHook{}, live: live}
	if _, err := wireLive(context.Background(), live, in, nil, wiring.NewAgentEndpointWithEnv); err != nil {
		t.Fatal(err)
	}
	if !live.ready() {
		t.Fatal("the stored Anthropic key did not reach the client")
	}
	assertKeyVarsUnset(t, "after an Anthropic start with model.env")

	// No key anywhere: the endpoint does not start (no model state).
	empty := &liveModel{startKeys: quickstart.NewKeys(nil, quickstart.ModelEnv{})}
	in.live = empty
	if _, err := wireLive(context.Background(), empty, in, nil, wiring.NewAgentEndpointWithEnv); err != nil {
		t.Fatal(err)
	}
	if empty.ready() {
		t.Fatal("an Anthropic endpoint started with no key")
	}
}

func ptrString(s string) *string { return &s }
