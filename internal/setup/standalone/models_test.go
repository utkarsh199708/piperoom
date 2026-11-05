package standalone

// The V7 models API on the standalone server (KAN-525): GET and PUT are
// mounted, the key in .piperoom/model.env reaches the adapters through the
// key overlay, and no key ever enters the process or a child's environment.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/quickstart"
)

// modelKeyVars are the variables a model key could be read from, and the ones
// a child process (setup/ops's docker children inherit the environment) would
// see if one were put there with os.Setenv.
var modelKeyVars = []string{config.EnvModelAPIKey, config.EnvModelAPIKeyFile, "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN",
	"OPENAI_API_KEY", "GEMINI_API_KEY"}

// childEnvProbe marks the child process of assertKeyNotInChildEnv.
const childEnvProbe = "PIPEROOM_MODELS_ENV_PROBE"

// clearModelKeyVars removes the key variables for the test (restored after it).
func clearModelKeyVars(t *testing.T) {
	t.Helper()
	for _, v := range modelKeyVars {
		t.Setenv(v, "")
		if err := os.Unsetenv(v); err != nil {
			t.Fatal(err)
		}
	}
}

// assertModelKeyVarsUnset fails when any key variable is in the process
// environment.
func assertModelKeyVarsUnset(t *testing.T, when string) {
	t.Helper()
	for _, v := range modelKeyVars {
		if val, ok := os.LookupEnv(v); ok {
			t.Fatalf("%s: %s is in the process environment (%d bytes)", when, v, len(val))
		}
	}
}

// assertKeyNotInChildEnv starts the test binary as a child and fails when a
// key variable or a secret value is in the environment it inherits: the check
// the ticket asks for, because setup/ops's docker children inherit exactly
// this.
func assertKeyNotInChildEnv(t *testing.T, secrets ...string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestModelKeyChildEnvHelper$")
	cmd.Env = append(os.Environ(), childEnvProbe+"=1")
	var probeInput bytes.Buffer
	if err := json.NewEncoder(&probeInput).Encode(secrets); err != nil {
		t.Fatalf("encode the child env probe input: %v", err)
	}
	cmd.Stdin = &probeInput
	if _, err := cmd.Output(); err != nil {
		t.Fatalf("run the child env probe: %v", err)
	}
}

// TestModelKeyChildEnvHelper is the child of assertKeyNotInChildEnv: it reports
// only a generic failure if a key or secret is present. In a normal test run
// it passes without doing anything: a skip here would fail CI's fail-on-skips
// gate (it is not a stack skip).
func TestModelKeyChildEnvHelper(t *testing.T) {
	if os.Getenv(childEnvProbe) != "1" {
		return
	}
	var secrets []string
	if err := json.NewDecoder(os.Stdin).Decode(&secrets); err != nil {
		t.Fatal("read the child env probe input")
	}
	for _, name := range modelKeyVars {
		if _, ok := os.LookupEnv(name); ok {
			t.Fatal("the child process inherited a model key variable")
		}
	}
	for _, kv := range os.Environ() {
		_, value, _ := strings.Cut(kv, "=")
		for _, secret := range secrets {
			if secret != "" && strings.Contains(value, secret) {
				t.Fatal("the child process inherited a secret value")
			}
		}
	}
}

// modelsProbeHost is an httptest vllm host that answers the start probe and
// records the Authorization header of each request.
func modelsProbeHost(t *testing.T) (*httptest.Server, func() []string) {
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

// vllmConfig is a standalone start whose model is a local vllm host: the
// start probe reaches the host, so the test sees the key the adapter uses.
func vllmConfig(baseURL string) config.Config {
	cfg := testConfig()
	cfg.ModelProvider = "openai"
	cfg.ModelFlavor = "vllm"
	cfg.ModelBaseURL = baseURL + "/v1"
	cfg.Model = "qwen3:32b"
	cfg.ModelUnverified = "on"
	return cfg
}

// startModelServer runs the standalone server with the real model host (no
// injected agent) and returns its URL and its log. The server stops with the
// test.
func startModelServer(t *testing.T, dir string, cfg config.Config) (string, *lockedBuffer) {
	t.Helper()
	out := newLockedBuffer()
	logs := newLockedBuffer()
	done, cancel := startRun(t, Options{
		Config:     cfg,
		ProjectDir: dir,
		Stdout:     out,
		Log:        slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			stopRun(t, done, cancel)
		}
	})
	select {
	case u := <-out.urls:
		return u, logs
	case err := <-done:
		stopped = true
		t.Fatalf("Run: %v\noutput:\n%s", err, out.String())
	case <-time.After(10 * time.Second):
		stopRun(t, done, cancel)
		stopped = true
		t.Fatalf("no URL printed within 10 s; output:\n%s", out.String())
	}
	return "", logs
}

// getModels reads GET /api/models as the solo user, returning the status, the
// panel and the raw body (to prove the key is not echoed).
func getModels(t *testing.T, base string) (int, httpapi.ModelPanel, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"api/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-User", "alice")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/models: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var p httpapi.ModelPanel
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("GET /api/models body: %v: %s", err, body)
	}
	return resp.StatusCode, p, string(body)
}

// putModels sends PUT /api/models as the solo user's same-origin page would.
func putModels(t *testing.T, base, body string) (int, httpapi.ModelPanel, string) {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPut, base+"api/models", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://"+u.Host)
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("X-User", "alice")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /api/models: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var p httpapi.ModelPanel
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatalf("PUT /api/models body: %v: %s", err, raw)
		}
	}
	return resp.StatusCode, p, string(raw)
}

// The ticket's done-when: on a standalone server GET serves the panel and PUT
// works on loopback, and the stored key reaches the adapter without entering
// any process environment.
func TestModelsStoredKeyReachesAdapter(t *testing.T) {
	clearModelKeyVars(t)
	a, authsA := modelsProbeHost(t)
	b, authsB := modelsProbeHost(t)
	const storedKey, putKey = "sk-stored-model-env-1111", "sk-put-key-2222"

	dir := t.TempDir()
	writeOwners(t, dir, ownersBody("alice", "Orders CDC"))
	if err := quickstart.WriteModelEnv(dir, quickstart.ModelEnv{Vars: map[string]string{}, KeyVar: config.EnvModelAPIKey, Key: storedKey}); err != nil {
		t.Fatal(err)
	}

	base, logs := startModelServer(t, dir, vllmConfig(a.URL))

	// GET: the panel is a standalone ready panel, and the stored key is set
	// without being echoed.
	code, panel, raw := getModels(t, base)
	if code != http.StatusOK {
		t.Fatalf("GET /api/models status %d, want 200: %s", code, raw)
	}
	if panel.Mode != httpapi.ModelModeStandalone {
		t.Fatalf("panel mode %q, want %q", panel.Mode, httpapi.ModelModeStandalone)
	}
	if panel.State != httpapi.ModelStateReady {
		t.Fatalf("panel state %q, want %q", panel.State, httpapi.ModelStateReady)
	}
	if len(panel.Roles) == 0 || !panel.Roles[0].KeySet {
		t.Fatalf("panel roles %+v; want the main role with key_set", panel.Roles)
	}
	if strings.Contains(raw, storedKey) {
		t.Fatal("GET /api/models echoed the stored key")
	}

	// The start endpoint read the stored key through the overlay.
	if got := authsA(); len(got) != 1 || got[0] != "Bearer "+storedKey {
		t.Fatalf("start probe Authorization = %q, want the stored key", got)
	}
	assertModelKeyVarsUnset(t, "after a start with model.env")

	// PUT on a new host with a new key: the adapter gets the PUT's key.
	body := fmt.Sprintf(`{"provider":"openai","flavor":"vllm","base_url":%q,"model":"qwen3:32b","key":%q}`, b.URL+"/v1", putKey)
	code, panel, raw = putModels(t, base, body)
	if code != http.StatusOK {
		t.Fatalf("PUT /api/models status %d, want 200: %s", code, raw)
	}
	if panel.State != httpapi.ModelStateReady || len(panel.Roles) == 0 || !panel.Roles[0].KeySet {
		t.Fatalf("panel after PUT = %+v; want ready with the key set", panel)
	}
	if strings.Contains(raw, putKey) {
		t.Fatal("PUT /api/models echoed the PUT's key")
	}
	if got := authsB(); len(got) != 1 || got[0] != "Bearer "+putKey {
		t.Fatalf("PUT probe Authorization = %q, want the PUT's key", got)
	}
	assertModelKeyVarsUnset(t, "after a successful PUT")

	// Neither key reached the logs or a child process's environment.
	if logs.String() != "" && (strings.Contains(logs.String(), storedKey) || strings.Contains(logs.String(), putKey)) {
		t.Fatal("a model key is in the server's log")
	}
	assertKeyNotInChildEnv(t, storedKey, putKey)
}

// The refusal path when no key is stored: the server starts in the no model
// state, a PUT that changes the binding without a key is 400 key_required and
// sends nothing to the new host, and the same PUT with a key works.
func TestModelsNoStoredKeyRefusesRebind(t *testing.T) {
	clearModelKeyVars(t)
	b, authsB := modelsProbeHost(t)
	const putKey = "sk-first-key-3333"

	dir := t.TempDir()
	writeOwners(t, dir, ownersBody("alice", "Orders CDC"))
	cfg := testConfig()
	cfg.ModelProvider = "anthropic"
	cfg.Model = "claude-opus-5-5"
	cfg.ModelUnverified = "on"

	base, _ := startModelServer(t, dir, cfg)

	code, panel, raw := getModels(t, base)
	if code != http.StatusOK {
		t.Fatalf("GET /api/models status %d, want 200: %s", code, raw)
	}
	if panel.State != httpapi.ModelStateNoModel {
		t.Fatalf("panel state %q, want %q with no key anywhere; roles %+v", panel.State, httpapi.ModelStateNoModel, panel.Roles)
	}
	if len(panel.Roles) == 0 || panel.Roles[0].KeySet {
		t.Fatalf("panel roles %+v; want a main role with key_set false", panel.Roles)
	}

	// New binding, no key: refused before anything leaves for the new host.
	body := fmt.Sprintf(`{"provider":"openai","flavor":"vllm","base_url":%q,"model":"qwen3:32b"}`, b.URL+"/v1")
	code, _, raw = putModels(t, base, body)
	if code != http.StatusBadRequest {
		t.Fatalf("PUT without a key on a new binding: status %d, want 400: %s", code, raw)
	}
	if !strings.Contains(raw, "key_required") {
		t.Fatalf("PUT refusal %q, want key_required", raw)
	}
	if got := authsB(); len(got) != 0 {
		t.Fatalf("a refused PUT sent %d request(s) to the new host", len(got))
	}

	// The same change with a key works and installs the endpoint.
	body = fmt.Sprintf(`{"provider":"openai","flavor":"vllm","base_url":%q,"model":"qwen3:32b","key":%q}`, b.URL+"/v1", putKey)
	code, panel, raw = putModels(t, base, body)
	if code != http.StatusOK {
		t.Fatalf("PUT with a key: status %d, want 200: %s", code, raw)
	}
	if panel.State != httpapi.ModelStateReady || len(panel.Roles) == 0 || !panel.Roles[0].KeySet {
		t.Fatalf("panel after PUT = %+v; want ready with the key set", panel)
	}
	if got := authsB(); len(got) != 1 || got[0] != "Bearer "+putKey {
		t.Fatalf("probe Authorization = %q, want the PUT's key", got)
	}
	assertModelKeyVarsUnset(t, "after the first PUT")
}
