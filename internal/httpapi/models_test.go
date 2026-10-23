package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/agent/llm"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

const testModelKey = "sk-test-0123456789abcdef"

// fakeModels is an httpapi.Models whose panel comes from BuildModelPanel.
type fakeModels struct {
	editable bool
	solo     string
	in       httpapi.PanelInput

	mu      sync.Mutex
	applied []httpapi.ModelChoice
	err     error
}

func (f *fakeModels) Panel() httpapi.ModelPanel { return httpapi.BuildModelPanel(f.in) }
func (f *fakeModels) Editable() bool            { return f.editable }
func (f *fakeModels) SoloUser() string          { return f.solo }
func (f *fakeModels) Apply(_ context.Context, c httpapi.ModelChoice) (httpapi.ModelPanel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, c)
	if f.err != nil {
		return httpapi.ModelPanel{}, f.err
	}
	return httpapi.BuildModelPanel(f.in), nil
}

func (f *fakeModels) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applied)
}

// gatewayConfig is an openai generic endpoint whose path carries an account.
func gatewayConfig() config.Config {
	c := config.Config{ModelProvider: "openai", ModelFlavor: "generic", Model: "acme-large",
		ModelBaseURL: "https://gw.example.com/acct-4711/v1", ModelUnverified: "on", AWSRegion: "eu-west-1"}
	return c
}

func newModelsServer(t *testing.T, m *fakeModels) (http.Handler, *syncBuf, *memstore.Store) {
	t.Helper()
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, Logger: log})
	h := httpapi.New(svc, st, log, httpapi.WithModels(m))
	t.Cleanup(h.Close)
	return h, logs, st
}

// putModels sends PUT /api/models as a same-origin browser on loopback would;
// mod changes the request first.
func putModels(h http.Handler, body string, mod func(*http.Request)) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1:8090/api/models", strings.NewReader(body))
	r.RemoteAddr = "127.0.0.1:50000"
	r.Header.Set("Origin", "http://127.0.0.1:8090")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-User", "priya")
	if mod != nil {
		mod(r)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func modelsErrCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("body %q: %v", w.Body.String(), err)
	}
	return e.Error.Code
}

func TestGetModelsNeverShowsKeyOrFullBaseURL(t *testing.T) {
	m := &fakeModels{in: httpapi.PanelInput{Mode: httpapi.ModelModeTeam, State: httpapi.ModelStateReady, Config: gatewayConfig(),
		Prices: agent.Prices{"openai:acme-large": {Input: 1, Output: 2}},
		Supported: []llm.SupportedModel{{Provider: "openai", Flavor: "groq", Model: "openai/gpt-oss-120b", Status: llm.GateSupported,
			Baseline: "evals/baseline/x.json", GatedAt: "2026-11-02", Sunset: "2027-03-01"}},
		Now:    time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC),
		Getenv: func(n string) string { return map[string]string{config.EnvModelAPIKey: testModelKey}[n] }}}
	h, _, _ := newModelsServer(t, m)
	for _, user := range []string{"priya", "kabir"} {
		r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8090/api/models", nil)
		r.Header.Set("X-User", user)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("GET as %s = %d %s", user, w.Code, w.Body)
		}
		body := w.Body.String()
		for _, leak := range []string{testModelKey, "acct-4711", "https://gw.example.com"} {
			if strings.Contains(body, leak) {
				t.Fatalf("GET /api/models leaks %q: %s", leak, body)
			}
		}
		var p httpapi.ModelPanel
		if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
			t.Fatal(err)
		}
		main := p.Roles[0]
		if p.Mode != "team" || p.State != "ready" || main.Role != "main" || main.BaseURLHost != "gw.example.com" ||
			main.Status != llm.GateUnverified || !main.KeySet || main.Price == nil || main.Price.Output != 2 || main.Flavor != "generic" {
			t.Fatalf("panel = %+v", p)
		}
		if len(p.Supported) != 1 || p.Supported[0].Sunset != "2027-03-01" || p.Supported[0].PassRate != nil {
			t.Fatalf("supported = %+v", p.Supported)
		}
	}
}

func TestBuildModelPanelClaudeIsReference(t *testing.T) {
	p := httpapi.BuildModelPanel(httpapi.PanelInput{Mode: "team", State: "ready",
		Config: config.Config{ModelProvider: "bedrock", Model: "anthropic.claude-x", AWSRegion: "eu-central-1"}})
	if len(p.Roles) < 2 || p.Roles[0].Status != httpapi.ModelStatusReference || !p.Roles[0].PromptCache ||
		p.Roles[0].Region != "eu-central-1" || !p.Roles[0].KeySet || p.Roles[0].Price != nil {
		t.Fatalf("panel = %+v", p)
	}
	b, _ := json.Marshal(p.Roles[0])
	if !strings.Contains(string(b), `"price":null`) || strings.Contains(string(b), "base_url_host") {
		t.Fatalf("role JSON = %s", b)
	}
}

func TestPutModelsTeamServerIs404(t *testing.T) {
	m := &fakeModels{editable: false, solo: "priya"}
	h, _, _ := newModelsServer(t, m)
	w := putModels(h, `{"provider":"anthropic","model":"claude-opus-5-5"}`, nil)
	if w.Code != http.StatusNotFound || modelsErrCode(t, w) != "not_found" || m.calls() != 0 {
		t.Fatalf("PUT on a team server = %d %s", w.Code, w.Body)
	}
}

func TestPutModelsRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mod    func(*http.Request)
		status int
		code   string
	}{
		{"non-loopback peer", func(r *http.Request) { r.RemoteAddr = "10.0.0.7:4000" }, 403, "not_allowed"},
		{"forwarded-for does not help", func(r *http.Request) {
			r.RemoteAddr = "192.168.1.9:4000"
			r.Header.Set("X-Forwarded-For", "127.0.0.1")
		}, 403, "not_allowed"},
		{"missing Origin", func(r *http.Request) { r.Header.Del("Origin") }, 403, "origin"},
		{"foreign Origin", func(r *http.Request) { r.Header.Set("Origin", "http://evil.example") }, 403, "origin"},
		{"other port", func(r *http.Request) { r.Header.Set("Origin", "http://127.0.0.1:9999") }, 403, "origin"},
		{"https origin on http", func(r *http.Request) { r.Header.Set("Origin", "https://127.0.0.1:8090") }, 403, "origin"},
		{"cross-site", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }, 403, "origin"},
		{"same-site", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-site") }, 403, "origin"},
		{"form post", func(r *http.Request) { r.Header.Set("Content-Type", "application/x-www-form-urlencoded") }, 403, "origin"},
		{"text/plain", func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, 403, "origin"},
		{"not the solo user", func(r *http.Request) { r.Header.Set("X-User", "kabir") }, 403, "not_allowed"},
		{"no login", func(r *http.Request) { r.Header.Del("X-User") }, 401, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &fakeModels{editable: true, solo: "priya"}
			h, _, _ := newModelsServer(t, m)
			w := putModels(h, `{"provider":"anthropic","model":"claude-opus-5-5","key":"`+testModelKey+`"}`, tc.mod)
			if w.Code != tc.status || (tc.code != "" && modelsErrCode(t, w) != tc.code) {
				t.Fatalf("= %d %s, want %d %s", w.Code, w.Body, tc.status, tc.code)
			}
			if m.calls() != 0 {
				t.Fatal("a refused request reached Apply")
			}
			if strings.Contains(w.Body.String(), testModelKey) {
				t.Fatal("the key is echoed")
			}
		})
	}
}

func TestPutModelsAppliesAndNeverLogsKey(t *testing.T) {
	m := &fakeModels{editable: true, solo: "priya", in: httpapi.PanelInput{Mode: "standalone", State: "ready",
		Config: config.Config{ModelProvider: "anthropic", Model: "claude-opus-5-5"}}}
	h, logs, st := newModelsServer(t, m)
	w := putModels(h, `{"provider":"anthropic","model":"claude-opus-5-5","key":"`+testModelKey+`"}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT = %d %s", w.Code, w.Body)
	}
	if m.calls() != 1 || m.applied[0].Key == nil || *m.applied[0].Key != testModelKey {
		t.Fatalf("applied = %+v", m.applied)
	}
	// A refusal from Apply keeps its code; an internal error never shows err.
	m.err = &room.Error{Status: 400, Code: "key_required", Msg: "send key"}
	if w := putModels(h, `{"provider":"openai","model":"x"}`, nil); w.Code != 400 || modelsErrCode(t, w) != "key_required" {
		t.Fatalf("PUT = %d %s", w.Code, w.Body)
	}
	m.err = io.ErrUnexpectedEOF
	if w := putModels(h, `{"provider":"openai","model":"x","key":"`+testModelKey+`"}`, nil); w.Code != 500 {
		t.Fatalf("PUT = %d %s", w.Code, w.Body)
	}
	if strings.Contains(logs.String(), testModelKey) || strings.Contains(logs.String(), "path=/api/models") {
		t.Fatalf("the log has the key or the route:\n%s", logs.String())
	}
	rooms, err := st.ListRooms(context.Background())
	if err != nil || len(rooms) != 0 {
		t.Fatalf("rooms = %v, %v; the route writes no event", rooms, err)
	}
}
