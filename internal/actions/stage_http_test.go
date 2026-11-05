package actions

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/events"
)

// tripwire is a production endpoint: any request fails the test (TC5).
func tripwire(t *testing.T, what string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("Stage called production %s: %s %s", what, r.Method, r.URL.Path)
		w.WriteHeader(http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// connectStub is a staging Kafka Connect over HTTP with one connector.
type connectStub struct {
	mu      sync.Mutex
	cfg     map[string]string
	failed  string // a task trace: task 0 reports FAILED with it after the PATCH
	patched bool
	methods []string
	puts    []map[string]string
}

func (s *connectStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.methods = append(s.methods, r.Method+" "+r.URL.Path)
	body, _ := io.ReadAll(r.Body)
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/connectors":
		w.Write([]byte(`["orders-src"]`))
	case r.Method == http.MethodGet && r.URL.Path == "/connectors/orders-src/config":
		json.NewEncoder(w).Encode(s.cfg)
	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/config/validate"):
		w.Write([]byte(`{"error_count":0,"configs":[]}`))
	case r.Method == http.MethodPatch && r.URL.Path == "/connectors/orders-src/config":
		var ch map[string]string
		json.Unmarshal(body, &ch)
		maps.Copy(s.cfg, ch)
		s.patched = true
		w.Write([]byte(`{}`))
	case r.Method == http.MethodGet && r.URL.Path == "/connectors/orders-src/status":
		task := map[string]any{"id": 0, "state": "RUNNING", "worker_id": "w1"}
		if s.patched && s.failed != "" {
			task["state"], task["trace"] = "FAILED", s.failed
		}
		json.NewEncoder(w).Encode(map[string]any{"name": "orders-src", "type": "source",
			"connector": map[string]any{"state": "RUNNING", "worker_id": "w1"}, "tasks": []any{task}})
	case r.Method == http.MethodPut && r.URL.Path == "/connectors/orders-src/config":
		var cfg map[string]string
		json.Unmarshal(body, &cfg)
		s.puts = append(s.puts, cfg)
		s.cfg = maps.Clone(cfg)
		w.Write([]byte(`{}`))
	default:
		http.NotFound(w, r)
	}
}

// TC1, TC2, TC5 over HTTP with the real clients: the staging connector gets
// the change, its health decides, and the last request to staging is the PUT
// of the original config; production is never called.
func TestStageConfigOverHTTP(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failed  string
		outcome string
	}{
		{"running", "", events.StageHealthy},
		{"task failed", "org.apache.kafka.connect.errors.DataException: bad decimal\n\tat x.y(Z.java:1)", events.StageFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &connectStub{cfg: origConfig(), failed: tc.failed}
			stg := httptest.NewServer(stub)
			defer stg.Close()
			prodC, prodR := tripwire(t, "Connect"), tripwire(t, "Registry")
			d := fastStage(Deps{
				Connect:  connect.New(prodC.URL, prodC.Client()),
				Registry: schemareg.New(prodR.URL, prodR.Client()),
				Staging:  &Staging{Connect: connect.New(stg.URL, stg.Client())},
			})
			got, err := (&updateConnectorConfig{d: d}).Stage(stageCtx(), json.RawMessage(cfgRaw))
			if err != nil || got.Outcome() != tc.outcome || got.Leftover != "" {
				t.Fatalf("result %+v, %v", got, err)
			}
			if tc.failed != "" && !strings.Contains(got.Detail, "task 0 FAILED: org.apache.kafka.connect.errors.DataException: bad decimal") {
				t.Fatalf("detail %q", got.Detail)
			}
			if strings.Contains(got.Detail+got.Leftover+got.Target, stg.URL) {
				t.Fatal("the result names the staging URL")
			}
			stub.mu.Lock()
			defer stub.mu.Unlock()
			if len(stub.puts) != 1 || !maps.Equal(stub.puts[0], origConfig()) {
				t.Fatalf("PUTs %v, want one with the original config", stub.puts)
			}
			if last := stub.methods[len(stub.methods)-1]; last != "PUT /connectors/orders-src/config" {
				t.Fatalf("last request %q; all %v", last, stub.methods)
			}
		})
	}
}

// TC4 over HTTP: the staging Registry's compatibility check says no, so no
// POST to /subjects/{s}/versions is made.
func TestStageRegisterOverHTTP(t *testing.T) {
	for _, tc := range []struct {
		name       string
		compatible bool
		outcome    string
	}{
		{"incompatible", false, events.StageFailed},
		{"compatible", true, events.StageHealthy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var registered bool
			versions := []int{1}
			stg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/subjects":
					w.Write([]byte(`["pg.public.orders-value"]`))
				case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/compatibility/subjects/pg.public.orders-value/versions/latest"):
					json.NewEncoder(w).Encode(map[string]any{"is_compatible": tc.compatible, "messages": []string{}})
				case r.Method == http.MethodGet && r.URL.Path == "/config/pg.public.orders-value":
					w.Write([]byte(`{"compatibilityLevel":"BACKWARD"}`))
				case r.Method == http.MethodGet && r.URL.Path == "/subjects/pg.public.orders-value/versions":
					json.NewEncoder(w).Encode(versions)
				case r.Method == http.MethodPost && r.URL.Path == "/subjects/pg.public.orders-value/versions":
					registered = true
					versions = append(versions, 2)
					w.Write([]byte(`{"id":99}`))
				default:
					http.NotFound(w, r)
				}
			}))
			defer stg.Close()
			prodC, prodR := tripwire(t, "Connect"), tripwire(t, "Registry")
			d := Deps{
				Connect:  connect.New(prodC.URL, prodC.Client()),
				Registry: schemareg.New(prodR.URL, prodR.Client()),
				Staging:  &Staging{Registry: schemareg.New(stg.URL, stg.Client())},
			}
			start := time.Now()
			got, err := (&registerSchema{d: d}).Stage(stageCtx(), json.RawMessage(regRaw))
			if err != nil || got.Outcome() != tc.outcome {
				t.Fatalf("result %+v, %v", got, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if registered != tc.compatible {
				t.Fatalf("registered = %v", registered)
			}
			if tc.compatible && (!strings.Contains(got.Detail, "schema id 99 (version 2)") || !strings.Contains(got.Leftover, "keeps version 2")) {
				t.Fatalf("result %+v", got)
			}
			if time.Since(start) > 5*time.Second {
				t.Fatal("a schema stage waited for an observe window")
			}
		})
	}
}
