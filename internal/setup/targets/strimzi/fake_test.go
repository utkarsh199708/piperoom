package strimzi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/setup/ops"
)

// fakeAPI is an httptest Kubernetes API server: named objects, RBAC by an
// allowed-path set (anything else is 403), JSON patch (test, add, replace,
// remove) with dryRun=All, and knobs for the failure cases of design §5.
type fakeAPI struct {
	t        *testing.T
	mu       sync.Mutex
	objects  map[string]map[string]any // path -> object
	allowed  map[string]bool           // path prefixes RBAC grants
	rv       int
	logs     map[string]string // pod log path -> text
	requests []string

	// knobs
	status       map[string]int // "METHOD path" -> forced status once
	defaultField map[string]any // a default the "schema" adds to /spec on every write and dry run
	tamperWrite  map[string]any // fields a webhook adds to /spec on a real write only
	retryAfter   int            // first request answers 429 with this Retry-After
}

func newFakeAPI(t *testing.T) *fakeAPI {
	return &fakeAPI{t: t, objects: map[string]map[string]any{}, allowed: map[string]bool{}, logs: map[string]string{}, status: map[string]int{}}
}

func (f *fakeAPI) put(path string, obj map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rv++
	md, _ := obj["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		obj["metadata"] = md
	}
	md["resourceVersion"] = strconv.Itoa(f.rv)
	f.objects[path] = obj
	f.allowed[path] = true
}

// allow grants path (a name the Role lists) without creating the object, so
// a GET answers 404 until put.
func (f *fakeAPI) allow(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowed[path] = true
}

func (f *fakeAPI) get(path string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, _ := json.Marshal(f.objects[path])
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func writeStatus(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "message": msg, "code": code})
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	key := r.Method + " " + path
	f.requests = append(f.requests, key+"?"+r.URL.RawQuery)
	if r.Header.Get("Authorization") != "Bearer test-token" {
		writeStatus(w, 401, "Unauthorized")
		return
	}
	if f.retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(f.retryAfter))
		f.retryAfter = 0
		writeStatus(w, 429, "slow down")
		return
	}
	if code, ok := f.status[key]; ok {
		delete(f.status, key)
		writeStatus(w, code, "forced")
		return
	}
	if text, ok := f.logs[path]; ok {
		_, _ = io.WriteString(w, text)
		return
	}
	if !f.allowed[path] {
		if strings.HasSuffix(path, "/events") && f.allowed["events"] {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": f.objects["events"]["items"]})
			return
		}
		writeStatus(w, 403, "forbidden by the Role")
		return
	}
	obj, ok := f.objects[path]
	if !ok {
		writeStatus(w, 404, "not found")
		return
	}
	switch r.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(obj)
	case http.MethodPatch:
		if r.Header.Get("Content-Type") != "application/json-patch+json" {
			writeStatus(w, 415, "json patch only")
			return
		}
		var ops []PatchOp
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &ops); err != nil {
			writeStatus(w, 400, "bad patch")
			return
		}
		b, _ := json.Marshal(obj)
		var next map[string]any
		_ = json.Unmarshal(b, &next)
		for _, op := range ops {
			if code := applyFakeOp(next, op); code != 0 {
				writeStatus(w, code, "patch failed")
				return
			}
		}
		if spec, ok := next["spec"].(map[string]any); ok {
			for k, v := range f.defaultField {
				if _, set := spec[k]; !set {
					spec[k] = v
				}
			}
		}
		if sd, ok := next["stringData"].(map[string]any); ok {
			data, _ := next["data"].(map[string]any)
			if data == nil {
				data = map[string]any{}
			}
			for k, v := range sd {
				data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
			}
			next["data"] = data
			delete(next, "stringData")
		}
		if r.URL.Query().Get("dryRun") != "All" {
			if spec, ok := next["spec"].(map[string]any); ok {
				for k, v := range f.tamperWrite {
					spec[k] = v
				}
			}
			f.rv++
			next["metadata"].(map[string]any)["resourceVersion"] = strconv.Itoa(f.rv)
			f.objects[path] = next
		}
		_ = json.NewEncoder(w).Encode(next)
	case http.MethodDelete:
		delete(f.objects, path)
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "status": "Success"})
	default:
		writeStatus(w, 405, "no")
	}
}

// applyFakeOp applies one patch op to obj; it returns an HTTP status on
// failure (409 for a failed test, 422 for a bad path).
func applyFakeOp(obj map[string]any, op PatchOp) int {
	parts := strings.Split(strings.TrimPrefix(op.Path, "/"), "/")
	for i := range parts {
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(parts[i], "~1", "/"), "~0", "~")
	}
	parent := obj
	for _, p := range parts[:len(parts)-1] {
		next, ok := parent[p].(map[string]any)
		if !ok {
			if op.Op == "add" {
				next = map[string]any{}
				parent[p] = next
			} else {
				return 422
			}
		}
		parent = next
	}
	last := parts[len(parts)-1]
	switch op.Op {
	case "test":
		if parent[last] != op.Value {
			return 409
		}
	case "replace":
		if _, ok := parent[last]; !ok {
			return 422
		}
		parent[last] = op.Value
	case "add":
		parent[last] = op.Value
	case "remove":
		if _, ok := parent[last]; !ok {
			return 422
		}
		delete(parent, last)
	default:
		return 422
	}
	return 0
}

// harness is a registry over the fake API with a Strimzi room's targets.
type harness struct {
	api *fakeAPI
	reg *ops.Registry
	env ops.Env
	d   *Driver
}

const testNS, testKafka, testPipeline = "data", "prod-kafka", "orders"

func newHarness(t *testing.T, class string, secrets map[string]string) *harness {
	t.Helper()
	f := newFakeAPI(t)
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	api, err := NewAPI(APIConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Token: func() (string, error) { return "test-token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	api.sleep = func(context.Context, time.Duration) error { return nil }
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: targetsOf("kind-test", testNS, testKafka)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	var lookup func(string) (string, bool)
	if secrets != nil {
		lookup = func(n string) (string, bool) { v, ok := secrets[n]; return v, ok }
	}
	d, err := Register(reg, Options{Pipeline: testPipeline, Connectors: []string{"orders-src"}, Class: class, API: api, Secret: lookup})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{api: f, reg: reg, env: ops.Env{Targets: targetsOf("kind-test", testNS, testKafka)}, d: d}
}

// shell pre-creates a paused CR shell the way the chart does.
func (h *harness) shell(kind, name string, spec map[string]any) string {
	path := objectPath(testNS, kind, name)
	h.api.put(path, map[string]any{
		"apiVersion": "kafka.strimzi.io/v1", "kind": kind,
		"metadata": map[string]any{"name": name, "annotations": map[string]any{PauseAnnotation: "true"}, "managedFields": []any{map[string]any{"manager": "helm"}}},
		"spec":     spec,
	})
	return path
}
