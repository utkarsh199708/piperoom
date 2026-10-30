package setupcmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/strimzi"
)

func newCatalogue(t *testing.T) *ops.Registry {
	t.Helper()
	cat, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// genericOps are what ops.New registers on its own, before any target driver:
// a registrar that registers no driver must leave exactly these.
var genericOps = []string{
	"env_detect", "file_read", "dir_list", "jar_list", "java_version",
	"http_get", "net_probe", "sink_probe", "file_write", "file_delete",
	"source_publication_tables",
}

func TestRegistrarHostTarget(t *testing.T) {
	cat := newCatalogue(t)
	reg := newTargetRegistrar(cat, nil)
	err := reg.Apply(context.Background(), "room-1", []events.SetupTarget{
		{Kind: "postgres", Address: "db.example.com:5432", Class: protocol.ClassShared},
		{Kind: "host", Address: "127.0.0.1", Class: protocol.ClassShared},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	want := map[string]string{
		"service_status": protocol.TierT0, "properties_read": protocol.TierT0,
		"journal_tail": protocol.TierT0, "properties_write": protocol.TierT1,
		"service_install": protocol.TierT2, "service_restart": protocol.TierT2,
	}
	for op, tier := range want {
		got, ok := cat.Floor(op)
		if !ok {
			t.Errorf("host target: %s is not in the catalogue", op)
		} else if got != tier {
			t.Errorf("host target: %s floor = %q, want %q", op, got, tier)
		}
	}
	if _, ok := cat.Floor("compose_up"); ok {
		t.Error("host target: the compose driver registered too")
	}
}

func TestRegistrarComposeDefault(t *testing.T) {
	for name, targets := range map[string][]events.SetupTarget{
		"no targets":  nil,
		"stack hosts": {{Kind: "kafka", Address: "localhost:9092"}, {Kind: "connect", Address: "http://localhost:8083"}},
	} {
		cat := newCatalogue(t)
		if err := newTargetRegistrar(cat, nil).Apply(context.Background(), "room-1", targets); err != nil {
			t.Fatalf("%s: Apply: %v", name, err)
		}
		if _, ok := cat.Floor("compose_up"); !ok {
			t.Errorf("%s: the compose driver did not register", name)
		}
		if _, ok := cat.Floor("service_install"); ok {
			t.Errorf("%s: the host driver registered too", name)
		}
	}
}

// A managed target registers no local driver; its own cloud driver is
// covered in managed_test.go.
func TestRegistrarManagedTargetRegistersNoLocalDriver(t *testing.T) {
	cat := newCatalogue(t)
	reg, _ := fakeManagedRegistrar(t, cat)
	err := reg.Apply(context.Background(), "room-1", []events.SetupTarget{
		{Kind: "aws", Address: "123456789012/eu-west-1", Class: protocol.ClassProduction},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, op := range []string{"compose_up", "service_install", "properties_write"} {
		if _, ok := cat.Floor(op); ok {
			t.Errorf("managed target: %s registered", op)
		}
	}
	for _, op := range genericOps {
		if _, ok := cat.Floor(op); !ok {
			t.Errorf("managed target: generic op %s is missing", op)
		}
	}
}

// TestRegistrarIdempotent: a reconnect re-sends the Welcome's targets; the
// second Apply must not register twice (a duplicate op fails Register) and
// must not switch drivers.
func TestRegistrarIdempotent(t *testing.T) {
	cat := newCatalogue(t)
	reg := newTargetRegistrar(cat, nil)
	host := []events.SetupTarget{{Kind: "host", Address: "127.0.0.1"}}
	if err := reg.Apply(context.Background(), "room-1", host); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := reg.Apply(context.Background(), "room-1", host); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	if err := reg.Apply(context.Background(), "room-1", nil); err != nil {
		t.Fatalf("Apply after a targetless Welcome: %v", err)
	}
	if _, ok := cat.Floor("compose_up"); ok {
		t.Error("a later Welcome switched the driver to compose")
	}
}

func TestChooseTargetDriver(t *testing.T) {
	cases := []struct {
		name    string
		targets []events.SetupTarget
		driver  string
		class   string
	}{
		{"host wins over stack kinds", []events.SetupTarget{{Kind: "kafka"}, {Kind: "host", Class: protocol.ClassShared}}, "host", protocol.ClassShared},
		{"highest host class", []events.SetupTarget{{Kind: "host", Class: protocol.ClassShared}, {Kind: "host", Class: protocol.ClassProduction}}, "host", protocol.ClassProduction},
		{"host wins over managed", []events.SetupTarget{{Kind: "aws"}, {Kind: "host"}}, "host", ""},
		{"managed registers none", []events.SetupTarget{{Kind: "kubernetes"}}, "", ""},
		{"compose stays local", []events.SetupTarget{{Kind: "postgres", Class: protocol.ClassProduction}}, "compose", ""},
		{"empty means compose", nil, "compose", ""},
	}
	for _, tc := range cases {
		driver, class := chooseTargetDriver(tc.targets)
		if driver != tc.driver || class != tc.class {
			t.Errorf("%s: chooseTargetDriver = (%q, %q), want (%q, %q)", tc.name, driver, class, tc.driver, tc.class)
		}
	}
}

func TestParsePluginHosts(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{" ", nil},
		{"repo1.maven.org", []string{"repo1.maven.org"}},
		{"a.example.com, b.example.com:8443 ,", []string{"a.example.com", "b.example.com:8443"}},
	}
	for _, tc := range cases {
		got := parsePluginHosts(tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("parsePluginHosts(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("parsePluginHosts(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

// With --embedded the registrar puts the catalogue in embedded mode whatever
// the targets: the embedded ops are registered, no Compose or host driver is,
// and file_write is refused.
func TestRegistrarEmbedded(t *testing.T) {
	cat := newCatalogue(t)
	reg := newTargetRegistrar(cat, nil)
	reg.embedded = true
	if err := reg.Apply(context.Background(), "room-1", []events.SetupTarget{{Kind: "host", Address: "127.0.0.1"}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, op := range []string{ops.OpProjectInspect, ops.OpReferenceWrite, ops.OpEmbeddedLogRead} {
		if _, ok := cat.Floor(op); !ok {
			t.Errorf("embedded: %s is not registered", op)
		}
	}
	for _, op := range []string{"compose_up", "service_install"} {
		if _, ok := cat.Floor(op); ok {
			t.Errorf("embedded: %s registered", op)
		}
	}
	if got := cat.CheckTarget(protocol.OpRequest{Op: "file_write", Params: []byte(`{"path":"a.txt","content":"x"}`)}); got != protocol.RefuseTarget {
		t.Errorf("file_write in embedded mode: CheckTarget = %q, want %q", got, protocol.RefuseTarget)
	}
}

// In the in-cluster Job (--confirm-secret-prefix) a kubernetes target
// registers the Strimzi driver; outside it the target registers nothing.
func TestRegistrarStrimziInJob(t *testing.T) {
	cat := newCatalogue(t)
	reg := newTargetRegistrar(cat, nil)
	api, err := strimzi.NewAPI(strimzi.APIConfig{BaseURL: "https://kubernetes.default.svc", Token: func() (string, error) { return "t", nil }})
	if err != nil {
		t.Fatal(err)
	}
	reg.strimzi = &strimzi.Options{Pipeline: "orders", Connectors: []string{"orders-src"}, API: api}
	targets := []events.SetupTarget{{Kind: "kubernetes", Address: "kind/data", Class: protocol.ClassProduction}, {Kind: "kafka", Address: "strimzi://prod-kafka"}}
	if err := reg.Apply(context.Background(), "room-1", targets); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, op := range []string{"k8s_get", "k8s_apply", "k8s_secret_apply"} {
		if _, ok := cat.Floor(op); !ok {
			t.Errorf("%s is not registered", op)
		}
	}
	if _, ok := cat.Floor("compose_up"); ok {
		t.Error("the compose driver registered too")
	}

	plain := newCatalogue(t)
	if err := newTargetRegistrar(plain, nil).Apply(context.Background(), "room-1", targets); err != nil {
		t.Fatal(err)
	}
	if _, ok := plain.Floor("k8s_get"); ok {
		t.Error("a laptop runner registered the Strimzi driver")
	}
}

// fakeSecretAPI is a Kubernetes API server holding one Secret: GET returns
// it, a JSON patch's stringData is stored base64 under data, as the API
// server does.
type fakeSecretAPI struct {
	mu      sync.Mutex
	path    string
	data    map[string]string
	patched int
}

func (f *fakeSecretAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path != f.path {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodPatch {
		var patch []struct {
			Op    string          `json:"op"`
			Path  string          `json:"path"`
			Value json.RawMessage `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, p := range patch {
			if p.Path == "/stringData" {
				var sd map[string]string
				_ = json.Unmarshal(p.Value, &sd)
				f.data = map[string]string{}
				for k, v := range sd {
					f.data[k] = base64.StdEncoding.EncodeToString([]byte(v))
				}
			}
		}
		f.patched++
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"kind": "Secret", "metadata": map[string]any{"name": "orders-db-credentials", "resourceVersion": "7"}, "data": f.data,
	})
}

// TestRegistrarStrimziSecretFromStore: in the in-cluster Job the Strimzi
// driver resolves k8s_secret_apply's ${secret:} references from the runner's
// own store (the one secret_set writes); without a store the op refuses.
func TestRegistrarStrimziSecretFromStore(t *testing.T) {
	kube := &fakeSecretAPI{path: "/api/v1/namespaces/data/secrets/orders-db-credentials", data: map[string]string{}}
	srv := httptest.NewTLSServer(kube)
	defer srv.Close()
	api, err := strimzi.NewAPI(strimzi.APIConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Token: func() (string, error) { return "t", nil }})
	if err != nil {
		t.Fatal(err)
	}
	targets := []events.SetupTarget{{Kind: "kubernetes", Address: "kind/data", Class: protocol.ClassShared}, {Kind: "kafka", Address: "strimzi://prod-kafka"}}
	env := ops.Env{Targets: targets, Now: time.Now}
	params := json.RawMessage(`{"keys":{"password":"${secret:source_password}"}}`)
	const value = "s3cr3t-db-value"

	st, open := openTestStore(t)
	if err := st.Set("source_password", value); err != nil {
		t.Fatal(err)
	}
	cat := newCatalogue(t)
	reg := newTargetRegistrar(cat, nil)
	reg.strimzi = &strimzi.Options{Pipeline: "orders", Connectors: []string{"orders-src"}, API: api}
	reg.secrets = open
	if err := reg.Apply(context.Background(), "room-1", targets); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	op, ok := cat.Get("k8s_secret_apply")
	if !ok {
		t.Fatal("k8s_secret_apply is not registered")
	}
	res, err := op.Execute(context.Background(), params, env)
	if err != nil {
		t.Fatalf("k8s_secret_apply: %v", err)
	}
	if b, _ := json.Marshal(res); strings.Contains(string(b), value) {
		t.Fatal("the secret value reached the result")
	}
	kube.mu.Lock()
	stored := kube.data["password"]
	kube.mu.Unlock()
	if dec, _ := base64.StdEncoding.DecodeString(stored); string(dec) != value {
		t.Errorf("the Secret holds %q, want the stored value", dec)
	}
	missing := json.RawMessage(`{"keys":{"password":"${secret:not_set}"}}`)
	if _, err := op.Execute(context.Background(), missing, env); !errors.Is(err, strimzi.ErrParams) || !strings.Contains(err.Error(), "not_set") {
		t.Errorf("a name not in the store: %v", err)
	}

	// No store wired (a registrar built without the runner): the op refuses.
	bare := newCatalogue(t)
	reg2 := newTargetRegistrar(bare, nil)
	reg2.strimzi = &strimzi.Options{Pipeline: "orders", Connectors: []string{"orders-src"}, API: api}
	if err := reg2.Apply(context.Background(), "room-1", targets); err != nil {
		t.Fatal(err)
	}
	op2, _ := bare.Get("k8s_secret_apply")
	if _, err := op2.Execute(context.Background(), params, env); !errors.Is(err, strimzi.ErrNoSecrets) {
		t.Errorf("no store: %v, want ErrNoSecrets", err)
	}
}

func TestReadCodeFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if got, err := readCodeFile(write("ok", "ABCD-EFGH\n")); err != nil || got != "ABCD-EFGH" {
		t.Errorf("readCodeFile = %q, %v", got, err)
	}
	for name, body := range map[string]string{"empty": " \n", "huge": strings.Repeat("A", maxCodeFile+1)} {
		if _, err := readCodeFile(write(name, body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := readCodeFile(filepath.Join(dir, "missing")); err == nil {
		t.Error("a missing file was accepted")
	}
}

func TestInClusterJobPrefix(t *testing.T) {
	if _, err := newInClusterJob("other-confirm", "orders", "orders-src"); err == nil || !strings.Contains(err.Error(), "piperoom-setup-<room>-confirm") {
		t.Fatalf("err = %v", err)
	}
}
