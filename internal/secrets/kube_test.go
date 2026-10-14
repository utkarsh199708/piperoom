package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/integrations/spec"
)

type fakeKube struct {
	mu      sync.Mutex
	token   string
	allowed map[string]bool // SSAR verb → allowed
	listOK  bool
	accepts []string
}

func (fk *fakeKube) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fk.mu.Lock()
	defer fk.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Header.Get("Authorization") != "Bearer "+fk.token {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"kind":"Status","message":"Unauthorized"}`))
		return
	}
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	switch {
	case r.URL.Path == "/version":
		_, _ = w.Write([]byte(`{"gitVersion":"v1.31.0"}`))
	case r.URL.Path == "/api/v1/namespaces/piperoom/secrets/orders-db" && r.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{"name": "orders-db", "resourceVersion": "4711"},
			"data":     map[string]string{"password": b64("k8s-pw"), "username": b64("ro")},
		})
	case r.URL.Path == "/api/v1/namespaces/piperoom/secrets/broken":
		_, _ = w.Write([]byte(`{"metadata":{"name":"broken"},"data":{"x":"!!notbase64"}}`))
	case r.URL.Path == "/api/v1/namespaces/other/secrets/orders-db":
		w.WriteHeader(403)
		_, _ = w.Write([]byte(`{"kind":"Status","message":"secrets \"orders-db\" is forbidden"}`))
	case r.URL.Path == "/api/v1/namespaces/piperoom/secrets" && r.Method == http.MethodGet:
		fk.accepts = append(fk.accepts, r.Header.Get("Accept"))
		if !fk.listOK {
			w.WriteHeader(403)
			_, _ = w.Write([]byte(`{"message":"forbidden"}`))
			return
		}
		_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"orders-db"}},{"metadata":{"name":"kafka"}}]}`))
	case r.URL.Path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" && r.Method == http.MethodPost:
		var body struct {
			Spec struct {
				ResourceAttributes map[string]string `json:"resourceAttributes"`
			} `json:"spec"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		allowed := fk.allowed[body.Spec.ResourceAttributes["verb"]]
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"allowed": allowed}})
	default:
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"message":"not found"}`))
	}
}

func newKubeTest(t *testing.T) (*fakeKube, *KubernetesProvider, *httptest.Server) {
	t.Helper()
	fk := &fakeKube{token: "sa-token", allowed: map[string]bool{}}
	srv := httptest.NewTLSServer(fk)
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	tokFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokFile, []byte("sa-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	p, err := NewKubernetesProvider(KubernetesOptions{Address: srv.URL, Namespace: "piperoom", TokenFile: tokFile, CAPEM: caPEM})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return fk, p, srv
}

func TestKubernetesResolve(t *testing.T) {
	_, p, _ := newKubeTest(t)
	ctx := context.Background()
	for _, path := range []string{"orders-db", "piperoom/orders-db"} {
		v, err := p.Resolve(ctx, Ref{Path: path})
		if err != nil {
			t.Fatal(err)
		}
		if pw, _ := ExtractKey(v.Data, "password"); pw.Reveal() != "k8s-pw" || v.Version != "4711" {
			t.Fatalf("%s: %q %q", path, pw.Reveal(), v.Version)
		}
	}
	if _, err := p.Resolve(ctx, Ref{Path: "orders-db", Version: "4711"}); err != nil {
		t.Fatalf("current version pin: %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "orders-db", Version: "1"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old version err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "other/orders-db"}); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("403 err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("404 err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "Bad_Name"}); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("bad name err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "a/b/c"}); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("too many segments err = %v", err)
	}
	_, err := p.Resolve(ctx, Ref{Path: "broken"})
	if err == nil || strings.Contains(err.Error(), "notbase64") {
		t.Fatalf("broken err = %v", err)
	}
	keys, _ := p.Keys(ctx, Ref{Path: "orders-db"})
	if !reflect.DeepEqual(keys, []string{"password", "username"}) {
		t.Fatalf("keys = %v", keys)
	}
}

func TestKubernetesBadToken(t *testing.T) {
	fk, p, _ := newKubeTest(t)
	fk.mu.Lock()
	fk.token = "rotated"
	fk.mu.Unlock()
	if _, err := p.Resolve(context.Background(), Ref{Path: "orders-db"}); !errors.Is(err, ErrLogin) {
		t.Fatalf("err = %v", err)
	}
}

func TestKubernetesList(t *testing.T) {
	fk, p, _ := newKubeTest(t)
	ctx := context.Background()
	if _, err := p.List(ctx, "", 10); !errors.Is(err, ErrListUnsupported) {
		t.Fatalf("403 err = %v", err)
	}
	fk.mu.Lock()
	fk.listOK = true
	fk.mu.Unlock()
	got, err := p.List(ctx, "piperoom/ord", 10)
	if err != nil || !reflect.DeepEqual(got, []string{"piperoom/orders-db"}) {
		t.Fatalf("List = %v %v", got, err)
	}
	fk.mu.Lock()
	defer fk.mu.Unlock()
	for _, a := range fk.accepts {
		if !strings.Contains(a, "PartialObjectMetadataList") {
			t.Fatalf("list asked for full objects (values): Accept %q", a)
		}
	}
}

func TestKubernetesCheck(t *testing.T) {
	fk, p, srv := newKubeTest(t)
	ctx := context.Background()
	checks := p.Check(ctx, &Ref{Path: "orders-db", Key: "password"})
	if spec.Failed(checks) {
		t.Fatalf("checks = %+v", checks)
	}
	if last := checks[len(checks)-1]; last.ID != CheckTooMuch || last.Status != spec.CheckPass {
		t.Fatalf("least privilege = %+v", last)
	}
	fk.mu.Lock()
	fk.allowed["create"] = true
	fk.mu.Unlock()
	checks = p.Check(ctx, nil)
	if last := checks[len(checks)-1]; last.Status != spec.CheckWarn || !strings.Contains(last.Detail, "create") {
		t.Fatalf("least privilege = %+v", last)
	}
	fk.mu.Lock()
	fk.token = "other"
	fk.mu.Unlock()
	checks = p.Check(ctx, nil)
	if checks[1].ID != CheckLogin || checks[1].Status != spec.CheckFail {
		t.Fatalf("bad token checks = %+v", checks)
	}
	srv.Close()
	checks = p.Check(ctx, nil)
	if checks[0].Status != spec.CheckFail {
		t.Fatalf("down checks = %+v", checks)
	}
}

func TestKubernetesOptions(t *testing.T) {
	env := func(string) string { return "" }
	if _, err := NewKubernetesProvider(KubernetesOptions{Getenv: env}); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("no address err = %v", err)
	}
	if _, err := NewKubernetesProvider(KubernetesOptions{Address: "http://10.0.0.1"}); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("http err = %v", err)
	}
	if _, err := NewKubernetesProvider(KubernetesOptions{Address: "https://k", Namespace: "Bad_NS"}); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("namespace err = %v", err)
	}
	inCluster := func(k string) string {
		return map[string]string{"KUBERNETES_SERVICE_HOST": "10.0.0.1", "KUBERNETES_SERVICE_PORT": "443"}[k]
	}
	p, err := NewKubernetesProvider(KubernetesOptions{Getenv: inCluster, Namespace: "ns"})
	if err != nil || p.base != "https://10.0.0.1:443" {
		t.Fatalf("in-cluster: %v %v", p, err)
	}
}
