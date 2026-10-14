package gcp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

const (
	canary  = "CANARY-gcp-4b3a2c"
	project = "acme-prod"
)

type fakeGoogle struct {
	srv *httptest.Server

	mu          sync.Mutex
	values      map[string]string // secret id -> latest value
	validTok    string            // "" = any bearer token
	granted     map[string][]string
	badCRC      bool
	jwtTokens   atomic.Int32
	rejectAll   atomic.Bool
	listDeny    atomic.Bool
	readDeny    atomic.Bool
	rmFail      atomic.Bool
	delay       atomic.Int64
	lastAuthHdr string
}

func newFake(t *testing.T) *fakeGoogle {
	t.Helper()
	f := &fakeGoogle{
		values: map[string]string{
			"piperoom-orders-db": `{"username":"ro","password":"` + canary + `"}`,
			"piperoom-kafka":     canary,
			"other":              "x",
		},
		granted: map[string][]string{},
	}
	mux := http.NewServeMux()
	apiErr := func(w http.ResponseWriter, code int, status, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "status": status, "message": msg}})
	}
	auth := func(w http.ResponseWriter, r *http.Request) bool {
		if d := time.Duration(f.delay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return false
			}
		}
		h := r.Header.Get("Authorization")
		f.mu.Lock()
		valid := f.validTok
		f.lastAuthHdr = h
		f.mu.Unlock()
		if h == "" || f.rejectAll.Load() || (valid != "" && h != "Bearer "+valid) {
			apiErr(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Request had invalid authentication credentials.")
			return false
		}
		return true
	}
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
			return
		}
		n := f.jwtTokens.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("sa-tok-%d", n), "expires_in": 3600, "token_type": "Bearer"})
	})
	mux.HandleFunc("GET /v1/projects/{p}/secrets", func(w http.ResponseWriter, r *http.Request) {
		if !auth(w, r) {
			return
		}
		if f.listDeny.Load() {
			apiErr(w, http.StatusForbidden, "PERMISSION_DENIED", "Permission 'secretmanager.secrets.list' denied")
			return
		}
		p := r.PathValue("p")
		if r.URL.Query().Get("pageToken") == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"secrets":       []map[string]any{{"name": "projects/123/secrets/piperoom-orders-db"}, {"name": "projects/123/secrets/other"}},
				"nextPageToken": "p2",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"secrets": []map[string]any{{"name": "projects/" + p + "/secrets/piperoom-kafka"}}})
	})
	mux.HandleFunc("GET /v1/projects/{p}/secrets/{s}/versions/{va}", func(w http.ResponseWriter, r *http.Request) {
		if !auth(w, r) {
			return
		}
		ver, ok := strings.CutSuffix(r.PathValue("va"), ":access")
		if !ok {
			apiErr(w, http.StatusNotFound, "NOT_FOUND", "no such method")
			return
		}
		if f.readDeny.Load() {
			apiErr(w, http.StatusForbidden, "PERMISSION_DENIED", "Permission 'secretmanager.versions.access' denied")
			return
		}
		if ver == "2" {
			apiErr(w, http.StatusBadRequest, "FAILED_PRECONDITION", "Secret Version is in DESTROYED state.")
			return
		}
		f.mu.Lock()
		v, found := f.values[r.PathValue("s")]
		bad := f.badCRC
		f.mu.Unlock()
		if !found || (ver != "latest" && ver != "7") {
			apiErr(w, http.StatusNotFound, "NOT_FOUND", "Secret ["+r.PathValue("s")+"] not found or has no versions.")
			return
		}
		crc := crc32.Checksum([]byte(v), crc32.MakeTable(crc32.Castagnoli))
		if bad {
			crc++
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name":    "projects/123/secrets/" + r.PathValue("s") + "/versions/7",
			"payload": map[string]any{"data": base64.StdEncoding.EncodeToString([]byte(v)), "dataCrc32c": fmt.Sprint(crc)},
		})
	})
	testPerms := func(key string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !auth(w, r) {
				return
			}
			var in struct {
				Permissions []string `json:"permissions"`
			}
			if json.NewDecoder(r.Body).Decode(&in) != nil || len(in.Permissions) == 0 {
				apiErr(w, http.StatusBadRequest, "INVALID_ARGUMENT", "bad")
				return
			}
			f.mu.Lock()
			g := f.granted[key]
			f.mu.Unlock()
			var out []string
			for _, p := range in.Permissions {
				for _, x := range g {
					if p == x {
						out = append(out, p)
					}
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"permissions": out})
		}
	}
	mux.HandleFunc("POST /v1/projects/{p}/secrets/{st}", func(w http.ResponseWriter, r *http.Request) {
		s, ok := strings.CutSuffix(r.PathValue("st"), ":testIamPermissions")
		if !ok {
			apiErr(w, http.StatusNotFound, "NOT_FOUND", "")
			return
		}
		testPerms("secret:"+s)(w, r)
	})
	mux.HandleFunc("POST /v1/projects/{pt}", func(w http.ResponseWriter, r *http.Request) {
		if f.rmFail.Load() {
			apiErr(w, http.StatusForbidden, "PERMISSION_DENIED", "Cloud Resource Manager API has not been used in project 123")
			return
		}
		testPerms("project")(w, r)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// countingSource hands out tok-1, tok-2, ...
type countingSource struct{ n atomic.Int32 }

func (c *countingSource) Token() (*oauth2.Token, error) {
	n := c.n.Add(1)
	return &oauth2.Token{AccessToken: fmt.Sprintf("tok-%d", n), Expiry: time.Now().Add(time.Hour)}, nil
}

type errSource struct{ err error }

func (e errSource) Token() (*oauth2.Token, error) { return nil, e.err }

func (f *fakeGoogle) provider(t *testing.T, ts oauth2.TokenSource) *Provider {
	t.Helper()
	p, err := New(Options{Project: project, Endpoint: f.srv.URL, ResourceManagerEndpoint: f.srv.URL, TokenSource: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestResolve(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, &countingSource{})
	v, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"})
	if err != nil || v.Data.Reveal() != canary || v.Version != "7" {
		t.Fatalf("resolve = %q, %v", v.Version, err)
	}
	v, err = p.Resolve(ctx(t), secrets.Ref{Path: "projects/other-proj/secrets/piperoom-kafka", Version: "7"})
	if err != nil || v.Data.Reveal() != canary {
		t.Fatalf("full path: %v", err)
	}
	keys, err := p.Keys(ctx(t), secrets.Ref{Path: "piperoom-orders-db", Key: "password"})
	if err != nil || strings.Join(keys, ",") != "password,username" {
		t.Fatalf("keys = %v, %v", keys, err)
	}
}

func TestServiceAccountFile(t *testing.T) {
	f := newFake(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	sa, _ := json.Marshal(map[string]string{
		"type": "service_account", "project_id": project, "private_key_id": "k1",
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email": "piperoom@acme-prod.iam.gserviceaccount.com", "client_id": "1",
		"token_uri": f.srv.URL + "/token",
	})
	dir := t.TempDir()
	file := filepath.Join(dir, "sa.json")
	if err := os.WriteFile(file, sa, 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{Project: project, Endpoint: f.srv.URL, ResourceManagerEndpoint: f.srv.URL, CredentialsFile: file})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	hdr := f.lastAuthHdr
	f.mu.Unlock()
	if hdr != "Bearer sa-tok-1" || f.jwtTokens.Load() != 1 {
		t.Fatalf("auth header %q, tokens %d", hdr, f.jwtTokens.Load())
	}

	// A non-service-account file is refused, naming the file but not quoting it.
	ext := filepath.Join(dir, "ext.json")
	if err := os.WriteFile(ext, []byte(`{"type":"external_account","audience":"`+canary+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	p2, err := New(Options{Project: project, Endpoint: f.srv.URL, CredentialsFile: ext})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	_, err = p2.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"})
	if !errors.Is(err, secrets.ErrLogin) || strings.Contains(err.Error(), canary) {
		t.Fatalf("external account: %v", err)
	}
	checks := p2.Check(ctx(t), nil)
	if byID(checks)[secrets.CheckLogin].Status != spec.CheckFail {
		t.Fatalf("checks = %+v", checks)
	}
}

func TestRetryOnceAfter401(t *testing.T) {
	f := newFake(t)
	ts := &countingSource{}
	p := f.provider(t, ts)
	f.mu.Lock()
	f.validTok = "tok-2"
	f.mu.Unlock()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	f.rejectAll.Store(true)
	before := ts.n.Load()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); !errors.Is(err, secrets.ErrLogin) {
		t.Fatalf("err = %v", err)
	}
	if ts.n.Load()-before != 2 {
		t.Fatalf("token calls = %d, want 2 (one retry)", ts.n.Load()-before)
	}
}

func TestErrorPaths(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, &countingSource{})
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "missing"}); !errors.Is(err, secrets.ErrNotFound) {
		t.Fatalf("404: %v", err)
	}
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka", Version: "2"}); !errors.Is(err, secrets.ErrNotFound) {
		t.Fatalf("destroyed: %v", err)
	}
	f.readDeny.Store(true)
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); !errors.Is(err, secrets.ErrAccessDenied) {
		t.Fatalf("403: %v", err)
	}
	f.readDeny.Store(false)
	f.mu.Lock()
	f.badCRC = true
	f.mu.Unlock()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("crc: %v", err)
	}
	for _, ref := range []secrets.Ref{
		{Path: "a.b"}, {Path: "projects/x/secrets"}, {Path: "projects/x/keys/y"}, {Path: "projects/X!/secrets/y"},
		{Path: "ok", Version: "1.2"},
	} {
		if _, err := p.Resolve(ctx(t), ref); !errors.Is(err, secrets.ErrInvalidRef) {
			t.Errorf("%+v: %v", ref, err)
		}
	}
	// Token endpoint refusal and network failure.
	p2 := f.provider(t, errSource{&oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}, ErrorCode: "invalid_grant",
		ErrorDescription: "Invalid JWT Signature.", Body: []byte(canary)}})
	_, err := p2.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"})
	if !errors.Is(err, secrets.ErrLogin) || !strings.Contains(err.Error(), "invalid_grant") || strings.Contains(err.Error(), canary) {
		t.Fatalf("retrieve error: %v", err)
	}
	p3 := f.provider(t, errSource{&oauth2.RetrieveError{Response: &http.Response{StatusCode: 503}}})
	if _, err := p3.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); !errors.Is(err, secrets.ErrUnreachable) {
		t.Fatalf("503 token: %v", err)
	}
}

// slowSource blocks until released, like a metadata server that hangs.
type slowSource struct{ release chan struct{} }

func (s slowSource) Token() (*oauth2.Token, error) {
	<-s.release
	return &oauth2.Token{AccessToken: "late"}, nil
}

func TestTimeoutAndDown(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, &countingSource{})
	f.delay.Store(int64(2 * time.Second))
	c, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := p.Resolve(c, secrets.Ref{Path: "piperoom-kafka"}); !errors.Is(err, secrets.ErrUnreachable) {
		t.Fatalf("timeout: %v", err)
	}
	f.delay.Store(0)

	// A hanging token source is bounded by ctx.
	rel := make(chan struct{})
	defer close(rel)
	p2 := f.provider(t, slowSource{rel})
	c2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	if _, err := p2.Resolve(c2, secrets.Ref{Path: "piperoom-kafka"}); !errors.Is(err, secrets.ErrUnreachable) {
		t.Fatalf("token hang: %v", err)
	}

	down := httptest.NewServer(http.NotFoundHandler())
	u := down.URL
	down.Close()
	p3, err := New(Options{Project: project, Endpoint: u, ResourceManagerEndpoint: u, TokenSource: &countingSource{}})
	if err != nil {
		t.Fatal(err)
	}
	defer p3.Close()
	if _, err := p3.Resolve(ctx(t), secrets.Ref{Path: "x"}); !errors.Is(err, secrets.ErrUnreachable) {
		t.Fatalf("down: %v", err)
	}
	checks := p3.Check(ctx(t), nil)
	if checks[0].ID != secrets.CheckReach || checks[0].Status != spec.CheckFail {
		t.Fatalf("checks = %+v", checks)
	}
}

func TestList(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, &countingSource{})
	names, err := p.List(ctx(t), "", 200)
	if err != nil || strings.Join(names, ",") != "other,piperoom-kafka,piperoom-orders-db" {
		t.Fatalf("names = %v, %v", names, err)
	}
	names, err = p.List(ctx(t), "piperoom-", 1)
	if err != nil || len(names) != 1 || !strings.HasPrefix(names[0], "piperoom-") {
		t.Fatalf("prefix = %v, %v", names, err)
	}
	f.listDeny.Store(true)
	if _, err := p.List(ctx(t), "", 10); !errors.Is(err, secrets.ErrListUnsupported) {
		t.Fatalf("list 403: %v", err)
	}
}

func byID(checks []spec.Check) map[string]spec.Check {
	m := map[string]spec.Check{}
	for _, c := range checks {
		m[c.ID] = c
	}
	return m
}

func TestCheck(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, &countingSource{})
	sample := &secrets.Ref{Path: "piperoom-orders-db", Key: "password"}
	checks := p.Check(ctx(t), sample)
	m := byID(checks)
	for _, id := range []string{secrets.CheckReach, secrets.CheckLogin, secrets.CheckRead, secrets.CheckTooMuch} {
		if m[id].Status != spec.CheckPass {
			t.Fatalf("%s = %+v", id, m[id])
		}
	}
	b, _ := json.Marshal(checks)
	if strings.Contains(string(b), canary) {
		t.Fatalf("value leaked: %s", b)
	}

	// Write access on the secret: warn and name the permission.
	f.mu.Lock()
	f.granted["secret:piperoom-orders-db"] = []string{"secretmanager.versions.add", "secretmanager.versions.access"}
	f.mu.Unlock()
	m = byID(p.Check(ctx(t), sample))
	if c := m[secrets.CheckTooMuch]; c.Status != spec.CheckWarn || !strings.Contains(c.Detail, "secretmanager.versions.add") ||
		strings.Contains(c.Detail, "versions.access") {
		t.Fatalf("write = %+v", c)
	}

	// Project-wide read.
	f.mu.Lock()
	f.granted = map[string][]string{"project": {"secretmanager.versions.access"}}
	f.mu.Unlock()
	m = byID(p.Check(ctx(t), sample))
	if c := m[secrets.CheckTooMuch]; c.Status != spec.CheckWarn || !strings.Contains(c.Detail, "every secret") {
		t.Fatalf("project read = %+v", c)
	}

	// testIamPermissions unavailable everywhere: skipped.
	f.mu.Lock()
	f.granted = map[string][]string{}
	f.mu.Unlock()
	f.rmFail.Store(true)
	m = byID(p.Check(ctx(t), nil))
	if c := m[secrets.CheckTooMuch]; c.Status != spec.CheckSkipped {
		t.Fatalf("rm down = %+v", c)
	}
	// Only the project half fails: partial pass.
	m = byID(p.Check(ctx(t), sample))
	if c := m[secrets.CheckTooMuch]; c.Status != spec.CheckPass || !strings.Contains(c.Detail, "partial") {
		t.Fatalf("partial = %+v", c)
	}
	f.rmFail.Store(false)

	// List denied: warning only.
	f.listDeny.Store(true)
	checks = p.Check(ctx(t), sample)
	if byID(checks)[secrets.CheckListNames].Status != spec.CheckWarn || spec.Failed(checks) {
		t.Fatalf("list denied = %+v", checks)
	}
	f.listDeny.Store(false)

	// Read denied on the sample: fail with a fix.
	f.readDeny.Store(true)
	m = byID(p.Check(ctx(t), sample))
	if c := m[secrets.CheckRead]; c.Status != spec.CheckFail || !strings.Contains(c.Fix, "secretAccessor") {
		t.Fatalf("read denied = %+v", c)
	}
	f.readDeny.Store(false)

	// Token rejected by the API.
	f.rejectAll.Store(true)
	m = byID(p.Check(ctx(t), sample))
	if m[secrets.CheckLogin].Status != spec.CheckFail || m[secrets.CheckRead].Status != spec.CheckSkipped {
		t.Fatalf("401 = %+v", m)
	}
}

func TestFormattingAndClose(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, &countingSource{})
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); err != nil {
		t.Fatal(err)
	}
	if s := fmt.Sprintf("%v %+v %#v", p, p, p); strings.Contains(s, canary) || strings.Contains(s, "tok-") {
		t.Fatalf("leak: %s", s)
	}
	p.Close()
	p.Close()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); !errors.Is(err, secrets.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	for name, o := range map[string]Options{
		"no project":   {},
		"bad project":  {Project: "Acme Prod"},
		"http":         {Project: project, Endpoint: "http://secretmanager.example"},
		"endpoint pth": {Project: project, Endpoint: "https://secretmanager.googleapis.com/v1"},
		"rm http":      {Project: project, ResourceManagerEndpoint: "http://rm.example"},
	} {
		if _, err := New(o); !errors.Is(err, secrets.ErrBadProviderConfig) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestFactory(t *testing.T) {
	f := newFake(t)
	specs, err := secrets.ParseProviders("gsm=gcp-secret-manager:" + project + ";endpoint=" + f.srv.URL + ";timeout=2s")
	if err != nil {
		t.Fatal(err)
	}
	fs := secrets.DefaultFactories()
	Register(fs)
	p, err := fs[secrets.TypeGoogleSecretManager](ctx(t), specs[0], secrets.Deps{})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	specs[0].Options["regon"] = "x"
	if _, err := Factory(ctx(t), specs[0], secrets.Deps{}); !errors.Is(err, secrets.ErrBadProviderConfig) || !strings.Contains(err.Error(), "regon") {
		t.Fatalf("unknown: %v", err)
	}
	delete(specs[0].Options, "regon")
	specs[0].Secrets["token"] = "t"
	if _, err := Factory(ctx(t), specs[0], secrets.Deps{}); !errors.Is(err, secrets.ErrBadProviderConfig) {
		t.Fatalf("secret option: %v", err)
	}
}

func TestSetupScript(t *testing.T) {
	s := SetupScript(project, "piperoom@acme-prod.iam.gserviceaccount.com", "piperoom-")
	for _, want := range []string{"roles/secretmanager.secretAccessor", "startsWith('projects/$PROJECT_NUMBER/secrets/piperoom-')", "PROJECT=acme-prod"} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in\n%s", want, s)
		}
	}
	s = SetupScript("$(evil)", "x; rm -rf /", "a'b")
	if strings.Contains(s, "evil") || strings.Contains(s, "rm -rf") || strings.Contains(s, "a'b") ||
		!strings.Contains(s, "<project-id>") || !strings.Contains(s, "<piperoom-service-account-email>") {
		t.Fatal(s)
	}
}

// blockingSource counts Token calls and blocks until released.
type blockingSource struct {
	calls   atomic.Int32
	release chan struct{}
}

func (s *blockingSource) Token() (*oauth2.Token, error) {
	s.calls.Add(1)
	<-s.release
	return &oauth2.Token{AccessToken: "late"}, nil
}

// TestHangingSourceHoldsOneGoroutine: while an injected source hangs,
// every caller times out on its own ctx and only one fetch is started.
func TestHangingSourceHoldsOneGoroutine(t *testing.T) {
	f := newFake(t)
	src := &blockingSource{release: make(chan struct{})}
	defer close(src.release)
	p := f.provider(t, src)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if _, err := p.Resolve(c, secrets.Ref{Path: "piperoom-kafka"}); !errors.Is(err, secrets.ErrUnreachable) {
				t.Errorf("err = %v", err)
			}
		}()
	}
	wg.Wait()
	if got := src.calls.Load(); got != 1 {
		t.Fatalf("Token calls = %d, want 1", got)
	}
}

// TestNewTokenSourceRebuiltAfter401: a source factory is rebuilt after a
// 401, so a caching source does not hand back the rejected token.
func TestNewTokenSourceRebuiltAfter401(t *testing.T) {
	f := newFake(t)
	f.mu.Lock()
	f.validTok = "f-2"
	f.mu.Unlock()
	var built atomic.Int32
	p, err := New(Options{Project: project, Endpoint: f.srv.URL, ResourceManagerEndpoint: f.srv.URL,
		NewTokenSource: func() (oauth2.TokenSource, error) {
			n := built.Add(1)
			return oauth2.ReuseTokenSource(nil, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: fmt.Sprintf("f-%d", n)})), nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); err != nil {
		t.Fatalf("after rebuild: %v", err)
	}
	if built.Load() != 2 {
		t.Fatalf("sources built = %d, want 2", built.Load())
	}

	// A factory error is ErrLogin and does not quote the hook's text.
	p2, err := New(Options{Project: project, Endpoint: f.srv.URL,
		NewTokenSource: func() (oauth2.TokenSource, error) { return nil, errors.New(canary) }})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if _, err := p2.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); !errors.Is(err, secrets.ErrLogin) || strings.Contains(err.Error(), canary) {
		t.Fatalf("factory error: %v", err)
	}
}

// TestOtherTokenErrorsDropText: errors other than RetrieveError or network
// errors may carry a response body; only the source and type are kept.
func TestOtherTokenErrorsDropText(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, errSource{errors.New("oauth2: status code 400: " + canary)})
	_, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"})
	if !errors.Is(err, secrets.ErrLogin) || strings.Contains(err.Error(), canary) || !strings.Contains(err.Error(), "injected token source") {
		t.Fatalf("err = %v", err)
	}
}

// TestInjectedIdentityClientNoRedirect: an injected identity client still
// refuses redirects, so the signed JWT assertion is not resent elsewhere.
func TestInjectedIdentityClientNoRedirect(t *testing.T) {
	f := newFake(t)
	var stolen atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/stolen", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/stolen", func(w http.ResponseWriter, r *http.Request) { stolen.Add(1) })
	tokSrv := httptest.NewServer(mux)
	defer tokSrv.Close()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	sa, _ := json.Marshal(map[string]string{
		"type": "service_account", "project_id": project, "private_key_id": "k1",
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email": "piperoom@acme-prod.iam.gserviceaccount.com", "client_id": "1",
		"token_uri": tokSrv.URL + "/token",
	})
	file := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(file, sa, 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{Project: project, Endpoint: f.srv.URL, CredentialsFile: file,
		IdentityHTTPClient: &http.Client{Timeout: 5 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "piperoom-kafka"}); err == nil {
		t.Fatal("want a login error on redirect")
	}
	if stolen.Load() != 0 {
		t.Fatal("the token request followed the redirect")
	}
}
