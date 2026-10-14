package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

const (
	canary  = "CANARY-az-9f8e7d"
	version = "0123456789abcdef0123456789abcdef"
	tenant  = "contoso.onmicrosoft.com"
)

// fakeAzure is Entra ID, IMDS and one Key Vault on one httptest server.
type fakeAzure struct {
	srv *httptest.Server

	mu        sync.Mutex
	secrets   map[string]string // name -> value
	tokenN    atomic.Int32
	validTok  string // the token Key Vault accepts; "" = any issued token
	rejectAll atomic.Bool
	listDeny  atomic.Bool
	readDeny  atomic.Bool
	tokenFail atomic.Bool
	delay     atomic.Int64
	lastForm  map[string]string
	badNext   bool
}

func newFake(t *testing.T) *fakeAzure {
	t.Helper()
	f := &fakeAzure{secrets: map[string]string{
		"orders-db":  `{"username":"piperoom_ro","password":"` + canary + `"}`,
		"kafka-pass": canary,
		"other-one":  "x",
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{tenant}/oauth2/v2.0/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.lastForm = map[string]string{"tenant": r.PathValue("tenant")}
		for k := range r.PostForm {
			f.lastForm[k] = r.PostForm.Get(k)
		}
		f.mu.Unlock()
		if f.tokenFail.Load() {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"AADSTS7000215: Invalid client secret provided."}`))
			return
		}
		n := f.tokenN.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("tok-%d", n), "expires_in": 3600, "token_type": "Bearer"})
	})
	mux.HandleFunc("GET /imds", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata") != "true" || r.URL.Query().Get("resource") != "https://vault.azure.net" {
			http.Error(w, `{"error":"invalid_request"}`, http.StatusBadRequest)
			return
		}
		n := f.tokenN.Add(1)
		// IMDS sends expires_in as a string.
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("tok-%d", n), "expires_in": "86399"})
	})
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
		f.mu.Unlock()
		if h == "" || f.rejectAll.Load() || (valid != "" && h != "Bearer "+valid) {
			w.Header().Set("WWW-Authenticate", `Bearer authorization="https://login.microsoftonline.com/x", resource="https://vault.azure.net"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"Unauthorized","message":"AKV10000: Request is missing a Bearer or PoP token."}}`))
			return false
		}
		if r.URL.Query().Get("api-version") != APIVersion {
			http.Error(w, `{"error":{"code":"BadParameter"}}`, http.StatusBadRequest)
			return false
		}
		return true
	}
	mux.HandleFunc("GET /secrets", func(w http.ResponseWriter, r *http.Request) {
		if !auth(w, r) {
			return
		}
		if f.listDeny.Load() {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"Caller is not authorized to perform action on resource."}}`))
			return
		}
		base := "http://" + r.Host
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.URL.Query().Get("$skiptoken") == "" {
			next := base + "/secrets?api-version=" + APIVersion + "&$skiptoken=p2"
			if f.badNext {
				next = "http://evil.example/secrets?api-version=" + APIVersion
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value":    []map[string]any{{"id": base + "/secrets/orders-db"}, {"id": base + "/secrets/other-one"}},
				"nextLink": next,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": []map[string]any{{"id": base + "/secrets/Orders-Archive"}}})
	})
	read := func(w http.ResponseWriter, r *http.Request) {
		if !auth(w, r) {
			return
		}
		if f.readDeny.Load() {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"The user does not have secrets get permission."}}`))
			return
		}
		name := r.PathValue("name")
		if name == "disabled-one" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":"Forbidden","message":"Operation get is not allowed on a disabled secret.","innererror":{"code":"SecretDisabled"}}}`))
			return
		}
		f.mu.Lock()
		v, ok := f.secrets[name]
		f.mu.Unlock()
		if !ok || (r.PathValue("ver") != "" && r.PathValue("ver") != version) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"SecretNotFound","message":"A secret with (name/id) ` + name + ` was not found in this key vault."}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"value": v, "id": "http://" + r.Host + "/secrets/" + name + "/" + version,
			"attributes": map[string]any{"enabled": true},
		})
	}
	mux.HandleFunc("GET /secrets/{name}", read)
	mux.HandleFunc("GET /secrets/{name}/{ver}", read)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func noEnv(string) string { return "" }

func (f *fakeAzure) clientSecretProvider(t *testing.T) *Provider {
	t.Helper()
	p, err := New(Options{
		VaultURL: f.srv.URL, TenantID: tenant, ClientID: "app-1", ClientSecret: netsec.Secret("cs-" + canary),
		AuthorityHost: f.srv.URL, Getenv: noEnv,
	})
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

func TestResolveClientSecret(t *testing.T) {
	f := newFake(t)
	p := f.clientSecretProvider(t)
	v, err := p.Resolve(ctx(t), secrets.Ref{Provider: "kv", Path: "kafka-pass"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Data.Reveal() != canary || v.Version != version {
		t.Fatalf("got version %q", v.Version)
	}
	f.mu.Lock()
	form := f.lastForm
	f.mu.Unlock()
	if form["tenant"] != tenant || form["client_id"] != "app-1" || form["grant_type"] != "client_credentials" ||
		form["scope"] != "https://vault.azure.net/.default" || form["client_secret"] != "cs-"+canary {
		t.Fatalf("token form = %v", form)
	}
	// Pinned version and the token cache: a second read issues no new token.
	if _, err := p.Resolve(ctx(t), secrets.Ref{Provider: "kv", Path: "kafka-pass", Version: version}); err != nil {
		t.Fatal(err)
	}
	if n := f.tokenN.Load(); n != 1 {
		t.Fatalf("token fetched %d times, want 1", n)
	}
	keys, err := p.Keys(ctx(t), secrets.Ref{Provider: "kv", Path: "orders-db", Key: "password"})
	if err != nil || strings.Join(keys, ",") != "password,username" {
		t.Fatalf("keys = %v, %v", keys, err)
	}
	keys, err = p.Keys(ctx(t), secrets.Ref{Provider: "kv", Path: "kafka-pass"})
	if err != nil || len(keys) != 0 || keys == nil {
		t.Fatalf("plain keys = %#v, %v", keys, err)
	}
}

func TestManagedIdentity(t *testing.T) {
	f := newFake(t)
	p, err := New(Options{VaultURL: f.srv.URL, IMDSEndpoint: f.srv.URL + "/imds", Getenv: noEnv})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.tokens.mode != AuthManagedIdentity {
		t.Fatalf("mode = %s", p.tokens.mode)
	}
	v, err := p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"})
	if err != nil || v.Data.Reveal() != canary {
		t.Fatalf("resolve: %v", err)
	}
	if exp := p.tokens.exp; time.Until(exp) < 23*time.Hour {
		t.Fatalf("string expires_in not parsed: %v", exp)
	}
}

func TestWorkloadIdentity(t *testing.T) {
	f := newFake(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "azure-identity-token")
	if err := os.WriteFile(file, []byte("fed-jwt-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"AZURE_TENANT_ID": tenant, "AZURE_CLIENT_ID": "app-2", "AZURE_FEDERATED_TOKEN_FILE": file,
		"AZURE_AUTHORITY_HOST": f.srv.URL}
	p, err := New(Options{VaultURL: f.srv.URL, Getenv: func(k string) string { return env[k] }})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	form := f.lastForm
	f.mu.Unlock()
	if form["client_assertion"] != "fed-jwt-1" || form["client_id"] != "app-2" ||
		form["client_assertion_type"] != "urn:ietf:params:oauth:client-assertion-type:jwt-bearer" || form["client_secret"] != "" {
		t.Fatalf("form = %v", form)
	}
}

func TestRetryOnceAfter401(t *testing.T) {
	f := newFake(t)
	p := f.clientSecretProvider(t)
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"}); err != nil {
		t.Fatal(err)
	}
	// The vault now accepts only the next token: the cached one is stale.
	f.mu.Lock()
	f.validTok = "tok-2"
	f.mu.Unlock()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"}); err != nil {
		t.Fatalf("after rotation: %v", err)
	}
	// Always rejected: one retry, then ErrLogin.
	f.rejectAll.Store(true)
	before := f.tokenN.Load()
	_, err := p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"})
	if !errors.Is(err, secrets.ErrLogin) {
		t.Fatalf("err = %v", err)
	}
	if got := f.tokenN.Load() - before; got != 1 {
		t.Fatalf("new tokens after 401 = %d, want 1", got)
	}
}

func TestErrorPaths(t *testing.T) {
	f := newFake(t)
	p := f.clientSecretProvider(t)
	_, err := p.Resolve(ctx(t), secrets.Ref{Path: "missing"})
	if !errors.Is(err, secrets.ErrNotFound) {
		t.Fatalf("404: %v", err)
	}
	f.readDeny.Store(true)
	_, err = p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"})
	if !errors.Is(err, secrets.ErrAccessDenied) {
		t.Fatalf("403: %v", err)
	}
	f.readDeny.Store(false)

	for _, ref := range []secrets.Ref{{Path: "a/b"}, {Path: "under_score"}, {Path: "ok", Version: "v1"}, {Path: ""}} {
		if _, err := p.Resolve(ctx(t), ref); !errors.Is(err, secrets.ErrInvalidRef) {
			t.Errorf("%+v: err = %v", ref, err)
		}
	}

	f.tokenFail.Store(true)
	p2 := f.clientSecretProvider(t)
	_, err = p2.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"})
	if !errors.Is(err, secrets.ErrLogin) || !strings.Contains(err.Error(), "AADSTS7000215") {
		t.Fatalf("token 400: %v", err)
	}
	if strings.Contains(err.Error(), canary) {
		t.Fatal("client secret leaked into error")
	}
}

func TestTimeoutAndDown(t *testing.T) {
	f := newFake(t)
	p := f.clientSecretProvider(t)
	f.delay.Store(int64(2 * time.Second))
	c, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := p.Resolve(c, secrets.Ref{Path: "kafka-pass"})
	if !errors.Is(err, secrets.ErrUnreachable) {
		t.Fatalf("timeout: %v", err)
	}

	down := httptest.NewServer(http.NotFoundHandler())
	url := down.URL
	down.Close()
	p2, err := New(Options{VaultURL: url, TenantID: tenant, ClientID: "a", ClientSecret: "s", AuthorityHost: url, Getenv: noEnv})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if _, err := p2.Resolve(ctx(t), secrets.Ref{Path: "x"}); !errors.Is(err, secrets.ErrUnreachable) {
		t.Fatalf("down: %v", err)
	}
	checks := p2.Check(ctx(t), nil)
	if checks[0].ID != secrets.CheckReach || checks[0].Status != spec.CheckFail || !spec.Failed(checks) {
		t.Fatalf("checks = %+v", checks)
	}
}

func TestList(t *testing.T) {
	f := newFake(t)
	p := f.clientSecretProvider(t)
	names, err := p.List(ctx(t), "", 200)
	if err != nil || strings.Join(names, ",") != "Orders-Archive,orders-db,other-one" {
		t.Fatalf("names = %v, %v", names, err)
	}
	names, err = p.List(ctx(t), "orders", 200)
	if err != nil || strings.Join(names, ",") != "Orders-Archive,orders-db" {
		t.Fatalf("prefix names = %v, %v", names, err)
	}
	names, err = p.List(ctx(t), "", 1)
	if err != nil || len(names) != 1 {
		t.Fatalf("limited = %v, %v", names, err)
	}
	f.mu.Lock()
	f.badNext = true
	f.mu.Unlock()
	if _, err := p.List(ctx(t), "", 200); err == nil || !strings.Contains(err.Error(), "outside the vault") {
		t.Fatalf("foreign nextLink: %v", err)
	}
	f.listDeny.Store(true)
	if _, err := p.List(ctx(t), "", 200); !errors.Is(err, secrets.ErrListUnsupported) {
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
	p := f.clientSecretProvider(t)
	sample := &secrets.Ref{Provider: "kv", Path: "orders-db", Key: "password"}
	checks := p.Check(ctx(t), sample)
	m := byID(checks)
	for _, id := range []string{secrets.CheckReach, secrets.CheckLogin, secrets.CheckRead} {
		if m[id].Status != spec.CheckPass {
			t.Fatalf("%s = %+v", id, m[id])
		}
	}
	if m[secrets.CheckTooMuch].Status != spec.CheckSkipped || spec.Failed(checks) {
		t.Fatalf("checks = %+v", checks)
	}
	if !strings.Contains(m[secrets.CheckRead].Detail, version) {
		t.Fatalf("read detail = %q", m[secrets.CheckRead].Detail)
	}
	assertNoCanary(t, checks)

	// Missing key: fail, listing key names only.
	m = byID(p.Check(ctx(t), &secrets.Ref{Path: "orders-db", Key: "pw"}))
	if m[secrets.CheckRead].Status != spec.CheckFail || !strings.Contains(m[secrets.CheckRead].Detail, "password, username") {
		t.Fatalf("missing key = %+v", m[secrets.CheckRead])
	}

	// List denied: a warning, not a failure.
	f.listDeny.Store(true)
	checks = p.Check(ctx(t), nil)
	m = byID(checks)
	if m[secrets.CheckListNames].Status != spec.CheckWarn || m[secrets.CheckLogin].Status != spec.CheckPass || spec.Failed(checks) {
		t.Fatalf("list denied = %+v", checks)
	}
	f.listDeny.Store(false)

	// Token refused by Entra ID.
	f.tokenFail.Store(true)
	p2 := f.clientSecretProvider(t)
	m = byID(p2.Check(ctx(t), sample))
	if m[secrets.CheckReach].Status != spec.CheckPass || m[secrets.CheckLogin].Status != spec.CheckFail ||
		m[secrets.CheckRead].Status != spec.CheckSkipped || m[secrets.CheckLogin].Fix == "" {
		t.Fatalf("login fail = %+v", m)
	}
	f.tokenFail.Store(false)

	// Token issued but rejected by the vault (wrong tenant).
	f.rejectAll.Store(true)
	m = byID(p.Check(ctx(t), sample))
	if m[secrets.CheckLogin].Status != spec.CheckFail || !strings.Contains(m[secrets.CheckLogin].Detail, "rejected") {
		t.Fatalf("vault 401 = %+v", m[secrets.CheckLogin])
	}
}

func assertNoCanary(t *testing.T, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if strings.Contains(string(b), canary) || strings.Contains(fmt.Sprintf("%v %+v %#v", v, v, v), canary) {
		t.Fatalf("secret value leaked: %s", b)
	}
}

func TestNoLeakInFormatting(t *testing.T) {
	f := newFake(t)
	p := f.clientSecretProvider(t)
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"}); err != nil {
		t.Fatal(err)
	}
	s := fmt.Sprintf("%v %+v %#v %s", p, p, p, p)
	if strings.Contains(s, canary) || strings.Contains(s, "tok-") {
		t.Fatalf("provider formatting leaks: %s", s)
	}
}

func TestClosed(t *testing.T) {
	f := newFake(t)
	p := f.clientSecretProvider(t)
	p.Close()
	p.Close()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"}); !errors.Is(err, secrets.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	cases := []struct {
		name string
		opts Options
	}{
		{"http to network", Options{VaultURL: "http://kv.vault.azure.net"}},
		{"path in url", Options{VaultURL: "https://kv.vault.azure.net/secrets"}},
		{"no url", Options{}},
		{"secret without tenant", Options{VaultURL: "https://kv.vault.azure.net", ClientSecret: "s", ClientID: "a"}},
		{"bad tenant", Options{VaultURL: "https://kv.vault.azure.net", ClientSecret: "s", ClientID: "a", TenantID: "../x"}},
		{"both secrets", Options{VaultURL: "https://kv.vault.azure.net", ClientSecret: "s", ClientSecretFile: "/f", ClientID: "a", TenantID: tenant}},
		{"workload without file", Options{VaultURL: "https://kv.vault.azure.net", Auth: AuthWorkloadIdentity, ClientID: "a", TenantID: tenant}},
		{"unknown auth", Options{VaultURL: "https://kv.vault.azure.net", Auth: "password"}},
		{"http authority", Options{VaultURL: "https://kv.vault.azure.net", AuthorityHost: "http://login.example"}},
	}
	for _, c := range cases {
		c.opts.Getenv = noEnv
		if _, err := New(c.opts); !errors.Is(err, secrets.ErrBadProviderConfig) {
			t.Errorf("%s: err = %v", c.name, err)
		}
	}
}

func TestResourceFor(t *testing.T) {
	for host, want := range map[string]string{
		"acme.vault.azure.net":         "https://vault.azure.net",
		"acme.vault.azure.cn":          "https://vault.azure.cn",
		"acme.vault.usgovcloudapi.net": "https://vault.usgovcloudapi.net",
		"127.0.0.1":                    "https://vault.azure.net",
	} {
		if got := resourceFor(host); got != want {
			t.Errorf("%s: %s, want %s", host, got, want)
		}
	}
}

func TestFactory(t *testing.T) {
	f := newFake(t)
	specs, err := secrets.ParseProviders("kv=azure-key-vault:" + f.srv.URL + ";tenant_id=" + tenant +
		";client_id=app-1;client_secret=cs-" + canary + ";authority_host=" + f.srv.URL + ";timeout=3s")
	if err != nil {
		t.Fatal(err)
	}
	fs := secrets.DefaultFactories()
	Register(fs)
	p, err := fs[secrets.TypeAzureKeyVault](ctx(t), specs[0], secrets.Deps{Getenv: noEnv})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Resolve(ctx(t), secrets.Ref{Path: "kafka-pass"}); err != nil {
		t.Fatal(err)
	}
	specs[0].Options["namspace"] = "x"
	if _, err := Factory(ctx(t), specs[0], secrets.Deps{Getenv: noEnv}); !errors.Is(err, secrets.ErrBadProviderConfig) ||
		!strings.Contains(err.Error(), "namspace") {
		t.Fatalf("unknown option: %v", err)
	}
	delete(specs[0].Options, "namspace")
	specs[0].Options["timeout"] = "soon"
	if _, err := Factory(ctx(t), specs[0], secrets.Deps{Getenv: noEnv}); !errors.Is(err, secrets.ErrBadProviderConfig) {
		t.Fatalf("bad timeout: %v", err)
	}
}

func TestSetupScript(t *testing.T) {
	s := SetupScript("acme-prod", "11111111-2222-3333-4444-555555555555", nil)
	if !strings.Contains(s, `--scope "$VAULT_ID"`) || !strings.Contains(s, "Key Vault Secrets User") || !strings.Contains(s, "acme-prod") {
		t.Fatal(s)
	}
	s = SetupScript("acme-prod", "11111111-2222-3333-4444-555555555555", []string{"orders-db", "bad;rm -rf /"})
	if !strings.Contains(s, `"$VAULT_ID/secrets/orders-db"`) || strings.Contains(s, "rm -rf") {
		t.Fatal(s)
	}
	s = SetupScript("$(evil)", "not-a-guid", nil)
	if strings.Contains(s, "evil") || !strings.Contains(s, "<vault-name>") || !strings.Contains(s, "<piperoom-identity-object-id>") {
		t.Fatal(s)
	}
}

func TestDisabledSecretIsNotFound(t *testing.T) {
	f := newFake(t)
	p := f.clientSecretProvider(t)
	_, err := p.Resolve(ctx(t), secrets.Ref{Path: "disabled-one"})
	if !errors.Is(err, secrets.ErrNotFound) || errors.Is(err, secrets.ErrAccessDenied) {
		t.Fatalf("disabled: %v", err)
	}
}

// TestTokenWaitersKeepOwnDeadline: a caller waiting on another caller's
// login gives up on its own ctx, and a login that failed only because its
// caller's deadline was short is retried by a waiter that still has time.
func TestTokenWaitersKeepOwnDeadline(t *testing.T) {
	release := make(chan struct{})
	var fetches atomic.Int32
	ts := &tokenSource{now: time.Now, mode: AuthClientSecret}
	ts.fetch = func(ctx context.Context) (netsec.Secret, time.Time, error) {
		n := fetches.Add(1)
		if n == 1 {
			select {
			case <-release:
			case <-ctx.Done():
				return "", time.Time{}, fmt.Errorf("%w: %w", secrets.ErrUnreachable, ctx.Err())
			}
		}
		return netsec.Secret(fmt.Sprintf("tok-%d", n)), time.Now().Add(time.Hour), nil
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() { _, err := ts.token(leaderCtx); leaderErr <- err }()
	for {
		ts.mu.Lock()
		started := ts.inflight != nil
		ts.mu.Unlock()
		if started {
			break
		}
		time.Sleep(time.Millisecond)
	}

	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := ts.token(short); !errors.Is(err, secrets.ErrUnreachable) {
		t.Fatalf("short waiter: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("short waiter blocked %v behind the login", d)
	}

	waiter := make(chan netsec.Secret, 1)
	go func() {
		tok, err := ts.token(ctx(t))
		if err != nil {
			t.Errorf("long waiter: %v", err)
		}
		waiter <- tok
	}()
	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, secrets.ErrUnreachable) {
		t.Fatalf("leader: %v", err)
	}
	if tok := <-waiter; tok != "tok-2" {
		t.Fatalf("waiter token %q, want tok-2 from its own login", tok.Reveal())
	}
	close(release)
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches = %d, want 2", got)
	}
}
