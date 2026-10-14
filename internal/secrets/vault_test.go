package secrets

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// fakeVault is a small Vault HTTP API.
type fakeVault struct {
	t *testing.T

	mu         sync.Mutex
	tokens     map[string]bool
	namespaces []string
	logins     atomic.Int64
	renews     atomic.Int64
	creds      atomic.Int64
	kv2Version int
	kv2        map[int]map[string]any
	down       bool
	slow       time.Duration
	jwt        string
	leaseTTL   int
	caps       map[string][]string
	denyList   bool
}

func newFakeVault(t *testing.T) (*fakeVault, *httptest.Server) {
	fv := &fakeVault{
		t: t, tokens: map[string]bool{"root-token": true}, kv2Version: 2,
		kv2: map[int]map[string]any{
			1: {"password": "old-pw", "username": "app"},
			2: {"password": "new-pw", "username": "app", "port": 5432},
		},
		jwt: "header.payload.sig", leaseTTL: 3600,
		caps: map[string][]string{},
	}
	srv := httptest.NewServer(fv)
	t.Cleanup(srv.Close)
	return fv, srv
}

func (fv *fakeVault) reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (fv *fakeVault) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fv.mu.Lock()
	down, slow := fv.down, fv.slow
	fv.namespaces = append(fv.namespaces, r.Header.Get("X-Vault-Namespace"))
	fv.mu.Unlock()
	if slow > 0 {
		select {
		case <-time.After(slow):
		case <-r.Context().Done():
			return
		}
	}
	if down {
		fv.reply(w, 500, map[string]any{"errors": []string{"internal error"}})
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)

	switch path {
	case "sys/health":
		fv.reply(w, 200, map[string]any{"initialized": true, "sealed": false, "version": "1.17.0"})
		return
	case "auth/approle/login":
		if body["role_id"] != "rid" || body["secret_id"] != "sid" {
			fv.reply(w, 400, map[string]any{"errors": []string{"invalid role or secret ID"}})
			return
		}
		fv.issue(w)
		return
	case "auth/kubernetes/login", "auth/jwt/login", "auth/custom-k8s/login":
		if body["role"] != "piperoom" || body["jwt"] != fv.jwt {
			fv.reply(w, 403, map[string]any{"errors": []string{"permission denied"}})
			return
		}
		fv.issue(w)
		return
	}

	tok := r.Header.Get("X-Vault-Token")
	fv.mu.Lock()
	ok := fv.tokens[tok]
	fv.mu.Unlock()
	if !ok {
		fv.reply(w, 403, map[string]any{"errors": []string{"permission denied"}})
		return
	}
	switch {
	case path == "auth/token/lookup-self":
		fv.reply(w, 200, map[string]any{"data": map[string]any{"ttl": 0, "renewable": false}})
	case path == "auth/token/renew-self":
		fv.reply(w, 200, map[string]any{"auth": map[string]any{"client_token": tok, "lease_duration": 3600, "renewable": true}})
	case path == "secret/data/app" && r.Method == http.MethodGet:
		v := fv.kv2Version
		if q := r.URL.Query().Get("version"); q != "" {
			fmt.Sscanf(q, "%d", &v)
		}
		data, ok := fv.kv2[v]
		if !ok {
			fv.reply(w, 404, map[string]any{"errors": []string{}})
			return
		}
		fv.reply(w, 200, map[string]any{"data": map[string]any{"data": data, "metadata": map[string]any{"version": v}}})
	case path == "kv/app":
		fv.reply(w, 200, map[string]any{"lease_duration": 2764800, "data": map[string]any{"token": "kv1-token"}})
	case path == "database/creds/ro":
		n := fv.creds.Add(1)
		fv.reply(w, 200, map[string]any{
			"lease_id": fmt.Sprintf("database/creds/ro/lease%d", n), "lease_duration": fv.leaseTTL, "renewable": true,
			"data": map[string]any{"username": fmt.Sprintf("v-ro-%d", n), "password": fmt.Sprintf("pw-%d", n)},
		})
	case path == "sys/leases/renew" && r.Method == http.MethodPut:
		fv.renews.Add(1)
		if !strings.HasPrefix(fmt.Sprint(body["lease_id"]), "database/creds/ro/") {
			fv.reply(w, 400, map[string]any{"errors": []string{"lease not found"}})
			return
		}
		fv.reply(w, 200, map[string]any{"lease_id": body["lease_id"], "lease_duration": fv.leaseTTL, "renewable": true})
	case path == "secret/metadata/piperoom" && r.Method == "LIST":
		if fv.denyList {
			fv.reply(w, 403, map[string]any{"errors": []string{"permission denied"}})
			return
		}
		fv.reply(w, 200, map[string]any{"data": map[string]any{"keys": []string{"orders-db", "kafka", "sub/"}}})
	case path == "sys/capabilities-self":
		out := map[string]any{}
		for _, p := range body["paths"].([]any) {
			c := fv.caps[p.(string)]
			if c == nil {
				c = []string{"deny"}
			}
			out[p.(string)] = c
		}
		fv.reply(w, 200, out)
	default:
		fv.reply(w, 404, map[string]any{"errors": []string{}})
	}
}

func (fv *fakeVault) issue(w http.ResponseWriter) {
	n := fv.logins.Add(1)
	tok := fmt.Sprintf("s.login-%d", n)
	fv.mu.Lock()
	fv.tokens[tok] = true
	fv.mu.Unlock()
	fv.reply(w, 200, map[string]any{"auth": map[string]any{"client_token": tok, "lease_duration": 3600, "renewable": true}})
}

func (fv *fakeVault) revokeAll() {
	fv.mu.Lock()
	fv.tokens = map[string]bool{}
	fv.mu.Unlock()
}

func newVault(t *testing.T, opts VaultOptions) *VaultProvider {
	t.Helper()
	p, err := NewVaultProvider(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestVaultKV2(t *testing.T) {
	fv, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Auth: VaultAuthToken, Token: "root-token", Namespace: "team-a"})
	ctx := context.Background()
	v, err := p.Resolve(ctx, Ref{Path: "secret/app"})
	if err != nil {
		t.Fatal(err)
	}
	if pw, _ := ExtractKey(v.Data, "password"); pw.Reveal() != "new-pw" || v.Version != "2" {
		t.Fatalf("latest: %q version %q", pw.Reveal(), v.Version)
	}
	if port, _ := ExtractKey(v.Data, "port"); port.Reveal() != "5432" {
		t.Fatalf("port %q", port.Reveal())
	}
	v, err = p.Resolve(ctx, Ref{Path: "secret/app", Version: "1"})
	if pw, _ := ExtractKey(v.Data, "password"); err != nil || pw.Reveal() != "old-pw" || v.Version != "1" {
		t.Fatalf("pinned: %q %v", pw.Reveal(), err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "secret/app", Version: "9"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing version err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "secret/app", Version: "x"}); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("bad version err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "secret/missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("404 err = %v", err)
	}
	keys, _ := p.Keys(ctx, Ref{Path: "secret/app", Key: "password"})
	if !reflect.DeepEqual(keys, []string{"password", "port", "username"}) {
		t.Fatalf("keys = %v", keys)
	}
	fv.mu.Lock()
	defer fv.mu.Unlock()
	for _, ns := range fv.namespaces {
		if ns != "team-a" {
			t.Fatalf("namespace header %q", ns)
		}
	}
}

func TestVaultKV1AndVersionRule(t *testing.T) {
	_, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	v, err := p.Resolve(context.Background(), Ref{Path: "kv/app"})
	if err != nil {
		t.Fatal(err)
	}
	if tok, _ := ExtractKey(v.Data, "token"); tok.Reveal() != "kv1-token" || !v.ExpiresAt.IsZero() {
		t.Fatalf("kv1: %q expires %v", tok.Reveal(), v.ExpiresAt)
	}
	if _, err := p.Resolve(context.Background(), Ref{Path: "kv/app", Version: "1"}); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("kv1 version err = %v", err)
	}
}

func TestVaultDatabaseLeaseThroughRegistry(t *testing.T) {
	fv, srv := newFakeVault(t)
	fv.leaseTTL = 1 // seconds: renew at ~667 ms
	p, err := NewVaultProvider(VaultOptions{Address: srv.URL, Token: "root-token"})
	if err != nil {
		t.Fatal(err)
	}
	r := newTestRegistry(t, Options{})
	mustRegister(t, r, "ws", "vault-corp", p)
	ctx := context.Background()
	user, err := r.Resolve(ctx, "ws", "secret://vault-corp/database/creds/ro#username")
	if err != nil {
		t.Fatal(err)
	}
	pw, err := r.Resolve(ctx, "ws", "secret://vault-corp/database/creds/ro#password")
	if err != nil {
		t.Fatal(err)
	}
	// One lease: username and password belong together.
	if fv.creds.Load() != 1 || user.Reveal() != "v-ro-1" || pw.Reveal() != "pw-1" {
		t.Fatalf("creds=%d user=%q pw=%q", fv.creds.Load(), user.Reveal(), pw.Reveal())
	}
	deadline := time.Now().Add(3 * time.Second)
	for fv.renews.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("lease never renewed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := r.Entries("ws"); len(got) != 1 || got[0].ExpiresAt.IsZero() {
		t.Fatalf("entries = %+v", got)
	}
	keys, _ := p.Keys(ctx, Ref{Path: "database/creds/ro"})
	if !reflect.DeepEqual(keys, []string{"password", "username"}) || fv.creds.Load() != 1 {
		t.Fatalf("keys %v issued creds %d (Keys must not issue a login)", keys, fv.creds.Load())
	}
	r.Close()
	n := fv.renews.Load()
	time.Sleep(900 * time.Millisecond)
	if fv.renews.Load() != n {
		t.Fatal("renewal continued after Close")
	}
}

func TestVaultRenewLeaseError(t *testing.T) {
	_, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	if _, err := p.RenewLease(context.Background(), Value{LeaseID: "other/lease"}); err == nil {
		t.Fatal("want error")
	}
	if _, err := p.RenewLease(context.Background(), Value{}); err == nil {
		t.Fatal("want error for no lease")
	}
}

func TestVaultAuthMethods(t *testing.T) {
	fv, srv := newFakeVault(t)
	dir := t.TempDir()
	jwtFile := filepath.Join(dir, "token")
	if err := os.WriteFile(jwtFile, []byte(fv.jwt+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sidFile := filepath.Join(dir, "sid")
	if err := os.WriteFile(sidFile, []byte("sid"), 0o600); err != nil {
		t.Fatal(err)
	}
	tokFile := filepath.Join(dir, "vault-token")
	if err := os.WriteFile(tokFile, []byte("root-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := map[string]VaultOptions{
		"approle":          {Auth: VaultAuthAppRole, RoleID: "rid", SecretID: "sid"},
		"approle file":     {Auth: VaultAuthAppRole, RoleID: "rid", SecretIDFile: sidFile},
		"kubernetes":       {Auth: VaultAuthKubernetes, Role: "piperoom", JWTFile: jwtFile},
		"kubernetes mount": {Auth: VaultAuthKubernetes, Role: "piperoom", JWTFile: jwtFile, AuthMount: "custom-k8s"},
		"jwt":              {Auth: VaultAuthJWT, Role: "piperoom", JWT: netsec.Secret(fv.jwt)},
		"token file":       {Auth: VaultAuthToken, TokenFile: tokFile},
	}
	for name, o := range cases {
		t.Run(name, func(t *testing.T) {
			o.Address = srv.URL
			p := newVault(t, o)
			v, err := p.Resolve(context.Background(), Ref{Path: "secret/app"})
			if err != nil {
				t.Fatal(err)
			}
			if pw, _ := ExtractKey(v.Data, "password"); pw.Reveal() != "new-pw" {
				t.Fatalf("pw %q", pw.Reveal())
			}
		})
	}
	t.Run("bad secret id", func(t *testing.T) {
		p := newVault(t, VaultOptions{Address: srv.URL, Auth: VaultAuthAppRole, RoleID: "rid", SecretID: "wrong"})
		_, err := p.Resolve(context.Background(), Ref{Path: "secret/app"})
		if !errors.Is(err, ErrLogin) || strings.Contains(err.Error(), "wrong") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bad jwt", func(t *testing.T) {
		p := newVault(t, VaultOptions{Address: srv.URL, Auth: VaultAuthJWT, Role: "piperoom", JWT: "forged"})
		if _, err := p.Resolve(context.Background(), Ref{Path: "secret/app"}); !errors.Is(err, ErrLogin) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bad static token", func(t *testing.T) {
		p := newVault(t, VaultOptions{Address: srv.URL, Token: "nope"})
		if _, err := p.Resolve(context.Background(), Ref{Path: "secret/app"}); !errors.Is(err, ErrLogin) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestVaultReloginAfterRevokedToken(t *testing.T) {
	fv, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Auth: VaultAuthAppRole, RoleID: "rid", SecretID: "sid"})
	ctx := context.Background()
	if _, err := p.Resolve(ctx, Ref{Path: "secret/app"}); err != nil {
		t.Fatal(err)
	}
	fv.revokeAll()
	if _, err := p.Resolve(ctx, Ref{Path: "secret/app"}); err != nil {
		t.Fatalf("after revoke: %v", err)
	}
	if fv.logins.Load() != 2 {
		t.Fatalf("logins = %d, want 2", fv.logins.Load())
	}
}

func TestVaultUnreachable(t *testing.T) {
	fv, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	fv.mu.Lock()
	fv.down = true
	fv.mu.Unlock()
	if _, err := p.Resolve(context.Background(), Ref{Path: "secret/app"}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("500 err = %v", err)
	}
	fv.mu.Lock()
	fv.down, fv.slow = false, time.Second
	fv.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Resolve(ctx, Ref{Path: "secret/app"}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("timeout err = %v", err)
	}
	srv.Close()
	p2 := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	if _, err := p2.Resolve(context.Background(), Ref{Path: "secret/app"}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("closed server err = %v", err)
	}
}

func TestVaultList(t *testing.T) {
	fv, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token", Prefix: "secret/piperoom/"})
	ctx := context.Background()
	got, err := p.List(ctx, "", 10)
	want := []string{"secret/piperoom/kafka", "secret/piperoom/orders-db", "secret/piperoom/sub/"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("List = %v %v", got, err)
	}
	got, _ = p.List(ctx, "secret/piperoom/or", 10)
	if !reflect.DeepEqual(got, []string{"secret/piperoom/orders-db"}) {
		t.Fatalf("List prefix = %v", got)
	}
	if got, _ := p.List(ctx, "secret/piperoom/", 1); len(got) != 1 {
		t.Fatalf("limit = %v", got)
	}
	if got, err := p.List(ctx, "secret/piperoom/none/", 10); err != nil || len(got) != 0 {
		t.Fatalf("404 = %v %v", got, err)
	}
	// Outside the configured prefix (names can be sensitive, §9).
	for _, prefix := range []string{"secret/none/", "secret/", "secret/piperoom", "kv/"} {
		if _, err := p.List(ctx, prefix, 10); !errors.Is(err, ErrAccessDenied) {
			t.Fatalf("List(%q) err = %v, want ErrAccessDenied", prefix, err)
		}
	}
	// Without a configured prefix, Vault's own endpoints are still refused.
	open := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	if _, err := open.List(ctx, "auth/token/accessors/", 10); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("List auth/ err = %v", err)
	}
	fv.mu.Lock()
	fv.denyList = true
	fv.mu.Unlock()
	if _, err := p.List(ctx, "secret/piperoom/", 10); !errors.Is(err, ErrListUnsupported) {
		t.Fatalf("403 err = %v", err)
	}
}

func TestVaultCheck(t *testing.T) {
	fv, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Auth: VaultAuthAppRole, RoleID: "rid", SecretID: "sid", Prefix: "secret/piperoom/"})
	ctx := context.Background()
	checks := p.Check(ctx, &Ref{Path: "secret/app", Key: "password"})
	byID := map[string]spec.Check{}
	for _, c := range checks {
		byID[c.ID] = c
	}
	if spec.Failed(checks) || byID[CheckReach].Status != spec.CheckPass || byID[CheckLogin].Status != spec.CheckPass ||
		byID[CheckRead].Status != spec.CheckPass || byID[CheckTooMuch].Status != spec.CheckPass ||
		byID[CheckLease].Status != spec.CheckSkipped {
		t.Fatalf("checks = %+v", checks)
	}
	if !strings.Contains(byID[CheckRead].Detail, "version 2") {
		t.Fatalf("read detail %q", byID[CheckRead].Detail)
	}
	for _, c := range checks {
		if strings.Contains(c.Detail+c.Fix, "new-pw") {
			t.Fatal("a check shows the secret value")
		}
	}

	fv.mu.Lock()
	fv.caps["secret/data/piperoom-access-probe-outside-prefix"] = []string{"read"}
	fv.caps["secret/data/app"] = []string{"read", "update"}
	fv.mu.Unlock()
	checks = p.Check(ctx, &Ref{Path: "secret/app"})
	for _, c := range checks {
		if c.ID == CheckTooMuch && (c.Status != spec.CheckWarn || !strings.Contains(c.Detail, "can write") || !strings.Contains(c.Detail, "outside")) {
			t.Fatalf("too much access check = %+v", c)
		}
	}

	// Static token: warn.
	pt := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	for _, c := range pt.Check(ctx, nil) {
		if c.ID == CheckLogin && (c.Status != spec.CheckWarn || !strings.Contains(c.Detail, "static token")) {
			t.Fatalf("static token check = %+v", c)
		}
	}
	// Lease check on dynamic credentials.
	for _, c := range pt.Check(ctx, &Ref{Path: "database/creds/ro", Key: "password"}) {
		if c.ID == CheckLease && c.Status != spec.CheckPass {
			t.Fatalf("lease check = %+v", c)
		}
	}
	// Bad login fails and skips the rest.
	pb := newVault(t, VaultOptions{Address: srv.URL, Auth: VaultAuthAppRole, RoleID: "rid", SecretID: "bad"})
	checks = pb.Check(ctx, &Ref{Path: "secret/app"})
	if !spec.Failed(checks) || checks[1].Status != spec.CheckFail || checks[2].Status != spec.CheckSkipped {
		t.Fatalf("bad login checks = %+v", checks)
	}
	// Unreachable.
	srv.Close()
	checks = pt.Check(ctx, nil)
	if checks[0].ID != CheckReach || checks[0].Status != spec.CheckFail {
		t.Fatalf("down checks = %+v", checks)
	}
}

func TestVaultOptionsValidation(t *testing.T) {
	bad := []VaultOptions{
		{Address: "http://vault.corp:8200", Token: "t"},  // plain HTTP off localhost
		{Address: "ftp://vault", Token: "t"},             // scheme
		{Address: "https://u:p@vault", Token: "t"},       // credentials in URL
		{Address: "https://vault", Auth: VaultAuthToken}, // no token
		{Address: "https://vault", Auth: VaultAuthAppRole, RoleID: "r"},
		{Address: "https://vault", Auth: VaultAuthKubernetes},
		{Address: "https://vault", Auth: VaultAuthJWT, Role: "r"},
		{Address: "https://vault", Auth: "ldap"},
		{Address: "https://vault", Token: "t", CAPEM: "not pem"},
	}
	for i, o := range bad {
		if _, err := NewVaultProvider(o); !errors.Is(err, ErrBadProviderConfig) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
}

func TestVaultTLSWithPastedCA(t *testing.T) {
	fv := &fakeVault{t: t, tokens: map[string]bool{"root-token": true}, kv2Version: 2,
		kv2: map[int]map[string]any{2: {"password": "p"}}, caps: map[string][]string{}}
	srv := httptest.NewTLSServer(fv)
	defer srv.Close()
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token", CAPEM: caPEM})
	if _, err := p.Resolve(context.Background(), Ref{Path: "secret/app"}); err != nil {
		t.Fatal(err)
	}
	// Without the CA the certificate is rejected (no skip-verify switch).
	p2 := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	if _, err := p2.Resolve(context.Background(), Ref{Path: "secret/app"}); err == nil {
		t.Fatal("unknown CA accepted")
	}
}
