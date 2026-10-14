package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/integrations/spec"
)

// leakServer records everything it is sent; it stands in for the host a
// redirect points at.
type leakServer struct {
	mu   sync.Mutex
	hits []string
}

func (l *leakServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	l.mu.Lock()
	l.hits = append(l.hits, r.Method+" "+r.URL.Path+" token="+r.Header.Get("X-Vault-Token")+
		" auth="+r.Header.Get("Authorization")+" body="+string(b))
	l.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"auth":{"client_token":"evil","lease_duration":60},"data":{"data":{"x":"1"},"metadata":{"version":1}}}`))
}

func (l *leakServer) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.hits)
}

func redirectTo(target string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target+r.URL.Path, http.StatusTemporaryRedirect)
	})
}

func TestVaultRefusesRedirects(t *testing.T) {
	evil := &leakServer{}
	evilSrv := httptest.NewServer(evil)
	defer evilSrv.Close()
	vaultSrv := httptest.NewServer(redirectTo(evilSrv.URL))
	defer vaultSrv.Close()
	ctx := context.Background()

	cases := []struct {
		name string
		opts VaultOptions
	}{
		{"token on a KV read", VaultOptions{Address: vaultSrv.URL, Token: "s.SECRET"}},
		{"approle login body", VaultOptions{Address: vaultSrv.URL, Auth: VaultAuthAppRole, RoleID: "r", SecretID: "SID-SECRET"}},
		{"jwt login body", VaultOptions{Address: vaultSrv.URL, Auth: VaultAuthJWT, Role: "piperoom", JWT: "JWT-SECRET"}},
		// An injected client is guarded too.
		{"injected client", VaultOptions{Address: vaultSrv.URL, Token: "s.SECRET", HTTPClient: &http.Client{Timeout: time.Second}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newVault(t, tc.opts)
			if _, err := p.Resolve(ctx, Ref{Path: "secret/app"}); err == nil || !errors.Is(err, errRedirect) {
				t.Fatalf("Resolve err = %v, want a refused redirect", err)
			}
			if n := evil.count(); n != 0 {
				t.Fatalf("redirect target was contacted %d times: %v", n, evil.hits)
			}
		})
	}
}

func TestKubernetesRefusesRedirects(t *testing.T) {
	evil := &leakServer{}
	evilSrv := httptest.NewServer(evil)
	defer evilSrv.Close()
	// A same-host downgrade keeps Authorization in net/http; refuse it all.
	kubeSrv := httptest.NewTLSServer(redirectTo(evilSrv.URL))
	defer kubeSrv.Close()
	tokFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokFile, []byte("sa-SECRET"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := NewKubernetesProvider(KubernetesOptions{
		Address: kubeSrv.URL, Namespace: "piperoom", TokenFile: tokFile, HTTPClient: kubeSrv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Resolve(context.Background(), Ref{Path: "orders-db"}); !errors.Is(err, errRedirect) {
		t.Fatalf("Resolve err = %v, want a refused redirect", err)
	}
	if n := evil.count(); n != 0 {
		t.Fatalf("redirect target was contacted: %v", evil.hits)
	}
}

func TestVaultKeepsNumberBytes(t *testing.T) {
	fv, srv := newFakeVault(t)
	fv.mu.Lock()
	fv.kv2[3] = map[string]any{"pin": json.RawMessage(`12345678901234567890123`), "ratio": json.RawMessage(`1.50`)}
	fv.kv2Version = 3
	fv.mu.Unlock()
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	v, err := p.Resolve(context.Background(), Ref{Path: "secret/app"})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"pin": "12345678901234567890123", "ratio": "1.50"} {
		if got, _ := ExtractKey(v.Data, key); got.Reveal() != want {
			t.Fatalf("%s = %q, want %q", key, got.Reveal(), want)
		}
	}
}

func TestVaultRefusesInternalPaths(t *testing.T) {
	_, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token"})
	ctx := context.Background()
	for _, path := range []string{
		"auth/token/lookup-self", "sys/leases/lookup", "cubbyhole/x", "identity/entity/id/1",
		"team-a/auth/token/lookup-self", "kv/sys/x",
	} {
		if _, err := p.Resolve(ctx, Ref{Path: path}); !errors.Is(err, ErrAccessDenied) {
			t.Errorf("Resolve(%s) err = %v, want ErrAccessDenied", path, err)
		}
		if _, err := p.Keys(ctx, Ref{Path: path}); !errors.Is(err, ErrAccessDenied) {
			t.Errorf("Keys(%s) err = %v, want ErrAccessDenied", path, err)
		}
	}
	// Segments named like an internal mount are fine inside a KV v2 mount.
	if _, err := p.Resolve(ctx, Ref{Path: "secret/auth"}); errors.Is(err, ErrAccessDenied) {
		t.Fatalf("KV v2 secret/auth refused: %v", err)
	}
}

func TestKubernetesListStaysInNamespace(t *testing.T) {
	fk, p, _ := newKubeTest(t)
	fk.mu.Lock()
	fk.listOK = true
	fk.mu.Unlock()
	if _, err := p.List(context.Background(), "kube-system/", 10); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("other namespace err = %v", err)
	}
	if got, err := p.List(context.Background(), "piperoom/", 10); err != nil || len(got) != 2 {
		t.Fatalf("own namespace = %v %v", got, err)
	}
}

func TestFilesRefuseWorkloadIdentity(t *testing.T) {
	ctx := context.Background()
	p, root, _ := newFilesTest(t, time.Hour)
	sa := filepath.Join(root, "kubernetes.io", "serviceaccount")
	write(t, filepath.Join(sa, "token"), "pod-identity")
	write(t, filepath.Join(root, "eks.amazonaws.com", "serviceaccount", "token"), "irsa")
	write(t, filepath.Join(root, "azure", "tokens", "azure-identity-token"), "azwi")
	write(t, filepath.Join(root, "app", "password"), "ok")
	// A symlink elsewhere under the root that points at the token.
	if err := os.Symlink(filepath.Join(sa, "token"), filepath.Join(root, "app", "sneaky")); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{
		filepath.Join(sa, "token"),
		filepath.Join(root, "eks.amazonaws.com", "serviceaccount", "token"),
		filepath.Join(root, "azure", "tokens", "azure-identity-token"),
		filepath.Join(root, "app", "sneaky"),
		filepath.Join(sa, "missing"),
	} {
		if _, err := p.Resolve(ctx, Ref{Path: refPathOf(f)}); !errors.Is(err, ErrAccessDenied) {
			t.Errorf("Resolve(%s) err = %v, want ErrAccessDenied", f, err)
		}
	}
	if _, err := p.List(ctx, refPathOf(sa)+"/", 10); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("List serviceaccount err = %v", err)
	}
	got, err := p.List(ctx, refPathOf(filepath.Join(root, "kubernetes.io"))+"/", 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("List kubernetes.io = %v %v (serviceaccount must be hidden)", got, err)
	}
	if v, err := p.Resolve(ctx, Ref{Path: refPathOf(filepath.Join(root, "app", "password"))}); err != nil || v.Data.Reveal() != "ok" {
		t.Fatalf("ordinary file: %v", err)
	}
}

func TestOnChangeMayCloseRegistry(t *testing.T) {
	_, root, _ := newFilesTest(t, time.Hour)
	fp, err := NewFilesProvider(FilesOptions{Roots: []string{root}, Poll: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var r *Registry
	r = NewRegistry(Options{OnChange: func(string, Ref) {
		r.Unregister(spec.DefaultWorkspace, spec.ProviderFiles) // waits for the poll goroutine
		r.Close()
		close(done)
	}})
	mustRegister(t, r, spec.DefaultWorkspace, spec.ProviderFiles, fp)
	path := filepath.Join(root, "pw")
	write(t, path, "one")
	if _, err := r.Resolve(context.Background(), spec.DefaultWorkspace, "secret://files/"+refPathOf(path)); err != nil {
		t.Fatal(err)
	}
	write(t, path, "two!")
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("OnChange handler that closes the registry deadlocked")
	}
}

func TestTruncateKeepsUTF8(t *testing.T) {
	s := strings.Repeat("a", 299) + "é" + "tail"
	got := errorDetail(errors.New(s))
	if !utf8.ValidString(got) {
		t.Fatalf("invalid UTF-8: %q", got)
	}
	if !strings.HasPrefix(got, strings.Repeat("a", 299)) || strings.Contains(got, "tail") {
		t.Fatalf("got %q", got)
	}
	if truncate("short", 10) != "short" {
		t.Fatal("short string changed")
	}
}
