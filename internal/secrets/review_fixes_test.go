package secrets

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/integrations/spec"
)

// E3: one failed renewal no longer drops credentials that are still valid.
func TestRenewerRetriesAFailedRenewal(t *testing.T) {
	var calls atomic.Int64
	lost := make(chan error, 1)
	r := NewRenewer(func(context.Context) (time.Time, error) {
		if calls.Add(1) <= 2 {
			return time.Time{}, errors.New("vault restarting")
		}
		return time.Now().Add(time.Hour), nil
	}, func(err error) { lost <- err })
	r.retryBase = 20 * time.Millisecond
	r.Start(context.Background(), time.Now().Add(900*time.Millisecond))
	defer r.Close()
	deadline := time.After(3 * time.Second)
	for calls.Load() < 3 {
		select {
		case err := <-lost:
			t.Fatalf("lost after %d calls: %v", calls.Load(), err)
		case <-deadline:
			t.Fatalf("only %d renewals", calls.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}
	select {
	case err := <-lost:
		t.Fatalf("lost after a successful retry: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// E3: renewals that keep failing give up before the lease ends, so the
// owner still has time to fetch new credentials.
func TestRenewerGivesUpBeforeExpiry(t *testing.T) {
	lost := make(chan time.Time, 1)
	start := time.Now()
	exp := start.Add(900 * time.Millisecond)
	r := NewRenewer(func(context.Context) (time.Time, error) { return time.Time{}, errors.New("down") },
		func(error) { lost <- time.Now() })
	r.retryBase = 20 * time.Millisecond
	r.Start(context.Background(), exp)
	defer r.Close()
	select {
	case at := <-lost:
		if !at.Before(exp) {
			t.Fatalf("lost at %v, after the expiry %v", at.Sub(start), exp.Sub(start))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lost never called")
	}
}

// E3: a lease that can never be renewed is not retried.
func TestRenewerDoesNotRetryNotRenewable(t *testing.T) {
	var calls atomic.Int64
	lost := make(chan error, 1)
	r := NewRenewer(func(context.Context) (time.Time, error) {
		calls.Add(1)
		return time.Time{}, errLeaseNotRenewable
	}, func(err error) { lost <- err })
	r.retryBase = time.Millisecond
	r.Start(context.Background(), time.Now().Add(60*time.Millisecond))
	defer r.Close()
	if err := <-lost; !errors.Is(err, errLeaseNotRenewable) || calls.Load() != 1 {
		t.Fatalf("err = %v after %d calls", err, calls.Load())
	}
}

// E3: the log says what really happens to the clients.
func TestRebuildTextDependsOnListener(t *testing.T) {
	r := NewRegistry(Options{})
	defer r.Close()
	if strings.Contains(r.rebuildText(), "rebuilding") {
		t.Fatal("claims a rebuild with no OnChange")
	}
	r2 := NewRegistry(Options{OnChange: func(string, Ref) {}})
	defer r2.Close()
	if !strings.Contains(r2.rebuildText(), "rebuilding") {
		t.Fatal("OnChange set but the log says nothing is rebuilt")
	}
}

// E1: two spellings of one file have the same identity.
func TestFilesIdentityFollowsSymlinks(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "roots")
	if err := os.MkdirAll(filepath.Join(root, "keys"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "keys", "sk"), []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "rootlink")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	r := NewRegistry(Options{})
	defer r.Close()
	p, err := NewFilesProvider(FilesOptions{Roots: []string{root, link}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Register(spec.DefaultWorkspace, spec.ProviderFiles, p); err != nil {
		t.Fatal(err)
	}
	a, _ := r.SecretIdentity(spec.DefaultWorkspace, "secret://files"+filepath.ToSlash(filepath.Join(root, "keys", "sk")))
	b, _ := r.SecretIdentity(spec.DefaultWorkspace, "secret://files"+filepath.ToSlash(filepath.Join(link, "keys", "sk"))+"#x")
	if a == "" || a != b {
		t.Fatalf("identities %q and %q differ", a, b)
	}
	c, _ := r.SecretIdentity(spec.DefaultWorkspace, "secret://vault/secret/a#k")
	if c != "vault/secret/a" {
		t.Fatalf("vault identity %q", c)
	}
}

// E14: a reserved variable is refused with its real reason.
func TestEnvReservedVariableMessage(t *testing.T) {
	p, err := NewEnvProvider(EnvOptions{Prefix: "PIPEROOM_", LookupEnv: func(string) (string, bool) { return "x", true }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Resolve(context.Background(), Ref{Path: "PIPEROOM_SECRET_KEY"})
	if !errors.Is(err, ErrAccessDenied) || strings.Contains(err.Error(), "does not start with") || !strings.Contains(err.Error(), "own settings") {
		t.Fatalf("err = %v", err)
	}
	_, err = p.Resolve(context.Background(), Ref{Path: "OTHER_X"})
	if !errors.Is(err, ErrAccessDenied) || !strings.Contains(err.Error(), "does not start with PIPEROOM_") {
		t.Fatalf("err = %v", err)
	}
}

// E11: key listing is limited to the provider's prefix, like name listing.
func TestVaultKeysOutsidePrefix(t *testing.T) {
	_, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token", Prefix: "secret/piperoom/"})
	if _, err := p.Keys(context.Background(), Ref{Path: "secret/app"}); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("err = %v", err)
	}
	if _, err := p.Keys(context.Background(), Ref{Path: "database/creds/ro"}); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("creds outside prefix err = %v", err)
	}
}

// E12: the least-privilege pass says what it checked.
func TestVaultLeastPrivilegeSaysWhatWasChecked(t *testing.T) {
	_, srv := newFakeVault(t)
	p := newVault(t, VaultOptions{Address: srv.URL, Token: "root-token", Prefix: "secret/piperoom/"})
	c := p.checkLeastPrivilege(context.Background(), nil)
	if c.Status != spec.CheckPass || strings.Contains(c.Detail, "inside the prefix") || !strings.Contains(c.Detail, "not checked") {
		t.Fatalf("check = %+v", c)
	}
}
