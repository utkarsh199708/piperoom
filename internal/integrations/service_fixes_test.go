package integrations

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

func refInput(ref string) spec.Input {
	in := connectInput()
	in.Secrets = map[string]spec.SecretInput{"password": {Op: spec.SecretRef, Ref: ref}}
	return in
}

// filesResolver is a real registry with a files provider over roots.
func filesResolver(t *testing.T, roots ...string) *secrets.Registry {
	t.Helper()
	reg := secrets.NewRegistry(secrets.Options{})
	t.Cleanup(reg.Close)
	fp, err := secrets.NewFilesProvider(secrets.FilesOptions{Roots: roots})
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(spec.DefaultWorkspace, spec.ProviderFiles, fp); err != nil {
		t.Fatal(err)
	}
	return reg
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

// E1: another spelling of a reserved files reference reads the same file.
func TestReservedRefAliasesAreRefused(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	roots := filepath.Join(dir, "roots")
	writeFile(t, filepath.Join(roots, "keys", "sk"), canary+"-sealing-key")
	link := filepath.Join(dir, "rootlink")
	if err := os.Symlink(roots, link); err != nil {
		t.Fatal(err)
	}
	// A Kubernetes secret volume: <name> -> ..data/<name>, ..data -> ..<stamp>.
	k8s := filepath.Join(roots, "k8s")
	writeFile(t, filepath.Join(k8s, "..2026_10_14", "token"), canary+"-alert-token")
	if err := os.Symlink("..2026_10_14", filepath.Join(k8s, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..data", "token"), filepath.Join(k8s, "token")); err != nil {
		t.Fatal(err)
	}
	reg := filesResolver(t, roots, link)
	ref := func(p string) string { return "secret://files" + filepath.ToSlash(p) }
	h := newHarness(t, func(o *Options) {
		o.Resolver = reg
		o.ReservedRefs = []string{ref(filepath.Join(roots, "keys", "sk")), ref(filepath.Join(k8s, "token"))}
	})
	for _, alias := range []string{
		ref(filepath.Join(roots, "keys", "sk")),
		ref(filepath.Join(link, "keys", "sk")),
		ref(filepath.Join(k8s, "..data", "token")),
		ref(filepath.Join(k8s, "..2026_10_14", "token")),
	} {
		_, err := h.svc.Test(context.Background(), ws, "a", refInput(alias))
		var ve *spec.ValidationError
		if !errors.As(err, &ve) || ve.Field != "password" {
			t.Fatalf("%s: err = %v, want the reserved refusal", alias, err)
		}
	}
	// A file of its own is fine.
	writeFile(t, filepath.Join(roots, "app", "pw"), "app-pw")
	if _, err := h.svc.Test(context.Background(), ws, "a", refInput(ref(filepath.Join(link, "app", "pw")))); err != nil {
		t.Fatal(err)
	}
}

// E1 (b): a reference that resolves to a server secret's value is refused
// whatever its spelling, also when a stored row is enabled.
func TestReservedValueIsRefused(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(o *Options) { o.ReservedValues = []netsec.Secret{canary + "-ref"} })
	in := refInput("secret://vault-corp/kafka/connect#password")
	checks, err := h.svc.Test(ctx, ws, "a", in)
	if err != nil {
		t.Fatal(err)
	}
	if c := wantStatus(t, checks, "secret:password", spec.CheckFail); !strings.Contains(c.Detail, "own settings") {
		t.Fatalf("check = %+v", c)
	}
	if _, _, err := h.svc.Create(ctx, ws, "a", in); err == nil {
		t.Fatal("created a row that sends a server secret")
	}
	if len(h.app.applied) != 0 {
		t.Fatal("applied a server secret")
	}
	// A row saved before the value became reserved is not enabled with it.
	h2 := newHarness(t, nil)
	view, _, err := h2.svc.Create(ctx, ws, "a", in)
	if err != nil {
		t.Fatal(err)
	}
	h2.svc.o.ReservedValues = []netsec.Secret{canary + "-ref"}
	if _, err := h2.svc.SetEnabled(ctx, ws, "a", view.ID, true); err == nil {
		t.Fatal("enable sent a server secret")
	}
	if _, err := h2.svc.OpenRow(ctx, mustGet(t, h2, view.ID)); err == nil {
		t.Fatal("restore would send a server secret")
	}
}

func mustGet(t *testing.T, h *harness, id string) spec.Integration {
	t.Helper()
	r, err := h.store.Get(context.Background(), ws, id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// E4: a row whose client is not running is not shown as connected.
func TestNotLiveRowIsFailing(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
	if err != nil || view.Status != spec.StatusConnected {
		t.Fatalf("create: %v %+v", err, view)
	}
	h.svc.MarkNotLive(view.ID, "not connected at start: ", fmt.Errorf("unsealing: %w", ErrOpen))
	list, _ := h.svc.List(ctx, ws)
	if list[0].Status != spec.StatusFailing {
		t.Fatalf("status = %s", list[0].Status)
	}
	c := wantStatus(t, list[0].LastTest, "live", spec.CheckFail)
	if !strings.Contains(c.Detail, "PIPEROOM_SECRET_KEY") {
		t.Fatalf("live check = %+v", c)
	}
	// The stored test is not changed.
	if r := mustGet(t, h, view.ID); len(r.LastTest) != 1 {
		t.Fatalf("stored last_test = %+v", r.LastTest)
	}
	// Enabling again (manual recovery) builds the client and clears it.
	if v, err := h.svc.SetEnabled(ctx, ws, "a", view.ID, true); err != nil || v.Status != spec.StatusConnected {
		t.Fatalf("re-enable: %v %+v", err, v)
	}
	// A save whose live apply fails is not live either.
	h.app.err = errors.New("dial failed")
	in := connectInput()
	in.Secrets = map[string]spec.SecretInput{}
	v, _, err := h.svc.Update(ctx, ws, "a", view.ID, 1, in)
	if err != nil || v.Status != spec.StatusFailing {
		t.Fatalf("update with failed apply: %v %+v", err, v)
	}
	// Disabling forgets it.
	if v, err := h.svc.SetEnabled(ctx, ws, "a", view.ID, false); err != nil || v.Status != spec.StatusDisabled {
		t.Fatalf("disable: %v %+v", err, v)
	}
}

// E5: a key change does not turn enable, test and update into 500s, and a
// PUT that retypes the secrets repairs the row.
func TestKeyMismatchIsRepairable(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
	if err != nil {
		t.Fatal(err)
	}
	other, _ := NewKeyring(testKey(9), "")
	h.svc.o.Keys = other
	if _, err := h.svc.SetEnabled(ctx, ws, "a", view.ID, true); !errors.Is(err, spec.ErrSecretKeyMismatch) {
		t.Fatalf("enable err = %v", err)
	}
	kept := connectInput()
	kept.ID = view.ID
	kept.Secrets = map[string]spec.SecretInput{}
	var ve *spec.ValidationError
	if _, err := h.svc.Test(ctx, ws, "a", kept); !errors.As(err, &ve) || ve.Field != "password" {
		t.Fatalf("test with id err = %v", err)
	}
	retyped := connectInput()
	retyped.Secrets = map[string]spec.SecretInput{"password": setSecret(canary + "-new")}
	v, _, err := h.svc.Update(ctx, ws, "a", view.ID, 1, retyped)
	if err != nil {
		t.Fatalf("retyped update: %v", err)
	}
	r := mustGet(t, h, v.ID)
	open, err := other.Open(SealAAD(ws, r.ID, r.Category), r.Sealed, r.KeyID)
	if err != nil || open["password"] != canary+"-new" {
		t.Fatalf("re-sealed under the new key: %v", err)
	}
}

type errResolver struct{ err error }

func (r errResolver) Resolve(context.Context, string, string) (netsec.Secret, error) {
	return "", r.err
}

// E5: the resolve error of a stored row keeps its cause, so the API maps it.
func TestStoredRefErrorsKeepTheirCause(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		err    error
		reason spec.SecretRefReason
	}{
		{fmt.Errorf("secrets: resolving: %w", secrets.ErrUnreachable), spec.SecretRefUnreachable},
		{fmt.Errorf("secrets: %w: %q", secrets.ErrUnknownProvider, "vault-corp"), spec.SecretRefUnknownProvider},
		{fmt.Errorf("secrets: %w", secrets.ErrAccessDenied), spec.SecretRefDenied},
		{fmt.Errorf("secrets: %w", secrets.ErrNotFound), spec.SecretRefNotFound},
	} {
		h := newHarness(t, nil)
		view, _, err := h.svc.Create(ctx, ws, "a", refInput("secret://vault-corp/kafka/connect#password"))
		if err != nil {
			t.Fatal(err)
		}
		h.svc.o.Resolver = errResolver{tc.err}
		_, err = h.svc.SetEnabled(ctx, ws, "a", view.ID, true)
		var se *spec.SecretRefError
		if !errors.As(err, &se) || se.Reason != tc.reason || se.Field != "password" || !errors.Is(err, tc.err) {
			t.Fatalf("%v: err = %v", tc.err, err)
		}
	}
}

func managerInput(name string) spec.Input {
	return spec.Input{Kind: "secrets-fake", Config: map[string]any{"name": name}, Secrets: map[string]spec.SecretInput{}}
}

// E6: a secret manager that enabled rows read from cannot be removed.
func TestSecretManagerInUse(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	mgr, _, err := h.svc.Create(ctx, ws, "a", managerInput("vault-corp"))
	if err != nil {
		t.Fatal(err)
	}
	dep, _, err := h.svc.Create(ctx, ws, "a", refInput("secret://vault-corp/kafka/connect#password"))
	if err != nil {
		t.Fatal(err)
	}
	var iu *spec.InUseError
	if err := h.svc.Delete(ctx, ws, "a", mgr.ID); !errors.As(err, &iu) || len(iu.Dependents) != 1 || iu.Dependents[0].ID != dep.ID {
		t.Fatalf("delete err = %v", err)
	}
	if _, err := h.svc.SetEnabled(ctx, ws, "a", mgr.ID, false); !errors.As(err, &iu) {
		t.Fatalf("disable err = %v", err)
	}
	if _, err := h.store.Get(ctx, ws, mgr.ID); err != nil {
		t.Fatal("the manager was deleted")
	}
	// Once the dependent is disabled, the manager can go.
	if _, err := h.svc.SetEnabled(ctx, ws, "a", dep.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := h.svc.Delete(ctx, ws, "a", mgr.ID); err != nil {
		t.Fatal(err)
	}
}

// E19: repeating a state does not bump the version or write audit rows.
func TestSetEnabledIsIdempotent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
	if err != nil {
		t.Fatal(err)
	}
	applied := len(h.app.applied)
	v, err := h.svc.SetEnabled(ctx, ws, "a", view.ID, true)
	if err != nil || v.Version != 1 {
		t.Fatalf("enable an enabled row: %v version %d", err, v.Version)
	}
	if len(h.app.applied) != applied+1 {
		t.Fatal("enable of an enabled row must still rebuild the client")
	}
	if v, err = h.svc.SetEnabled(ctx, ws, "a", view.ID, false); err != nil || v.Version != 2 {
		t.Fatalf("disable: %v version %d", err, v.Version)
	}
	if v, err = h.svc.SetEnabled(ctx, ws, "a", view.ID, false); err != nil || v.Version != 2 {
		t.Fatalf("disable again: %v version %d", err, v.Version)
	}
	audit, _ := h.svc.Audit(ctx, ws, view.ID, 0)
	if len(audit) != 2 {
		t.Fatalf("audit = %+v", audit)
	}
	if len(h.note.entries) != 2 {
		t.Fatalf("notices = %d", len(h.note.entries))
	}
}

// E3: a changed secret rebuilds the clients of the rows that read it.
func TestSecretChangedRebuildsReaders(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	if _, _, err := h.svc.Create(ctx, ws, "a", refInput("secret://vault-corp/kafka/connect#password")); err != nil {
		t.Fatal(err)
	}
	n := len(h.app.applied)
	h.svc.SecretChanged(ctx, ws, "vault-corp", "other/path")
	if len(h.app.applied) != n {
		t.Fatal("rebuilt a row that does not read the changed secret")
	}
	h.res.vals["secret://vault-corp/kafka/connect#password"] = canary + "-rotated"
	h.svc.SecretChanged(ctx, ws, "vault-corp", "kafka/connect")
	if len(h.app.applied) != n+1 || h.app.applied[n].Secret("password") != canary+"-rotated" {
		t.Fatalf("applied %d, want the rotated value", len(h.app.applied))
	}
	// A rebuild that fails leaves the row shown as not live.
	h.app.err = errors.New("login failed")
	h.svc.SecretChanged(ctx, ws, "vault-corp", "kafka/connect")
	list, _ := h.svc.List(ctx, ws)
	if list[0].Status != spec.StatusFailing {
		t.Fatalf("status = %s", list[0].Status)
	}
}

// E9: a field with AllowRef takes a reference, resolved into text for the kind.
func TestAllowRefTextField(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	k := h.kind.kind()
	k.ID = "fake-lease"
	for i := range k.Fields {
		if k.Fields[i].Name == "username" {
			k.Fields[i].AllowRef = true
		}
	}
	if err := h.svc.o.Registry.Register(k); err != nil {
		t.Fatal(err)
	}
	h.res.vals["secret://vault-corp/database/creds/ro#username"] = "v-ro-1"
	h.res.vals["secret://vault-corp/database/creds/ro#password"] = canary + "-lease"
	in := spec.Input{Kind: "fake-lease",
		Config:  map[string]any{"url": "https://c.corp", "username": map[string]any{spec.RefKey: "secret://vault-corp/database/creds/ro#username"}},
		Secrets: map[string]spec.SecretInput{"password": {Op: spec.SecretRef, Ref: "secret://vault-corp/database/creds/ro#password"}}}
	view, _, err := h.svc.Create(ctx, ws, "a", in)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.kind.last(); got.Get("username") != "v-ro-1" || got.Secret("password") != canary+"-lease" {
		t.Fatalf("kind saw user %q", got.Get("username"))
	}
	if m, ok := view.Config["username"].(map[string]any); !ok || m[spec.RefKey] != "secret://vault-corp/database/creds/ro#username" {
		t.Fatalf("view config = %v", view.Config)
	}
	if strings.Join(view.SecretsSet, ",") != "password" {
		t.Fatalf("secrets_set = %v", view.SecretsSet)
	}
	// Enable reads it back from the stored row.
	if _, err := h.svc.SetEnabled(ctx, ws, "a", view.ID, true); err != nil {
		t.Fatal(err)
	}
	if got := h.app.applied[len(h.app.applied)-1]; got.Get("username") != "v-ro-1" {
		t.Fatalf("applied user %q", got.Get("username"))
	}
	// A field without AllowRef still refuses a reference.
	bad := connectInput()
	bad.Config["url"] = map[string]any{spec.RefKey: "secret://vault-corp/x"}
	var ve *spec.ValidationError
	if _, _, err := h.svc.Create(ctx, ws, "a", bad); !errors.As(err, &ve) || ve.Field != "url" {
		t.Fatalf("ref in a plain field: %v", err)
	}
}

// E21: a database URL held as a reference is not parsed as a URL.
func TestEnvDBURLReference(t *testing.T) {
	env := EnvFromLookup(func(name string) (string, bool) {
		if name == "SOURCE_DB_URL" {
			return "secret://vault-env/secret/piperoom/env-urls#source", true
		}
		return "", false
	})
	ec := env.Categories[spec.CategorySourceDB]
	if len(ec.Config) != 0 || strings.Join(ec.SecretsSet, ",") != "password" {
		t.Fatalf("env view = %+v", ec)
	}
	pw := EnvFromLookup(func(name string) (string, bool) {
		if name == "SOURCE_DB_PASSWORD" {
			return "secret://vault-env/db#password", true
		}
		return "", false
	})
	if _, ok := pw.Categories[spec.CategorySourceDB]; !ok {
		t.Fatal("SOURCE_DB_PASSWORD does not make source_db env-managed")
	}
}
