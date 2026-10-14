package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
)

// canary is a fake secret value. No String, %v, %+v, %#v, JSON or slog
// output of this package's types may ever contain it (§9).
const canary = "CANARY-7f3a91-do-not-print"

func renderAll(t *testing.T, name string, v any) {
	t.Helper()
	var outs []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		outs = append(outs, fmt.Sprintf(verb, v))
	}
	if b, err := json.Marshal(v); err == nil {
		outs = append(outs, string(b))
	}
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("x", "v", v)
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("x", "v", v)
	outs = append(outs, buf.String())
	for _, o := range outs {
		if strings.Contains(o, canary) {
			t.Errorf("%s leaks the canary: %s", name, o)
		}
	}
}

func TestCanaryNeverPrinted(t *testing.T) {
	ctx := context.Background()
	fv, srv := newFakeVault(t)
	fv.mu.Lock()
	fv.tokens[canary] = true
	fv.kv2[2] = map[string]any{"password": canary}
	fv.mu.Unlock()

	vp := newVault(t, VaultOptions{Address: srv.URL, Token: canary, SecretID: canary, JWT: canary})
	v, err := vp.Resolve(ctx, Ref{Path: "secret/app"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(v.Data.Reveal(), canary) {
		t.Fatal("setup: the canary was not read")
	}

	r := newTestRegistry(t, Options{})
	mustRegister(t, r, "ws", "vault", vp)
	s, err := r.Resolve(ctx, "ws", "secret://vault/secret/app#password")
	if err != nil || s.Reveal() != canary {
		t.Fatalf("setup: %v", err)
	}
	fval, _ := r.ResolveValue(ctx, "ws", "secret://vault/secret/app#password")

	specs, err := ParseProviders("v=vault:https://vault:8200;token=" + canary + ";secret_id=" + canary)
	if err != nil {
		t.Fatal(err)
	}
	vopts := VaultOptions{Address: "https://v", Token: canary, SecretID: canary, JWT: canary}
	envp, _ := NewEnvProvider(EnvOptions{Prefix: "X_", LookupEnv: func(string) (string, bool) { return canary, true }})
	ev, _ := envp.Resolve(ctx, Ref{Path: "X_A"})

	things := map[string]any{
		"Value":            v,
		"*Value":           &v,
		"registry value":   fval,
		"Secret":           s,
		"VaultOptions":     vopts,
		"*VaultOptions":    &vopts,
		"VaultProvider":    vp,
		"Registry":         r,
		"Entries":          r.Entries("ws"),
		"ProviderSpec":     specs[0],
		"[]ProviderSpec":   specs,
		"EnvProvider":      envp,
		"env value":        ev,
		"checks":           vp.Check(ctx, &Ref{Path: "secret/app", Key: "password"}),
		"registry wrapper": struct{ R *Registry }{r},
	}
	for name, x := range things {
		renderAll(t, name, x)
	}

	// Errors never carry the value either.
	errs := []error{}
	_, err = ExtractKey(netsec.Secret(`{"a":"`+canary), "a")
	errs = append(errs, err)
	_, err = ExtractKey(netsec.Secret(canary), "a")
	errs = append(errs, err)
	_, err = r.Resolve(ctx, "ws", "secret://vault/secret/app#nope")
	errs = append(errs, err)
	_, err = ParseRef(canary)
	errs = append(errs, err)
	_, err = ParseProviders("bad;" + canary)
	errs = append(errs, err)
	bad := newVault(t, VaultOptions{Address: srv.URL, Auth: VaultAuthAppRole, RoleID: "rid", SecretID: canary + "x"})
	_, err = bad.Resolve(ctx, Ref{Path: "secret/app"})
	errs = append(errs, err)
	for i, e := range errs {
		if e == nil {
			t.Errorf("error %d: want an error", i)
			continue
		}
		if strings.Contains(e.Error(), canary) || strings.Contains(fmt.Sprintf("%+v", e), canary) {
			t.Errorf("error %d leaks the canary: %v", i, e)
		}
	}
	for _, c := range bad.Check(ctx, &Ref{Path: "secret/app"}) {
		if strings.Contains(c.Detail+c.Fix+c.Title, canary) {
			t.Errorf("check leaks the canary: %+v", c)
		}
	}
}

// TestCanaryNotLogged runs a failing and a successful resolve with the
// default logger captured, and checks no line holds the value.
func TestCanaryNotLogged(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	changed := make(chan struct{}, 1)
	r := newTestRegistry(t, Options{Logger: log, OnChange: func(string, Ref) { changed <- struct{}{} }})
	f := newFake(map[string]string{"x": canary})
	f.renewErr = fmt.Errorf("renew failed")
	f.set("lease", Value{Data: canary, LeaseID: "l", Renewable: true, ExpiresAt: time.Now().Add(30 * time.Millisecond)})
	mustRegister(t, r, "ws", "p", f)
	ctx := context.Background()
	_, _ = r.Resolve(ctx, "ws", "secret://p/x")
	_, _ = r.Resolve(ctx, "ws", "secret://p/missing")
	_, _ = r.Resolve(ctx, "ws", "secret://p/lease")
	<-changed
	f.fireChange("x")
	<-changed
	if strings.Contains(buf.String(), canary) {
		t.Fatalf("log leaks the canary:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "secret://p/lease") {
		t.Fatalf("expected the lease loss to be logged by reference:\n%s", buf.String())
	}
}
