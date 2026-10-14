package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

const ws = spec.DefaultWorkspace

type harness struct {
	svc   *Service
	store *memStore
	kind  *fakeKind
	app   *fakeApplier
	note  *fakeNotifier
	res   *fakeResolverSecrets
	now   time.Time
}

func newHarness(t *testing.T, mod func(*Options)) *harness {
	t.Helper()
	h := &harness{store: newMemStore(), kind: &fakeKind{}, app: &fakeApplier{}, note: &fakeNotifier{},
		res: &fakeResolverSecrets{vals: map[string]string{"secret://vault-corp/kafka/connect#password": canary + "-ref"}},
		now: time.Unix(1_800_000_000, 0)}
	reg := NewRegistry()
	for _, k := range append([]spec.Kind{h.kind.kind(), fakeSecretManagerKind()}, alertKinds()...) {
		if err := reg.Register(k); err != nil {
			t.Fatal(err)
		}
	}
	kr, err := NewKeyring(testKey(7), "")
	if err != nil {
		t.Fatal(err)
	}
	o := Options{Store: h.store, Registry: reg, Keys: kr, Resolver: h.res, Applier: h.app, Notifier: h.note,
		PublicURL: "https://piperoom.corp/", Now: func() time.Time { return h.now }}
	if mod != nil {
		mod(&o)
	}
	svc, err := NewService(o)
	if err != nil {
		t.Fatal(err)
	}
	h.svc = svc
	return h
}

func setSecret(v string) spec.SecretInput {
	return spec.SecretInput{Op: spec.SecretSet, Value: netsec.Secret(v)}
}

func connectInput() spec.Input {
	return spec.Input{Kind: "fake-connect",
		Config:  map[string]any{"url": "https://connect.corp:8083", "username": "piperoom", "port": float64(8083), "tls": true},
		Secrets: map[string]spec.SecretInput{"password": setSecret(canary)}}
}

// leakCheck fails if v's JSON (or its %v forms) contains the canary.
func leakCheck(t *testing.T, what string, v any) {
	t.Helper()
	js, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	all := string(js) + fmt.Sprintf("%v %+v %#v", v, v, v)
	if strings.Contains(all, canary) {
		t.Fatalf("%s leaks the secret: %s", what, all)
	}
}

func TestCreateSealsAndNeverReturnsSecrets(t *testing.T) {
	h := newHarness(t, nil)
	view, checks, err := h.svc.Create(context.Background(), ws, "priya", connectInput())
	if err != nil {
		t.Fatal(err)
	}
	if view.Version != 1 || view.Status != spec.StatusConnected || view.Source != spec.SourceDB || view.LastTestedAt == nil {
		t.Fatalf("view = %+v", view)
	}
	if !strings.HasPrefix(view.ID, "int_") || len(view.ID) != 28 {
		t.Fatalf("id = %q", view.ID)
	}
	if got := view.SecretsSet; len(got) != 1 || got[0] != "password" {
		t.Fatalf("secrets_set = %v", got)
	}
	if view.Config["port"] != "8083" || view.Config["tls"] != "true" || view.Config["url"] != "https://connect.corp:8083" {
		t.Fatalf("config = %v", view.Config)
	}
	if _, ok := view.Config["password"]; ok {
		t.Fatal("config holds the password")
	}
	leakCheck(t, "view", view)
	leakCheck(t, "checks", checks)

	rec, _ := h.store.Get(context.Background(), ws, view.ID)
	if rec.Sealed == nil || rec.KeyID != h.svc.o.Keys.CurrentKeyID() {
		t.Fatalf("row not sealed: key %q", rec.KeyID)
	}
	if strings.Contains(string(rec.Sealed), canary) {
		t.Fatal("sealed blob holds the plaintext")
	}
	leakCheck(t, "stored config", rec.Config)
	open, err := h.svc.o.Keys.Open(SealAAD(ws, rec.ID, rec.Category), rec.Sealed, rec.KeyID)
	if err != nil || open["password"] != canary {
		t.Fatalf("unseal: %v", err)
	}
	// The kind saw the real value; the applier got the client values.
	if h.kind.last().Secret("password") != canary || h.kind.last().Get("mode") != "a" {
		t.Fatalf("kind saw %v", h.kind.last().Config)
	}
	if len(h.app.applied) != 1 {
		t.Fatal("not applied")
	}
	audit, _ := h.svc.Audit(context.Background(), ws, view.ID, 0)
	if len(audit) != 1 || audit[0].Action != spec.AuditCreated || audit[0].Actor != "priya" {
		t.Fatalf("audit = %+v", audit)
	}
	if strings.Join(audit[0].Changed, ",") != "password,port,tls,url,username" {
		t.Fatalf("changed = %v", audit[0].Changed)
	}
	leakCheck(t, "audit", audit)
	if len(h.note.entries) != 1 || h.note.titles[0] != "Fake Connect" {
		t.Fatalf("notify = %+v", h.note.entries)
	}
	list, _ := h.svc.List(context.Background(), ws)
	leakCheck(t, "list", list)
}

func TestCreateErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("unknown kind", func(t *testing.T) {
		h := newHarness(t, nil)
		_, _, err := h.svc.Create(ctx, ws, "a", spec.Input{Kind: "nope"})
		if !errors.Is(err, spec.ErrUnknownKind) {
			t.Fatalf("err = %v", err)
		}
	})
	for name, tc := range map[string]struct {
		mut   func(*spec.Input)
		field string
	}{
		"unknown config field": {func(in *spec.Input) { in.Config["evil"] = "x" }, "evil"},
		"pattern":              {func(in *spec.Input) { in.Config["url"] = "ftp://x" }, "url"},
		"secret in config":     {func(in *spec.Input) { in.Config["password"] = canary }, "password"},
		"required missing":     {func(in *spec.Input) { delete(in.Config, "url") }, "url"},
		"required secret":      {func(in *spec.Input) { delete(in.Secrets, "password") }, "password"},
		"bad number":           {func(in *spec.Input) { in.Config["port"] = "eighty" }, "port"},
		"bad bool":             {func(in *spec.Input) { in.Config["tls"] = "yes" }, "tls"},
		"bad select":           {func(in *spec.Input) { in.Config["mode"] = "z" }, "mode"},
		"object value":         {func(in *spec.Input) { in.Config["username"] = map[string]any{"a": 1} }, "username"},
		"multi-line":           {func(in *spec.Input) { in.Config["username"] = "a\nb" }, "username"},
		"unknown secret":       {func(in *spec.Input) { in.Secrets["token"] = setSecret("x") }, "token"},
		"text as secret":       {func(in *spec.Input) { in.Secrets["username"] = setSecret("x") }, "username"},
		"bad ref prefix": {func(in *spec.Input) {
			in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "vault://x/y"}
		}, "password"},
		"bad ref provider": {func(in *spec.Input) {
			in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://Vault!/y"}
		}, "password"},
		"ref dotdot": {func(in *spec.Input) {
			in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://files/run/secrets/../../etc/shadow"}
		}, "password"},
		"ref encoded dotdot": {func(in *spec.Input) {
			in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://files/run/%2e%2e/etc"}
		}, "password"},
		"ref no path": {func(in *spec.Input) {
			in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://vault-corp/"}
		}, "password"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, nil)
			in := connectInput()
			tc.mut(&in)
			_, _, err := h.svc.Create(ctx, ws, "a", in)
			var ve *spec.ValidationError
			if !errors.As(err, &ve) || ve.Field != tc.field {
				t.Fatalf("err = %v, want validation on %s", err, tc.field)
			}
			if strings.Contains(err.Error(), canary) {
				t.Fatal("validation error quotes the value")
			}
			if rows, _ := h.store.List(ctx, ws); len(rows) != 0 {
				t.Fatal("stored despite the error")
			}
		})
	}
	t.Run("checks fail", func(t *testing.T) {
		h := newHarness(t, nil)
		h.kind.checks = []spec.Check{{ID: "login", Status: spec.CheckFail}, {ID: "x", Status: spec.CheckWarn}}
		_, checks, err := h.svc.Create(ctx, ws, "a", connectInput())
		var cf *spec.ChecksFailedError
		if !errors.As(err, &cf) || len(cf.Checks) != 2 || len(checks) != 2 {
			t.Fatalf("err = %v", err)
		}
		if rows, _ := h.store.List(ctx, ws); len(rows) != 0 || len(h.app.applied) != 0 || len(h.note.entries) != 0 {
			t.Fatal("a failed test saved, applied or notified")
		}
	})
	t.Run("warn allows save", func(t *testing.T) {
		h := newHarness(t, nil)
		h.kind.checks = []spec.Check{{ID: "x", Status: spec.CheckWarn}}
		if _, _, err := h.svc.Create(ctx, ws, "a", connectInput()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		h := newHarness(t, nil)
		if _, _, err := h.svc.Create(ctx, ws, "a", connectInput()); err != nil {
			t.Fatal(err)
		}
		if _, _, err := h.svc.Create(ctx, ws, "a", connectInput()); !errors.Is(err, spec.ErrDuplicate) {
			t.Fatalf("err = %v", err)
		}
		// Another workspace may have its own.
		if _, _, err := h.svc.Create(ctx, "acme", "a", connectInput()); err != nil {
			t.Fatalf("other workspace: %v", err)
		}
	})
	t.Run("no key", func(t *testing.T) {
		h := newHarness(t, func(o *Options) { o.Keys = nil })
		if _, _, err := h.svc.Create(ctx, ws, "a", connectInput()); !errors.Is(err, spec.ErrSecretKeyMissing) {
			t.Fatalf("err = %v", err)
		}
		// A reference needs no key.
		in := connectInput()
		in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://vault-corp/kafka/connect#password"}
		view, _, err := h.svc.Create(ctx, ws, "a", in)
		if err != nil {
			t.Fatal(err)
		}
		ref, ok := refOf(view.Config["password"])
		if !ok || ref != "secret://vault-corp/kafka/connect#password" {
			t.Fatalf("config = %v", view.Config)
		}
		rec, _ := h.store.Get(ctx, ws, view.ID)
		if rec.Sealed != nil || rec.KeyID != "" {
			t.Fatal("a reference-only row has a sealed blob")
		}
		if h.kind.last().Secret("password") != canary+"-ref" || h.res.ws[len(h.res.ws)-1] != ws {
			t.Fatal("reference not resolved in the caller's workspace")
		}
	})
	t.Run("reference unresolvable", func(t *testing.T) {
		h := newHarness(t, nil)
		in := connectInput()
		in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://vault-corp/missing"}
		_, checks, err := h.svc.Create(ctx, ws, "a", in)
		var cf *spec.ChecksFailedError
		if !errors.As(err, &cf) {
			t.Fatalf("err = %v", err)
		}
		wantStatus(t, checks, "secret:password", spec.CheckFail)
	})
	t.Run("no resolver", func(t *testing.T) {
		h := newHarness(t, func(o *Options) { o.Resolver = nil })
		in := connectInput()
		in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://vault-corp/kafka/connect#password"}
		_, checks, err := h.svc.Create(ctx, ws, "a", in)
		var cf *spec.ChecksFailedError
		if !errors.As(err, &cf) {
			t.Fatalf("err = %v", err)
		}
		wantStatus(t, checks, "secret:password", spec.CheckFail)
	})
	t.Run("apply fails", func(t *testing.T) {
		h := newHarness(t, nil)
		h.app.err = errors.New("build failed for " + canary)
		view, checks, err := h.svc.Create(ctx, ws, "a", connectInput())
		if err != nil {
			t.Fatal(err)
		}
		wantStatus(t, checks, "apply", spec.CheckWarn)
		leakCheck(t, "checks", checks)
		if view.ID == "" {
			t.Fatal("row not saved")
		}
	})
	t.Run("store down", func(t *testing.T) {
		h := newHarness(t, nil)
		h.store.fail = errors.New("db down")
		if _, _, err := h.svc.Create(ctx, ws, "a", connectInput()); err == nil || !strings.Contains(err.Error(), "db down") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestEnvWins(t *testing.T) {
	env := EnvFromLookup(func(k string) (string, bool) {
		m := map[string]string{"CONNECT_URL": "https://connect.env:8083", "CONNECT_PASSWORD": canary, "CONNECT_USERNAME": "envuser",
			"GRAFANA_ALERT_SECRET": canary, "SOURCE_DB_URL": "postgres://ro:" + canary + "@db.env:5433/orders?sslmode=verify-full",
			"KAFKA_BROKERS": "   "}
		v, ok := m[k]
		return v, ok
	})
	if _, ok := env.Categories[spec.CategoryKafka]; ok {
		t.Fatal("a blank variable made kafka env-managed")
	}
	h := newHarness(t, func(o *Options) { o.Env = env })
	ctx := context.Background()
	if _, _, err := h.svc.Create(ctx, ws, "a", connectInput()); !errors.Is(err, spec.ErrManagedByEnv) {
		t.Fatalf("err = %v", err)
	}
	// Test is allowed: it stores nothing.
	if _, err := h.svc.Test(ctx, ws, "a", connectInput()); err != nil {
		t.Fatal(err)
	}
	// Other workspaces are not managed by this process's env.
	if _, _, err := h.svc.Create(ctx, "acme", "a", connectInput()); err != nil {
		t.Fatalf("hosted workspace: %v", err)
	}
	list, err := h.svc.List(ctx, ws)
	if err != nil {
		t.Fatal(err)
	}
	leakCheck(t, "env list", list)
	var connect, source, grafana *spec.View
	for i := range list {
		switch list[i].ID {
		case "env_connect":
			connect = &list[i]
		case "env_source_db":
			source = &list[i]
		case "env_alerts_grafana":
			grafana = &list[i]
		}
	}
	if connect == nil || source == nil || grafana == nil {
		t.Fatalf("env views missing: %+v", list)
	}
	if connect.Source != spec.SourceEnv || connect.Version != 0 || connect.UpdatedBy != "env" || connect.Status != spec.StatusUntested {
		t.Fatalf("connect view = %+v", connect)
	}
	// The fake registry has no connect-rest kind, so config shows nothing.
	if len(connect.Config) != 0 {
		t.Fatalf("unknown-kind env view shows config: %v", connect.Config)
	}
	if strings.Join(source.SecretsSet, ",") != "password" {
		t.Fatalf("source secrets_set = %v", source.SecretsSet)
	}
	// RotateAlertSecret refuses an env-managed source.
	if _, _, err := h.svc.RotateAlertSecret(ctx, ws, "a", "grafana"); !errors.Is(err, spec.ErrManagedByEnv) {
		t.Fatalf("rotate err = %v", err)
	}
	a, _ := h.svc.AlertSetup(ctx, ws, "grafana")
	if !a.SecretSet || a.SecretSource != spec.SourceEnv {
		t.Fatalf("alert setup = %+v", a)
	}
}

func TestEnvViewWithBuiltinKinds(t *testing.T) {
	env := EnvFromLookup(func(k string) (string, bool) {
		m := map[string]string{"CONNECT_URL": "https://connect.env:8083", "CONNECT_PASSWORD": "secret://vault-corp/connect#password",
			"SOURCE_DB_URL": "postgres://ro:" + canary + "@db.env:5433/orders?sslmode=verify-full", "WAREHOUSE_KIND": "snowflake",
			"KAFKA_BROKERS": "pkc-1.us-east-1.aws.confluent.cloud:9092"}
		v, ok := m[k]
		return v, ok
	})
	reg, err := NewBuiltinRegistry(Deps{})
	if err != nil {
		t.Fatal(err)
	}
	svc, _ := NewService(Options{Store: newMemStore(), Registry: reg, Env: env})
	list, _ := svc.List(context.Background(), ws)
	leakCheck(t, "list", list)
	got := map[string]spec.View{}
	for _, v := range list {
		got[v.ID] = v
	}
	if c := got["env_connect"]; c.Kind != KindConnectREST || c.Config["url"] != "https://connect.env:8083" {
		t.Fatalf("connect = %+v", c)
	} else if ref, ok := refOf(c.Config["password"]); !ok || ref != "secret://vault-corp/connect#password" {
		t.Fatalf("connect password ref = %v", c.Config["password"])
	}
	if s := got["env_source_db"]; s.Config["host"] != "db.env" || s.Config["port"] != "5433" || s.Config["user"] != "ro" || s.Config["database"] != "orders" {
		t.Fatalf("source = %+v", s.Config)
	}
	if got["env_warehouse"].Kind != KindSnowflake || got["env_kafka"].Kind != KindKafkaConfluentCloud {
		t.Fatalf("kinds: %s %s", got["env_warehouse"].Kind, got["env_kafka"].Kind)
	}
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, err := h.svc.Create(ctx, ws, "priya", connectInput())
	if err != nil {
		t.Fatal(err)
	}
	id := view.ID

	// Keep the password: the body leaves it out.
	in := connectInput()
	in.Config["username"] = "piperoom2"
	delete(in.Secrets, "password")
	v2, _, err := h.svc.Update(ctx, ws, "arjun", id, 1, in)
	if err != nil {
		t.Fatal(err)
	}
	if v2.Version != 2 || v2.Config["username"] != "piperoom2" || v2.UpdatedBy != "arjun" {
		t.Fatalf("v2 = %+v", v2)
	}
	if h.kind.last().Secret("password") != canary {
		t.Fatal("kept password not used in the test")
	}
	audit, _ := h.svc.Audit(ctx, ws, id, 10)
	if audit[0].Action != spec.AuditUpdated || strings.Join(audit[0].Changed, ",") != "username" {
		t.Fatalf("audit = %+v", audit[0])
	}

	// Stale version.
	if _, _, err := h.svc.Update(ctx, ws, "arjun", id, 1, in); !errors.Is(err, spec.ErrVersionConflict) {
		t.Fatalf("stale err = %v", err)
	}
	// Kind must match.
	bad := in
	bad.Kind = "secrets-fake"
	var ve *spec.ValidationError
	if _, _, err := h.svc.Update(ctx, ws, "arjun", id, 2, bad); !errors.As(err, &ve) || ve.Field != "kind" {
		t.Fatalf("kind err = %v", err)
	}
	// Clearing a required secret is a validation error.
	clr := connectInput()
	clr.Secrets["password"] = spec.SecretInput{Op: spec.SecretClear}
	if _, _, err := h.svc.Update(ctx, ws, "arjun", id, 2, clr); !errors.As(err, &ve) || ve.Field != "password" {
		t.Fatalf("clear err = %v", err)
	}
	// Optional secret: set, then clear.
	set := connectInput()
	delete(set.Secrets, "password")
	set.Config["username"] = "piperoom2"
	set.Secrets["ca_cert"] = setSecret("PEM-" + canary)
	v3, _, err := h.svc.Update(ctx, ws, "arjun", id, 2, set)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(v3.SecretsSet, ",") != "ca_cert,password" {
		t.Fatalf("secrets_set = %v", v3.SecretsSet)
	}
	leakCheck(t, "v3", v3)
	clrCA := connectInput()
	delete(clrCA.Secrets, "password")
	clrCA.Config["username"] = "piperoom2"
	clrCA.Secrets["ca_cert"] = spec.SecretInput{Op: spec.SecretClear}
	v4, _, err := h.svc.Update(ctx, ws, "arjun", id, 3, clrCA)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(v4.SecretsSet, ",") != "password" {
		t.Fatalf("secrets_set after clear = %v", v4.SecretsSet)
	}
	audit, _ = h.svc.Audit(ctx, ws, id, 1)
	if strings.Join(audit[0].Changed, ",") != "ca_cert" {
		t.Fatalf("changed = %v", audit[0].Changed)
	}
	// Switch the password to a reference: the sealed blob drops it.
	ref := connectInput()
	ref.Config["username"] = "piperoom2"
	ref.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://vault-corp/kafka/connect#password"}
	v5, _, err := h.svc.Update(ctx, ws, "arjun", id, 4, ref)
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := h.store.Get(ctx, ws, id)
	if rec.Sealed != nil {
		t.Fatal("sealed blob kept after moving the only secret to a reference")
	}
	if _, ok := refOf(v5.Config["password"]); !ok {
		t.Fatalf("config = %v", v5.Config)
	}
	// Failing checks leave the row unchanged.
	h.kind.checks = []spec.Check{{ID: "login", Status: spec.CheckFail}}
	if _, _, err := h.svc.Update(ctx, ws, "arjun", id, 5, ref); err == nil {
		t.Fatal("want ChecksFailedError")
	}
	if rec2, _ := h.store.Get(ctx, ws, id); rec2.Version != 5 {
		t.Fatalf("version moved to %d", rec2.Version)
	}
	// Another workspace cannot see the row.
	if _, _, err := h.svc.Update(ctx, "acme", "x", id, 5, ref); !errors.Is(err, spec.ErrNotFound) {
		t.Fatalf("cross-workspace err = %v", err)
	}
}

func TestUpdateWithWrongKey(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
	if err != nil {
		t.Fatal(err)
	}
	other, _ := NewKeyring(testKey(9), "")
	h.svc.o.Keys = other
	in := connectInput()
	delete(in.Secrets, "password")
	_, _, err = h.svc.Update(ctx, ws, "a", view.ID, 1, in)
	if !errors.Is(err, ErrOpen) || strings.Contains(err.Error(), canary) {
		t.Fatalf("err = %v", err)
	}
	// Rotation: the old key as previous opens it, and the save re-seals with the new key.
	rot, _ := NewKeyring(testKey(9), testKey(7))
	h.svc.o.Keys = rot
	if _, _, err := h.svc.Update(ctx, ws, "a", view.ID, 1, in); err != nil {
		t.Fatal(err)
	}
	if rec, _ := h.store.Get(ctx, ws, view.ID); rec.KeyID != rot.CurrentKeyID() {
		t.Fatal("not re-sealed with the current key")
	}
}

func TestTestReusesStoredSecretsAndStoresNothing(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, _ := h.svc.Create(ctx, ws, "a", connectInput())
	before, _ := h.store.Get(ctx, ws, view.ID)
	auditBefore := len(h.store.audit)
	in := connectInput()
	in.ID = view.ID
	delete(in.Secrets, "password")
	checks, err := h.svc.Test(ctx, ws, "a", in)
	if err != nil {
		t.Fatal(err)
	}
	leakCheck(t, "checks", checks)
	if h.kind.last().Secret("password") != canary {
		t.Fatal("stored secret not used")
	}
	after, _ := h.store.Get(ctx, ws, view.ID)
	if after.Version != before.Version || !after.LastTestAt.Equal(before.LastTestAt) || len(h.store.audit) != auditBefore {
		t.Fatal("Test stored something")
	}
	// Without ID, a missing required secret is a validation error.
	in.ID = ""
	var ve *spec.ValidationError
	if _, err := h.svc.Test(ctx, ws, "a", in); !errors.As(err, &ve) {
		t.Fatalf("err = %v", err)
	}
	// Test works without PIPEROOM_SECRET_KEY for typed values.
	nokey := newHarness(t, func(o *Options) { o.Keys = nil })
	if _, err := nokey.svc.Test(ctx, ws, "a", connectInput()); err != nil {
		t.Fatalf("test without key: %v", err)
	}
	// ID of another workspace is not found.
	in.ID = view.ID
	if _, err := h.svc.Test(ctx, "acme", "a", in); !errors.Is(err, spec.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestRateLimit(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	for i := 0; i < DefaultTestsPerMinute; i++ {
		if _, err := h.svc.Test(ctx, ws, "priya", connectInput()); err != nil {
			t.Fatalf("test %d: %v", i, err)
		}
	}
	if _, err := h.svc.Test(ctx, ws, "priya", connectInput()); !errors.Is(err, spec.ErrRateLimited) {
		t.Fatalf("11th err = %v", err)
	}
	if _, _, err := h.svc.Create(ctx, ws, "priya", connectInput()); !errors.Is(err, spec.ErrRateLimited) {
		t.Fatalf("create err = %v", err)
	}
	if _, err := h.svc.Test(ctx, ws, "arjun", connectInput()); err != nil {
		t.Fatalf("other admin: %v", err)
	}
	h.now = h.now.Add(61 * time.Second)
	if _, err := h.svc.Test(ctx, ws, "priya", connectInput()); err != nil {
		t.Fatalf("after a minute: %v", err)
	}
	// Validation errors do not use up the budget.
	h2 := newHarness(t, nil)
	bad := connectInput()
	bad.Config["evil"] = 1.0
	for i := 0; i < 20; i++ {
		_, _ = h2.svc.Test(ctx, ws, "priya", bad)
	}
	if _, err := h2.svc.Test(ctx, ws, "priya", connectInput()); err != nil {
		t.Fatalf("budget used by invalid input: %v", err)
	}
}

func TestEnableDisableDelete(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, _ := h.svc.Create(ctx, ws, "a", connectInput())
	off, err := h.svc.SetEnabled(ctx, ws, "b", view.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if off.Status != spec.StatusDisabled || off.Version != 2 || len(h.app.cleared) != 1 {
		t.Fatalf("disable: %+v cleared %v", off, h.app.cleared)
	}
	// Enabling builds from the stored (sealed) values.
	h.app.applied = nil
	on, err := h.svc.SetEnabled(ctx, ws, "b", view.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if on.Status != spec.StatusConnected || len(h.app.applied) != 1 || h.app.applied[0].Secret("password") != canary {
		t.Fatalf("enable: %+v", on)
	}
	// A failed build keeps the row as it was.
	_, _ = h.svc.SetEnabled(ctx, ws, "b", view.ID, false)
	h.app.err = errors.New("cannot build")
	if _, err := h.svc.SetEnabled(ctx, ws, "b", view.ID, true); err == nil {
		t.Fatal("want error")
	}
	if rec, _ := h.store.Get(ctx, ws, view.ID); rec.Enabled {
		t.Fatal("enabled despite the failed build")
	}
	h.app.err = nil
	actions := []spec.AuditAction{}
	audit, _ := h.svc.Audit(ctx, ws, view.ID, 0)
	for _, e := range audit {
		actions = append(actions, e.Action)
	}
	if fmt.Sprint(actions) != "[disabled enabled disabled created]" {
		t.Fatalf("actions = %v", actions)
	}
	// Delete clears only an enabled row's client, and keeps the audit.
	_, _ = h.svc.SetEnabled(ctx, ws, "b", view.ID, true)
	h.app.cleared = nil
	if err := h.svc.Delete(ctx, ws, "c", view.ID); err != nil {
		t.Fatal(err)
	}
	if len(h.app.cleared) != 1 {
		t.Fatal("delete did not clear the client")
	}
	if _, err := h.store.Get(ctx, ws, view.ID); !errors.Is(err, spec.ErrNotFound) {
		t.Fatal("row still there")
	}
	audit, _ = h.svc.Audit(ctx, ws, view.ID, 1)
	if audit[0].Action != spec.AuditDeleted || audit[0].Actor != "c" {
		t.Fatalf("audit = %+v", audit)
	}
	if err := h.svc.Delete(ctx, ws, "c", view.ID); !errors.Is(err, spec.ErrNotFound) {
		t.Fatalf("second delete err = %v", err)
	}
	if _, err := h.svc.SetEnabled(ctx, ws, "c", "int_nope", true); !errors.Is(err, spec.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestOnChangeWithoutApplier(t *testing.T) {
	ctx := context.Background()
	type change struct {
		cat    spec.Category
		client any
		hasCl  bool
	}
	var got []change
	h := newHarness(t, func(o *Options) {
		o.Applier = nil
		o.OnChange = func(c spec.Category, client any, closeFn func()) {
			got = append(got, change{c, client, closeFn != nil})
		}
	})
	view, _, _ := h.svc.Create(ctx, ws, "a", connectInput())
	_, _ = h.svc.SetEnabled(ctx, ws, "a", view.ID, false)
	if len(got) != 2 || got[0].client != "client" || !got[0].hasCl || got[1].client != nil || got[1].hasCl {
		t.Fatalf("changes = %+v", got)
	}
	// A failing Build on create gives a warning, not an error.
	h.kind.buildErr = errors.New("no")
	h2 := newHarness(t, func(o *Options) { o.Applier = nil; o.OnChange = func(spec.Category, any, func()) {} })
	h2.kind.buildErr = errors.New("no")
	_, checks, err := h2.svc.Create(ctx, ws, "a", connectInput())
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, checks, "apply", spec.CheckWarn)
}

func TestAuditLimits(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, _ := h.svc.Create(ctx, ws, "a", connectInput())
	for i := 0; i < 600; i++ {
		_ = h.store.AppendAudit(ctx, spec.AuditEntry{WorkspaceID: ws, IntegrationID: view.ID, Action: spec.AuditTested, Actor: "x"})
	}
	for limit, want := range map[int]int{0: 100, -5: 100, 10: 10, 500: 500, 10_000: 500} {
		got, err := h.svc.Audit(ctx, ws, view.ID, limit)
		if err != nil || len(got) != want {
			t.Fatalf("limit %d: %d entries, %v", limit, len(got), err)
		}
	}
	got, _ := h.svc.Audit(ctx, ws, "int_none", 0)
	if got == nil || len(got) != 0 {
		t.Fatalf("empty audit = %#v", got)
	}
}

func TestAlerts(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	if _, err := h.svc.AlertSetup(ctx, ws, "nagios"); !errors.Is(err, spec.ErrUnknownAlertSource) {
		t.Fatalf("err = %v", err)
	}
	if _, _, err := h.svc.RotateAlertSecret(ctx, ws, "a", "nagios"); !errors.Is(err, spec.ErrUnknownAlertSource) {
		t.Fatalf("err = %v", err)
	}
	a, err := h.svc.AlertSetup(ctx, ws, "alertmanager")
	if err != nil {
		t.Fatal(err)
	}
	if a.WebhookURL != "https://piperoom.corp/api/alerts/alertmanager" || a.SecretSet || a.IntegrationID != "" || a.SnippetLang != "yaml" ||
		!strings.Contains(a.Snippet, "<secret>") || !strings.Contains(a.Snippet, a.WebhookURL) {
		t.Fatalf("setup = %+v", a)
	}
	a1, s1, err := h.svc.RotateAlertSecret(ctx, ws, "priya", "alertmanager")
	if err != nil {
		t.Fatal(err)
	}
	if len(s1.Reveal()) < 40 || !a1.SecretSet || a1.SecretSource != spec.SourceDB || a1.IntegrationID == "" {
		t.Fatalf("rotate = %+v", a1)
	}
	js, _ := json.Marshal(a1)
	if strings.Contains(string(js), s1.Reveal()) || strings.Contains(a1.Snippet, s1.Reveal()) {
		t.Fatal("AlertSetup carries the secret")
	}
	if len(h.app.applied) != 1 || h.app.applied[0].Secret(alertSecretField) != s1 {
		t.Fatal("rotated secret not applied")
	}
	a2, s2, err := h.svc.RotateAlertSecret(ctx, ws, "priya", "alertmanager")
	if err != nil {
		t.Fatal(err)
	}
	if s2 == s1 || a2.IntegrationID != a1.IntegrationID {
		t.Fatal("second rotate did not replace the secret in the same row")
	}
	rec, _ := h.store.Get(ctx, ws, a1.IntegrationID)
	if rec.Version != 2 || strings.Contains(string(rec.Sealed), s2.Reveal()) {
		t.Fatalf("row = v%d", rec.Version)
	}
	audit, _ := h.svc.Audit(ctx, ws, a1.IntegrationID, 0)
	if len(audit) != 2 || audit[0].Action != spec.AuditSecretRotated || strings.Join(audit[0].Changed, ",") != "secret" {
		t.Fatalf("audit = %+v", audit)
	}
	got, _ := h.svc.AlertSetup(ctx, ws, "alertmanager")
	if !got.SecretSet || got.IntegrationID != a1.IntegrationID {
		t.Fatalf("setup after rotate = %+v", got)
	}
	// PagerDuty owns its secret: rotate refuses; create with the pasted value works.
	var ve *spec.ValidationError
	if _, _, err := h.svc.RotateAlertSecret(ctx, ws, "a", "pagerduty"); !errors.As(err, &ve) {
		t.Fatalf("pagerduty rotate err = %v", err)
	}
	pd, _, err := h.svc.Create(ctx, ws, "a", spec.Input{Kind: "alerts-pagerduty", Secrets: map[string]spec.SecretInput{"secret": setSecret(canary)}})
	if err != nil {
		t.Fatal(err)
	}
	leakCheck(t, "pagerduty view", pd)
	pdSetup, _ := h.svc.AlertSetup(ctx, ws, "pagerduty")
	if !pdSetup.SecretSet || pdSetup.IntegrationID != pd.ID {
		t.Fatalf("pagerduty setup = %+v", pdSetup)
	}
	// Other alert kinds are not created through the form.
	if _, _, err := h.svc.Create(ctx, ws, "a", spec.Input{Kind: "alerts-grafana"}); !errors.As(err, &ve) {
		t.Fatalf("grafana create err = %v", err)
	}
	// No key: rotation needs one.
	nokey := newHarness(t, func(o *Options) { o.Keys = nil })
	if _, _, err := nokey.svc.RotateAlertSecret(ctx, ws, "a", "generic"); !errors.Is(err, spec.ErrSecretKeyMissing) {
		t.Fatalf("err = %v", err)
	}
	for _, src := range AlertSources {
		s, lang := alertSnippet(src, "https://p/api/alerts/"+src)
		if s == "" || lang == "" || !strings.Contains(s, "https://p/api/alerts/"+src) {
			t.Fatalf("%s snippet: %q %q", src, s, lang)
		}
	}
}

func TestSecretNamesAndKeys(t *testing.T) {
	ctx := context.Background()
	many := make([]string, 300)
	for i := range many {
		many[i] = fmt.Sprintf("piperoom/s%03d", i)
	}
	b := &fakeBrowser{names: many}
	h := newHarness(t, func(o *Options) { o.Secrets = b })
	conn, _, _ := h.svc.Create(ctx, ws, "a", connectInput())
	if _, err := h.svc.SecretNames(ctx, ws, conn.ID, ""); !errors.Is(err, spec.ErrNotSecretManager) {
		t.Fatalf("err = %v", err)
	}
	sm, _, err := h.svc.Create(ctx, ws, "a", spec.Input{Kind: "secrets-fake", Config: map[string]any{"name": "vault-corp"}})
	if err != nil {
		t.Fatal(err)
	}
	names, err := h.svc.SecretNames(ctx, ws, sm.ID, "piperoom/")
	if err != nil || len(names) != maxSecretNames {
		t.Fatalf("names = %d, %v", len(names), err)
	}
	keys, err := h.svc.SecretKeys(ctx, ws, sm.ID, "secret://vault-corp/database/creds/ro")
	if err != nil || strings.Join(keys, ",") != "password,username" {
		t.Fatalf("keys = %v %v", keys, err)
	}
	var ve *spec.ValidationError
	if _, err := h.svc.SecretKeys(ctx, ws, sm.ID, "secret://aws-prod/x"); !errors.As(err, &ve) || ve.Field != "ref" {
		t.Fatalf("other provider err = %v", err)
	}
	if _, err := h.svc.SecretKeys(ctx, ws, sm.ID, "not-a-ref"); !errors.As(err, &ve) {
		t.Fatalf("bad ref err = %v", err)
	}
	b.err = spec.ErrProviderUnreachable
	if _, err := h.svc.SecretNames(ctx, ws, sm.ID, ""); !errors.Is(err, spec.ErrProviderUnreachable) {
		t.Fatalf("err = %v", err)
	}
	b.err = spec.ErrListUnsupported
	if _, err := h.svc.SecretNames(ctx, ws, sm.ID, ""); !errors.Is(err, spec.ErrListUnsupported) {
		t.Fatalf("err = %v", err)
	}
	if _, err := h.svc.SecretNames(ctx, "acme", sm.ID, ""); !errors.Is(err, spec.ErrNotFound) {
		t.Fatalf("cross-workspace err = %v", err)
	}
	// Without a browser, listing is unsupported.
	h2 := newHarness(t, nil)
	sm2, _, _ := h2.svc.Create(ctx, ws, "a", spec.Input{Kind: "secrets-fake", Config: map[string]any{"name": "vault-corp"}})
	if _, err := h2.svc.SecretNames(ctx, ws, sm2.ID, ""); !errors.Is(err, spec.ErrListUnsupported) {
		t.Fatalf("err = %v", err)
	}
	// The built-in "files" provider name is reserved.
	if _, _, err := h2.svc.Create(ctx, ws, "a", spec.Input{Kind: "secrets-fake", Config: map[string]any{"name": "files"}}); !errors.As(err, &ve) {
		t.Fatalf("files err = %v", err)
	}
	// Several managers per workspace, unique by name.
	if _, _, err := h2.svc.Create(ctx, ws, "a", spec.Input{Kind: "secrets-fake", Config: map[string]any{"name": "aws-prod"}}); err != nil {
		t.Fatalf("second manager: %v", err)
	}
	if _, _, err := h2.svc.Create(ctx, ws, "a", spec.Input{Kind: "secrets-fake", Config: map[string]any{"name": "aws-prod"}}); !errors.Is(err, spec.ErrDuplicate) {
		t.Fatalf("duplicate name err = %v", err)
	}
}

func TestListStatuses(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, _ := h.svc.Create(ctx, ws, "a", connectInput())
	_ = h.store.SaveLastTest(ctx, ws, view.ID, []spec.Check{{ID: "x", Status: spec.CheckFail}})
	list, _ := h.svc.List(ctx, ws)
	if len(list) != 1 || list[0].Status != spec.StatusFailing {
		t.Fatalf("list = %+v", list)
	}
	// A row with no test is untested; its JSON has [] and null, never missing.
	rec, _ := h.store.Get(ctx, ws, view.ID)
	rec.LastTest, rec.LastTestAt, rec.SecretsSet = nil, time.Time{}, nil
	v := h.svc.view(rec)
	js, _ := json.Marshal(v)
	if v.Status != spec.StatusUntested || !strings.Contains(string(js), `"last_test":[]`) || !strings.Contains(string(js), `"last_tested_at":null`) ||
		!strings.Contains(string(js), `"secrets_set":[]`) {
		t.Fatalf("view json = %s", js)
	}
	// A stored config that somehow holds a secret field's value is dropped.
	rec.Config = map[string]any{"url": "https://x", "password": canary, "extra": canary}
	leakCheck(t, "view of a bad row", h.svc.view(rec))
	if empty, _ := h.svc.List(ctx, "acme"); len(empty) != 0 || empty == nil {
		t.Fatalf("other workspace list = %#v", empty)
	}
}

func TestSetupScriptAndCatalog(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := h.svc.SetupScript("nope", nil); !errors.Is(err, spec.ErrUnknownKind) {
		t.Fatalf("err = %v", err)
	}
	s, err := h.svc.SetupScript("fake-connect", map[string]any{"username": "ro", "password": canary})
	if err != nil || s != "grant to ro" {
		t.Fatalf("script = %q %v", s, err)
	}
	if s, err := h.svc.SetupScript("secrets-fake", nil); s != "" || err != nil {
		t.Fatalf("no-script kind = %q %v", s, err)
	}
	// Built-in catalog: valid, no null slices, in category order.
	reg, err := NewBuiltinRegistry(Deps{})
	if err != nil {
		t.Fatal(err)
	}
	svc, _ := NewService(Options{Store: newMemStore(), Registry: reg})
	kinds := svc.Catalog()
	if len(kinds) < 14 {
		t.Fatalf("%d kinds", len(kinds))
	}
	last := -1
	order := map[spec.Category]int{}
	for i, c := range spec.Categories {
		order[c] = i
	}
	for _, k := range kinds {
		if order[k.Category] < last {
			t.Fatalf("catalog out of category order at %s", k.ID)
		}
		last = order[k.Category]
		js, _ := json.Marshal(spec.ToCatalog(k))
		if strings.Contains(string(js), "null") {
			t.Fatalf("%s catalog JSON has null: %s", k.ID, js)
		}
	}
	src, err := svc.SetupScript(KindPostgresSource, map[string]any{"user": "piperoom_ro", "database": "orders", "tables": "public.orders"})
	if err != nil || !strings.Contains(src, "GRANT SELECT ON public.orders TO piperoom_ro;") {
		t.Fatalf("postgres script: %v\n%s", err, src)
	}
	// A value failing its pattern is replaced by a placeholder.
	src, _ = svc.SetupScript(KindPostgresSource, map[string]any{"user": "x'; DROP ROLE y; --"})
	if strings.Contains(src, "DROP ROLE") {
		t.Fatalf("unsafe value in script:\n%s", src)
	}
}

func TestRegistryValidation(t *testing.T) {
	ok := func() spec.Kind {
		return spec.Kind{ID: "x", Category: spec.CategoryConnect, Test: func(context.Context, spec.Values) []spec.Check { return nil },
			Build: func(context.Context, spec.Values) (any, func(), error) { return nil, func() {}, nil }}
	}
	for name, mut := range map[string]func(*spec.Kind){
		"bad id":          func(k *spec.Kind) { k.ID = "X Y" },
		"bad category":    func(k *spec.Kind) { k.Category = "nope" },
		"no test":         func(k *spec.Kind) { k.Test = nil },
		"bad field":       func(k *spec.Kind) { k.Fields = []spec.Field{{Name: "Bad-Name"}} },
		"dup field":       func(k *spec.Kind) { k.Fields = []spec.Field{{Name: "a"}, {Name: "a"}} },
		"bad pattern":     func(k *spec.Kind) { k.Fields = []spec.Field{{Name: "a", Pattern: "("}} },
		"secret default":  func(k *spec.Kind) { k.Fields = []spec.Field{{Name: "a", Type: spec.FieldSecret, Default: "x"}} },
		"select no opts":  func(k *spec.Kind) { k.Fields = []spec.Field{{Name: "a", Type: spec.FieldSelect}} },
		"manager no name": func(k *spec.Kind) { k.Category = spec.CategorySecretManager },
	} {
		t.Run(name, func(t *testing.T) {
			k := ok()
			mut(&k)
			if err := NewRegistry().Register(k); err == nil {
				t.Fatal("want error")
			}
		})
	}
	r := NewRegistry()
	if err := r.Register(ok()); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(ok()); err == nil {
		t.Fatal("duplicate id accepted")
	}
}

func TestNewServiceNeedsDeps(t *testing.T) {
	if _, err := NewService(Options{}); err == nil {
		t.Fatal("want error")
	}
}

func TestParseRef(t *testing.T) {
	for ref, want := range map[string]string{
		"secret://vault-corp/database/creds/piperoom-ro#password": "vault-corp",
		"secret://files/run/secrets/kafka/password":               "files",
		"secret://aws-sm-prod/prod/piperoom/orders-db?version=3":  "aws-sm-prod",
	} {
		got, err := ParseRef(ref)
		if err != nil || got != want {
			t.Fatalf("%s: %q %v", ref, got, err)
		}
	}
	for _, ref := range []string{"", "secret://", "secret://UPPER/x", "secret://ok", "secret://ok/..", "secret://ok/a/%2E%2E/b", "secret://ok/a b", "http://ok/x", "secret://ok/%zz"} {
		if _, err := ParseRef(ref); err == nil {
			t.Fatalf("%q: want error", ref)
		}
	}
}

func TestLogSourcesHasNoValues(t *testing.T) {
	var buf strings.Builder
	log := slogTo(&buf)
	env := EnvFromLookup(func(k string) (string, bool) {
		if k == "CONNECT_PASSWORD" || k == "GENERIC_ALERT_SECRET" {
			return canary, true
		}
		return "", false
	})
	LogSources(log, env, []spec.Category{spec.CategoryKafka})
	out := buf.String()
	if strings.Contains(out, canary) || !strings.Contains(out, "connect from env") || !strings.Contains(out, "kafka from db") || !strings.Contains(out, "source_db not configured") {
		t.Fatalf("log = %s", out)
	}
}

func TestUpdateRefusedOnceEnvManages(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
	if err != nil {
		t.Fatal(err)
	}
	h.svc.o.Env = EnvFromLookup(func(k string) (string, bool) { return "https://c:8083", k == "CONNECT_URL" })
	in := connectInput()
	if _, _, err := h.svc.Update(ctx, ws, "a", view.ID, 1, in); !errors.Is(err, spec.ErrManagedByEnv) {
		t.Fatalf("update err = %v", err)
	}
	_, _ = h.svc.SetEnabled(ctx, ws, "a", view.ID, false)
	if _, err := h.svc.SetEnabled(ctx, ws, "a", view.ID, true); !errors.Is(err, spec.ErrManagedByEnv) {
		t.Fatalf("enable err = %v", err)
	}
	// Deleting the shadowed row is allowed.
	if err := h.svc.Delete(ctx, ws, "a", view.ID); err != nil {
		t.Fatal(err)
	}
}

// An alert row's generated secret is no Field: a form update must not be able
// to drop it, and enabling the row must still apply it.
func TestAlertSecretSurvivesUpdateAndEnable(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	a, sec, err := h.svc.RotateAlertSecret(ctx, ws, "priya", "grafana")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := h.store.Get(ctx, ws, a.IntegrationID)
	var ve *spec.ValidationError
	if _, _, err := h.svc.Update(ctx, ws, "priya", a.IntegrationID, before.Version, spec.Input{Kind: "alerts-grafana"}); !errors.As(err, &ve) || !strings.Contains(ve.Message, "rotate") {
		t.Fatalf("update err = %v", err)
	}
	after, _ := h.store.Get(ctx, ws, a.IntegrationID)
	if len(after.Sealed) == 0 || strings.Join(after.SecretsSet, ",") != alertSecretField || after.Version != before.Version {
		t.Fatalf("row changed: sealed=%d set=%v v%d", len(after.Sealed), after.SecretsSet, after.Version)
	}
	if _, err := h.svc.SetEnabled(ctx, ws, "priya", a.IntegrationID, false); err != nil {
		t.Fatal(err)
	}
	n := len(h.app.applied)
	if _, err := h.svc.SetEnabled(ctx, ws, "priya", a.IntegrationID, true); err != nil {
		t.Fatal(err)
	}
	if len(h.app.applied) != n+1 || h.app.applied[n].Secret(alertSecretField) != sec {
		t.Fatal("enabling the alert row did not apply its secret")
	}
	// PagerDuty keeps its form: an update without the secret keeps it.
	pd, _, err := h.svc.Create(ctx, ws, "a", spec.Input{Kind: "alerts-pagerduty", Secrets: map[string]spec.SecretInput{"secret": setSecret(canary)}})
	if err != nil {
		t.Fatal(err)
	}
	up, _, err := h.svc.Update(ctx, ws, "a", pd.ID, pd.Version, spec.Input{Kind: "alerts-pagerduty"})
	if err != nil || strings.Join(up.SecretsSet, ",") != alertSecretField {
		t.Fatalf("pagerduty update = %+v, %v", up, err)
	}
}

// A stale db row in a category the environment now manages must never clear
// the live client the environment set up (env wins, §4.4).
func TestStaleRowNeverClearsEnvClient(t *testing.T) {
	ctx := context.Background()
	for _, op := range []string{"disable", "delete"} {
		t.Run(op, func(t *testing.T) {
			h := newHarness(t, nil)
			view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
			if err != nil {
				t.Fatal(err)
			}
			h.svc.o.Env = EnvFromLookup(func(k string) (string, bool) { return "https://c:8083", k == "CONNECT_URL" })
			if op == "disable" {
				_, err = h.svc.SetEnabled(ctx, ws, "a", view.ID, false)
			} else {
				err = h.svc.Delete(ctx, ws, "a", view.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(h.app.cleared) != 0 {
				t.Fatalf("cleared = %v", h.app.cleared)
			}
		})
	}
	t.Run("alert", func(t *testing.T) {
		h := newHarness(t, nil)
		a, _, err := h.svc.RotateAlertSecret(ctx, ws, "a", "generic")
		if err != nil {
			t.Fatal(err)
		}
		h.svc.o.Env = EnvFromLookup(func(k string) (string, bool) { return "x", k == "GENERIC_ALERT_SECRET" })
		if err := h.svc.Delete(ctx, ws, "a", a.IntegrationID); err != nil {
			t.Fatal(err)
		}
		if len(h.app.cleared) != 0 {
			t.Fatalf("cleared = %v", h.app.cleared)
		}
	})
	t.Run("not env managed still clears", func(t *testing.T) {
		h := newHarness(t, nil)
		view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
		if err != nil {
			t.Fatal(err)
		}
		if err := h.svc.Delete(ctx, ws, "a", view.ID); err != nil {
			t.Fatal(err)
		}
		if len(h.app.cleared) != 1 {
			t.Fatalf("cleared = %v", h.app.cleared)
		}
	})
}

// Writes to an env view id answer managed_by_env, not not_found.
func TestEnvViewIDWrites(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	h.svc.o.Env = EnvFromLookup(func(k string) (string, bool) {
		return "x", k == "CONNECT_URL" || k == "GRAFANA_ALERT_SECRET"
	})
	for _, id := range []string{"env_connect", "env_alerts_grafana"} {
		if _, _, err := h.svc.Update(ctx, ws, "a", id, 0, spec.Input{Kind: "fake-connect"}); !errors.Is(err, spec.ErrManagedByEnv) {
			t.Fatalf("%s update err = %v", id, err)
		}
		if _, err := h.svc.SetEnabled(ctx, ws, "a", id, false); !errors.Is(err, spec.ErrManagedByEnv) {
			t.Fatalf("%s disable err = %v", id, err)
		}
		if err := h.svc.Delete(ctx, ws, "a", id); !errors.Is(err, spec.ErrManagedByEnv) {
			t.Fatalf("%s delete err = %v", id, err)
		}
	}
	// Not managed (or another workspace): still not found.
	for _, c := range []struct{ ws, id string }{{ws, "env_kafka"}, {"acme", "env_connect"}, {ws, "env_alerts_datadog"}} {
		if err := h.svc.Delete(ctx, c.ws, "a", c.id); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("%s/%s delete err = %v", c.ws, c.id, err)
		}
	}
}

func TestAlertSetupWithoutPublicURL(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.PublicURL = "" })
	a, err := h.svc.AlertSetup(context.Background(), ws, "grafana")
	if err != nil {
		t.Fatal(err)
	}
	if a.WebhookURL != "<PIPEROOM_PUBLIC_URL>/api/alerts/grafana" || !strings.Contains(a.Snippet, a.WebhookURL) {
		t.Fatalf("setup = %+v", a)
	}
}
