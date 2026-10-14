package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/integrations"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
	"piperoom/internal/store/memstore"
	"piperoom/internal/wiring"
)

const srvCanary = "CANARY-srv-s3cr3t-41aa"

// fakeProvider is a secrets.Provider holding one value per path.
type fakeProvider struct {
	vals    map[string]string
	listErr error
	block   bool
	closed  bool
	mu      sync.Mutex
}

func (p *fakeProvider) Resolve(_ context.Context, r secrets.Ref) (secrets.Value, error) {
	v, ok := p.vals[r.Path]
	if !ok {
		return secrets.Value{}, secrets.ErrNotFound
	}
	return secrets.Value{Data: netsec.Secret(v)}, nil
}

func (p *fakeProvider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if p.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if p.listErr != nil {
		return nil, p.listErr
	}
	var out []string
	for k := range p.vals {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (p *fakeProvider) Keys(context.Context, secrets.Ref) ([]string, error) {
	return []string{"password", "username"}, nil
}
func (p *fakeProvider) Check(context.Context, *secrets.Ref) []spec.Check { return nil }
func (p *fakeProvider) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

// fakeLive records Apply and Clear, and fails Apply for url "broken".
type fakeLive struct {
	mu      sync.Mutex
	applied []spec.Integration
	values  []spec.Values
	cleared []spec.Category
}

func (f *fakeLive) Apply(_ context.Context, rec spec.Integration, _ spec.Kind, v spec.Values) error {
	if v.Get("url") == "broken" {
		return errors.New("dial: connection refused")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, rec)
	f.values = append(f.values, v)
	return nil
}

func (f *fakeLive) Clear(_ context.Context, rec spec.Integration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleared = append(f.cleared, rec.Category)
	return nil
}

func testKinds(t *testing.T, prov *fakeProvider) *integrations.Registry {
	t.Helper()
	reg := integrations.NewRegistry()
	for _, k := range []spec.Kind{
		{
			ID: "fake-connect", Category: spec.CategoryConnect, Title: "Kafka Connect",
			Fields: []spec.Field{{Name: "url", Type: spec.FieldText, Required: true}, {Name: "password", Type: spec.FieldSecret}, {Name: "mode", Type: spec.FieldText, Default: "ro"}},
		},
		{
			ID: "fake-registry", Category: spec.CategoryRegistry, Title: "Schema Registry",
			Fields: []spec.Field{{Name: "url", Type: spec.FieldText, Required: true}, {Name: "password", Type: spec.FieldSecret}},
		},
		{
			ID: "fake-vault", Category: spec.CategorySecretManager, Title: "Vault",
			Fields: []spec.Field{{Name: spec.ProviderNameField, Type: spec.FieldText, Required: true}},
			Build:  func(context.Context, spec.Values) (any, func(), error) { return prov, nil, nil },
		},
	} {
		if k.Test == nil {
			k.Test = func(context.Context, spec.Values) []spec.Check { return nil }
		}
		if k.Build == nil {
			k.Build = func(context.Context, spec.Values) (any, func(), error) { return nil, nil, nil }
		}
		if err := reg.Register(k); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range integrations.BuiltinKinds(integrations.Deps{}) {
		if k.Category == spec.CategoryAlerts {
			if err := reg.Register(k); err != nil {
				t.Fatal(err)
			}
		}
	}
	return reg
}

func testKeyring(t *testing.T) *integrations.Keyring {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	k, err := integrations.NewKeyring(netsec.Secret(base64.StdEncoding.EncodeToString(raw)), "")
	if err != nil {
		t.Fatal(err)
	}
	return k
}

type restoreEnv struct {
	setup *intSetup
	live  *fakeLive
	store *memstore.Integrations
	keys  *integrations.Keyring
	logs  *bytes.Buffer
	prov  *fakeProvider
}

func newRestoreEnv(t *testing.T, env integrations.Env) *restoreEnv {
	t.Helper()
	prov := &fakeProvider{vals: map[string]string{"kafka/connect": srvCanary + "-ref"}}
	logs := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	sreg := secrets.NewRegistry(secrets.Options{Logger: log})
	t.Cleanup(sreg.Close)
	live := &fakeLive{}
	st := memstore.NewIntegrations()
	keys := testKeyring(t)
	s := &intSetup{
		reg: testKinds(t, prov), keys: keys, store: st, secrets: sreg,
		applier: &intApplier{live: live, secrets: sreg, log: log}, env: env, log: log,
	}
	return &restoreEnv{setup: s, live: live, store: st, keys: keys, logs: logs, prov: prov}
}

// addRow stores a row with typed secrets sealed like the service does.
func (e *restoreEnv) addRow(t *testing.T, id string, cat spec.Category, kind string, cfg map[string]any, typed map[string]netsec.Secret, enabled bool) {
	t.Helper()
	rec := spec.Integration{
		ID: id, WorkspaceID: spec.DefaultWorkspace, Category: cat, Kind: kind, Scope: spec.ScopeAll,
		Config: cfg, Enabled: enabled, UpdatedBy: "arjun", Source: spec.SourceDB,
	}
	for k := range typed {
		rec.SecretsSet = append(rec.SecretsSet, k)
	}
	var err error
	if rec.Sealed, rec.KeyID, err = e.keys.Seal(integrations.SealAAD(rec.WorkspaceID, rec.ID, rec.Category), typed); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Create(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreAppliesSavedRows(t *testing.T) {
	e := newRestoreEnv(t, integrations.Env{Categories: map[spec.Category]integrations.EnvCategory{
		spec.CategoryRegistry: {Kind: "fake-registry"},
	}, AlertSecrets: map[string]bool{"datadog": true}})
	// A reference row before its secret manager in list order: restore must
	// still register the manager first.
	e.addRow(t, "int_a", spec.CategoryConnect, "fake-connect",
		map[string]any{"url": "http://connect:8083", "password": map[string]any{spec.RefKey: "secret://vault-corp/kafka/connect"}}, nil, true)
	e.addRow(t, "int_b", spec.CategorySecretManager, "fake-vault", map[string]any{"name": "vault-corp"}, nil, true)
	// Env-managed category: skipped.
	e.addRow(t, "int_c", spec.CategoryRegistry, "fake-registry", map[string]any{"url": "http://sr"}, map[string]netsec.Secret{"password": srvCanary}, true)
	// Saved alert secrets: grafana used, datadog shadowed by env.
	e.addRow(t, "int_d", spec.CategoryAlerts, integrations.AlertKindID("grafana"), map[string]any{}, map[string]netsec.Secret{"secret": srvCanary + "-g"}, true)
	e.addRow(t, "int_e", spec.CategoryAlerts, integrations.AlertKindID("datadog"), map[string]any{}, map[string]netsec.Secret{"secret": "dd"}, true)

	res := e.setup.restore(context.Background())

	if len(e.live.applied) != 1 || e.live.applied[0].ID != "int_a" {
		t.Fatalf("applied %+v", e.live.applied)
	}
	v := e.live.values[0]
	if v.Secret("password").Reveal() != srvCanary+"-ref" || v.Get("mode") != "ro" || v.Get("url") != "http://connect:8083" {
		t.Fatalf("values: url=%q mode=%q ref resolved=%v", v.Get("url"), v.Get("mode"), v.Secret("password").Reveal() == srvCanary+"-ref")
	}
	if _, err := e.setup.secrets.Provider(spec.DefaultWorkspace, "vault-corp"); err != nil {
		t.Fatalf("secret manager not registered: %v", err)
	}
	if got := res.applied; len(got) != 2 || got[0] != spec.CategorySecretManager || got[1] != spec.CategoryConnect {
		t.Fatalf("applied categories %v", got)
	}
	if res.alertSecrets["grafana"].Reveal() != srvCanary+"-g" || res.alertSecrets["datadog"] != "" {
		t.Fatalf("alert secrets: grafana set=%v datadog=%q", res.alertSecrets["grafana"] != "", res.alertSecrets["datadog"])
	}
	if strings.Contains(e.logs.String(), srvCanary) {
		t.Fatalf("logs leak a secret: %s", e.logs.String())
	}
}

func TestRestoreLeavesBrokenRowsNotConfigured(t *testing.T) {
	e := newRestoreEnv(t, integrations.Env{})
	e.addRow(t, "int_a", spec.CategoryConnect, "fake-connect", map[string]any{"url": "broken"}, map[string]netsec.Secret{"password": srvCanary}, true)
	e.addRow(t, "int_b", spec.CategoryRegistry, "fake-registry", map[string]any{"url": "http://sr", "password": map[string]any{spec.RefKey: "secret://nobody/x"}}, nil, true)
	e.addRow(t, "int_c", spec.CategoryWarehouse, "gone-kind", map[string]any{}, nil, true)
	e.addRow(t, "int_d", spec.CategoryKafka, "fake-connect", map[string]any{"url": "x"}, nil, false) // disabled

	res := e.setup.restore(context.Background())
	if len(e.live.applied) != 0 || len(res.applied) != 0 {
		t.Fatalf("nothing should apply: %+v", e.live.applied)
	}
	logs := e.logs.String()
	for _, want := range []string{"int_a", "int_b", "int_c"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("no log line for %s: %s", want, logs)
		}
	}
	if strings.Contains(logs, srvCanary) {
		t.Fatalf("logs leak a secret: %s", logs)
	}
}

func TestRestoreWithoutKeyOrWithWrongKey(t *testing.T) {
	e := newRestoreEnv(t, integrations.Env{})
	e.addRow(t, "int_a", spec.CategoryConnect, "fake-connect", map[string]any{"url": "http://c"}, map[string]netsec.Secret{"password": srvCanary}, true)
	e.setup.keys = nil
	e.setup.restore(context.Background())
	e.setup.keys = testKeyring(t)
	e.setup.restore(context.Background())
	if len(e.live.applied) != 0 {
		t.Fatal("a row whose secrets cannot be opened must not apply")
	}
}

func TestRestoreStoreDown(t *testing.T) {
	e := newRestoreEnv(t, integrations.Env{})
	e.setup.store = downStore{e.store}
	res := e.setup.restore(context.Background())
	if len(res.applied) != 0 || !strings.Contains(e.logs.String(), "cannot list") {
		t.Fatalf("store down: %v %s", res.applied, e.logs.String())
	}
}

type downStore struct{ spec.Store }

func (downStore) List(context.Context, string) ([]spec.Integration, error) {
	return nil, errors.New("db down")
}

func TestApplierRoutesCategories(t *testing.T) {
	e := newRestoreEnv(t, integrations.Env{})
	a := e.setup.applier
	ctx := context.Background()
	vault, _ := e.setup.reg.Kind("fake-vault")
	rec := spec.Integration{ID: "int_v", WorkspaceID: spec.DefaultWorkspace, Category: spec.CategorySecretManager, Config: map[string]any{"name": "vault-corp"}}
	if err := a.Apply(ctx, rec, vault, spec.Values{Config: map[string]string{"name": "vault-corp"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.setup.secrets.Provider(spec.DefaultWorkspace, "vault-corp"); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(ctx, rec, vault, spec.Values{Config: map[string]string{}}); err == nil {
		t.Fatal("a secret manager without a name must fail")
	}
	if err := a.Clear(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if _, err := e.setup.secrets.Provider(spec.DefaultWorkspace, "vault-corp"); !errors.Is(err, secrets.ErrUnknownProvider) {
		t.Fatalf("clear should unregister: %v", err)
	}
	// A kind that builds something else is refused.
	bad := spec.Kind{ID: "bad", Category: spec.CategorySecretManager, Build: func(context.Context, spec.Values) (any, func(), error) { return "x", nil, nil }}
	if err := a.Apply(ctx, rec, bad, spec.Values{Config: map[string]string{"name": "v2"}}); err == nil {
		t.Fatal("a non-provider client must fail")
	}
	// Alerts and chat never fail (enable must not become a 500).
	for _, c := range []spec.Category{spec.CategoryAlerts, spec.CategoryChat} {
		if err := a.Apply(ctx, spec.Integration{Category: c}, spec.Kind{}, spec.Values{}); err != nil {
			t.Fatalf("%s: %v", c, err)
		}
	}
	// Data categories go to Live; demo (no Live) builds nothing.
	conn, _ := e.setup.reg.Kind("fake-connect")
	crec := spec.Integration{ID: "int_c", Category: spec.CategoryConnect}
	if err := a.Apply(ctx, crec, conn, spec.Values{Config: map[string]string{"url": "u"}}); err != nil || len(e.live.applied) != 1 {
		t.Fatalf("live apply: %v %d", err, len(e.live.applied))
	}
	if err := a.Clear(ctx, crec); err != nil || len(e.live.cleared) != 1 {
		t.Fatalf("live clear: %v", err)
	}
	demo := &intApplier{log: slog.New(slog.DiscardHandler)}
	if err := demo.Apply(ctx, crec, conn, spec.Values{}); err != nil {
		t.Fatal(err)
	}
	if err := demo.Clear(ctx, crec); err != nil {
		t.Fatal(err)
	}
}

func TestSecretBrowser(t *testing.T) {
	sreg := secrets.NewRegistry(secrets.Options{})
	t.Cleanup(sreg.Close)
	b := secretBrowser{r: sreg}
	rec := spec.Integration{ID: "int_v", WorkspaceID: spec.DefaultWorkspace, Category: spec.CategorySecretManager, Config: map[string]any{"name": "vault-corp"}}
	ctx := context.Background()

	if _, err := b.Names(ctx, rec, "", 10); !errors.Is(err, spec.ErrProviderUnreachable) {
		t.Fatalf("not registered: %v", err)
	}
	p := &fakeProvider{vals: map[string]string{"kafka/a": srvCanary, "db/b": "x"}}
	if err := sreg.Register(spec.DefaultWorkspace, "vault-corp", p); err != nil {
		t.Fatal(err)
	}
	names, err := b.Names(ctx, rec, "kafka/", 10)
	if err != nil || len(names) != 1 || names[0] != "kafka/a" {
		t.Fatalf("names %v %v", names, err)
	}
	keys, err := b.Keys(ctx, rec, "secret://vault-corp/kafka/a")
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys %v %v", keys, err)
	}
	var ve *spec.ValidationError
	if _, err := b.Keys(ctx, rec, "nope"); !errors.As(err, &ve) {
		t.Fatalf("bad ref: %v", err)
	}
	p.listErr = secrets.ErrListUnsupported
	if _, err := b.Names(ctx, rec, "", 10); !errors.Is(err, spec.ErrListUnsupported) {
		t.Fatalf("unsupported: %v", err)
	}
	p.listErr = nil
	p.block = true
	tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := b.Names(tctx, rec, "", 10); !errors.Is(err, spec.ErrProviderUnreachable) {
		t.Fatalf("timeout: %v", err)
	}
	// Another workspace's row never reaches this provider.
	other := rec
	other.WorkspaceID = "ws2"
	if _, err := b.Names(ctx, other, "", 10); !errors.Is(err, spec.ErrProviderUnreachable) {
		t.Fatalf("cross workspace: %v", err)
	}
}

// fakeResolver resolves one reference.
type fakeResolver struct{}

func (fakeResolver) Resolve(_ context.Context, ws, ref string) (netsec.Secret, error) {
	if ws == spec.DefaultWorkspace && ref == "secret://files/kafka/password" {
		return netsec.Secret(srvCanary), nil
	}
	return "", secrets.ErrNotFound
}

func TestResolveEnvSecrets(t *testing.T) {
	cfg := config.Config{KafkaSASLPassword: "secret://files/kafka/password", ConnectPassword: "plain"}
	if err := resolveEnvSecrets(context.Background(), &cfg, fakeResolver{}); err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaSASLPassword != srvCanary || cfg.ConnectPassword != "plain" {
		t.Fatal("reference not resolved or plain value changed")
	}
	cfg = config.Config{SlackBotToken: "secret://files/missing"}
	err := resolveEnvSecrets(context.Background(), &cfg, fakeResolver{})
	if err == nil || !strings.Contains(err.Error(), "SLACK_BOT_TOKEN") || !errors.Is(err, secrets.ErrNotFound) {
		t.Fatalf("missing ref: %v", err)
	}
}

func TestProcessSecretRefs(t *testing.T) {
	cfg := config.Config{SecretKey: "secret://files/run/secrets/key", KafkaSASLPassword: "plain", OIDCClientSecret: "secret://vault/oidc#cs"}
	got := processSecretRefs(cfg)
	if len(got) != 2 || !slices.Contains(got, "secret://vault/oidc#cs") || !slices.Contains(got, "secret://files/run/secrets/key") {
		t.Fatalf("refs = %v", got)
	}
}

func TestWithSavedAlertSecretsEnvWins(t *testing.T) {
	a := withSavedAlertSecrets(alertin.Config{GrafanaSecret: "from-env"}, map[string]netsec.Secret{
		"grafana": "saved-g", "generic": "saved-x", "alertmanager": "saved-a",
	})
	if a.GrafanaSecret != "from-env" || a.GenericSecret != "saved-x" || a.AlertmanagerToken != "saved-a" || a.DatadogToken != "" {
		t.Fatalf("%+v", a)
	}
}

func TestDataStackFromEnv(t *testing.T) {
	all := []spec.Category{spec.CategorySourceDB, spec.CategoryWarehouse, spec.CategoryKafka, spec.CategoryConnect, spec.CategoryRegistry, spec.CategoryModel}
	for _, c := range []struct {
		name    string
		cats    []spec.Category
		open    bool
		partial bool
	}{
		{"none", nil, false, false},
		{"model and chat only", []spec.Category{spec.CategoryModel, spec.CategoryChat}, false, false},
		{"all five", all, true, false},
		{"kafka only", []spec.Category{spec.CategoryModel, spec.CategoryKafka}, false, true},
		{"four of five", all[1:], false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			open, err := dataStackFromEnv(c.cats)
			if open != c.open || errors.Is(err, errPartialDataStack) != c.partial || (err != nil) != c.partial {
				t.Fatalf("dataStackFromEnv = %v, %v", open, err)
			}
		})
	}
}

// TestDataStackFromEnvLookup: one env category set (KAFKA_BROKERS) and the
// others missing refuses start with an error naming the missing ones,
// instead of dialing localhost defaults for them.
func TestDataStackFromEnvLookup(t *testing.T) {
	env := map[string]string{"KAFKA_BROKERS": "b1:9092", "SOURCE_DB_URL": "  "}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	open, err := dataStackFromEnv(wiring.EnvCategories(lookup))
	if open || !errors.Is(err, errPartialDataStack) {
		t.Fatalf("got %v, %v", open, err)
	}
	for _, want := range []string{"set kafka", "source_db", "warehouse", "connect", "registry"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error should name %q: %v", want, err)
		}
	}
}

func TestCheckAnthropicKey(t *testing.T) {
	for _, c := range []struct {
		v    string
		set  bool
		fail bool
	}{
		{"", false, false},
		{"sk-ant-x", true, false},
		{"secret://vault/anthropic#key", true, true},
		{" secret://vault/anthropic#key", true, true},
	} {
		lookup := func(string) (string, bool) { return c.v, c.set }
		if err := checkAnthropicKey(lookup); errors.Is(err, errAnthropicKeyRef) != c.fail {
			t.Fatalf("checkAnthropicKey(%q) = %v", c.v, err)
		}
	}
}

// TestDemoApplierBuildsNothing: in --demo enabling a secret-manager row
// must not build (and so dial) a provider.
func TestDemoApplierBuildsNothing(t *testing.T) {
	built := false
	k := spec.Kind{ID: "fake-vault", Category: spec.CategorySecretManager,
		Build: func(context.Context, spec.Values) (any, func(), error) {
			built = true
			return nil, nil, errors.New("dialed")
		}}
	a := &intApplier{demo: true, log: slog.Default()}
	rec := spec.Integration{ID: "int_1", WorkspaceID: spec.DefaultWorkspace, Category: spec.CategorySecretManager, Kind: k.ID,
		Config: map[string]any{spec.ProviderNameField: "vault"}}
	if err := a.Apply(context.Background(), rec, k, spec.Values{Config: map[string]string{spec.ProviderNameField: "vault"}}); err != nil || built {
		t.Fatalf("demo apply: err=%v built=%v", err, built)
	}
	if err := a.Clear(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}

func TestNewIntegrationsKeyHandling(t *testing.T) {
	logs := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	o := intOptions{Store: memstore.NewIntegrations(), Log: log}
	s, err := newIntegrations(config.Config{}, o)
	if err != nil || s.keys != nil || !strings.Contains(logs.String(), "PIPEROOM_SECRET_KEY is not set") {
		t.Fatalf("no key: %v %s", err, logs.String())
	}
	if _, err := newIntegrations(config.Config{SecretKey: "not-base64!"}, o); err == nil {
		t.Fatal("a bad key must refuse start")
	}
	if _, err := newIntegrations(config.Config{IntegrationAllowedHosts: "bad host!"}, o); err == nil {
		t.Fatal("a bad allow list must refuse start")
	}
	raw := make([]byte, 32)
	key := base64.StdEncoding.EncodeToString(raw)
	s, err = newIntegrations(config.Config{SecretKey: key}, o)
	if err != nil || s.keys == nil || len(s.svc.Catalog()) == 0 {
		t.Fatalf("with key: %v", err)
	}
	if strings.Contains(logs.String(), key) {
		t.Fatal("the key was logged")
	}
}

func TestLateNotifier(t *testing.T) {
	var l lateNotifier
	l.IntegrationChanged(context.Background(), spec.DefaultWorkspace, spec.AuditEntry{}, "x") // dropped, no panic
	r := &recNotifier{}
	l.set(r)
	l.IntegrationChanged(context.Background(), spec.DefaultWorkspace, spec.AuditEntry{Action: spec.AuditUpdated}, "Kafka")
	if r.n != 1 {
		t.Fatal("not forwarded")
	}
}

type recNotifier struct{ n int }

func (r *recNotifier) IntegrationChanged(context.Context, string, spec.AuditEntry, string) { r.n++ }

func TestApplierEnvDeclaredProviderWins(t *testing.T) {
	e := newRestoreEnv(t, integrations.Env{})
	a := e.setup.applier
	a.envProviders = envProviderIDs(config.Config{SecretProviders: "vault-corp=vault:https://vault.corp:8200;auth=token"})
	if !a.envProviders["vault-corp"] {
		t.Fatalf("env ids = %v", a.envProviders)
	}
	ctx := context.Background()
	vault, _ := e.setup.reg.Kind("fake-vault")
	for _, name := range []string{"vault-corp", spec.ProviderFiles} {
		rec := spec.Integration{ID: "int_v", WorkspaceID: spec.DefaultWorkspace, Category: spec.CategorySecretManager, Config: map[string]any{"name": name}}
		if err := a.Apply(ctx, rec, vault, spec.Values{Config: map[string]string{"name": name}}); !errors.Is(err, spec.ErrManagedByEnv) {
			t.Fatalf("%s: Apply = %v, want ErrManagedByEnv", name, err)
		}
	}
	// Another workspace may use the same name: env providers live only in default.
	rec := spec.Integration{ID: "int_w", WorkspaceID: "ws2", Category: spec.CategorySecretManager, Config: map[string]any{"name": "vault-corp"}}
	if err := a.Apply(ctx, rec, vault, spec.Values{Config: map[string]string{"name": "vault-corp"}}); err != nil {
		t.Fatalf("other workspace: %v", err)
	}
}

func TestCatalogHasSecretManagerCards(t *testing.T) {
	s, err := newIntegrations(config.Config{}, intOptions{Store: memstore.NewIntegrations(), Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, k := range s.svc.Catalog() {
		if k.Category == spec.CategorySecretManager {
			n++
		}
	}
	if n < 6 {
		t.Fatalf("secret_manager cards = %d, want at least 6", n)
	}
}
