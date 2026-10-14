package secretkinds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

const canary = "CANARY-7f3e-do-not-print"

type fakeProvider struct {
	sample *secrets.Ref
	closed int
}

func (p *fakeProvider) Resolve(context.Context, secrets.Ref) (secrets.Value, error) {
	return secrets.Value{}, secrets.ErrNotFound
}
func (p *fakeProvider) List(context.Context, string, int) ([]string, error) { return nil, nil }
func (p *fakeProvider) Keys(context.Context, secrets.Ref) ([]string, error) { return nil, nil }
func (p *fakeProvider) Check(_ context.Context, s *secrets.Ref) []spec.Check {
	p.sample = s
	return []spec.Check{{ID: secrets.CheckReach, Title: "reach", Status: spec.CheckPass}}
}
func (p *fakeProvider) Close() { p.closed++ }

// recorder is a factory that keeps what it was given.
type recorder struct {
	spec secrets.ProviderSpec
	deps secrets.Deps
	p    *fakeProvider
	err  error
}

func (r *recorder) factory(_ context.Context, s secrets.ProviderSpec, d secrets.Deps) (secrets.Provider, error) {
	r.spec, r.deps = s, d
	if r.err != nil {
		return nil, r.err
	}
	r.p = &fakeProvider{}
	return r.p, nil
}

func fakeDeps(r *recorder) Deps {
	fs := map[string]secrets.Factory{}
	for _, t := range []string{secrets.TypeVault, secrets.TypeAWSSecretsManager, secrets.TypeAWSSSM,
		secrets.TypeKubernetes, secrets.TypeAzureKeyVault, secrets.TypeGoogleSecretManager} {
		fs[t] = r.factory
	}
	return Deps{Factories: fs}
}

func kindByID(t *testing.T, d Deps, id string) spec.Kind {
	t.Helper()
	for _, k := range Kinds(d) {
		if k.ID == id {
			return k
		}
	}
	t.Fatalf("no kind %s", id)
	return spec.Kind{}
}

func vals(cfg map[string]string, sec map[string]netsec.Secret) spec.Values {
	return spec.Values{Config: cfg, Secrets: sec}
}

func TestRegisterIntoCatalog(t *testing.T) {
	reg, err := integrations.NewBuiltinRegistry(integrations.Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if err := Register(reg, Deps{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	n := 0
	for _, k := range reg.Kinds() {
		if k.Category == spec.CategorySecretManager {
			n++
			if k.SetupScript == nil || len(k.Reads) == 0 || len(k.Never) == 0 {
				t.Errorf("%s: missing script, reads or never", k.ID)
			}
		}
	}
	if n != 6 {
		t.Fatalf("secret_manager kinds = %d, want 6", n)
	}
	if err := Register(reg, Deps{}); err == nil {
		t.Fatal("second Register should refuse duplicate kinds")
	}
}

func TestFactoriesCoverEveryCard(t *testing.T) {
	fs := Factories()
	for _, c := range []card{vaultCard(), awsSMCard(), awsSSMCard(), kubeCard(), azureCard(), gcpCard()} {
		if fs[c.ptype] == nil {
			t.Errorf("%s: no factory for %s", c.id, c.ptype)
		}
	}
}

func TestSpecMapping(t *testing.T) {
	tests := []struct {
		kind    string
		v       spec.Values
		addr    string
		opts    map[string]string
		secrets []string
		guarded bool
	}{
		{
			kind: KindVault,
			v: vals(map[string]string{"name": "vault-corp", "address": " https://vault.corp:8200 ", "auth": "approle",
				"role_id": "r1", "namespace": "", "prefix": "secret/piperoom/", "kv2_mounts": "secret,kv"},
				map[string]netsec.Secret{"secret_id": "sid", "token": ""}),
			addr: "https://vault.corp:8200", opts: map[string]string{"auth": "approle", "role_id": "r1", "prefix": "secret/piperoom/", "kv2_mounts": "secret,kv"},
			secrets: []string{"secret_id"}, guarded: true,
		},
		{kind: KindAWSSM, v: vals(map[string]string{"name": "aws-prod", "region": "eu-west-1", "account_id": "123456789012"}, nil),
			addr: "eu-west-1", opts: map[string]string{}},
		{kind: KindAWSSSM, v: vals(map[string]string{"name": "ssm", "region": "us-east-1"}, nil), addr: "us-east-1", opts: map[string]string{}},
		{kind: KindKubernetes, v: vals(map[string]string{"name": "k8s", "namespace": "data"}, nil),
			opts: map[string]string{"namespace": "data"}},
		{kind: KindKubernetes, v: vals(map[string]string{"name": "k8s", "address": "https://k8s.corp:6443"}, nil),
			addr: "https://k8s.corp:6443", opts: map[string]string{}, guarded: true},
		{kind: KindAzure, v: vals(map[string]string{"name": "akv", "vault_url": "https://acme.vault.azure.net", "auth": "auto", "client_id": "c"},
			map[string]netsec.Secret{"client_secret": "cs"}),
			addr: "https://acme.vault.azure.net", opts: map[string]string{"client_id": "c"}, secrets: []string{"client_secret"}, guarded: true},
		{kind: KindGCP, v: vals(map[string]string{"name": "gsm", "project": "acme-prod", "prefix": "piperoom-"}, nil),
			addr: "acme-prod", opts: map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.kind+"/"+tc.addr, func(t *testing.T) {
			r := &recorder{}
			k := kindByID(t, fakeDeps(r), tc.kind)
			client, closeFn, err := k.Build(context.Background(), tc.v)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if _, ok := client.(secrets.Provider); !ok {
				t.Fatalf("Build returned %T", client)
			}
			if r.spec.ID != tc.v.Get("name") || r.spec.Address != tc.addr {
				t.Errorf("spec id/address = %q/%q", r.spec.ID, r.spec.Address)
			}
			if fmt.Sprint(r.spec.Options) != fmt.Sprint(tc.opts) {
				t.Errorf("options = %v, want %v", r.spec.Options, tc.opts)
			}
			var got []string
			for k := range r.spec.Secrets {
				got = append(got, k)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.secrets) && !(len(got) == 0 && len(tc.secrets) == 0) {
				t.Errorf("secrets = %v, want %v", got, tc.secrets)
			}
			if (r.deps.HTTPClient != nil) != tc.guarded {
				t.Errorf("guarded client = %v, want %v", r.deps.HTTPClient != nil, tc.guarded)
			}
			closeFn()
			if r.p.closed != 1 {
				t.Errorf("close called %d times", r.p.closed)
			}
		})
	}
}

func TestTestPaths(t *testing.T) {
	base := map[string]string{"name": "vault-corp", "address": "https://vault.corp:8200"}
	with := func(k, v string) map[string]string {
		m := map[string]string{}
		for a, b := range base {
			m[a] = b
		}
		m[k] = v
		return m
	}
	t.Run("sample passed as a ref", func(t *testing.T) {
		r := &recorder{}
		k := kindByID(t, fakeDeps(r), KindVault)
		checks := k.Test(context.Background(), vals(with("sample", "/secret/piperoom/db#password"), nil))
		if len(checks) != 1 || checks[0].Status != spec.CheckPass {
			t.Fatalf("checks = %+v", checks)
		}
		if r.p.sample == nil || r.p.sample.Provider != "vault-corp" || r.p.sample.Path != "secret/piperoom/db" || r.p.sample.Key != "password" {
			t.Fatalf("sample = %+v", r.p.sample)
		}
		if r.p.closed != 1 {
			t.Error("Test did not close the provider")
		}
	})
	t.Run("no sample", func(t *testing.T) {
		r := &recorder{}
		kindByID(t, fakeDeps(r), KindVault).Test(context.Background(), vals(base, nil))
		if r.p.sample != nil {
			t.Fatal("sample should be nil")
		}
	})
	cases := []struct {
		name, id, detail string
		cfg              map[string]string
		ferr             error
	}{
		{"bad sample", "sample", "not a valid path", with("sample", "a/../b"), nil},
		{"bad name", "config", "name must match", with("name", "Bad Name"), nil},
		{"reserved name", "config", "reserved", with("name", spec.ProviderFiles), nil},
		{"bad config", "config", "unknown options x", base, fmt.Errorf("w: %w: provider \"v\" (vault): unknown options x", secrets.ErrBadProviderConfig)},
		{"other error hides text", "config", "could not be set up", base, errors.New("dial " + canary)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{err: tc.ferr}
			checks := kindByID(t, fakeDeps(r), KindVault).Test(context.Background(), vals(tc.cfg, nil))
			if len(checks) != 1 || checks[0].ID != tc.id || checks[0].Status != spec.CheckFail || !strings.Contains(checks[0].Detail, tc.detail) {
				t.Fatalf("checks = %+v", checks)
			}
			if strings.Contains(fmt.Sprintf("%+v", checks), canary) {
				t.Fatal("canary leaked")
			}
		})
	}
	t.Run("bad CA", func(t *testing.T) {
		r := &recorder{}
		checks := kindByID(t, fakeDeps(r), KindVault).Test(context.Background(), vals(base, map[string]netsec.Secret{"ca_cert": "not pem"}))
		if len(checks) != 1 || checks[0].ID != "config" || !strings.Contains(checks[0].Detail, "PEM") {
			t.Fatalf("checks = %+v", checks)
		}
	})
}

// TestRealVaultDownUsesDialAndLeaksNothing runs the real Vault factory
// against a dial that refuses, with a canary in every secret field.
func TestRealVaultDownUsesDialAndLeaksNothing(t *testing.T) {
	dialed := 0
	d := Deps{
		TestTimeout: 3 * time.Second,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialed++
			return nil, errors.New("refused by guard")
		},
	}
	k := kindByID(t, d, KindVault)
	v := vals(map[string]string{"name": "vault-corp", "address": "https://vault.corp:8200", "auth": "token", "sample": "secret/piperoom/db#password"},
		map[string]netsec.Secret{"token": canary, "secret_id": canary, "jwt": canary})
	checks := k.Test(context.Background(), v)
	if len(checks) == 0 {
		t.Fatal("no checks")
	}
	failed := false
	for _, c := range checks {
		failed = failed || c.Status == spec.CheckFail
	}
	if !failed {
		t.Fatalf("expected a failing check, got %+v", checks)
	}
	if dialed == 0 {
		t.Fatal("the guarded dialer was not used")
	}
	b, _ := json.Marshal(checks)
	for _, out := range []string{string(b), fmt.Sprintf("%v %+v %#v", checks, checks, checks)} {
		if strings.Contains(out, canary) {
			t.Fatalf("canary in check output: %s", out)
		}
	}
	p, closeFn, err := k.Build(context.Background(), v)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer closeFn()
	if s := fmt.Sprintf("%v %+v %#v %s", p, p, p, p); strings.Contains(s, canary) {
		t.Fatal("canary in provider print")
	}
}

func TestSetupScripts(t *testing.T) {
	evil := "x\"; rm -rf / #"
	for _, k := range Kinds(Deps{}) {
		t.Run(k.ID, func(t *testing.T) {
			cfg := map[string]string{}
			for _, f := range k.Fields {
				cfg[f.Name] = evil
			}
			s := k.SetupScript(spec.Values{Config: cfg})
			if strings.Contains(s, "rm -rf") {
				t.Fatalf("pasted text reached the script:\n%s", s)
			}
			if strings.TrimSpace(s) == "" {
				t.Fatal("empty script")
			}
		})
	}
	t.Run("aws json is valid and scoped", func(t *testing.T) {
		for _, svc := range []string{"secretsmanager", "ssm"} {
			s := awsScript(spec.Values{Config: map[string]string{"region": "eu-west-1", "account_id": "123456789012", "prefix": "piperoom/"}}, svc)
			var lines []string
			for _, l := range strings.Split(s, "\n") {
				if !strings.HasPrefix(l, "//") {
					lines = append(lines, l)
				}
			}
			var doc map[string]any
			if err := json.Unmarshal([]byte(strings.Join(lines, "\n")), &doc); err != nil {
				t.Fatalf("%s: invalid JSON: %v\n%s", svc, err, s)
			}
			if !strings.Contains(s, ":123456789012:") || !strings.Contains(s, "piperoom/*") || !strings.Contains(s, svc+".eu-west-1.amazonaws.com") {
				t.Errorf("%s: script not scoped:\n%s", svc, s)
			}
		}
	})
	t.Run("vault kv2 and kv1", func(t *testing.T) {
		s := vaultScript(spec.Values{Config: map[string]string{"prefix": "secret/piperoom/", "auth": "kubernetes", "role": "pr"}})
		if !strings.Contains(s, `path "secret/data/piperoom/*"`) || !strings.Contains(s, `path "secret/metadata/piperoom/*"`) || !strings.Contains(s, "auth/kubernetes/role/pr") {
			t.Errorf("kv2 script:\n%s", s)
		}
		s = vaultScript(spec.Values{Config: map[string]string{"prefix": "kv1/piperoom", "kv2_mounts": "secret", "auth": "approle"}})
		if !strings.Contains(s, `path "kv1/piperoom/*"`) || !strings.Contains(s, "auth/approle/role/piperoom/role-id") {
			t.Errorf("kv1 script:\n%s", s)
		}
	})
	t.Run("kubernetes resourceNames", func(t *testing.T) {
		s := kubeScript(spec.Values{Config: map[string]string{"namespace": "data", "secret_names": "db, kafka ,Bad_Name"}})
		if !strings.Contains(s, `resourceNames: ["db", "kafka"]`) || !strings.Contains(s, "namespace: data") || strings.Contains(s, "list") && strings.Contains(s, `verbs: ["get", "list"]`) {
			t.Errorf("script:\n%s", s)
		}
	})
}
