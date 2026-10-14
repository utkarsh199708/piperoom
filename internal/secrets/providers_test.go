package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/integrations/spec"
)

func TestParseProviders(t *testing.T) {
	got, err := ParseProviders(" vault-corp=vault:https://vault.corp:8200; auth=kubernetes ;role=piperoom;" +
		"role_arn=arn:aws:iam::123:role/x;aws-prod=aws-secrets-manager:eu-west-1;;local=env:PIPEROOM_SECRET_;" +
		"bao=openbao:https://bao:8200;token=s.hunter2")
	if err != nil {
		t.Fatal(err)
	}
	want := []ProviderSpec{
		{ID: "vault-corp", Type: TypeVault, Address: "https://vault.corp:8200",
			Options: map[string]string{"auth": "kubernetes", "role": "piperoom", "role_arn": "arn:aws:iam::123:role/x"},
			Secrets: map[string]netsec.Secret{}},
		{ID: "aws-prod", Type: TypeAWSSecretsManager, Address: "eu-west-1", Options: map[string]string{}, Secrets: map[string]netsec.Secret{}},
		{ID: "local", Type: TypeEnv, Address: "PIPEROOM_SECRET_", Options: map[string]string{}, Secrets: map[string]netsec.Secret{}},
		{ID: "bao", Type: TypeOpenBao, Address: "https://bao:8200", Options: map[string]string{},
			Secrets: map[string]netsec.Secret{"token": "s.hunter2"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %#v\nwant %#v", got, want)
	}
	if s := got[3].String(); strings.Contains(s, "hunter2") || !strings.Contains(s, "token=[redacted]") {
		t.Fatalf("String = %q", s)
	}
	if empty, err := ParseProviders(""); err != nil || len(empty) != 0 {
		t.Fatalf("empty: %v %v", empty, err)
	}

	bad := []string{
		"auth=token",                        // option before a provider
		"Vault=vault:https://v",             // bad id
		"files=vault:https://v",             // reserved id
		"a=vault:https://v;a=env:X_",        // duplicate id
		"a=vault:https://v;auth=x;auth=y",   // duplicate option
		"a=vault:https://v;noequals",        // not key=value
		"a=mystery:https://v",               // unknown type: becomes an option before any provider
		"a=vault:https://v;token=1;token=2", // duplicate secret option
	}
	for _, s := range bad {
		_, err := ParseProviders(s)
		if !errors.Is(err, ErrBadProviderConfig) {
			t.Errorf("ParseProviders(%q) err = %v", s, err)
		}
		if err != nil && strings.Contains(err.Error(), "https://v") {
			t.Errorf("error quotes a value: %v", err)
		}
	}
}

func TestFactories(t *testing.T) {
	ctx := context.Background()
	f := DefaultFactories()
	build := func(decl string) (Provider, error) {
		specs, err := ParseProviders(decl)
		if err != nil {
			return nil, err
		}
		p, err := f[specs[0].Type](ctx, specs[0], Deps{
			LoadAWS: func(context.Context, string) (aws.Config, error) { return testAWSConfig(), nil },
			Getenv:  func(string) string { return "" },
		})
		if p != nil {
			t.Cleanup(p.Close)
		}
		return p, err
	}
	ok := []string{
		"v=vault:https://vault:8200;auth=approle;role_id=r;secret_id=s;namespace=ns;kv2_mounts=secret,kv;prefix=secret/piperoom/;timeout=3s",
		"v=openbao:https://bao:8200;token_file=/run/secrets/bao-token",
		"k=kubernetes:https://kube:6443;namespace=piperoom",
		"a=aws-secrets-manager:eu-west-1;endpoint=https://vpce.example",
		"s=aws-ssm:us-east-1",
		"e=env:PIPEROOM_SECRET_",
	}
	for _, d := range ok {
		if _, err := build(d); err != nil {
			t.Errorf("%s: %v", d, err)
		}
	}
	p, _ := build("v=vault:https://vault:8200;auth=approle;role_id=r;secret_id=s;kv2_mounts=kv")
	if vp := p.(*VaultProvider); !vp.kv2["kv"] || vp.kv2["secret"] {
		t.Errorf("kv2 mounts = %v", vp.kv2)
	}
	bad := []string{
		"v=vault:https://vault:8200;token=t;namspace=typo", // unknown option
		"v=vault:https://vault:8200;token=t;timeout=soon",
		"v=vault:http://vault.corp:8200;token=t", // no TLS
		"a=aws-secrets-manager:",                 // no region
		"e=env:",                                 // no prefix
		"k=kubernetes:",                          // outside a cluster
	}
	for _, d := range bad {
		if _, err := build(d); !errors.Is(err, ErrBadProviderConfig) {
			t.Errorf("%s: err = %v", d, err)
		}
	}
}

func TestParseTTL(t *testing.T) {
	if d, err := ParseTTL(""); err != nil || d != 5*time.Minute {
		t.Fatalf("default %v %v", d, err)
	}
	if d, err := ParseTTL("30s"); err != nil || d != 30*time.Second {
		t.Fatalf("30s %v %v", d, err)
	}
	for _, s := range []string{"5", "-1m", "0s", "soon"} {
		if _, err := ParseTTL(s); !errors.Is(err, ErrBadProviderConfig) {
			t.Errorf("%q err = %v", s, err)
		}
	}
}

func TestFromConfigAndResolveEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX paths")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "kafka"), []byte("file-pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"PIPEROOM_SECRET_DB": "env-pw"}
	cfg := config.Config{
		SecretFileRoots: root, SecretCacheTTL: "1m",
		SecretProviders: "local=env:PIPEROOM_SECRET_",
	}
	r, err := FromConfig(context.Background(), cfg, ConfigOptions{Deps: Deps{
		LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		Environ:   func() []string { return nil },
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	for raw, want := range map[string]string{
		"literal-value":                                "literal-value",
		"secret://local/PIPEROOM_SECRET_DB":            "env-pw",
		"secret://files/" + refPathOf(root) + "/kafka": "file-pw",
	} {
		got, err := ResolveEnv(ctx, r, raw)
		if err != nil || got.Reveal() != want {
			t.Errorf("ResolveEnv(%q) = %q, %v", raw, got.Reveal(), err)
		}
	}
	// Env-declared providers live in the default workspace only.
	if _, err := r.Resolve(ctx, "tenant-x", "secret://local/PIPEROOM_SECRET_DB"); !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("other workspace err = %v", err)
	}
	if _, err := ResolveEnv(ctx, nil, "secret://local/X"); err == nil {
		t.Fatal("nil resolver accepted a reference")
	}

	for _, c := range []config.Config{
		{SecretCacheTTL: "never"},
		{SecretProviders: "x=nope"},
		{SecretProviders: "v=vault:http://vault.corp;token=t"},
		{SecretFileRoots: "relative"},
	} {
		if _, err := FromConfig(context.Background(), c, ConfigOptions{}); err == nil {
			t.Errorf("%+v: want error", c)
		}
	}
	// A type with no factory in this binary.
	_, err = FromConfig(context.Background(), config.Config{SecretProviders: "az=azure-key-vault:https://kv.vault.azure.net"},
		ConfigOptions{Factories: DefaultFactories()})
	if !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("missing factory err = %v", err)
	}
	// Extra factories plug in (the azure and gcp sub-packages use this).
	fs := DefaultFactories()
	fs[TypeAzureKeyVault] = func(context.Context, ProviderSpec, Deps) (Provider, error) {
		return newFake(map[string]string{"x": "az"}), nil
	}
	r2, err := FromConfig(context.Background(), config.Config{SecretProviders: "az=azure-key-vault:https://kv"}, ConfigOptions{Factories: fs})
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if v, err := r2.Resolve(ctx, spec.DefaultWorkspace, "secret://az/x"); err != nil || v.Reveal() != "az" {
		t.Fatalf("%q %v", v.Reveal(), err)
	}
	_ = fmt.Sprint(r2)
}

func TestParseProvidersSecretOptionLooksLikeEntry(t *testing.T) {
	specs, err := ParseProviders("v=vault:https://vault:8200;token=vault:CANARY-tok;client_secret=env:CANARY-cs")
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 || specs[0].ID != "v" {
		t.Fatalf("specs = %v", specs)
	}
	if specs[0].Secrets["token"].Reveal() != "vault:CANARY-tok" || specs[0].Secrets["client_secret"].Reveal() != "env:CANARY-cs" {
		t.Fatal("secret options not kept as secrets")
	}
	if s := specs[0].String(); strings.Contains(s, "CANARY") {
		t.Fatalf("String leaks: %s", s)
	}
}
