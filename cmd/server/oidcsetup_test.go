package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

func loadOwners(t *testing.T, yaml string) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	src := "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\n" + yaml
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func soloUsers(t *testing.T) *room.Users {
	t.Helper()
	u, err := room.ParseUsers([]byte("users:\n  - id: priya\n    name: Priya\n    layers: [postgres]\n  - id: arjun\n    name: Arjun\n    layers: [kafka]\n"))
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// Solo mode with other owners in owners.yaml does not start.
func TestSoloSetupRefusesOtherOwners(t *testing.T) {
	log, buf := testLog()
	users := soloUsers(t)
	cfg := config.Config{SoloUser: "priya"}

	for name, yaml := range map[string]string{
		"another user in layers":      "layers:\n  postgres: priya\n  kafka: arjun\n",
		"a group in layers":           "layers:\n  postgres: priya\n  kafka: group:oncall\n",
		"another user in a pipeline":  "layers:\n  postgres: priya\n",
		"another principal in audits": "layers:\n  postgres: priya\nauditors: [user:arjun]\n",
	} {
		own := loadOwners(t, yaml)
		if name == "another user in a pipeline" {
			own.Pipelines[0].Owners = map[string]owners.LayerRights{
				"kafka": {Approvers: owners.Principals{{Kind: owners.KindUser, Name: "arjun"}}},
			}
		}
		if _, err := soloSetup(cfg, users, own, log); err == nil || !strings.Contains(err.Error(), "solo mode") {
			t.Errorf("%s: soloSetup = %v, want a solo mode refusal", name, err)
		}
	}
	if buf.Len() != 0 {
		t.Errorf("a refused start logged a solo warning:\n%s", buf.String())
	}

	// The solo user must be in users.yaml.
	if _, err := soloSetup(config.Config{SoloUser: "ghost"}, users, loadOwners(t, "layers:\n  postgres: priya\n"), log); err == nil {
		t.Error("an unknown solo user started")
	}
}

func TestSoloSetupPassesAndWarns(t *testing.T) {
	log, buf := testLog()
	got, err := soloSetup(config.Config{SoloUser: " priya "}, soloUsers(t), loadOwners(t, "layers:\n  postgres: priya\n  kafka: [user:priya]\n"), log)
	if err != nil || got != "priya" {
		t.Fatalf("soloSetup = %q, %v", got, err)
	}
	if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "SOLO MODE") {
		t.Fatalf("no warning:\n%s", out)
	}
	// Off by default and silent.
	log2, buf2 := testLog()
	if got, err := soloSetup(config.Config{}, soloUsers(t), nil, log2); err != nil || got != "" || buf2.Len() != 0 {
		t.Fatalf("solo off: %q, %v, log %q", got, err, buf2.String())
	}
}

func oidcCfgFull() config.Config {
	return config.Config{
		AuthMode: "oidc", PublicURL: "https://piperoom.example.com", OIDCIssuer: "https://idp.example.com",
		OIDCClientID: "cid", OIDCClientSecret: "super-secret-value", OIDCScopes: "openid email",
		OIDCGroupsClaim: "groups", HTTPAddr: "127.0.0.1:8090",
	}
}

func TestOIDCSetup(t *testing.T) {
	log, buf := testLog()
	cfg := oidcCfgFull()
	cfg.AuthViewerGroups = "viewers, auditors-ro"
	cfg.TrustedProxies = "10.0.0.0/8"
	settings, err := authSetup(cfg, false, false, log)
	if err != nil {
		t.Fatal(err)
	}
	if len(settings.ViewerGroups) != 2 || settings.ViewerGroups[1] != "auditors-ro" || len(settings.TrustedProxies) != 1 {
		t.Fatalf("settings = %+v", settings)
	}
	pub, err := httpapi.ParsePublicURL(cfg.PublicURL)
	if err != nil {
		t.Fatal(err)
	}
	o, err := oidcSetup(cfg, settings, pub, log)
	if err != nil || o == nil {
		t.Fatalf("oidcSetup = %v, %v", o, err)
	}
	if o.Ready() {
		t.Fatal("ready without discovery: setup must not touch the network")
	}
	if strings.Contains(buf.String(), "super-secret-value") {
		t.Fatalf("the client secret was logged:\n%s", buf.String())
	}

	// Dev mode builds nothing.
	dev := config.Config{AuthMode: "dev", HTTPAddr: "127.0.0.1:8090"}
	ds, _ := authSetup(dev, false, false, log)
	if o, err := oidcSetup(dev, ds, httpapi.PublicOrigin{}, log); o != nil || err != nil {
		t.Fatalf("dev mode oidcSetup = %v, %v", o, err)
	}

	// An empty secret is allowed only for a public client.
	noSecret := oidcCfgFull()
	noSecret.OIDCClientSecret = ""
	if _, err := authSetup(noSecret, false, false, log); err == nil {
		t.Error("an empty client secret started without PIPEROOM_OIDC_PUBLIC_CLIENT=1")
	}
	noSecret.OIDCPublicClient = "1"
	s2, err := authSetup(noSecret, false, false, log)
	if err != nil {
		t.Fatalf("public client: %v", err)
	}
	if _, err := oidcSetup(noSecret, s2, pub, log); err != nil {
		t.Fatalf("public client oidcSetup: %v", err)
	}

	// A scope list without openid is refused.
	badScopes := oidcCfgFull()
	badScopes.OIDCScopes = "email profile"
	s3, _ := authSetup(badScopes, false, false, log)
	if _, err := oidcSetup(badScopes, s3, pub, log); err == nil {
		t.Error("scopes without openid were accepted")
	}

	// A bad trusted-proxy list stops the start.
	badProxy := oidcCfgFull()
	badProxy.TrustedProxies = "not-a-cidr"
	if _, err := authSetup(badProxy, false, false, log); err == nil {
		t.Error("a bad PIPEROOM_TRUSTED_PROXIES was accepted")
	}
	_ = auth.ModeOIDC
}
