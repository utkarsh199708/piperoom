package integrations

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake"

	"piperoom/internal/integrations/spec"
)

type fakeSF struct {
	pingErr  error
	grants   []sfGrant
	grantErr error
	gotCfg   gosnowflake.Config
	closed   bool
}

func (f *fakeSF) Ping(context.Context) error { return f.pingErr }
func (f *fakeSF) Grants(_ context.Context, role string) ([]sfGrant, error) {
	return f.grants, f.grantErr
}
func (f *fakeSF) Close() error { f.closed = true; return nil }

func rsaPEM(t *testing.T) string {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func sfDeps(t *testing.T, f *fakeSF) Deps {
	t.Helper()
	_, port := listenTCP(t)
	g, err := NewGuard("", fakeResolver{"acme-prod.snowflakecomputing.com": {"127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	d := fastDeps()
	d.Guard = g
	d.sfPort = port
	d.sfOpen = func(cfg gosnowflake.Config) (sfSession, error) { f.gotCfg = cfg; return f, nil }
	return d.withDefaults()
}

func TestSnowflakeChecks(t *testing.T) {
	key := rsaPEM(t)
	v := func() spec.Values {
		return vals(map[string]string{"account": "acme-prod", "user": "PIPEROOM", "role": "PIPEROOM_RO", "warehouse": "WH",
			"database": "DB", "schema": "PUBLIC", "auth": "key_pair"}, map[string]string{"private_key": key})
	}
	ro := []sfGrant{{"USAGE", "WAREHOUSE", "WH"}, {"USAGE", "DATABASE", "DB"}, {"SELECT", "TABLE", "DB.PUBLIC.ORDERS"}}
	tests := []struct {
		name string
		f    *fakeSF
		want map[string]spec.CheckStatus
		fix  string
	}{
		{"read-only", &fakeSF{grants: ro}, map[string]spec.CheckStatus{"reach": spec.CheckPass, "login": spec.CheckPass, "read_only": spec.CheckPass}, ""},
		{"can insert", &fakeSF{grants: append(ro, sfGrant{"INSERT", "TABLE", "DB.PUBLIC.ORDERS"})}, map[string]spec.CheckStatus{"read_only": spec.CheckFail},
			"REVOKE INSERT ON TABLE DB.PUBLIC.ORDERS FROM ROLE PIPEROOM_RO;"},
		{"owns schema", &fakeSF{grants: append(ro, sfGrant{"OWNERSHIP", "SCHEMA", "DB.PUBLIC"})}, map[string]spec.CheckStatus{"read_only": spec.CheckFail}, "REVOKE OWNERSHIP"},
		{"create table", &fakeSF{grants: append(ro, sfGrant{"CREATE TABLE", "SCHEMA", "DB.PUBLIC"})}, map[string]spec.CheckStatus{"read_only": spec.CheckFail}, ""},
		{"inherits", &fakeSF{grants: append(ro, sfGrant{"USAGE", "ROLE", "ANALYST"})}, map[string]spec.CheckStatus{"read_only": spec.CheckWarn}, ""},
		{"grants unreadable", &fakeSF{grantErr: errors.New("insufficient privileges")}, map[string]spec.CheckStatus{"read_only": spec.CheckSkipped}, ""},
		{"login rejected", &fakeSF{pingErr: &gosnowflake.SnowflakeError{Number: 390144, Message: "JWT token is invalid"}}, map[string]spec.CheckStatus{"login": spec.CheckFail, "read_only": spec.CheckSkipped}, "RSA_PUBLIC_KEY"},
		{"login error", &fakeSF{pingErr: errors.New("boom")}, map[string]spec.CheckStatus{"login": spec.CheckFail}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checks := sfDeps(t, tc.f).snowflakeKind().Test(context.Background(), v())
			for id, st := range tc.want {
				c := wantStatus(t, checks, id, st)
				if tc.fix != "" && st == spec.CheckFail && !strings.Contains(c.Fix, tc.fix) {
					t.Fatalf("fix = %q, want %q", c.Fix, tc.fix)
				}
			}
			if tc.f.gotCfg.Authenticator != gosnowflake.AuthTypeJwt || tc.f.gotCfg.PrivateKey == nil || tc.f.gotCfg.Transporter == nil {
				t.Fatalf("driver config: auth %v, key set %v, transporter set %v", tc.f.gotCfg.Authenticator, tc.f.gotCfg.PrivateKey != nil, tc.f.gotCfg.Transporter != nil)
			}
			for _, c := range checks {
				if strings.Contains(c.Detail+c.Fix, "PRIVATE KEY") {
					t.Fatal("key material in a check")
				}
			}
			if !tc.f.closed {
				t.Fatal("session not closed")
			}
		})
	}
}

func TestSnowflakeInput(t *testing.T) {
	d := sfDeps(t, &fakeSF{})
	for name, tc := range map[string]struct {
		cfg   map[string]string
		sec   map[string]string
		field string
	}{
		"no key":      {map[string]string{"account": "acme-prod", "auth": "key_pair"}, nil, "private_key"},
		"bad key":     {map[string]string{"account": "acme-prod"}, map[string]string{"private_key": "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----"}, "private_key"},
		"no password": {map[string]string{"account": "acme-prod", "auth": "password"}, nil, "password"},
		"bad role":    {map[string]string{"account": "acme-prod", "role": `x"; drop`}, nil, "role"},
		"no account":  {map[string]string{}, nil, "account"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sfConfig(vals(tc.cfg, tc.sec))
			var ve *spec.ValidationError
			if !errors.As(err, &ve) || ve.Field != tc.field {
				t.Fatalf("err = %v, want field %s", err, tc.field)
			}
			wantStatus(t, d.snowflakeKind().Test(context.Background(), vals(tc.cfg, tc.sec)), "input", spec.CheckFail)
		})
	}
	// Password login sets the password, not the key.
	cfg, err := sfConfig(vals(map[string]string{"account": "a", "auth": "password"}, map[string]string{"password": canary}))
	if err != nil || cfg.Password != canary || cfg.PrivateKey != nil {
		t.Fatalf("password cfg: %v", err)
	}
}

func TestSnowflakeUnreachable(t *testing.T) {
	g, _ := NewGuard("", fakeResolver{"acme-prod.snowflakecomputing.com": {"127.0.0.1"}})
	d := fastDeps()
	d.Guard, d.sfPort = g, closedPort(t)
	d.sfOpen = func(gosnowflake.Config) (sfSession, error) { t.Fatal("must not open"); return nil, nil }
	checks := d.withDefaults().snowflakeKind().Test(context.Background(),
		vals(map[string]string{"account": "acme-prod", "auth": "password"}, map[string]string{"password": canary}))
	wantStatus(t, checks, "reach", spec.CheckFail)
}

func TestSnowflakeSetupScript(t *testing.T) {
	s := sfSetupScript(vals(map[string]string{"role": "piperoom_ro", "warehouse": "wh", "database": "db", "schema": "public", "user": "piperoom"}, nil))
	for _, want := range []string{"CREATE ROLE IF NOT EXISTS PIPEROOM_RO;", "GRANT SELECT ON ALL TABLES IN SCHEMA DB.PUBLIC TO ROLE PIPEROOM_RO;", "RSA_PUBLIC_KEY"} {
		if !strings.Contains(s, want) {
			t.Fatalf("script lacks %q:\n%s", want, s)
		}
	}
	if bad := sfSetupScript(vals(map[string]string{"role": "x; DROP DATABASE prod"}, nil)); strings.Contains(bad, "DROP") {
		t.Fatalf("unsafe role in script:\n%s", bad)
	}
}

func TestSFWrite(t *testing.T) {
	for _, tc := range []struct {
		p    string
		want bool
	}{{"SELECT", false}, {"USAGE", false}, {"REFERENCES", false}, {"INSERT", true}, {"TRUNCATE", true}, {"OWNERSHIP", true}, {"CREATE VIEW", true}, {"APPLYBUDGET", true}} {
		if got := sfWrite(sfGrant{Privilege: tc.p}); got != tc.want {
			t.Fatalf("%s: %v", tc.p, got)
		}
	}
}
