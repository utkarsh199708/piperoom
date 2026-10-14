package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/integrations/spec"
)

// TestNoKindLeaksASecret runs every built-in kind's Test against a dead
// target with the canary in every secret field, and checks that no check,
// setup script or build error carries it.
func TestNoKindLeaksASecret(t *testing.T) {
	port := closedPort(t)
	g, err := NewGuard("", fakeResolver{"acme.snowflakecomputing.com": {"127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	d := fastDeps()
	d.Guard, d.sfPort = g, port
	d.slackBase = "http://127.0.0.1:" + port
	d.anthropicBase = "http://127.0.0.1:" + port
	// The Snowflake driver would dial the real account host; echo the DSN
	// back in an error instead, to prove it is redacted.
	d.sfNew = func(_ context.Context, dsn string) (any, func(), error) {
		return nil, nil, errors.New("driver failed for " + dsn)
	}
	d = d.withDefaults()
	for _, k := range BuiltinKinds(d) {
		t.Run(k.ID, func(t *testing.T) {
			v := vals(map[string]string{}, map[string]string{})
			for _, f := range k.Fields {
				switch {
				case f.Type.IsSecret():
					v.Secrets[f.Name] = canary
				case f.Name == "host":
					v.Config[f.Name] = "127.0.0.1"
				case f.Name == "port":
					v.Config[f.Name] = port
				case f.Name == "url":
					v.Config[f.Name] = "http://127.0.0.1:" + port
				case f.Name == "brokers" || f.Name == "bootstrap":
					v.Config[f.Name] = "127.0.0.1:" + port
				case f.Name == "account":
					v.Config[f.Name] = "acme"
				case f.Name == "auth":
					v.Config[f.Name] = sfAuthPassword
				case f.Default != "":
					v.Config[f.Name] = f.Default
				case f.Type == spec.FieldText:
					v.Config[f.Name] = "piperoom"
				}
			}
			checks := k.Test(context.Background(), v)
			js, _ := json.Marshal(checks)
			if strings.Contains(string(js), canary) {
				t.Fatalf("checks leak the secret: %s", js)
			}
			if k.SetupScript != nil && strings.Contains(k.SetupScript(v), canary) {
				t.Fatal("setup script leaks the secret")
			}
			if _, closeFn, err := k.Build(context.Background(), v); err != nil {
				if strings.Contains(err.Error(), canary) {
					t.Fatalf("build error leaks the secret: %v", err)
				}
			} else if closeFn != nil {
				closeFn()
			}
		})
	}
}
