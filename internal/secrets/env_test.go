package secrets

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"piperoom/internal/integrations/spec"
)

func TestEnvProvider(t *testing.T) {
	env := map[string]string{
		"PIPEROOM_SECRET_DB":    `{"password":"p"}`,
		"PIPEROOM_SECRET_KAFKA": "k",
		"PIPEROOM_SECRET_KEY":   "the-sealing-key",
		"HOME":                  "/root",
	}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	environ := func() []string {
		var out []string
		for k, v := range env {
			out = append(out, k+"="+v)
		}
		return out
	}
	if _, err := NewEnvProvider(EnvOptions{}); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("no prefix err = %v", err)
	}
	p, err := NewEnvProvider(EnvOptions{Prefix: "PIPEROOM_SECRET_", LookupEnv: lookup, Environ: environ})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if v, err := p.Resolve(ctx, Ref{Path: "PIPEROOM_SECRET_KAFKA"}); err != nil || v.Data.Reveal() != "k" {
		t.Fatalf("%q %v", v.Data.Reveal(), err)
	}
	for _, own := range []string{"PIPEROOM_SECRET_KEY", "PIPEROOM_SECRET_KEY_PREVIOUS", "PIPEROOM_SECRET_PROVIDERS"} {
		env[own] = "x"
		if _, err := p.Resolve(ctx, Ref{Path: own}); !errors.Is(err, ErrAccessDenied) {
			t.Fatalf("%s err = %v", own, err)
		}
	}
	delete(env, "PIPEROOM_SECRET_KEY_PREVIOUS")
	delete(env, "PIPEROOM_SECRET_PROVIDERS")
	if _, err := p.Resolve(ctx, Ref{Path: "HOME"}); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("outside prefix err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "PIPEROOM_SECRET_NOPE"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "PIPEROOM_SECRET_KAFKA", Version: "1"}); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("version err = %v", err)
	}
	names, _ := p.List(ctx, "PIPEROOM_SECRET_K", 10)
	// The process's own settings are never listed or read, even when the
	// operator's prefix covers them.
	if !reflect.DeepEqual(names, []string{"PIPEROOM_SECRET_KAFKA"}) {
		t.Fatalf("names = %v", names)
	}
	keys, _ := p.Keys(ctx, Ref{Path: "PIPEROOM_SECRET_DB"})
	if !reflect.DeepEqual(keys, []string{"password"}) {
		t.Fatalf("keys = %v", keys)
	}
	if checks := p.Check(ctx, &Ref{Path: "PIPEROOM_SECRET_DB", Key: "password"}); spec.Failed(checks) {
		t.Fatalf("checks = %+v", checks)
	}
	if checks := p.Check(ctx, &Ref{Path: "PIPEROOM_SECRET_NOPE"}); !spec.Failed(checks) {
		t.Fatalf("missing sample should fail: %+v", checks)
	}
}
