package integrations

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/integrations/spec"
)

// A stored typed-in secret must never be sent to an address the caller
// has just changed (review finding: write-only bypass).
func TestStoredSecretDoesNotFollowChangedAddress(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		mod  func(in *spec.Input)
	}{
		{"url", func(in *spec.Input) { in.Config["url"] = "https://attacker.example" }},
		{"port", func(in *spec.Input) { in.Config["port"] = float64(9999) }},
		{"tls", func(in *spec.Input) { in.Config["tls"] = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
			if err != nil {
				t.Fatal(err)
			}
			seen := len(h.kind.seen)
			in := connectInput()
			in.ID = view.ID
			delete(in.Secrets, "password")
			tc.mod(&in)

			var ve *spec.ValidationError
			if _, err := h.svc.Test(ctx, ws, "a", in); !errors.As(err, &ve) || ve.Field != "password" || !strings.Contains(ve.Message, tc.name) {
				t.Fatalf("Test err = %v", err)
			}
			if _, _, err := h.svc.Update(ctx, ws, "a", view.ID, view.Version, in); !errors.As(err, &ve) || ve.Field != "password" {
				t.Fatalf("Update err = %v", err)
			}
			if len(h.kind.seen) != seen {
				t.Fatal("a test ran with the stored secret against the new address")
			}

			// Entering the secret again is accepted.
			in.Secrets["password"] = setSecret("new-one")
			if _, err := h.svc.Test(ctx, ws, "a", in); err != nil {
				t.Fatalf("Test with a new secret: %v", err)
			}
			if h.kind.last().Secret("password") != "new-one" {
				t.Fatal("new secret not used")
			}
		})
	}
}

func TestStoredSecretKeptWhenAddressUnchanged(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	view, _, err := h.svc.Create(ctx, ws, "a", connectInput())
	if err != nil {
		t.Fatal(err)
	}
	in := connectInput()
	in.ID = view.ID
	delete(in.Secrets, "password")
	in.Config["username"] = "someone-else" // not a destination field
	if _, err := h.svc.Test(ctx, ws, "a", in); err != nil {
		t.Fatal(err)
	}
	if h.kind.last().Secret("password") != canary {
		t.Fatal("stored secret not reused")
	}
}

func TestSecretManagerNameIsFixed(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, nil)
	in := spec.Input{Kind: "secrets-fake", Config: map[string]any{"name": "vault-a"}}
	view, _, err := h.svc.Create(ctx, ws, "a", in)
	if err != nil {
		t.Fatal(err)
	}
	in.Config["name"] = "vault-b"
	var ve *spec.ValidationError
	if _, _, err := h.svc.Update(ctx, ws, "a", view.ID, view.Version, in); !errors.As(err, &ve) || ve.Field != "name" {
		t.Fatalf("rename err = %v", err)
	}
	in.Config["name"] = "vault-a"
	if _, _, err := h.svc.Update(ctx, ws, "a", view.ID, view.Version, in); err != nil {
		t.Fatalf("same-name update: %v", err)
	}
}

func TestReservedRefsRefused(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, func(o *Options) {
		o.ReservedRefs = []string{"secret://files/run/secrets/piperoom-key", "not a ref"}
	})
	in := connectInput()
	in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://files/run/secrets/piperoom-key#other"}
	var ve *spec.ValidationError
	if _, err := h.svc.Test(ctx, ws, "a", in); !errors.As(err, &ve) || ve.Field != "password" {
		t.Fatalf("Test err = %v", err)
	}
	if _, _, err := h.svc.Create(ctx, ws, "a", in); !errors.As(err, &ve) || strings.Contains(ve.Message, "piperoom-key") {
		t.Fatalf("Create err = %v", err)
	}
	// Another secret of the same manager is fine.
	in.Secrets["password"] = spec.SecretInput{Op: spec.SecretRef, Ref: "secret://vault-corp/kafka/connect#password"}
	if _, _, err := h.svc.Create(ctx, ws, "a", in); err != nil {
		t.Fatalf("Create with an ordinary ref: %v", err)
	}
}
