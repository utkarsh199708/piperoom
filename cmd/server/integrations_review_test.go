package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/integrations"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

// E10: picker refusals become field errors the API answers with a 4xx.
func TestBrowseErrMapsRefusals(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason spec.SecretRefReason
	}{
		{fmt.Errorf("%w: '..' in prefix", secrets.ErrInvalidRef), spec.SecretRefInvalid},
		{fmt.Errorf("%w: listing is limited to the provider's prefix", secrets.ErrAccessDenied), spec.SecretRefDenied},
		{fmt.Errorf("secrets: vault GET x: %w", secrets.ErrNotFound), spec.SecretRefNotFound},
		{fmt.Errorf("%w: outside", secrets.ErrOutsideRoots), spec.SecretRefDenied},
	} {
		var se *spec.SecretRefError
		if err := browseErr("prefix", tc.err); !errors.As(err, &se) || se.Reason != tc.reason || se.Field != "prefix" {
			t.Fatalf("%v → %v", tc.err, err)
		}
	}
	// A list refusal stays "cannot list" (the UI shows a text field).
	if err := browseErr("prefix", fmt.Errorf("%w: %w", secrets.ErrListUnsupported, secrets.ErrAccessDenied)); !errors.Is(err, spec.ErrListUnsupported) {
		t.Fatalf("list unsupported → %v", err)
	}
}

// E4: a row restore cannot build is shown failing, not connected.
func TestRestoreMarksBrokenRowsNotLive(t *testing.T) {
	e := newRestoreEnv(t, integrations.Env{})
	e.addRow(t, "int_a", spec.CategoryConnect, "fake-connect", map[string]any{"url": "http://c"}, map[string]netsec.Secret{"password": srvCanary}, true)
	e.addRow(t, "int_b", spec.CategoryRegistry, "fake-registry", map[string]any{"url": "http://sr", "password": map[string]any{spec.RefKey: "secret://gone/x"}}, nil, true)
	ctx := context.Background()
	for _, id := range []string{"int_a", "int_b"} {
		if err := e.store.SaveLastTest(ctx, spec.DefaultWorkspace, id, []spec.Check{{ID: "reach", Status: spec.CheckPass}}); err != nil {
			t.Fatal(err)
		}
	}
	e.withKeys(t, testKeyring(t)) // another key: int_a's secrets cannot be opened
	e.setup.restore(ctx)
	list, err := e.setup.svc.List(ctx, spec.DefaultWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range list {
		if v.Status != spec.StatusFailing {
			t.Fatalf("%s status = %s, want failing", v.ID, v.Status)
		}
		var live *spec.Check
		for i := range v.LastTest {
			if v.LastTest[i].ID == "live" {
				live = &v.LastTest[i]
			}
		}
		if live == nil || !strings.HasPrefix(live.Detail, "not connected at start") || strings.Contains(live.Detail, srvCanary) {
			t.Fatalf("%s live check = %+v", v.ID, live)
		}
	}
}

// E1 (b): the process's own values, and the passwords in its URLs, are
// reserved.
func TestProcessSecretValues(t *testing.T) {
	cfg := config.Config{SecretKey: "k3y", RoomDBURL: "postgres://u:room-pw@db/piperoom", AlertmanagerToken: "am"}
	got := map[string]bool{}
	for _, v := range processSecretValues(cfg) {
		got[v.Reveal()] = true
	}
	for _, want := range []string{"k3y", "room-pw", "am", "postgres://u:room-pw@db/piperoom"} {
		if !got[want] {
			t.Fatalf("%q not reserved: %v", want, len(got))
		}
	}
}

// E9: SOURCE_DB_PASSWORD is spliced into a URL without a password.
func TestSpliceDBPasswords(t *testing.T) {
	cfg := config.Config{SourceDBURL: "postgres://piperoom_ro@db:5432/orders?sslmode=verify-full", SourceDBPassword: "p@ss/w:rd",
		WarehouseDBURL: "postgres://wh:inline@db/wh"}
	if err := spliceDBPasswords(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SourceDBURL != "postgres://piperoom_ro:p%40ss%2Fw%3Ard@db:5432/orders?sslmode=verify-full" || cfg.WarehouseDBURL != "postgres://wh:inline@db/wh" {
		t.Fatalf("urls: %q %q", cfg.SourceDBURL, cfg.WarehouseDBURL)
	}
	for _, bad := range []config.Config{
		{SourceDBURL: "postgres://u:inline@db/x", SourceDBPassword: "pw"},
		{SourceDBURL: "postgres://db/x", SourceDBPassword: "pw"},
		{WarehouseDBURL: "user@account/db", WarehouseDBPassword: "pw"},
	} {
		if err := spliceDBPasswords(&bad); err == nil || strings.Contains(err.Error(), "pw") && !strings.Contains(err.Error(), "PASSWORD") {
			t.Fatalf("%+v: err = %v", bad, err)
		}
	}
}

// E3: the registry's change hook reaches the service once it is set.
func TestSecretChangeHookRebuilds(t *testing.T) {
	e := newRestoreEnv(t, integrations.Env{})
	e.addRow(t, "int_b", spec.CategorySecretManager, "fake-vault", map[string]any{"name": "vault-corp"}, nil, true)
	e.addRow(t, "int_a", spec.CategoryConnect, "fake-connect",
		map[string]any{"url": "http://connect:8083", "password": map[string]any{spec.RefKey: "secret://vault-corp/kafka/connect"}}, nil, true)
	e.setup.restore(context.Background())
	n := len(e.live.applied)
	hook := &secretChangeHook{}
	hook.onChange(spec.DefaultWorkspace, secrets.Ref{Provider: "vault-corp", Path: "kafka/connect"}) // not set yet: no-op
	hook.set(e.setup.svc)
	hook.onChange(spec.DefaultWorkspace, secrets.Ref{Provider: "vault-corp", Path: "kafka/connect"})
	if len(e.live.applied) != n+1 || e.live.applied[n].ID != "int_a" {
		t.Fatalf("applied %d → %d", n, len(e.live.applied))
	}
}
