package memstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/integrations/spec"
	"piperoom/internal/store/memstore"
)

// The behaviour shared with Postgres is covered by the suite in
// internal/store/integrations_suite_test.go; these tests cover what only the
// in-memory store can get wrong: aliasing and its clock.

func memRec() spec.Integration {
	return spec.Integration{
		ID: "int_1", WorkspaceID: spec.DefaultWorkspace, Category: spec.CategoryKafka, Kind: "kafka-generic",
		Config: map[string]any{"bootstrap": "k:9092", "nested": map[string]any{"a": "b"}},
		Sealed: []byte{9, 9}, SecretsSet: []string{"sasl_password"},
		LastTest:   []spec.Check{{ID: "reach", Status: spec.CheckPass}},
		LastTestAt: time.Date(2026, 1, 2, 3, 4, 5, 6789, time.FixedZone("x", 3600)),
		Enabled:    true, UpdatedBy: "u",
	}
}

func TestIntegrationsCopiesInAndOut(t *testing.T) {
	ctx := context.Background()
	m := memstore.NewIntegrations()
	in := memRec()
	out, err := m.Create(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	// Mutating what was passed in, or what came out, must not reach the store.
	in.Config["bootstrap"] = "evil:1"
	in.Config["nested"].(map[string]any)["a"] = "evil"
	in.Sealed[0], in.SecretsSet[0], in.LastTest[0].ID = 0, "x", "x"
	out.Config["bootstrap"] = "evil:2"
	out.Sealed[1], out.SecretsSet[0], out.LastTest[0].ID = 0, "y", "y"

	got, err := m.Get(ctx, spec.DefaultWorkspace, "int_1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Config["bootstrap"] != "k:9092" || got.Config["nested"].(map[string]any)["a"] != "b" ||
		string(got.Sealed) != "\x09\x09" || got.SecretsSet[0] != "sasl_password" || got.LastTest[0].ID != "reach" {
		t.Fatalf("store aliased caller memory: %+v", got)
	}
	rows, _ := m.List(ctx, spec.DefaultWorkspace)
	rows[0].Config["bootstrap"] = "evil:3"
	if again, _ := m.Get(ctx, spec.DefaultWorkspace, "int_1"); again.Config["bootstrap"] != "k:9092" {
		t.Fatal("List result aliased the stored config")
	}
	// LastTestAt from the caller is kept, in UTC at microsecond precision like timestamptz.
	want := time.Date(2026, 1, 2, 2, 4, 5, 6000, time.UTC)
	if !got.LastTestAt.Equal(want) || got.LastTestAt.Location() != time.UTC {
		t.Fatalf("last_test_at = %v, want %v", got.LastTestAt, want)
	}
}

func TestIntegrationsClock(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 14, 9, 0, 0, 123456789, time.UTC)
	m := memstore.NewIntegrationsWithClock(func() time.Time { return now })
	rec, err := m.Create(ctx, memRec())
	if err != nil {
		t.Fatal(err)
	}
	if want := now.Truncate(time.Microsecond); !rec.UpdatedAt.Equal(want) {
		t.Fatalf("created at %v, want %v", rec.UpdatedAt, want)
	}
	now = now.Add(time.Hour)
	if err := m.SaveLastTest(ctx, rec.WorkspaceID, rec.ID, nil); err != nil {
		t.Fatal(err)
	}
	got, _ := m.Get(ctx, rec.WorkspaceID, rec.ID)
	if !got.LastTestAt.Equal(now.Truncate(time.Microsecond)) || !got.UpdatedAt.Equal(rec.UpdatedAt) {
		t.Fatalf("after test: last_test_at=%v updated_at=%v", got.LastTestAt, got.UpdatedAt)
	}
	now = now.Add(time.Hour)
	if got, err = m.SetEnabled(ctx, rec.WorkspaceID, rec.ID, false, "u2"); err != nil || !got.UpdatedAt.Equal(now.Truncate(time.Microsecond)) {
		t.Fatalf("set enabled: %+v %v", got, err)
	}
	if err := m.AppendAudit(ctx, spec.AuditEntry{WorkspaceID: rec.WorkspaceID, IntegrationID: rec.ID, Action: spec.AuditDisabled, Actor: "u2"}); err != nil {
		t.Fatal(err)
	}
	audit, _ := m.ListAudit(ctx, rec.WorkspaceID, rec.ID, 0)
	if len(audit) != 1 || audit[0].Seq != 1 || !audit[0].At.Equal(now.Truncate(time.Microsecond)) {
		t.Fatalf("audit = %+v", audit)
	}
}

func TestIntegrationsSecretManagerWithoutNameClashes(t *testing.T) {
	// Postgres indexes COALESCE(config->>'name', ''), so two unnamed managers
	// clash there; memstore must agree.
	ctx := context.Background()
	m := memstore.NewIntegrations()
	a := memRec()
	a.Category, a.Kind, a.Config = spec.CategorySecretManager, "secrets-vault", nil
	if _, err := m.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	b := a
	b.ID = "int_2"
	if _, err := m.Create(ctx, b); !errors.Is(err, spec.ErrDuplicate) {
		t.Fatalf("second unnamed manager: %v, want ErrDuplicate", err)
	}
}
