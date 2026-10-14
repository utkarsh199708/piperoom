package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"piperoom/internal/integrations/spec"
	"piperoom/internal/store/memstore"
)

// The same suite runs against memstore here (no Docker) and against Postgres
// in integrations_integration_test.go, so both stores keep one behaviour.
func TestIntegrationsSuiteMemstore(t *testing.T) {
	runIntegrationsSuite(t, func(t *testing.T) spec.Store { return memstore.NewIntegrations() })
}

// suiteIDs gives each subtest its own workspace and ids, so the Postgres run
// can share one schema across parallel tests.
type suiteIDs struct{ prefix string }

func (s suiteIDs) ws(name string) string { return s.prefix + "_ws_" + name }
func (s suiteIDs) id(name string) string { return s.prefix + "_int_" + name }

var suiteSeq struct {
	mu sync.Mutex
	n  int
}

func newSuiteIDs() suiteIDs {
	suiteSeq.mu.Lock()
	defer suiteSeq.mu.Unlock()
	suiteSeq.n++
	return suiteIDs{prefix: fmt.Sprintf("it_%d_%d", time.Now().UnixNano(), suiteSeq.n)}
}

func baseRec(ws, id string, cat spec.Category, kind string) spec.Integration {
	return spec.Integration{
		ID: id, WorkspaceID: ws, Category: cat, Kind: kind, Scope: spec.ScopeAll,
		Config:  map[string]any{"host": "db.internal", "port": 5432},
		Enabled: true, UpdatedBy: "u_admin", Source: spec.SourceDB,
	}
}

func suiteCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustCreate(t *testing.T, s spec.Store, rec spec.Integration) spec.Integration {
	t.Helper()
	got, err := s.Create(suiteCtx(t), rec)
	if err != nil {
		t.Fatalf("create %s: %v", rec.ID, err)
	}
	return got
}

func runIntegrationsSuite(t *testing.T, newStore func(t *testing.T) spec.Store) {
	t.Run("create get round trip", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		rec := baseRec(ids.ws("a"), ids.id("pg"), spec.CategorySourceDB, "postgres-source")
		rec.Config["password"] = map[string]any{spec.RefKey: "secret://vault-corp/db#password"}
		rec.Sealed = []byte{1, 2, 3, 4}
		rec.KeyID = "deadbeef"
		rec.SecretsSet = []string{"password", "sslkey"}
		rec.Version = 42 // ignored: Create always starts at 1
		before := time.Now().Add(-time.Minute)
		got := mustCreate(t, s, rec)
		if got.Version != 1 {
			t.Fatalf("version = %d, want 1", got.Version)
		}
		if got.UpdatedAt.Before(before) || got.UpdatedAt.Location() != time.UTC {
			t.Fatalf("updated_at = %v, want now in UTC", got.UpdatedAt)
		}
		back, err := s.Get(ctx, rec.WorkspaceID, rec.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if back.Kind != "postgres-source" || back.Category != spec.CategorySourceDB || back.Scope != spec.ScopeAll ||
			back.KeyID != "deadbeef" || string(back.Sealed) != "\x01\x02\x03\x04" || !back.Enabled ||
			back.UpdatedBy != "u_admin" || back.Source != spec.SourceDB || back.Version != 1 {
			t.Fatalf("get = %+v", back)
		}
		if fmt.Sprint(back.SecretsSet) != "[password sslkey]" {
			t.Fatalf("secrets_set = %v", back.SecretsSet)
		}
		if back.Config["port"] != float64(5432) || back.Config["host"] != "db.internal" {
			t.Fatalf("config = %#v (numbers come back as float64, as from jsonb)", back.Config)
		}
		ref, _ := back.Config["password"].(map[string]any)
		if ref[spec.RefKey] != "secret://vault-corp/db#password" {
			t.Fatalf("config ref = %#v", back.Config["password"])
		}
		if back.LastTest != nil || !back.LastTestAt.IsZero() {
			t.Fatalf("never tested: last_test=%v at=%v", back.LastTest, back.LastTestAt)
		}
	})

	t.Run("defaults and empty values", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		rec := baseRec(ids.ws("a"), ids.id("k"), spec.CategoryKafka, "kafka-generic")
		rec.Scope, rec.Source, rec.Config, rec.SecretsSet, rec.Sealed = "", "", nil, nil, []byte{}
		got := mustCreate(t, s, rec)
		if got.Scope != spec.ScopeAll || got.Source != spec.SourceDB {
			t.Fatalf("scope=%q source=%q, want defaults", got.Scope, got.Source)
		}
		if got.Config == nil || len(got.Config) != 0 || got.SecretsSet == nil || len(got.SecretsSet) != 0 || got.Sealed != nil {
			t.Fatalf("empty values: config=%#v secrets_set=%#v sealed=%#v", got.Config, got.SecretsSet, got.Sealed)
		}
	})

	t.Run("bad input", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		ok := baseRec(ids.ws("a"), ids.id("x"), spec.CategoryKafka, "kafka-generic")
		cases := map[string]func(r *spec.Integration){
			"no workspace":   func(r *spec.Integration) { r.WorkspaceID = "" },
			"no id":          func(r *spec.Integration) { r.ID = "" },
			"bad category":   func(r *spec.Integration) { r.Category = "nope" },
			"no kind":        func(r *spec.Integration) { r.Kind = "" },
			"no updated_by":  func(r *spec.Integration) { r.UpdatedBy = "" },
			"nul in kind":    func(r *spec.Integration) { r.Kind = "a\x00b" },
			"bad utf8 field": func(r *spec.Integration) { r.SecretsSet = []string{"\xff"} },
			"unencodable":    func(r *spec.Integration) { r.Config = map[string]any{"c": make(chan int)} },
			"nul in checks": func(r *spec.Integration) {
				r.LastTest = []spec.Check{{ID: "reach", Title: "t", Status: spec.CheckFail, Detail: "a\x00b"}}
			},
			"secret manager name not a string": func(r *spec.Integration) {
				r.Category, r.Kind = spec.CategorySecretManager, "vault"
				r.Config = map[string]any{spec.ProviderNameField: 7}
			},
		}
		for name, mut := range cases {
			rec := ok
			rec.Config = map[string]any{}
			rec.LastTest = nil
			mut(&rec)
			if _, err := s.Create(ctx, rec); err == nil {
				t.Errorf("%s: create succeeded, want error", name)
			}
		}
		if rows, err := s.List(ctx, ids.ws("a")); err != nil || len(rows) != 0 {
			t.Fatalf("bad input left rows: %v %v", rows, err)
		}
	})

	t.Run("duplicates", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		ws := ids.ws("a")
		mustCreate(t, s, baseRec(ws, ids.id("k1"), spec.CategoryKafka, "kafka-generic"))
		// Same category, other kind: still one kafka per workspace.
		if _, err := s.Create(ctx, baseRec(ws, ids.id("k2"), spec.CategoryKafka, "kafka-confluent-cloud")); !errors.Is(err, spec.ErrDuplicate) {
			t.Fatalf("second kafka: %v, want ErrDuplicate", err)
		}
		// Same id.
		if _, err := s.Create(ctx, baseRec(ws, ids.id("k1"), spec.CategoryConnect, "connect")); !errors.Is(err, spec.ErrDuplicate) {
			t.Fatalf("same id: %v, want ErrDuplicate", err)
		}
		// Another scope is allowed (section 9).
		other := baseRec(ws, ids.id("k3"), spec.CategoryKafka, "kafka-generic")
		other.Scope = "orders"
		mustCreate(t, s, other)
		// Alerts: one row per source kind.
		mustCreate(t, s, baseRec(ws, ids.id("al1"), spec.CategoryAlerts, "alerts-grafana"))
		mustCreate(t, s, baseRec(ws, ids.id("al2"), spec.CategoryAlerts, "alerts-pagerduty"))
		if _, err := s.Create(ctx, baseRec(ws, ids.id("al3"), spec.CategoryAlerts, "alerts-grafana")); !errors.Is(err, spec.ErrDuplicate) {
			t.Fatalf("second grafana: %v, want ErrDuplicate", err)
		}
		// Secret managers: one row per provider name, any number of names.
		sm := func(id, kind, name string) spec.Integration {
			r := baseRec(ws, ids.id(id), spec.CategorySecretManager, kind)
			r.Config = map[string]any{spec.ProviderNameField: name}
			return r
		}
		mustCreate(t, s, sm("sm1", "secrets-vault", "vault-corp"))
		mustCreate(t, s, sm("sm2", "secrets-vault", "vault-eu"))
		mustCreate(t, s, sm("sm3", "secrets-aws", "aws-prod"))
		if _, err := s.Create(ctx, sm("sm4", "secrets-aws", "vault-corp")); !errors.Is(err, spec.ErrDuplicate) {
			t.Fatalf("second vault-corp: %v, want ErrDuplicate", err)
		}
		// The same unique key in another workspace is fine.
		mustCreate(t, s, baseRec(ids.ws("b"), ids.id("k1"), spec.CategoryKafka, "kafka-generic"))
		bsm := sm("sm1", "secrets-vault", "vault-corp")
		bsm.WorkspaceID = ids.ws("b")
		mustCreate(t, s, bsm)
	})

	t.Run("workspace isolation", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		a, b := ids.ws("a"), ids.ws("b")
		rec := mustCreate(t, s, baseRec(a, ids.id("c"), spec.CategoryConnect, "connect"))
		mustCreate(t, s, baseRec(b, ids.id("r"), spec.CategoryRegistry, "registry"))
		if _, err := s.Get(ctx, b, rec.ID); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("get from other workspace: %v, want ErrNotFound", err)
		}
		upd := rec
		upd.WorkspaceID = b
		if _, err := s.Update(ctx, upd, 1); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("update from other workspace: %v, want ErrNotFound", err)
		}
		if _, err := s.SetEnabled(ctx, b, rec.ID, false, "u"); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("set enabled from other workspace: %v", err)
		}
		if err := s.SaveLastTest(ctx, b, rec.ID, nil); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("save test from other workspace: %v", err)
		}
		if err := s.Delete(ctx, b, rec.ID); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("delete from other workspace: %v", err)
		}
		rows, err := s.List(ctx, a)
		if err != nil || len(rows) != 1 || rows[0].ID != rec.ID {
			t.Fatalf("list a = %v, %v", rows, err)
		}
		if err := s.AppendAudit(ctx, spec.AuditEntry{WorkspaceID: a, IntegrationID: rec.ID, Action: spec.AuditCreated, Actor: "u"}); err != nil {
			t.Fatal(err)
		}
		if got, err := s.ListAudit(ctx, b, rec.ID, 0); err != nil || len(got) != 0 {
			t.Fatalf("audit from other workspace = %v, %v", got, err)
		}
	})

	t.Run("list order and empty", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		ws := ids.ws("a")
		empty, err := s.List(ctx, ws)
		if err != nil || empty == nil || len(empty) != 0 {
			t.Fatalf("empty list = %#v, %v; want []", empty, err)
		}
		mustCreate(t, s, baseRec(ws, ids.id("w"), spec.CategoryWarehouse, "warehouse-postgres"))
		mustCreate(t, s, baseRec(ws, ids.id("b2"), spec.CategoryAlerts, "alerts-grafana"))
		mustCreate(t, s, baseRec(ws, ids.id("b1"), spec.CategoryAlerts, "alerts-generic"))
		mustCreate(t, s, baseRec(ws, ids.id("c"), spec.CategoryConnect, "connect"))
		rows, err := s.List(ctx, ws)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, r := range rows {
			got = append(got, r.ID)
		}
		want := fmt.Sprint([]string{ids.id("b1"), ids.id("b2"), ids.id("c"), ids.id("w")})
		if fmt.Sprint(got) != want {
			t.Fatalf("order = %v, want %v", got, want)
		}
	})

	t.Run("update", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		rec := baseRec(ids.ws("a"), ids.id("pg"), spec.CategorySourceDB, "postgres-source")
		rec.Sealed, rec.KeyID, rec.SecretsSet = []byte("old"), "aaaaaaaa", []string{"password"}
		created := mustCreate(t, s, rec)

		upd := created
		upd.Config = map[string]any{"host": "db2.internal"}
		upd.Sealed, upd.KeyID, upd.SecretsSet = []byte("new"), "bbbbbbbb", []string{}
		upd.Enabled, upd.UpdatedBy = false, "u_other"
		upd.Kind, upd.Category, upd.Scope = "ignored-kind", spec.CategoryKafka, "ignored" // not replaced
		got, err := s.Update(ctx, upd, 1)
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		if got.Version != 2 || got.Config["host"] != "db2.internal" || len(got.Config) != 1 ||
			string(got.Sealed) != "new" || got.KeyID != "bbbbbbbb" || len(got.SecretsSet) != 0 ||
			got.Enabled || got.UpdatedBy != "u_other" {
			t.Fatalf("update result = %+v", got)
		}
		if got.Kind != "postgres-source" || got.Category != spec.CategorySourceDB || got.Scope != spec.ScopeAll {
			t.Fatalf("update changed identity: %+v", got)
		}
		if got.UpdatedAt.Before(created.UpdatedAt) {
			t.Fatalf("updated_at went back: %v < %v", got.UpdatedAt, created.UpdatedAt)
		}
		// Stale If-Match.
		if _, err := s.Update(ctx, upd, 1); !errors.Is(err, spec.ErrVersionConflict) {
			t.Fatalf("stale update: %v, want ErrVersionConflict", err)
		}
		// Clearing the sealed blob.
		upd.Sealed = nil
		got, err = s.Update(ctx, upd, 2)
		if err != nil || got.Sealed != nil || got.Version != 3 {
			t.Fatalf("clear sealed: %+v, %v", got, err)
		}
		// Missing row.
		missing := upd
		missing.ID = ids.id("missing")
		if _, err := s.Update(ctx, missing, 1); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("update missing: %v, want ErrNotFound", err)
		}
		// Bad input.
		bad := upd
		bad.UpdatedBy = ""
		if _, err := s.Update(ctx, bad, 3); err == nil || errors.Is(err, spec.ErrVersionConflict) {
			t.Fatalf("bad update: %v, want a validation error", err)
		}
	})

	t.Run("update renaming a secret manager onto another name", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		ws := ids.ws("a")
		one := baseRec(ws, ids.id("sm1"), spec.CategorySecretManager, "secrets-vault")
		one.Config = map[string]any{spec.ProviderNameField: "vault-corp"}
		two := baseRec(ws, ids.id("sm2"), spec.CategorySecretManager, "secrets-vault")
		two.Config = map[string]any{spec.ProviderNameField: "vault-eu"}
		mustCreate(t, s, one)
		created := mustCreate(t, s, two)
		created.Config = map[string]any{spec.ProviderNameField: "vault-corp"}
		if _, err := s.Update(ctx, created, 1); !errors.Is(err, spec.ErrDuplicate) {
			t.Fatalf("rename onto taken name: %v, want ErrDuplicate", err)
		}
		if back, _ := s.Get(ctx, ws, created.ID); back.Version != 1 || back.Config[spec.ProviderNameField] != "vault-eu" {
			t.Fatalf("failed update changed the row: %+v", back)
		}
		created.Config = map[string]any{spec.ProviderNameField: "vault-apac"}
		if _, err := s.Update(ctx, created, 1); err != nil {
			t.Fatalf("rename onto free name: %v", err)
		}
	})

	t.Run("concurrent updates with the same If-Match", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		rec := mustCreate(t, s, baseRec(ids.ws("a"), ids.id("c"), spec.CategoryConnect, "connect"))
		const n = 8
		var wg sync.WaitGroup
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				upd := rec
				upd.Config = map[string]any{"url": fmt.Sprintf("http://connect-%d:8083", i)}
				_, err := s.Update(context.Background(), upd, 1)
				errs <- err
			}(i)
		}
		wg.Wait()
		close(errs)
		won, conflicts := 0, 0
		for err := range errs {
			switch {
			case err == nil:
				won++
			case errors.Is(err, spec.ErrVersionConflict):
				conflicts++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}
		if won != 1 || conflicts != n-1 {
			t.Fatalf("won=%d conflicts=%d, want 1 and %d", won, conflicts, n-1)
		}
	})

	t.Run("set enabled", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		rec := mustCreate(t, s, baseRec(ids.ws("a"), ids.id("m"), spec.CategoryModel, "anthropic"))
		got, err := s.SetEnabled(ctx, rec.WorkspaceID, rec.ID, false, "u_ops")
		if err != nil || got.Enabled || got.Version != 2 || got.UpdatedBy != "u_ops" {
			t.Fatalf("disable = %+v, %v", got, err)
		}
		got, err = s.SetEnabled(ctx, rec.WorkspaceID, rec.ID, true, "u_ops")
		if err != nil || !got.Enabled || got.Version != 3 {
			t.Fatalf("enable = %+v, %v", got, err)
		}
		if _, err := s.Update(ctx, rec, 1); !errors.Is(err, spec.ErrVersionConflict) {
			t.Fatalf("update after toggle with old version: %v, want ErrVersionConflict", err)
		}
		if _, err := s.SetEnabled(ctx, rec.WorkspaceID, ids.id("missing"), true, "u"); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("missing: %v", err)
		}
		if _, err := s.SetEnabled(ctx, rec.WorkspaceID, rec.ID, true, ""); err == nil {
			t.Fatal("empty actor: want error")
		}
	})

	t.Run("save last test keeps version", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		rec := mustCreate(t, s, baseRec(ids.ws("a"), ids.id("r"), spec.CategoryRegistry, "registry"))
		checks := []spec.Check{
			{ID: "reach", Title: "Registry answers", Status: spec.CheckPass},
			{ID: "auth", Title: "Login works", Status: spec.CheckFail, Detail: "401", Fix: "check the key"},
		}
		before := time.Now().Add(-time.Minute)
		if err := s.SaveLastTest(ctx, rec.WorkspaceID, rec.ID, checks); err != nil {
			t.Fatalf("save: %v", err)
		}
		got, err := s.Get(ctx, rec.WorkspaceID, rec.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Version != 1 || !got.UpdatedAt.Equal(rec.UpdatedAt) {
			t.Fatalf("save test bumped version or updated_at: %+v", got)
		}
		if len(got.LastTest) != 2 || got.LastTest[1] != checks[1] || got.LastTestAt.Before(before) {
			t.Fatalf("last test = %+v at %v", got.LastTest, got.LastTestAt)
		}
		if _, err := s.Update(ctx, got, 1); err != nil {
			t.Fatalf("open edit broken by a background test: %v", err)
		}
		// A test with no checks is still a test: [] not nil.
		if err := s.SaveLastTest(ctx, rec.WorkspaceID, rec.ID, nil); err != nil {
			t.Fatal(err)
		}
		got, _ = s.Get(ctx, rec.WorkspaceID, rec.ID)
		if got.LastTest == nil || len(got.LastTest) != 0 {
			t.Fatalf("empty test = %#v, want []", got.LastTest)
		}
		if err := s.SaveLastTest(ctx, rec.WorkspaceID, ids.id("missing"), checks); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("missing: %v", err)
		}
		// Checks Postgres jsonb would refuse are refused by every store, and
		// the stored test result stays as it was.
		bad := []spec.Check{{ID: "reach", Title: "t", Status: spec.CheckFail, Detail: "a\x00b"}}
		if err := s.SaveLastTest(ctx, rec.WorkspaceID, rec.ID, bad); err == nil {
			t.Fatal("bad checks: saved, want error")
		}
		got, _ = s.Get(ctx, rec.WorkspaceID, rec.ID)
		if got.LastTest == nil || len(got.LastTest) != 0 {
			t.Fatalf("bad checks changed the stored test: %#v", got.LastTest)
		}
	})

	t.Run("delete keeps audit", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		rec := mustCreate(t, s, baseRec(ids.ws("a"), ids.id("w"), spec.CategoryWarehouse, "warehouse-postgres"))
		if err := s.AppendAudit(ctx, spec.AuditEntry{WorkspaceID: rec.WorkspaceID, IntegrationID: rec.ID, Action: spec.AuditCreated, Actor: "u"}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, rec.WorkspaceID, rec.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, err := s.Get(ctx, rec.WorkspaceID, rec.ID); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("get after delete: %v", err)
		}
		if err := s.Delete(ctx, rec.WorkspaceID, rec.ID); !errors.Is(err, spec.ErrNotFound) {
			t.Fatalf("second delete: %v, want ErrNotFound", err)
		}
		if got, err := s.ListAudit(ctx, rec.WorkspaceID, rec.ID, 0); err != nil || len(got) != 1 {
			t.Fatalf("audit after delete = %v, %v", got, err)
		}
		// The unique key is free again.
		mustCreate(t, s, baseRec(rec.WorkspaceID, ids.id("w2"), spec.CategoryWarehouse, "warehouse-snowflake"))
	})

	t.Run("audit", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		ws, id := ids.ws("a"), ids.id("k")
		actions := []spec.AuditAction{spec.AuditCreated, spec.AuditTested, spec.AuditUpdated, spec.AuditDisabled,
			spec.AuditEnabled, spec.AuditSecretRotated, spec.AuditDeleted}
		before := time.Now().Add(-time.Minute)
		for i, a := range actions {
			e := spec.AuditEntry{WorkspaceID: ws, IntegrationID: id, Action: a, Actor: fmt.Sprintf("u%d", i),
				Seq: 999, At: time.Unix(1, 0)} // Seq and At are the store's
			if a == spec.AuditUpdated {
				e.Changed = []string{"bootstrap", "sasl_password"}
			}
			if err := s.AppendAudit(ctx, e); err != nil {
				t.Fatalf("append %s: %v", a, err)
			}
		}
		if err := s.AppendAudit(ctx, spec.AuditEntry{WorkspaceID: ws, IntegrationID: ids.id("other"), Action: spec.AuditCreated, Actor: "u"}); err != nil {
			t.Fatal(err)
		}
		all, err := s.ListAudit(ctx, ws, id, 0)
		if err != nil || len(all) != len(actions) {
			t.Fatalf("list = %d rows, %v", len(all), err)
		}
		for i, e := range all {
			want := actions[len(actions)-1-i]
			if e.Action != want || e.WorkspaceID != ws || e.IntegrationID != id || e.At.Before(before) || e.Changed == nil {
				t.Fatalf("row %d = %+v, want action %s", i, e, want)
			}
			if i > 0 && e.Seq >= all[i-1].Seq {
				t.Fatalf("not newest first: %d then %d", all[i-1].Seq, e.Seq)
			}
		}
		if fmt.Sprint(all[len(actions)-3].Changed) != "[bootstrap sasl_password]" {
			t.Fatalf("changed = %v", all[len(actions)-3].Changed)
		}
		two, err := s.ListAudit(ctx, ws, id, 2)
		if err != nil || len(two) != 2 || two[0].Action != spec.AuditDeleted {
			t.Fatalf("limit 2 = %+v, %v", two, err)
		}
		none, err := s.ListAudit(ctx, ws, ids.id("nobody"), 10)
		if err != nil || none == nil || len(none) != 0 {
			t.Fatalf("empty audit = %#v, %v; want []", none, err)
		}
		bad := []spec.AuditEntry{
			{IntegrationID: id, Action: spec.AuditCreated, Actor: "u"},
			{WorkspaceID: ws, Action: spec.AuditCreated, Actor: "u"},
			{WorkspaceID: ws, IntegrationID: id, Actor: "u"},
			{WorkspaceID: ws, IntegrationID: id, Action: "exploded", Actor: "u"},
			{WorkspaceID: ws, IntegrationID: id, Action: spec.AuditCreated},
			{WorkspaceID: ws, IntegrationID: id, Action: spec.AuditCreated, Actor: "u", Changed: []string{"a\x00"}},
		}
		for i, e := range bad {
			if err := s.AppendAudit(ctx, e); err == nil {
				t.Errorf("bad audit %d accepted", i)
			}
		}
	})

	t.Run("default limit is 100", func(t *testing.T) {
		s, ids := newStore(t), newSuiteIDs()
		ctx := suiteCtx(t)
		ws, id := ids.ws("a"), ids.id("k")
		for i := 0; i < 105; i++ {
			if err := s.AppendAudit(ctx, spec.AuditEntry{WorkspaceID: ws, IntegrationID: id, Action: spec.AuditTested, Actor: "u"}); err != nil {
				t.Fatal(err)
			}
		}
		for _, limit := range []int{0, -1} {
			got, err := s.ListAudit(ctx, ws, id, limit)
			if err != nil || len(got) != 100 {
				t.Fatalf("limit %d: %d rows, %v; want 100", limit, len(got), err)
			}
		}
		if got, _ := s.ListAudit(ctx, ws, id, 500); len(got) != 105 {
			t.Fatalf("limit 500: %d rows, want 105", len(got))
		}
	})
}
