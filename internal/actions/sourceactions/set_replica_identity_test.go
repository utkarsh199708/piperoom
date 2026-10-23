package sourceactions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func riParams(table, mode, index string) json.RawMessage {
	p := map[string]string{"connector": testConnector, "table": table, "mode": mode}
	if index != "" {
		p["index"] = index
	}
	b, _ := json.Marshal(p)
	return b
}

var riGood = riParams("public.orders", "full", "")

func TestSetReplicaIdentityMeta(t *testing.T) {
	_, _, _, a := newReplicaFixture(nil, ordersState())
	if a.Name() != "pg_set_replica_identity" || a.Layer() != "postgres" || a.Describe() == "" {
		t.Fatalf("meta: %s %s", a.Name(), a.Layer())
	}
	schema := a.ParamsSchema()
	if schema["type"] != "object" || len(schema["required"].([]string)) != 3 {
		t.Fatalf("schema = %v", schema)
	}
}

func TestSetReplicaIdentityValidate(t *testing.T) {
	c, s, w, a := newReplicaFixture(nil, ordersState())
	if err := a.Validate(scopedCtx(), riGood); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 || s.pubCalls != 1 || !s.deadline {
		t.Fatalf("reads: connect %d, publication %d, deadline %v", c.calls, s.pubCalls, s.deadline)
	}
	if s.pub != "dbz_publication" || s.table != "public.orders" {
		t.Fatalf("read %q %q", s.pub, s.table)
	}
	if len(w.calls) != 0 {
		t.Fatalf("Validate wrote: %v", w.calls)
	}
}

func TestSetReplicaIdentityRefusesWithoutWriter(t *testing.T) {
	_, _, _, a := newReplicaFixture(nil, ordersState())
	a.d.Writer = nil
	err := a.Validate(scopedCtx(), riGood)
	if err == nil || !strings.Contains(err.Error(), "no write credentials") || !strings.Contains(err.Error(), "SOURCE_DB_WRITE_URL") {
		t.Fatalf("Validate = %v", err)
	}
	if _, err := a.Execute(scopedCtx(), riGood); err == nil || !strings.Contains(err.Error(), "SOURCE_DB_WRITE_URL") {
		t.Fatalf("Execute = %v", err)
	}
}

func TestSetReplicaIdentityRefusesOutOfScopeBeforeIO(t *testing.T) {
	other := owners.Pipeline{ID: "other", Kind: "cdc", Connectors: []string{"pg-other"}, SourceTables: []string{"public.other"}}
	ctx := tools.WithPipeline(context.Background(), other)
	for _, raw := range []json.RawMessage{
		riParams("public.orders", "full", ""), // connector is not in "other"
		riParams("public.other", "full", ""),  // table is not in "other" either
	} {
		c, s, _, a := newReplicaFixture(nil, ordersState())
		if err := a.Validate(ctx, raw); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("Validate(%s) = %v", raw, err)
		}
		if c.calls != 0 || s.pubCalls != 0 {
			t.Fatalf("out-of-scope Validate did I/O: connect %d, source %d", c.calls, s.pubCalls)
		}
		if err := a.CheckScope(other, raw); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("CheckScope(%s) = %v", raw, err)
		}
	}
	// A batch pipeline has no Postgres layer.
	batch := owners.Pipeline{ID: "warehouse", Kind: "batch"}
	_, _, _, a := newReplicaFixture(nil, ordersState())
	if err := a.CheckScope(batch, riGood); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("batch pipeline = %v", err)
	}
}

func TestSetReplicaIdentityValidateBadParamsWithoutIO(t *testing.T) {
	cases := map[string]json.RawMessage{
		"unknown field":   json.RawMessage(`{"connector":"pg-orders","table":"public.orders","mode":"full","nope":1}`),
		"no connector":    json.RawMessage(`{"table":"public.orders","mode":"full"}`),
		"bad table":       riParams("orders", "full", ""),
		"bad mode":        riParams("public.orders", "FULL", ""),
		"index on full":   riParams("public.orders", "full", "orders_pkey"),
		"index mode bare": riParams("public.orders", "index", ""),
		"bad index":       riParams("public.orders", "index", `x"; drop`),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			c, s, _, a := newReplicaFixture(nil, ordersState())
			if err := a.Validate(scopedCtx(), raw); err == nil {
				t.Fatal("no error")
			}
			if c.calls != 0 || s.pubCalls != 0 {
				t.Fatalf("bad params did I/O: connect %d, source %d", c.calls, s.pubCalls)
			}
		})
	}
}

func TestSetReplicaIdentityValidateRefusesMissingTable(t *testing.T) {
	st := ordersState()
	st.TableExists, st.RelKind = false, ""
	_, _, _, a := newReplicaFixture(nil, st)
	if err := a.Validate(scopedCtx(), riGood); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table = %v", err)
	}
}

func TestSetReplicaIdentityValidateRefusesDefaultWithoutKey(t *testing.T) {
	st := ordersState()
	st.HasPrimaryKey = false
	_, _, _, a := newReplicaFixture(nil, st)
	err := a.Validate(scopedCtx(), riParams("public.orders", "default", ""))
	if err == nil || !strings.Contains(err.Error(), "no primary key") {
		t.Fatalf("default without key = %v", err)
	}
}

func TestSetReplicaIdentityValidateRefusesView(t *testing.T) {
	st := ordersState()
	st.RelKind = "v"
	_, _, _, a := newReplicaFixture(nil, st)
	if err := a.Validate(scopedCtx(), riGood); err == nil || !strings.Contains(err.Error(), "not an ordinary") {
		t.Fatalf("view = %v", err)
	}
}

func TestSetReplicaIdentityPreviewFull(t *testing.T) {
	_, _, w, a := newReplicaFixture(nil, ordersState())
	res, err := a.Preview(scopedCtx(), riGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Current, "9.0 MiB on disk") || !strings.Contains(res.Current, "REPLICA IDENTITY DEFAULT") {
		t.Fatalf("current = %q", res.Current)
	}
	for _, want := range []string{"one brief ACCESS EXCLUSIVE lock", "REPLICA IDENTITY FULL", "8.0 MiB of WAL per full pass", "1000 rows"} {
		if !strings.Contains(res.Expected, want) {
			t.Fatalf("expected %q lacks %q", res.Expected, want)
		}
	}
	if !res.Reversible || res.Rollback != `ALTER TABLE "public"."orders" REPLICA IDENTITY DEFAULT` {
		t.Fatalf("rollback = %q reversible %v", res.Rollback, res.Reversible)
	}
	if len(res.Downstream) == 0 || !strings.Contains(strings.Join(res.Downstream, " "), "no re-snapshot") {
		t.Fatalf("downstream = %v", res.Downstream)
	}
	if len(w.calls) != 0 {
		t.Fatalf("Preview wrote: %v", w.calls)
	}
}

func TestSetReplicaIdentityPreviewAlreadyFull(t *testing.T) {
	st := ordersState()
	st.ReplicaIdentity = "f"
	_, _, _, a := newReplicaFixture(nil, st)
	res, err := a.Preview(scopedCtx(), riGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Expected, "no change") || !strings.Contains(res.Rollback, "none needed") {
		t.Fatalf("already full: %+v", res)
	}
}

func TestSetReplicaIdentityPreviewSizeUnavailable(t *testing.T) {
	_, _, _, a := newReplicaFixture(nil, ordersState())
	a.d.Source = noSizeSource{}
	res, err := a.Preview(scopedCtx(), riGood)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Expected, "cannot be estimated") || !strings.Contains(res.Expected, "no TableSize") {
		t.Fatalf("expected = %q", res.Expected)
	}
}

func TestSetReplicaIdentityPreviewRefusesUnscoped(t *testing.T) {
	c, s, _, a := newReplicaFixture(nil, ordersState())
	if _, err := a.Preview(context.Background(), riGood); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Preview = %v", err)
	}
	if c.calls != 0 || s.pubCalls != 0 {
		t.Fatalf("unscoped Preview did I/O")
	}
}

func TestSetReplicaIdentityExecute(t *testing.T) {
	_, _, w, a := newReplicaFixture(nil, ordersState())
	out, err := a.Execute(scopedCtx(), riParams("public.orders", "index", "orders_pkey"))
	if err != nil {
		t.Fatal(err)
	}
	if len(w.calls) != 1 || w.calls[0] != "SetReplicaIdentity" || w.table != "public.orders" || w.mode != "index" || w.index != "orders_pkey" {
		t.Fatalf("writer = %v %q %q %q", w.calls, w.table, w.mode, w.index)
	}
	for _, want := range []string{"REPLICA IDENTITY USING INDEX orders_pkey", "Rollback: ALTER TABLE \"public\".\"orders\" REPLICA IDENTITY DEFAULT"} {
		if !strings.Contains(out, want) {
			t.Fatalf("result %q lacks %q", out, want)
		}
	}
}

func TestSetReplicaIdentityExecuteOutOfScope(t *testing.T) {
	other := owners.Pipeline{ID: "other", Kind: "cdc", Connectors: []string{"pg-other"}, SourceTables: []string{"public.other"}}
	ctx := tools.WithPipeline(context.Background(), other)
	_, _, w, a := newReplicaFixture(nil, ordersState())
	if _, err := a.Execute(ctx, riGood); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Execute = %v", err)
	}
	if len(w.calls) != 0 {
		t.Fatalf("out-of-scope Execute wrote: %v", w.calls)
	}
}

func TestSetReplicaIdentityWriterError(t *testing.T) {
	_, _, w, a := newReplicaFixture(nil, ordersState())
	w.err = errors.New("boom")
	if _, err := a.Execute(scopedCtx(), riGood); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Execute = %v", err)
	}
}

func TestSetReplicaIdentityRequiresPipelineScopeInCtx(t *testing.T) {
	_, _, w, a := newReplicaFixture(nil, postgres.PublicationAddState{TableExists: true})
	if _, err := a.Execute(context.Background(), riGood); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Execute = %v", err)
	}
	if len(w.calls) != 0 {
		t.Fatal("unscoped Execute wrote")
	}
}
