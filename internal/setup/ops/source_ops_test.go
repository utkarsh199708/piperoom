package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/setup/errcode"
	"piperoom/internal/setup/protocol"
)

// fakeSource answers PublicationTables and records what it was asked.
type fakeSource struct {
	calls  int
	gotPub string
	gotRol string
	gotTbl []string
	out    []postgres.PublicationTable
	err    error
}

func (f *fakeSource) PublicationTables(_ context.Context, publication, role string, tables []string) ([]postgres.PublicationTable, error) {
	f.calls++
	f.gotPub, f.gotRol, f.gotTbl = publication, role, tables
	return f.out, f.err
}

var pgTarget = events.SetupTarget{Kind: "postgres", Address: "db.acme:5432"}

func sourceRegistry(t *testing.T, src SourceReader, targets ...events.SetupTarget) *Registry {
	t.Helper()
	cfg := Config{ProjectDir: t.TempDir(), Targets: targets}
	if src != nil {
		cfg.Source = src
	}
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func execPub(t *testing.T, r *Registry, params string) (protocol.OpResult, error) {
	t.Helper()
	return r.Execute(context.Background(), protocol.OpRequest{
		ID: "op_1", Op: OpSourcePublicationTables, Tier: protocol.TierT0, Params: json.RawMessage(params),
	})
}

func TestPublicationTablesParams(t *testing.T) {
	long := strings.Repeat("a", maxIdentBytes+1)
	many := make([]string, maxPublicationTables+1)
	for i := range many {
		many[i] = "public.t"
	}
	manyJSON, _ := json.Marshal(map[string]any{"publication": "dbz", "tables": many})
	cases := []struct {
		name   string
		params string
		ok     bool
	}{
		{"full", `{"publication":"dbz_publication","role":"debezium","tables":["public.orders"]}`, true},
		{"no role, no tables", `{"publication":"dbz"}`, true},
		{"missing publication", `{"role":"debezium"}`, false},
		{"empty publication", `{"publication":""}`, false},
		{"publication with a quote", `{"publication":"dbz\"; DROP TABLE x; --"}`, false},
		{"publication with a dash", `{"publication":"dbz-pub"}`, false},
		{"publication starts with a digit", `{"publication":"1dbz"}`, false},
		{"publication too long", `{"publication":"` + long + `"}`, false},
		{"bad role", `{"publication":"dbz","role":"de bezium"}`, false},
		{"table without schema", `{"publication":"dbz","tables":["orders"]}`, false},
		{"table with three parts", `{"publication":"dbz","tables":["a.b.c"]}`, false},
		{"table with a bad schema", `{"publication":"dbz","tables":["pub lic.orders"]}`, false},
		{"too many tables", string(manyJSON), false},
		{"unknown field", `{"publication":"dbz","sql":"select 1"}`, false},
		{"case-colliding keys", `{"publication":"dbz","Publication":"other"}`, false},
		{"not an object", `[]`, false},
	}
	op := sourcePublicationTablesOp{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := op.Validate(json.RawMessage(c.params), Env{})
			if c.ok && err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			if !c.ok && !errors.Is(err, ErrParams) {
				t.Fatalf("Validate = %v, want ErrParams", err)
			}
		})
	}
}

// A refused param never reaches the source.
func TestPublicationTablesRefusesBeforeRead(t *testing.T) {
	src := &fakeSource{}
	r := sourceRegistry(t, src, pgTarget)
	res, err := execPub(t, r, `{"publication":"x'; DROP PUBLICATION y; --"}`)
	if !errors.Is(err, ErrParams) || res.OK {
		t.Fatalf("Execute = %+v, %v; want a refused result and ErrParams", res, err)
	}
	if src.calls != 0 {
		t.Fatalf("the source was read %d times for refused params", src.calls)
	}
	if got := r.CheckTarget(protocol.OpRequest{Op: OpSourcePublicationTables, Params: json.RawMessage(`{"publication":"bad name"}`)}); got != protocol.RefuseSchema {
		t.Fatalf("CheckTarget(bad params) = %q, want %q", got, protocol.RefuseSchema)
	}
}

func TestPublicationTablesExecute(t *testing.T) {
	src := &fakeSource{out: []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
		{Schema: "public", Table: "customers", InPublication: false, Selectable: false},
	}}
	r := sourceRegistry(t, src, pgTarget)
	res, err := execPub(t, r, `{"publication":"dbz_publication","role":"debezium","tables":["public.orders","public.customers"]}`)
	if err != nil || !res.OK {
		t.Fatalf("Execute = %+v, %v", res, err)
	}
	if src.gotPub != "dbz_publication" || src.gotRol != "debezium" || len(src.gotTbl) != 2 || src.gotTbl[1] != "public.customers" {
		t.Fatalf("source asked %q %q %v", src.gotPub, src.gotRol, src.gotTbl)
	}
	var data struct {
		Tables []postgres.PublicationTable `json:"tables"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatalf("Data %s: %v", res.Data, err)
	}
	if len(data.Tables) != 2 || data.Tables[0] != src.out[0] || data.Tables[1] != src.out[1] {
		t.Fatalf("Data tables = %+v", data.Tables)
	}
	if !strings.Contains(res.Summary, "2 tables listed, 1 in the publication") {
		t.Fatalf("Summary = %q", res.Summary)
	}

	// An empty answer is an empty list, never null: the client reads a
	// missing field as a bad result.
	src.out = nil
	res, err = execPub(t, r, `{"publication":"dbz_publication"}`)
	if err != nil || string(res.Data) != `{"tables":[]}` {
		t.Fatalf("empty Execute = %s, %v", res.Data, err)
	}
}

func TestPublicationTablesErrors(t *testing.T) {
	t.Run("missing publication carries not_found", func(t *testing.T) {
		src := &fakeSource{err: fmt.Errorf("postgres: publication %q: %w", "dbz", clients.ErrNotFound)}
		res, err := execPub(t, sourceRegistry(t, src, pgTarget), `{"publication":"dbz"}`)
		if !errors.Is(err, clients.ErrNotFound) || res.OK {
			t.Fatalf("Execute = %+v, %v", res, err)
		}
		if string(res.Data) != `{"error_code":"`+errcode.NotFound+`"}` {
			t.Fatalf("Data = %s", res.Data)
		}
	})
	t.Run("read error", func(t *testing.T) {
		src := &fakeSource{err: errors.New("postgres: list publication tables: connection refused")}
		res, err := execPub(t, sourceRegistry(t, src, pgTarget), `{"publication":"dbz"}`)
		if err == nil || res.OK || !strings.Contains(res.Summary, "connection refused") {
			t.Fatalf("Execute = %+v, %v", res, err)
		}
	})
	t.Run("the error text is redacted", func(t *testing.T) {
		src := &fakeSource{err: errors.New("dial postgres://debezium:hunter2-secret@db.acme:5432/orders failed")}
		res, _ := execPub(t, sourceRegistry(t, src, pgTarget), `{"publication":"dbz"}`)
		if strings.Contains(res.Summary, "hunter2-secret") {
			t.Fatalf("Summary leaks the password: %q", res.Summary)
		}
	})
	t.Run("no source connection", func(t *testing.T) {
		res, err := execPub(t, sourceRegistry(t, nil, pgTarget), `{"publication":"dbz"}`)
		if !errors.Is(err, ErrNoSource) || res.OK {
			t.Fatalf("Execute = %+v, %v", res, err)
		}
		if string(res.Data) != `{"error_code":"`+errcode.NotSupported+`"}` {
			t.Fatalf("Data = %s", res.Data)
		}
	})
	t.Run("no postgres target", func(t *testing.T) {
		src := &fakeSource{}
		r := sourceRegistry(t, src, events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"})
		res, err := execPub(t, r, `{"publication":"dbz"}`)
		if !errors.Is(err, ErrTarget) || res.OK || src.calls != 0 {
			t.Fatalf("Execute = %+v, %v (source calls %d)", res, err, src.calls)
		}
		got := r.CheckTarget(protocol.OpRequest{Op: OpSourcePublicationTables, Params: json.RawMessage(`{"publication":"dbz"}`)})
		if got != protocol.RefuseTarget {
			t.Fatalf("CheckTarget = %q, want %q", got, protocol.RefuseTarget)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		src := &blockingSource{}
		r := sourceRegistry(t, src, pgTarget)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		res, err := r.Execute(ctx, protocol.OpRequest{ID: "op_1", Op: OpSourcePublicationTables, Tier: protocol.TierT0, Params: json.RawMessage(`{"publication":"dbz"}`)})
		if !errors.Is(err, context.Canceled) || res.OK {
			t.Fatalf("Execute = %+v, %v", res, err)
		}
	})
}

// blockingSource waits for ctx, as a hung database would.
type blockingSource struct{}

func (blockingSource) PublicationTables(ctx context.Context, _, _ string, _ []string) ([]postgres.PublicationTable, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// The op is a read: the guard never forbids it, and it is a T0 op of the
// postgres layer reaching the postgres target.
func TestPublicationTablesCatalogue(t *testing.T) {
	r := sourceRegistry(t, nil, pgTarget)
	op, ok := r.Get(OpSourcePublicationTables)
	if !ok {
		t.Fatalf("%s is not in the catalogue %v", OpSourcePublicationTables, r.Names())
	}
	if tier, _ := r.Floor(OpSourcePublicationTables); tier != protocol.TierT0 {
		t.Fatalf("Floor = %q", tier)
	}
	if got := r.Target(protocol.OpRequest{Op: OpSourcePublicationTables}); got != "postgres" {
		t.Fatalf("Target = %q", got)
	}
	if layer, _ := r.LayerOf(OpSourcePublicationTables); layer != "postgres" {
		t.Fatalf("Layer = %q", layer)
	}
	for _, class := range []string{protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction} {
		if r.GuardForbidden(OpSourcePublicationTables, class) {
			t.Fatalf("a read is forbidden on %s", class)
		}
	}
	schema := op.ParamsSchema()
	if schema["additionalProperties"] != false {
		t.Fatalf("schema allows unknown fields: %v", schema)
	}
}
