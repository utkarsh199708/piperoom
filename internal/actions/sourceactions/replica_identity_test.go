package sourceactions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func riFixture(ident string, pk bool) (*fakeSource, *fakeWriter, *setReplicaIdentity) {
	src := &fakeSource{
		tables: map[string]TableReplication{"public.refunds": {Table: "public.refunds", HasPrimaryKey: pk, ReplicaIdentity: ident}},
		rows:   1000,
		toast: []clients.ToastTable{{Table: "public.refunds", Found: true, ReplicaIdentity: strings.ToUpper(ident), ToastBytes: 2 << 20,
			Columns: []clients.ToastColumn{{Name: "note", Type: "text", Storage: "x", AvgWidth: 60}, {Name: "doc", Type: "jsonb", Storage: "x", AvgWidth: 40}, {Name: "x", AvgWidth: -1}}}},
	}
	w := &fakeWriter{}
	return src, w, &setReplicaIdentity{d: ReplicaIdentityDeps{Source: src, Writer: w}}
}

func riParams(mode, index string) json.RawMessage {
	m := map[string]string{"table": "public.refunds", "mode": mode}
	if index != "" {
		m["index"] = index
	}
	b, _ := json.Marshal(m)
	return b
}

func TestReplicaIdentityValidate(t *testing.T) {
	tests := []struct {
		name    string
		ident   string
		pk      bool
		mode    string
		index   string
		wantErr string
	}{
		{name: "default to full", ident: "default", mode: "full"},
		{name: "full to default with pk", ident: "full", pk: true, mode: "default"},
		{name: "index again with another index", ident: "index", pk: true, mode: "index", index: "refunds_uk"},
		{name: "already full", ident: "full", mode: "full", wantErr: "already has REPLICA IDENTITY FULL"},
		{name: "default without pk", ident: "full", mode: "default", wantErr: "no primary key"},
		{name: "nothing to full", ident: "nothing", mode: "full"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, a := riFixture(tt.ident, tt.pk)
			err := a.Validate(scopedCtx(), riParams(tt.mode, tt.index))
			if tt.wantErr == "" && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("Validate = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestReplicaIdentityValidateReadErrors(t *testing.T) {
	src, _, a := riFixture("default", false)
	src.tables = nil
	if err := a.Validate(scopedCtx(), riParams("full", "")); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table = %v, want ErrNotFound", err)
	}
	a.d.Source = nil
	if err := a.Validate(scopedCtx(), riParams("full", "")); err == nil || !strings.Contains(err.Error(), "cannot read replica identities") {
		t.Fatalf("no reader = %v", err)
	}
}

func TestReplicaIdentityPreviewFull(t *testing.T) {
	_, w, a := riFixture("default", true)
	res, err := a.Preview(scopedCtx(), riParams("full", ""))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	for _, want := range []string{`ALTER TABLE "public"."refunds" REPLICA IDENTITY FULL`, "ACCESS EXCLUSIVE", "source_blocking_sessions", "Extra WAL", "~2.1 KiB per change", "~2.1 MiB of WAL"} {
		if !strings.Contains(res.Expected, want) {
			t.Errorf("Expected lacks %q:\n%s", want, res.Expected)
		}
	}
	for _, want := range []string{"REPLICA IDENTITY DEFAULT (primary key)", "~1000 rows", "TOAST 2.0 MiB"} {
		if !strings.Contains(res.Current, want) {
			t.Errorf("Current lacks %q: %s", want, res.Current)
		}
	}
	if !res.Reversible || res.Rollback != `pg_set_replica_identity {"table": "public.refunds", "mode": "default"}` {
		t.Fatalf("rollback: %v %q", res.Reversible, res.Rollback)
	}
	if len(w.calls) != 0 {
		t.Fatal("Preview wrote")
	}
}

func TestReplicaIdentityPreviewOtherModes(t *testing.T) {
	_, _, a := riFixture("full", true)
	res, err := a.Preview(scopedCtx(), riParams("default", ""))
	if err != nil || !strings.Contains(res.Expected, "Less WAL") || !strings.Contains(res.Rollback, `"mode": "full"`) {
		t.Fatalf("full to default: %v %+v", err, res)
	}
	res, err = a.Preview(scopedCtx(), riParams("index", "refunds_uk"))
	if err != nil || !strings.Contains(res.Expected, `USING INDEX "refunds_uk"`) {
		t.Fatalf("index: %v %+v", err, res)
	}

	_, _, a = riFixture("index", true)
	res, _ = a.Preview(scopedCtx(), riParams("full", ""))
	if res.Reversible || !strings.Contains(res.Rollback, "previous index") {
		t.Fatalf("from index: rollback %v %q", res.Reversible, res.Rollback)
	}
}

func TestReplicaIdentityPreviewRefusedAndUnknownSize(t *testing.T) {
	src, _, a := riFixture("full", false)
	res, err := a.Preview(scopedCtx(), riParams("full", ""))
	if err != nil || !strings.HasPrefix(res.Expected, "no change: this step is refused") || res.Reversible {
		t.Fatalf("refused preview: %v %+v", err, res)
	}

	// Size reads that fail are a note, never an error.
	src.tables["public.refunds"] = TableReplication{Table: "public.refunds", ReplicaIdentity: "default"}
	src.toast = nil
	a.d.Source = &onlyTableReplication{s: src}
	res, err = a.Preview(scopedCtx(), riParams("full", ""))
	if err != nil || !strings.Contains(res.Current, "size not read") || !strings.Contains(res.Expected, "amount is unknown") {
		t.Fatalf("no size readers: %v %+v", err, res)
	}
}

// onlyTableReplication hides the optional size readers of fakeSource.
type onlyTableReplication struct {
	clients.SourceDB
	s *fakeSource
}

func (o *onlyTableReplication) TableReplication(ctx context.Context, tables []string) ([]TableReplication, error) {
	return o.s.TableReplication(ctx, tables)
}

func TestReplicaIdentityExecute(t *testing.T) {
	_, w, a := riFixture("default", true)
	out, err := a.Execute(scopedCtx(), riParams("index", "refunds_uk"))
	if err != nil || !strings.Contains(out, `USING INDEX "refunds_uk"`) {
		t.Fatalf("Execute: %q %v", out, err)
	}
	if len(w.calls) != 1 || w.calls[0].method != "SetReplicaIdentity" || w.calls[0].args[0] != "public.refunds" || w.calls[0].args[1] != "index" || w.calls[0].args[2] != "refunds_uk" {
		t.Fatalf("writes: %+v", w.calls)
	}
	w.err = clients.ErrNotFound
	if _, err := a.Execute(scopedCtx(), riParams("full", "")); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("writer error = %v, want wrapped ErrNotFound", err)
	}
}
