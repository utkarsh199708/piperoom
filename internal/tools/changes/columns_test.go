package changes

import (
	"context"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// The DDL audit logs a column rename under schema.table.column. what_changed
// kept only rows whose identity was exactly a source table, so the rename
// that breaks a CDC pipeline was dropped, with or without object (QA sweep).
func TestWhatChangedKeepsColumnDDL(t *testing.T) {
	src := &fakeSrc{rows: []clients.DDLChange{
		{At: t0.Add(-time.Minute), CommandTag: "ALTER TABLE", ObjectIdentity: "public.orders.amount_cents", Query: "ALTER TABLE orders RENAME COLUMN amount TO amount_cents", Username: "u"},
		{At: t0.Add(-time.Minute), CommandTag: "ALTER TABLE", ObjectIdentity: "public.orders_archive.x", Query: "ALTER TABLE orders_archive RENAME COLUMN a TO x", Username: "u"},
	}}
	for _, in := range []string{`{}`, `{"object":"public.orders"}`} {
		res, err := run(t, newTool(&fakeLog{}, src), scoped(), in)
		if err != nil {
			t.Fatal(err)
		}
		d := dataOf(t, res)
		if len(d.Changes) != 1 || d.Changes[0].Object != "public.orders.amount_cents" || strings.Contains(res.Summary, "orders_archive") {
			t.Fatalf("input %s: changes %+v, want only the column rename on public.orders", in, d.Changes)
		}
	}
}

// Postgres prints a mixed-case name quoted in object_identity, while owners
// lists the plain catalog name (#439). Names are compared as parsed
// identifiers: quoted keeps case, so "Orders" and orders stay two tables.
func TestWhatChangedMixedCaseTable(t *testing.T) {
	at := t0.Add(-time.Minute)
	rows := []clients.DDLChange{
		{At: at, CommandTag: "ALTER TABLE", ObjectIdentity: `public."Mixed"`, Query: "ALTER TABLE \"Mixed\" ADD c int", Username: "u"},
		{At: at.Add(-time.Second), CommandTag: "ALTER TABLE", ObjectIdentity: `public."Mixed".note`, Query: "ALTER TABLE \"Mixed\" RENAME COLUMN n TO note", Username: "u"},
		{At: at.Add(-2 * time.Second), CommandTag: "ALTER TABLE", ObjectIdentity: `public.orders`, Query: "ALTER TABLE orders ADD d int", Username: "u"},
		{At: at.Add(-3 * time.Second), CommandTag: "ALTER TABLE", ObjectIdentity: `public.mixed`, Query: "ALTER TABLE mixed ADD e int", Username: "u"},
		{At: at.Add(-4 * time.Second), CommandTag: "ALTER TABLE", ObjectIdentity: `public."MIXED"`, Query: "ALTER TABLE \"MIXED\" ADD f int", Username: "u"},
	}
	cases := []struct {
		owned []string
		input string
		want  []string
	}{
		{[]string{"public.Mixed"}, `{}`, []string{`public."Mixed"`, `public."Mixed".note`}},
		{[]string{"public.Mixed"}, `{"object":"public.Mixed"}`, []string{`public."Mixed"`, `public."Mixed".note`}},
		{[]string{"public.Mixed"}, `{"object":"public.\"Mixed\""}`, []string{`public."Mixed"`, `public."Mixed".note`}},
		{[]string{`public."Mixed"`}, `{"object":"public.Mixed"}`, []string{`public."Mixed"`, `public."Mixed".note`}},
		{[]string{"public.orders"}, `{"object":"public.orders"}`, []string{`public.orders`}},
		{[]string{"public.mixed"}, `{}`, []string{`public.mixed`}},
	}
	for _, c := range cases {
		sc := scopedTables(t, c.owned...)
		res, err := run(t, newTool(&fakeLog{}, &fakeSrc{rows: rows}), sc, c.input)
		if err != nil {
			t.Fatalf("%v %s: %v", c.owned, c.input, err)
		}
		var got []string
		for _, ch := range dataOf(t, res).Changes {
			got = append(got, ch.Object)
		}
		if strings.Join(got, "|") != strings.Join(c.want, "|") {
			t.Errorf("owned %v input %s: got %v, want %v", c.owned, c.input, got, c.want)
		}
	}
}

func scopedTables(t *testing.T, tables ...string) context.Context {
	t.Helper()
	return tools.WithPipeline(context.Background(), owners.Pipeline{
		ID: "orders", TopicPrefix: "pg", SourceTables: tables, Connectors: []string{"pg-orders-connector"},
	})
}
