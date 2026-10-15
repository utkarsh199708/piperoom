package changes

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
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

// Log rows (the watcher's, stored with the quoted identity) take the same
// object filter as live rows.
func TestWhatChangedMixedCaseFromLog(t *testing.T) {
	at := t0.Add(-time.Minute)
	log := &fakeLog{rows: []clients.PipelineChange{
		{ID: 1, Kind: clients.ChangeDDL, Object: `public."Mixed"`, At: at, Source: "watcher"},
		{ID: 2, Kind: clients.ChangeDDL, Object: `public."Mixed".note`, At: at.Add(-time.Second), Source: "watcher"},
		{ID: 3, Kind: clients.ChangeDDL, Object: `public."MIXED"`, At: at.Add(-2 * time.Second), Source: "watcher"},
	}}
	res, err := run(t, newTool(log, &fakeSrc{}), scopedTables(t, "public.Mixed"), `{"object":"public.Mixed"}`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ch := range dataOf(t, res).Changes {
		got = append(got, ch.Object)
	}
	if want := []string{`public."Mixed"`, `public."Mixed".note`}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitIdent(t *testing.T) {
	for _, tc := range []struct {
		in    string
		fold  bool
		want  []string
		valid bool
	}{
		{`public.Orders`, true, []string{"public", "orders"}, true},
		{`public.Orders`, false, []string{"public", "Orders"}, true},
		{`public."Orders"`, true, []string{"public", "Orders"}, true},
		{`public."a.b"`, true, []string{"public", "a.b"}, true},
		{`public."a""b"`, true, []string{"public", `a"b`}, true},
		{`public."order"`, true, []string{"public", "order"}, true},
		{`public."a"b"`, true, nil, false},
		{`public.a"b`, true, nil, false},
		{`public..x`, true, nil, false},
		{"public.\u212Aey", true, []string{"public", "\u212Aey"}, true}, // Kelvin sign: not folded
	} {
		got, ok := splitIdent(tc.in, tc.fold)
		if ok != tc.valid || !slices.Equal(got, tc.want) {
			t.Errorf("splitIdent(%q, %v) = %q, %v; want %q, %v", tc.in, tc.fold, got, ok, tc.want, tc.valid)
		}
	}
}

// A table name containing '.' or '"' matches when the owners entry is quoted
// (#461). The plain spelling public.a.b is refused when owners.yaml loads
// (internal/owners TestLoadSourceTableSpelling), so it never reaches the tool.
// Rows with a gap marker would assert today's wrong output; none is left.
func TestWhatChangedIdentGaps(t *testing.T) {
	at := t0.Add(-time.Minute)
	for _, tc := range []struct {
		owned, identity, input string
		gap                    string // "" once fixed
		ideal                  bool   // kept when the gap is closed
	}{
		{owned: `public."a.b"`, identity: `public."a.b"`, input: `{"object":"public.\"a.b\""}`, ideal: true},
		{owned: `public."a.b"`, identity: `public."a.b".col`, input: `{}`, ideal: true},
		{owned: `public."a""b"`, identity: `public."a""b"`, input: `{"object":"public.\"a\"\"b\""}`, ideal: true},
		{owned: `public."a.b"`, identity: `public."a.b"`, input: `{}`, ideal: true},
		{owned: `public."a""b"`, identity: `public."a""b"`, input: `{}`, ideal: true},
	} {
		src := &fakeSrc{rows: []clients.DDLChange{{At: at, CommandTag: "ALTER TABLE", ObjectIdentity: tc.identity, Query: "q", Username: "u"}}}
		res, err := run(t, newTool(&fakeLog{}, src), scopedTables(t, tc.owned), tc.input)
		if err != nil && tc.gap == "" {
			t.Fatalf("owned %s input %s: %v", tc.owned, tc.input, err)
		}
		kept := err == nil && len(dataOf(t, res).Changes) == 1
		want := tc.ideal
		if tc.gap != "" {
			want = false // today's output
		}
		if kept != want {
			t.Errorf("owned %s identity %s input %s (gap %q): kept = %v, want %v", tc.owned, tc.identity, tc.input, tc.gap, kept, want)
		}
	}
}
