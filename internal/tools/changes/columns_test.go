package changes

import (
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
