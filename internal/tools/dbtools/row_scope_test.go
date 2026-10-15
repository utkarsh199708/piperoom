package dbtools

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// pkSource is a fakeSource that reports the table's primary key column, as
// the real postgres client does.
type pkSource struct {
	*fakeSource
	pk    string
	pkErr error
}

func (s *pkSource) PrimaryKeyColumn(context.Context, string) (string, error) { return s.pk, s.pkErr }

// pg_row reads by primary key only. Any other column (an email, a tax id,
// ctid) would make it an existence oracle for personal data in the source
// table and put the probed value in the room log (QA sweep, pg_row
// pk_column).
func TestPgRowRequiresThePrimaryKey(t *testing.T) {
	src := &pkSource{fakeSource: testDeps().Src.(*fakeSource), pk: "id"}
	d := testDeps()
	d.Src = src
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	pg, _ := reg.Get("pg_row")

	for _, col := range []string{"email", "ctid", "customer_id"} {
		_, err := runTool(t, pg, `{"pipeline_id":"orders","table":"public.orders","pk_column":"`+col+`","key":"7"}`)
		if !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("pk_column %s: err = %v, want ErrBadInput", col, err)
		}
	}
	if src.rowCalls != 0 {
		t.Fatalf("read %d row(s) by a non-key column", src.rowCalls)
	}
	if res, err := runTool(t, pg, `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`); err != nil || src.rowCalls != 1 {
		t.Fatalf("by primary key: %+v, %v (row calls %d)", res, err, src.rowCalls)
	}

	src.pk, src.pkErr = "", errors.New("postgres: primary key of public.orders: no rows in result set")
	if _, err := runTool(t, pg, `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`); err == nil || src.rowCalls != 1 {
		t.Fatalf("key lookup failed: err = %v, row calls %d; want an error and no read", err, src.rowCalls)
	}
}

// A column is shown by value only when its name says timestamp AND its value
// is one. A text column that merely ends in "_at" ("contact_at" holding a
// phone number) is personal data, not a load time (QA sweep, PII in row
// tools).
func TestRowToolsShowOnlyTimeValues(t *testing.T) {
	const phone = "+1 555 0100"
	d := testDeps()
	d.Src.(*fakeSource).rows["public.orders|7"]["contact_at"] = phone
	d.Wh.(*fakeWarehouse).rows["analytics.orders|7"]["contact_at"] = phone
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	for name, in := range map[string]string{
		"pg_row":        `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`,
		"warehouse_row": `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"id","key":"7"}`,
	} {
		tt, _ := reg.Get(name)
		res, err := runTool(t, tt, in)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		data := res.Data.(map[string]any)
		if strings.Contains(res.Summary, phone) || data["contact_at"] != nil {
			t.Errorf("%s shows the text value of contact_at: summary %q data %v", name, res.Summary, data)
		}
		if data["updated_at"] != "2026-10-11T10:00:00Z" {
			t.Errorf("%s: updated_at = %v, want the timestamp kept", name, data["updated_at"])
		}
	}
}

// Without a SQL filter the tool filters in memory; a column rename is logged
// under schema.table.column and must count as DDL on the table, while a
// longer table name must not.
func TestFilterDDLKeepsColumnDDL(t *testing.T) {
	changes := []clients.DDLChange{
		{ObjectIdentity: "public.orders"},
		{ObjectIdentity: "public.orders.amount"},
		{ObjectIdentity: "public.orders_archive"},
	}
	got := filterDDL(changes, "public.orders")
	if len(got) != 2 || got[1].ObjectIdentity != "public.orders.amount" {
		t.Fatalf("got %+v, want the table and its column", got)
	}
}
