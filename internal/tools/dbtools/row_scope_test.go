package dbtools

import (
	"context"
	"errors"
	"testing"

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
