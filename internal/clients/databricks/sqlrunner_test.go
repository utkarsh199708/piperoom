package databricks

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"
)

// fakeDriver is a minimal database/sql driver, so sqlRunner (the adapter
// over *sql.DB) is tested without the network: column types, named
// parameters, the row cap and closing the result early.
type fakeDriver struct {
	rows     int
	closed   atomic.Bool
	gotNames []string
	gotVals  []driver.Value
	err      error
}

type fakeConnector struct{ d *fakeDriver }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn(c), nil }
func (c fakeConnector) Driver() driver.Driver                        { return nil }

type fakeConn struct{ d *fakeDriver }

func (c fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c fakeConn) Close() error                        { return nil }
func (c fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c fakeConn) QueryContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Rows, error) {
	for _, a := range args {
		c.d.gotNames = append(c.d.gotNames, a.Name)
		c.d.gotVals = append(c.d.gotVals, a.Value)
	}
	if c.d.err != nil {
		return nil, c.d.err
	}
	return &fakeRows{d: c.d}, nil
}

type fakeRows struct {
	d *fakeDriver
	i int
}

func (r *fakeRows) Columns() []string { return []string{"id", "amount"} }
func (r *fakeRows) Close() error      { r.d.closed.Store(true); return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= r.d.rows {
		return io.EOF
	}
	r.i++
	dest[0] = int64(r.i)
	dest[1] = []byte("1.50")
	return nil
}
func (r *fakeRows) ColumnTypeDatabaseTypeName(i int) string { return []string{"bigint", "DECIMAL"}[i] }

func TestSQLRunner(t *testing.T) {
	fd := &fakeDriver{rows: 5}
	db := sql.OpenDB(fakeConnector{fd})
	defer db.Close()
	res, err := sqlRunner{db: db}.run(context.Background(), query{
		SQL:     "SELECT ...",
		Params:  []sql.NamedArg{sql.Named("key", "a'b"), sql.Named("since", int64(7))},
		MaxRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("read %d rows, want the cap of 2", len(res.Rows))
	}
	if res.Fields[0] != (field{"id", "BIGINT"}) || res.Fields[1] != (field{"amount", "DECIMAL"}) {
		t.Fatalf("fields = %+v", res.Fields)
	}
	if b, ok := res.Rows[0][1].([]byte); !ok || string(b) != "1.50" {
		t.Fatalf("cell = %#v", res.Rows[0][1])
	}
	if len(fd.gotNames) != 2 || fd.gotNames[0] != "key" || fd.gotNames[1] != "since" || fd.gotVals[0] != "a'b" {
		t.Fatalf("args = %v %v", fd.gotNames, fd.gotVals)
	}
	if !fd.closed.Load() {
		t.Fatal("result not closed")
	}
}

func TestSQLRunnerError(t *testing.T) {
	want := execErr{"boom", "42P01"}
	db := sql.OpenDB(fakeConnector{&fakeDriver{err: want}})
	defer db.Close()
	if _, err := (sqlRunner{db: db}).run(context.Background(), query{SQL: "x", MaxRows: 1}); !errors.As(err, new(sqlStater)) {
		t.Fatalf("err = %v", err)
	}
}
