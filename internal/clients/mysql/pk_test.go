package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"piperoom/internal/clients"
)

func TestPrimaryKeyColumn(t *testing.T) {
	cols := []string{"COLUMN_NAME"}
	cases := []struct {
		name string
		srv  *fakeServer
		want string
		err  error
		text string
	}{
		{name: "single", srv: (&fakeServer{}).on("KEY_COLUMN_USAGE", cols, row([]byte("order_id"))), want: "order_id"},
		{name: "composite", srv: (&fakeServer{}).on("KEY_COLUMN_USAGE", cols, row([]byte("order_id")), row([]byte("line"))), err: clients.ErrNotSupported},
		{name: "no primary key", srv: (&fakeServer{}).on("KEY_COLUMN_USAGE", cols, row(nil)), err: clients.ErrNotFound, text: "no primary key"},
		{name: "missing table", srv: (&fakeServer{}).on("KEY_COLUMN_USAGE", cols), err: clients.ErrNotFound, text: "missing"},
		{name: "privilege", srv: (&fakeServer{}).onErr("KEY_COLUMN_USAGE", &gomysql.MySQLError{Number: 1142, Message: "denied"}), err: ErrPrivilege},
		{name: "odd column name", srv: (&fakeServer{}).on("KEY_COLUMN_USAGE", cols, row([]byte("a`b"))), text: "not a plain identifier"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newFake(t, tc.srv)
			got, err := d.PrimaryKeyColumn(context.Background(), "shop.orders")
			if tc.err == nil && tc.text == "" {
				if err != nil || got != tc.want {
					t.Fatalf("got %q, %v", got, err)
				}
				var q call
				for _, c := range tc.srv.queries() {
					if strings.Contains(c.query, "KEY_COLUMN_USAGE") {
						q = c
					}
				}
				if len(q.args) != 2 || q.args[0] != "shop" || q.args[1] != "orders" || strings.Contains(q.query, "orders") {
					t.Fatalf("names must be bound: %+v", q)
				}
				if d := tc.srv.deadlines[len(tc.srv.deadlines)-1]; d <= 0 || d > callTimeout {
					t.Fatalf("deadline %s", d)
				}
				return
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("err %v, want %v", err, tc.err)
			}
			if tc.text != "" && (err == nil || !strings.Contains(err.Error(), tc.text)) {
				t.Fatalf("err %v, want %q", err, tc.text)
			}
		})
	}
	for _, bad := range []string{"orders", "shop.or`ders", "a.b.c", ""} {
		srv := &fakeServer{}
		d := newFake(t, srv)
		n := len(srv.queries())
		if _, err := d.PrimaryKeyColumn(context.Background(), bad); err == nil || len(srv.queries()) != n {
			t.Errorf("%q: err %v", bad, err)
		}
	}
}
