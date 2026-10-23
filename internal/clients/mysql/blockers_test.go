package mysql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

var blockerCols = []string{"pid", "started", "user", "app", "state", "query", "blocks", "in_scope", "old", "same_db", "client"}

func TestBlockers(t *testing.T) {
	started := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	srv := (&fakeServer{}).on("WITH waited AS", blockerCols,
		row(int64(11), started.Unix(), b("app"), b("orders-svc"), b("Query updating"), b("UPDATE users SET password='hunter2xyz' WHERE id=1"), int64(0), int64(1), int64(1), int64(1), int64(1)),
		row(int64(12), int64(0), b("ops"), b(""), b("Sleep"), b("LOCK TABLES orders WRITE "+strings.Repeat("x", 300)), int64(1), int64(1), int64(0), int64(0), int64(1)),
	)
	d := newFake(t, srv)
	got, err := d.Blockers(context.Background(), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %+v", got)
	}
	s := got[0]
	if s.PID != 11 || !s.StartedAt.Equal(started) || !s.XactStart.Equal(started) || s.User != "app" || s.App != "orders-svc" ||
		s.State != "Query updating" || s.BlocksSlot || !s.InScope || !s.OldXact || !s.SameDB || !s.ClientBackend {
		t.Errorf("session 0 = %+v", s)
	}
	if strings.Contains(s.Query, "hunter2xyz") {
		t.Errorf("query not redacted: %q", s.Query)
	}
	s = got[1]
	if !s.StartedAt.IsZero() || !s.XactStart.IsZero() || !s.BlocksSlot || s.OldXact || len(s.Query) > MaxSessionQueryBytes {
		t.Errorf("session 1 = %+v", s)
	}
	q := srv.queries()[len(srv.queries())-1]
	if !reflect.DeepEqual(q.args, []any{int64(300), false}) || !strings.Contains(q.query, "AND FALSE)") {
		t.Fatalf("unscoped read: args %v", q.args)
	}
	for _, must := range []string{"t.TYPE = 'FOREGROUND'", "CONNECTION_ID()", "'Binlog Dump', 'Binlog Dump GTID'",
		"CASE WHEN in_scope THEN COALESCE(usr, '') ELSE '' END", "CASE WHEN in_scope THEN COALESCE(info, '') ELSE '' END",
		"CASE WHEN in_scope THEN COALESCE(app, '') ELSE '' END"} {
		if !strings.Contains(q.query, must) {
			t.Errorf("blockers SQL lacks %q", must)
		}
	}
	if _, err := d.Blockers(context.Background(), -time.Second); err == nil {
		t.Error("negative age accepted")
	}
}

func TestBlockersOnTablesBindsTheScope(t *testing.T) {
	srv := (&fakeServer{}).on("WITH waited AS", blockerCols,
		row(int64(21), int64(0), b(""), b(""), b("Sleep"), b(""), int64(1), int64(0), int64(0), int64(0), int64(1)))
	d := newFake(t, srv)
	got, err := d.BlockersOnTables(context.Background(), time.Minute, []string{"shop.orders", "shop.order_items"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].InScope || got[0].User != "" {
		t.Fatalf("got %+v", got)
	}
	q := srv.queries()[len(srv.queries())-1]
	if !reflect.DeepEqual(q.args, []any{int64(60), true, "shop", "orders", "shop", "order_items"}) {
		t.Fatalf("args %v", q.args)
	}
	if !strings.Contains(q.query, "(m.OBJECT_SCHEMA, m.OBJECT_NAME) IN ((?, ?), (?, ?))") || strings.Contains(q.query, "order_items") {
		t.Fatalf("scope not bound: %s", q.query)
	}
	// No tables: nothing is in scope, and the read still runs.
	if _, err := d.BlockersOnTables(context.Background(), time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	q = srv.queries()[len(srv.queries())-1]
	if !reflect.DeepEqual(q.args, []any{int64(60), true}) || !strings.Contains(q.query, "AND FALSE)") {
		t.Fatalf("empty scope: %v", q.args)
	}
}

func TestBlockersPrivilege(t *testing.T) {
	srv := (&fakeServer{}).onErr("WITH waited AS", &gomysql.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the PROCESS privilege(s)"})
	d := newFake(t, srv)
	if _, err := d.Blockers(context.Background(), time.Minute); !errors.Is(err, ErrPrivilege) || !strings.Contains(err.Error(), "PROCESS") {
		t.Fatalf("err = %v", err)
	}
}
