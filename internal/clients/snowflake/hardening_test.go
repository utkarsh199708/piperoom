package snowflake

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake"

	"piperoom/internal/clients"
)

// GO-2026-6665: gosnowflake v1 logs session tokens once its level is raised.
// New must refuse the DSN knobs that raise it and pin the level to error.
func TestNewRefusesLogLevelKnobs(t *testing.T) {
	for _, tc := range []struct{ name, param string }{
		{"tracing", "tracing=debug"},
		{"clientConfigFile", "clientConfigFile=/tmp/sf.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := gosnowflake.GetLogger().SetLogLevel("debug"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = gosnowflake.GetLogger().SetLogLevel("error") })

			dsn := "user:s3cretpw@acct/db/sch?warehouse=wh&" + tc.param
			_, err := New(context.Background(), dsn)
			if err == nil {
				t.Fatalf("New accepted a DSN with %s", tc.name)
			}
			if strings.Contains(err.Error(), "s3cretpw") {
				t.Errorf("error leaks the password: %v", err)
			}
			if got := gosnowflake.GetLogger().GetLogLevel(); got != "error" {
				t.Errorf("driver log level = %q after New, want error", got)
			}
		})
	}
}

func TestPrimaryKeyColumn(t *testing.T) {
	cols := []string{"created_on", "database_name", "schema_name", "table_name", "column_name", "key_sequence", "constraint_name"}
	t.Run("lowest key_sequence wins, lower-cased", func(t *testing.T) {
		q := &fakeQuerier{rows: &fakeRows{cols: cols, data: [][]any{
			{"t", "DB", "ANALYTICS", "ORDERS", "TENANT_ID", "2", "pk"},
			{"t", "DB", "ANALYTICS", "ORDERS", "ORDER_ID", "1", "pk"},
		}}}
		d := &DB{q: q}
		got, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders")
		if err != nil || got != "order_id" {
			t.Fatalf("got %q, %v; want order_id", got, err)
		}
		if want := `SHOW PRIMARY KEYS IN TABLE "ANALYTICS"."ORDERS"`; q.lastQuery != want {
			t.Errorf("query = %q, want %q", q.lastQuery, want)
		}
	})
	t.Run("no key is ErrNotFound", func(t *testing.T) {
		d := &DB{q: &fakeQuerier{rows: &fakeRows{cols: cols}}}
		if _, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders"); !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("missing table is ErrNotFound", func(t *testing.T) {
		d := &DB{q: &fakeQuerier{err: &gosnowflake.SnowflakeError{Number: errObjectDoesNotExist}}}
		if _, err := d.PrimaryKeyColumn(context.Background(), "analytics.nope"); !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("bad identifier is rejected before any query", func(t *testing.T) {
		q := &fakeQuerier{}
		d := &DB{q: q}
		if _, err := d.PrimaryKeyColumn(context.Background(), "analytics.x;drop"); err == nil || q.lastQuery != "" {
			t.Fatalf("err = %v, query = %q", err, q.lastQuery)
		}
	})
	t.Run("missing columns in result", func(t *testing.T) {
		d := &DB{q: &fakeQuerier{rows: &fakeRows{cols: []string{"a"}, data: [][]any{{"x"}}}}}
		if _, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders"); err == nil {
			t.Fatal("want error")
		}
	})
}
