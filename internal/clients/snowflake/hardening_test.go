package snowflake

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"

	"piperoom/internal/clients"
)

// GO-2026-6665: gosnowflake v1 logged session tokens once its level was raised;
// v2 masks them, and these checks stay as defence in depth. v2 reports the
// level upper-case ("ERROR"), v1 lower-case, so the level is compared folded.
// hardenedConfig must refuse the DSN knobs that raise it and pin the level to
// error. It is tested directly so no case can reach PingContext (no network).
func TestHardenedConfigRefusesLogLevelKnobs(t *testing.T) {
	// Parameter values must survive ParseDSN, which splits database/schema at
	// the last "/": a raw path would be swallowed into the schema name.
	for _, tc := range []struct{ name, param, wantMsg string }{
		{"tracing", "tracing=debug", "tracing"},
		{"clientConfigFile", "clientConfigFile=%2Ftmp%2Fsf.json", "clientConfigFile"},
		{"clientConfigFile relative", "clientConfigFile=sf.json", "clientConfigFile"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := gosnowflake.GetLogger().SetLogLevel("debug"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = gosnowflake.GetLogger().SetLogLevel("error") })

			cfg, err := hardenedConfig("user:pw@acct/db/sch?warehouse=wh&" + tc.param)
			if err == nil || cfg != nil {
				t.Fatalf("hardenedConfig = %v, %v; want a refusal", cfg, err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error %q does not name %s", err, tc.wantMsg)
			}
			if got := gosnowflake.GetLogger().GetLogLevel(); !strings.EqualFold(got, "error") {
				t.Errorf("driver log level = %q after refusal, want error", got)
			}
		})
	}
}

func TestHardenedConfigAcceptsCleanDSN(t *testing.T) {
	if err := gosnowflake.GetLogger().SetLogLevel("debug"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gosnowflake.GetLogger().SetLogLevel("error") })

	cfg, err := hardenedConfig("user:pw@acct/db/sch?warehouse=wh")
	if err != nil || cfg == nil {
		t.Fatalf("hardenedConfig = %v, %v", cfg, err)
	}
	if cfg.Database != "db" || cfg.Schema != "sch" || cfg.Warehouse != "wh" {
		t.Errorf("config = %+v", cfg)
	}
	if got := gosnowflake.GetLogger().GetLogLevel(); !strings.EqualFold(got, "error") {
		t.Errorf("driver log level = %q, want error", got)
	}
}

// New must refuse before opening anything, and must not echo the password.
func TestNewRefusesTracingWithoutNetwork(t *testing.T) {
	t.Cleanup(func() { _ = gosnowflake.GetLogger().SetLogLevel("error") })
	_, err := New(context.Background(), "user:s3cretpw@acct/db/sch?warehouse=wh&tracing=debug")
	if err == nil || !strings.Contains(err.Error(), "tracing") {
		t.Fatalf("err = %v, want a tracing refusal", err)
	}
	if strings.Contains(err.Error(), "s3cretpw") {
		t.Errorf("error leaks the password: %v", err)
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
	t.Run("no key says so, still ErrNotFound", func(t *testing.T) {
		d := &DB{q: &fakeQuerier{rows: &fakeRows{cols: cols}}}
		_, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders")
		if err == nil || !strings.Contains(err.Error(), "no declared primary key") {
			t.Fatalf("err = %v, want \"no declared primary key\"", err)
		}
	})
	t.Run("scan error", func(t *testing.T) {
		d := &DB{q: &fakeQuerier{rows: &fakeRows{cols: cols, data: [][]any{{"t", "d", "s", "t", "ID", "1", "pk"}}, scanErr: errors.New("boom")}}}
		if _, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders"); err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("non-numeric key_sequence", func(t *testing.T) {
		d := &DB{q: &fakeQuerier{rows: &fakeRows{cols: cols, data: [][]any{{"t", "d", "s", "t", "ID", "x", "pk"}}}}}
		if _, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders"); err == nil || !strings.Contains(err.Error(), "key_sequence") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("rows.Err", func(t *testing.T) {
		d := &DB{q: &fakeQuerier{rows: &fakeRows{cols: cols, err: errors.New("stream broke")}}}
		if _, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders"); err == nil || !strings.Contains(err.Error(), "stream broke") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing columns in result", func(t *testing.T) {
		d := &DB{q: &fakeQuerier{rows: &fakeRows{cols: []string{"a"}, data: [][]any{{"x"}}}}}
		if _, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders"); err == nil {
			t.Fatal("want error")
		}
	})
}

// gosnowflake v2.3.0 turned OCSP off by default; hardenedConfig must opt back
// in, and must leave an explicit operator choice alone.
func TestHardenedConfigOCSP(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  gosnowflake.OCSPFailOpenMode
	}{
		{"default opts in to fail-open", "", gosnowflake.OCSPFailOpenTrue},
		{"explicit fail-closed is kept", "&ocspFailOpen=false", gosnowflake.OCSPFailOpenFalse},
		{"explicit fail-open is kept", "&ocspFailOpen=true", gosnowflake.OCSPFailOpenTrue},
		{"disableOCSPChecks is kept", "&disableOCSPChecks=true", ocspNotSet},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := hardenedConfig("user:pw@acct/db/sch?warehouse=wh" + tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.OCSPFailOpen != tc.want {
				t.Errorf("OCSPFailOpen = %v, want %v", cfg.OCSPFailOpen, tc.want)
			}
		})
	}
}
