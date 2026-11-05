package connprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

const sfDSN = "svc_piperoom:" + pw + "@myorg-myacct/ANALYTICS/PUBLIC?warehouse=PIPEROOM_WH&role=PIPEROOM_RO"

// fakeSF is a Snowflake session whose statements fail when failOn matches.
type fakeSF struct {
	failOn func(query string) error
	ran    []string
	closed bool
}

func (f *fakeSF) probe(_ context.Context, q string) error {
	f.ran = append(f.ran, q)
	if f.failOn != nil {
		return f.failOn(q)
	}
	return nil
}

func (f *fakeSF) close() { f.closed = true }

func sfConnectorFor(f *fakeSF, err error) sfConnector {
	return func(context.Context, *gosnowflake.Config, *Guard) (sfSession, error) {
		if err != nil {
			return nil, err
		}
		return f, nil
	}
}

func sfTarget(dsn string) Target {
	return Target{
		Kind:   KindWarehouse,
		Config: config.Config{WarehouseKind: "snowflake", WarehouseDBURL: dsn},
		Pipelines: []owners.Pipeline{{
			ID: "orders", SinkTables: []string{"analytics.orders"}, SinkTSColumn: "updated_at",
		}},
	}
}

func noSFLeaks(t *testing.T, r Result) {
	t.Helper()
	b, _ := json.Marshal(r)
	for _, bad := range []string{pw, leak, sfDSN, "svc_piperoom", "@myorg"} {
		if strings.Contains(string(b), bad) {
			t.Errorf("result contains %q: %s", bad, b)
		}
	}
}

func TestWarehouseSnowflakeOK(t *testing.T) {
	d := &fakeDialer{}
	f := &fakeSF{}
	r := probeWarehouseWith(context.Background(), sfTarget(sfDSN), testGuard("10.1.2.3", d), nil, sfConnectorFor(f, nil))
	if got := strings.Join(ids(r), ","); got != "config,reach,login,select_sink" {
		t.Fatalf("ids = %s", got)
	}
	for _, c := range r.Checks {
		if c.Status != StatusOK {
			t.Errorf("%s = %s (%s / %s)", c.ID, c.Status, c.Problem, c.Fix)
		}
	}
	if len(d.dials) != 1 || d.dials[0] != "10.1.2.3:443" {
		t.Errorf("reach dialed %v", d.dials)
	}
	want := `SELECT "UPDATED_AT" FROM "ANALYTICS"."ORDERS" LIMIT 0`
	if len(f.ran) != 1 || f.ran[0] != want {
		t.Errorf("statements = %v, want %s", f.ran, want)
	}
	if !f.closed {
		t.Error("session not closed")
	}
	noSFLeaks(t, r)
}

func TestWarehouseSnowflakeConfigFailures(t *testing.T) {
	tests := []struct{ name, dsn, problem string }{
		{"empty", "", "no connection string"},
		{"unparsable", "not a dsn " + pw, "cannot be parsed"},
		{"http protocol", sfDSN + "&protocol=http", "other than https"},
		{"tracing", sfDSN + "&tracing=debug", "tracing or clientConfigFile"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &fakeDialer{}
			f := &fakeSF{}
			r := probeWarehouseWith(context.Background(), sfTarget(tc.dsn), testGuard("10.1.2.3", d), nil, sfConnectorFor(f, nil))
			if c := get(r, "config"); c.Status != StatusFail || !strings.Contains(c.Problem, tc.problem) || c.Fix == "" {
				t.Fatalf("config = %+v", c)
			}
			for _, id := range []string{"reach", "login", "select_sink"} {
				if get(r, id).Status != StatusSkip {
					t.Errorf("%s = %s, want skip", id, get(r, id).Status)
				}
			}
			if len(d.dials) != 0 || len(f.ran) != 0 {
				t.Error("nothing may run after a config failure")
			}
			noSFLeaks(t, r)
		})
	}
}

func TestWarehouseSnowflakeReachGuard(t *testing.T) {
	// A Snowflake host that resolves to the metadata address is refused and never dialed.
	d := &fakeDialer{}
	r := probeWarehouseWith(context.Background(), sfTarget(sfDSN), testGuard("169.254.169.254", d), nil, sfConnectorFor(&fakeSF{}, nil))
	if c := get(r, "reach"); c.Status != StatusFail || !strings.Contains(c.Problem, "is not allowed") {
		t.Fatalf("reach = %+v", c)
	}
	if len(d.dials) != 0 {
		t.Errorf("dialed %v", d.dials)
	}
	if get(r, "login").Status != StatusSkip || get(r, "select_sink").Status != StatusSkip {
		t.Errorf("later checks must be skipped: %+v", r.Checks)
	}
	// Dial failure.
	r = probeWarehouseWith(context.Background(), sfTarget(sfDSN), testGuard("10.1.2.3", &fakeDialer{err: errors.New(leak)}), nil, sfConnectorFor(&fakeSF{}, nil))
	if c := get(r, "reach"); c.Status != StatusFail || !strings.Contains(c.Fix, "no HTTP proxy") {
		t.Fatalf("reach = %+v", c)
	}
	noSFLeaks(t, r)
}

func TestWarehouseSnowflakeLogin(t *testing.T) {
	for _, err := range []error{errors.New("390100 " + leak + pw), fmt.Errorf("x: %w", ErrBlockedAddress)} {
		r := probeWarehouseWith(context.Background(), sfTarget(sfDSN), testGuard("10.1.2.3", &fakeDialer{}), nil, sfConnectorFor(nil, err))
		if c := get(r, "login"); c.Status != StatusFail || c.Problem == "" || c.Fix == "" {
			t.Fatalf("login = %+v", c)
		}
		if get(r, "select_sink").Status != StatusSkip {
			t.Error("select_sink must be skipped after a failed login")
		}
		noSFLeaks(t, r)
	}
}

func TestWarehouseSnowflakeBrowserAuthIsSkipped(t *testing.T) {
	called := false
	conn := func(context.Context, *gosnowflake.Config, *Guard) (sfSession, error) {
		called = true
		return &fakeSF{}, nil
	}
	r := probeWarehouseWith(context.Background(), sfTarget(sfDSN+"&authenticator=externalbrowser"), testGuard("10.1.2.3", &fakeDialer{}), nil, conn)
	if called {
		t.Fatal("an external browser login must not be attempted")
	}
	if c := get(r, "login"); c.Status != StatusSkip || !strings.Contains(c.Detail, "not tested") {
		t.Fatalf("login = %+v", c)
	}
	if anyFailed(r) {
		t.Error("a skipped check is not a failure")
	}
}

// anyFailed is the rule Run uses for OK, for tests that bypass Run.
func anyFailed(r Result) bool {
	for _, c := range r.Checks {
		if c.Status == StatusFail {
			return true
		}
	}
	return false
}

func TestWarehouseSnowflakeSelectSink(t *testing.T) {
	failing := func(q string) error { return fmt.Errorf("sql: %s", leak) }
	t.Run("select fails", func(t *testing.T) {
		f := &fakeSF{failOn: failing}
		r := probeWarehouseWith(context.Background(), sfTarget(sfDSN), testGuard("10.1.2.3", &fakeDialer{}), nil, sfConnectorFor(f, nil))
		c := get(r, "select_sink")
		if c.Status != StatusFail || !strings.Contains(c.Problem, "analytics.orders (column updated_at)") ||
			!strings.Contains(c.Fix, `GRANT SELECT ON TABLE "ANALYTICS"."ORDERS" TO ROLE PIPEROOM_RO;`) {
			t.Fatalf("select_sink = %+v", c)
		}
		noSFLeaks(t, r)
	})
	t.Run("timeout", func(t *testing.T) {
		f := &fakeSF{failOn: func(string) error { return fmt.Errorf("x: %w", context.DeadlineExceeded) }}
		r := probeWarehouseWith(context.Background(), sfTarget(sfDSN), testGuard("10.1.2.3", &fakeDialer{}), nil, sfConnectorFor(f, nil))
		if c := get(r, "select_sink"); c.Status != StatusFail || !strings.Contains(c.Problem, "timed out") {
			t.Fatalf("select_sink = %+v", c)
		}
	})
	t.Run("invalid names never reach SQL", func(t *testing.T) {
		f := &fakeSF{}
		tg := sfTarget(sfDSN)
		tg.Pipelines[0].SinkTables = []string{`analytics.orders"; DROP TABLE x; --`, "orders"}
		r := probeWarehouseWith(context.Background(), tg, testGuard("10.1.2.3", &fakeDialer{}), nil, sfConnectorFor(f, nil))
		if c := get(r, "select_sink"); c.Status != StatusFail || !strings.Contains(c.Problem, "not a valid Snowflake name") {
			t.Fatalf("select_sink = %+v", c)
		}
		if len(f.ran) != 0 {
			t.Errorf("statements ran: %v", f.ran)
		}
	})
	t.Run("no sink tables", func(t *testing.T) {
		tg := sfTarget(sfDSN)
		tg.Pipelines[0].SinkTables = nil
		r := probeWarehouseWith(context.Background(), tg, testGuard("10.1.2.3", &fakeDialer{}), nil, sfConnectorFor(&fakeSF{}, nil))
		if get(r, "select_sink").Status != StatusSkip {
			t.Fatalf("select_sink = %+v", get(r, "select_sink"))
		}
	})
	t.Run("three part name and no timestamp column", func(t *testing.T) {
		f := &fakeSF{}
		tg := sfTarget(sfDSN)
		tg.Pipelines[0].SinkTables = []string{"other.analytics.orders"}
		tg.Pipelines[0].SinkTSColumn = ""
		probeWarehouseWith(context.Background(), tg, testGuard("10.1.2.3", &fakeDialer{}), nil, sfConnectorFor(f, nil))
		if len(f.ran) != 1 || f.ran[0] != `SELECT 1 FROM "OTHER"."ANALYTICS"."ORDERS" LIMIT 0` {
			t.Errorf("statements = %v", f.ran)
		}
	})
}

func TestSFQuote(t *testing.T) {
	tests := []struct {
		in, want string
		ok       bool
	}{
		{"analytics.orders", `"ANALYTICS"."ORDERS"`, true},
		{"db.s.t$1", `"DB"."S"."T$1"`, true},
		{"orders", "", false},
		{"a.b.c.d", "", false},
		{`a."b`, "", false},
		{"a.b c", "", false},
		{"a.1b", "", false},
	}
	for _, tc := range tests {
		got, ok := sfQuote(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("sfQuote(%q) = %q, %v", tc.in, got, ok)
		}
	}
}
