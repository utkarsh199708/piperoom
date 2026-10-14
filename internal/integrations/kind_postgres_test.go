package integrations

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/integrations/spec"
)

// fakeRow returns fixed values or an error.
type fakeRow struct {
	vals []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.vals) {
		return fmt.Errorf("fake: scan %d dest, have %d values", len(dest), len(r.vals))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *int:
			*p = r.vals[i].(int)
		case *bool:
			*p = r.vals[i].(bool)
		case *string:
			*p = r.vals[i].(string)
		default:
			return fmt.Errorf("fake: unsupported dest %T", d)
		}
	}
	return nil
}

// fakePG answers queries by substring; tables maps the quoted table name
// argument to its four privileges.
type fakePG struct {
	tls     bool
	version int
	replica bool
	slots   int
	slotErr error
	super   bool
	timeout string
	ro      string
	tables  map[string][4]bool
	block   bool // every query waits for ctx
	closed  bool
}

func (f *fakePG) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if f.block {
		<-ctx.Done()
		return fakeRow{err: ctx.Err()}
	}
	switch {
	case strings.Contains(sql, "server_version_num"):
		return fakeRow{vals: []any{f.version}}
	case strings.Contains(sql, "pg_is_in_recovery"):
		return fakeRow{vals: []any{f.replica}}
	case strings.Contains(sql, "pg_replication_slots"):
		if f.slotErr != nil {
			return fakeRow{err: f.slotErr}
		}
		return fakeRow{vals: []any{f.slots}}
	case strings.Contains(sql, "rolsuper"):
		return fakeRow{vals: []any{f.super}}
	case strings.Contains(sql, "has_table_privilege"):
		p, ok := f.tables[args[0].(string)]
		if !ok {
			return fakeRow{err: &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}}
		}
		return fakeRow{vals: []any{p[0], p[1], p[2], p[3]}}
	case strings.Contains(sql, "statement_timeout"):
		return fakeRow{vals: []any{f.timeout, f.ro}}
	}
	return fakeRow{err: errors.New("fake: unexpected query " + sql)}
}

func (f *fakePG) TLS() bool                   { return f.tls }
func (f *fakePG) Close(context.Context) error { f.closed = true; return nil }

func goodPG() *fakePG {
	return &fakePG{tls: true, version: 160002, slots: 1, timeout: "10s", ro: "on",
		tables: map[string][4]bool{`"public"."orders"`: {true, false, false, false}}}
}

func pgDeps(sess *fakePG, connErr error) Deps {
	d := fastDeps()
	d.pgConnect = func(ctx context.Context, _ pgTarget) (pgSession, error) {
		if connErr != nil {
			if errors.Is(connErr, context.DeadlineExceeded) {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return nil, connErr
		}
		return sess, nil
	}
	return d
}

func pgValues(port string) spec.Values {
	return vals(map[string]string{"host": "127.0.0.1", "port": port, "database": "orders", "user": "piperoom_ro",
		"sslmode": "verify-full", "tables": "public.orders"}, map[string]string{"password": canary})
}

func TestPostgresSourceChecks(t *testing.T) {
	_, port := listenTCP(t)
	tests := []struct {
		name    string
		sess    func() *fakePG
		connErr error
		v       func(spec.Values)
		want    map[string]spec.CheckStatus
		fix     map[string]string // check id → substring of Fix
	}{
		{name: "all pass", sess: goodPG, want: map[string]spec.CheckStatus{
			"reach": spec.CheckPass, "tls": spec.CheckPass, "login": spec.CheckPass, "version": spec.CheckPass,
			"primary": spec.CheckPass, "slots_visible": spec.CheckPass, "not_superuser": spec.CheckPass,
			"select:public.orders": spec.CheckPass, "read_only:public.orders": spec.CheckPass, "timeout": spec.CheckPass}},
		{name: "old version", sess: func() *fakePG { f := goodPG(); f.version = 120005; return f },
			want: map[string]spec.CheckStatus{"version": spec.CheckFail}, fix: map[string]string{"version": "Postgres 13"}},
		{name: "replica", sess: func() *fakePG { f := goodPG(); f.replica = true; return f },
			want: map[string]spec.CheckStatus{"primary": spec.CheckFail}, fix: map[string]string{"primary": "primary"}},
		{name: "slots unreadable", sess: func() *fakePG {
			f := goodPG()
			f.slotErr = &pgconn.PgError{Code: "42501", Message: "permission denied"}
			return f
		}, want: map[string]spec.CheckStatus{"slots_visible": spec.CheckFail}, fix: map[string]string{"slots_visible": "GRANT pg_monitor TO piperoom_ro;"}},
		{name: "no slots", sess: func() *fakePG { f := goodPG(); f.slots = 0; return f },
			want: map[string]spec.CheckStatus{"slots_visible": spec.CheckWarn}},
		{name: "superuser", sess: func() *fakePG { f := goodPG(); f.super = true; return f },
			want: map[string]spec.CheckStatus{"not_superuser": spec.CheckFail}},
		{name: "cannot select", sess: func() *fakePG {
			f := goodPG()
			f.tables[`"public"."orders"`] = [4]bool{false, false, false, false}
			return f
		}, want: map[string]spec.CheckStatus{"select:public.orders": spec.CheckFail, "read_only:public.orders": spec.CheckPass},
			fix: map[string]string{"select:public.orders": "GRANT SELECT ON public.orders TO piperoom_ro;"}},
		{name: "can write", sess: func() *fakePG {
			f := goodPG()
			f.tables[`"public"."orders"`] = [4]bool{true, true, false, true}
			return f
		}, want: map[string]spec.CheckStatus{"read_only:public.orders": spec.CheckFail},
			fix: map[string]string{"read_only:public.orders": "REVOKE INSERT, DELETE ON public.orders FROM piperoom_ro;"}},
		{name: "missing table", sess: goodPG, v: func(v spec.Values) { v.Config["tables"] = "public.nope" },
			want: map[string]spec.CheckStatus{"select:public.nope": spec.CheckFail, "read_only:public.nope": spec.CheckSkipped}},
		{name: "no tables", sess: goodPG, v: func(v spec.Values) { delete(v.Config, "tables") },
			want: map[string]spec.CheckStatus{"tables": spec.CheckSkipped}},
		{name: "no timeout, not read-only", sess: func() *fakePG { f := goodPG(); f.timeout, f.ro = "0", "off"; return f },
			want: map[string]spec.CheckStatus{"timeout": spec.CheckWarn}, fix: map[string]string{"timeout": "statement_timeout = '10s'"}},
		{name: "tls off", sess: func() *fakePG { f := goodPG(); f.tls = false; return f }, v: func(v spec.Values) { v.Config["sslmode"] = "disable" },
			want: map[string]spec.CheckStatus{"tls": spec.CheckWarn, "login": spec.CheckPass}},
		{name: "server without tls", sess: func() *fakePG { f := goodPG(); f.tls = false; return f },
			want: map[string]spec.CheckStatus{"tls": spec.CheckFail}},
		{name: "bad password", connErr: &pgconn.PgError{Code: "28P01", Message: "password authentication failed"},
			want: map[string]spec.CheckStatus{"tls": spec.CheckPass, "login": spec.CheckFail}, fix: map[string]string{"login": "ALTER ROLE piperoom_ro PASSWORD"}},
		{name: "no database", connErr: &pgconn.PgError{Code: "3D000"}, want: map[string]spec.CheckStatus{"login": spec.CheckFail}},
		{name: "login timeout", connErr: context.DeadlineExceeded, want: map[string]spec.CheckStatus{"login": spec.CheckFail}},
		{name: "login error echoing the password", connErr: errors.New("dial failed for postgres://u:" + canary + "@h"),
			want: map[string]spec.CheckStatus{"login": spec.CheckFail}},
		{name: "queries time out", sess: func() *fakePG { f := goodPG(); f.block = true; return f },
			want: map[string]spec.CheckStatus{"login": spec.CheckPass, "version": spec.CheckFail, "primary": spec.CheckFail, "not_superuser": spec.CheckFail}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var sess *fakePG
			if tc.sess != nil {
				sess = tc.sess()
			}
			d := pgDeps(sess, tc.connErr)
			v := pgValues(port)
			if tc.v != nil {
				tc.v(v)
			}
			checks := d.postgresKinds()[0].Test(context.Background(), v)
			for id, st := range tc.want {
				c := wantStatus(t, checks, id, st)
				if f := tc.fix[id]; f != "" && !strings.Contains(c.Fix, f) {
					t.Fatalf("check %s fix = %q, want it to contain %q", id, c.Fix, f)
				}
				if st == spec.CheckFail && c.Fix == "" && id != "version" && id != "primary" && id != "not_superuser" && !strings.Contains(c.Detail, "timed out") {
					// Every fail the user can act on carries a fix.
					t.Fatalf("check %s failed without a fix: %+v", id, c)
				}
			}
			noCanary(t, checks)
			if sess != nil && !sess.closed {
				t.Fatal("session not closed")
			}
		})
	}
}

func TestPostgresTimeoutDetail(t *testing.T) {
	_, port := listenTCP(t)
	f := goodPG()
	f.block = true
	checks := pgDeps(f, nil).postgresKinds()[0].Test(context.Background(), pgValues(port))
	if c := checkByID(t, checks, "version"); !strings.Contains(c.Detail, "timed out") {
		t.Fatalf("detail = %q", c.Detail)
	}
}

func TestPostgresUnreachable(t *testing.T) {
	d := pgDeps(goodPG(), nil)
	checks := d.postgresKinds()[0].Test(context.Background(), pgValues(closedPort(t)))
	wantStatus(t, checks, "reach", spec.CheckFail)
	wantStatus(t, checks, "login", spec.CheckSkipped)
	if c := checkByID(t, checks, "reach"); c.Fix == "" {
		t.Fatal("reach failure without a fix")
	}
}

func TestPostgresMetadataHostBlocked(t *testing.T) {
	d := pgDeps(goodPG(), nil)
	v := pgValues("80")
	v.Config["host"] = "169.254.169.254"
	checks := d.postgresKinds()[0].Test(context.Background(), v)
	c := wantStatus(t, checks, "reach", spec.CheckFail)
	if !strings.Contains(c.Detail, "metadata") {
		t.Fatalf("detail = %q", c.Detail)
	}
}

func TestPostgresBadInput(t *testing.T) {
	for name, mut := range map[string]func(spec.Values){
		"bad port":    func(v spec.Values) { v.Config["port"] = "70000" },
		"bad sslmode": func(v spec.Values) { v.Config["sslmode"] = "require" },
		"bad table":   func(v spec.Values) { v.Config["tables"] = "public.orders; DROP TABLE x" },
		"ca with disable": func(v spec.Values) {
			v.Config["sslmode"] = "disable"
			v.Secrets["ca_cert"] = "pem"
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := pgValues("5432")
			mut(v)
			checks := pgDeps(goodPG(), nil).postgresKinds()[0].Test(context.Background(), v)
			wantStatus(t, checks, "input", spec.CheckFail)
		})
	}
}

func TestPostgresWarehouseHasNoSourceChecks(t *testing.T) {
	_, port := listenTCP(t)
	v := pgValues(port)
	checks := pgDeps(goodPG(), nil).postgresKinds()[1].Test(context.Background(), v)
	for _, c := range checks {
		if c.ID == "version" || c.ID == "slots_visible" || c.ID == "primary" {
			t.Fatalf("warehouse ran source check %s", c.ID)
		}
	}
	wantStatus(t, checks, "read_only:public.orders", spec.CheckPass)
}

func TestPostgresCAUnsupportedFails(t *testing.T) {
	_, port := listenTCP(t)
	v := pgValues(port)
	v.Secrets["ca_cert"] = "-----BEGIN CERTIFICATE-----"
	checks := pgDeps(goodPG(), nil).postgresKinds()[0].Test(context.Background(), v)
	wantStatus(t, checks, "tls_ca", spec.CheckFail)
	_, _, err := pgDeps(goodPG(), nil).postgresKinds()[0].Build(context.Background(), v)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("build err = %v", err)
	}
}

func TestPostgresBuildRefusesMetadataHost(t *testing.T) {
	v := pgValues("5432")
	v.Config["host"] = "169.254.169.254"
	_, _, err := fastDeps().withDefaults().postgresKinds()[0].Build(context.Background(), v)
	if !errors.Is(err, ErrBlockedHost) {
		t.Fatalf("err = %v", err)
	}
}

func TestPostgresBuildBadHost(t *testing.T) {
	// Nothing listens there: postgres.New fails its ping, and the error must
	// not carry the password even though the URL did.
	v := pgValues(closedPort(t))
	v.Config["sslmode"] = "disable"
	_, _, err := fastDeps().withDefaults().postgresKinds()[0].Build(context.Background(), v)
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), canary) {
		t.Fatalf("error leaks the password: %v", err)
	}
}

func TestPGURLEscapes(t *testing.T) {
	u := pgURL(pgTarget{Host: "db", Port: 5432, Database: "orders", User: "ro", Password: "p@ss/w:rd?#", SSLMode: "verify-full"})
	cfg, err := pgx.ParseConfig(u)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Password != "p@ss/w:rd?#" || cfg.Host != "db" || cfg.Database != "orders" {
		t.Fatalf("parsed %+v", cfg.Config)
	}
}

func TestPostgresSetupScript(t *testing.T) {
	script := pgSetupScript(true)(vals(map[string]string{"user": "piperoom_ro", "database": "orders", "tables": "public.orders, sales.items"}, nil))
	for _, want := range []string{
		"CREATE ROLE piperoom_ro LOGIN", "NOSUPERUSER", "default_transaction_read_only = on",
		"GRANT CONNECT ON DATABASE orders TO piperoom_ro;", "GRANT USAGE ON SCHEMA sales TO piperoom_ro;",
		"GRANT SELECT ON public.orders TO piperoom_ro;", "GRANT SELECT ON sales.items TO piperoom_ro;",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script lacks %q:\n%s", want, script)
		}
	}
	// Unsafe names become placeholders, never SQL.
	bad := pgSetupScript(false)(vals(map[string]string{"user": "x; DROP ROLE admin", "database": "a'b", "tables": "t; drop"}, nil))
	if strings.Contains(bad, "DROP") || strings.Contains(bad, "a'b") {
		t.Fatalf("unsafe input reached the script:\n%s", bad)
	}
}

func TestPGTLSConfig(t *testing.T) {
	c, err := pgTarget{Host: "db", SSLMode: sslDisable}.tlsConfig()
	if c != nil || err != nil {
		t.Fatalf("disable = %v %v", c, err)
	}
	c, err = pgTarget{Host: "db.corp", SSLMode: sslVerifyFull}.tlsConfig()
	if err != nil || c.ServerName != "db.corp" || c.InsecureSkipVerify || c.VerifyConnection != nil {
		t.Fatalf("verify-full = %+v %v", c, err)
	}
	c, err = pgTarget{Host: "db.corp", SSLMode: sslVerifyCA}.tlsConfig()
	if err != nil || c.VerifyConnection == nil {
		t.Fatalf("verify-ca = %+v %v", c, err)
	}
	if err := c.VerifyConnection(tls.ConnectionState{}); err == nil {
		t.Fatal("verify-ca accepted no certificate")
	}
	if _, err := (pgTarget{SSLMode: sslVerifyFull, TLS: tlsMaterial{CA: "not pem"}}).tlsConfig(); err == nil {
		t.Fatal("bad CA accepted")
	}
}

func TestRealPGConnectErrors(t *testing.T) {
	g, _ := NewGuard("", nil)
	connect := realPGConnect(g)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tg := pgTarget{Host: "127.0.0.1", Port: 1, Database: "x", User: "u", Password: canary, SSLMode: sslDisable}
	if p, err := strconv.Atoi(closedPort(t)); err == nil {
		tg.Port = uint16(p)
	}
	_, err := connect(ctx, tg)
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Fatalf("err = %v", err)
	}
	tg.Host = "169.254.169.254"
	if _, err := connect(ctx, tg); !errors.Is(err, ErrBlockedHost) {
		t.Fatalf("metadata err = %v", err)
	}
}

func TestPGVersionText(t *testing.T) {
	for num, want := range map[int]string{120005: "12.5", 130000: "13.0", 90624: "9.6.24", 100023: "10.23"} {
		if got := pgVersionText(num); got != want {
			t.Errorf("pgVersionText(%d) = %q, want %q", num, got, want)
		}
	}
}

func TestSQLTableQuotesMixedCase(t *testing.T) {
	for in, want := range map[string]string{
		"public.orders": "public.orders", "public.Orders": `public."Orders"`, "Sales.items": `"Sales".items`,
	} {
		if got := sqlTable(in); got != want {
			t.Errorf("sqlTable(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPGTableFixQuotesMixedCase(t *testing.T) {
	ctx := context.Background()
	f := &fakePG{tables: map[string][4]bool{`"public"."Orders"`: {false, true, false, false}}}
	cl := newChecklist(ctx, time.Second, spec.Values{})
	pgTableChecks(cl, f, "public.Orders", "piperoom_ro")
	sel := wantStatus(t, cl.result(), "select:public.Orders", spec.CheckFail)
	if !strings.Contains(sel.Fix, `GRANT SELECT ON public."Orders" TO piperoom_ro;`) {
		t.Fatalf("select fix = %q", sel.Fix)
	}
	f.tables[`"public"."Orders"`] = [4]bool{true, true, false, false}
	cl = newChecklist(ctx, time.Second, spec.Values{})
	pgTableChecks(cl, f, "public.Orders", "piperoom_ro")
	ro := wantStatus(t, cl.result(), "read_only:public.Orders", spec.CheckFail)
	if ro.Fix != `REVOKE INSERT ON public."Orders" FROM piperoom_ro;` {
		t.Fatalf("revoke fix = %q", ro.Fix)
	}
}
