package snowflake

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strconv"
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
	t.Run("lowest key_sequence wins", func(t *testing.T) {
		q := &fakeQuerier{rows: &fakeRows{cols: cols, data: [][]any{
			{"t", "DB", "ANALYTICS", "ORDERS", "TENANT_ID", "2", "pk"},
			{"t", "DB", "ANALYTICS", "ORDERS", "ORDER_ID", "1", "pk"},
		}}}
		d := &DB{q: q}
		got, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders")
		if err != nil || got != "ORDER_ID" {
			t.Fatalf("got %q, %v; want ORDER_ID", got, err)
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

// protocol=http would send the login, the password or JWT and the session
// token in clear text; only https (the default) is accepted.
func TestHardenedConfigRefusesPlainHTTP(t *testing.T) {
	for _, proto := range []string{"http", "HTTP", "ftp", "ws"} {
		t.Run(proto, func(t *testing.T) {
			cfg, err := hardenedConfig("user:s3cretpw@acct/db/sch?warehouse=wh&protocol=" + proto)
			if err == nil || !strings.Contains(err.Error(), "protocol") {
				t.Fatalf("hardenedConfig = %v, %v; want a protocol refusal", cfg, err)
			}
			if strings.Contains(err.Error(), "s3cretpw") {
				t.Errorf("error leaks the password: %v", err)
			}
		})
	}
	for _, q := range []string{"", "&protocol=https", "&protocol=HTTPS"} {
		if _, err := hardenedConfig("user:pw@acct/db/sch?warehouse=wh" + q); err != nil {
			t.Errorf("hardenedConfig(%q) = %v, want ok", q, err)
		}
	}
	_, err := New(context.Background(), "user:s3cretpw@acct/db/sch?warehouse=wh&protocol=http")
	if err == nil || !strings.Contains(err.Error(), "protocol") || strings.Contains(err.Error(), "s3cretpw") {
		t.Fatalf("New err = %v, want a protocol refusal without the password", err)
	}
}

// An explicit certificate revocation (CRL) mode is the operator's choice:
// the OCSP opt-in must leave it alone.
func TestHardenedConfigCRLModeKept(t *testing.T) {
	for _, mode := range []string{"ENABLED", "ADVISORY"} {
		cfg, err := hardenedConfig("user:pw@acct/db/sch?warehouse=wh&certRevocationCheckMode=" + mode)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.OCSPFailOpen != ocspNotSet {
			t.Errorf("%s: OCSPFailOpen = %v, want unset (CRL mode chosen)", mode, cfg.OCSPFailOpen)
		}
		if cfg.CertRevocationCheckMode == gosnowflake.CertRevocationCheckDisabled {
			t.Errorf("%s: CRL mode was reset to disabled", mode)
		}
	}
}

// disableOCSPChecks=true is allowed (operator trust) but must be loud, and
// the warning must not carry the DSN.
func TestHardenedConfigWarnsOnDisabledOCSP(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	cfg, err := hardenedConfig("user:s3cretpw@acct/db/sch?warehouse=wh&disableOCSPChecks=true")
	if err != nil || !cfg.DisableOCSPChecks {
		t.Fatalf("hardenedConfig = %v, %v; want it accepted", cfg, err)
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "disableOCSPChecks") {
		t.Errorf("log = %q, want a warning naming disableOCSPChecks", out)
	}
	if strings.Contains(out, "s3cretpw") || strings.Contains(out, "acct") {
		t.Errorf("warning leaks the DSN: %q", out)
	}
	buf.Reset()
	if _, err := hardenedConfig("user:pw@acct/db/sch?warehouse=wh"); err != nil || buf.Len() != 0 {
		t.Errorf("default DSN logged %q (err %v), want silence", buf.String(), err)
	}
}

// pkShow is a SHOW PRIMARY KEYS result with one row per name, in key order.
func pkShow(names ...string) *fakeRows {
	r := &fakeRows{cols: []string{"created_on", "database_name", "schema_name", "table_name", "column_name", "key_sequence", "constraint_name"}}
	for i, n := range names {
		r.data = append(r.data, []any{"t", "DB", "ANALYTICS", "ORDERS", n, strconv.Itoa(i + 1), "pk"})
	}
	return r
}

// A quoted lower-case "id" key is stored as id, an unquoted one as ID; the
// client must report and read exactly that column (#463).
func TestPrimaryKeyColumnKeepsStoredCase(t *testing.T) {
	for _, name := range []string{"id", "ID", "Order_Id"} {
		d := &DB{q: &fakeQuerier{rows: pkShow(name)}}
		got, err := d.PrimaryKeyColumn(context.Background(), "analytics.orders")
		if err != nil || got != name {
			t.Errorf("PrimaryKeyColumn = %q, %v; want %q", got, err, name)
		}
	}
}

func TestRowQuotesStoredPKName(t *testing.T) {
	data := &fakeRows{cols: []string{"id", "ID", "AMOUNT"}, data: [][]any{{"lower", "upper", "1"}}}
	cases := []struct {
		name    string
		stored  []string
		pk      string
		wantCol string
	}{
		{"quoted lower-case key", []string{"id"}, "id", `"id"`},
		{"unquoted key, lower-case request", []string{"ID"}, "id", `"ID"`},
		{"unquoted key, exact request", []string{"ID"}, "ID", `"ID"`},
		{"quoted key, upper-case request", []string{"id"}, "ID", `"id"`},
		{"both columns, key is ID", []string{"ID"}, "ID", `"ID"`},
		{"both columns, key is id", []string{"id"}, "id", `"id"`},
		{"no declared key keeps the legacy fold", nil, "id", `"ID"`},
		{"other column keeps the legacy fold", []string{"ORDER_ID"}, "id", `"ID"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fq := &fakeQuerier{rows: data, showRows: pkShow(tc.stored...)}
			d := &DB{q: fq}
			if _, _, err := d.Row(context.Background(), "analytics.orders", tc.pk, "1"); err != nil {
				t.Fatal(err)
			}
			want := `SELECT * FROM "ANALYTICS"."ORDERS" WHERE ` + tc.wantCol + ` = ? LIMIT 1`
			if fq.lastQuery != want {
				t.Errorf("query = %q, want %q", fq.lastQuery, want)
			}
		})
	}
}

// A table with both "id" and ID must not lose a column to the lower-casing
// of result names.
func TestRowKeepsBothIdColumns(t *testing.T) {
	data := &fakeRows{cols: []string{"id", "ID", "AMOUNT"}, data: [][]any{{"lower", "upper", "1"}}}
	d := &DB{q: &fakeQuerier{rows: data, showRows: pkShow("ID")}}
	row, found, err := d.Row(context.Background(), "analytics.orders", "ID", "1")
	if err != nil || !found {
		t.Fatalf("Row = %v, %v, %v", row, found, err)
	}
	if row["amount"] != "1" || len(row) != 3 {
		t.Fatalf("row = %v, want three distinct keys with amount lower-cased", row)
	}
	if row["id"] == row["ID"] {
		t.Errorf("columns id and ID collapsed: %v", row)
	}
}

// An OAuth token or authorization URL that is not https would send the client
// secret and the access token in clear text (#496): each setting must be an
// absolute https URL without userinfo, and the refusal names the setting but
// never the URL, the client secret or the DSN password.
func TestHardenedConfigOAuthURLsMustBeHTTPS(t *testing.T) {
	const base = "user:s3cretpw@acct/db/sch?warehouse=wh&authenticator=OAUTH_CLIENT_CREDENTIALS&oauthClientId=c&oauthClientSecret=cl1entsecret"
	secrets := []string{"s3cretpw", "cl1entsecret", "us3rinfopw", "idp.example", "acct"}
	for _, param := range []string{"oauthTokenRequestUrl", "oauthAuthorizationUrl"} {
		for _, tc := range []struct{ name, value, wantMsg string }{
			{"http", "http://idp.example/oauth", "https"},
			{"HTTP upper-case", "HTTP://idp.example/oauth", "https"},
			{"ftp", "ftp://idp.example/oauth", "https"},
			{"no scheme", "idp.example/oauth", "https"},
			{"relative path", "/oauth/token", "https"},
			{"https without host", "https:///oauth", "https"},
			{"unparseable", "https://idp.example/%zz", "https"},
			{"http with userinfo", "http://c:us3rinfopw@idp.example/oauth", "https"},
			{"https with userinfo", "https://c:us3rinfopw@idp.example/oauth", "credentials"},
			{"scheme-relative", "//idp.example/oauth", "https"},
			{"leading space https", " https://idp.example/oauth", "https"},
			{"leading space http", " http://idp.example/oauth", "https"},
		} {
			t.Run(param+"/"+tc.name, func(t *testing.T) {
				dsn := base + "&" + param + "=" + url.QueryEscape(tc.value)
				// The parsed config holds the DSN password: never print it.
				_, err := hardenedConfig(dsn)
				if err == nil {
					t.Fatalf("hardenedConfig accepted %s=%q; want it refused", param, tc.value)
				}
				msg := err.Error()
				if !strings.Contains(msg, param) || !strings.Contains(msg, tc.wantMsg) {
					t.Errorf("err = %q; want it to name %s and %q", msg, param, tc.wantMsg)
				}
				for _, s := range secrets {
					if strings.Contains(msg, s) {
						t.Errorf("err = %q leaks %q", msg, s)
					}
				}
				_, err = New(context.Background(), dsn)
				if err == nil || !strings.Contains(err.Error(), param) {
					t.Fatalf("New err = %v; want the %s refusal", err, param)
				}
				for _, s := range secrets {
					if strings.Contains(err.Error(), s) {
						t.Errorf("New err = %q leaks %q", err, s)
					}
				}
			})
		}
		t.Run(param+"/HTTPS upper-case accepted", func(t *testing.T) {
			cfg, err := hardenedConfig(base + "&" + param + "=" + url.QueryEscape("HTTPS://idp.example/oauth"))
			if err != nil {
				t.Fatalf("hardenedConfig = %v; want HTTPS accepted", err)
			}
			got := cfg.OauthTokenRequestURL
			if param == "oauthAuthorizationUrl" {
				got = cfg.OauthAuthorizationURL
			}
			if !strings.EqualFold(got, "https://idp.example/oauth") {
				t.Errorf("%s = %q; want the value kept", param, got)
			}
		})
	}
	// Both empty: the driver derives the endpoints from the https account
	// host, so the default must stay accepted and unchanged.
	cfg, err := hardenedConfig(base)
	if err != nil {
		t.Fatalf("hardenedConfig = %v; want the default accepted", err)
	}
	if cfg.OauthTokenRequestURL != "" || cfg.OauthAuthorizationURL != "" {
		t.Errorf("urls = %q, %q; want both left empty", cfg.OauthTokenRequestURL, cfg.OauthAuthorizationURL)
	}
	// Both https and set together must pass; one bad one among them must not.
	ok := "&oauthTokenRequestUrl=" + url.QueryEscape("https://idp.example/token") + "&oauthAuthorizationUrl=" + url.QueryEscape("https://idp.example/auth")
	if _, err := hardenedConfig(base + ok); err != nil {
		t.Errorf("hardenedConfig(both https) = %v; want ok", err)
	}
	mixed := "&oauthTokenRequestUrl=" + url.QueryEscape("https://idp.example/token") + "&oauthAuthorizationUrl=" + url.QueryEscape("http://idp.example/auth")
	if _, err := hardenedConfig(base + mixed); err == nil || !strings.Contains(err.Error(), "oauthAuthorizationUrl") {
		t.Errorf("hardenedConfig(mixed) = %v; want oauthAuthorizationUrl refused", err)
	}
}

// workloadIdentityHost is the AWS STS endpoint for workload identity: its
// calls carry the signed session token and return temporary credentials or
// the JWT used to log in. gosnowflake accepts http there; hardenedConfig must
// not. A bare host is what the driver expects (it prefixes https://).
func TestHardenedConfigWorkloadIdentityHostMustBeHTTPS(t *testing.T) {
	const base = "user:s3cretpw@acct/db/sch?warehouse=wh&authenticator=WORKLOAD_IDENTITY&workloadIdentityProvider=AWS"
	secrets := []string{"s3cretpw", "us3rinfopw", "sts.example", "acct"}
	for _, tc := range []struct{ name, value string }{
		{"http", "http://sts.example"},
		{"HTTP upper-case", "HTTP://sts.example"},
		{"http with spaces", "  http://sts.example  "},
		{"http with port and path", "http://sts.example:8080/sts"},
		{"ftp", "ftp://sts.example"},
		{"scheme without host", "https://"},
		{"https with userinfo", "https://k:us3rinfopw@sts.example"},
		{"spaces only", "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := base + "&workloadIdentityHost=" + url.QueryEscape(tc.value)
			// The parsed config holds the DSN password: never print it.
			_, err := hardenedConfig(dsn)
			if err == nil {
				t.Fatalf("hardenedConfig accepted workloadIdentityHost=%q; want it refused", tc.value)
			}
			_, nerr := New(context.Background(), dsn)
			for _, e := range []error{err, nerr} {
				if e == nil || !strings.Contains(e.Error(), "workloadIdentityHost") {
					t.Fatalf("err = %v; want it to name workloadIdentityHost", e)
				}
				for _, s := range secrets {
					if strings.Contains(e.Error(), s) {
						t.Errorf("err = %q leaks %q", e, s)
					}
				}
			}
		})
	}
	for _, tc := range []struct{ name, value string }{
		{"https", "https://sts.example"},
		{"HTTPS upper-case", "HTTPS://sts.example"},
		{"bare host", "sts.example"},
		{"bare host with port and spaces", " sts.example:8443 "},
		{"empty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := hardenedConfig(base + "&workloadIdentityHost=" + url.QueryEscape(tc.value))
			if err != nil {
				t.Fatalf("hardenedConfig = %v; want %q accepted", err, tc.value)
			}
			if cfg.WorkloadIdentityHost != tc.value {
				t.Errorf("WorkloadIdentityHost = %q; want %q kept as written", cfg.WorkloadIdentityHost, tc.value)
			}
		})
	}
}
