package wiring

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/config"
)

// configWarehouseKinds reads the WAREHOUSE_KIND values config.go documents.
// config exposes no list of its own (Load accepts any string and leaves the
// check to wiring), so the field's comment in internal/config/config.go,
// "// WAREHOUSE_KIND: postgres | snowflake | ...", is the accepted set. Reading
// it here makes a kind added there without a reader here fail this test.
func configWarehouseKinds(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "config", "config.go"))
	if err != nil {
		t.Fatalf("read config.go: %v", err)
	}
	const marker = "// WAREHOUSE_KIND:"
	for _, line := range strings.Split(string(b), "\n") {
		_, after, ok := strings.Cut(line, marker)
		if !ok {
			continue
		}
		var kinds []string
		for _, k := range strings.Split(after, "|") {
			if k = strings.TrimSpace(k); k != "" {
				kinds = append(kinds, k)
			}
		}
		if len(kinds) == 0 {
			t.Fatalf("config.go %q comment lists no kinds", marker)
		}
		return kinds
	}
	t.Fatalf("config.go has no %q comment; update this test to the new source of the accepted set", marker)
	return nil
}

func TestWarehouseKindsMatchConfig(t *testing.T) {
	got := slices.Sorted(slices.Values(warehouseKinds))
	want := slices.Sorted(slices.Values(configWarehouseKinds(t)))
	if !slices.Equal(got, want) {
		t.Fatalf("wiring kinds %v, config.go WAREHOUSE_KIND kinds %v: every kind config accepts needs a reader here", got, want)
	}
	if warehouseKinds[0] != WarehousePostgres {
		t.Errorf("warehouseKinds[0] = %q, want the default %q first", warehouseKinds[0], WarehousePostgres)
	}
}

// TestOpenWarehouseEveryConfigKind: each kind config accepts selects its own
// opener, hands it WAREHOUSE_DB_URL unchanged, and gets that opener's closer.
func TestOpenWarehouseEveryConfigKind(t *testing.T) {
	for _, kind := range configWarehouseKinds(t) {
		for _, spelled := range []string{kind, strings.ToUpper(kind), " " + kind + " "} {
			t.Run(spelled, func(t *testing.T) {
				var calls, urls []string
				cfg := config.Config{WarehouseKind: spelled, WarehouseDBURL: "dsn-for-" + kind}
				if got, err := warehouseKind(cfg); err != nil || got != kind {
					t.Fatalf("warehouseKind = %q, %v; want %q", got, err, kind)
				}
				wh, closeFn, err := openWarehouse(context.Background(), cfg, fakeOpeners(&calls, &urls))
				if err != nil {
					t.Fatalf("openWarehouse: %v", err)
				}
				if got := wh.(whFake).kind; got != kind {
					t.Errorf("opened %q, want %q", got, kind)
				}
				if len(urls) != 1 || urls[0] != "dsn-for-"+kind {
					t.Errorf("urls = %v, want exactly WAREHOUSE_DB_URL", urls)
				}
				closeFn()
				if calls[len(calls)-1] != "close-"+kind {
					t.Errorf("closer did not close the %s client: %v", kind, calls)
				}
			})
		}
	}
}

// TestOpenWarehouseMissingOpener: a kind whose opener is not set fails with a
// named error instead of a nil call.
func TestOpenWarehouseMissingOpener(t *testing.T) {
	for _, kind := range warehouseKinds {
		_, _, err := openWarehouse(context.Background(), config.Config{WarehouseKind: kind}, warehouseOpeners{})
		if err == nil || !strings.Contains(err.Error(), "no opener") {
			t.Errorf("kind %q: err = %v, want a no-opener error", kind, err)
		}
	}
}

// TestOpenWarehouseErrorsAreRedactedAndNamed: a driver that echoes its whole
// DSN back still yields an error that names the kind, keeps the cause, and
// holds neither the DSN nor its secret.
func TestOpenWarehouseErrorsAreRedactedAndNamed(t *testing.T) {
	cases := []struct {
		kind, dsn string
		secrets   []string
	}{
		{WarehouseSnowflake, "svc:Sup3rS3cretPw@acme-xy12345/ANALYTICS/PUBLIC?warehouse=WH", []string{"Sup3rS3cretPw"}},
		{WarehouseDatabricks, "token:dapiSup3rS3cretTok@dbc-1234abcd-5678.cloud.databricks.com:443/sql/1.0/warehouses/abc?catalog=main", []string{"dapiSup3rS3cretTok"}},
		{WarehouseClickHouse, "clickhouse://piperoom:Sup3rS3cretPw@clickhouse.example:9000/analytics?secure=true", []string{"Sup3rS3cretPw"}},
		// The bigquery DSN holds no secret, but it is still not repeated.
		{WarehouseBigQuery, "bigquery://acme-data/us?billing_project=acme-billing", []string{"bigquery://acme-data/us?billing_project=acme-billing"}},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			cause := errors.New("login refused")
			var echo warehouseOpener = func(_ context.Context, url string) (clients.Warehouse, func(), error) {
				return nil, nil, errors.Join(errors.New("open "+url), cause)
			}
			var calls, urls []string
			o := fakeOpeners(&calls, &urls)
			switch tc.kind {
			case WarehouseSnowflake:
				o.snowflake = echo
			case WarehouseDatabricks:
				o.databricks = echo
			case WarehouseClickHouse:
				o.clickhouse = echo
			case WarehouseBigQuery:
				o.bigquery = echo
			}
			_, _, err := openWarehouse(context.Background(), config.Config{WarehouseKind: tc.kind, WarehouseDBURL: tc.dsn}, o)
			if err == nil {
				t.Fatal("want error")
			}
			if prefix := "wiring: warehouse (" + tc.kind + "): "; !strings.HasPrefix(err.Error(), prefix) {
				t.Errorf("error = %q, want the %q prefix", err, prefix)
			}
			for _, s := range append([]string{tc.dsn}, tc.secrets...) {
				if strings.Contains(err.Error(), s) {
					t.Errorf("error leaks %q: %q", s, err)
				}
			}
			if !errors.Is(err, cause) || !strings.Contains(err.Error(), cause.Error()) {
				t.Errorf("error lost the cause: %q", err)
			}
		})
	}
}

// TestOpenWarehouseRealOpenersBadDSN runs the real bigquery, databricks and
// clickhouse openers on DSNs each client refuses before any network call:
// the error is named, and the secret in the DSN does not appear.
func TestOpenWarehouseRealOpenersBadDSN(t *testing.T) {
	cases := []struct {
		kind, dsn, secret string
	}{
		// Not a GCP project id: refused by bigquery.ParseDSN, before ADC is read.
		{WarehouseBigQuery, "bigquery://Not_A_Project/us", ""},
		// No catalog: refused by databricks.ParseDSN.
		{WarehouseDatabricks, "token:dapiSup3rS3cretTok@dbc-1234abcd-5678.cloud.databricks.com:443/sql/1.0/warehouses/abc", "dapiSup3rS3cretTok"},
		// A dial_timeout that is not a duration: refused by the driver's ParseDSN.
		{WarehouseClickHouse, "clickhouse://piperoom:Sup3rS3cretPw@127.0.0.1:1/analytics?dial_timeout=not-a-duration", "Sup3rS3cretPw"},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			_, _, err := openWarehouse(context.Background(), config.Config{WarehouseKind: tc.kind, WarehouseDBURL: tc.dsn}, defaultWarehouseOpeners)
			if err == nil {
				t.Fatal("want error for a DSN the client refuses")
			}
			if prefix := "wiring: warehouse (" + tc.kind + "): "; !strings.HasPrefix(err.Error(), prefix) {
				t.Errorf("error = %q, want the %q prefix", err, prefix)
			}
			if strings.Contains(err.Error(), "unknown WAREHOUSE_KIND") {
				t.Errorf("kind %q treated as unsupported: %v", tc.kind, err)
			}
			if tc.secret != "" && strings.Contains(err.Error(), tc.secret) {
				t.Errorf("error leaks the secret: %q", err)
			}
		})
	}
}

// TestLiteralOKEveryConfigKind: owners.yaml's warehouse_kind literal accepts
// exactly the kinds config accepts, in any case, and says so when it refuses.
func TestLiteralOKEveryConfigKind(t *testing.T) {
	for _, kind := range configWarehouseKinds(t) {
		for _, v := range []string{kind, strings.ToUpper(kind), " " + kind + " "} {
			if !literalOK("warehouse_kind", v) {
				t.Errorf("literalOK(warehouse_kind, %q) = false, want true", v)
			}
		}
		if !strings.Contains(literalWant["warehouse_kind"], kind) {
			t.Errorf("literalWant[warehouse_kind] = %q does not name %q", literalWant["warehouse_kind"], kind)
		}
	}
	for _, v := range []string{"oracle", "mysql", "postgresql", "big query", ""} {
		if literalOK("warehouse_kind", v) {
			t.Errorf("literalOK(warehouse_kind, %q) = true, want false", v)
		}
	}
}

// TestPipelineWarehouseKindLiteralOpensThatKind: a pipeline's own
// warehouse_kind literal reaches its set's settings for every kind config
// accepts, so openClients opens that kind's reader for it.
func TestPipelineWarehouseKindLiteralOpensThatKind(t *testing.T) {
	for _, kind := range configWarehouseKinds(t) {
		t.Run(kind, func(t *testing.T) {
			y := strings.Replace(twoTenantYAML(""), "      kafka_sasl_password: {env: B_KAFKA_PASSWORD}",
				"      kafka_sasl_password: {env: B_KAFKA_PASSWORD}\n      warehouse_kind: {value: "+kind+"}", 1)
			f := newFakeOpener()
			if _, err := openStacks(context.Background(), config.Config{}, nil, loadOwners(t, y), StackOptions{}, f.deps(env(twoTenantEnv()))); err != nil {
				t.Fatalf("openStacks: %v", err)
			}
			var found bool
			for _, c := range f.cfgs {
				if c.SourceDBURL != "src-b" {
					continue
				}
				found = true
				if got, err := warehouseKind(c); err != nil || got != kind {
					t.Errorf("pipeline pb warehouse kind = %q, %v; want %q", got, err, kind)
				}
				if c.WarehouseDBURL != "wh-b" {
					t.Errorf("pipeline pb warehouse URL = %q, want its own", c.WarehouseDBURL)
				}
			}
			if !found {
				t.Fatalf("pipeline pb's set was not opened: %d sets", len(f.cfgs))
			}
		})
	}
}
