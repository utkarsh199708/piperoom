package wiring

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
)

// whFake is a distinct Warehouse type per kind, so a test can tell which
// opener produced the client.
type whFake struct{ kind string }

func (whFake) Freshness(context.Context, string, string) (time.Time, error) {
	return time.Time{}, nil
}
func (whFake) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return nil, false, nil
}

// fakeOpeners records which opener ran and the URL it was given.
func fakeOpeners(calls *[]string, urls *[]string) warehouseOpeners {
	mk := func(kind string) warehouseOpener {
		return func(_ context.Context, url string) (clients.Warehouse, func(), error) {
			*calls = append(*calls, kind)
			*urls = append(*urls, url)
			return whFake{kind: kind}, func() { *calls = append(*calls, "close-"+kind) }, nil
		}
	}
	return warehouseOpeners{postgres: mk("postgres"), snowflake: mk("snowflake"), bigquery: mk("bigquery"), databricks: mk("databricks"), clickhouse: mk("clickhouse")}
}

func TestOpenWarehousePicksClientByKind(t *testing.T) {
	cases := []struct {
		kind string
		want string
	}{
		{"", "postgres"}, // unset keeps today's behaviour
		{"postgres", "postgres"},
		{"snowflake", "snowflake"},
		{"Snowflake", "snowflake"},
		{"  SNOWFLAKE ", "snowflake"},
	}
	for _, tc := range cases {
		t.Run("kind="+tc.kind, func(t *testing.T) {
			var calls, urls []string
			cfg := config.Config{WarehouseKind: tc.kind, WarehouseDBURL: "the-url"}
			wh, closeFn, err := openWarehouse(context.Background(), cfg, fakeOpeners(&calls, &urls))
			if err != nil {
				t.Fatalf("openWarehouse: %v", err)
			}
			if got := wh.(whFake).kind; got != tc.want {
				t.Errorf("client kind = %q, want %q", got, tc.want)
			}
			if len(calls) != 1 || len(urls) != 1 || urls[0] != "the-url" {
				t.Errorf("calls=%v urls=%v, want exactly one opener given WAREHOUSE_DB_URL", calls, urls)
			}
			closeFn()
			if calls[len(calls)-1] != "close-"+tc.want {
				t.Errorf("closer did not close the %s client: %v", tc.want, calls)
			}
		})
	}
}

func TestOpenWarehouseRejectsUnknownKind(t *testing.T) {
	for _, kind := range []string{"oracle", "mysql", "postgresql", "snow flake", "redshift"} {
		var calls, urls []string
		_, _, err := openWarehouse(context.Background(), config.Config{WarehouseKind: kind}, fakeOpeners(&calls, &urls))
		if err == nil {
			t.Fatalf("kind %q: want error, got nil", kind)
		}
		msg := err.Error()
		for _, want := range append([]string{"WAREHOUSE_KIND", kind}, warehouseKinds...) {
			if !strings.Contains(msg, want) {
				t.Errorf("kind %q: error %q does not mention %q", kind, msg, want)
			}
		}
		if len(calls) != 0 {
			t.Errorf("kind %q: an opener ran: %v", kind, calls)
		}
	}
}

func TestOpenRejectsUnknownWarehouseKindBeforeAnyClient(t *testing.T) {
	ownersFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(ownersFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write owners file: %v", err)
	}
	cfg := devA2Config()
	cfg.OwnersFile = ownersFile
	cfg.WarehouseKind = "oracle"
	_, err := Open(context.Background(), cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "unknown WAREHOUSE_KIND") {
		t.Fatalf("Open error = %v, want the unknown WAREHOUSE_KIND error (before any database ping)", err)
	}
}

func TestOpenWarehouseSnowflakeErrorIsRedactedAndNamed(t *testing.T) {
	const dsn = "svc:Sup3rS3cretPw@acme-xy12345/ANALYTICS/PUBLIC?warehouse=WH&role=PIPEROOM_RO"
	sentinel := errors.New("login refused")
	o := warehouseOpeners{
		snowflake: func(_ context.Context, url string) (clients.Warehouse, func(), error) {
			// A driver that echoes the whole DSN back in its error.
			return nil, nil, errors.New("open " + url + ": " + sentinel.Error())
		},
	}
	_, _, err := openWarehouse(context.Background(), config.Config{WarehouseKind: "snowflake", WarehouseDBURL: dsn}, o)
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.HasPrefix(err.Error(), "wiring: warehouse (snowflake): ") {
		t.Errorf("error = %q, want the %q prefix", err, "wiring: warehouse (snowflake): ")
	}
	if strings.Contains(err.Error(), "Sup3rS3cretPw") || strings.Contains(err.Error(), "acme-xy12345") {
		t.Errorf("error leaks the DSN: %q", err)
	}
	if !strings.Contains(err.Error(), sentinel.Error()) {
		t.Errorf("error lost the cause: %q", err)
	}
}

// TestOpenWarehouseRealSnowflakeBadDSN runs the real Snowflake opener on a
// DSN the driver rejects before any network call.
func TestOpenWarehouseRealSnowflakeBadDSN(t *testing.T) {
	const dsn = "svc:Sup3rS3cretPw@"
	_, _, err := openWarehouse(context.Background(), config.Config{WarehouseKind: "snowflake", WarehouseDBURL: dsn}, defaultWarehouseOpeners)
	if err == nil {
		t.Fatal("want error for a DSN with no account")
	}
	if !strings.HasPrefix(err.Error(), "wiring: warehouse (snowflake): ") {
		t.Errorf("error = %q", err)
	}
	if strings.Contains(err.Error(), "Sup3rS3cretPw") {
		t.Errorf("error leaks the password: %q", err)
	}
}

func TestOpenWarehousePostgresErrorKeepsOldWrap(t *testing.T) {
	boom := errors.New("boom")
	o := warehouseOpeners{postgres: func(context.Context, string) (clients.Warehouse, func(), error) {
		return nil, nil, boom
	}}
	_, _, err := openWarehouse(context.Background(), config.Config{WarehouseKind: "postgres"}, o)
	if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "wiring: warehouse: ") {
		t.Fatalf("error = %v, want the unchanged %q wrap", err, "wiring: warehouse: ")
	}
}
