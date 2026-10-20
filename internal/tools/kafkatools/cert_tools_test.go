package kafkatools

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

func certTool(t *testing.T, targets []CertTarget, probe func(context.Context, string, *tls.Config) (netsec.CertInfo, error)) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: &fakeKafka{}, CertTargets: targets, CertProbe: probe}); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get("tls_cert_expiry")
	if !ok || tool.Kind() != tools.KindRead {
		t.Fatal("tls_cert_expiry not registered as a read tool")
	}
	return tool
}

func TestCertExpiryToolSummary(t *testing.T) {
	var probed atomic.Int32
	probe := func(_ context.Context, addr string, _ *tls.Config) (netsec.CertInfo, error) {
		probed.Add(1)
		switch addr {
		case "kafka-1:9093":
			return netsec.CertInfo{Issuer: "R3", NotAfter: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), DaysLeft: 12, Verified: true}, nil
		case "connect:443":
			return netsec.CertInfo{Issuer: "Corp CA", NotAfter: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC), DaysLeft: -3}, nil
		}
		return netsec.CertInfo{}, errors.New("connection refused")
	}
	tool := certTool(t, []CertTarget{
		{Label: "broker", Addr: "kafka-1:9093", TLS: true},
		{Label: "connect", Addr: "connect:443", TLS: true},
		{Label: "schema registry", Addr: "sr:8081"},
		{Label: "warehouse", Addr: "acct.snowflakecomputing.com:443", TLS: true},
	}, probe)
	r, err := tool.Run(tools.WithUnscoped(context.Background()), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"broker kafka-1:9093 expires in 12 d (2026-10-26) issuer R3 ✔ chain ok",
		"connect connect:443 EXPIRED 3 d ago (2026-10-11) issuer Corp CA ✘ chain not trusted",
		"schema registry sr:8081: no TLS configured",
		"warehouse acct.snowflakecomputing.com:443: cannot read the certificate: connection refused",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("missing %q in:\n%s", want, r.Summary)
		}
	}
	if probed.Load() != 3 {
		t.Fatalf("plain-text endpoint was dialed or a TLS one skipped: %d probes", probed.Load())
	}
	if _, err := tool.Run(context.Background(), []byte(`{}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Errorf("no scope: %v", err)
	}
	if _, err := tool.Run(tools.WithUnscoped(context.Background()), []byte(`{"addr":"evil:1"}`)); err != nil {
		t.Errorf("unknown fields are ignored but must never become targets: %v", err)
	}
	if probed.Load() != 6 {
		t.Errorf("probes = %d: an input address was dialed", probed.Load())
	}
}

func TestCertExpiryOnlyConfiguredEndpoints(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: &fakeKafka{}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("tls_cert_expiry"); ok {
		t.Fatal("registered without configured endpoints")
	}
	cfg := config.Config{
		KafkaBrokers: "k1:9093, k2:9093", KafkaSecurityProtocol: "sasl_ssl",
		ConnectURL: "https://connect.example:8443", SchemaRegistryURL: "http://sr:8081",
		WarehouseKind: "snowflake", WarehouseDBURL: "user:s3cret@myacct/db/schema?warehouse=w",
	}
	var got []string
	for _, tg := range CertTargets(cfg) {
		got = append(got, tg.Label+" "+tg.Addr+" "+map[bool]string{true: "tls", false: "plain"}[tg.TLS])
	}
	want := "broker k1:9093 tls|broker k2:9093 tls|connect connect.example:8443 tls|schema registry sr:8081 plain|warehouse myacct.snowflakecomputing.com:443 tls"
	if strings.Join(got, "|") != want {
		t.Fatalf("targets:\n got %s\nwant %s", strings.Join(got, "|"), want)
	}
	if strings.Contains(strings.Join(got, ""), "s3cret") {
		t.Fatal("credential leaked into a target")
	}
	// Plaintext Kafka and a postgres warehouse: no TLS, no warehouse target.
	for _, tg := range CertTargets(config.Config{KafkaBrokers: "k:9092", WarehouseKind: "postgres", WarehouseDBURL: "postgres://u:p@h/db"}) {
		if tg.TLS || tg.Label == "warehouse" {
			t.Fatalf("unexpected target %+v", tg)
		}
	}
}

func TestWarehouseTargets(t *testing.T) {
	tests := []struct {
		name    string
		kind    string
		dsn     string
		secret  string // a credential substring that must never appear in the result
		want    string // host:port, or "" for no target
		wantTLS bool   // TLS flag when want != ""
		server  string // expected tls.Config.ServerName, or "" to skip the check
	}{
		{
			name:    "snowflake account",
			kind:    "snowflake",
			dsn:     "user:s3cret@myacct/db/schema?warehouse=w",
			secret:  "s3cret",
			want:    "myacct.snowflakecomputing.com:443",
			wantTLS: true,
		},
		{
			name:    "snowflake dotted account",
			kind:    "snowflake",
			dsn:     "user:s3cret@xy12345.eu-central-1.aws/db",
			secret:  "s3cret",
			want:    "xy12345.eu-central-1.aws:443",
			wantTLS: true,
		},
		{
			name: "snowflake bad dsn no host",
			kind: "snowflake",
			dsn:  "user:s3cret@/db",
			want: "",
		},
		{
			name:    "clickhouse native tls",
			kind:    "clickhouse",
			dsn:     "clickhouse://reader:s3cret@ch.example:9440/analytics?secure=true",
			secret:  "s3cret",
			want:    "ch.example:9440",
			wantTLS: true,
		},
		{
			name:    "clickhouse https",
			kind:    "clickhouse",
			dsn:     "https://reader:s3cret@ch.example:8443/analytics?secure=true",
			secret:  "s3cret",
			want:    "ch.example:8443",
			wantTLS: true,
		},
		{
			name:    "clickhouse native plaintext",
			kind:    "clickhouse",
			dsn:     "clickhouse://reader:s3cret@ch.example:9000/analytics",
			secret:  "s3cret",
			want:    "ch.example:9000",
			wantTLS: false,
		},
		{
			name:    "clickhouse http plaintext",
			kind:    "clickhouse",
			dsn:     "http://reader:s3cret@ch.example:8123/analytics",
			secret:  "s3cret",
			want:    "ch.example:8123",
			wantTLS: false,
		},
		{
			name:    "clickhouse secure empty value",
			kind:    "clickhouse",
			dsn:     "clickhouse://reader:s3cret@ch.example:9440/analytics?secure",
			secret:  "s3cret",
			want:    "ch.example:9440",
			wantTLS: true,
		},
		{
			name:    "clickhouse secure false",
			kind:    "clickhouse",
			dsn:     "clickhouse://reader:s3cret@ch.example:9440/analytics?secure=false",
			secret:  "s3cret",
			want:    "ch.example:9440",
			wantTLS: false,
		},
		{
			name:    "clickhouse tls server name",
			kind:    "clickhouse",
			dsn:     "clickhouse://reader:s3cret@ch.example:9440/analytics?secure=true&tls_server_name=ch.internal",
			secret:  "s3cret",
			want:    "ch.example:9440",
			wantTLS: true,
			server:  "ch.internal",
		},
		{
			name:    "clickhouse at in password",
			kind:    "clickhouse",
			dsn:     "clickhouse://reader:p@ss@ch.example:9440/db?secure=true",
			secret:  "p@ss",
			want:    "ch.example:9440",
			wantTLS: true,
		},
		{
			name:    "clickhouse ipv6",
			kind:    "clickhouse",
			dsn:     "clickhouse://reader:s3cret@[2001:db8::1]:9440/db?secure=true",
			secret:  "s3cret",
			want:    "[2001:db8::1]:9440",
			wantTLS: true,
		},
		{
			name:    "clickhouse multi-host takes first",
			kind:    "clickhouse",
			dsn:     "clickhouse://reader:s3cret@ch1.example:9440,ch2.example:9440/db?secure=true",
			secret:  "s3cret",
			want:    "ch1.example:9440",
			wantTLS: true,
		},
		{
			name:   "clickhouse credential slash in userinfo",
			kind:   "clickhouse",
			dsn:    "clickhouse://u:12345/x@ch.example:9440/db?secure=true",
			secret: "12345",
			want:   "",
		},
		{
			name: "clickhouse bad dsn no port",
			kind: "clickhouse",
			dsn:  "clickhouse://reader:s3cret@ch.example/analytics",
			want: "",
		},
		{
			name: "clickhouse bad dsn no host",
			kind: "clickhouse",
			dsn:  "clickhouse://:9440/analytics",
			want: "",
		},
		{
			name: "clickhouse bad dsn unparseable",
			kind: "clickhouse",
			dsn:  "not a dsn",
			want: "",
		},
		{
			name:    "databricks default port",
			kind:    "databricks",
			dsn:     "token:dapi123abc@dbc-abc.cloud.databricks.com/sql/1.0/warehouses/xyz?catalog=main",
			secret:  "dapi123abc",
			want:    "dbc-abc.cloud.databricks.com:443",
			wantTLS: true,
		},
		{
			name:    "databricks explicit port",
			kind:    "databricks",
			dsn:     "token:dapi123abc@dbc-abc.cloud.databricks.com:8443/sql/1.0/warehouses/xyz?catalog=main",
			secret:  "dapi123abc",
			want:    "dbc-abc.cloud.databricks.com:8443",
			wantTLS: true,
		},
		{
			name:    "databricks https prefix",
			kind:    "databricks",
			dsn:     "https://token:dapi123abc@adb-1.azuredatabricks.net/sql/1.0/warehouses/x?catalog=main",
			secret:  "dapi123abc",
			want:    "adb-1.azuredatabricks.net:443",
			wantTLS: true,
		},
		{
			name:    "databricks at in token",
			kind:    "databricks",
			dsn:     "token:dapi@ss@dbc.example/sql/1.0/warehouses/x?catalog=main",
			secret:  "dapi@ss",
			want:    "dbc.example:443",
			wantTLS: true,
		},
		{
			name: "databricks bad dsn no host",
			kind: "databricks",
			dsn:  "token:dapi123abc@",
			want: "",
		},
		{
			name:   "databricks credential slash in userinfo",
			kind:   "databricks",
			dsn:    "token:1234/x@dbc-1.cloud.databricks.com/sql/1.0/warehouses/x?catalog=main",
			secret: "1234",
			want:   "",
		},
		{
			name: "databricks port zero",
			kind: "databricks",
			dsn:  "token:t@dbc.example:0/sql/1.0/warehouses/x?catalog=main",
			want: "",
		},
		{
			name: "databricks port too large",
			kind: "databricks",
			dsn:  "token:t@dbc.example:99999/sql/1.0/warehouses/x?catalog=main",
			want: "",
		},
		{
			name: "databricks ipv6 host refused",
			kind: "databricks",
			dsn:  "token:t@[::1]/sql/1.0/warehouses/x?catalog=main",
			want: "",
		},
		{
			name: "bigquery skipped",
			kind: "bigquery",
			dsn:  "bigquery://my-project/us",
			want: "",
		},
		{
			name: "postgres skipped",
			kind: "postgres",
			dsn:  "postgres://u:p@h:5432/db",
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tg, ok := warehouseTarget(tc.kind, tc.dsn)
			got := ""
			if ok {
				got = tg.Addr
			}
			if got != tc.want {
				t.Fatalf("warehouseTarget(%q, %q) addr = %q, want %q", tc.kind, tc.dsn, got, tc.want)
			}
			if tc.secret != "" && strings.Contains(got, tc.secret) {
				t.Fatalf("credential %q leaked into target %q", tc.secret, got)
			}
			if !ok {
				return
			}
			if tg.TLS != tc.wantTLS {
				t.Fatalf("warehouseTarget(%q, %q) tls = %v, want %v", tc.kind, tc.dsn, tg.TLS, tc.wantTLS)
			}
			if tc.server != "" && (tg.Config == nil || tg.Config.ServerName != tc.server) {
				t.Fatalf("warehouseTarget(%q, %q) server name = %v, want %q", tc.kind, tc.dsn, tg.Config, tc.server)
			}
		})
	}
}

func TestCertTargetsWarehouseKinds(t *testing.T) {
	tests := []struct {
		kind string
		dsn  string
		want string // warehouse host:port, or "" for no warehouse target
	}{
		{"snowflake", "u:p@acct/db", "acct.snowflakecomputing.com:443"},
		{"clickhouse", "clickhouse://u:p@ch.example:9440/db?secure=true", "ch.example:9440"},
		{"databricks", "token:t@dbc.example/sql/1.0/warehouses/x?catalog=main", "dbc.example:443"},
		{"bigquery", "bigquery://proj/us", ""},
		{"postgres", "postgres://u:p@h/db", ""},
	}
	for _, tc := range tests {
		t.Run(tc.kind, func(t *testing.T) {
			var wh *CertTarget
			for _, tg := range CertTargets(config.Config{WarehouseKind: tc.kind, WarehouseDBURL: tc.dsn}) {
				if tg.Label == "warehouse" {
					wh = &tg
				}
			}
			if tc.want == "" {
				if wh != nil {
					t.Fatalf("unexpected warehouse target %+v", wh)
				}
				return
			}
			if wh == nil {
				t.Fatalf("no warehouse target for kind %q", tc.kind)
			}
			if wh.Addr != tc.want || !wh.TLS {
				t.Fatalf("warehouse target = %+v, want addr %q with TLS", wh, tc.want)
			}
		})
	}
}
