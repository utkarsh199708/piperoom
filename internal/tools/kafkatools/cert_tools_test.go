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
