package integrations

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/integrations/spec"
)

type fakeKafka struct {
	brokers   int
	brokerErr error
	groupErr  error
	acls      []string
	aclErr    error
	block     bool
	closed    bool
}

func (f *fakeKafka) wait(ctx context.Context) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (f *fakeKafka) Brokers(ctx context.Context) (int, error) {
	if err := f.wait(ctx); err != nil {
		return 0, err
	}
	return f.brokers, f.brokerErr
}

func (f *fakeKafka) Groups(ctx context.Context) (int, error) {
	if err := f.wait(ctx); err != nil {
		return 0, err
	}
	return 3, f.groupErr
}

func (f *fakeKafka) WriteACLs(ctx context.Context, _ string) ([]string, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return f.acls, f.aclErr
}

func (f *fakeKafka) Close() { f.closed = true }

func kafkaDeps(f *fakeKafka) Deps {
	d := fastDeps()
	d.kafkaOpen = func([]string, []kgo.Opt) (kafkaProbe, error) { return f, nil }
	return d
}

func TestKafkaChecks(t *testing.T) {
	host, port := listenTCP(t)
	broker := host + ":" + port
	base := func() spec.Values {
		// SASL_PLAINTEXT with SCRAM keeps the test off TLS; the TLS check has its own test.
		return vals(map[string]string{"brokers": broker, "protocol": "SASL_PLAINTEXT", "mechanism": "SCRAM-SHA-512", "username": "piperoom"},
			map[string]string{"password": canary})
	}
	tests := []struct {
		name string
		f    *fakeKafka
		want map[string]spec.CheckStatus
	}{
		{"all pass", &fakeKafka{brokers: 3}, map[string]spec.CheckStatus{"reach": spec.CheckPass, "metadata": spec.CheckPass, "groups": spec.CheckPass, "read_only": spec.CheckPass}},
		{"sasl failed", &fakeKafka{brokerErr: fmt.Errorf("wrapped: %w", kerr.SaslAuthenticationFailed)}, map[string]spec.CheckStatus{"metadata": spec.CheckFail, "groups": spec.CheckSkipped}},
		{"no brokers", &fakeKafka{}, map[string]spec.CheckStatus{"metadata": spec.CheckFail}},
		{"groups denied", &fakeKafka{brokers: 1, groupErr: kerr.GroupAuthorizationFailed}, map[string]spec.CheckStatus{"groups": spec.CheckFail}},
		{"can produce", &fakeKafka{brokers: 1, acls: []string{"TOPIC orders WRITE"}}, map[string]spec.CheckStatus{"read_only": spec.CheckWarn}},
		{"acls unreadable", &fakeKafka{brokers: 1, aclErr: kerr.SecurityDisabled}, map[string]spec.CheckStatus{"read_only": spec.CheckSkipped}},
		{"timeout", &fakeKafka{block: true}, map[string]spec.CheckStatus{"metadata": spec.CheckFail}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checks := kafkaDeps(tc.f).kafkaKinds()[0].Test(context.Background(), base())
			for id, st := range tc.want {
				c := wantStatus(t, checks, id, st)
				if st == spec.CheckFail && c.Fix == "" {
					t.Fatalf("check %s failed without a fix", id)
				}
			}
			noCanary(t, checks)
			if !tc.f.closed {
				t.Fatal("probe not closed")
			}
		})
	}
}

func TestKafkaUnreachableAndTLS(t *testing.T) {
	f := &fakeKafka{brokers: 1}
	checks := kafkaDeps(f).kafkaKinds()[0].Test(context.Background(),
		vals(map[string]string{"brokers": "127.0.0.1:" + closedPort(t), "protocol": "PLAINTEXT"}, nil))
	wantStatus(t, checks, "reach", spec.CheckFail)

	// A plain TCP listener cannot do TLS: the handshake check fails.
	host, port := listenTCP(t)
	checks = kafkaDeps(f).kafkaKinds()[0].Test(context.Background(),
		vals(map[string]string{"brokers": host + ":" + port, "protocol": "SSL"}, nil))
	wantStatus(t, checks, "tls", spec.CheckFail)
	wantStatus(t, checks, "login", spec.CheckSkipped)
}

func TestConfluentCloudPreset(t *testing.T) {
	v := vals(map[string]string{"bootstrap": "pkc-123.us-east-1.aws.confluent.cloud:9092", "api_key": "ABC"}, map[string]string{"api_secret": canary})
	tg, err := parseKafka(v, true)
	if err != nil {
		t.Fatal(err)
	}
	if tg.Protocol != "SASL_SSL" || tg.Mechanism != "PLAIN" || tg.Username != "ABC" {
		t.Fatalf("preset = %+v", tg)
	}
	host, port := listenTCP(t)
	v.Config["bootstrap"] = host + ":" + port
	f := &fakeKafka{brokers: 1}
	checks := kafkaDeps(f).kafkaKinds()[1].Test(context.Background(), v)
	// TLS against a plain listener fails; the preset forces TLS.
	wantStatus(t, checks, "tls", spec.CheckFail)
	noCanary(t, checks)
}

func TestKafkaTLSWithPastedCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	defer srv.Close()
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	addr := strings.TrimPrefix(srv.URL, "https://")
	f := &fakeKafka{brokers: 1}
	checks := kafkaDeps(f).kafkaKinds()[0].Test(context.Background(), vals(
		map[string]string{"brokers": addr, "protocol": "SASL_SSL", "mechanism": "SCRAM-SHA-256", "username": "piperoom"},
		map[string]string{"password": canary, "ca_cert": caPEM}))
	wantStatus(t, checks, "tls", spec.CheckPass)
	wantStatus(t, checks, "metadata", spec.CheckPass)
	noCanary(t, checks)

	// Without the CA the same server fails verification.
	checks = kafkaDeps(f).kafkaKinds()[0].Test(context.Background(), vals(
		map[string]string{"brokers": addr, "protocol": "SSL"}, nil))
	c := wantStatus(t, checks, "tls", spec.CheckFail)
	if c.Fix == "" {
		t.Fatal("tls failure without a fix")
	}
}

func TestKafkaParseErrors(t *testing.T) {
	tests := []struct {
		name  string
		cfg   map[string]string
		sec   map[string]string
		field string
	}{
		{"no brokers", map[string]string{}, nil, "brokers"},
		{"no port", map[string]string{"brokers": "kafka.corp"}, nil, "brokers"},
		{"plain without tls", map[string]string{"brokers": "k:9092", "protocol": "SASL_PLAINTEXT", "mechanism": "PLAIN", "username": "u"}, map[string]string{"password": "p"}, "protocol"},
		{"login without sasl", map[string]string{"brokers": "k:9092", "protocol": "SSL", "username": "u"}, map[string]string{"password": "p"}, "protocol"},
		{"certs without tls", map[string]string{"brokers": "k:9092", "protocol": "PLAINTEXT"}, map[string]string{"ca_cert": "x"}, "protocol"},
		{"sasl without password", map[string]string{"brokers": "k:9092", "protocol": "SASL_SSL", "mechanism": "SCRAM-SHA-256", "username": "u"}, nil, "protocol"},
		{"msk iam", map[string]string{"brokers": "k:9098", "protocol": "SASL_SSL", "mechanism": "AWS_MSK_IAM"}, nil, "mechanism"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseKafka(vals(tc.cfg, tc.sec), false)
			var ve *spec.ValidationError
			if !errors.As(err, &ve) || ve.Field != tc.field {
				t.Fatalf("err = %v, want validation on %s", err, tc.field)
			}
		})
	}
}

func TestKafkaBuild(t *testing.T) {
	// kgo dials lazily, so Build works without a cluster.
	v := vals(map[string]string{"brokers": "127.0.0.1:9", "protocol": "SASL_SSL", "mechanism": "SCRAM-SHA-512", "username": "u"}, map[string]string{"password": canary})
	client, closeFn, err := fastDeps().withDefaults().kafkaKinds()[0].Build(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	if client == nil || closeFn == nil {
		t.Fatal("nil client or close")
	}
	closeFn()
	if strings.Contains(fmt.Sprintf("%v %+v", client, client), canary) {
		t.Fatal("client prints the password")
	}
	if _, _, err := fastDeps().withDefaults().kafkaKinds()[0].Build(context.Background(), vals(map[string]string{}, nil)); err == nil {
		t.Fatal("bad input: want error")
	}
}

func TestKafkaSetupScript(t *testing.T) {
	s := kafkaSetupScript(vals(map[string]string{"username": "piperoom"}, nil))
	if !strings.Contains(s, "User:piperoom") || strings.Contains(s, "--operation Write ") {
		t.Fatalf("script:\n%s", s)
	}
	if bad := kafkaSetupScript(vals(map[string]string{"username": "x; rm -rf /"}, nil)); strings.Contains(bad, "rm -rf") {
		t.Fatalf("unsafe user reached the script:\n%s", bad)
	}
}
