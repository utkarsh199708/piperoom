package connprobe

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

const kafkaTopic = "pg.public.orders"

var kafkaIDs = []string{"config", "reach", "login", "topics", "offsets", "groups", "acls"}

func newFake(t *testing.T, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func kafkaTarget(c *kfake.Cluster) Target {
	return Target{
		Kind:   KindKafka,
		Config: config.Config{KafkaBrokers: strings.Join(c.ListenAddrs(), ",")},
		Pipelines: []owners.Pipeline{{
			ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"},
			ConsumerGroups: []string{"order-sync"},
		}},
	}
}

func withSCRAM(tg Target, user, pass string) Target {
	tg.Config.KafkaSecurityProtocol = "SASL_PLAINTEXT"
	tg.Config.KafkaSASLMechanism = "SCRAM-SHA-256"
	tg.Config.KafkaSASLUsername = user
	tg.Config.KafkaSASLPassword = pass
	return tg
}

func TestKafkaOK(t *testing.T) {
	c := newFake(t, kfake.SeedTopics(3, kafkaTopic))
	r := Run(context.Background(), kafkaTarget(c), loopGuard())
	// The group has no member yet. kfake without EnableACLs answers
	// DescribeACLs with an empty list (a real broker without an authorizer
	// answers SECURITY_DISABLED, which the integration test covers).
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusSkip, "topics", StatusOK,
		"offsets", StatusOK, "groups", StatusWarn, "acls", StatusOK)
	if !r.OK {
		t.Errorf("want ok: %s", mustJSON(t, r))
	}
	if d := check(t, r, "topics").Detail; d != kafkaTopic+" (3 partitions)" {
		t.Errorf("topics detail %q", d)
	}
	if p := check(t, r, "groups").Problem; !strings.Contains(p, "order-sync") {
		t.Errorf("groups problem %q", p)
	}
}

func TestKafkaMissingTopic(t *testing.T) {
	c := newFake(t, kfake.SeedTopics(1, "other"))
	r := Run(context.Background(), kafkaTarget(c), loopGuard())
	tc := check(t, r, "topics")
	if tc.Status != StatusFail || !strings.Contains(tc.Problem, kafkaTopic) || !strings.Contains(tc.Fix, "owners.yaml") {
		t.Errorf("topics: %+v", tc)
	}
	if s := check(t, r, "offsets").Status; s != StatusSkip {
		t.Errorf("offsets %s, want skip", s)
	}
	if r.OK {
		t.Error("want not ok")
	}
}

func TestKafkaSASL(t *testing.T) {
	const admin, adminPass = "admin-7a", "admin-pass-31c"
	c := newFake(t, kfake.SeedTopics(1, kafkaTopic), kfake.EnableSASL(), kfake.Superuser("SCRAM-SHA-256", admin, adminPass))

	r := Run(context.Background(), withSCRAM(kafkaTarget(c), admin, adminPass), loopGuard())
	if lc := check(t, r, "login"); lc.Status != StatusOK || lc.Detail != "SASL SCRAM-SHA-256 accepted" {
		t.Errorf("login: %+v", lc)
	}
	assertNoSecret(t, r, admin, adminPass)

	// kfake drops the connection on a wrong password; a real broker answers
	// SASL_AUTHENTICATION_FAILED with a message naming the user. Answer like
	// the real broker for this run.
	var refuse atomic.Bool
	refuse.Store(true)
	c.ControlKey(int16(kmsg.SASLAuthenticate), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !refuse.Load() {
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.SASLAuthenticateResponse)
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		resp.ErrorMessage = kmsg.StringPtr("Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256 for user " + admin)
		return resp, nil, true
	})
	r = Run(context.Background(), withSCRAM(kafkaTarget(c), admin, "wrong-"+adminPass), loopGuard())
	refuse.Store(false)
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusFail, "topics", StatusSkip,
		"offsets", StatusSkip, "groups", StatusSkip, "acls", StatusSkip)
	lc := check(t, r, "login")
	if !strings.Contains(lc.Fix, "KAFKA_SASL_USERNAME") || !strings.Contains(lc.Fix, "KAFKA_SASL_PASSWORD") {
		t.Errorf("login fix %q", lc.Fix)
	}
	assertNoSecret(t, r, admin, adminPass, "invalid credentials")

	// kfake's own answer to a wrong password: it closes the connection.
	r = Run(context.Background(), withSCRAM(kafkaTarget(c), admin, "wrong-"+adminPass), loopGuard())
	if rc := check(t, r, "reach"); rc.Status != StatusFail || !strings.Contains(rc.Problem, "closed the connection") {
		t.Errorf("dropped login: reach %+v", rc)
	}
	assertNoSecret(t, r, admin, adminPass)

	// A PLAINTEXT client to a SASL listener: the broker closes the connection.
	r = Run(context.Background(), kafkaTarget(c), loopGuard())
	if rc := check(t, r, "reach"); rc.Status != StatusFail {
		t.Errorf("plaintext to sasl: reach %+v", rc)
	}
}

func TestKafkaACLs(t *testing.T) {
	const admin, adminPass = "admin-7a", "admin-pass-31c"
	const user, pass = "piperoom-u", "piperoom-pass-88"
	allow := func(res kmsg.ACLResourceType, name string, pat kmsg.ACLResourcePatternType, op kmsg.ACLOperation) kfake.ACL {
		return kfake.ACL{Resource: res, Name: name, Pattern: pat, Operation: op, Allow: true}
	}
	base := []kfake.ACL{
		allow(kmsg.ACLResourceTypeTopic, "pg.", kmsg.ACLResourcePatternTypePrefixed, kmsg.ACLOperationDescribe),
		allow(kmsg.ACLResourceTypeTopic, "pg.", kmsg.ACLResourcePatternTypePrefixed, kmsg.ACLOperationRead),
		allow(kmsg.ACLResourceTypeGroup, "order-sync", kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLOperationDescribe),
	}
	clusterDescribe := allow(kmsg.ACLResourceTypeCluster, "kafka-cluster", kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLOperationDescribe)
	write := allow(kmsg.ACLResourceTypeTopic, "pg.", kmsg.ACLResourcePatternTypePrefixed, kmsg.ACLOperationWrite)

	t.Run("more rights than needed", func(t *testing.T) {
		acls := append(append([]kfake.ACL{}, base...), clusterDescribe, write)
		c := newFake(t, kfake.SeedTopics(1, kafkaTopic), kfake.EnableSASL(), kfake.EnableACLs(),
			kfake.Superuser("SCRAM-SHA-256", admin, adminPass), kfake.User("SCRAM-SHA-256", user, pass, acls...))
		r := Run(context.Background(), withSCRAM(kafkaTarget(c), user, pass), loopGuard())
		ac := check(t, r, "acls")
		if ac.Status != StatusWarn || !strings.Contains(ac.Problem, "Write on topic pg.") || !strings.Contains(ac.Fix, "--remove") ||
			!strings.Contains(ac.Fix, "User:<KAFKA_SASL_USERNAME>") {
			t.Errorf("acls: %+v\n%s", ac, mustJSON(t, r))
		}
		if s := check(t, r, "topics").Status; s != StatusOK {
			t.Errorf("topics %s", s)
		}
		if !r.OK {
			t.Error("a warning must not fail the probe")
		}
		assertNoSecret(t, r, user, pass, admin, adminPass)
	})

	t.Run("least privilege", func(t *testing.T) {
		acls := append(append([]kfake.ACL{}, base...), clusterDescribe)
		c := newFake(t, kfake.SeedTopics(1, kafkaTopic), kfake.EnableSASL(), kfake.EnableACLs(),
			kfake.Superuser("SCRAM-SHA-256", admin, adminPass), kfake.User("SCRAM-SHA-256", user, pass, acls...))
		r := Run(context.Background(), withSCRAM(kafkaTarget(c), user, pass), loopGuard())
		if ac := check(t, r, "acls"); ac.Status != StatusOK {
			t.Errorf("acls: %+v", ac)
		}
	})

	t.Run("may not describe ACLs", func(t *testing.T) {
		c := newFake(t, kfake.SeedTopics(1, kafkaTopic), kfake.EnableSASL(), kfake.EnableACLs(),
			kfake.Superuser("SCRAM-SHA-256", admin, adminPass), kfake.User("SCRAM-SHA-256", user, pass, base...))
		r := Run(context.Background(), withSCRAM(kafkaTarget(c), user, pass), loopGuard())
		ac := check(t, r, "acls")
		if ac.Status != StatusSkip || !strings.Contains(ac.Detail, "Describe on Cluster") {
			t.Errorf("acls: %+v", ac)
		}
		if !r.OK {
			t.Errorf("want ok: %s", mustJSON(t, r))
		}
	})

	t.Run("may not describe topic or group", func(t *testing.T) {
		c := newFake(t, kfake.SeedTopics(1, kafkaTopic), kfake.EnableSASL(), kfake.EnableACLs(),
			kfake.Superuser("SCRAM-SHA-256", admin, adminPass), kfake.User("SCRAM-SHA-256", user, pass))
		r := Run(context.Background(), withSCRAM(kafkaTarget(c), user, pass), loopGuard())
		tc := check(t, r, "topics")
		if tc.Status != StatusFail || !strings.Contains(tc.Fix, "--operation Describe") || !strings.Contains(tc.Fix, "--topic pg.") {
			t.Errorf("topics: %+v", tc)
		}
		gc := check(t, r, "groups")
		if gc.Status != StatusFail || !strings.Contains(gc.Fix, "--group order-sync") {
			t.Errorf("groups: %+v", gc)
		}
		assertNoSecret(t, r, user, pass)
	})
}

func TestKafkaUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	tg := Target{Kind: KindKafka, Config: config.Config{KafkaBrokers: addr}}
	start := time.Now()
	r := Run(context.Background(), tg, loopGuard())
	if took := time.Since(start); took > 12*time.Second {
		t.Errorf("unreachable probe took %v", took)
	}
	wantChecks(t, r, "config", StatusOK, "reach", StatusFail, "login", StatusSkip, "topics", StatusSkip,
		"offsets", StatusSkip, "groups", StatusSkip, "acls", StatusSkip)
}

func TestKafkaGuardRefusal(t *testing.T) {
	c := newFake(t, kfake.SeedTopics(1, kafkaTopic))
	for name, tc := range map[string]struct {
		brokers string
		g       *Guard
	}{
		"loopback not allowed": {strings.Join(c.ListenAddrs(), ","), &Guard{}},
		"metadata address":     {"169.254.169.254:9092", loopGuard()},
	} {
		t.Run(name, func(t *testing.T) {
			tg := kafkaTarget(c)
			tg.Config.KafkaBrokers = tc.brokers
			r := Run(context.Background(), tg, tc.g)
			rc := check(t, r, "reach")
			if rc.Status != StatusFail || !strings.Contains(rc.Problem, "not allowed") {
				t.Errorf("reach: %+v", rc)
			}
		})
	}
}

func TestKafkaTLS(t *testing.T) {
	cert, ca := selfSigned(t)
	c := newFake(t, kfake.SeedTopics(1, kafkaTopic), kfake.TLS(&tls.Config{Certificates: []tls.Certificate{cert}}))

	// SSL goes through the guard's dialer with our own handshake (netsec's
	// TLS dialer is replaced); this fails if kgo refuses the option mix.
	tg := kafkaTarget(c)
	tg.Config.KafkaSecurityProtocol = "SSL"
	tg.Config.KafkaTLSCAFile = ca
	r := Run(context.Background(), tg, loopGuard())
	if rc := check(t, r, "reach"); rc.Status != StatusOK {
		t.Fatalf("reach over TLS: %+v", rc)
	}
	if tc := check(t, r, "topics"); tc.Status != StatusOK {
		t.Errorf("topics over TLS: %+v", tc)
	}

	// System roots do not trust the test CA.
	tg.Config.KafkaTLSCAFile = ""
	r = Run(context.Background(), tg, loopGuard())
	rc := check(t, r, "reach")
	if rc.Status != StatusFail || !strings.Contains(rc.Problem, "not trusted") {
		t.Errorf("untrusted: %+v", rc)
	}

	// A TLS client to a plaintext listener: the broker waits for a request
	// that never completes, so the handshake runs into the deadline (cut
	// short here to keep the test fast).
	plain := newFake(t)
	tg = kafkaTarget(plain)
	tg.Config.KafkaSecurityProtocol = "SSL"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r = Run(ctx, tg, loopGuard())
	if rc := check(t, r, "reach"); rc.Status != StatusFail {
		t.Errorf("tls to plaintext: %+v", rc)
	}
}

func TestKafkaMSKIAM(t *testing.T) {
	cert, ca := selfSigned(t)
	c := newFake(t, kfake.TLS(&tls.Config{Certificates: []tls.Certificate{cert}}))
	tg := kafkaTarget(c)
	tg.Config.KafkaSecurityProtocol = "SASL_SSL"
	tg.Config.KafkaSASLMechanism = "AWS_MSK_IAM"
	tg.Config.KafkaTLSCAFile = ca
	r := Run(context.Background(), tg, loopGuard())
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusSkip, "topics", StatusSkip,
		"offsets", StatusSkip, "groups", StatusSkip, "acls", StatusSkip)
	if d := check(t, r, "login").Detail; !strings.Contains(d, "AWS_MSK_IAM") {
		t.Errorf("login detail %q", d)
	}
	if d := check(t, r, "reach").Detail; d != "TLS handshake with 1 of 1 seed brokers" {
		t.Errorf("reach detail %q", d)
	}
}

func TestKafkaConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		in   string
	}{
		{"no brokers", config.Config{KafkaBrokers: " , "}, "KAFKA_BROKERS is not set"},
		{"not host:port", config.Config{KafkaBrokers: "broker1"}, "not host:port"},
		{"unknown protocol", config.Config{KafkaBrokers: "b:9092", KafkaSecurityProtocol: "TLS"}, "not a known protocol"},
		{"tls files on plaintext", config.Config{KafkaBrokers: "b:9092", KafkaTLSCAFile: "ca.pem"}, "has no TLS"},
		{"sasl on ssl", config.Config{KafkaBrokers: "b:9092", KafkaSecurityProtocol: "SSL", KafkaSASLMechanism: "PLAIN", KafkaSASLUsername: testUser, KafkaSASLPassword: testPassword}, "has no SASL"},
		{"plain without tls", config.Config{KafkaBrokers: "b:9092", KafkaSecurityProtocol: "SASL_PLAINTEXT", KafkaSASLMechanism: "PLAIN", KafkaSASLUsername: testUser, KafkaSASLPassword: testPassword}, "clear text"},
		{"no mechanism", config.Config{KafkaBrokers: "b:9092", KafkaSecurityProtocol: "SASL_SSL", KafkaSASLUsername: testUser, KafkaSASLPassword: testPassword}, "KAFKA_SASL_MECHANISM is not set"},
		{"half login", config.Config{KafkaBrokers: "b:9092", KafkaSecurityProtocol: "SASL_SSL", KafkaSASLMechanism: "SCRAM-SHA-512", KafkaSASLUsername: testUser}, "incomplete"},
		{"unknown mechanism", config.Config{KafkaBrokers: "b:9092", KafkaSecurityProtocol: "SASL_SSL", KafkaSASLMechanism: "GSSAPI", KafkaSASLUsername: testUser, KafkaSASLPassword: testPassword}, "not a known mechanism"},
		{"bad ca file", config.Config{KafkaBrokers: "b:9092", KafkaSecurityProtocol: "SSL", KafkaTLSCAFile: "does-not-exist.pem"}, "cannot be loaded"},
		{"msk over plaintext", config.Config{KafkaBrokers: "b:9098", KafkaSecurityProtocol: "SASL_PLAINTEXT", KafkaSASLMechanism: "AWS_MSK_IAM"}, "needs TLS"},
		{"msk with password", config.Config{KafkaBrokers: "b:9098", KafkaSecurityProtocol: "SASL_SSL", KafkaSASLMechanism: "AWS_MSK_IAM", KafkaSASLUsername: testUser, KafkaSASLPassword: testPassword}, "Unset"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := Run(context.Background(), Target{Kind: KindKafka, Config: tc.cfg}, loopGuard())
			want := []string{"config", StatusFail}
			for _, id := range kafkaIDs[1:] {
				want = append(want, id, StatusSkip)
			}
			wantChecks(t, r, want...)
			c := check(t, r, "config")
			if !strings.Contains(c.Problem+" "+c.Fix, tc.in) {
				t.Errorf("config check %+v does not contain %q", c, tc.in)
			}
			assertNoSecret(t, r, testUser, testPassword, "does-not-exist")
		})
	}
}
