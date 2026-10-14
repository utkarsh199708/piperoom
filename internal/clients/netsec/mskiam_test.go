package netsec

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

const (
	testAccessKey    = "AKIAPIPEROOMTEST"
	testSecretKey    = "aws-secret-do-not-log"
	testSessionToken = "aws-session-token-do-not-log"
)

// countingCreds returns a credentials func that counts its calls.
func countingCreds(calls *atomic.Int32) func(context.Context) (AWSCredentials, error) {
	return func(context.Context) (AWSCredentials, error) {
		calls.Add(1)
		return AWSCredentials{AccessKey: testAccessKey, SecretKey: testSecretKey, SessionToken: testSessionToken}, nil
	}
}

func TestKafkaOptsMSKIAM(t *testing.T) {
	var calls atomic.Int32
	creds := countingCreds(&calls)
	tests := []struct {
		name    string
		in      Kafka
		wantErr string // "" = valid; otherwise a substring of the ErrInvalid message
	}{
		{name: "sasl_ssl", in: Kafka{Protocol: SASLSSL, Mechanism: MechAWSMSKIAM, AWSCredentialsFunc: creds}},
		{name: "lower case mechanism", in: Kafka{Protocol: "sasl_ssl", Mechanism: "aws_msk_iam", AWSCredentialsFunc: creds}},
		{name: "sasl_plaintext refused", in: Kafka{Protocol: SASLPlaintext, Mechanism: MechAWSMSKIAM, AWSCredentialsFunc: creds}, wantErr: "needs security protocol SASL_SSL"},
		{name: "username set", in: Kafka{Protocol: SASLSSL, Mechanism: MechAWSMSKIAM, Username: "u", AWSCredentialsFunc: creds}, wantErr: "unset the kafka SASL username and password"},
		{name: "password set", in: Kafka{Protocol: SASLSSL, Mechanism: MechAWSMSKIAM, Password: testPassword, AWSCredentialsFunc: creds}, wantErr: "unset the kafka SASL username and password"},
		{name: "no credentials func", in: Kafka{Protocol: SASLSSL, Mechanism: MechAWSMSKIAM}, wantErr: "needs AWS credentials"},
		{name: "credentials func with scram", in: Kafka{Protocol: SASLSSL, Mechanism: MechSCRAMSHA512, Username: "u", Password: testPassword, AWSCredentialsFunc: creds}, wantErr: "used only with AWS_MSK_IAM"},
		{name: "credentials func with ssl", in: Kafka{Protocol: SSL, AWSCredentialsFunc: creds}, wantErr: "security protocol is SSL"},
		{name: "credentials func with plaintext", in: Kafka{AWSCredentialsFunc: creds}, wantErr: "security protocol is PLAINTEXT"},
		{name: "mechanism with ssl", in: Kafka{Protocol: SSL, Mechanism: MechAWSMSKIAM, AWSCredentialsFunc: creds}, wantErr: "security protocol is SSL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := tt.in.Opts()
			if tt.wantErr != "" {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("err = %v, want ErrInvalid", err)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("err = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:1")}, opts...)...)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			mechs, _ := cl.OptValue(kgo.SASL).([]sasl.Mechanism)
			if len(mechs) != 1 || mechs[0].Name() != MechAWSMSKIAM {
				t.Errorf("SASL mechanisms = %v, want %s", mechNames(mechs), MechAWSMSKIAM)
			}
			if gotTLS, _ := cl.OptValue(kgo.DialTLSConfig).(*tls.Config); gotTLS == nil {
				t.Error("TLS config not set")
			}
		})
	}
	// Opts only builds options; credentials are fetched per SASL session.
	if n := calls.Load(); n != 0 {
		t.Errorf("credentials func called %d times while building options, want 0", n)
	}
}

// TestKafkaWireMSKIAM drives a kgo client against a TLS fake broker that only
// offers AWS_MSK_IAM: the mechanism is requested in SASLHandshake, the
// credentials func is called, and the signed payload carries the access key
// and our user agent but never the secret key.
func TestKafkaWireMSKIAM(t *testing.T) {
	// The fake broker is 127.0.0.1, not an MSK host name, so franz-go falls
	// back to AWS_REGION for the signing scope.
	t.Setenv("AWS_REGION", "ap-south-1")
	p := newTestPKI(t)
	b := newFakeMSKIAMBroker(t, p.serverTLS(false), testAccessKey)

	var calls atomic.Int32
	opts, err := Kafka{
		Protocol: SASLSSL, Mechanism: MechAWSMSKIAM,
		AWSCredentialsFunc: countingCreds(&calls),
		TLS:                TLS{CAFile: p.caFile},
	}.Opts()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(b.addr())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		_, _, _, _, connErrs := b.stats()
		t.Fatalf("ping: %v (broker errors: %v)", err, connErrs)
	}

	mechs, authOK, authFail, metadata, _ := b.stats()
	if len(mechs) == 0 || mechs[0] != MechAWSMSKIAM {
		t.Errorf("SASLHandshake mechanisms = %v, want %s", mechs, MechAWSMSKIAM)
	}
	if authOK == 0 || authFail != 0 {
		t.Errorf("auth ok/fail = %d/%d, want >0/0", authOK, authFail)
	}
	if metadata == 0 {
		t.Error("broker served no metadata after IAM login")
	}
	if calls.Load() == 0 {
		t.Error("AWSCredentialsFunc was never called")
	}
	payloads := b.payloads()
	if len(payloads) == 0 {
		t.Fatal("no AWS_MSK_IAM payload received")
	}
	var got map[string]string
	if err := json.Unmarshal(payloads[0], &got); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if got["user-agent"] != mskIAMUserAgent {
		t.Errorf("user-agent = %q, want %q", got["user-agent"], mskIAMUserAgent)
	}
	if !strings.Contains(got["x-amz-credential"], "/ap-south-1/kafka-cluster/") {
		t.Errorf("credential scope = %q, want region ap-south-1", got["x-amz-credential"])
	}
	if got["x-amz-security-token"] != testSessionToken {
		t.Error("session token missing from the signed payload")
	}
	if bytes.Contains(payloads[0], []byte(testSecretKey)) {
		t.Error("signed payload contains the secret key")
	}
}

// TestKafkaWireMSKIAMCredentialError: a failing credential chain fails the
// login without dialing on into Metadata, and the error leaks nothing.
func TestKafkaWireMSKIAMCredentialError(t *testing.T) {
	t.Setenv("AWS_REGION", "ap-south-1")
	p := newTestPKI(t)
	b := newFakeMSKIAMBroker(t, p.serverTLS(false), testAccessKey)
	opts, err := Kafka{
		Protocol: SASLSSL, Mechanism: MechAWSMSKIAM,
		AWSCredentialsFunc: func(context.Context) (AWSCredentials, error) {
			return AWSCredentials{}, errors.New("no EC2 IMDS role found")
		},
		TLS: TLS{CAFile: p.caFile},
	}.Opts()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(b.addr()), kgo.RequestRetries(0)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err == nil {
		t.Fatal("ping succeeded without credentials")
	}
	if _, authOK, _, metadata, _ := b.stats(); authOK != 0 || metadata != 0 {
		t.Errorf("auth ok = %d, metadata = %d; want 0, 0", authOK, metadata)
	}
}

func TestAWSCredentialsAreRedacted(t *testing.T) {
	c := AWSCredentials{AccessKey: testAccessKey, SecretKey: testSecretKey, SessionToken: testSessionToken}
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("creds", "c", c)
	out := fmt.Sprintf("%v %+v %#v %s", c, c, c, c) + buf.String()
	for _, secret := range []string{testSecretKey, testSessionToken} {
		if strings.Contains(out, secret) {
			t.Fatalf("credentials leak in %q", out)
		}
	}
}
