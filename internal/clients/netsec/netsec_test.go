package netsec

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

const testPassword = "s3cret-value"

func TestSecretIsRedacted(t *testing.T) {
	cfg := Kafka{Protocol: SASLSSL, Mechanism: MechPlain, Username: "agent", Password: testPassword}
	hcfg := HTTP{Username: "key", Password: testPassword}

	var logBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&logBuf, nil)).Info("cfg", "kafka", cfg, "password", cfg.Password, "http", hcfg)
	js, err := json.Marshal(hcfg)
	if err != nil {
		t.Fatal(err)
	}
	outputs := map[string]string{
		"%v":   fmt.Sprintf("%v", cfg),
		"%+v":  fmt.Sprintf("%+v", hcfg),
		"%#v":  fmt.Sprintf("%#v", cfg),
		"%s":   fmt.Sprintf("%s", cfg.Password),
		"slog": logBuf.String(),
		"json": string(js),
	}
	for name, out := range outputs {
		if strings.Contains(out, testPassword) {
			t.Errorf("%s leaks the password: %s", name, out)
		}
		if !strings.Contains(out, "[redacted]") {
			t.Errorf("%s does not show [redacted]: %s", name, out)
		}
	}
	if cfg.Password.Reveal() != testPassword {
		t.Errorf("Reveal() = %q", cfg.Password.Reveal())
	}
	if Secret("").String() != "" {
		t.Errorf("empty secret should print empty")
	}
}

func TestTLSConfig(t *testing.T) {
	p := newTestPKI(t)
	missing := filepath.Join(t.TempDir(), "missing.pem")
	tests := []struct {
		name    string
		in      TLS
		wantErr error // nil means success
		certs   int
		roots   bool
	}{
		{name: "zero uses system roots", in: TLS{}},
		{name: "custom CA", in: TLS{CAFile: p.caFile}, roots: true},
		{name: "mutual TLS", in: TLS{CAFile: p.caFile, CertFile: p.clientCertFile, KeyFile: p.clientKeyFile}, roots: true, certs: 1},
		{name: "server name", in: TLS{ServerName: "kafka.internal"}},
		{name: "CA file missing", in: TLS{CAFile: missing}, wantErr: fs.ErrNotExist},
		{name: "CA file not PEM", in: TLS{CAFile: p.notPEMFile}, wantErr: ErrInvalid},
		{name: "cert without key", in: TLS{CertFile: p.clientCertFile}, wantErr: ErrInvalid},
		{name: "key without cert", in: TLS{KeyFile: p.clientKeyFile}, wantErr: ErrInvalid},
		{name: "cert and key mismatch", in: TLS{CertFile: p.clientCertFile, KeyFile: p.clientCertWrongKey}, wantErr: errAny},
		{name: "cert file missing", in: TLS{CertFile: missing, KeyFile: p.clientKeyFile}, wantErr: fs.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := tt.in.Config()
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("want error, got config")
				}
				if tt.wantErr != errAny && !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.MinVersion != tls.VersionTLS12 {
				t.Errorf("MinVersion = %x", cfg.MinVersion)
			}
			if cfg.InsecureSkipVerify {
				t.Errorf("InsecureSkipVerify must never be set")
			}
			if (cfg.RootCAs != nil) != tt.roots {
				t.Errorf("RootCAs set = %v, want %v", cfg.RootCAs != nil, tt.roots)
			}
			if len(cfg.Certificates) != tt.certs {
				t.Errorf("certificates = %d, want %d", len(cfg.Certificates), tt.certs)
			}
			if cfg.ServerName != tt.in.ServerName {
				t.Errorf("ServerName = %q", cfg.ServerName)
			}
		})
	}
}

// errAny marks a test case that expects some error, whatever its kind.
var errAny = errors.New("any error")

func TestKafkaOpts(t *testing.T) {
	p := newTestPKI(t)
	ca := TLS{CAFile: p.caFile}
	tests := []struct {
		name     string
		in       Kafka
		wantErr  bool
		wantMech string // "" means no SASL option
		wantTLS  bool
	}{
		{name: "zero is plaintext", in: Kafka{}},
		{name: "explicit plaintext", in: Kafka{Protocol: "plaintext"}},
		{name: "ssl", in: Kafka{Protocol: SSL, TLS: ca}, wantTLS: true},
		{name: "ssl with system roots", in: Kafka{Protocol: SSL}, wantTLS: true},
		{name: "sasl_ssl plain", in: Kafka{Protocol: SASLSSL, Mechanism: MechPlain, Username: "u", Password: testPassword}, wantMech: "PLAIN", wantTLS: true},
		{name: "sasl_ssl scram-256", in: Kafka{Protocol: SASLSSL, Mechanism: "scram-sha-256", Username: "u", Password: testPassword, TLS: ca}, wantMech: "SCRAM-SHA-256", wantTLS: true},
		{name: "sasl_ssl scram-512", in: Kafka{Protocol: SASLSSL, Mechanism: MechSCRAMSHA512, Username: "u", Password: testPassword}, wantMech: "SCRAM-SHA-512", wantTLS: true},
		{name: "sasl_plaintext scram", in: Kafka{Protocol: SASLPlaintext, Mechanism: MechSCRAMSHA512, Username: "u", Password: testPassword}, wantMech: "SCRAM-SHA-512"},
		{name: "sasl_plaintext plain refused", in: Kafka{Protocol: SASLPlaintext, Mechanism: MechPlain, Username: "u", Password: testPassword}, wantErr: true},
		{name: "unknown protocol", in: Kafka{Protocol: "TLS"}, wantErr: true},
		{name: "unknown mechanism", in: Kafka{Protocol: SASLSSL, Mechanism: "GSSAPI", Username: "u", Password: testPassword}, wantErr: true},
		{name: "missing mechanism", in: Kafka{Protocol: SASLSSL, Username: "u", Password: testPassword}, wantErr: true},
		{name: "missing password", in: Kafka{Protocol: SASLSSL, Mechanism: MechPlain, Username: "u"}, wantErr: true},
		{name: "missing username", in: Kafka{Protocol: SASLSSL, Mechanism: MechPlain, Password: testPassword}, wantErr: true},
		{name: "login with plaintext protocol", in: Kafka{Username: "u", Password: testPassword}, wantErr: true},
		{name: "mechanism with ssl protocol", in: Kafka{Protocol: SSL, Mechanism: MechPlain}, wantErr: true},
		{name: "tls files with plaintext", in: Kafka{TLS: ca}, wantErr: true},
		{name: "tls files with sasl_plaintext", in: Kafka{Protocol: SASLPlaintext, Mechanism: MechSCRAMSHA256, Username: "u", Password: testPassword, TLS: ca}, wantErr: true},
		{name: "bad CA file", in: Kafka{Protocol: SSL, TLS: TLS{CAFile: p.notPEMFile}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := tt.in.Opts()
			if tt.wantErr {
				if err == nil {
					t.Fatal("want error")
				}
				if !errors.Is(err, ErrInvalid) && !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("err = %v, want ErrInvalid", err)
				}
				if strings.Contains(err.Error(), tt.in.Password.Reveal()) && tt.in.Password != "" {
					t.Errorf("error leaks password: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// kgo.NewClient does not dial; it only validates and stores options.
			cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:1")}, opts...)...)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			mechs, _ := cl.OptValue(kgo.SASL).([]sasl.Mechanism)
			switch {
			case tt.wantMech == "" && len(mechs) != 0:
				t.Errorf("unexpected SASL mechanisms %d", len(mechs))
			case tt.wantMech != "" && (len(mechs) != 1 || mechs[0].Name() != tt.wantMech):
				t.Errorf("SASL mechanisms = %v, want %s", mechNames(mechs), tt.wantMech)
			}
			// Without TLS the default dialer is used; DialTLSConfig sets the
			// TLS config option.
			gotTLS, _ := cl.OptValue(kgo.DialTLSConfig).(*tls.Config)
			if (gotTLS != nil) != tt.wantTLS {
				t.Errorf("TLS config set = %v, want %v", gotTLS != nil, tt.wantTLS)
			}
		})
	}
}

func mechNames(ms []sasl.Mechanism) []string {
	var out []string
	for _, m := range ms {
		out = append(out, m.Name())
	}
	return out
}

// TestKafkaWire drives a real kgo client against a fake broker over TLS and
// SASL, so the options are proven on the wire, not only in the option list.
func TestKafkaWire(t *testing.T) {
	p := newTestPKI(t)
	tests := []struct {
		name       string
		mtls       bool // broker requires a client certificate
		saslUser   string
		saslPass   string
		cfg        Kafka
		wantOK     bool
		wantAuthOK int
	}{
		{
			name:   "tls with private CA",
			cfg:    Kafka{Protocol: SSL, TLS: TLS{CAFile: p.caFile}},
			wantOK: true,
		},
		{
			name:   "mutual tls",
			mtls:   true,
			cfg:    Kafka{Protocol: SSL, TLS: TLS{CAFile: p.caFile, CertFile: p.clientCertFile, KeyFile: p.clientKeyFile}},
			wantOK: true,
		},
		{
			name: "mutual tls without client cert",
			mtls: true,
			cfg:  Kafka{Protocol: SSL, TLS: TLS{CAFile: p.caFile}},
		},
		{
			name: "untrusted server certificate",
			cfg:  Kafka{Protocol: SSL, TLS: TLS{CAFile: p.otherCAFile}},
		},
		{
			name:     "sasl_ssl plain",
			saslUser: "agent", saslPass: testPassword,
			cfg:        Kafka{Protocol: SASLSSL, Mechanism: MechPlain, Username: "agent", Password: testPassword, TLS: TLS{CAFile: p.caFile}},
			wantOK:     true,
			wantAuthOK: 1,
		},
		{
			name:     "sasl_ssl plain wrong password",
			saslUser: "agent", saslPass: testPassword,
			cfg: Kafka{Protocol: SASLSSL, Mechanism: MechPlain, Username: "agent", Password: "wrong", TLS: TLS{CAFile: p.caFile}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newFakeBroker(t, p.serverTLS(tt.mtls), tt.saslUser, tt.saslPass)
			opts, err := tt.cfg.Opts()
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
			err = cl.Ping(ctx)
			mechs, authOK, _, metadata, connErrs := b.stats()
			if tt.wantOK {
				if err != nil {
					t.Fatalf("ping: %v (broker errors: %v)", err, connErrs)
				}
				if metadata == 0 {
					t.Errorf("broker served no metadata")
				}
			} else {
				if err == nil {
					t.Fatal("ping succeeded, want failure")
				}
				if metadata != 0 {
					t.Errorf("broker served metadata to a client that should have been refused")
				}
				if strings.Contains(err.Error(), testPassword) {
					t.Errorf("error leaks password: %v", err)
				}
			}
			if authOK != tt.wantAuthOK {
				t.Errorf("SASL auth ok = %d, want %d (mechanisms %v)", authOK, tt.wantAuthOK, mechs)
			}
		})
	}
}

// TestKafkaPlaintextUnaffected checks the zero config still talks plain TCP,
// so the dev stack keeps working.
func TestKafkaPlaintextUnaffected(t *testing.T) {
	b := newFakeBroker(t, nil, "", "")
	opts, err := Kafka{}.Opts()
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
		t.Fatal(err)
	}
}

func TestHTTPClientValidation(t *testing.T) {
	p := newTestPKI(t)
	tests := []struct {
		name    string
		url     string
		in      HTTP
		wantErr bool
	}{
		{name: "dev http, no login", url: "http://localhost:8081"},
		{name: "https, no login", url: "https://sr.example.com"},
		{name: "https with login and CA", url: "https://sr.example.com", in: HTTP{Username: "k", Password: "s", TLS: TLS{CAFile: p.caFile}}},
		{name: "basic auth over http refused", url: "http://sr.example.com", in: HTTP{Username: "k", Password: "s"}, wantErr: true},
		{name: "tls files over http refused", url: "http://sr.example.com", in: HTTP{TLS: TLS{CAFile: p.caFile}}, wantErr: true},
		{name: "username only", url: "https://sr.example.com", in: HTTP{Username: "k"}, wantErr: true},
		{name: "password only", url: "https://sr.example.com", in: HTTP{Password: "s"}, wantErr: true},
		{name: "userinfo in URL refused", url: "https://k:" + testPassword + "@sr.example.com", wantErr: true},
		{name: "bad scheme", url: "ftp://sr.example.com", wantErr: true},
		{name: "no host", url: "https://", wantErr: true},
		{name: "unparseable URL with secret", url: "https://k:" + testPassword + "@sr.example.com:bad port", wantErr: true},
		{name: "bad CA file", url: "https://sr.example.com", in: HTTP{TLS: TLS{CAFile: p.notPEMFile}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc, err := tt.in.Client(tt.url, 0)
			if tt.wantErr {
				if err == nil {
					t.Fatal("want error")
				}
				if !errors.Is(err, ErrInvalid) {
					t.Errorf("err = %v, want ErrInvalid", err)
				}
				if strings.Contains(err.Error(), testPassword) {
					t.Errorf("error leaks password: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if hc.Timeout != DefaultHTTPTimeout {
				t.Errorf("timeout = %v, want default", hc.Timeout)
			}
		})
	}
}

func TestHTTPClientTLSAndBasicAuth(t *testing.T) {
	p := newTestPKI(t)
	var gotAuth atomic.Value
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, pw, ok := r.BasicAuth()
		gotAuth.Store(fmt.Sprintf("%v:%s:%s", ok, u, pw))
		if !ok || u != "api-key" || pw != testPassword {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = p.serverTLS(true)
	// The refused handshakes below are expected; keep them out of test output.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	defer srv.Close()

	good := HTTP{Username: "api-key", Password: testPassword, TLS: TLS{CAFile: p.caFile, CertFile: p.clientCertFile, KeyFile: p.clientKeyFile}}
	hc, err := good.Client(srv.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if hc.Timeout != 5*time.Second {
		t.Errorf("timeout = %v", hc.Timeout)
	}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/subjects", nil)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, server saw %v", resp.StatusCode, gotAuth.Load())
	}
	if req.Header.Get("Authorization") != "" {
		t.Errorf("caller's request was modified")
	}

	// Wrong CA: the handshake must fail before any credential is sent.
	gotAuth.Store("none")
	bad := good
	bad.TLS.CAFile = p.otherCAFile
	hc, err = bad.Client(srv.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = hc.Get(srv.URL + "/subjects")
	if err == nil {
		resp.Body.Close()
		t.Fatal("request to an untrusted server succeeded")
	}
	if gotAuth.Load() != "none" {
		t.Errorf("server received a request: %v", gotAuth.Load())
	}

	// No client certificate: the server must refuse the connection.
	noCert := good
	noCert.TLS.CertFile, noCert.TLS.KeyFile = "", ""
	hc, err = noCert.Client(srv.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := hc.Get(srv.URL + "/subjects"); err == nil {
		resp.Body.Close()
		t.Fatal("request without client certificate succeeded")
	}
}

// TestBasicAuthNotSentToOtherHosts checks a redirect to another host does not
// carry the credentials.
func TestBasicAuthNotSentToOtherHosts(t *testing.T) {
	p := newTestPKI(t)
	var otherAuth atomic.Value
	otherAuth.Store("")
	other := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherAuth.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	other.TLS = p.serverTLS(false)
	other.StartTLS()
	defer other.Close()

	main := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := r.BasicAuth(); !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// 127.0.0.1 vs localhost: same machine, different host for the client.
		http.Redirect(w, r, strings.Replace(other.URL, "127.0.0.1", "localhost", 1)+"/x", http.StatusFound)
	}))
	main.TLS = p.serverTLS(false)
	main.StartTLS()
	defer main.Close()

	hc, err := HTTP{Username: "api-key", Password: testPassword, TLS: TLS{CAFile: p.caFile}}.Client(main.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Get(main.URL + "/subjects")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got := otherAuth.Load(); got != "" {
		t.Errorf("credentials leaked to another host: %q", got)
	}
}
