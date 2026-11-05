package connprobe

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// loopGuard allows the loopback test servers.
func loopGuard() *Guard { return &Guard{AllowLoopback: true} }

// check returns the check with id, failing the test when it is missing.
func check(t *testing.T, r Result, id string) Check {
	t.Helper()
	for _, c := range r.Checks {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("check %q missing in %+v", id, r.Checks)
	return Check{}
}

// wantChecks asserts the status of each listed check, in order.
func wantChecks(t *testing.T, r Result, want ...string) {
	t.Helper()
	if len(want)%2 != 0 {
		t.Fatal("wantChecks needs id, status pairs")
	}
	if len(r.Checks) != len(want)/2 {
		t.Errorf("got %d checks, want %d: %s", len(r.Checks), len(want)/2, mustJSON(t, r))
	}
	for i := 0; i+1 < len(want); i += 2 {
		if i/2 < len(r.Checks) && r.Checks[i/2].ID != want[i] {
			t.Errorf("check %d is %q, want %q", i/2, r.Checks[i/2].ID, want[i])
		}
		if c := check(t, r, want[i]); c.Status != want[i+1] {
			t.Errorf("check %s: status %s, want %s (problem %q, detail %q)", want[i], c.Status, want[i+1], c.Problem, c.Detail)
		}
	}
}

func mustJSON(t *testing.T, r Result) string {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// assertNoSecret fails when any secret appears in the marshalled result.
func assertNoSecret(t *testing.T, r Result, secrets ...string) {
	t.Helper()
	s := mustJSON(t, r)
	for _, sec := range secrets {
		if sec != "" && strings.Contains(s, sec) {
			t.Errorf("result leaks %q: %s", sec, s)
		}
	}
}

// selfSigned makes a certificate for 127.0.0.1 and localhost, and writes it
// as a PEM CA file.
func selfSigned(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "connprobe test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, path
}
