// Command certgen writes a throwaway test CA and a server certificate for
// localhost, for the OIDC end-to-end stack in test/e2e/oidc. Caddy serves the
// server certificate on both TLS ports; the piperoom container (SSL_CERT_FILE)
// and the Go test trust ca.crt. Nothing here is a real secret: the key is
// generated on every run, lives in a gitignored folder and is valid for 7 days.
//
//	go run ./test/e2e/oidc/certgen -out test/e2e/oidc/.certs
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func main() {
	out := flag.String("out", ".certs", "directory to write ca.crt, server.crt and server.key into")
	flag.Parse()
	if err := run(*out); err != nil {
		fmt.Fprintln(os.Stderr, "certgen:", err)
		os.Exit(1)
	}
}

func run(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("make %s: %w", dir, err)
	}
	now := time.Now()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("ca key: %w", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "piperoom e2e test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(7 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("ca cert: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fmt.Errorf("parse ca cert: %w", err)
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("server key: %w", err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(7 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("server cert: %w", err)
	}
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		return fmt.Errorf("server key: %w", err)
	}

	// 0644 on purpose: the piperoom image runs as uid 65532 and must read
	// ca.crt from a bind mount; Caddy reads the key. These are test-only files.
	files := []struct {
		name, typ string
		der       []byte
	}{
		{"ca.crt", "CERTIFICATE", caDER},
		{"server.crt", "CERTIFICATE", srvDER},
		{"server.key", "EC PRIVATE KEY", srvKeyDER},
	}
	for _, f := range files {
		b := pem.EncodeToMemory(&pem.Block{Type: f.typ, Bytes: f.der})
		if err := os.WriteFile(filepath.Join(dir, f.name), b, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", f.name, err)
		}
	}
	return nil
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return big.NewInt(time.Now().UnixNano())
	}
	return n
}
