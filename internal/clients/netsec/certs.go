package netsec

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	certDialTimeout = 5 * time.Second
	maxCertNames    = 5
)

// CertInfo is what a server presented in its TLS handshake.
type CertInfo struct {
	Subject  string    `json:"subject"` // common name
	Issuer   string    `json:"issuer"`  // common name
	NotAfter time.Time `json:"not_after"`
	DNSNames []string  `json:"dns_names,omitempty"` // at most 5
	// DaysLeft is whole days until NotAfter; negative once expired.
	DaysLeft int `json:"days_left"`
	// Verified is true when the chain verifies against the configured roots
	// for the dialed host name.
	Verified bool `json:"verified"`
	// NameMatches reports whether the certificate is valid for the dialed
	// name (a SAN DNS name or a SAN IP address; like Go's own TLS clients it
	// never falls back to the common name). It separates "the name does not match" from
	// "the chain is not trusted", which a caller must tell apart: the first is
	// usually a server misconfiguration, the second a missing CA.
	NameMatches bool `json:"name_matches"`
}

// CertExpiry dials addr ("host:port"), reads the leaf certificate and
// reports its expiry. It reads, it does not trust: the handshake skips
// verification so an expired or unknown-CA certificate can still be read, and
// Verified says separately whether the configured roots accept it. The
// handshake presents no client certificate and sends no credential. cfg may
// be nil (system roots); its RootCAs and ServerName are used for the
// Verified check only.
func CertExpiry(ctx context.Context, addr string, cfg *tls.Config) (CertInfo, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return CertInfo{}, fmt.Errorf("netsec: cert expiry %q: %w", addr, err)
	}
	var roots *x509.CertPool
	serverName := host
	if cfg != nil {
		roots = cfg.RootCAs
		if cfg.ServerName != "" {
			serverName = cfg.ServerName
		}
	}
	// A fresh dialer: the handshake happens in CertFromConn, which presents no
	// client certificate.
	d := &net.Dialer{Timeout: certDialTimeout}
	cctx, cancel := context.WithTimeout(ctx, certDialTimeout)
	defer cancel()
	conn, err := d.DialContext(cctx, "tcp", addr)
	if err != nil {
		return CertInfo{}, fmt.Errorf("netsec: cert expiry %s: %w", addr, err)
	}
	defer conn.Close()
	info, err := CertFromConn(cctx, conn, serverName, roots)
	if err != nil {
		return CertInfo{}, fmt.Errorf("netsec: cert expiry %s: %w", addr, err)
	}
	return info, nil
}

// CertFromConn performs the TLS handshake on conn, which the caller dialed from
// its own network position, and reports the certificate the server presented.
// The setup runner's probe dials through the runner's guarded dialer and then
// reads the certificate here, so both callers share one set of TLS rules.
// serverName is the name checked against the certificate; roots are the CAs the
// Verified verdict uses (nil = the system roots). It presents no client
// certificate and sends no credential. The caller closes conn.
func CertFromConn(ctx context.Context, conn net.Conn, serverName string, roots *x509.CertPool) (CertInfo, error) {
	tc := tls.Client(conn, handshakeManualVerifyConfig(serverName))
	if err := tc.HandshakeContext(ctx); err != nil {
		return CertInfo{}, fmt.Errorf("netsec: tls handshake: %w", err)
	}
	peers := tc.ConnectionState().PeerCertificates
	if len(peers) == 0 {
		return CertInfo{}, errors.New("netsec: the server presented no certificate")
	}
	return certInfo(peers, serverName, roots), nil
}

// handshakeManualVerifyConfig is the handshake's TLS config: the standard
// verification is off because the probe reads the certificate rather than
// trusting the connection; certInfo verifies the chain and the name by hand
// (leaf.Verify, VerifyHostname) and reports the real verdict. Nothing but
// the handshake ever runs on the connection.
func handshakeManualVerifyConfig(serverName string) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // reading the certificate, not trusting it; Verified reports the real verdict
		ServerName:         serverName,
		MinVersion:         tls.VersionTLS12,
	}
}

// certInfo builds the report for the peer chain: the leaf's names and dates,
// whether the chain and the dialed name verify, and the leaf's SAN DNS names
// (at most maxCertNames).
func certInfo(peers []*x509.Certificate, serverName string, roots *x509.CertPool) CertInfo {
	leaf := peers[0]
	inter := x509.NewCertPool()
	for _, c := range peers[1:] {
		inter.AddCert(c)
	}
	_, verr := leaf.Verify(x509.VerifyOptions{DNSName: serverName, Roots: roots, Intermediates: inter})
	info := CertInfo{
		Subject: leaf.Subject.CommonName, Issuer: leaf.Issuer.CommonName, NotAfter: leaf.NotAfter,
		DaysLeft: int(time.Until(leaf.NotAfter).Hours() / 24), Verified: verr == nil,
		NameMatches: leaf.VerifyHostname(serverName) == nil,
	}
	if time.Now().After(leaf.NotAfter) && info.DaysLeft == 0 {
		info.DaysLeft = -1 // expired earlier today still reads as expired
	}
	names := leaf.DNSNames
	if len(names) > maxCertNames {
		names = names[:maxCertNames]
	}
	info.DNSNames = append([]string(nil), names...)
	return info
}
