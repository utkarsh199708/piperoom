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
	// A fresh config: no client certificates, no verification during the read.
	dial := &tls.Config{InsecureSkipVerify: true, ServerName: serverName, MinVersion: tls.VersionTLS12} //nolint:gosec // reading the certificate, not trusting it
	d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: certDialTimeout}, Config: dial}
	cctx, cancel := context.WithTimeout(ctx, certDialTimeout)
	defer cancel()
	conn, err := d.DialContext(cctx, "tcp", addr)
	if err != nil {
		return CertInfo{}, fmt.Errorf("netsec: cert expiry %s: %w", addr, err)
	}
	defer conn.Close()
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return CertInfo{}, errors.New("netsec: cert expiry: not a TLS connection")
	}
	peers := tc.ConnectionState().PeerCertificates
	if len(peers) == 0 {
		return CertInfo{}, fmt.Errorf("netsec: cert expiry %s: the server presented no certificate", addr)
	}
	leaf := peers[0]
	inter := x509.NewCertPool()
	for _, c := range peers[1:] {
		inter.AddCert(c)
	}
	_, verr := leaf.Verify(x509.VerifyOptions{DNSName: serverName, Roots: roots, Intermediates: inter})
	info := CertInfo{
		Subject: leaf.Subject.CommonName, Issuer: leaf.Issuer.CommonName, NotAfter: leaf.NotAfter,
		DaysLeft: int(time.Until(leaf.NotAfter).Hours() / 24), Verified: verr == nil,
	}
	if time.Now().After(leaf.NotAfter) && info.DaysLeft == 0 {
		info.DaysLeft = -1 // expired earlier today still reads as expired
	}
	names := leaf.DNSNames
	if len(names) > maxCertNames {
		names = names[:maxCertNames]
	}
	info.DNSNames = append([]string(nil), names...)
	return info, nil
}
