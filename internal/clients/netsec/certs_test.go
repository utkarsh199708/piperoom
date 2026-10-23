package netsec

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// certServer serves a self-signed certificate valid for [notBefore, notAfter]
// and counts the client certificates it was shown.
func certServer(t *testing.T, notBefore, notAfter time.Time) (addr string, caPool *x509.CertPool, clientCerts *atomic.Int32) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "kafka-1"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	caPool = x509.NewCertPool()
	caPool.AddCert(leaf)
	clientCerts = new(atomic.Int32)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		ClientAuth:   tls.RequestClientCert,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			clientCerts.Add(int32(len(raw)))
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = c.(*tls.Conn).Handshake(); c.Close() }()
		}
	}()
	return ln.Addr().String(), caPool, clientCerts
}

func TestCertExpiryNoClientCert(t *testing.T) {
	now := time.Now()
	addr, pool, clientCerts := certServer(t, now.Add(-time.Hour), now.Add(12*24*time.Hour+time.Hour))
	// A config carrying a client certificate must not leak it into the read.
	cfg := &tls.Config{RootCAs: pool, ServerName: "localhost", Certificates: []tls.Certificate{{Certificate: [][]byte{{1}}}}}
	info, err := CertExpiry(context.Background(), addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if info.Subject != "kafka-1" || info.DaysLeft != 12 || !info.Verified || len(info.DNSNames) != 1 {
		t.Fatalf("%+v", info)
	}
	if n := clientCerts.Load(); n != 0 {
		t.Fatalf("server saw %d client certificates", n)
	}
	// Unknown CA: still readable, not verified.
	info, err = CertExpiry(context.Background(), addr, &tls.Config{RootCAs: x509.NewCertPool(), ServerName: "localhost"})
	if err != nil || info.Verified {
		t.Fatalf("err=%v info=%+v", err, info)
	}
}

func TestCertExpiryExpired(t *testing.T) {
	now := time.Now()
	addr, pool, _ := certServer(t, now.Add(-30*24*time.Hour), now.Add(-3*24*time.Hour))
	info, err := CertExpiry(context.Background(), addr, &tls.Config{RootCAs: pool, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	if info.DaysLeft > -3 || info.Verified || !info.NameMatches {
		t.Fatalf("%+v", info)
	}
}

// CertFromConn reads a certificate on a connection the caller dialed: the name
// check reports SANs by IP as well as by name, and a name the certificate does
// not carry is reported as such, not as an untrusted chain.
func TestCertFromConnNameAndIPSAN(t *testing.T) {
	now := time.Now()
	addr, pool, _ := certServer(t, now.Add(-time.Hour), now.Add(48*time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	info, err := CertFromConn(ctx, conn, "localhost", pool)
	_ = conn.Close()
	if err != nil || !info.Verified || !info.NameMatches || info.DaysLeft != 1 {
		t.Fatalf("err=%v info=%+v", err, info)
	}
	conn, err = net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	info, err = CertFromConn(ctx, conn, "127.0.0.1", pool)
	_ = conn.Close()
	if err != nil || !info.Verified || !info.NameMatches {
		t.Fatalf("IP SAN: err=%v info=%+v", err, info)
	}
	conn, err = net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	info, err = CertFromConn(ctx, conn, "elsewhere.example", pool)
	_ = conn.Close()
	if err != nil || info.Verified || info.NameMatches {
		t.Fatalf("wrong name: err=%v info=%+v", err, info)
	}
}

func TestCertExpiryErrors(t *testing.T) {
	if _, err := CertExpiry(context.Background(), "no-port", nil); err == nil {
		t.Error("address without port accepted")
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	if _, err := CertExpiry(context.Background(), addr, nil); err == nil {
		t.Error("closed port: want error")
	}
	// A plain-text server never completes a handshake.
	pl, _ := net.Listen("tcp", "127.0.0.1:0")
	defer pl.Close()
	go func() {
		c, err := pl.Accept()
		if err == nil {
			c.Write([]byte("not tls\n"))
			c.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := CertExpiry(ctx, pl.Addr().String(), nil); err == nil {
		t.Error("plain-text server: want error")
	}
}
