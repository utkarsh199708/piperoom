package netsec

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// testPKI is a throwaway CA with a server cert for 127.0.0.1/localhost and a
// client cert, written as PEM files to a temp dir. Keys are generated per
// test run so no key material is checked in.
type testPKI struct {
	caPool             *x509.CertPool
	serverCert         tls.Certificate
	caFile             string
	clientCertFile     string
	clientKeyFile      string
	otherCAFile        string // a CA that signed nothing here
	notPEMFile         string
	clientCertWrongKey string // key file that does not match clientCertFile
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	caKey, caCert, caDER := newCA(t, "piperoom test CA")
	_, _, otherDER := newCA(t, "other CA")

	srvKey := newKey(t)
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	cliKey := newKey(t)
	cliTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "piperoom-agent"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTmpl, caCert, &cliKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	p := &testPKI{caPool: x509.NewCertPool()}
	p.caPool.AddCert(caCert)
	p.serverCert = tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}
	p.caFile = writePEM(t, dir, "ca.pem", "CERTIFICATE", caDER)
	p.otherCAFile = writePEM(t, dir, "other-ca.pem", "CERTIFICATE", otherDER)
	p.clientCertFile = writePEM(t, dir, "client.pem", "CERTIFICATE", cliDER)
	p.clientKeyFile = writePEM(t, dir, "client-key.pem", "PRIVATE KEY", marshalKey(t, cliKey))
	p.clientCertWrongKey = writePEM(t, dir, "wrong-key.pem", "PRIVATE KEY", marshalKey(t, newKey(t)))
	p.notPEMFile = filepath.Join(dir, "not-pem.txt")
	if err := os.WriteFile(p.notPEMFile, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// serverTLS returns a server config; requireClient turns on mutual TLS.
func (p *testPKI) serverTLS(requireClient bool) *tls.Config {
	cfg := &tls.Config{Certificates: []tls.Certificate{p.serverCert}, MinVersion: tls.VersionTLS12}
	if requireClient {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = p.caPool
	}
	return cfg
}

func newCA(t *testing.T, name string) (*ecdsa.PrivateKey, *x509.Certificate, []byte) {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return key, cert, der
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func marshalKey(t *testing.T, k *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// fakeBroker speaks just enough of the Kafka protocol for kgo.Client.Ping:
// ApiVersions, SASLHandshake, SASLAuthenticate (PLAIN) and a broker-only
// Metadata. It records what it saw so tests can assert on the wire.
type fakeBroker struct {
	ln       net.Listener
	saslUser string
	saslPass string

	mu        sync.Mutex
	mechs     []string // mechanisms requested in SASLHandshake
	authOK    int      // successful SASL authentications
	authFail  int
	metadata  int // Metadata requests served (only after auth when SASL is on)
	connErrs  []error
	wantsSASL bool
}

// newFakeBroker listens on 127.0.0.1; tlsCfg nil means plaintext. A non-empty
// user turns on SASL PLAIN with that user/pass.
func newFakeBroker(t *testing.T, tlsCfg *tls.Config, user, pass string) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if tlsCfg != nil {
		ln = tls.NewListener(ln, tlsCfg)
	}
	b := &fakeBroker{ln: ln, saslUser: user, saslPass: pass, wantsSASL: user != ""}
	done := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				if err := b.serve(c); err != nil && !errors.Is(err, io.EOF) {
					b.mu.Lock()
					b.connErrs = append(b.connErrs, err)
					b.mu.Unlock()
				}
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
		wg.Wait()
	})
	return b
}

func (b *fakeBroker) addr() string { return b.ln.Addr().String() }

func (b *fakeBroker) stats() (mechs []string, authOK, authFail, metadata int, connErrs []error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.mechs...), b.authOK, b.authFail, b.metadata, append([]error(nil), b.connErrs...)
}

func (b *fakeBroker) serve(c net.Conn) error {
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if tc, ok := c.(*tls.Conn); ok {
		if err := tc.Handshake(); err != nil {
			return fmt.Errorf("tls handshake: %w", err)
		}
	}
	authed := !b.wantsSASL
	for {
		var size int32
		if err := binary.Read(c, binary.BigEndian, &size); err != nil {
			return err
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(c, buf); err != nil {
			return err
		}
		key := int16(binary.BigEndian.Uint16(buf[0:]))
		ver := int16(binary.BigEndian.Uint16(buf[2:]))
		corr := binary.BigEndian.Uint32(buf[4:])
		cidLen := int(int16(binary.BigEndian.Uint16(buf[8:])))
		body := buf[10:]
		if cidLen > 0 {
			body = body[cidLen:]
		}
		req := kmsg.RequestForKey(key)
		if req == nil {
			return fmt.Errorf("unknown key %d", key)
		}
		req.SetVersion(ver)
		if req.IsFlexible() {
			body = skipTags(body)
		}
		if err := req.ReadFrom(body); err != nil {
			return fmt.Errorf("parse key %d v%d: %w", key, ver, err)
		}
		if !authed && key != kmsg.ApiVersions.Int16() && key != kmsg.SASLHandshake.Int16() && key != kmsg.SASLAuthenticate.Int16() {
			return fmt.Errorf("key %d before SASL auth", key)
		}

		resp := req.ResponseKind()
		resp.SetVersion(ver)
		switch r := req.(type) {
		case *kmsg.ApiVersionsRequest:
			ar := resp.(*kmsg.ApiVersionsResponse)
			for _, k := range []kmsg.Key{kmsg.ApiVersions, kmsg.SASLHandshake, kmsg.SASLAuthenticate, kmsg.Metadata} {
				ar.ApiKeys = append(ar.ApiKeys, kmsg.ApiVersionsResponseApiKey{
					ApiKey: k.Int16(), MinVersion: 0, MaxVersion: kmsg.RequestForKey(k.Int16()).MaxVersion(),
				})
			}
		case *kmsg.SASLHandshakeRequest:
			b.mu.Lock()
			b.mechs = append(b.mechs, r.Mechanism)
			b.mu.Unlock()
			hr := resp.(*kmsg.SASLHandshakeResponse)
			hr.SupportedMechanisms = []string{"PLAIN"}
			if r.Mechanism != "PLAIN" {
				hr.ErrorCode = 33 // UNSUPPORTED_SASL_MECHANISM
			}
		case *kmsg.SASLAuthenticateRequest:
			ar := resp.(*kmsg.SASLAuthenticateResponse)
			want := "\x00" + b.saslUser + "\x00" + b.saslPass
			b.mu.Lock()
			if string(r.SASLAuthBytes) == want {
				b.authOK++
				authed = true
			} else {
				b.authFail++
				ar.ErrorCode = 58 // SASL_AUTHENTICATION_FAILED
				msg := "bad credentials"
				ar.ErrorMessage = &msg
			}
			b.mu.Unlock()
		case *kmsg.MetadataRequest:
			b.mu.Lock()
			b.metadata++
			b.mu.Unlock()
			host, port, _ := net.SplitHostPort(b.addr())
			p, _ := strconv.Atoi(port)
			mr := resp.(*kmsg.MetadataResponse)
			mr.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0, Host: host, Port: int32(p)}}
		default:
			return fmt.Errorf("unhandled key %d", key)
		}

		out := binary.BigEndian.AppendUint32(nil, corr)
		// ApiVersions always answers with response header v0 (KIP-511).
		if resp.IsFlexible() && key != kmsg.ApiVersions.Int16() {
			out = append(out, 0) // empty tagged fields
		}
		out = resp.AppendTo(out)
		frame := binary.BigEndian.AppendUint32(nil, uint32(len(out)))
		if _, err := c.Write(append(frame, out...)); err != nil {
			return err
		}
		if !authed && key == kmsg.SASLAuthenticate.Int16() {
			return nil // failed login: close like a real broker
		}
	}
}

// skipTags skips a request header's tagged-field section.
func skipTags(b []byte) []byte {
	n, used := binary.Uvarint(b)
	b = b[used:]
	for range n {
		_, used = binary.Uvarint(b) // tag
		b = b[used:]
		size, used := binary.Uvarint(b)
		b = b[used+int(size):]
	}
	return b
}
