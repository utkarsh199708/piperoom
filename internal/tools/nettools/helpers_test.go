package nettools

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// listenLoopback starts a TCP listener on loopback that accepts and closes
// connections until the test ends.
func listenLoopback(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	return ln.Addr().String()
}

// deadLoopbackAddr returns a loopback address nothing listens on.
func deadLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// certOpts describes the test server's certificate.
type certOpts struct {
	dns       []string
	ips       []string
	notBefore time.Time
	notAfter  time.Time
}

// tlsServer serves a self-signed certificate and counts the client
// certificates a client presented, so a test can prove the probe sends none.
// caFile is the server's certificate as a PEM bundle, for settings that name a
// CA file (KAFKA_TLS_CA_FILE).
func tlsServer(t *testing.T, o certOpts) (addr string, roots *x509.CertPool, caFile string, clientCerts *atomic.Int32) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-endpoint"},
		DNSNames:              o.dns,
		NotBefore:             o.notBefore,
		NotAfter:              o.notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	for _, ip := range o.ips {
		if parsed := net.ParseIP(ip); parsed != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, parsed)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	count := new(atomic.Int32)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		ClientAuth:   tls.RequestClientCert,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			count.Add(int32(len(raw)))
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = c.(*tls.Conn).Handshake(); _ = c.Close() }()
		}
	}()
	pemPath := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(pemPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	return ln.Addr().String(), pool, pemPath, count
}

// fakeLocation replaces the probe's network position: tests steer lookups and
// dials, and capture the deadline a step was given.
type fakeLocation struct {
	lookup       func(ctx context.Context, host string) ([]netip.Addr, error)
	dial         func(ctx context.Context, network, address string) (net.Conn, error)
	dialDeadline time.Duration
}

func (f *fakeLocation) Lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	if f.lookup != nil {
		return f.lookup(ctx, host)
	}
	return ProcessLocation{}.Lookup(ctx, host)
}

func (f *fakeLocation) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	if dl, ok := ctx.Deadline(); ok {
		f.dialDeadline = time.Until(dl)
	}
	if f.dial != nil {
		return f.dial(ctx, network, address)
	}
	return ProcessLocation{}.Dial(ctx, network, address)
}

// fakeConnect implements clients.ConnectClient and kafkatools.ConfigValidator.
// It never writes.
type fakeConnect struct {
	configFn   func(context.Context, string) (map[string]string, error)
	validateFn func(context.Context, string, map[string]string) (connect.ValidateResult, error)
}

var errFakeWrite = errors.New("fakeConnect: write called by a read-only path")

func (f *fakeConnect) ListConnectors(context.Context) ([]string, error) { return nil, errFakeWrite }
func (f *fakeConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{}, errFakeWrite
}
func (f *fakeConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	if f.configFn == nil {
		return nil, errFakeWrite
	}
	return f.configFn(ctx, name)
}
func (f *fakeConnect) RestartTask(context.Context, string, int) error { return errFakeWrite }
func (f *fakeConnect) RestartConnector(context.Context, string) error { return errFakeWrite }
func (f *fakeConnect) ValidateConfig(ctx context.Context, class string, cfg map[string]string) (connect.ValidateResult, error) {
	if f.validateFn == nil {
		return connect.ValidateResult{}, errFakeWrite
	}
	return f.validateFn(ctx, class, cfg)
}

// PluginConfigDef completes kafkatools.ConfigValidator; net_probe never calls it.
func (f *fakeConnect) PluginConfigDef(context.Context, string) ([]connect.ConfigKeyDef, error) {
	return nil, errFakeWrite
}

// fakeKafka implements clients.KafkaAdmin and clients.ClusterReader. Reads
// only; a write would fail the test by falling to errFakeWrite.
type fakeKafka struct {
	health  clients.ClusterHealth
	healthE error
	calls   int
}

func (f *fakeKafka) ClusterHealth(context.Context, ...string) (clients.ClusterHealth, error) {
	f.calls++
	return f.health, f.healthE
}
func (f *fakeKafka) Topics(context.Context) ([]string, error) { return nil, errFakeWrite }
func (f *fakeKafka) TopicOffsets(context.Context, string) ([]clients.PartitionOffsets, error) {
	return nil, errFakeWrite
}
func (f *fakeKafka) ConsumerGroups(context.Context) ([]string, error) { return nil, errFakeWrite }
func (f *fakeKafka) GroupLag(context.Context, string) (clients.GroupLag, error) {
	return clients.GroupLag{}, errFakeWrite
}
func (f *fakeKafka) FindByKey(context.Context, string, string, time.Time, int) ([]clients.RecordRef, error) {
	return nil, errFakeWrite
}

// scoped returns a context scoped to one pipeline.
func scoped(p owners.Pipeline) context.Context {
	return tools.WithPipeline(context.Background(), p)
}
