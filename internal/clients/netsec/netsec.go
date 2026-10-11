// Package netsec builds encrypted and authenticated connections to a
// customer's Kafka brokers, Schema Registry and Kafka Connect.
//
// The dev stack needs none of this (plain TCP and plain HTTP, no login), so
// every zero value here means "no TLS, no login" and the dev defaults keep
// working. A real cluster usually needs one or more of:
//
//   - Kafka: TLS (security.protocol SSL), SASL login (SASL_SSL with PLAIN,
//     SCRAM-SHA-256 or SCRAM-SHA-512), a private CA, mutual TLS.
//   - Schema Registry / Connect: an https URL, HTTP basic auth (Confluent
//     Cloud API key + secret), a private CA, mutual TLS.
//
// The kafka, schemareg and connect clients all build on it.
//
// The package refuses settings that would send a password in clear text:
// SASL/PLAIN without TLS and basic auth to an http:// URL are errors, not
// warnings, because the agent runs against production clusters.
package netsec

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

// ErrInvalid marks a configuration that is incomplete or unsafe.
var ErrInvalid = errors.New("netsec: invalid configuration")

// Secret is a password or API secret. It prints as "[redacted]" with fmt and
// slog so a config struct can be logged without leaking it.
type Secret string

// String hides the value.
func (s Secret) String() string { return redacted(s) }

// GoString hides the value for %#v.
func (s Secret) GoString() string { return redacted(s) }

// LogValue hides the value for log/slog.
func (s Secret) LogValue() slog.Value { return slog.StringValue(redacted(s)) }

// MarshalText hides the value for encoding/json and similar encoders.
func (s Secret) MarshalText() ([]byte, error) { return []byte(redacted(s)), nil }

// Reveal returns the real value. Call it only where the secret is handed to
// the wire protocol.
func (s Secret) Reveal() string { return string(s) }

func redacted(s Secret) string {
	if s == "" {
		return ""
	}
	return "[redacted]"
}

// TLS describes how to verify the server and, optionally, which client
// certificate to present.
type TLS struct {
	// CAFile is a PEM bundle of CAs that sign the server certificate.
	// Empty means the system roots.
	CAFile string
	// CertFile and KeyFile are a PEM client certificate and key for mutual
	// TLS. Set both or neither.
	CertFile string
	KeyFile  string
	// ServerName overrides the name checked against the server certificate.
	// Empty means the host being dialed.
	ServerName string
}

// IsZero reports whether no TLS setting is given.
func (t TLS) IsZero() bool { return t == TLS{} }

// Config builds a *tls.Config that always verifies the server certificate.
// There is deliberately no "skip verify" switch: a self-signed cluster is
// handled by pointing CAFile at its CA.
func (t TLS) Config() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("netsec: reading CA file %s: %w", t.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%w: CA file %s has no PEM certificates", ErrInvalid, t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if (t.CertFile == "") != (t.KeyFile == "") {
		return nil, fmt.Errorf("%w: client certificate and key must be set together", ErrInvalid)
	}
	if t.CertFile != "" {
		// The error from LoadX509KeyPair names the files, never key bytes.
		pair, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("netsec: loading client certificate %s: %w", t.CertFile, err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}
