package testenv

import (
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"piperoom/internal/stacklock"
)

const (
	// OracleDSNEnv names the go-ora URL of the `oracle` compose profile, the
	// variable the U9a client's integration tests read too.
	OracleDSNEnv = "ORACLE_DSN"
	// DefaultOracleDSN is the profile's SYSTEM login on FREEPDB1
	// (dev/docker-compose.yml service oracle; dev-only password, AGENTS.md
	// section 3). SYSTEM reads the V$ and DBA_ views the prerequisite checks use.
	DefaultOracleDSN = "oracle://system:piperoom-dev@localhost:1521/FREEPDB1"
)

// OracleDSNFromEnv returns ORACLE_DSN from getenv, or DefaultOracleDSN.
func OracleDSNFromEnv(getenv func(string) string) string {
	if v := strings.TrimSpace(getenv(OracleDSNEnv)); v != "" {
		return v
	}
	return DefaultOracleDSN
}

// oracleDefaultPort is the listener port go-ora assumes when the URL names
// none.
const oracleDefaultPort = "1521"

// oracleHost is the host:port of a go-ora URL, with the port defaulted to
// 1521 as go-ora does ("" when the URL has no host).
func oracleHost(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	port := u.Port()
	if port == "" {
		port = oracleDefaultPort
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// RequireOracle returns the DSN of the `oracle` profile, or skips the test
// when the shared stack is locked by someone else or the listener does not
// answer (`docker compose -f dev/docker-compose.yml --profile oracle up -d
// --wait oracle` starts it). The skip message never carries the DSN, which
// holds a password.
func RequireOracle(t *testing.T) string {
	t.Helper()
	if msg := lockConflict(stacklock.New(stacklock.Path()), os.Getenv(stackOwner)); msg != "" {
		t.Skipf("testenv: %s", msg)
	}
	dsn := OracleDSNFromEnv(os.Getenv)
	host := oracleHost(dsn)
	if host == "" {
		t.Fatalf("testenv: %s is not a go-ora URL (oracle://user:pass@host:port/service)", OracleDSNEnv)
	}
	conn, err := net.DialTimeout("tcp", host, 2*time.Second)
	if err != nil {
		t.Skipf("testenv: oracle profile not reachable at %s (docker compose -f dev/docker-compose.yml --profile oracle up -d --wait oracle): %v", host, err)
	}
	_ = conn.Close()
	return dsn
}
