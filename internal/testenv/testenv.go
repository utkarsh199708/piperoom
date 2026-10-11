//go:build integration

// Package testenv helps integration tests use the dev stack (dev/docker-compose.yml).
// Start it with `go run ./cmd/devctl up` (or `reset` for a clean state).
package testenv

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/stacklock"
)

// Require returns the dev config, or skips the test when the stack is not
// reachable (Kafka Connect is the last service to come up) or when another
// agent holds the shared stack lock.
func Require(t *testing.T) config.Config {
	t.Helper()
	lk := stacklock.New(stacklock.Path())
	if msg := lockConflict(lk, os.Getenv(stackOwner)); msg != "" {
		t.Skipf("testenv: %s", msg)
	}

	cfg := config.Load()
	u, err := url.Parse(cfg.ConnectURL)
	if err != nil {
		t.Fatalf("testenv: bad CONNECT_URL %q: %v", cfg.ConnectURL, err)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
	if err != nil {
		t.Skipf("testenv: dev stack not reachable at %s (run `go run ./cmd/devctl up`): %v", u.Host, err)
	}
	_ = conn.Close()
	return cfg
}

// Name returns a unique name like "it_orders_3fa9c1" so parallel tests do not collide.
func Name(prefix string) string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return "it_" + prefix + "_" + hex.EncodeToString(b)
}

// WaitFor polls cond every 500 ms until it returns true, an error, or the timeout.
func WaitFor(t *testing.T, timeout time.Duration, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil {
			t.Fatalf("testenv: wait: %v", err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("testenv: condition not met within %s", timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
