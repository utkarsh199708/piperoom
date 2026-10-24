package runner_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// TestDigestCheckFailureRefusesToStart: an injected check that fails makes
// Run refuse to start before anything is dialed. The server never sees a
// request, and the returned error wraps the check's error so the caller can
// branch on it.
func TestDigestCheckFailureRefusesToStart(t *testing.T) {
	var hit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Store(true)
	}))
	defer srv.Close()

	sentinel := errors.New("check_test: injected digest failure")
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true,
		Stdout: io.Discard, Log: quiet(),
		Check: func() error { return sentinel },
	})
	if err != nil {
		t.Fatal(err)
	}
	err = r.Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run = %v, want it to wrap the injected check error", err)
	}
	if hit.Load() {
		t.Fatal("the runner dialed the server although the digest check failed")
	}
}

// TestDigestCheckRunsBeforePairing: the injected check completes before the
// runner dials, and a nil result lets the pairing proceed. The server
// observes the check's flag at accept time, which happens strictly after the
// dial, so it must already be set.
func TestDigestCheckRunsBeforePairing(t *testing.T) {
	var checked atomic.Bool

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		if !checked.Load() {
			t.Error("the server was dialed before the digest check ran")
		}
		if _, _, err := handshake(conn, ctx, protocol.CurrentMinor, "tok"); err != nil {
			t.Errorf("handshake: %v", err)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true,
		Stdout: io.Discard, Log: quiet(),
		Check: func() error { checked.Store(true); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if !checked.Load() {
		t.Fatal("the digest check never ran")
	}
}

// TestDefaultDigestCheckAllowsPairing: with no injected check the runner runs
// the real knowledge.Check (U1-24) and still pairs, proving the default check
// passes on the shipped index and does not block the pairing.
func TestDefaultDigestCheckAllowsPairing(t *testing.T) {
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		if _, _, err := handshake(conn, ctx, protocol.CurrentMinor, "tok"); err != nil {
			t.Errorf("handshake: %v", err)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
}
