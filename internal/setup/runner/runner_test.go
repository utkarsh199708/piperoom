package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// mustFrame marshals a Frame; json.Marshal cannot fail for these types.
func mustFrame(f protocol.Frame) []byte {
	b, err := json.Marshal(f)
	if err != nil {
		panic("runner_test: " + err.Error())
	}
	return b
}

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// runServer starts an httptest server whose handler runs fn with the accepted
// WebSocket and request context, then closes the socket and signals done.
func runServer(t *testing.T, fn func(conn *websocket.Conn, ctx context.Context)) (*httptest.Server, <-chan struct{}) {
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			close(done)
			return
		}
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, ""); close(done) }()
		fn(conn, r.Context())
	}))
	return srv, done
}

// handshake plays the server side of pairing: it reads Pair, writes Welcome
// at the given minor, reads Hello and writes Token.
func handshake(conn *websocket.Conn, ctx context.Context, minor int, token string) (protocol.Pair, protocol.Hello, error) {
	var pair, hello protocol.Frame
	if _, data, err := conn.Read(ctx); err != nil {
		return protocol.Pair{}, protocol.Hello{}, err
	} else if err := json.Unmarshal(data, &pair); err != nil {
		return protocol.Pair{}, protocol.Hello{}, err
	}
	if err := conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameWelcome, Welcome: &protocol.Welcome{
		RoomID: "r1", Title: "orders", PipelineID: "orders",
		Creator:         events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ada"},
		ProtocolVersion: 1, ProtocolMinor: minor,
	}})); err != nil {
		return protocol.Pair{}, protocol.Hello{}, err
	}
	if _, data, err := conn.Read(ctx); err != nil {
		return protocol.Pair{}, protocol.Hello{}, err
	} else if err := json.Unmarshal(data, &hello); err != nil {
		return protocol.Pair{}, protocol.Hello{}, err
	}
	if err := conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{Token: token}})); err != nil {
		return protocol.Pair{}, protocol.Hello{}, err
	}
	return *pair.Pair, *hello.Hello, nil
}

// TestPairingRefusedBeforeWelcome: a used or an expired code is refused. The
// fake server stands in for the room service closing on a bad code; the
// runner must report ErrPairingRefused, not hang or pair.
func TestPairingRefusedBeforeWelcome(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status websocket.StatusCode
		reason string
	}{
		{"used code", websocket.StatusPolicyViolation, "used"},
		{"expired code", websocket.StatusPolicyViolation, "expired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
				_, _, _ = conn.Read(ctx) // the runner must have sent Pair
				_ = conn.Close(tc.status, tc.reason)
			})
			defer srv.Close()

			r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH"})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Run(context.Background()); !errors.Is(err, runner.ErrPairingRefused) {
				t.Fatalf("Run = %v, want ErrPairingRefused", err)
			}
			<-done
		})
	}
}

// TestVersionOneMinorBehindPairs: a server one minor behind the runner still
// pairs. The runner accepts Welcome 1.0, sends Hello with its own Current
// (1.1), and answers a ping with a pong.
func TestVersionOneMinorBehindPairs(t *testing.T) {
	var (
		mu    sync.Mutex
		pair  protocol.Pair
		hello protocol.Hello
	)
	pong := make(chan struct{}, 1)

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		p, h, err := handshake(conn, ctx, 0, "tok-secret-value") // 0 = one minor behind
		if err != nil {
			t.Errorf("handshake: %v", err)
			return
		}
		mu.Lock()
		pair, hello = p, h
		mu.Unlock()

		if err := conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FramePing})); err != nil {
			t.Errorf("write ping: %v", err)
			return
		}
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Errorf("read pong: %v", err)
			return
		}
		var pf protocol.Frame
		if json.Unmarshal(data, &pf) == nil && pf.Type == protocol.FramePong {
			pong <- struct{}{}
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	mu.Lock()
	defer mu.Unlock()
	if pair.Code != "ABCDEFGH" || pair.ProtocolVersion != 1 || pair.ProtocolMinor != 1 {
		t.Errorf("pair = %+v, want code ABCDEFGH version 1.1", pair)
	}
	if hello.ProtocolVersion != 1 || hello.ProtocolMinor != 1 || hello.Label == "" {
		t.Errorf("hello = %+v, want version 1.1 and a label", hello)
	}
	select {
	case <-pong:
	default:
		t.Error("runner did not answer ping with pong")
	}
}

// TestRefusedBeforeDial: --yes off loopback and plain http off loopback are
// both refused before anything is dialed (192.0.2.1 is TEST-NET-1).
func TestRefusedBeforeDial(t *testing.T) {
	cases := []struct {
		name string
		yes  bool
		want error
	}{
		{"--yes off loopback", true, runner.ErrYesOffLoopback},
		{"plain http off loopback", false, runner.ErrPlainHTTPOffLoopback},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := runner.New(runner.Options{ServerURL: "http://192.0.2.1:8080", Code: "ABCDEFGH", Yes: c.yes})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Run(context.Background()); !errors.Is(err, c.want) {
				t.Fatalf("Run = %v, want %v", err, c.want)
			}
		})
	}
}

// TestTokenNeverLogged: the session token appears in no log line and in no
// terminal output.
func TestTokenNeverLogged(t *testing.T) {
	const token = "tok-very-secret-value"
	var logBuf, outBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		if _, _, err := handshake(conn, ctx, 1, token); err != nil {
			t.Errorf("handshake: %v", err)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Log: log, Stdout: &outBuf})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	for _, s := range []string{logBuf.String(), outBuf.String()} {
		if strings.Contains(s, token) {
			t.Fatalf("token leaked into output: %q", s)
		}
	}
}

// TestOversizedFrameRefused: a frame over 1 MiB fails the read (the runner
// does not act on it).
func TestOversizedFrameRefused(t *testing.T) {
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		if _, _, err := handshake(conn, ctx, 1, "tok"); err != nil {
			t.Errorf("handshake: %v", err)
			return
		}
		_ = conn.Write(ctx, websocket.MessageText, bytes.Repeat([]byte("x"), protocol.MaxFrameBytes+1))
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, runner.ErrFrameTooBig) {
		t.Fatalf("Run = %v, want ErrFrameTooBig", err)
	}
	<-done
}
