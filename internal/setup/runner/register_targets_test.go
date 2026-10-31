package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// welcomeWithTargets plays the server side of pairing with targets in the
// Welcome: Pair, Welcome{Targets}, Hello, Token.
func welcomeWithTargets(conn *websocket.Conn, ctx context.Context, targets []events.SetupTarget) error {
	if _, _, err := conn.Read(ctx); err != nil { // Pair
		return err
	}
	if err := conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameWelcome, Welcome: &protocol.Welcome{
		RoomID: "r1", Title: "orders", PipelineID: "orders",
		Creator:         events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ada"},
		ProtocolVersion: 1, ProtocolMinor: 1,
		Targets: targets,
	}})); err != nil {
		return err
	}
	if _, _, err := conn.Read(ctx); err != nil { // Hello
		return err
	}
	return conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{Token: "tok-secret-value"}}))
}

// TestRegisterTargetsSeesTheWelcome (row U8f): the targets the Welcome names
// reach Options.RegisterTargets before the session executes anything, so
// setupcmd can register the room's target driver there.
func TestRegisterTargetsSeesTheWelcome(t *testing.T) {
	want := []events.SetupTarget{
		{Kind: "host", Address: "127.0.0.1", Class: protocol.ClassShared},
		{Kind: "postgres", Address: "db.example.com:5432", Class: protocol.ClassShared},
	}
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		if err := welcomeWithTargets(conn, ctx, want); err != nil {
			t.Errorf("handshake: %v", err)
		}
	})
	defer srv.Close()

	var (
		mu    sync.Mutex
		got   [][]events.SetupTarget
		rooms []string
	)
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet(),
		RegisterTargets: func(_ context.Context, roomID string, targets []events.SetupTarget) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, targets)
			rooms = append(rooms, roomID)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("RegisterTargets ran %d times, want once", len(got))
	}
	if b, _ := json.Marshal(got[0]); string(b) != string(mustJSON(want)) {
		t.Errorf("RegisterTargets got %s, want %s", b, mustJSON(want))
	}
	if rooms[0] != "r1" {
		t.Errorf("RegisterTargets got room %q, want the Welcome's r1", rooms[0])
	}
}

// TestRegisterTargetsErrorFailsTheHandshake: a runner that cannot register
// the room's target driver must not pair silently without it, and must not
// retry either: the failure is terminal (ErrRegisterTargets), so the pairing
// code is presented exactly once and the registrar is called exactly once,
// even with a zero backoff that would make a retry immediate.
func TestRegisterTargetsErrorFailsTheHandshake(t *testing.T) {
	var conns atomic.Int32
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		conns.Add(1)
		// Pair then Welcome; the runner must give up before Hello.
		_ = welcomeWithTargets(conn, ctx, []events.SetupTarget{{Kind: "host", Address: "127.0.0.1"}})
	})
	defer srv.Close()

	boom := errors.New("duplicate op")
	var calls atomic.Int32
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet(),
		Backoff: func(int) time.Duration { return 0 },
		RegisterTargets: func(context.Context, string, []events.SetupTarget) error {
			calls.Add(1)
			return boom
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = r.Run(context.Background())
	if !errors.Is(err, boom) || !errors.Is(err, runner.ErrRegisterTargets) {
		t.Fatalf("Run = %v, want the registration error wrapped in ErrRegisterTargets", err)
	}
	if !strings.Contains(err.Error(), "target driver") {
		t.Errorf("Run error %q does not name the target driver", err)
	}
	<-done
	if n := calls.Load(); n != 1 {
		t.Errorf("RegisterTargets ran %d times, want once (a registration failure is not retried)", n)
	}
	if n := conns.Load(); n != 1 {
		t.Errorf("the runner connected %d times, want once (the pairing code is not re-sent)", n)
	}
}

// TestRegisterTargetsAfterConfirmation: the target drivers register only
// after the person's "y": a declined pairing never calls RegisterTargets, so
// no cloud credential chain runs for a room nobody confirmed. The terminal
// block names each target with its class and the fingerprint.
func TestRegisterTargetsAfterConfirmation(t *testing.T) {
	targets := []events.SetupTarget{{Kind: "aws", Address: "123456789012/eu-west-1", Class: protocol.ClassProduction}}
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		if _, _, err := conn.Read(ctx); err != nil { // Pair
			return
		}
		_ = conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameWelcome, Welcome: &protocol.Welcome{
			RoomID: "r1", Title: "orders", PipelineID: "orders", Tenant: "acme",
			Creator:         events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ada"},
			ProtocolVersion: 1, ProtocolMinor: 1,
			Targets: targets, Fingerprint: "a1b2c3d4e5f6",
		}}))
		_, _, _ = conn.Read(ctx) // the runner closes after "n"
	})
	defer srv.Close()

	var calls atomic.Int32
	var stdout strings.Builder
	var mu sync.Mutex
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Stdin: strings.NewReader("n\n"),
		Stdout: writerFunc(func(p []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return stdout.Write(p) }),
		Log:    quiet(),
		RegisterTargets: func(context.Context, string, []events.SetupTarget) error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, runner.ErrDeclined) {
		t.Fatalf("Run = %v, want ErrDeclined", err)
	}
	<-done
	if n := calls.Load(); n != 0 {
		t.Fatalf("RegisterTargets ran %d times before a confirmation, want 0", n)
	}
	mu.Lock()
	out := stdout.String()
	mu.Unlock()
	for _, want := range []string{"target aws 123456789012/eu-west-1 (production)", "fingerprint a1b2c3d4e5f6", "pipeline orders, tenant acme", "created by Ada"} {
		if !strings.Contains(out, want) {
			t.Errorf("the terminal block lacks %q:\n%s", want, out)
		}
	}
}

// writerFunc adapts a func to io.Writer.
type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// mustJSON marshals v; it cannot fail for these types.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("runner_test: " + err.Error())
	}
	return b
}
