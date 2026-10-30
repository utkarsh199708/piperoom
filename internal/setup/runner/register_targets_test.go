package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

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
// the room's target driver must not pair silently without it.
func TestRegisterTargetsErrorFailsTheHandshake(t *testing.T) {
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		// Pair then Welcome; the runner must give up before Hello.
		_ = welcomeWithTargets(conn, ctx, []events.SetupTarget{{Kind: "host", Address: "127.0.0.1"}})
	})
	defer srv.Close()

	boom := errors.New("duplicate op")
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet(),
		RegisterTargets: func(context.Context, string, []events.SetupTarget) error { return boom },
	})
	if err != nil {
		t.Fatal(err)
	}
	err = r.Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the registration error", err)
	}
	if !strings.Contains(err.Error(), "target driver") {
		t.Errorf("Run error %q does not name the target driver", err)
	}
	<-done
}

// mustJSON marshals v; it cannot fail for these types.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("runner_test: " + err.Error())
	}
	return b
}
