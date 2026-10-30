package runner_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

func welcomeWithOrigin(conn *websocket.Conn, ctx context.Context, origin string) error {
	if _, _, err := conn.Read(ctx); err != nil { // Pair
		return err
	}
	if err := conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameWelcome, Welcome: &protocol.Welcome{
		RoomID: "r1", Title: "orders", PipelineID: "orders",
		Creator:         events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ada"},
		ProtocolVersion: 1, ProtocolMinor: 1,
		PublicOrigin: origin, Fingerprint: "a1b2c3d4e5f6",
		Targets: []events.SetupTarget{{Kind: "kubernetes", Address: "kind/data", Class: protocol.ClassShared}},
	}})); err != nil {
		return err
	}
	if _, _, err := conn.Read(ctx); err != nil { // Hello
		return err
	}
	return conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{Token: "tok-secret-value"}}))
}

// TestExternalConfirm (row U12c): a runner without a terminal (the
// in-cluster Job) hands the Welcome to Options.Confirm, which prints the
// block with the fingerprint through show and decides.
func TestExternalConfirm(t *testing.T) {
	var srvURL string
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		if err := welcomeWithOrigin(conn, ctx, srvURL+"/"); err != nil {
			t.Errorf("handshake: %v", err)
		}
	})
	defer srv.Close()
	srvURL = srv.URL
	var out bytes.Buffer
	var seen *protocol.Welcome
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Stdout: &out, Log: quiet(),
		Confirm: func(_ context.Context, w *protocol.Welcome, show func()) (bool, error) {
			seen = w
			show()
			return true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if seen == nil || seen.Fingerprint != "a1b2c3d4e5f6" {
		t.Fatalf("Confirm saw %+v", seen)
	}
	for _, want := range []string{"fingerprint a1b2c3d4e5f6", "target kubernetes kind/data (shared)", "pipeline orders", "created by Ada"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the Welcome block lacks %q:\n%s", want, out.String())
		}
	}
}

func TestExternalConfirmOriginMismatch(t *testing.T) {
	srv, _ := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		_ = welcomeWithOrigin(conn, ctx, "https://other.example.com")
	})
	defer srv.Close()
	called := false
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Stdout: &bytes.Buffer{}, Log: quiet(),
		Confirm: func(context.Context, *protocol.Welcome, func()) (bool, error) { called = true; return true, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, runner.ErrOriginMismatch) {
		t.Fatalf("Run = %v, want ErrOriginMismatch", err)
	}
	if called {
		t.Error("Confirm ran for a Welcome from another origin")
	}
}

func TestExternalConfirmRefusal(t *testing.T) {
	var srvURL string
	srv, _ := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		_ = welcomeWithOrigin(conn, ctx, srvURL)
	})
	defer srv.Close()
	srvURL = srv.URL
	boom := errors.New("confirm Secret existed before the Welcome")
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Stdout: &bytes.Buffer{}, Log: quiet(),
		Confirm: func(context.Context, *protocol.Welcome, func()) (bool, error) { return false, boom },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the confirmer's error", err)
	}
}
