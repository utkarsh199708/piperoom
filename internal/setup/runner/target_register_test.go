package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

func readFrame(conn *websocket.Conn, ctx context.Context) (protocol.Frame, error) {
	var f protocol.Frame
	_, data, err := conn.Read(ctx)
	if err != nil {
		return f, err
	}
	return f, json.Unmarshal(data, &f)
}

// welcomeWith plays the server side of a pairing with a Welcome that carries
// the given Target and targets; it reads Pair, writes Welcome, and (when the
// runner continues) reads Hello and writes Token. It returns the next frame the
// runner sent after Welcome.
func welcomeWith(conn *websocket.Conn, ctx context.Context, target string, targets []events.SetupTarget) (protocol.Frame, error) {
	if _, _, err := conn.Read(ctx); err != nil {
		return protocol.Frame{}, err
	}
	w := protocol.Welcome{
		RoomID: "r1", Title: "orders", PipelineID: "orders",
		Creator:         events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Ada"},
		ProtocolVersion: 1, ProtocolMinor: protocol.CurrentMinor,
		Target: target, Targets: targets,
	}
	if err := conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameWelcome, Welcome: &w})); err != nil {
		return protocol.Frame{}, err
	}
	f, err := readFrame(conn, ctx)
	if err != nil {
		return f, err
	}
	if f.Type == protocol.FrameHello {
		err = conn.Write(ctx, websocket.MessageText, mustFrame(protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{Token: "tok"}}))
	}
	return f, err
}

// A Welcome that names a target reaches RegisterTarget once, with the target
// and the Welcome's targets, before the runner sends Hello (so before any op).
func TestRegisterTargetRunsBeforeHello(t *testing.T) {
	var calls atomic.Int32
	var gotTarget string
	var gotTargets []events.SetupTarget
	var next protocol.Frame
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		var err error
		next, err = welcomeWith(conn, ctx, "compose", []events.SetupTarget{{Kind: "host", Address: "h", Class: "shared"}})
		if err != nil {
			t.Errorf("handshake: %v", err)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet(),
		RegisterTarget: func(target string, targets []events.SetupTarget) error {
			calls.Add(1)
			gotTarget, gotTargets = target, targets
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if calls.Load() != 1 || gotTarget != "compose" {
		t.Fatalf("RegisterTarget calls = %d, target %q; want 1 and compose", calls.Load(), gotTarget)
	}
	if len(gotTargets) != 1 || gotTargets[0].Class != "shared" {
		t.Errorf("targets = %+v, want the Welcome's", gotTargets)
	}
	if next.Type != protocol.FrameHello {
		t.Errorf("the frame after Welcome = %s, want hello", next.Type.Short())
	}
}

// A Welcome with no target (a server older than 1.2, or a room without one)
// never calls RegisterTarget: the catalogue stays as it was.
func TestRegisterTargetNotCalledWithoutTarget(t *testing.T) {
	var calls atomic.Int32
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		if _, err := welcomeWith(conn, ctx, "", nil); err != nil {
			t.Errorf("handshake: %v", err)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet(),
		RegisterTarget: func(string, []events.SetupTarget) error { calls.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if calls.Load() != 0 {
		t.Fatalf("RegisterTarget called %d times for a Welcome with no target", calls.Load())
	}
}

// A target with no driver is refused by name at pairing (a Refused frame with
// RefuseTarget, never a Hello), not left to answer unknown_op later.
func TestRegisterTargetErrorRefusesPairing(t *testing.T) {
	var next protocol.Frame
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		var err error
		next, err = welcomeWith(conn, ctx, "strimzi", nil)
		if err != nil {
			t.Errorf("handshake: %v", err)
		}
	})
	defer srv.Close()

	var out strings.Builder
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: &out, Log: quiet(),
		RegisterTarget: func(target string, _ []events.SetupTarget) error {
			return errors.New("no setup driver for target " + target)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = r.Run(ctx)
	if !errors.Is(err, runner.ErrTargetUnsupported) || !strings.Contains(err.Error(), "strimzi") {
		t.Fatalf("Run = %v, want an error naming the target", err)
	}
	<-done
	if next.Type != protocol.FrameRefused || next.Refused == nil || next.Refused.Reason != protocol.RefuseTarget {
		t.Fatalf("frame after Welcome = %+v, want refused/target", next)
	}
	if !strings.Contains(out.String(), "strimzi") {
		t.Errorf("the person was not told which target: %q", out.String())
	}
}
