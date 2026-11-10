package httpapi_test

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
)

// pairWelcome opens the runner socket, sends a code Pair at the given minor
// (0 = a Pair with no version, a 1.0 runner) and returns the Welcome.
func pairWelcome(t *testing.T, srvURL, code string, minor int) protocol.Welcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srvURL, "http") + "/api/runners/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	pair := protocol.Pair{Code: code}
	if minor > 0 {
		pair.ProtocolVersion, pair.ProtocolMinor = protocol.CurrentMajor, minor
	}
	if err := writeRunnerFrame(ctx, conn, protocol.Frame{Type: protocol.FramePair, Pair: &pair}); err != nil {
		t.Fatalf("write pair: %v", err)
	}
	f, err := readRunnerFrame(ctx, conn)
	if err != nil || f.Type != protocol.FrameWelcome || f.Welcome == nil {
		t.Fatalf("read welcome: %+v, %v", f, err)
	}
	return *f.Welcome
}

var setupRoomSeq atomic.Int32

func newSetupRoomCode(t *testing.T, env *setupAPIEnv, target string) (room.SetupRoom, string) {
	t.Helper()
	alice, ok := env.svc.LookupUser("alice")
	if !ok {
		t.Fatal("alice missing")
	}
	res, err := env.svc.CreateSetupRoom(context.Background(), alice, events.SetupRequest{
		PipelineID: "orders-setup-" + strconv.Itoa(int(setupRoomSeq.Add(1))), Tenant: "a", Title: "Orders setup",
		SourceKind: "postgres", Target: target, TopicPrefix: "orders",
		Tables:     []string{"public.orders"},
		Connectors: []string{"orders-src"},
		Targets:    []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	})
	if err != nil {
		t.Fatalf("CreateSetupRoom: %v", err)
	}
	return res, res.Code
}

// The Welcome of a setup room carries the request's Target (since 1.2) and the
// room's targets with their classes, so the runner can register its driver.
func TestRunnerWelcomeCarriesSetupTarget(t *testing.T) {
	for _, target := range []string{"compose", "host", "dbzserver"} {
		t.Run(target, func(t *testing.T) {
			env := newSetupAPIEnv(t, true)
			_, code := newSetupRoomCode(t, env, target)
			w := pairWelcome(t, env.srv.URL, code, protocol.CurrentMinor)
			if w.Target != target {
				t.Errorf("Welcome.Target = %q, want %q", w.Target, target)
			}
			if len(w.Targets) != 1 || w.Targets[0].Kind != "postgres" || w.Targets[0].Address != "db.acme:5432" {
				t.Errorf("Welcome.Targets = %+v, want the request's postgres target", w.Targets)
			}
		})
	}
}

// A target registered after the request is in the Welcome with its class.
func TestRunnerWelcomeCarriesRegisteredTargets(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	res, code := newSetupRoomCode(t, env, "compose")
	alice, _ := env.svc.LookupUser("alice")
	if _, err := env.svc.AddSetupTarget(context.Background(), alice, res.Room.ID, events.SetupTarget{Kind: "kafka", Address: "k.acme:9092", Class: protocol.ClassShared}); err != nil {
		t.Fatalf("AddSetupTarget: %v", err)
	}
	w := pairWelcome(t, env.srv.URL, code, protocol.CurrentMinor)
	if w.Target != "compose" || len(w.Targets) != 2 {
		t.Fatalf("Welcome = target %q, targets %+v; want compose and two targets", w.Target, w.Targets)
	}
	var kafka *events.SetupTarget
	for i := range w.Targets {
		if w.Targets[i].Kind == "kafka" {
			kafka = &w.Targets[i]
		}
	}
	if kafka == nil || kafka.Class != protocol.ClassShared {
		t.Errorf("registered kafka target = %+v, want class shared", kafka)
	}
}

// Target is sent only when the negotiated minor has it: a runner one minor
// behind (1.1) pairs and gets no Target, as does a 1.0 Pair with no version.
func TestRunnerWelcomeOmitsTargetBelow12(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	_, code := newSetupRoomCode(t, env, "compose")
	w := pairWelcome(t, env.srv.URL, code, 1)
	if w.Target != "" {
		t.Errorf("Welcome.Target = %q for a 1.1 runner, want none", w.Target)
	}
	if len(w.Targets) != 1 {
		t.Errorf("Welcome.Targets = %+v; the target list is not minor-gated", w.Targets)
	}

	_, code = newSetupRoomCode(t, env, "compose")
	if w := pairWelcome(t, env.srv.URL, code, 0); w.Target != "" {
		t.Errorf("Welcome.Target = %q for a Pair with no version, want none", w.Target)
	}
}

// A room that is not a setup room has neither: its Welcome is what it was.
func TestRunnerWelcomeNoSetupFieldsOnPlainRoom(t *testing.T) {
	srv, svc := newRunnerServer(t, true)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "Incident", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	code, err := svc.IssuePairingCode("", snap.ID, events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"})
	if err != nil {
		t.Fatal(err)
	}
	w := pairWelcome(t, srv.URL, code, protocol.CurrentMinor)
	if w.Target != "" || len(w.Targets) != 0 {
		t.Errorf("plain room Welcome = target %q, targets %+v", w.Target, w.Targets)
	}
}
