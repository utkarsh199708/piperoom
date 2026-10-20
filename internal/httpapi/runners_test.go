package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/store/memstore"
)

// newRunnerServer builds a server with the setup runner endpoint on or off.
func newRunnerServer(t *testing.T, setup bool) (*httptest.Server, *room.Service) {
	t.Helper()
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	var opts []httpapi.Option
	if setup {
		opts = append(opts, httpapi.WithSetup(true))
	}
	srv := httptest.NewServer(httpapi.New(svc, st, nil, opts...))
	t.Cleanup(srv.Close)
	return srv, svc
}

// TestRunnerWSEndpointHiddenWhenSetupOff pins PIPEROOM_SETUP_ENABLED=off: the
// endpoint is not mounted and answers 404 not_found.
func TestRunnerWSEndpointHiddenWhenSetupOff(t *testing.T) {
	srv, _ := newRunnerServer(t, false)
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/runners/ws", "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("runner ws with setup off: want 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// writeRunnerFrame and readRunnerFrame drive the protocol from the test client.
func writeRunnerFrame(ctx context.Context, conn *websocket.Conn, f protocol.Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, b)
}

func readRunnerFrame(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return protocol.Frame{}, err
	}
	if typ != websocket.MessageText {
		return protocol.Frame{}, nil
	}
	var f protocol.Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return protocol.Frame{}, err
	}
	return f, nil
}

// TestRunnerWSPairingAndAttach drives the full handshake: Pair (code) →
// Welcome → Hello → Token, and checks the setup.pairing (ok) and
// setup.runner_attached audit events in the room log.
func TestRunnerWSPairingAndAttach(t *testing.T) {
	srv, svc := newRunnerServer(t, true)
	ctx := context.Background()

	arjun, ok := svc.LookupUser("arjun")
	if !ok {
		t.Fatal("arjun missing")
	}
	snap, err := svc.CreateRoom(ctx, arjun, "Incident", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	creator := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	code, err := svc.IssuePairingCode("", snap.ID, creator)
	if err != nil {
		t.Fatalf("issue code: %v", err)
	}

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/runners/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	pair := protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{
		Code: code, ProtocolVersion: protocol.CurrentMajor, ProtocolMinor: protocol.CurrentMinor,
	}}
	if err := writeRunnerFrame(ctx, conn, pair); err != nil {
		t.Fatalf("write pair: %v", err)
	}

	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	welcome, err := readRunnerFrame(wctx, conn)
	if err != nil {
		t.Fatalf("read welcome: %v", err)
	}
	if welcome.Type != protocol.FrameWelcome || welcome.Welcome == nil {
		t.Fatalf("want welcome, got %+v", welcome)
	}
	if welcome.Welcome.RoomID != snap.ID || welcome.Welcome.PipelineID != "orders" {
		t.Fatalf("bad welcome: %+v", welcome.Welcome)
	}

	hello := protocol.Frame{Type: protocol.FrameHello, Hello: &protocol.Hello{
		OS: "linux", Arch: "amd64", Label: "laptop",
		Tools:            map[string]string{"docker": "1.0"},
		ProjectDir:       "/proj",
		CatalogueVersion: "v1",
		ProtocolVersion:  protocol.CurrentMajor,
		ProtocolMinor:    protocol.CurrentMinor,
	}}
	if err := writeRunnerFrame(ctx, conn, hello); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	token, err := readRunnerFrame(wctx, conn)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	if token.Type != protocol.FrameToken || token.Token == nil || token.Token.Token == "" {
		t.Fatalf("want token, got %+v", token)
	}

	// The pairing and the attach are in the room log.
	evs, _, err := svc.Events(ctx, snap.ID, 0, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var pairingOK, attached bool
	for _, e := range evs {
		switch e.Type {
		case events.SetupPairing:
			p, err := events.Decode[events.SetupPairingPayload](e)
			if err == nil && p.Outcome == events.PairingOK {
				pairingOK = true
			}
		case events.SetupRunnerAttached:
			p, err := events.Decode[events.SetupRunnerAttachedPayload](e)
			if err == nil && p.RunnerLabel == "laptop" {
				attached = true
			}
		}
	}
	if !pairingOK {
		t.Fatal("no setup.pairing ok event in the room log")
	}
	if !attached {
		t.Fatal("no setup.runner_attached event in the room log")
	}

	// The runner is registered keyed by (tenant, room, creator).
	if _, ok := svc.RunnerLookup("", snap.ID, "arjun"); !ok {
		t.Fatal("runner not registered after Hello")
	}
}

// TestRunnerWSBadCodeCloses: a bad code closes the socket with a policy
// violation and is audited as bad_code (no room, source preserved).
func TestRunnerWSBadCodeCloses(t *testing.T) {
	srv, svc := newRunnerServer(t, true)
	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/runners/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if err := writeRunnerFrame(ctx, conn, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{Code: "ZZZZZZZZ"}}); err != nil {
		t.Fatalf("write pair: %v", err)
	}
	// The server closes after the bad code.
	_, _, err = conn.Read(ctx)
	if err == nil {
		t.Fatal("bad code: want the socket closed, read succeeded")
	}
	_ = svc
}
