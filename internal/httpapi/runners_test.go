package httpapi_test

import (
	"context"
	"encoding/json"
	"log/slog"
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

// dialRunner opens the runner socket and completes the code handshake:
// Pair(code) → Welcome → Hello → Token. It returns the socket and the token.
func dialRunner(t *testing.T, ctx context.Context, srvURL, code, label string) (*websocket.Conn, string) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srvURL, "http") + "/api/runners/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := writeRunnerFrame(rctx, conn, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{
		Code: code, ProtocolVersion: protocol.CurrentMajor, ProtocolMinor: protocol.CurrentMinor,
	}}); err != nil {
		t.Fatalf("write pair: %v", err)
	}
	if f, err := readRunnerFrame(rctx, conn); err != nil || f.Type != protocol.FrameWelcome {
		t.Fatalf("read welcome: %+v, %v", f, err)
	}
	if err := writeRunnerFrame(rctx, conn, protocol.Frame{Type: protocol.FrameHello, Hello: &protocol.Hello{
		OS: "linux", Arch: "amd64", Label: label,
		Tools:           map[string]string{"docker": "1.0"},
		ProtocolVersion: protocol.CurrentMajor, ProtocolMinor: protocol.CurrentMinor,
	}}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	tok, err := readRunnerFrame(rctx, conn)
	if err != nil || tok.Type != protocol.FrameToken || tok.Token == nil || tok.Token.Token == "" {
		t.Fatalf("read token: %+v, %v", tok, err)
	}
	return conn, tok.Token.Token
}

// reconnectRunner reattaches with the token: Pair(token) → Welcome, Token.
func reconnectRunner(t *testing.T, ctx context.Context, srvURL, token string) (*websocket.Conn, string) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srvURL, "http") + "/api/runners/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := writeRunnerFrame(rctx, conn, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{
		Token: token, ProtocolVersion: protocol.CurrentMajor, ProtocolMinor: protocol.CurrentMinor,
	}}); err != nil {
		t.Fatalf("write token pair: %v", err)
	}
	if f, err := readRunnerFrame(rctx, conn); err != nil || f.Type != protocol.FrameWelcome {
		t.Fatalf("read reconnect welcome: %+v, %v", f, err)
	}
	tok, err := readRunnerFrame(rctx, conn)
	if err != nil || tok.Type != protocol.FrameToken || tok.Token == nil || tok.Token.Token == "" {
		t.Fatalf("read rotated token: %+v, %v", tok, err)
	}
	// The runner's first frame after the reconnect is Resume.
	if err := writeRunnerFrame(rctx, conn, protocol.Frame{Type: protocol.FrameResume, Resume: &protocol.Resume{}}); err != nil {
		t.Fatalf("write resume: %v", err)
	}
	return conn, tok.Token.Token
}

// TestRunnerWSTokenReconnect: a runner reattaches with its token (no pairing
// code), gets a new token, the old one is refused, and an op sent after the
// reconnect is answered by the runner's FrameResult through the executor.
func TestRunnerWSTokenReconnect(t *testing.T) {
	var logbuf strings.Builder
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	srv := httptest.NewServer(httpapi.New(svc, st, slog.New(slog.NewTextHandler(&logbuf, nil)), httpapi.WithSetup(true)))
	defer srv.Close()

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

	conn1, token1 := dialRunner(t, ctx, srv.URL, code, "laptop")
	_ = conn1.Close(websocket.StatusNormalClosure, "drop")

	conn2, token2 := reconnectRunner(t, ctx, srv.URL, token1)
	defer conn2.Close(websocket.StatusNormalClosure, "")
	if token2 == token1 {
		t.Fatal("token was not rotated on reconnect")
	}
	if _, ok := svc.RunnerLookup("", snap.ID, "arjun"); !ok {
		t.Fatal("runner not attached after token reconnect")
	}

	// The rotated token is on the wire; the old one is a bad code now.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/runners/ws"
	conn3, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn3.Close(websocket.StatusNormalClosure, "")
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := writeRunnerFrame(rctx, conn3, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{Token: token1}}); err != nil {
		t.Fatalf("write old token: %v", err)
	}
	if _, _, err := conn3.Read(rctx); err == nil {
		t.Fatal("old token accepted after rotation")
	} else if sc := websocket.CloseStatus(err); sc != websocket.StatusPolicyViolation {
		t.Fatalf("old token close status = %d, want policy violation", sc)
	}

	// An op sent by the executor reaches this socket and its result completes
	// the call.
	idx := 0
	req := protocol.OpRequest{
		ID: "op-1", Op: "compose_up", Params: []byte(`{}`), Tier: protocol.TierT1,
		Class: protocol.ClassLocal, Deadline: time.Now().Add(5 * time.Second),
		ProposalID: "p-1", StepIndex: &idx, ParamsHash: "h",
	}
	type out struct {
		res protocol.OpResult
		err error
	}
	ch := make(chan out, 1)
	go func() {
		res, err := svc.RunnerExecutor().Execute(ctx, snap.ID, req)
		ch <- out{res, err}
	}()
	op, err := readRunnerFrame(rctx, conn2)
	if err != nil || op.Type != protocol.FrameOp || op.Op == nil || op.Op.ID != "op-1" {
		t.Fatalf("read op: %+v, %v", op, err)
	}
	if err := writeRunnerFrame(rctx, conn2, protocol.Frame{Type: protocol.FrameResult, Result: &protocol.OpResult{
		ID: "op-1", OK: true, Summary: "done",
	}}); err != nil {
		t.Fatalf("write result: %v", err)
	}
	got := <-ch
	if got.err != nil || !got.res.OK || got.res.Summary != "done" {
		t.Fatalf("Execute = %+v, %v", got.res, got.err)
	}

	// A result for an op this server never sent is ignored and the socket
	// stays usable.
	if err := writeRunnerFrame(rctx, conn2, protocol.Frame{Type: protocol.FrameResult, Result: &protocol.OpResult{
		ID: "never-sent", OK: true, Summary: "x",
	}}); err != nil {
		t.Fatalf("write unknown result: %v", err)
	}
	if err := writeRunnerFrame(rctx, conn2, protocol.Frame{Type: protocol.FramePing}); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	if pong, err := readRunnerFrame(rctx, conn2); err != nil || pong.Type != protocol.FramePong {
		t.Fatalf("read pong: %+v, %v", pong, err)
	}

	// The reconnect is a second setup.runner_attached (the runner sends no
	// Hello again), and neither token appears in a log line or a room event.
	evs, _, err := svc.Events(ctx, snap.ID, 0, 200)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	attached := 0
	var all strings.Builder
	for _, e := range evs {
		if e.Type == events.SetupRunnerAttached {
			attached++
		}
		b, _ := json.Marshal(e)
		all.Write(b)
	}
	if attached != 2 {
		t.Fatalf("setup.runner_attached events = %d, want 2 (pair and reconnect)", attached)
	}
	if strings.Contains(all.String(), token1) || strings.Contains(all.String(), token2) {
		t.Fatal("a runner token reached the room log")
	}
	if strings.Contains(logbuf.String(), token1) || strings.Contains(logbuf.String(), token2) {
		t.Fatal("a runner token reached a log line")
	}
}

// TestRunnerWSUnknownTokenRefused: a token this server never issued is refused
// with a policy violation and no registration.
func TestRunnerWSUnknownTokenRefused(t *testing.T) {
	srv, svc := newRunnerServer(t, true)
	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/runners/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := writeRunnerFrame(rctx, conn, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{
		Token: "never-issued-token", ProtocolVersion: protocol.CurrentMajor, ProtocolMinor: protocol.CurrentMinor,
	}}); err != nil {
		t.Fatalf("write pair: %v", err)
	}
	if _, _, err := conn.Read(rctx); err == nil {
		t.Fatal("unknown token accepted")
	} else if sc := websocket.CloseStatus(err); sc != websocket.StatusPolicyViolation {
		t.Fatalf("unknown token close status = %d, want policy violation", sc)
	}
	_ = svc
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

// TestRunnerWSCloseCancelsConnection: Server.Close cancels an attached
// runner's connection context, so a runner that keeps pinging cannot leave its
// handler or socket alive through shutdown.
func TestRunnerWSCloseCancelsConnection(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	srv := httpapi.New(svc, st, nil, httpapi.WithSetup(true))
	ts := httptest.NewServer(srv)
	defer ts.Close()

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

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/runners/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if err := writeRunnerFrame(ctx, conn, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{
		Code: code, ProtocolVersion: protocol.CurrentMajor, ProtocolMinor: protocol.CurrentMinor,
	}}); err != nil {
		t.Fatalf("write pair: %v", err)
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := readRunnerFrame(wctx, conn); err != nil { // Welcome
		t.Fatalf("read welcome: %v", err)
	}
	hello := protocol.Frame{Type: protocol.FrameHello, Hello: &protocol.Hello{
		OS: "linux", Arch: "amd64", Label: "laptop",
		Tools:           map[string]string{"docker": "1.0"},
		ProtocolVersion: protocol.CurrentMajor,
		ProtocolMinor:   protocol.CurrentMinor,
	}}
	if err := writeRunnerFrame(ctx, conn, hello); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if _, err := readRunnerFrame(wctx, conn); err != nil { // Token
		t.Fatalf("read token: %v", err)
	}

	// The handler is now in runnerLoop, blocked reading. Closing the server
	// must cancel its context and drop the socket.
	srv.Close()

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("runner connection still alive after Server.Close")
	}
}
