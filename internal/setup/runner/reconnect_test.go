package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// TestReconnectSendsResume: after a drop with one running and one done op, the
// first frame after the reconnect is FrameResume listing both, the done one
// with its Result.
func TestReconnectSendsResume(t *testing.T) {
	clock := newTestClock()
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})

	cat := &fakeCatalogue{
		floor:  map[string]string{"slow": protocol.TierT1, "fast": protocol.TierT1},
		target: map[string]string{"slow": "host", "fast": "host"},
		onExecute: func(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
			if req.Op == "slow" {
				close(slowStarted)
				select {
				case <-releaseSlow:
				case <-ctx.Done():
					return protocol.OpResult{}, ctx.Err()
				}
			}
			return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok",
				Receipt: events.SetupReceipt{Kind: "exec", Target: "host", ExitCode: 0, At: time.Now(), TookMS: 1, OutMAC: "mac"}}, nil
		},
	}

	var (
		mu     sync.Mutex
		resume protocol.Resume
	)

	handler1 := func(conn *websocket.Conn) {
		ctx := context.Background()
		doHandshake(t, conn, ctx, "tok1", clock.Now, nil)
		p1 := testProposal("p1")
		h := proposalHash(t, p1)
		raw := proposalRaw(t, p1)

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("fast-op", "fast", protocol.TierT1, "p1", 0, h, raw)})
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameResult {
			t.Fatalf("expected fast result, got %+v", f)
		}

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("slow-op", "slow", protocol.TierT1, "p1", 1, h, raw)})
		<-slowStarted
		_ = conn.Close(websocket.StatusPolicyViolation, "drop")
	}

	handler2 := func(conn *websocket.Conn) {
		ctx := context.Background()
		mu.Lock()
		resume = doReconnect(t, conn, ctx, "tok1", "tok2", clock.Now)
		mu.Unlock()
	}

	srv := runMultiServer(handler1, handler2)
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Now: clock.Now, Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		close(releaseSlow)
		t.Fatalf("Run = %v", err)
	}
	close(releaseSlow)

	mu.Lock()
	defer mu.Unlock()
	if len(resume.InFlight) != 2 {
		t.Fatalf("resume in_flight = %+v, want 2 entries", resume.InFlight)
	}
	byID := map[string]protocol.OpStatus{}
	for _, s := range resume.InFlight {
		byID[s.ID] = s
	}
	fast, ok := byID["fast-op"]
	if !ok || fast.State != protocol.StateDone || fast.Result == nil || fast.Result.ID != "fast-op" {
		t.Errorf("fast op status = %+v, want done with result", fast)
	}
	slow, ok := byID["slow-op"]
	if !ok || slow.State != protocol.StateRunning || slow.Result != nil {
		t.Errorf("slow op status = %+v, want running without result", slow)
	}
}

// TestDoneOpNeverRerunAfterReconnect: a done op is reported in Resume when the
// socket drops, and a re-sent (ProposalID, StepIndex) after the reconnect is
// refused as a duplicate — a done op never re-runs, even across a reconnect.
func TestDoneOpNeverRerunAfterReconnect(t *testing.T) {
	clock := newTestClock()
	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}

	var (
		mu     sync.Mutex
		resume protocol.Resume
	)

	handler1 := func(conn *websocket.Conn) {
		ctx := context.Background()
		doHandshake(t, conn, ctx, "tok1", clock.Now, nil)
		p1 := testProposal("p1")
		h := proposalHash(t, p1)
		raw := proposalRaw(t, p1)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("done-op", "compose_up", protocol.TierT1, "p1", 0, h, raw)})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult {
			t.Fatalf("expected result, got %+v", f)
		}
		_ = conn.Close(websocket.StatusPolicyViolation, "drop")
	}

	handler2 := func(conn *websocket.Conn) {
		ctx := context.Background()
		mu.Lock()
		resume = doReconnect(t, conn, ctx, "tok1", "tok2", clock.Now)
		mu.Unlock()
		// The server re-sends the step it is unsure about; the runner refuses
		// it rather than running the done op again.
		p1 := testProposal("p1")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("again-op", "compose_up", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
		assertRefused(t, conn, ctx, protocol.RefuseDuplicateStep)
	}

	srv := runMultiServer(handler1, handler2)
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Now: clock.Now, Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(resume.InFlight) != 1 {
		t.Fatalf("resume in_flight = %+v, want 1 done entry", resume.InFlight)
	}
	got := resume.InFlight[0]
	if got.State != protocol.StateDone || got.Result == nil || got.Result.ID != "done-op" {
		t.Errorf("resume entry = %+v, want done with its result", got)
	}
	if n := cat.callCount(); n != 1 {
		t.Fatalf("executor called %d times, want 1 (the done op never re-runs)", n)
	}
}

// TestTokenIdleExpiry: the token is valid 29 minutes in and expired at 30, at
// which point the runner stops and requires a new pairing code.
func TestTokenIdleExpiry(t *testing.T) {
	t.Run("29 minutes still valid", func(t *testing.T) {
		clock := newTestClock()
		paired := make(chan struct{})
		drop := make(chan struct{})
		gotToken := make(chan string, 1)

		handler1 := func(conn *websocket.Conn) {
			doHandshake(t, conn, context.Background(), "tok1", clock.Now, nil)
			close(paired)
			<-drop
			_ = conn.Close(websocket.StatusPolicyViolation, "drop")
		}
		handler2 := func(conn *websocket.Conn) {
			ctx := context.Background()
			f := readFrameJSON(t, conn, ctx)
			if f.Type != protocol.FramePair || f.Pair == nil {
				t.Fatalf("expected pair, got %+v", f)
			}
			gotToken <- f.Pair.Token
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{Token: "tok2", ExpiresAt: clock.Now().Add(30 * time.Minute)}})
			readFrameJSON(t, conn, ctx) // Resume
		}

		srv := runMultiServer(handler1, handler2)
		defer srv.Close()
		r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet(), Now: clock.Now})
		if err != nil {
			t.Fatal(err)
		}

		errCh := make(chan error, 1)
		go func() { errCh <- r.Run(context.Background()) }()

		<-paired
		clock.Advance(29 * time.Minute)
		close(drop)

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("Run = %v, want nil (token still valid)", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return")
		}
		if tok := <-gotToken; tok != "tok1" {
			t.Fatalf("reconnect used token %q, want %q", tok, "tok1")
		}
	})

	t.Run("30 minutes expired", func(t *testing.T) {
		clock := newTestClock()
		paired := make(chan struct{})
		drop := make(chan struct{})

		handler1 := func(conn *websocket.Conn) {
			doHandshake(t, conn, context.Background(), "tok1", clock.Now, nil)
			close(paired)
			<-drop
			_ = conn.Close(websocket.StatusPolicyViolation, "drop")
		}

		srv := runMultiServer(handler1)
		defer srv.Close()
		r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet(), Now: clock.Now})
		if err != nil {
			t.Fatal(err)
		}

		errCh := make(chan error, 1)
		go func() { errCh <- r.Run(context.Background()) }()

		<-paired
		clock.Advance(30 * time.Minute)
		close(drop)

		select {
		case err := <-errCh:
			if !errors.Is(err, runner.ErrTokenExpired) {
				t.Fatalf("Run = %v, want ErrTokenExpired", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return")
		}
	})
}

// TestTokenRotatesOnReconnect: the runner presents the old token on the first
// reconnect and the rotated one on the next.
func TestTokenRotatesOnReconnect(t *testing.T) {
	clock := newTestClock()
	paired := make(chan struct{})
	drop1 := make(chan struct{})
	drop2 := make(chan struct{})
	reconnected2 := make(chan struct{})

	var (
		mu     sync.Mutex
		tokens []string
	)

	handler1 := func(conn *websocket.Conn) {
		doHandshake(t, conn, context.Background(), "tok1", clock.Now, nil)
		close(paired)
		<-drop1
		_ = conn.Close(websocket.StatusPolicyViolation, "drop")
	}
	handler2 := func(conn *websocket.Conn) {
		ctx := context.Background()
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FramePair || f.Pair == nil {
			t.Fatalf("expected pair, got %+v", f)
		}
		mu.Lock()
		tokens = append(tokens, f.Pair.Token)
		mu.Unlock()
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{Token: "tok2", ExpiresAt: clock.Now().Add(30 * time.Minute)}})
		readFrameJSON(t, conn, ctx) // Resume
		close(reconnected2)
		<-drop2
		_ = conn.Close(websocket.StatusPolicyViolation, "drop")
	}
	handler3 := func(conn *websocket.Conn) {
		ctx := context.Background()
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FramePair || f.Pair == nil {
			t.Fatalf("expected pair, got %+v", f)
		}
		mu.Lock()
		tokens = append(tokens, f.Pair.Token)
		mu.Unlock()
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{Token: "tok3", ExpiresAt: clock.Now().Add(30 * time.Minute)}})
		readFrameJSON(t, conn, ctx) // Resume
	}

	srv := runMultiServer(handler1, handler2, handler3)
	defer srv.Close()
	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, Stdout: io.Discard, Log: quiet(), Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(context.Background()) }()

	<-paired
	close(drop1)
	<-reconnected2
	close(drop2)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"tok1", "tok2"}
	if len(tokens) != len(want) {
		t.Fatalf("reconnect tokens = %v, want %v", tokens, want)
	}
	for i := range want {
		if tokens[i] != want[i] {
			t.Fatalf("reconnect tokens = %v, want %v", tokens, want)
		}
	}
}

// TestTokenNeverLogged: the token (old and rotated) appears in no log line, no
// terminal output, no refusal frame and no op log line.
func TestTokenNeverLogged(t *testing.T) {
	const tok1 = "tok-old-secret-value"
	const tok2 = "tok-new-secret-value"

	var logBuf, outBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	clock := newTestClock()
	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}
	dir := t.TempDir()

	var (
		mu         sync.Mutex
		refusalRaw []byte
	)

	handler1 := func(conn *websocket.Conn) {
		ctx := context.Background()
		doHandshake(t, conn, ctx, tok1, clock.Now, nil)

		// A refusal (unknown op) — the frame must not carry the token.
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "x", Op: "no_such_op", Params: json.RawMessage(`{}`), Tier: protocol.TierT0,
		}})
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameRefused {
			t.Fatalf("expected refusal, got %+v", f)
		}
		raw, _ := json.Marshal(f)
		mu.Lock()
		refusalRaw = raw
		mu.Unlock()

		// A T1+ op that runs and is logged to the op log.
		p1 := testProposal("p1")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("y", "compose_up", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
		if f = readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult {
			t.Fatalf("expected result, got %+v", f)
		}
		_ = conn.Close(websocket.StatusPolicyViolation, "drop")
	}
	handler2 := func(conn *websocket.Conn) {
		doReconnect(t, conn, context.Background(), tok1, tok2, clock.Now)
	}

	srv := runMultiServer(handler1, handler2)
	defer srv.Close()
	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir, Stdout: &outBuf, Log: log, Now: clock.Now, Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, tok := range []string{tok1, tok2} {
		for _, where := range []string{logBuf.String(), outBuf.String(), string(refusalRaw), readOpLog(t, dir)} {
			if strings.Contains(where, tok) {
				t.Fatalf("token %q leaked into output: %q", tok, where)
			}
		}
	}
}
