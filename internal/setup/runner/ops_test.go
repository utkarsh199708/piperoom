package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// TestT1WithoutProposalRefused: a T1+ request missing ProposalID, StepIndex or
// ParamsHash is refused with RefuseNoProposal and the executor is never called.
func TestT1WithoutProposalRefused(t *testing.T) {
	p := testProposal("p1")
	hash := proposalHash(t, p)
	raw := proposalRaw(t, p)
	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)

		step0 := 0
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "a", Op: "compose_up", Params: json.RawMessage(`{}`), Tier: protocol.TierT1,
			StepIndex: &step0, ParamsHash: hash, Proposal: raw,
		}})
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "b", Op: "compose_up", Params: json.RawMessage(`{}`), Tier: protocol.TierT1,
			ProposalID: "p1", ParamsHash: hash, Proposal: raw,
		}})
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "c", Op: "compose_up", Params: json.RawMessage(`{}`), Tier: protocol.TierT1,
			ProposalID: "p1", StepIndex: &step0, Proposal: raw,
		}})

		for i := 0; i < 3; i++ {
			f := readFrameJSON(t, conn, ctx)
			if f.Type != protocol.FrameRefused || f.Refused == nil {
				t.Fatalf("frame %d = %+v, want refused", i, f)
			}
			if f.Refused.Reason != protocol.RefuseNoProposal {
				t.Errorf("refusal %d reason = %q, want %q", i, f.Refused.Reason, protocol.RefuseNoProposal)
			}
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 0 {
		t.Fatalf("executor called %d times, want 0", n)
	}
}

// TestParamsHashRecomputed: a correct hash runs; a hash or a frozen proposal
// that differs by one byte is refused with RefuseParamsHash.
func TestParamsHashRecomputed(t *testing.T) {
	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)

		// 1. Correct hash -> runs.
		p1 := testProposal("p1")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameResult || f.Result == nil || !f.Result.OK {
			t.Fatalf("correct hash: got %+v, want a result", f)
		}

		// 2. ParamsHash one byte changed -> refused.
		p2 := testProposal("p2")
		h2 := proposalHash(t, p2)
		badHash := flipByte(h2)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("b", "compose_up", protocol.TierT1, "p2", 0, badHash, proposalRaw(t, p2))})
		assertRefused(t, conn, ctx, protocol.RefuseParamsHash)

		// 3. Frozen proposal one byte changed (params differ) -> refused.
		p3 := testProposal("p3")
		raw3 := proposalRaw(t, p3)
		raw3 = tamperOneByte(raw3)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("c", "compose_up", protocol.TierT1, "p3", 0, proposalHash(t, p3), raw3)})
		assertRefused(t, conn, ctx, protocol.RefuseParamsHash)
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 1 {
		t.Fatalf("executor called %d times, want 1 (only the correct hash)", n)
	}
}

// TestDuplicateStepRefused: a (ProposalID, StepIndex) already started is
// refused, even while the first op is still running.
func TestDuplicateStepRefused(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	cat := &fakeCatalogue{
		floor: map[string]string{"compose_up": protocol.TierT1},
		onExecute: func(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return protocol.OpResult{}, ctx.Err()
			}
			return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok"}, nil
		},
	}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposal("p1")
		h := proposalHash(t, p1)
		raw := proposalRaw(t, p1)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, h, raw)})

		<-started // the first op is marked started and executing
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: opReqPtr("b", "compose_up", protocol.TierT1, "p1", 0, h, raw)})
		assertRefused(t, conn, ctx, protocol.RefuseDuplicateStep)

		close(release)
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameResult || f.Result == nil {
			t.Fatalf("expected the first op's result, got %+v", f)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 1 {
		t.Fatalf("executor called %d times, want 1", n)
	}
}

// TestDoneOpNeverRerun: once an op is done, the same step is never run again.
func TestDoneOpNeverRerun(t *testing.T) {
	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposal("p1")
		h := proposalHash(t, p1)
		raw := proposalRaw(t, p1)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, h, raw)})
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameResult {
			t.Fatalf("expected result, got %+v", f)
		}
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: opReqPtr("b", "compose_up", protocol.TierT1, "p1", 0, h, raw)})
		assertRefused(t, conn, ctx, protocol.RefuseDuplicateStep)
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 1 {
		t.Fatalf("executor called %d times, want 1", n)
	}
}

// TestClassFloorRaised: a production target confirmed at pairing keeps its
// floor even when the server sends Class "shared", so a guard-forbidden op is
// still refused.
func TestClassFloorRaised(t *testing.T) {
	cat := &fakeCatalogue{
		floor:  map[string]string{"alter_system": protocol.TierT1},
		target: map[string]string{"alter_system": "postgres"},
		guard:  map[string]bool{"alter_system|production": true},
	}
	targets := []events.SetupTarget{{Kind: "postgres", Address: "db1", Class: protocol.ClassProduction}}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, targets)
		p1 := testProposal("p1")
		req := opReq("a", "alter_system", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
		req.Class = protocol.ClassShared
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &req})
		assertRefused(t, conn, ctx, protocol.RefuseGuard)
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 0 {
		t.Fatalf("executor called %d times, want 0", n)
	}
}

// TestT3WaitsForY: a T3 op runs only after y on the runner's terminal; N is
// RefuseNotConfirmed, and no answer before the ctx ends means it never runs.
func TestT3WaitsForY(t *testing.T) {
	t.Run("y runs", func(t *testing.T) {
		cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
		srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
			doHandshake(t, conn, ctx, "tok", time.Now, nil)
			p1 := testProposalWithAction("p1", "source_restart")
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
			f := readFrameJSON(t, conn, ctx)
			if f.Type != protocol.FrameResult || f.Result == nil {
				t.Fatalf("expected result after y, got %+v", f)
			}
		})
		defer srv.Close()

		r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: strings.NewReader("y\n"), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Run(context.Background()); err != nil {
			t.Fatalf("Run = %v", err)
		}
		<-done
		if n := cat.callCount(); n != 1 {
			t.Fatalf("executor called %d times, want 1", n)
		}
	})

	t.Run("N refuses", func(t *testing.T) {
		cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
		srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
			doHandshake(t, conn, ctx, "tok", time.Now, nil)
			p1 := testProposalWithAction("p1", "source_restart")
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
			assertRefused(t, conn, ctx, protocol.RefuseNotConfirmed)
		})
		defer srv.Close()

		r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: strings.NewReader("n\n"), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Run(context.Background()); err != nil {
			t.Fatalf("Run = %v", err)
		}
		<-done
		if n := cat.callCount(); n != 0 {
			t.Fatalf("executor called %d times, want 0", n)
		}
	})

	t.Run("no answer until ctx ends", func(t *testing.T) {
		cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
		started := make(chan struct{})
		opSent := make(chan struct{})
		srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
			doHandshake(t, conn, ctx, "tok", time.Now, nil)
			p1 := testProposalWithAction("p1", "source_restart")
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
			close(opSent)
			// Wait for the runner to close the connection (its ctx ended).
			for {
				if _, _, err := conn.Read(ctx); err != nil {
					return
				}
			}
		})
		defer srv.Close()

		ctx, cancel := context.WithCancel(context.Background())
		r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: blockingReader{started: started}, Stdout: io.Discard, Log: quiet(), Catalogue: cat})
		if err != nil {
			t.Fatal(err)
		}

		errCh := make(chan error, 1)
		go func() { errCh <- r.Run(ctx) }()

		<-opSent
		<-started // the runner is blocked waiting for the T3 answer
		cancel()

		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
		<-done
		if n := cat.callCount(); n != 0 {
			t.Fatalf("executor called %d times, want 0", n)
		}
	})
}

// TestOpPrintedOnTerminal: every T1+ op is printed on the runner's terminal
// with its op, target, proposal id and step.
func TestOpPrintedOnTerminal(t *testing.T) {
	var outBuf bytes.Buffer
	cat := &fakeCatalogue{
		floor:  map[string]string{"compose_up": protocol.TierT1},
		target: map[string]string{"compose_up": "compose"},
	}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposal("p1")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult {
			t.Fatalf("expected result, got %+v", f)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: &outBuf, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	out := outBuf.String()
	for _, want := range []string{"op compose_up", "target=compose", "proposal=p1", "step=0"} {
		if !strings.Contains(out, want) {
			t.Errorf("terminal output missing %q: %q", want, out)
		}
	}
}

// TestStepBindingRefused: a T1+ request that carries the valid params_hash but
// names a different op, params or step than the frozen proposal step it claims
// is refused — the hash alone does not bind the request to the step.
func TestStepBindingRefused(t *testing.T) {
	cat := &fakeCatalogue{
		floor: map[string]string{
			"compose_up": protocol.TierT1,
			"other_op":   protocol.TierT1,
		},
	}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposal("p1")
		h := proposalHash(t, p1)
		raw := proposalRaw(t, p1)

		// 1. A different op, quoting the valid hash and the step's params.
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "other_op", protocol.TierT1, "p1", 0, h, raw)})
		assertRefused(t, conn, ctx, protocol.RefuseParamsHash)

		// 2. The right op but different params.
		req := opReq("b", "compose_up", protocol.TierT1, "p1", 0, h, raw)
		req.Params = json.RawMessage(`{"service":"kafka"}`)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &req})
		assertRefused(t, conn, ctx, protocol.RefuseParamsHash)

		// 3. A step index outside the frozen proposal.
		req = opReq("c", "compose_up", protocol.TierT1, "p1", 3, h, raw)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &req})
		assertRefused(t, conn, ctx, protocol.RefuseParamsHash)
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 0 {
		t.Fatalf("executor called %d times, want 0", n)
	}
}

// TestStepActionMapping: when an op name and a proposal step's action name
// differ, the catalogue's explicit mapping binds them, and the matching op
// runs; a mapped op whose params still differ is refused.
func TestStepActionMapping(t *testing.T) {
	p := events.ProposalCreatedPayload{
		ProposalID:     "p1",
		Title:          "fix the stack",
		Risk:           "low",
		Rollback:       "revert",
		RequiredLayers: []string{"environment"},
		Steps: []events.ProposalStep{
			{Action: "start_service", Layer: "environment", Params: json.RawMessage(`{"service":"connect"}`)},
		},
	}
	h := proposalHash(t, p)
	raw := proposalRaw(t, p)

	cat := &fakeCatalogue{
		floor:  map[string]string{"compose_up": protocol.TierT1},
		action: map[string]string{"compose_up": "start_service"},
	}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)

		// Matching params -> runs.
		req := opReq("a", "compose_up", protocol.TierT1, "p1", 0, h, raw)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &req})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult || f.Result == nil || !f.Result.OK {
			t.Fatalf("expected a result, got %+v", f)
		}

		// Mapped op but differing params -> refused.
		req = opReq("b", "compose_up", protocol.TierT1, "p1", 0, h, raw)
		req.Params = json.RawMessage(`{"service":"kafka"}`)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &req})
		assertRefused(t, conn, ctx, protocol.RefuseParamsHash)
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 1 {
		t.Fatalf("executor called %d times, want 1", n)
	}
}

// TestOpRefusedWhenStartRecordNotPersisted: a T1+ op whose durable "started"
// marker cannot be written is refused (not executed). A directory in place of
// the op log file stands in for a disk that refuses the write or sync.
func TestOpRefusedWhenStartRecordNotPersisted(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".piperoom", "ops.jsonl"), 0o700); err != nil {
		t.Fatal(err)
	}
	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposal("p1")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameRefused || f.Refused == nil {
			t.Fatalf("expected refusal, got %+v", f)
		}
		if f.Refused.Reason != "start_persist" {
			t.Fatalf("refusal reason = %q, want %q", f.Refused.Reason, "start_persist")
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir, Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 0 {
		t.Fatalf("executor called %d times, want 0", n)
	}
}

// --- small helpers ---

// opReqPtr returns a pointer to a T1+ OpRequest built by opReq.
func opReqPtr(id, op, tier, proposalID string, step int, hash string, frozen []byte) *protocol.OpRequest {
	req := opReq(id, op, tier, proposalID, step, hash, frozen)
	return &req
}

func assertRefused(t *testing.T, conn *websocket.Conn, ctx context.Context, reason string) {
	t.Helper()
	f := readFrameJSON(t, conn, ctx)
	if f.Type != protocol.FrameRefused || f.Refused == nil {
		t.Fatalf("expected refusal, got %+v", f)
	}
	if f.Refused.Reason != reason {
		t.Fatalf("refusal reason = %q, want %q", f.Refused.Reason, reason)
	}
}

func flipByte(s string) string {
	b := []byte(s)
	if b[0] == 'a' {
		b[0] = 'b'
	} else {
		b[0] = 'a'
	}
	return string(b)
}

func tamperOneByte(b []byte) []byte {
	out := append([]byte(nil), b...)
	for i := range out {
		if out[i] == 'c' {
			out[i] = 'd'
			return out
		}
	}
	out[0] ^= 0xff
	return out
}
