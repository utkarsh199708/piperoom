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
	"sync/atomic"
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

		r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: strings.NewReader("y\n"), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
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

		r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: strings.NewReader("n\n"), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
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
		r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: blockingReader{started: started}, Stdout: io.Discard, Log: quiet(), Catalogue: cat})
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

		// 4. A frozen step whose params hold keys that differ only in case
		// (the ALTER SYSTEM probe): the hash and the params match the frozen
		// step exactly, but the decoder would run the other value.
		p2 := testProposal("p2")
		p2.Steps[0].Params = json.RawMessage(`{"name":"wal_level","NAME":"archive_command","value":"x"}`)
		req = opReq("d", "compose_up", protocol.TierT1, "p2", 0, proposalHash(t, p2), proposalRaw(t, p2))
		req.Params = p2.Steps[0].Params
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

// TestFrozenProposalNotSwapped: the first request of a proposal freezes it. A
// later request that carries a different Proposal (same id, different content)
// is refused with RefuseParamsHash and does not replace the cache, so a request
// cannot swap the frozen proposal — and the hash checkStep binds to — together.
func TestFrozenProposalNotSwapped(t *testing.T) {
	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}
	p1 := events.ProposalCreatedPayload{
		ProposalID:     "p1",
		Title:          "fix the stack",
		Risk:           "low",
		Rollback:       "revert",
		RequiredLayers: []string{"environment"},
		Steps: []events.ProposalStep{
			{Action: "compose_up", Layer: "environment", Params: json.RawMessage(`{"service":"connect"}`)},
			{Action: "compose_up", Layer: "environment", Params: json.RawMessage(`{"service":"connect"}`)},
		},
	}
	// p2 has the same id but different content, so a different params_hash.
	p2 := p1
	p2.Title = "drop the database"

	h1 := proposalHash(t, p1)
	raw1 := proposalRaw(t, p1)
	h2 := proposalHash(t, p2)
	raw2 := proposalRaw(t, p2)

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)

		// 1. The first request freezes p1 and runs.
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, h1, raw1)})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult {
			t.Fatalf("expected result for the frozen proposal, got %+v", f)
		}

		// 2. A later request for the same proposal carries a different Proposal
		// whose own hash matches; it must be refused, not swap the cache.
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("b", "compose_up", protocol.TierT1, "p1", 1, h2, raw2)})
		assertRefused(t, conn, ctx, protocol.RefuseParamsHash)

		// 3. A request that names the still-frozen proposal (no Proposal this
		// time) with its hash runs, proving the cache still holds p1.
		req := opReq("c", "compose_up", protocol.TierT1, "p1", 1, h1, nil)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &req})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult {
			t.Fatalf("expected result from the still-frozen proposal, got %+v", f)
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
	if n := cat.callCount(); n != 2 {
		t.Fatalf("executor called %d times, want 2 (the frozen step and the still-frozen step, never the swapped one)", n)
	}
}

// TestConcurrentT3OnlyConfirmedRuns: two T3 ops arriving at once share one
// terminal; a single typed "y" confirms exactly one of them and the other is
// refused — a concurrent confirm cannot read (or steal) the other's answer.
func TestConcurrentT3OnlyConfirmedRuns(t *testing.T) {
	cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
	p1 := events.ProposalCreatedPayload{
		ProposalID:     "p1",
		Title:          "restart the stack",
		Risk:           "low",
		Rollback:       "revert",
		RequiredLayers: []string{"environment"},
		Steps: []events.ProposalStep{
			{Action: "source_restart", Layer: "environment", Params: json.RawMessage(`{"service":"connect"}`)},
			{Action: "source_restart", Layer: "environment", Params: json.RawMessage(`{"service":"connect"}`)},
		},
	}
	h := proposalHash(t, p1)
	raw := proposalRaw(t, p1)

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, h, raw)})
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("b", "source_restart", protocol.TierT3, "p1", 1, h, raw)})

		results, refused := 0, 0
		for i := 0; i < 2; i++ {
			f := readFrameJSON(t, conn, ctx)
			switch f.Type {
			case protocol.FrameResult:
				results++
			case protocol.FrameRefused:
				if f.Refused.Reason != protocol.RefuseNotConfirmed {
					t.Fatalf("refusal reason = %q, want %q", f.Refused.Reason, protocol.RefuseNotConfirmed)
				}
				refused++
			default:
				t.Fatalf("unexpected frame %+v", f)
			}
		}
		if results != 1 || refused != 1 {
			t.Fatalf("got %d results and %d refusals, want exactly 1 and 1", results, refused)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: strings.NewReader("y\n"), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 1 {
		t.Fatalf("executor called %d times, want 1 (only the confirmed T3 op runs)", n)
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

// TestT3ConfirmationBoundByTheOpDeadline: the "y" must come before the op's
// own deadline (the server stops waiting then and fails the step). No answer
// by the deadline is refused as deadline; a "y" typed after it is refused too,
// and in neither case is the step marked started or run.
func TestT3ConfirmationBoundByTheOpDeadline(t *testing.T) {
	t.Run("no answer by the deadline", func(t *testing.T) {
		cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
		srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
			doHandshake(t, conn, ctx, "tok", time.Now, nil)
			p1 := testProposalWithAction("p1", "source_restart")
			req := opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
			req.Deadline = time.Now().Add(300 * time.Millisecond)
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: req})
			assertRefused(t, conn, ctx, protocol.RefuseDeadline)
		})
		defer srv.Close()
		r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: blockingReader{started: make(chan struct{})}, Stdout: io.Discard, Log: quiet(), Catalogue: cat})
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

	t.Run("a y after the deadline", func(t *testing.T) {
		cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
		clock := newTestClock()
		srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
			doHandshake(t, conn, ctx, "tok", clock.Now, nil)
			p1 := testProposalWithAction("p1", "source_restart")
			req := opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
			// An hour away (the clock starts at the real time), so the op's
			// context never expires: only the check after the answer sees
			// that the runner's clock passed it.
			req.Deadline = clock.Now().Add(time.Hour)
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: req})
			assertRefused(t, conn, ctx, protocol.RefuseDeadline)
		})
		defer srv.Close()
		// The person answers y, two hours later on the runner's clock.
		stdin := &lateYes{clock: clock, after: 2 * time.Hour}
		r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: stdin, Stdout: io.Discard, Log: quiet(), Now: clock.Now, Catalogue: cat})
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
}

// lateYes answers "y" once, after advancing clock by after.
type lateYes struct {
	clock *testClock
	after time.Duration
	done  bool
}

func (l *lateYes) Read(p []byte) (int, error) {
	if l.done {
		select {}
	}
	l.done = true
	l.clock.Advance(l.after)
	return copy(p, "y\n"), nil
}

// delayedYes answers "y" once, at a fixed real time.
type delayedYes struct {
	at   time.Time
	done bool
}

func (d *delayedYes) Read(p []byte) (int, error) {
	if d.done {
		select {}
	}
	time.Sleep(time.Until(d.at))
	d.done = true
	return copy(p, "y\n"), nil
}

// TestStaleYesDoesNotConfirmTheNextT3 is the regression for a late "y": the
// person answers op A's prompt after A's deadline passed (A was refused), and
// the line must not confirm the next T3 op B, which nobody was asked about yet.
func TestStaleYesDoesNotConfirmTheNextT3(t *testing.T) {
	cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
	start := time.Now()
	var stdout syncBuffer
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposalWithAction("p1", "source_restart")
		a := opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
		a.Deadline = time.Now().Add(300 * time.Millisecond)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: a})
		assertRefused(t, conn, ctx, protocol.RefuseDeadline)
		// The late "y" for A is typed at ~800 ms; B arrives at ~1.5 s and
		// nobody answers it.
		time.Sleep(time.Until(start.Add(1500 * time.Millisecond)))
		p2 := testProposalWithAction("p2", "source_restart")
		b := opReqPtr("b", "source_restart", protocol.TierT3, "p2", 0, proposalHash(t, p2), proposalRaw(t, p2))
		// B has ample time left (a stale "y" would pass the start margin);
		// the server gives up on it after a while.
		b.Deadline = time.Now().Add(time.Hour)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: b})
		time.Sleep(300 * time.Millisecond)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameCancel, Cancel: &protocol.Cancel{ID: "b"}})
		assertRefused(t, conn, ctx, protocol.RefuseNotConfirmed)
	})
	defer srv.Close()
	r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: &delayedYes{at: start.Add(800 * time.Millisecond)}, Stdout: &stdout, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 0 {
		t.Fatalf("op B ran %d time(s) on a \"y\" typed for the expired op A", n)
	}
	if !strings.Contains(stdout.String(), "confirmation expired") {
		t.Fatalf("the terminal was not told the prompt expired: %q", stdout.String())
	}
}

// TestServerCancelDropsTheT3Prompt: a FrameCancel for a T3 op still waiting
// for its "y" (the room lost its claim on the proposal) drops the prompt and
// refuses the op at once, long before its deadline; a "y" typed afterwards
// confirms neither it nor the next T3 op.
func TestServerCancelDropsTheT3Prompt(t *testing.T) {
	cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
	start := time.Now()
	var stdout syncBuffer
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposalWithAction("p1", "source_restart")
		a := opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
		a.Deadline = time.Now().Add(time.Hour)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: a})
		time.Sleep(200 * time.Millisecond) // the prompt is up
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameCancel, Cancel: &protocol.Cancel{ID: "a"}})
		assertRefused(t, conn, ctx, protocol.RefuseNotConfirmed)
		if waited := time.Since(start); waited > 700*time.Millisecond {
			t.Errorf("the cancel took %s to drop the prompt", waited)
		}
		// The "y" is typed at ~800 ms, for the dropped prompt; B arrives at
		// ~1.5 s and nobody answers it.
		time.Sleep(time.Until(start.Add(1500 * time.Millisecond)))
		p2 := testProposalWithAction("p2", "source_restart")
		b := opReqPtr("b", "source_restart", protocol.TierT3, "p2", 0, proposalHash(t, p2), proposalRaw(t, p2))
		// B has ample time left (a stale "y" would pass the start margin);
		// the server gives up on it after a while.
		b.Deadline = time.Now().Add(time.Hour)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: b})
		time.Sleep(300 * time.Millisecond)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameCancel, Cancel: &protocol.Cancel{ID: "b"}})
		assertRefused(t, conn, ctx, protocol.RefuseNotConfirmed)
	})
	defer srv.Close()
	r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: &delayedYes{at: start.Add(800 * time.Millisecond)}, Stdout: &stdout, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 0 {
		t.Fatalf("an op ran %d time(s) after its prompt was cancelled", n)
	}
	if !strings.Contains(stdout.String(), "cancelled by the server") {
		t.Fatalf("the terminal was not told the op was cancelled: %q", stdout.String())
	}
}

// TestPipedAnswersStillAnswerInOrder: after an answered prompt, answers read
// ahead (piped stdin) still answer the following prompts; only lines typed
// for an abandoned prompt are dropped.
func TestPipedAnswersStillAnswerInOrder(t *testing.T) {
	cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		for i, id := range []string{"a", "b"} {
			p := testProposalWithAction("p"+id, "source_restart")
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: opReqPtr(id, "source_restart", protocol.TierT3, "p"+id, 0, proposalHash(t, p), proposalRaw(t, p))})
			if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult {
				t.Fatalf("op %d: got %+v, want a result", i, f)
			}
		}
	})
	defer srv.Close()
	r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: strings.NewReader("y\ny\n"), Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 2 {
		t.Fatalf("executor called %d times, want 2", n)
	}
}

// TestT3ConfirmEdges: a "y" with less than the start margin left is refused
// as deadline, and a terminal that cannot be read is no confirmation.
func TestT3ConfirmEdges(t *testing.T) {
	run := func(t *testing.T, stdin io.Reader, clock *testClock, deadline func() time.Time, want string) {
		t.Helper()
		cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}}
		srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
			doHandshake(t, conn, ctx, "tok", clock.Now, nil)
			p1 := testProposalWithAction("p1", "source_restart")
			req := opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
			req.Deadline = deadline()
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: req})
			assertRefused(t, conn, ctx, want)
		})
		defer srv.Close()
		r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: stdin, Stdout: io.Discard, Log: quiet(), Now: clock.Now, Catalogue: cat})
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
	t.Run("y with under the margin left", func(t *testing.T) {
		clock := newTestClock()
		// The deadline is an hour away; the y comes 2 s before it.
		run(t, &lateYes{clock: clock, after: time.Hour - 2*time.Second}, clock,
			func() time.Time { return clock.Now().Add(time.Hour) }, protocol.RefuseDeadline)
	})
	t.Run("unreadable terminal", func(t *testing.T) {
		clock := newTestClock()
		run(t, failingReader{}, clock, func() time.Time { return time.Now().Add(time.Hour) }, protocol.RefuseNotConfirmed)
	})
}

// failingReader fails every read with a non-EOF error.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("terminal gone") }

// readSpy records whether anything read the runner's stdin.
type readSpy struct{ reads atomic.Int32 }

func (r *readSpy) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return copy(p, "y\n"), nil
}

// TestT3OffByDefault: a runner built without AllowT3 (as cmd/setup builds it)
// refuses every T3 op as tier_disabled before any prompt: stdin is never
// read (a "y" waiting there changes nothing), nothing is marked started and
// the executor is never called. The same op at T2 runs.
func TestT3OffByDefault(t *testing.T) {
	cat := &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT2}}
	var stdout syncBuffer
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposalWithAction("p1", "source_restart")
		a := opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: a})
		assertRefused(t, conn, ctx, "tier_disabled")
		// The step was not burned: the same step at T2 still runs.
		b := opReqPtr("b", "source_restart", protocol.TierT2, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: b})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult {
			t.Errorf("the T2 op: got %+v, want a result", f)
		}
	})
	defer srv.Close()
	spy := &readSpy{}
	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: spy, Stdout: &stdout, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := spy.reads.Load(); n != 0 {
		t.Fatalf("stdin read %d time(s) for a disabled T3 op", n)
	}
	if n := cat.callCount(); n != 1 {
		t.Fatalf("executor called %d time(s), want 1 (the T2 op only)", n)
	}
	if strings.Contains(stdout.String(), "confirm?") {
		t.Fatalf("a disabled T3 op prompted: %q", stdout.String())
	}
}

// slowCheckCatalogue holds every op in its target check until release is
// closed, so a test can land a FrameCancel while the op is still in runOp's
// checks (not started, not prompting).
type slowCheckCatalogue struct {
	*fakeCatalogue
	entered chan struct{}
	release chan struct{}
}

func (c *slowCheckCatalogue) CheckTarget(protocol.OpRequest) string {
	c.entered <- struct{}{}
	<-c.release
	return ""
}

// TestCancelDuringChecksStopsTheOp: a FrameCancel read while the op is still
// in its checks reaches it (the read loop registers the op's cancel before
// its goroutine starts), so the op is refused and never runs.
func TestCancelDuringChecksStopsTheOp(t *testing.T) {
	cat := &slowCheckCatalogue{
		fakeCatalogue: &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT1}},
		entered:       make(chan struct{}, 1),
		release:       make(chan struct{}),
	}
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposalWithAction("p1", "source_restart")
		a := opReqPtr("a", "source_restart", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
		a.Deadline = time.Now().Add(time.Hour)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: a})
		<-cat.entered
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameCancel, Cancel: &protocol.Cancel{ID: "a"}})
		// The read loop handles a cancel at once; give it ample time before
		// the check returns.
		time.Sleep(300 * time.Millisecond)
		close(cat.release)
		assertRefused(t, conn, ctx, protocol.RefuseNotConfirmed)
	})
	defer srv.Close()
	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 0 {
		t.Fatalf("op ran %d time(s) after it was cancelled during its checks", n)
	}
}

// TestCancelBeforeThePromptDropsAStaleYes: a T3 op cancelled while still in
// its checks never shows its prompt, but a "y" typed meanwhile (for it) must
// not confirm the next T3 op either.
func TestCancelBeforeThePromptDropsAStaleYes(t *testing.T) {
	cat := &slowCheckCatalogue{
		fakeCatalogue: &fakeCatalogue{floor: map[string]string{"source_restart": protocol.TierT3}},
		entered:       make(chan struct{}, 2),
		release:       make(chan struct{}),
	}
	start := time.Now()
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposalWithAction("p1", "source_restart")
		a := opReqPtr("a", "source_restart", protocol.TierT3, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))
		a.Deadline = time.Now().Add(time.Hour)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: a})
		<-cat.entered
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameCancel, Cancel: &protocol.Cancel{ID: "a"}})
		time.Sleep(300 * time.Millisecond)
		close(cat.release)
		assertRefused(t, conn, ctx, protocol.RefuseNotConfirmed)
		// The "y" is typed at ~600 ms; B arrives at ~1.2 s and nobody
		// answers it.
		time.Sleep(time.Until(start.Add(1200 * time.Millisecond)))
		p2 := testProposalWithAction("p2", "source_restart")
		b := opReqPtr("b", "source_restart", protocol.TierT3, "p2", 0, proposalHash(t, p2), proposalRaw(t, p2))
		b.Deadline = time.Now().Add(time.Hour)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: b})
		time.Sleep(300 * time.Millisecond)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameCancel, Cancel: &protocol.Cancel{ID: "b"}})
		assertRefused(t, conn, ctx, protocol.RefuseNotConfirmed)
	})
	defer srv.Close()
	r, err := runner.New(runner.Options{AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: t.TempDir(), Stdin: &delayedYes{at: start.Add(600 * time.Millisecond)}, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
	if n := cat.callCount(); n != 0 {
		t.Fatalf("an op ran %d time(s) on a \"y\" typed before its prompt", n)
	}
}
