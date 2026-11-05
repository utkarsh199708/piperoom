package runner_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// TestOpLogRecordsRefusals: a refusal is appended to the op log with its
// reason code.
func TestOpLogRecordsRefusals(t *testing.T) {
	dir := t.TempDir()
	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		step0 := 0
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "a", Op: "compose_up", Params: json.RawMessage(`{}`), Tier: protocol.TierT1,
			StepIndex: &step0, // missing ProposalID and ParamsHash
		}})
		assertRefused(t, conn, ctx, protocol.RefuseNoProposal)
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

	content := readOpLog(t, dir)
	if !strings.Contains(content, `"state":"refused"`) {
		t.Fatalf("op log missing a refused entry: %q", content)
	}
	if !strings.Contains(content, `"reason":"no_proposal"`) {
		t.Fatalf("op log refusal missing the reason: %q", content)
	}
}

// TestOpLogAppendOnly: the op log is never truncated — pre-existing content is
// kept and new entries are appended after it.
func TestOpLogAppendOnly(t *testing.T) {
	dir := t.TempDir()
	opPath := filepath.Join(dir, ".piperoom", "ops.jsonl")
	if err := os.MkdirAll(filepath.Dir(opPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(opPath, []byte("SENTINEL-LINE\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cat := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}

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

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir, Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	content := readOpLog(t, dir)
	if !strings.HasPrefix(content, "SENTINEL-LINE\n") {
		t.Fatalf("op log was truncated; first line should be the sentinel: %q", content)
	}
	if !strings.Contains(content, `"state":"done"`) {
		t.Fatalf("op log missing the done entry: %q", content)
	}
}

// TestOpLogSurvivesRestart: the duplicate ledger is read back from the op log
// on startup, so a (ProposalID, StepIndex) already executed is refused after a
// runner restart.
func TestOpLogSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cat1 := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}

	srv1, done1 := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposal("p1")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult {
			t.Fatalf("expected result, got %+v", f)
		}
	})
	r1, err := runner.New(runner.Options{ServerURL: srv1.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir, Stdout: io.Discard, Log: quiet(), Catalogue: cat1})
	if err != nil {
		t.Fatal(err)
	}
	if err := r1.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done1
	srv1.Close()

	// A fresh runner over the same project dir must refuse the same step.
	cat2 := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}
	srv2, done2 := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1 := testProposal("p1")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
		assertRefused(t, conn, ctx, protocol.RefuseDuplicateStep)
	})
	defer srv2.Close()

	r2, err := runner.New(runner.Options{ServerURL: srv2.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir, Stdout: io.Discard, Log: quiet(), Catalogue: cat2})
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done2

	if n := cat2.callCount(); n != 0 {
		t.Fatalf("executor called %d times after restart, want 0", n)
	}
}
