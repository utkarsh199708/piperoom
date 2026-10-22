package runner_test

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// TestRealCatalogueAdapter drives the runner with internal/setup/ops instead
// of a fake: the registry satisfies runner.Catalogue structurally, an unknown
// op is refused with the protocol code, a T1 op without a proposal is refused,
// and a T0 op runs and returns an OpResult.
func TestRealCatalogueAdapter(t *testing.T) {
	dir := t.TempDir()
	cat, err := ops.New(ops.Config{ProjectDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "a", Op: "no_such_op", Params: json.RawMessage(`{}`), Tier: protocol.TierT0,
		}})
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameRefused || f.Refused == nil || f.Refused.Reason != protocol.RefuseUnknownOp {
			t.Fatalf("unknown op frame = %+v, want refused/%s", f, protocol.RefuseUnknownOp)
		}

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "b", Op: "file_write", Tier: protocol.TierT1,
			Params: json.RawMessage(`{"path":"notes.txt","content":"x"}`),
		}})
		f = readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameRefused || f.Refused == nil || f.Refused.Reason != protocol.RefuseNoProposal {
			t.Fatalf("T1 without proposal frame = %+v, want refused/%s", f, protocol.RefuseNoProposal)
		}

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "c", Op: "http_get", Tier: protocol.TierT0,
			Params: json.RawMessage(`{"url":"http://127.0.0.1:1/"}`),
		}})
		f = readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameRefused || f.Refused == nil || f.Refused.Reason != protocol.RefuseTarget {
			t.Fatalf("http_get outside targets frame = %+v, want refused/%s", f, protocol.RefuseTarget)
		}

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "d", Op: "env_detect", Params: json.RawMessage(`{}`), Tier: protocol.TierT0,
		}})
		f = readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameResult || f.Result == nil || !f.Result.OK {
			t.Fatalf("env_detect frame = %+v, want an OK result", f)
		}
		if len(f.Result.Data) == 0 {
			t.Fatalf("env_detect Data is empty: %+v", f.Result)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdout: io.Discard, Log: quiet(), Catalogue: cat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
}
