package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
	"piperoom/internal/setup/targets/host"
)

// The real registry raises a floor by an op's params (U-fu8).
var _ runner.ParamFloorer = (*ops.Registry)(nil)

// workerEntries are the keys a distributed worker file needs.
func workerEntries() map[string]string {
	return map[string]string{
		"bootstrap.servers":                 "127.0.0.1:29092",
		"group.id":                          "piperoom-connect",
		"config.storage.topic":              "piperoom-configs",
		"offset.storage.topic":              "piperoom-offsets",
		"status.storage.topic":              "piperoom-status",
		"config.storage.replication.factor": "1",
		"offset.storage.replication.factor": "1",
		"status.storage.replication.factor": "1",
		"key.converter":                     "org.apache.kafka.connect.json.JsonConverter",
		"value.converter":                   "org.apache.kafka.connect.json.JsonConverter",
	}
}

// propertiesStep is one properties_write request bound to its own frozen
// one-step proposal, at tier.
func propertiesStep(t *testing.T, id, proposalID, tier, file string) *protocol.OpRequest {
	t.Helper()
	params, err := json.Marshal(host.PropertiesParams{File: file, Entries: workerEntries()})
	if err != nil {
		t.Fatal(err)
	}
	p := events.ProposalCreatedPayload{
		ProposalID: proposalID, Title: "write the worker file", Risk: "low", Rollback: "restore the file",
		RequiredLayers: []string{"environment"},
		Steps:          []events.ProposalStep{{Action: "properties_write", Layer: "environment", Params: params, Description: "worker file"}},
	}
	step := 0
	return &protocol.OpRequest{
		ID: id, Op: "properties_write", Params: params, Tier: tier, Class: protocol.ClassLocal,
		ProposalID: proposalID, StepIndex: &step, ParamsHash: proposalHash(t, p), Proposal: proposalRaw(t, p),
	}
}

// TestParamTierRefusesAT1StepOutsideTheProject drives the runner with the
// real catalogue and the host target routed by tier: a properties file
// outside the project directory sent at T1 is refused below its floor and
// nothing is written; the same file at T2 runs and writes; the project-root
// worker file stays T1 and runs.
func TestParamTierRefusesAT1StepOutsideTheProject(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX outside paths; the Windows path shape is covered by host's own tests")
	}
	project := t.TempDir()
	outsideDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outside := outsideDir + "/connect-distributed.properties"
	cat, err := ops.New(ops.Config{ProjectDir: project})
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	if _, err := host.Register(cat, host.Options{TierRouted: true, ResolvePath: func(p string) (string, error) { return p, nil }}); err != nil {
		t.Fatalf("host.Register: %v", err)
	}

	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: propertiesStep(t, "a", "p_out_t1", protocol.TierT1, outside)})
		assertRefused(t, conn, ctx, protocol.RefuseTierBelowFloor)
		if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a T1 step wrote outside the project: %v", err)
		}

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: propertiesStep(t, "b", "p_out_t2", protocol.TierT2, outside)})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult || f.Result == nil || !f.Result.OK {
			t.Fatalf("T2 outside: %+v, want an OK result", f)
		}
		if _, err := os.Stat(outside); err != nil {
			t.Errorf("the T2 step did not write the outside file: %v", err)
		}

		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: propertiesStep(t, "c", "p_root_t1", protocol.TierT1, "connect-distributed.properties")})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult || f.Result == nil || !f.Result.OK {
			t.Fatalf("T1 project-root file: %+v, want an OK result", f)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: project, Stdout: io.Discard, Log: quiet(), Catalogue: cat})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
}

// paramFloorCatalogue is fakeCatalogue with a ParamFloorer answer.
type paramFloorCatalogue struct {
	*fakeCatalogue
	floorFor func(op string, params json.RawMessage) (string, bool, error)
}

func (c *paramFloorCatalogue) FloorFor(op string, params json.RawMessage) (string, bool, error) {
	return c.floorFor(op, params)
}

// TestParamFloorErrorIsRefused: a catalogue that cannot compute a request's
// param tier gets the request refused with RefuseSchema, never executed at the
// fixed floor.
func TestParamFloorErrorIsRefused(t *testing.T) {
	inner := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}
	cat := &paramFloorCatalogue{fakeCatalogue: inner, floorFor: func(op string, _ json.RawMessage) (string, bool, error) {
		return protocol.TierT1, true, errors.New("cannot classify")
	}}
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p := testProposal("p1")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "compose_up", protocol.TierT3, "p1", 0, proposalHash(t, p), proposalRaw(t, p))})
		assertRefused(t, conn, ctx, protocol.RefuseSchema)
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
	if n := inner.callCount(); n != 0 {
		t.Fatalf("executor called %d times, want 0", n)
	}
}

// TestParamFloorRaisesAFakeCatalogue: a ParamFloorer's raised floor refuses a
// request at the fixed floor and admits it at the raised one; a catalogue
// without the capability (every other test's fakeCatalogue) keeps Floor.
func TestParamFloorRaisesAFakeCatalogue(t *testing.T) {
	inner := &fakeCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}}
	cat := &paramFloorCatalogue{fakeCatalogue: inner, floorFor: func(op string, _ json.RawMessage) (string, bool, error) {
		return protocol.TierT2, op == "compose_up", nil
	}}
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		p1, p2 := testProposal("p1"), testProposal("p2")
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("a", "compose_up", protocol.TierT1, "p1", 0, proposalHash(t, p1), proposalRaw(t, p1))})
		assertRefused(t, conn, ctx, protocol.RefuseTierBelowFloor)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp,
			Op: opReqPtr("b", "compose_up", protocol.TierT2, "p2", 0, proposalHash(t, p2), proposalRaw(t, p2))})
		if f := readFrameJSON(t, conn, ctx); f.Type != protocol.FrameResult || f.Result == nil || !f.Result.OK {
			t.Fatalf("T2: %+v, want an OK result", f)
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
	if n := inner.callCount(); n != 1 {
		t.Fatalf("executor called %d times, want 1 (the T2 request)", n)
	}
}
