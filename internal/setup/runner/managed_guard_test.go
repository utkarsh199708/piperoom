package runner_test

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
	"piperoom/internal/setup/targets/managed"
)

// mskNoCalls satisfies managed.MSKAPI; the guard refuses before any call, so
// a call would panic on the nil embedded interface and fail the test.
type mskNoCalls struct{ managed.MSKAPI }

// TestGuardUsesTheDriversProductionClass (review L2): the MSK driver classes
// its account production from PIPEROOM_SETUP_PROD_PATTERNS; the server's
// Welcome says shared. The runner's guard asks the driver too, so
// msk_delete_connector is refused with RefuseGuard on a runner that allows
// T3, before any prompt or cloud call.
func TestGuardUsesTheDriversProductionClass(t *testing.T) {
	dir := t.TempDir()
	aws := events.SetupTarget{Kind: managed.KindAWS, Address: "123456789012/eu-west-1", Class: protocol.ClassShared}
	cat, err := ops.New(ops.Config{ProjectDir: dir, Targets: []events.SetupTarget{aws}})
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	if _, err := managed.RegisterMSK(cat, managed.MSKOptions{
		Client: mskNoCalls{}, Target: aws, RoomID: "r_setup", ProdPatterns: []string{"123456789012"},
	}); err != nil {
		t.Fatal(err)
	}
	if !cat.GuardForbidden("msk_delete_connector", protocol.ClassShared) || !cat.GuardForbidden("msk_delete_connector", "") {
		t.Fatal("the registry's guard does not use the driver's production class")
	}
	if cat.GuardForbidden(managed.ReadMSKConnector, protocol.ClassShared) {
		t.Fatal("a read is guarded")
	}

	step := 0
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, []events.SetupTarget{aws})
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "del", Op: "msk_delete_connector", Tier: protocol.TierT3, Class: protocol.ClassShared,
			Params:     json.RawMessage(`{"name":"orders-src"}`),
			ProposalID: "p1", StepIndex: &step, ParamsHash: "h",
		}})
		f := readFrameJSON(t, conn, ctx)
		if f.Type != protocol.FrameRefused || f.Refused == nil || f.Refused.Reason != protocol.RefuseGuard {
			t.Errorf("production delete frame = %+v, want refused/%s", f, protocol.RefuseGuard)
		}
	})
	defer srv.Close()

	r, err := runner.New(runner.Options{
		AllowT3: true, ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdin: strings.NewReader("y\n"), Stdout: io.Discard, Log: quiet(), Catalogue: cat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done
}
