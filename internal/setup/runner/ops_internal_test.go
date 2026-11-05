package runner

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// testCatalogue is a minimal Catalogue for the internal op tests.
type testCatalogue struct {
	floor  map[string]string
	action map[string]string
	calls  int
}

func (c *testCatalogue) Execute(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
	c.calls++
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok"}, nil
}

func (c *testCatalogue) Floor(op string) (string, bool)     { t, ok := c.floor[op]; return t, ok }
func (c *testCatalogue) Target(protocol.OpRequest) string   { return "" }
func (c *testCatalogue) GuardForbidden(string, string) bool { return false }
func (c *testCatalogue) StepAction(op string) (string, bool) {
	if c.action != nil {
		if a, ok := c.action[op]; ok {
			return a, true
		}
	}
	return op, true
}

// TestMarkStartedFailsClosedWhenStartRecordNotDurable: a T1+ op whose durable
// "started" record cannot be written is refused rather than run, and the
// in-memory ledger is left untouched (so the same step could still be refused
// as a duplicate only after a real start).
func TestMarkStartedFailsClosedWhenStartRecordNotDurable(t *testing.T) {
	dir := t.TempDir()
	// A closed file makes every append fail with os.ErrClosed, standing in for
	// a disk that refuses the write or sync.
	broken, err := os.Create(filepath.Join(dir, "broken"))
	if err != nil {
		t.Fatal(err)
	}
	if err := broken.Close(); err != nil {
		t.Fatal(err)
	}

	r := &Runner{
		projectDir: dir,
		stdout:     io.Discard,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:        time.Now,
		catalogue:  &testCatalogue{floor: map[string]string{"compose_up": protocol.TierT1}},
		started:    map[stepKey]struct{}{},
		proposals:  map[string]events.ProposalCreatedPayload{},
		floors:     map[string]string{},
		inflight:   map[string]*inflightOp{},
		opLog:      &opLog{f: broken},
	}
	step := 0
	req := &protocol.OpRequest{
		ID: "a", Op: "compose_up", Params: json.RawMessage(`{}`), Tier: protocol.TierT1,
		ProposalID: "p1", StepIndex: &step,
	}

	if err := r.markStarted(req, "", protocol.ClassLocal, func() {}); err == nil {
		t.Fatal("markStarted = nil, want an error when the start record is not durable")
	}

	r.opMu.Lock()
	defer r.opMu.Unlock()
	if _, ok := r.started[stepKey{proposalID: "p1", step: 0}]; ok {
		t.Error("started ledger was updated despite the failed durable write")
	}
	if _, ok := r.inflight["a"]; ok {
		t.Error("op was marked in-flight despite the failed durable write")
	}
}

// TestBuildResumeKeepsDoneUntilDropped: buildResume reports done ops but does
// not delete them; the caller deletes only the ids it actually delivered after
// the Resume write succeeds, so a failed write keeps the results and a result
// that finished after the snapshot stays for the next Resume.
func TestBuildResumeKeepsDoneUntilDropped(t *testing.T) {
	doneResult := &protocol.OpResult{ID: "done", OK: true, Summary: "ok"}
	r := &Runner{
		inflight: map[string]*inflightOp{
			"done":    {status: protocol.OpStatus{ID: "done", State: protocol.StateDone, Result: doneResult}},
			"running": {status: protocol.OpStatus{ID: "running", State: protocol.StateRunning}},
		},
	}

	res, done := r.buildResume()
	if len(res.InFlight) != 2 {
		t.Fatalf("resume in_flight = %+v, want 2 entries", res.InFlight)
	}
	if len(done) != 1 || done[0] != "done" {
		t.Fatalf("done ids = %v, want [done]", done)
	}

	// buildResume must not delete anything yet: a failed write keeps the
	// results in place for the next Resume.
	if _, ok := r.inflight["done"]; !ok {
		t.Fatal("buildResume deleted the done entry before delivery")
	}

	// A result that finishes after the snapshot is not in the returned ids and
	// must survive dropDone.
	r.inflight["late"] = &inflightOp{status: protocol.OpStatus{ID: "late", State: protocol.StateRunning}}
	r.markDone("late", &protocol.OpResult{ID: "late", OK: true, Summary: "late"})

	r.dropDone(done)

	if _, ok := r.inflight["done"]; ok {
		t.Error("dropDone did not delete the delivered done entry")
	}
	if _, ok := r.inflight["running"]; !ok {
		t.Error("dropDone deleted a running entry")
	}
	if _, ok := r.inflight["late"]; !ok {
		t.Error("dropDone deleted a result that was not in the delivered Resume")
	}
}

// TestWriteFrameResultWaitsForHandshake: a FrameResult is held until the
// reconnect handshake finishes, while other frames are not; a result from an op
// that finished in the handshake window cannot be the first frame on the new
// socket.
func TestWriteFrameResultWaitsForHandshake(t *testing.T) {
	r := &Runner{}
	r.beginHandshake()

	resCh := make(chan error, 1)
	go func() {
		resCh <- r.writeFrame(context.Background(), protocol.Frame{Type: protocol.FrameResult, Result: &protocol.OpResult{ID: "x", OK: true}})
	}()

	select {
	case <-resCh:
		t.Fatal("writeFrame(FrameResult) returned while the handshake was in progress")
	case <-time.After(50 * time.Millisecond):
	}

	r.endHandshake()

	select {
	case <-resCh: // returns (conn is nil here, so "not connected" is expected)
	case <-time.After(2 * time.Second):
		t.Fatal("writeFrame(FrameResult) did not return after the handshake finished")
	}

	// A non-result frame is not held back by the handshake.
	r2 := &Runner{}
	r2.beginHandshake()
	pingCh := make(chan error, 1)
	go func() {
		pingCh <- r2.writeFrame(context.Background(), protocol.Frame{Type: protocol.FramePing})
	}()
	select {
	case <-pingCh:
	case <-time.After(time.Second):
		t.Fatal("writeFrame(FramePing) was held up by the handshake")
	}
}
