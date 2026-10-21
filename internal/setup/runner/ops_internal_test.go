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
