package agent

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/tools"
)

// An alert with no pipeline must not inherit a pipeline from the context the
// caller passed to Run; it is refused, and only the explicit opt-out helps.
func TestRunAlertWithoutPipelineDoesNotInheritScope(t *testing.T) {
	probe := &scopeProbe{owners: probeOwners()}
	r := newTestRunner(t, probeModel(t), &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, probe, Options{MaxTurns: 3})
	r.alert.PipelineID = ""

	if err := r.run(tools.WithPipelineID(context.Background(), "billing")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(probe.errs) != 1 || !errors.Is(probe.errs[0], tools.ErrNoScope) {
		t.Fatalf("errs = %v, want ErrNoScope (no inherited billing scope)", probe.errs)
	}
}
