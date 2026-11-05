package remoteclients

import (
	"errors"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
)

// allowT3Steps lets a test session send T3 step writes, so the dormant T3
// path stays exercised. Production has no way to set it.
func allowT3Steps() Option { return func(s *Session) { s.allowT3 = true } }

// TestT3StepRefusedBeforeSending: a session refuses a T3 step write itself,
// with ErrT3Disabled and nothing sent: source_alter_system (T3 at its floor)
// and a T2 write on a step routed T3. The step is not spent, and the error
// is not one the room records as outcome unknown.
func TestT3StepRefusedBeforeSending(t *testing.T) {
	for _, tc := range []struct{ method, stepTier string }{
		{"Source.AlterSystem", protocol.TierT2},
		{"Source.AlterSystem", protocol.TierT3},
		{"Source.SetReplicaIdentity", protocol.TierT3},
	} {
		w := writeCaseFor(t, tc.method)
		f := newFakeRunner()
		ctx := writeCtx(t, tc.stepTier, protocol.ClassLocal, w.action, w.params)
		err := w.run(ctx, New(f, "r_setup"))
		if !errors.Is(err, ErrT3Disabled) {
			t.Fatalf("%s on a %s step = %v, want ErrT3Disabled", tc.method, tc.stepTier, err)
		}
		if errors.Is(err, clients.ErrWriteSpent) || errors.Is(err, room.ErrRunnerLost) {
			t.Fatalf("%s: %v reads as sent or as outcome unknown", tc.method, err)
		}
		if n := len(f.allCalls()); n != 0 {
			t.Fatalf("%s: sent %d ops, want 0", tc.method, n)
		}
	}
	// T2 writes are unaffected.
	w := writeCaseFor(t, "Source.SetReplicaIdentity")
	f := newFakeRunner()
	if err := w.run(writeCtx(t, protocol.TierT2, protocol.ClassLocal, w.action, w.params), New(f, "r_setup")); err != nil {
		t.Fatalf("a T2 write: %v", err)
	}
}

// TestT3OffEndToEnd runs source_alter_system against a real runner built as
// cmd/setup builds it (AllowT3 false). The default session refuses the step
// without a round trip; a session that sends it anyway (an older server) gets
// the runner's clean tier_disabled refusal. Either way the catalogue never
// runs it and the error is not outcome unknown.
func TestT3OffEndToEnd(t *testing.T) {
	cat := &stepCatalogue{}
	ex := startRunner(t, cat, false)
	alter := writeCaseFor(t, "Source.AlterSystem")

	ctx := frozenStepCtx(t, "p_t3_client", alter.tier, alter.action, alter.params)
	err := alter.run(ctx, New(ex, "r_setup"))
	if !errors.Is(err, ErrT3Disabled) || errors.Is(err, room.ErrRunnerLost) {
		t.Fatalf("the default session = %v, want ErrT3Disabled", err)
	}

	ctx = frozenStepCtx(t, "p_t3_runner", alter.tier, alter.action, alter.params)
	err = alter.run(ctx, New(ex, "r_setup", allowT3Steps()))
	var ref *protocol.RefusedError
	if !errors.As(err, &ref) || ref.Refusal.Reason != "tier_disabled" {
		t.Fatalf("the runner = %v, want a tier_disabled refusal", err)
	}
	if errors.Is(err, room.ErrRunnerLost) {
		t.Fatalf("the refusal reads as outcome unknown: %v", err)
	}
	if n := len(cat.executed(alter.action)); n != 0 {
		t.Fatalf("the runner ran %s %d times, want 0", alter.action, n)
	}
}
