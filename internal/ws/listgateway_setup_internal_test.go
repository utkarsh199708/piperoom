package ws

import (
	"testing"

	"piperoom/internal/room"
)

// KAN-602: the digest follows the setup fields a list row shows (mode, phase,
// runner.connected) and ignores the rest of the setup member, so a phase
// change sends a frame and a plan or target change does not.
func TestListDigestFollowsSetupRowFields(t *testing.T) {
	base := room.NewSnapshot()
	base.ID, base.Status, base.Title = "r_1", "open", "Orders setup"
	base.Setup = &room.SetupSnapshot{Mode: true, Phase: "plan"}
	d0 := listDigest(&base)

	with := func(edit func(s *room.SetupSnapshot)) [32]byte {
		c := base
		s := *base.Setup
		edit(&s)
		c.Setup = &s
		return listDigest(&c)
	}
	if with(func(s *room.SetupSnapshot) { s.Phase = "build" }) == d0 {
		t.Error("a phase change is not in the digest")
	}
	up := with(func(s *room.SetupSnapshot) { s.Runner = &room.SetupRunnerState{Connected: true} })
	if up == d0 {
		t.Error("a runner attaching is not in the digest")
	}
	if with(func(s *room.SetupSnapshot) { s.Runner = &room.SetupRunnerState{Connected: false} }) == up {
		t.Error("runner.connected is not in the digest")
	}
	if with(func(s *room.SetupSnapshot) { s.Runner = &room.SetupRunnerState{Connected: true, Label: "laptop"} }) != up {
		t.Error("the runner label is in the digest, but the list row drops it")
	}
	if with(func(s *room.SetupSnapshot) { s.PlanID, s.StepIndex = "plan_2", 3 }) != d0 {
		t.Error("a field the list row drops is in the digest")
	}
	// The full member and the trimmed row of one room hash the same, so a
	// resync row does not look changed against a live frame.
	svc := room.New(room.Deps{})
	defer svc.Close()
	if listDigest(svc.ListRow(&base)) != d0 {
		t.Error("trimmed row and full snapshot hash differently")
	}
}
