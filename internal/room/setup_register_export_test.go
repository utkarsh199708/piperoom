package room

import (
	"context"

	"piperoom/internal/events"
)

// EmitSetupVerifiedForTest appends setup.verified for the room's current plan
// the way the ladder runner does (emitLocked under the room's lock). EmitSetup
// refuses the type on purpose (U1-8b: only the ladder runner writes the
// verdict), so the tests of the register gate that do not run a ladder use
// this; TestRegisterAfterRealLadder runs the real one. Test binaries only.
func EmitSetupVerifiedForTest(ctx context.Context, svc *Service, roomID string, rungs []events.SetupRung) error {
	st, err := svc.lockRoom(ctx, roomID)
	if err != nil {
		return err
	}
	defer svc.unlockRoom(st)
	planID := ""
	if st.snap.Setup != nil {
		planID = st.snap.Setup.PlanID
	}
	_, err = svc.emitLocked(ctx, st, roomID, events.SetupVerified, systemActor, events.SetupVerifiedPayload{
		PlanID: planID, Rungs: rungs,
	})
	return err
}
