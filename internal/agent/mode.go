package agent

// RoomMode is the room's mode as the runner reads it for wording (P2d). It
// mirrors the two members of the room snapshot's mode that change what the
// agent says (docs/api.md): mode.write and mode.auto_approve. The room service
// fills it and hands it to the runner; until that plumbing lands (a contract
// change), every run gets RoomMode{Write: true}, which reads as write mode on
// with no policy list — the default of rooms before the P wave and of the
// evals.
type RoomMode struct {
	// Write is mode.write: true when a fully approved proposal runs. False
	// (write mode off) makes the CDC prompt tell the agent to say once per
	// room that approved fixes will not run here.
	Write bool
	// AutoApprove is mode.auto_approve: the pipeline's policy actions, in
	// owners.yaml file order. It is carried so the runner can name the policy
	// actions once it also hears a proposal.scheduled for its own proposal;
	// the prompt's scheduling note does not depend on the list.
	AutoApprove []string
}
