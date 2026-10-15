package room_test

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

// QA R8: a person's resolution newer than an earlier executed step or fix note
// is the newest sign of a fix, so a hand-off older than it is shown as stale.
func TestPostmortemResolutionNewerThanEarlierFixSignalIsEvidence(t *testing.T) {
	for _, variant := range []string{"exec", "fixnote"} {
		t.Run(variant, func(t *testing.T) {
			svc, st, c := newTestService(t)
			r := createRoom(t, svc, "arjun")
			id := r.ID
			investigation(t, st, c, id)
			if variant == "exec" {
				app(t, st, c, id, events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{
					ProposalID: "p_1", Title: "Fix", Risk: "low", RequiredLayers: []string{"consumer"},
					Steps: []events.ProposalStep{{Action: "restart_consumer", Layer: "consumer", Description: "restart"}},
				})
				app(t, st, c, id, events.ActionExecuted, pmSystem, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true, Output: "ok"})
			} else {
				app(t, st, c, id, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{CommentID: "c_1", Kind: events.CommentKindFixNote, Text: "tried restart"})
			}
			handoffLog(t, st, c, id, "h_1", staleSummary())
			app(t, st, c, id, events.RoomClosed, pmHuman("kabir", "Kabir"), events.RoomClosedPayload{Resolution: "Fixed: restarted the consumer, lag drained"})
			app(t, st, c, id, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: "h_1"})
			md := renderRoom(t, svc, id)
			if strings.Contains(section(t, md, "## Still open"), "- Restart the consumer") && !strings.Contains(md, "Earlier, from the") {
				t.Fatalf("stale open item shown as current:\n%s", md)
			}
		})
	}
}
