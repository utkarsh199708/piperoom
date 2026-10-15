package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
)

// openAndClose opens an alert room, gives arjun the driver role, optionally
// reports the alert resolved, and closes the room with a resolution.
func openAndClose(t *testing.T, resolvedFirst bool) string {
	t.Helper()
	ctx := context.Background()
	svc, _, _ := newTestService(t)
	a := alertin.Alert{Source: alertin.Generic, Status: alertin.StatusFiring, Title: "orders connector down", Severity: "sev2", PipelineID: "orders", DedupKey: "k1"}
	snap, _, err := svc.OpenAlertRoom(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	arjun := mustUser(t, svc, "arjun")
	if _, err := svc.GrantControl(ctx, arjun, snap.ID, "arjun"); err != nil {
		t.Fatal(err)
	}
	if resolvedFirst {
		a.Status = alertin.StatusResolved
		if _, _, err := svc.OpenAlertRoom(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.CloseRoom(ctx, arjun, snap.ID, "restarted the connector by hand"); err != nil {
		t.Fatal(err)
	}
	return renderRoom(t, svc, snap.ID)
}

// TestPostmortemCloseWhileFiringIsNotAFix (Q5 T4): a room closed while its
// alert still fired (no resolved report, no all-clear) was not shown fixed,
// whatever the resolution says.
func TestPostmortemCloseWhileFiringIsNotAFix(t *testing.T) {
	md := openAndClose(t, false)
	if strings.Contains(md, "outside Piperoom") {
		t.Fatalf("a close while firing reads as a person's fix:\n%s", md)
	}
	if v := section(t, md, "## Verification"); !strings.Contains(v, "still firing") {
		t.Fatalf("verification must say the alert was still firing:\n%s", v)
	}
	// Reported resolved before the close: the resolution is fix evidence.
	md = openAndClose(t, true)
	if !strings.Contains(md, "the fix was made outside Piperoom, per the resolution") {
		t.Fatalf("a close after the all-clear lost the person's fix:\n%s", md)
	}
}

// TestPostmortemWatcherAllClear (Q5 T4): the watcher's all-clear note in a
// room it only joined ends the firing state; a later evidence note starts it
// again.
func TestPostmortemWatcherAllClear(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		notes  []string
		firing bool
	}{
		{[]string{"watcher: orders: all watcher signals are clear"}, false},
		{[]string{"watcher: orders: all watcher signals are clear", "evidence"}, true},
	} {
		svc, _, _ := newTestService(t)
		snap, _, err := svc.OpenAlertRoom(ctx, alertin.Alert{Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2", PipelineID: "orders"})
		if err != nil {
			t.Fatal(err)
		}
		arjun := mustUser(t, svc, "arjun")
		if _, err := svc.GrantControl(ctx, arjun, snap.ID, "arjun"); err != nil {
			t.Fatal(err)
		}
		for _, n := range tc.notes {
			var ev json.RawMessage
			if n == "evidence" {
				ev = json.RawMessage(`{"source":"watcher","signals":[{"signal":"consumer_lag"}]}`)
			}
			if err := svc.PostRoomEvidence(ctx, snap.ID, n, ev); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := svc.CloseRoom(ctx, arjun, snap.ID, "fixed the consumer"); err != nil {
			t.Fatal(err)
		}
		md := renderRoom(t, svc, snap.ID)
		if got := !strings.Contains(md, "outside Piperoom"); got != tc.firing {
			t.Fatalf("notes %v: firing at close = %v, want %v:\n%s", tc.notes, got, tc.firing, md)
		}
	}
}

// TestPostmortemTimelinePagesAndRestarts (Q5 T5): the timeline lists every
// owner.paged (who paged, the layer, automatic or not) and the server's
// restart and takeover notes, so a reader sees why the agent went quiet.
func TestPostmortemTimelinePagesAndRestarts(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	app(t, st, c, r.ID, events.OwnerPaged, pmSystem, events.OwnerPagedPayload{
		Layer: "consumer", Owners: []string{"kabir"}, Reason: "nobody drives", Auto: true,
	})
	app(t, st, c, r.ID, events.OwnerPaged, pmHuman("arjun", "Arjun"), events.OwnerPagedPayload{
		Layer: "kafka-connect", Owners: []string{"arjun", "group:connect"}, Reason: "connector down",
	})
	app(t, st, c, r.ID, events.AgentMessage, pmSystem, events.AgentMessagePayload{
		Text: "server restarted — the agent is paused; the driver can steer, or anyone can mention @agent, to resume",
	})
	app(t, st, c, r.ID, events.AgentMessage, pmSystem, events.AgentMessagePayload{
		Text: "the server that ran this room's agent stopped; the agent resumes on another server",
	})
	// A person's message with the same words is not a restart.
	app(t, st, c, r.ID, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{CommentID: "c_1", Text: "server restarted?"})
	md := section(t, renderRoom(t, svc, r.ID), "## Timeline")
	for _, want := range []string{
		"owners of consumer paged automatically (kabir): nobody drives",
		"owners of kafka-connect paged by Arjun (arjun, group:connect): connector down",
		"server restarted; the agent was paused",
		"the agent's server stopped; the agent resumed on another server",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("timeline lacks %q:\n%s", want, md)
		}
	}
	if strings.Count(md, "server restarted;") != 1 {
		t.Errorf("one restart row expected:\n%s", md)
	}
}

// TestPostmortemExecutedButNotVerified (Q5 T1c): an executed proposal with no
// agent.verification gets a "not verified: <reason>" line, whether the room
// closed before one could be written or the bound passed without one.
func TestPostmortemExecutedButNotVerified(t *testing.T) {
	type appendFn func(events.Type, events.Actor, any)
	for _, tc := range []struct {
		name  string
		after func(a appendFn, c *clock)
		want  string
	}{
		{"closed before", func(a appendFn, c *clock) {
			a(events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "done"})
		}, "- `p_1`: not verified: the room closed 0 min 30 s after its last step ran, before any verification"},
		{"bound passed", func(a appendFn, c *clock) {
			c.Advance(20 * time.Minute)
			a(events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{CommentID: "c_1", Text: "still broken?"})
		}, "- `p_1`: not verified: no verification was written within 15 min of its last step"},
		{"verified", func(a appendFn, c *clock) {
			a(events.AgentVerification, events.AgentActor, events.AgentVerificationPayload{ProposalID: "p_1", Recovered: true, Verdict: "recovered: every layer check passed"})
		}, "- `p_1` recovered: every layer check passed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, st, c := newTestService(t)
			r := createRoom(t, svc, "arjun")
			a := func(typ events.Type, actor events.Actor, p any) { app(t, st, c, r.ID, typ, actor, p) }
			a(events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{ProposalID: "p_1", Title: "restart", Steps: []events.ProposalStep{{Action: "connect_restart", Layer: "kafka-connect"}}, RequiredLayers: []string{"kafka-connect"}})
			a(events.ActionExecuted, pmSystem, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true, Output: "restarted"})
			tc.after(a, c)
			v := section(t, renderRoom(t, svc, r.ID), "## Verification")
			if !strings.Contains(v, tc.want) {
				t.Fatalf("verification lacks %q:\n%s", tc.want, v)
			}
			if tc.name == "verified" && strings.Contains(v, "not verified") {
				t.Fatalf("a verified proposal reads as not verified:\n%s", v)
			}
		})
	}
}
