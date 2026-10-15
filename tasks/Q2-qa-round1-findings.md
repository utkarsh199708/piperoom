# Q2 — QA review round 1: confirmed findings
Source: adversarial review of 5bcad77..06a7919 (8 reviewers, each finding checked by a skeptic). R = reproduced with a failing test.

## F1. [medium, R] G1: A signal that escalated before it got a room never escalates again after that room is closed while it still fires (G1(c) silence returns)
- Where: `internal/watch/incident.go:245`
- Scenario: The cap is reached (6 watcher rooms). Pipeline b's connector_down fires at T, and openRoom blocks on max_open_watcher_rooms. At T+10m escalateLocked sets ss.escalated=true, logs the warning and the error, and still opens nothing. At T+20m another room closes, so b's room r_b opens. At T+25m the driver closes r_b while the connector is still FAILED, and ackLocked moves the signal to phaseAcked. At T+35m and later, escalateLocked skips it because ss.escalated is still true. ss.escalated is reset only when the signal resolves (sigState.step tResolved). The outage stays silent until the signal clears, which is exactly G1(c). The same happens whenever the first escalation came from the cooldown (same-key re-fire, or the 'unknown' cooldown after a restart), from a failed OpenAlertRoom for 10 minutes, or from a storm wait: re-open, close while firing, and the watcher is silent from then on.
- Suggested fix: Track the two escalations separately: one 'no room yet' flag and one 'after close' flag. At minimum, reset ss.escalated in ackLocked when the signal was never acked before in this episode, so the once-per-episode limit counts only re-opens after a close.
- Verifier: The defect is real in a narrower form than described. ss.escalated is set in escalateLocked (internal/watch/incident.go:261) whenever the no-room escalation fires, even when the escalation opens nothing (cap reached, failed open, storm wait). It is cleared only in sigState.step on tResolved (state.go:179). ackLocked (watch.go:500) does not reset it. So if the room opens later through the normal path and the driver closes it while the signal still fires, the signal moves to phaseAcked and the `ss.escalated` guard at incident.go:245 skips it for the rest of the episode. The watcher stays silent until the signal clears, which is the G1(c) symptom. The Q0 text in docs/api.md says a signal still firing EscalateAfter after its room closed re-opens a room once, so the unused re-open budget should survive.

I reproduced it with a throwaway test (deleted afterwards) at 06a7919. maxRooms=1, pipelines a and b, escalateAfter=10m. Both connectors break and the cap blocks b. The escalation at +10m logs the warning and the cap error and opens nothing. After a recovers and its room closes, b's room r2 opens. I closed r2 with b still FAILED and stepped 120 minutes. No third room opened, and the log kept saying "signal still firing" with room="".

Why it is not high:
- If the first escalation does open a room (cooldown case: same-key re-fire, then the escalation room, then the driver closes it), the doc comment states the intent explicitly: "closed again while it fires, it stays acked until it clears". Silence after that is by design, so most of the finding's variants are not bugs.
- Only the narrow case is a bug: the escalation fired but opened no room, then a normal open happened, then a close. It needs the cap or a failing open or a storm wait, then a later close while the signal still fires. The result is one missed re-open, not data loss or a security issue.
- Operators would see the earlier WARN and ERROR logs, so the outage is not completely unannounced.

Likely fix: set ss.escalated only when the escalation's open succeeds. Alternatively, clear ss.escalated whenever a room is opened for the signal, so the "once" budget is spent only by an escalation that produced a room.

Repro test (zz_repro_test.go, package watch, uses existing harness helpers). Command: go test ./internal/watch -run TestReproEscalateAfterCapThenClose -v
Result at 06a7919: FAIL with "rooms=2, want 3".
- Repro:
```go
// internal/watch/zz_repro_test.go (deleted after the run)
package watch

import (
	"testing"
	"time"
)

func TestReproEscalateAfterCapThenClose(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}}, maxRooms: 1, escalateAfter: 10 * time.Minute})
	h.step(0)
	h.breakConnector("a", "x")
	h.steps(4, time.Minute)
	h.breakConnector("b", "x")
	h.steps(4, time.Minute)
	h.steps(30, time.Minute) // escalation at cap: nothing opens, flag spent
	h.fixConnector("a")      // a recovers; only b keeps firing
	a := h.rooms.created()[0].roomID
	h.rooms.closeRoom(a, h.clk.Now())
	h.w.OnRoomClosed(a, "a", "a|watcher:a")
	h.step(time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 2 || rooms[1].alert.PipelineID != "b" {
		t.Fatalf("setup: rooms=%d", len(rooms))
	}
	b := rooms[1].roomID
	h.rooms.closeRoom(b, h.clk.Now())
	h.w.OnRoomClosed(b, "b", "b|watcher:b")
	h.steps(120, time.Minute) // b's connector still FAILED
	if got := len(h.rooms.created()); got != 3 {
		t.Fatalf("rooms=%d, want 3 (b re-opened once after close while firing)\n%s", got, h.logs)
	}
}

// Run: go test ./internal/watch -run TestReproEscalateAfterCapThenClose -v
// Result at 06a7919: FAIL "rooms=2, want 3"
```

## F2. [low, R] G1: A connector going from PAUSED (sev3) to FAILED or crash-looping (sev2) on the same connector counts as the same incident: it is held back by the cooldown or ack, and its evidence stays out of the new room
- Where: `internal/watch/state.go:127`
- Scenario: This replays QA (a). The room for the paused connector pg-orders (connector_down:pg-orders, sev3) is closed right after the resume while the signal is still clearing, so the signal is acked. 19 s later the connector-auth break makes task 0 crash-loop or go FAILED. That is a breach on the same key, so the signal stays phaseAcked: step() returns tNone for phase != OK, and reviveSev1 handles only a rise to sev1. If the room had resolved before the close, the re-fire is pending but cooldownOf finds connector_down:pg-orders cooled and stays quiet. Either way the connector_down evidence (state FAILED or CRASH_LOOP, the 'must be owner of table orders' trace) appears nowhere until EscalateAfter (10 min), or never with ESCALATE_AFTER=0. In the dev scenario a room still opens only because slot_inactive (a different key) also fires, and that room carries only the slot. For a sink connector, or with slot_inactive set to log, the whole outage is silent for 10 min. The unit test encodes exactly this: 'same signal 19s after a resolved room closed' with the trace 'auth failed' expects wantRooms 1 plus a cooldown line.
- Suggested fix: Treat a severity rise above ackSev (or above the cooled severity), or a change of failure class (PAUSED/STOPPED to FAILED/CRASH_LOOP), as a new incident: un-ack it, or skip the cooldown for that key the way reviveSev1 does for sev1.
- Verifier: The mechanism is real, but the impact is smaller than claimed. The test below reproduces it on 06a7919. The harness uses a PAUSED connector pg-orders (sev3) with a room that opens (r1). The room is closed while the signal still fires, which acks it. 19 s later the connector goes FAILED (sev2), with the trace 'must be owner of table orders'. After 100 s the connector_down phase is still phaseAcked and rooms stay at 1, so the FAILED state and trace are in no room. Cause: step() returns tNone whenever phase != phaseOK, and reviveSev1 only handles a rise to sev1 (ackSev != sev1). The key connector_down:pg-orders is the same, so ps.cooled covers it. The cooldown path with a resolved room behaves the same way, and the unit test 'same signal 19s after a resolved room closed' encodes that on purpose.

What weakens the claim:
- It is not 'silent until EscalateAfter or never' by default. escalateLocked un-acks a signal that stays acked for EscalateAfter (default 10 min), logs a warning, and opens a room that skips the cooldown. It is only never when ESCALATE_AFTER=0, which is an explicit opt-out.
- The sequence needs a person to close the room while the connector is deliberately PAUSED. Humans were looking at that outage seconds earlier.
- Treating a repeat of the same key within the cooldown as the same incident is the designed behaviour (tasks/Q1-qa-gaps.md G1: key the cooldown to the incident, escalate when a signal keeps firing). Only a sev1 rise has a special fast path, and that predates this change.
- In the dev scenario slot_inactive is a different key and still opens a room.

So the real gap is that a worsening from PAUSED/STOPPED (sev3) to FAILED/CRASH_LOOP (sev2) is not treated as a new incident. It is a bounded detection delay of about 10 min in an edge case, not a total silencing and not a data-loss or security risk.
- Repro:
```go
File internal/watch/zz_repro_test.go (removed after the run):

func TestReproPausedToFailed(t *testing.T) {
	h := newHarness(t, opts{q1Sink: true})
	h.conn.setStatus("pg-orders", clients.ConnectorStatus{Name: "pg-orders", State: "PAUSED", Tasks: []clients.TaskStatus{{ID: 0, State: "PAUSED"}}})
	h.steps(4, time.Minute)
	if len(h.rooms.created()) != 1 { t.Fatalf("setup") }
	r1 := h.rooms.created()[0].roomID
	h.closeNow(r1)
	h.step(19 * time.Second)
	h.breakConnector("orders", "must be owner of table orders")
	h.steps(5, 20*time.Second)
	if len(h.rooms.created()) != 2 { t.Fatalf("worsening to FAILED opened no room") }
}

Command: go test ./internal/watch -run TestReproPausedToFailed -count=1
Result: FAIL with phase=acked rooms=1.
```

## F3. [low, R] G1: A close the watcher hears about, including one it learns from its own failed post, acks and cools every firing signal of the pipeline, even signals that were never posted to that room
- Where: `internal/watch/watch.go:502`
- Scenario: Room r1 carries connector_down. slot_inactive fires in tick N (phaseFiring, notified=false), and emit is about to post it to r1 as a 'new signal'. The close lands in between. With a shared store this does not need tight timing: the close happened on another replica and is folded on the leader only when postWatcherNote calls syncRoom. The post returns room_closed, and post() calls OnRoomClosed(r1). Without a shared store it happens when a transient post failure leaves slot_inactive pending until the next tick. ackLocked then acks every phaseFiring signal and cools its keys under r1, including slot_inactive, which r1 never received. So the different incident opens no room ('acked: its room was closed while it still fired') until EscalateAfter, or until it clears when ESCALATE_AFTER=0.
- Suggested fix: In ackLocked and in recordPost's closed branch, ack and cool only the signals that are notified and whose keys are in told[roomID]. Leave pending signals in phaseFiring with notified=false, so the next tick opens or joins another room for them.
- Verifier: The code does what the finding says. ackLocked (internal/watch/watch.go:500) acks and cools every phaseFiring signal of the pipeline. It does not check ss.notified or whether the closed room was ever told that key. post() in output.go calls OnRoomClosed(roomID) when a post returns ErrRoomClosed, and recordPost's closed branch acks v.pending() the same way. So a close the watcher learns of at the moment it posts a new signal also acks that signal. That signal was never delivered to the closed room. docs/api.md Q0 says the close-while-firing ack applies only to the room's incident set, and that a signal outside it opens a room at once. This behaviour breaks that rule. I reproduced it with a throwaway test, now deleted, in internal/watch using the existing harness (newHarness with q1Sink). connector_down opens room r1. The room is then closed in the fake room store without calling OnRoomClosed, which simulates a close made on another replica or a missed event. slot_inactive then starts firing, and the watcher's post of it to r1 fails with room_closed. Result: slot_inactive phase=acked, connector_down phase=acked, and only one room has been created even after 11 more minutes. The test asserted that a second room opens for the new signal and failed with 'slot_inactive never got a room: rooms=1 slot phase=acked'. Command: go test ./internal/watch -run TestReproNewSignal -v. I rate it low rather than medium for three reasons. It needs a close the watcher has not yet heard about, or a post failure at the wrong moment. The existing test TestRoomClosedWhilePostingIsNotRevived deliberately accepts a close landing in flight. With the default PIPEROOM_WATCH_ESCALATE_AFTER of 10 minutes, the suppressed signal re-opens after that delay. It is suppressed for good only when ESCALATE_AFTER=0. The fix is small. Acking should skip phaseFiring signals that are not notified, or whose keys the closed room was never told. Those signals should stay firing and open a new room.
- Repro:
```go
// internal/watch/zz_repro_test.go (throwaway, deleted after the run)
func TestReproNewSignalPostFailsRoomClosed(t *testing.T) {
	h := newHarness(t, opts{q1Sink: true})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	r1 := h.rooms.created()[0].roomID
	// Close happened elsewhere; the watcher has not heard.
	h.rooms.closeRoom(r1, h.clk.Now())
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute)
	t.Logf("slot phase=%v conn phase=%v rooms=%d", h.phase("orders", owners.SignalSlotInactive), h.phase("orders", owners.SignalConnectorDown), len(h.rooms.created()))
	h.steps(5, time.Minute)
	if got := len(h.rooms.created()); got < 2 {
		t.Fatalf("slot_inactive never got a room: rooms=%d slot phase=%v", got, h.phase("orders", owners.SignalSlotInactive))
	}
}
// Command: go test ./internal/watch -run TestReproNewSignal -v
// Output: slot phase=acked conn phase=acked rooms=1 / FAIL: slot_inactive never got a room: rooms=1 slot phase=acked
```

## F4. [medium, R] G2: Room list socket never pushes a page: listDigest ignores snapshot pages, so the PagedBanner does not fire live
- Where: `internal/ws/listgateway.go:348`
- Scenario: Kabir is in the app, on the room list or in another room, with the list socket in push mode (the default). A member POSTs /api/rooms/r_1/page {layer:"consumer"}, or the server writes its auto page for an undriven watcher room. The owner.paged fold changes only snap.Pages. The hub marks the room dirty, but listConn.changed (line 248) finds l.known[id] == listDigest(snap) and drops the update, because the digest hashes only status, title, severity, driver, agent state, triage, hypothesis, proposals, suggestions, closed_at and resolution. No `room` frame is sent. useOwnerPages never sees the page, so no banner and no browser notification. Kabir only learns of it after a reload or reconnect, or when an unrelated list field changes. Auto pages and member pages in an idle undriven room usually change nothing else, so in practice they are never shown live.
- Suggested fix: Add pages to listDigest, e.g. per page {layer, at, owners}, or the newest owner.paged seq. Add `pages` to the list-relevant fields in docs/api.md. Add a ws test where an owner.paged alone produces a `room` frame.
- Verifier: Confirmed at 06a7919. listDigest (internal/ws/listgateway.go:348-385) omits snap.Pages. listConn.changed (line 248) drops any snapshot whose digest equals the known one. A room whose only change is a new owner.paged therefore produces no `room` frame on the list socket. The web useOwnerPages hook reads pages only from the room list, which useRoomList fills from pushed frames in push mode. Polling happens only in fallback mode. So the live PagedBanner and browser notification do not fire until a reconnect, a reload, or an unrelated digested change. The fix is small: add the pages to the digest. This contradicts docs/api.md:331-333 (Q0), and the socket rule at docs/api.md:631-633 does not list pages. The diff 5bcad77..06a7919 does not touch internal/ws/. Existing tests inject frames on the web side, so nothing covers this. I reproduced it with a throwaway internal test, which I removed afterwards. I rate it medium rather than high. It defeats the feature's main purpose, live notification of a paged owner, but nothing is lost: the page is persisted, shown after reload or reconnect, and visible in the room's timeline.
- Repro:
```go
File internal/ws/zz_repro_test.go (package ws; deleted after the run):

func TestReproPageNotPushed(t *testing.T) {
	now := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	svc := room.New(room.Deps{Store: memstore.New()})
	defer svc.Close()
	g := &Gateway{svc: svc, now: func() time.Time { return now }}
	c := &client{send: make(chan []byte, 16)}
	l := &listConn{g: g, c: c, known: map[string][32]byte{}, sent: map[string]time.Time{}, pending: map[string]*room.RoomSnapshot{}}
	snap := room.NewSnapshot()
	snap.ID, snap.Status, snap.Title = "r_1", "open", "a"
	s1 := snap
	l.changed(&s1)
	now = now.Add(time.Hour)
	s2 := snap
	s2.Pages = []room.Page{{Layer: "consumer", Owners: []string{"kabir"}, At: now}}
	l.changed(&s2)
	if len(c.send) != 2 {
		t.Fatalf("frames = %d, want 2 (page change not pushed)", len(c.send))
	}
}

Command: go test ./internal/ws -run TestReproPageNotPushed
Result: FAIL, "frames = 1, want 2 (page change not pushed)"
```

## F5. [medium, R] G2: Slack bridge never delivers owner.paged, though docs/api.md, the page dialog and service comments say it does
- Where: `internal/slackbridge/render.go:27`
- Scenario: Deployment with the Slack bridge on and no PIPEROOM_NOTIFY_WEBHOOK_URL (the QA setup G2 describes as 'without Slack nobody is paged'). The agent calls page_owner, a member uses 'Page an owner', or the auto page fires. owner.paged is written, but the bridge's event filter (render.go:27-31) and its paging switch (paging.go:187-233) do not include events.OwnerPaged. Nothing is posted and nobody is DM'd. The dialog still tells the member owners are reached 'by Slack or the webhook when the server has them set up', and the agent's tool says owners are notified 'outside it, where it is set up'. With no webhook, the only delivery is in-app, and that in-app path is itself broken for live users (finding 1).
- Suggested fix: Add owner.paged to the slackbridge render/paging switch: post in the room thread and DM the resolved owners through PagingOptions' allow-list. Or, until that lands, remove the Slack claims from docs/api.md, the dialog text and the service comments.
- Verifier: The finding checks out. `grep -rn OwnerPaged internal/slackbridge` finds nothing. `Important()` in render.go does not list events.OwnerPaged, and the paging switch in paging.go only handles ProposalCreated, HandoffCreated and ControlLeaseExpired. So a member page, an agent page_owner call or the auto page never posts to Slack and never DMs anyone. docs/api.md:417 says the page is "Delivered ... by the Slack bridge when it is on". The comment in service_page.go and the PageOwnerDialog text say the same. Nothing in tasks/Q1-qa-gaps.md assigns internal/slackbridge, and no recent slackbridge commit touches owner.paged. Effect: a deployment with the Slack bridge but no PIPEROOM_NOTIFY_WEBHOOK_URL gets only the in-app notice, so the page does not reach the owner, which is the thing G2 is meant to fix. The tradeoff is that this is a missing delivery channel, not a crash or data loss. The webhook channel does work, and the Slack bridge still pages for approval needed, hand-off and lease expiry. I rate it medium rather than high. I could not check the claim that the in-app path is broken (finding 1). To reproduce, I wrote a throwaway test in internal/slackbridge asserting `Important(events.OwnerPaged)`. It fails on 06a7919. I then deleted the test file and the worktree is clean.
- Repro:
```go
// internal/slackbridge/zz_repro_test.go (throwaway, removed after run)
package slackbridge

import (
	"testing"

	"piperoom/internal/events"
)

func TestOwnerPagedReachesSlack(t *testing.T) {
	if !Important(events.OwnerPaged) {
		t.Fatal("owner.paged is not an event the Slack bridge posts (docs/api.md:417 says it is)")
	}
}

// Command: go test ./internal/slackbridge -run TestOwnerPagedReachesSlack
// Result: FAIL (zz_repro_test.go:11: owner.paged is not an event the Slack bridge posts ...)
```

## F6. [medium] G2: Group-owned layers resolve to only currently-logged-in members; offline owners are never paged, the webhook carries no group, and the one-shot auto page is spent on nobody
- Where: `internal/room/service_page.go:410`
- Scenario: OIDC deployment where owners.yaml names the consumer layer as `group:data-eng`, with backups also groups (the principal style the auth design recommends). Nobody from data-eng is logged in. pageOwners adds approvers.Users() and backups.Users(), which skip group principals, plus pageCandidates. pageCandidates is users.yaml plus *live* sessions only (line 469-490), so owners = []. The page is written with owners [], and the webhook body has owners [] and no group or principal to route on, so a PagerDuty or e-mail relay cannot reach anyone. The agent is told 'no owner is on record for consumer, so nobody was paged' (tools_room_q1.go:165), which contradicts owners.yaml. For the auto page, autoPaged is set by this empty page (foldRoomFacts), so autoPageDueLocked never retries: the room's single automatic page has notified nobody. If someone from data-eng logs in later, they get no banner, because they are not in `owners`.
- Suggested fix: Put the layer's principals (including group:…) in the webhook body, and/or resolve group members from recent (not only live) sessions. Do not count an auto page with zero owners as the once-only page, or log a warning when it happens. Reword the agent's empty-owner summary when the layer lists groups.
- Verifier: The behaviour is real. I confirmed it by reading the code and running a throwaway test, which I deleted. The test was not a failing test, so I did not count it as a reproduction.

Mechanism, in internal/room/service_page.go at 06a7919:
- pageOwners takes only the user: principals from owners.yaml (approvers.Users(), backups.Users()). Group principals are never named.
- Group members are found only through pageCandidates, which is users.yaml plus LiveSessions(...) with the session's IdP groups. The code comment says this on purpose ("members with a live session").
- notify.OwnerPaged and the owner.paged payload carry only user ids (Owner{id, name, email}). There is no group or principal field, so a relay has nothing to route a group page on.

What I observed: with the repo's own owners fixture (pageOwnersYAML) and no sessions, pageOwners("orders","consumer") returned [kabir meera]. The group principals group:consumers and group:night appear nowhere in the page. TestPageOwnersResolution already asserts this ("without sessions: [kabir, meera]"). A layer owned only by groups returns [], and the test suite accepts that as the "nobody" case.

Consequences:
- foldRoomFacts sets autoPaged = true whenever p.Auto is true, whatever len(p.Owners) is. autoPageDueLocked then returns false, so the room's single automatic page is spent on nobody.
- The agent's page_owner tool tells the model "no owner is on record ... nobody was paged" (internal/agent/tools_room_q1.go:165). That contradicts owners.yaml.
- A group member who logs in later is not in owners, so they get no banner from the snapshot pages.

Why this is not high:
- The server has no IdP directory, so it cannot list group members. The same limit applies to hand-off and grant targets, and docs/design/auth.md and tenancy.md accept it.
- A manual or agent page can be retried after the 10 minute window.
- Users listed by id in owners.yaml, and users.yaml users, are paged normally.

Why it is still a real gap: group principals are the style the auth design recommends for OIDC, and the owners who are offline are the ones a page exists to reach. A cheap fix is to add the layer's group principals to the owner.paged payload and webhook body, or to not mark the auto page as spent when it reached no one.

## F7. [low, R] G9: Web reducer and approval store disagree with the server when withdraw races the last approval across replicas: a withdrawn proposal shows as approved or open again
- Where: `web/src/state/reducer.ts:182`
- Scenario: Two replicas. Approver on replica A gives the last approval: A appends approval.given, then calls ClaimProposal under its own s.mu. The driver's withdraw on replica B synced before A's append, so fullyApproved is false on B's snapshot, and B's store.WithdrawProposal lands before A's claim (the row is open and claimed_by is NULL). The log is now approval.given (completing) then proposal.withdrawn. The server snapshot drops the proposal and A's claim fails, so nothing runs. The web reducer's endIfOpen only ends proposals whose status is 'open', so this one stays 'approved' and roomStatus shows 'Executing fix · 1 of N' forever. The reverse order can also happen: A runs GetProposal (open), B withdraws the row and appends proposal.withdrawn, then A runs RecordApproval, which is an unconditional INSERT, and appends approval.given. A's approver gets 200 instead of 409 proposal_withdrawn, and the reducer's approval.given case sets status back to 'open' or 'approved', so the Approve buttons come back for a withdrawn proposal.
- Suggested fix: Make RecordApproval conditional (`INSERT ... SELECT ... WHERE EXISTS (SELECT 1 FROM proposals WHERE proposal_id=$1 AND status='open')` and map 0 rows to ErrProposalWithdrawn when the row is withdrawn). In the reducer, treat proposal.withdrawn as final for any status that has no executed step, and have approval.given leave a 'withdrawn' proposal alone.
- Verifier: The defect exists in the code, but it needs a very narrow cross-replica race and its effect is limited to what the UI shows. Nothing executes and no data is lost.

Server side: there is no cross-replica lock on a room. s.mu is per process, and the withdraw is not conditioned on approvals. store.WithdrawProposal checks only status='open', claimed_by IS NULL and no steps (internal/store/withdraw.go:38). store.RecordApproval (internal/store/store.go:706) is an unconditional INSERT with no check on proposal status. withdrawLocked trusts the local snapshot for fullyApproved, even though B calls syncRoom just before. So both log orders in the finding are reachable with two replicas. The first is approval.given completing the proposal, then proposal.withdrawn. The second is proposal.withdrawn, then approval.given.

The window is a few milliseconds. It needs the driver's withdraw and the last approval to land at the same moment on different replicas.

Server outcome is safe. ClaimProposal requires status='open', so a withdrawn proposal never runs (service_executor.go:235-241 just logs and returns). The server snapshot drops the proposal (snapshot.go:687). The only server-side flaw is that the last approver gets 200 instead of 409 proposal_withdrawn, and a stray approval row is stored.

Web reducer: the disagreement with the server is real. I wrote a throwaway Vitest test, ran it against 06a7919, and both cases failed:
- approval.given (completing) then proposal.withdrawn leaves status 'approved'. endIfOpen only ends open proposals, so the room status would show 'Executing fix' until a reload.
- proposal.withdrawn then approval.given sets status back to 'approved'. The approval.given case ignores a prior withdrawal.

A reload fixes the UI, because the snapshot drops the proposal. In the second case, a click on Approve returns 409 proposal_withdrawn. The existing test reducer.withdrawn.test.ts:63 encodes the 'approved ignores withdrawn' behaviour deliberately. That assumes the server never logs the order, which the race breaks.

Overstated parts of the finding: 'forever' is true only until reload or reconnect. The impact is a stale UI plus a misleading 200 to one approver, not an unsafe execution. Severity is low.

Fix: make proposal.withdrawn terminal in the reducer. Alternatively, add a status condition to RecordApproval or the withdraw store call.

I removed the throwaway test and the node_modules symlink.
- Repro:
```go
File web/src/state/reducer.race.test.ts (throwaway, deleted after the run). It needs node_modules symlinked from /home/user/piperoom/web, because the worktree has none.

```ts
import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { applyEvent } from './reducer'
import { emptyRoomState } from './types'
const human = { kind: 'human' as const, id: 'k', name: 'K' }
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const ev = (seq: number, type: Event['type'], payload: unknown, actor: Event['actor'] = agent): Event => ({ room_id: 'r1', seq, type, actor, payload, at: `2026-10-15T10:00:${String(seq).padStart(2, '0')}Z` })
const created = { proposal_id: 'p1', title: 't', steps: [], risk: 'r', rollback: 'u', required_layers: ['kafka-connect'] }
describe('withdraw race', () => {
  it('approval.given then withdrawn ends withdrawn', () => {
    const s = [ev(1,'proposal.created',created), ev(2,'approval.given',{proposal_id:'p1',layer:'kafka-connect'},human), ev(3,'proposal.withdrawn',{proposal_id:'p1',reason:'x'},human)].reduce(applyEvent, emptyRoomState('r1'))
    expect(s.proposals[0].status).toBe('withdrawn')   // FAILS: 'approved'
  })
  it('withdrawn then approval.given stays withdrawn', () => {
    const s = [ev(1,'proposal.created',created), ev(2,'proposal.withdrawn',{proposal_id:'p1',reason:'x'},human), ev(3,'approval.given',{proposal_id:'p1',layer:'kafka-connect'},human)].reduce(applyEvent, emptyRoomState('r1'))
    expect(s.proposals[0].status).toBe('withdrawn')   // FAILS: 'approved'
  })
})
```

Command, run from the web/ directory: `npx vitest run src/state/reducer.race.test.ts`. Result: 2 failed ('approved' received, 'withdrawn' expected).
```

## F8. [medium, R] G9: Hand-off and post-mortem input renders proposal.withdrawn without a proposal id and may drop it, so summaries list withdrawn fixes as still awaiting approval
- Where: `internal/agent/prompt.go:583`
- Scenario: In the G9 flow the agent proposes fix A (needs kafka-connect, consumer, warehouse), withdraws it, and proposes fix B. At hand-off, renderRoomEvents always keeps both proposal.created lines ('proposal p_A: ... needs: kafka-connect, consumer, warehouse'), because they are handoff-critical. proposal.withdrawn goes through the default case as 'proposal.withdrawn by agent', with no id. It is not handoff-critical, so in a long room it is dropped outside the tail window. The hand-off prompt says 'List every proposed fix that is not finished in open, by its id and title, with the layers that still have to approve it', so the summary tells the next driver that p_A still needs consumer and warehouse approval. The post-mortem input uses the same renderer, so after a close (every open proposal gets a 'proposal.withdrawn by system' line with no id) its prose can say proposals were still waiting.
- Suggested fix: Add a ProposalWithdrawn case that renders 'withdrawal <id> by <actor>: <reason>' and add the type to isHandoffCritical. Have indexLog mark the proposal as closed.
- Verifier: Confirmed at 06a7919. renderRoomEvent in internal/agent/prompt.go has no ProposalWithdrawn case, so it falls to the default at about line 583 and renders "proposal.withdrawn by <untrusted>agent</untrusted>" with no proposal id and no reason. isHandoffCritical (about line 433) covers ProposalCreated but not ProposalWithdrawn. ProposalCreated lines are always kept, so a long room drops the id-less withdrawal line outside the 30-event tail, while the "proposal p_A ... needs: ..." line stays. The hand-off prompt tells the model to list every proposed fix that is not finished, with the layers that still have to approve it, and nothing in the input says p_A was withdrawn. renderRoomEvents also feeds the post-mortem input (postmortem.go) and the judge (handoff_judge_claude.go), so the same gap reaches them. I found no other source of open-proposal state in the hand-off prompt and no test covering it. Even when the withdrawal line survives, it has no id, so with two proposals it cannot be matched to p_A. I rated it medium: the model can still read prose, and the next driver sees wrong "awaiting approval" items in the summary, but nothing runs wrongly (execution is gated server-side) and there is no security risk. A throwaway test rendered created p_A, withdrawn p_A, created p_B. The output was "proposal.withdrawn by agent" with no p_A, and the check that p_A appears twice failed. I removed the test file afterwards; the worktree is clean.
- Repro:
```go
// internal/agent/zz_repro_test.go (throwaway, deleted)
package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestReproWithdrawnHasID(t *testing.T) {
	evs := []events.Event{
		pmEvent(1, events.ProposalCreated, "agent", events.ProposalCreatedPayload{ProposalID: "p_A", Title: "Fix A", RequiredLayers: []string{"kafka-connect", "consumer", "warehouse"}}),
		pmEvent(2, events.ProposalWithdrawn, "agent", events.ProposalWithdrawnPayload{ProposalID: "p_A", Reason: "wrong"}),
		pmEvent(3, events.ProposalCreated, "agent", events.ProposalCreatedPayload{ProposalID: "p_B", Title: "Fix B", RequiredLayers: []string{"consumer"}}),
	}
	out := renderRoomEvents(evs)
	if strings.Count(out, "p_A") < 2 {
		t.Fatalf("withdrawal of p_A not identified in rendered input")
	}
}

// Command: go test ./internal/agent -run TestReproWithdrawnHasID -v
// Output: "- proposal p_A: ... needs: kafka-connect, consumer, warehouse" / "- proposal.withdrawn by agent" / "- proposal p_B: ..." then FAIL
```

## F9. [low] G5: propose_fix result tells the model only its own approvers, leaving out impacted layers the server adds
- Where: `internal/agent/tools_room.go:618`
- Scenario: The agent proposes connect_update_connector_config decimal.handling.mode=double and omits impacted_layers (the field is optional). The room service's applyImpact adds consumer and warehouse to required_layers. The tool result still says 'proposal p_X waiting for approval from: kafka-connect'. The agent then tells the room that only kafka-connect is needed and pages or nudges only that layer, while the UI waits on three layers. Prose and enforcement disagree again, this time the other way round.
- Suggested fix: Say in the summary that the server may add impacted layers, or compute the action's Impact locally (d.Actions has the same registry) and add its Layers to the list.
- Verifier: The mechanics check out in the code, but I did not write a failing test. Impact is small.

How it happens: `proposeFixTool.Run` (internal/agent/tools_room.go:~590-620) builds the approver list from the step layers plus the agent's own `impacted_layers`. `Proposals.Create` (internal/agent/contract.go:67) returns only the id. The room service's `applyImpact` (internal/room/proposal_impact.go) adds the layers from `actions.Impacting` (for example, connector config changes that reach consumer and warehouse) to `RequiredLayers` after the agent has called Create. So when the agent omits `impacted_layers`, the result says "waiting for approval from: kafka-connect" while the room requires more layers.

What the existing test covers: the only assertion is tools_room_q1_test.go:515, and it passes only because the agent named consumer and warehouse itself. Nothing covers the case where the server adds layers on its own.

Why it is low:
- It is a text-only mismatch. Enforcement is unaffected: the server still requires every layer, and the UI shows the true required layers.
- The prompt and the tool schema tell the agent to name impacted layers, so the omission is the less common path.
- The effect is that the agent may mis-state or mis-page approvers in prose. The proposal cannot run without the extra approvals, and the wrong prose is not a security or data-loss risk.

I did not reproduce it with a test. A faithful repro needs the real room adapter, because a fake `Proposals` would not apply `applyImpact`, and a test built on a fake would not show the stated failure.

## F10. [low] G5: Only the connector-config action implements Impacting; value-schema changes (schema_register, compatibility NONE) still need only schema-registry approval
- Where: `internal/actions/kafka_actions.go:542`
- Scenario: The agent proposes schema_register on orders-value (or schema_set_compatibility NONE), which the system prompt itself calls high-risk because it 'can break the warehouse'. If the model leaves out impacted_layers and risk_level, the server stores required_layers=[schema-registry] and risk_level=medium. The consumer and warehouse owners are never asked, which is the G5 symptom of approvals covering only the action's own layer.
- Suggested fix: Implement Impact for registerSchema (a -value or -key subject reaches consumer and warehouse, at least medium risk) and for setCompatibility NONE (high risk, reaches consumer and warehouse).
- Verifier: The code facts are correct. At 06a7919, internal/actions/impact.go has the only `Impacting` implementation, and it is for `updateConnectorConfig`. `registerSchema` and `setCompatibility` (kafka_actions.go) only implement Validate, CheckScope, Execute and Preview. In `createProposal`, `stepImpact` returns false for them. `applyImpact` then derives layers only from what the proposer put in `impacted_layers`. `risk_level` is normalised to medium when left out. So a bare `schema_register` or `schema_set_compatibility NONE` proposal stores required_layers=[schema-registry] and risk_level=medium. The consumer and warehouse owners are not asked.

Why it is only low:
- The reported G5 symptom is fixed. It was a `decimal.handling.mode=double` connector change that asked only kafka-connect. That path now derives consumer and warehouse and high risk on the server.
- The `Impact` doc comment's "value-schema change" is an example in a "e.g." clause, not a requirement. The task file's Q1-tools row only asks for "G5 (action)", and the spec text names decimal precision.
- The agent side covers the gap by instruction. prompt.go:39 and the `propose_fix` tool text tell the model to name `impacted_layers` for consumer and warehouse. prompt.go:37 tells it to treat schema_register and compat NONE as high-risk and to propose them only as a last resort.
- The failure needs the model to ignore those instructions. A schema-registry owner still has to approve.

It is still a real server-side enforcement gap. Schema changes are the main example of a cross-layer effect, and the safeguard depends on model compliance, which is the "nothing enforced it" problem G5 described. A small `Impact` method on `registerSchema` and `setCompatibility` (NONE giving high risk) would close it. I did not write a test. The defect is an interface that is not implemented, so a test would only assert my own expectation.

## F11. [medium, R] G10: Any non-empty close resolution is treated as proof that a person fixed the problem
- Where: `/home/user/piperoom/internal/room/postmortem.go:411`
- Scenario: A room that is not fixed gets a non-empty resolution, with no executed step and no fix note. Examples are the open-room limit's system close ("retention: open longer than 30 days"), "False alarm", "Duplicate of r_x" and "Not fixed, escalated to vendor". The hand-off written earlier lists open items such as "Restart the consumer" and the risk "lag keeps growing". I replayed the retention-close case in a scratch copy of the repo. Verification said "No agent verification ran: the fix was made outside Piperoom, per the resolution." What was done said "Piperoom closed the room ... with the resolution: retention: ...". Still open and Risks said "none recorded after the fix", and the real open items were moved under "Earlier ... (before the fix: resolution at 10:04), possibly resolved since". The draft therefore invents a fix and hides live risks. That is the G10 failure again, in the opposite direction.
- Suggested fix: Do not infer a fix from the resolution text. Count only an executed step or a fix note as fix evidence. Alternatively, skip the resolution fallback when the closer is the system actor. For a human resolution, state only that the room was closed with that resolution, and keep the hand-off's open items and risks visible, labelled as the last recorded state. Keep the wording neutral: drop "the fix was made outside Piperoom" in Verification, or require a fix note for it.
- Verifier: Confirmed at 06a7919. In internal/room/postmortem.go, hasResolution() is only `f.closed && TrimSpace(f.resolution) != ""`. Nothing in the fold checks the closer's actor kind or the wording. fixEvidence(), fixedByPerson(), writeDone() and writeVerification() all treat any non-empty resolution as "a person fixed it". I closed a room as the system actor with the real retention text ("retention: open longer than 30 days"), with no executed step and no fix note. The draft said "Verification: No agent verification ran: the fix was made outside Piperoom, per the resolution." and "What was done: ... System closed the room ... with the resolution: retention: ...". The test failed on the assertion that the draft must not claim a fix. The system close path in service_limits.go autoClose does produce that resolution. API and driver closes take free text such as "False alarm" or "Duplicate of r_x", and these fall into the same branch. The repro's hand-off was not attached (summary source: none), so I did not independently see the Still open and Risks sections move to the stale label. The code path `stale := p.have && haveEv && ev.seq > p.asOfSeq` does apply once a hand-off summary exists, so that part follows from the code. It is partly by design: tasks/Q1-qa-gaps.md G10 says to "build from the resolution", and the reported human-fixed case relied on it. But a system-actor retention close is clearly not a fix, and a fabricated fix claim that hides open items and risks is a real defect. Severity stays medium and not high: it is a draft document, and only the system/retention case is unambiguous. The general free-text ambiguity is a heuristic limitation. A fix would be to ignore resolutions from non-human closers and ideally require an explicit fixed/not-fixed signal or fix note.
- Repro:
```go
File internal/room/zz_repro_test.go, package room_test, using the helpers clock, createRoom, app, renderRoom, testOwners, fakeRun and mustUser from the existing room tests. I deleted it after running.

func TestReproRetentionClose(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	w := func(ctx context.Context, evs []events.Event, o agent.Options) (events.HandoffSummary, error) {
		return events.HandoffSummary{WhatBroke: "lag", Why: "stuck", Open: []string{"Restart the consumer"}, Risks: []string{"lag keeps growing"}}, nil
	}
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: w, Now: c.Now})
	defer svc.Close()
	r := createRoom(t, svc, "arjun")
	if _, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), r.ID, "priya", false); err != nil { t.Fatal(err) }
	app(t, st, c, r.ID, events.RoomClosed, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}, events.RoomClosedPayload{Resolution: "retention: open longer than 30 days"})
	md := renderRoom(t, svc, r.ID)
	if strings.Contains(md, "fix was made outside Piperoom") { t.Fatal("invented a fix") }
}

Command: go test ./internal/room -run TestReproRetentionClose -v
Result: FAIL, "invented a fix". The draft contains "No agent verification ran: the fix was made outside Piperoom, per the resolution."
```

## F12. [low, R] G10: isHandoffNoise drops real incident risks that mention "summary" and "truncat..."
- Where: `/home/user/piperoom/internal/room/postmortem.go:371`
- Scenario: A hand-off or closing summary has the risk "orders_summary table was truncated by the failed reload; rows missing". Truncating a summary table is common in warehouse loads. The line contains "summary" and "truncat", so it is removed from Risks (and from Still open, Done and Ruled out) and counted in the footnote as "summary line ... about the hand-off process (a failed or truncated hand-off)". I confirmed this with a scratch render: Risks showed "none recorded" and the footnote reported 1 dropped line. The reader loses a real data-loss risk and is told it was hand-off bookkeeping.
- Suggested fix: Require the line to be about the agent's or hand-off summary. For example, match the whole phrases "summary line(s)" or "summary was truncated" with no identifier characters around "summary". Alternatively, drop only the exact fixed strings the agent emits. Do not drop on a bare substring of "summary".
- Verifier: isHandoffNoise (internal/room/postmortem.go:368) drops any summary line containing the substring "summary" plus "truncat", "cut off", "clipped" or "shortened". The match is a plain substring test, so "orders_summary table was truncated by the failed reload; rows missing" is classed as hand-off bookkeeping. It is removed from Done, Ruled out, Still open and Risks, and counted in the footnote as a hand-off line. The existing tests only cover "Agent summary lines were truncated" and similar phrasing, not table names that contain "summary". A real data-loss risk disappears from the postmortem and the reader is told it was process noise. Severity stays low because the line is dropped only when the model's summary contains both words. The footnote still reports that one line was dropped, and the full log and timeline are unaffected. Fix: require "agent summary", "hand-off summary" or "the summary" as a word-bounded phrase, or exclude identifier-like tokens such as orders_summary.
- Repro:
```go
File internal/room/zz_repro_internal_test.go (deleted after running):

package room
import "testing"
func TestReproNoise(t *testing.T) {
	if isHandoffNoise("orders_summary table was truncated by the failed reload; rows missing") {
		t.Fatal("real risk dropped as hand-off noise")
	}
}

Command: go test ./internal/room -run TestReproNoise
Result: FAIL, "real risk dropped as hand-off noise"
```

## F13. [low, R] G10: A fix note logged while the post-mortem is drafting is not seen by the model summary and is not marked stale
- Where: `/home/user/piperoom/internal/room/postmortem.go:342`
- Scenario: The room closes and draftPostmortem starts a model call. This happens when the last work is newer than the newest hand-off, or there is no hand-off. The call reads the events up to the close and can take up to 3 minutes. The person who fixed it by hand logs a fix note a minute later, which is the flow "Log a fix" on a closed room is built for. The note gets a seq below the later postmortem.drafted. The model summary was written without the note and may still list "Restart the consumer" under open and "lag keeps growing" under risks. In render(), asOfSeq is set to draftSeq, which is greater than the note's seq, so `stale` is false. The draft shows those items as current next to the fix note.
- Suggested fix: Record the highest seq the draft was written from (for example `through_seq` on postmortem.drafted, or compute it from the events read in choosePostmortem) and use that as asOfSeq for the model source. If events were appended in between, treat the summary as older than them. Alternatively, re-read the log just before the drafted append and rerun the model, or drop to the events-only source, when a fix note arrived meanwhile.
- Verifier: The defect is real and I reproduced it. choosePostmortem reads the events once and the model summary covers only those. The fix-note-aware stale check uses asOfSeq = f.draftSeq (the seq of postmortem.drafted), not the last seq the model actually saw. AddCommentKind accepts a fix note on a closed room right away, with no wait for postmortem status. A note logged while the model call runs gets a seq below postmortem.drafted, so `ev.seq > p.asOfSeq` is false and the stale label is skipped. The draft then shows "Restart the consumer" under Still open and "lag keeps growing" under Risks as current, next to the fix note. The existing test for a later fix note only covers a note after the drafted event, not one during the model call. I found nothing that refutes it. Severity stays low: the window is the model call (seconds, up to 3 minutes), it needs the model path (the last work is newer than the newest hand-off, or there is no hand-off), and the result is a misleading draft that is still labelled a draft, with no data loss or security impact. A fix would be to set asOfSeq for the model source to the last event seq the writer was given. The service would carry that seq in postmortem.drafted, or re-check for newer fix notes before appending. Repro file: /home/user/piperoom/.claude/worktrees/wf_deafcfee-22f-24/internal/room/zz_repro_test.go (throwaway, uncommitted).
- Repro:
```go
File internal/room/zz_repro_test.go (package room_test), run with: go test ./internal/room -run TestReproFixNoteDuringDraft -count=1

func TestReproFixNoteDuringDraft(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	svc, st, _ := newPMService(t, func(ctx context.Context, evs []events.Event, _ agent.Options) (agent.PostmortemDraft, error) {
		close(entered)
		<-release
		return agent.PostmortemDraft{Summary: events.HandoffSummary{WhatBroke: "w", Why: "stuck", Open: []string{"Restart the consumer"}, Risks: []string{"lag keeps growing"}}}, nil
	})
	r := createRoom(t, svc, "arjun")
	closeAs(t, svc, r.ID)
	<-entered (with 3s timeout)
	svc.AddCommentKind(ctx, mustUser(t, svc, "arjun"), r.ID, "", "restarted the consumer by hand", events.CommentKindFixNote)
	close(release)
	p := drafted(t, waitPM(t, st, r.ID))   // Source == "model"
	md := renderRoom(t, svc, r.ID)
	if !strings.Contains(md, "Earlier, from the closing summary") { t.Fatalf("not marked stale:\n%s", md) }
}

Result: FAIL. The rendered draft lists "- Restart the consumer" under Still open and "- lag keeps growing" under Risks as current, below the fix note "restarted the consumer by hand".
```

## F14. [medium, R] G3: connect_worker_log drops Debezium's own lines: the filter looks for the Connect connector name, but Debezium's log context holds topic.prefix
- Where: `internal/clients/connect/worker_log.go:403`
- Scenario: Dev stack: connector `pg-orders-connector` with `topic.prefix: pg` (dev/connectors/pg-orders-connector.json, dev/owners.yaml). The connector-auth crash loop is logged by Debezium (image layout `%X{dbz.connectorType}|%X{dbz.connectorName}|%X{dbz.connectorContext}`), for example `ERROR  Postgres|pg|streaming  Producer failure ...` followed by `Caused by: ... must be owner of table orders`. In Debezium the MDC connector name is the logical name (topic.prefix), not the Connect connector name. These entries contain neither `pg-orders-connector` nor `pg-orders-connector-0`. The model calls connect_worker_log with the default connector and gets `No WARN or higher lines name pg-orders-connector ...`. This is the same blind spot G3 was meant to remove, and the summary then hints the log may be the wrong one. The unit and wiring tests pass only because they fabricate the MDC as `Postgres|pg-orders-connector|streaming`. The wiring test even sets `TopicPrefix: "pg"` on the pipeline and never uses it.
- Suggested fix: Also match the connector's logical name. The tool can read `topic.prefix` (and `slot.name`) from Connect.Config, or from owners `TopicPrefix`, and pass them in as extra match names (a new field on WorkerLogQuery, or a second query). The match must be an exact token in the `Type|name|context` MDC. Change the tests to use a prefix that differs from the connector name, as the real stack does.
- Verifier: The filter drops Debezium's own log entries, and the tests that cover it fabricate the log context. parseWorkerLog (internal/clients/connect/worker_log.go:403) keeps an entry only if mentionsConnector(text, q.Connector) matches. The query's Connector is the Connect connector name (default pg-orders-connector), and no alias is passed anywhere. A grep for TopicPrefix, Alias and Aliases found nothing in the connect client, wiring or the tool. Debezium's log context (dbz.connectorName) holds the logical name, which is topic.prefix (dev/connectors/pg-orders-connector.json sets "pg"). From my knowledge of Debezium and its image layout, not from any log in this repo, its entries read `Postgres|pg|streaming` and never contain pg-orders-connector. The wiring test (internal/wiring/connectlog_test.go:74) writes `Postgres|pg-orders-connector|streaming` even though it sets TopicPrefix "pg". The connect unit tests also fabricate the connector name in the log context. I found no real Debezium log sample anywhere in the repo to contradict this.

Reproduction: I wrote a throwaway test in internal/clients/connect with the line `<ts> ERROR  Postgres|pg|streaming  Producer failure ...` followed by `Caused by: ... must be owner of table orders`, queried with Connector "pg-orders-connector". It failed with `got [] err <nil>`. I have deleted the test file.

Why medium, not high: Connect's own task log lines still match, for example `WorkerSourceTask{id=pg-orders-connector-0}`. Connect warns on a retriable poll failure ("failed to poll records from SourceTask. Will retry operation") and attaches the exception with its Caused-by chain. If that line appears in the worker log, the root cause could still reach the tool. I could not confirm from this repo that the line is emitted or that it is not filtered out. So the blind spot is real for Debezium's own lines and the covering tests are fake, but whether the crash-loop evidence is lost end to end is unproven. The fix is to also match topic.prefix (the Debezium logical name) as an alias, scoped to the pipeline, in the filter.

## F15. [low] G3: A missing or dropped connector role makes pg_publication_tables fail with a raw SQL error, so the new `role does not exist` finding and RoleExists=false are unreachable
- Where: `internal/clients/postgres/publication.go:126`
- Scenario: The connector's `database.user` is a role that was dropped or renamed (a connector pointed at a dropped role is the case PublicationInfo documents as a finding). The tool first calls PublicationSourceTables/PublicationTables, whose SQL runs `has_table_privilege($2::name, c.oid, 'SELECT')` and `pg_has_role($2::name, c.relowner, 'MEMBER')`. Postgres raises `ERROR: role "debezium" does not exist` for these functions when the role is missing. The tool returns `dbtools: pg_publication_tables: postgres: check publication ... source privileges: ERROR: role ... does not exist` as a tool error. The `PROBLEM connector user X does not exist as a role` finding (pg_publication.go:296) and PublicationInfo's careful `LEFT JOIN pg_roles` / RoleExists=false path never run, because readRoleFacts only runs after the table read succeeded. The unit test for this finding uses a fake that returns tables without error, and the integration test calls PublicationInfo directly, so neither covers it.
- Suggested fix: Guard the role-dependent expressions with `EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $2)` (return false when it is absent). Or have the tool call PublicationInfo first and skip the privilege columns when RoleExists is false.
- Verifier: The defect is real, but I could not run it against Postgres. The sandbox refused the `su` command I used to start a local server. I read the code at 06a7919 and relied on documented Postgres behaviour.

- `publicationMembers` (publication.go:126-128) and `sourcePrivileges` (publication.go:183-185) pass the connector role as `$2::name` to `has_table_privilege` and `pg_has_role`. Postgres raises `ERROR: role "x" does not exist` (SQLSTATE 42704) when a role given by name is missing and the query reaches a row. In `sourcePrivileges` the `c.oid IS NOT NULL AND ...` guard does not help whenever a configured source table exists.
- Nothing catches that error. It comes back wrapped from `PublicationTables` or `PublicationSourceTables`. `pg_publication.go:142-145` returns it as a tool error before `readRoleFacts` runs at line 147.
- `PublicationInfo` (publication.go:237-246) uses `LEFT JOIN pg_roles` so a dropped role gives `RoleExists=false`. That path, and the `PROBLEM connector user X does not exist as a role` finding at pg_pg_publication.go:296 (`!i.RoleExists`), are therefore unreachable through the tool in its normal case: a missing role with existing source tables.
- Existing tests miss this. The unit test in `pg_publication_owner_test.go:179` uses a fake that returns tables without error. The integration test (`publication_integration_test.go:173`) calls `PublicationInfo` directly. Nothing in the code handles 42704 or "does not exist".

The impact is a lost diagnostic, not a security or data-loss risk. The operator gets a tool error that already says `role "debezium" does not exist`, so the cause is still visible, but not as the structured finding the feature promised. Severity is low rather than medium.

A fix would call `PublicationInfo` first, or resolve the role through `pg_roles` and skip the privilege functions when it is missing.
- Repro:
```go
Not reproduced. A faithful repro needs a live Postgres, run as: go test -tags integration ./internal/clients/postgres -run X. The test would create a publication and a table, then call db.PublicationSourceTables(ctx, pub, "no_such_role", []string{"public.t"}). It should return a 42704 error instead of a result with RoleMemberOfOwner=false.
```

## F16. [low] G3: Ownership test uses pg_has_role(..., 'MEMBER'), which is true for roles that cannot actually own-check (NOINHERIT / WITH INHERIT FALSE membership)
- Where: `internal/clients/postgres/publication.go:128`
- Scenario: `debezium` is a member of `orders_owner` through `GRANT orders_owner TO debezium WITH INHERIT FALSE`, or `debezium` is a NOINHERIT role. In `publication.autocreate.mode=filtered`, `ALTER PUBLICATION ... SET TABLE public.orders` fails with `must be owner of table orders`, because Postgres's ownership check uses has_privs_of_role, which is the inherited-privileges (USAGE) semantics. The tool reports RoleMemberOfOwner=true and publicationFindings (pg_publication.go:280, 302) skips both the PROBLEM and the NOTE. The agent then sees no explanation for the crash loop that the new tool was added to explain.
- Suggested fix: Use 'USAGE' (the has_privs_of_role semantics that ownership checks use), or report both flags. Rename the field/comment accordingly.
- Verifier: The code does what the finding says. The three queries in internal/clients/postgres/publication.go (lines 128, 185 and 242) use pg_has_role(..., 'MEMBER') for the ownership test. The comments there say membership gives ownership. In Postgres, ownership checks (object_ownercheck, used by ALTER PUBLICATION ... SET/ADD TABLE) call has_privs_of_role. That corresponds to pg_has_role(..., 'USAGE'), which respects NOINHERIT and GRANT ... WITH INHERIT FALSE. MEMBER ignores inheritance. So for a role that is a member of the owner without inheriting its privileges, the tool reports RoleMemberOfOwner=true. dbtools/pg_publication.go then skips both findings: the PROBLEM at line 280 for filtered mode and the NOTE for other modes, and the publication-owner PROBLEM at line 302. The tool then misses the exact "must be owner of table" crash loop it was written to explain. I found nothing elsewhere that corrects this. The tests only use direct ownership or a plain unrelated role, so none covers a non-inheriting membership. The impact is narrow and non-default. In PG16+ a plain GRANT inherits by default, and a role is NOINHERIT only if someone sets it explicitly. The result is a missed diagnostic, a false negative with no security or data-loss risk. It is a correctness slip with a one-word fix (change 'MEMBER' to 'USAGE'), so I rate it low severity. I did not reproduce it. Sandbox command restrictions blocked me from starting the local PG16 instance, and there is no Docker. I did not write a Go test. I am relying on the documented pg_has_role semantics and on Postgres's has_privs_of_role ownership check.

## F17. [low, R] G3: connect_worker_log returns whole entries that name the room's connector even when they also name other pipelines' connectors
- Where: `internal/clients/connect/worker_log.go:403`
- Scenario: The worker is shared by several pipelines. Connect logs entries such as `Joined group ... got assignment: Assignment{... connectorIds=[pg-orders-connector, pg-payments-connector], taskIds=[pg-orders-connector-0, pg-payments-connector-0] ...}` (INFO; the model may ask min_level=INFO) and rebalance/WARN lines listing every connector. For the orders room these entries match, so the whole text, including the payments pipeline's connector and task names, goes into the summary, Data and the room log. The tool's scope check only covers the *argument*, and the rest of the codebase deliberately hides out-of-scope names (`tools.NotShown`).
- Suggested fix: For entries that name other known connectors (owners.yaml or Connect list), either drop them or replace the other names with a count. At minimum skip entries whose text names a connector outside the pipeline's `Connectors` list.
- Verifier: At 06a7919, parseWorkerLog (internal/clients/connect/worker_log.go ~line 400) keeps a whole folded entry whenever mentionsConnector finds the room's connector name in it. mentionsConnector is a plain boundary-aware substring test. Nothing removes or counts other connector names. The tool (internal/tools/kafkatools/worker_log_tool.go) checks scope only on the connector argument. It then masks secrets and puts the text in the summary, Data and the room log, with no NotShown-style handling. I could not refute this from the callers or tests. The only multi-connector test covers separate single-name lines. docs/design/msk.md already treats other pipelines' log lines on a shared worker as breaking the scoping promise.

I reproduced it with a throwaway test, since removed. A real Kafka Connect INFO "Joined group ... got assignment: Assignment{connectorIds=[pg-orders-connector, pg-payments-connector], taskIds=[...]}" entry, queried for pg-orders-connector with min_level INFO, comes back whole, including the pg-payments names.

Severity is low. The leak is only other connectors' and tasks' names, not data or secrets. The model must request min_level=INFO for assignment lines. Multi-name WARN/ERROR rebalance lines are the other route. Operators on a shared worker would see it, but the impact is cosmetic or tenancy-hygiene.
- Repro:
```go
File internal/clients/connect/zz_repro_test.go (package connect), removed after the run:

func TestReproSharedEntry(t *testing.T) {
	log := "[2026-10-15 04:10:09,123] INFO [Worker clientId=connect-1, groupId=connect-cluster] Joined group at generation 5 with protocol version 2 and got assignment: Assignment{error=0, leader='connect-1', leaderUrl='http://x:8083/', offset=9, connectorIds=[pg-orders-connector, pg-payments-connector], taskIds=[pg-orders-connector-0, pg-payments-connector-0], revokedConnectorIds=[], revokedTaskIds=[], delay=0} (org.apache.kafka.connect.runtime.distributed.DistributedHerder:1)\n"
	p := writeLog(t, log)
	got, err := NewLogReader(p).WorkerLog(t.Context(), clients.WorkerLogQuery{Connector: "pg-orders-connector", MinLevel: "INFO"})
	if err != nil { t.Fatal(err) }
	if len(got) == 0 { t.Fatal("no entry matched") }
	for _, l := range got {
		if strings.Contains(l.Message, "pg-payments") { t.Fatalf("other pipeline's connector leaked: %s", l.Message) }
	}
}

Command: go test ./internal/clients/connect -run TestReproSharedEntry
Result: FAIL, "other pipeline's connector leaked: [Worker clientId=connect-1, ...] ... connectorIds=[pg-orders-connector, pg-payments-connector], taskIds=[pg-orders-connector-0, pg-payments-connector-0] ..."
```

## F18. [medium, R] G6: Hand-off and post-mortem model input loses the verification verdict now that the room accepts agent.verification
- Where: `internal/agent/prompt.go:583`
- Scenario: Before Q1 the room's agentEventTypes refused agent.verification, so announceVerdict's fallback always wrote the verdict as an agent.message, and renderRoomEvent showed it as 'agent: <verdict text>'. Q1 added events.AgentVerification to agentEventTypes (internal/room/service.go:775), so the verdict now goes into the log only as an agent.verification event. renderRoomEvent has no case for that type. It falls to the default branch and renders the line 'agent.verification by agent', with no proposal id, no recovered flag and no verdict text. isHandoffCritical also omits the type, so in a long room the line can be dropped from the tail window. Replay: connector config change, RESTARTING 20 s, RUNNING, verdict 'recovered'. At shift change, or on a manual close, the hand-off writer, the post-mortem model pass (agent/postmortem.go, which renders the log through renderRoomEvents) and the hand-off judge (handoff_judge_claude.go) see no verification outcome. The prompt rule says 'Never write that no fix was ... verified ... unless an event shows a verification'. The summary can therefore say 'not verified' or 'unknown' for a fix that was verified recovered. It can also claim recovery for a fix whose verdict was 'NOT recovered after 1m30s', because the NOT recovered text is invisible to it. The server-side markdown post-mortem (room/postmortem.go) handles the event correctly, so the two outputs disagree.
- Suggested fix: Add a case events.AgentVerification to renderRoomEvent. Decode AgentVerificationPayload and render 'verification <proposal_id> (recovered|NOT recovered): <truncatePromptField verdict>' inside wrapUntrusted. Skip the line when Repeat is true, or mark it 'repeat'. Add events.AgentVerification to isHandoffCritical so the verdict is never trimmed from the window. Add a table test.
- Verifier: Confirmed at 06a7919. At 5bcad77 the room's agentEventTypes did not include events.AgentVerification, so the agent's announceVerdict fallback wrote the verdict as an agent.message. renderRoomEvent renders that as "agent: <text>", so the hand-off model saw it. The diff added AgentVerification to agentEventTypes (internal/room/service.go:775), so the typed event now goes through and the agent.message is no longer written. The follow-up message is only a compat warning. renderRoomEvent in internal/agent/prompt.go has no case for AgentVerification and falls to the default branch, which prints only the type and actor name. isHandoffCritical also omits the type, so a long room can drop the line from the tail window. renderRoomEvents feeds the hand-off writer, the post-mortem model pass and the hand-off judge, so none of them see the proposal id, the recovered flag or the verdict text. The agent.status "done: pipeline recovered" line only partly covers the recovered case. The NOT recovered verdict is entirely invisible, so a hand-off summary can claim recovery or say "not verified" wrongly. It is a model-input regression in a normal flow, not a security or data-loss risk, and the server-side markdown post-mortem handles the event correctly, so the two outputs can disagree. Medium severity stands. Existing tests do not cover this type. Repro: a throwaway test fails with the rendered line "agent.verification by <untrusted>unknown</untrusted>", and I deleted the test afterwards, so the worktree is clean.
- Repro:
```go
// internal/agent/zz_repro_test.go (throwaway, deleted after the run)
package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestReproVerificationRendered(t *testing.T) {
	line := renderOne(t, events.AgentVerification, events.AgentVerificationPayload{
		ProposalID: "p_1", Recovered: false, Verdict: "NOT recovered after 1m30s",
		Checks: []events.VerificationCheck{},
	})
	if !strings.Contains(line, "NOT recovered") || !strings.Contains(line, "p_1") {
		t.Fatalf("verdict lost from hand-off rendering: %q", line)
	}
	if !isHandoffCritical(events.AgentVerification) {
		t.Errorf("agent.verification not hand-off critical")
	}
}
// Command: go test ./internal/agent -run TestReproVerificationRendered
// Output: FAIL: verdict lost from hand-off rendering: "agent.verification by <untrusted>unknown</untrusted>"
```

## F19. [medium, R] G11: Step links are plain #hash anchors under HashRouter, so clicking one leaves the room
- Where: `web/src/components/room/AgentText.tsx:32`
- Scenario: The agent writes 'Checked s_01M4Y9X0ZPZ0NPJW00JGWVY8VP'. The UI renders <a href="#step-s_01M4...">step 3</a>. App.tsx wraps everything in HashRouter, so the URL hash is the router location (#/rooms/r_x). Clicking the link sets the hash to '#step-s_01M4...'. HashRouter reads that as path '/step-s_01M4...', which matches the catch-all Route path='*' and renders RoomList. The user is dropped out of the incident room and the step is not scrolled to. The G11 link therefore does not work in the real app. The same pattern already exists in TriageCard.tsx:61, but the new feature copies it into every piece of agent prose: timeline messages, hypothesis, hand-off dialog.
- Suggested fix: Render a button or <a> with onClick that calls preventDefault and then document.getElementById(stepAnchor(id))?.scrollIntoView (and focuses it). Do the same in TriageCard. Add a test that clicking keeps location.hash at #/rooms/... .
- Verifier: At commit 06a7919, AgentText.tsx renders a plain `<a href="#step-<id>">` with no onClick. App.tsx uses HashRouter with a catch-all `*` route that renders RoomList. I found no click interception or scrollIntoView for step anchors; the only scrollIntoView calls are in ProposalsSummary and ProposalsPanel. Clicking the link changes the hash from `#/rooms/r_x` to `#step-s_...`, and the router location becomes `/step-s_...`. That path matches `*`, so the user leaves the room and the step is not scrolled to. A browser fires popstate on fragment navigation, which is what HashRouter listens to. I reproduced this with a throwaway test (not committed, deleted afterwards). The same pattern already exists in TriageCard.tsx:61, and AgentText.test.tsx only checks the href attribute, so no test caught it. I rate it medium rather than high. It is a functional bug that breaks the new G11 feature everywhere agent prose is shown, and users can recover with the back button. It involves no data loss or security risk.
- Repro:
```go
File web/src/components/room/zz.test.tsx (deleted after the run):

```tsx
import { describe, expect, it } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { HashRouter, Route, Routes, useLocation } from 'react-router-dom'
import type { StepState } from '@/state/types'
import { AgentText, StepNumbersProvider } from './AgentText'
const A = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const steps: StepState[] = [{ id: A, title: 't', tool_calls: [], messages: [] }]
function Loc() { return <div data-testid="loc">{useLocation().pathname}</div> }
describe('x', () => {
  it('keeps route', async () => {
    window.location.hash = '#/rooms/r_x'
    render(
      <HashRouter>
        <Loc />
        <Routes><Route path="/rooms/:id" element={<StepNumbersProvider steps={steps}><AgentText text={`Checked ${A}`} /></StepNumbersProvider>} /><Route path="*" element={<div>LIST</div>} /></Routes>
      </HashRouter>)
    fireEvent.click(screen.getByRole('link'))
    await new Promise((r) => setTimeout(r, 100))
    expect(screen.getByTestId('loc').textContent).toBe('/rooms/r_x')
  })
})
```

Command, run in web/: `npx vitest run src/components/room/zz.test.tsx`

Result: FAIL. Expected '/rooms/r_x', received '/step-s_01M4Y9X0ZPZ0NPJW00JGWVY8VP'. window.location.hash became '#step-s_01M4Y9X0ZPZ0NPJW00JGWVY8VP'.
```

## F20. [low] G7: Web 'Seen before' filter hides hits the server judged to be incidents, and shows a '(no resolution text)' placeholder as a resolution
- Where: `web/src/lib/f8.ts:112`
- Scenario: The Go filter isIncident keeps a room with a resolution (any closer, even unverified) plus a hypothesis or label. But loadRoom keeps pr.Resolution only when e.Actor.Verified, and Verified means 'in users.yaml' (service.go humanActor). In an OIDC deployment without users.yaml, every closer is unverified, so every hit has resolution '' and the web isUsableHit (requires resolution.trim() !== '') drops every real past incident that has a fix. The Seen before card never appears. In the opposite case, a room with only a layer label, an unverified closer and no fix gets hitOf's placeholder Resolution='(no resolution text)'. That is non-blank, so the card passes the filter and shows the placeholder line as if it were the team's resolution. Hypothesis-only rooms that the server accepts are also hidden by the web filter (the code comment admits this).
- Suggested fix: Have the server emit an explicit boolean (e.g. has_resolution, or leave unusable rooms out of the hits). Have the web filter use that flag, not the displayed text. Drop the placeholder in hitOf, or have the web treat it as empty.
- Verifier: I read the code and the mechanics are as described, but the impact is narrower than the "medium" label suggests. I did not write a failing test.

Confirmed in the code:
- `internal/tools/similarincidents/room.go` sets `HasResolution` for any closer, so `isIncident` accepts the room.
- `Resolution` and `ClosedBy` are set only when `e.Actor.Verified`. `Verified` is true only for users found in `users.yaml` (`service.go` `humanActor`).
- `hitOf` in `tool.go:439` sets `Resolution = "(no resolution text)"` when the resolution, fix and fix actions are all empty.
- `web/src/lib/f8.ts` `isUsableHit` first requires `resolution.trim() !== ''`, then a fix or a layer label.
- `SimilarCard.tsx:32` renders `hit.resolution` as-is.

Consequences:
1. A closer who is not in `users.yaml` and a room with a layer label but no fix: the server returns the placeholder, the web filter lets it through, and the card shows "(no resolution text)" as the team's resolution. This is a real cosmetic or misleading-text bug. It needs an unverified closer (`docs/api.md` says login also works for principals named in `owners.yaml`) and a label-only room.
2. An unverified closer on a room with a `fix_that_worked` or a label: `resolution` is '' and the web drops the hit. Hiding unverified resolutions is intentional (the "Gap 10" comment in `room.go`: a resolution from someone the server cannot vouch for must not steer future rooms). The web filter makes the hit disappear entirely instead of showing it without the resolution text, but that follows from the design, not from an accident.
3. Hypothesis-only rooms the server accepts are hidden by the web filter. The `f8.ts` comment says this on purpose.

The claim that an OIDC deployment without `users.yaml` never shows "Seen before" depends on every closer being outside `users.yaml`. The default config loads `dev/users.yaml`. No data loss or security risk is involved, because the Gap 10 design means unverified text is withheld. The placeholder leaking into the card is the one clear defect. It is minor and rare, so I rate it low.

## F21. [medium, R] G1: New-incident wake is lost when another replica holds the room's agent lease
- Where: `internal/room/service_watch.go:237`
- Scenario: Multi-replica deploy (Postgres store, so s.coord != nil). An Alertmanager alert lands on replica B through the load balancer. B opens the room, starts the agent and holds the room lease, which it renews for as long as it is alive. Later its run parks after a fix, or ends by itself. The watcher only runs on the leader replica A. A sees a different incident, roomToJoin picks B's room, and A calls PostIncidentJSON. On A, newIncidentLocked calls ensureAgentLocked, then startAgentLocked, then claimLeaseLocked, which returns false because B holds the lease. A logs 'no agent run here to tell' and returns without pushing anything. B folds the system agent.message from the log, but feedInbox and trackPending both go through inputFor, and inputFor has no case for a system agent.message. So nothing with NewIncident ever reaches B's inbox or st.pending. B's agent stays parked (or stays ended: agentDone is only checked on the replica that posts) while a second outage sits in the room as one system line. This is the G1(b) failure again.
- Suggested fix: Make the new incident readable from the log. One way: write the incident note as an event or payload that inputFor maps to HumanInput{Kind: "alert_update", NewIncident: true}, with the alert evidence as text. Then the lease owner's feedInbox and pending pick it up and wake the run. The owner replica must also restart a run that ended on its own (the agentDone check) when it folds that event, not only the replica that posted it.
- Verifier: Confirmed by reading and by a failing test. The watcher runs on the leader replica only (cmd/server startElectedWatcher). When the room's agent lease is held by another replica, newIncidentLocked (internal/room/service_watch.go) calls ensureAgentLocked and startAgentLocked, and claimLeaseLocked returns false. It then logs "no agent run here to tell" and returns without pushing NewIncident input anywhere. The only thing written is the system agent.message in the log. The lease owner folds that event through foldLocked, which calls trackPending and feedInbox. Both use inputFor (service.go ~1094), which has no case for AgentMessage, so it returns nil. The owner's inbox and st.pending never see the new incident. Nothing else carries it across replicas. The comment in service_triage.go ("the lease owner reads it from the log") assumes a path that does not exist, and renewLeases keeps renewing the lease while B is alive, so A cannot take it over. Result: on a multi-replica deploy, a second incident that the watcher joins into a room whose agent is parked or finished on another replica is only a system line, and the agent is not woken. This is the G1(b) symptom again. It is medium rather than high because it needs several replicas, the leader being a different replica from the lease owner, and an idle or ended agent. Humans still see the note and can steer.
- Repro:
```go
Test file saved at /tmp/claude-0/-home-user-piperoom/53bb11e1-975e-5bd6-9e2a-762a0f7a558e/scratchpad/zz_g1_repro_test.go. It is an in-package test in internal/room, and I removed it from the worktree afterwards. It builds a leaseCoord wrapping fakeCoord from lease_expiry_test.go, with owner-aware AcquireRoomLease, on one memstore shared by two Services. Replica B creates the room, takes the lease and runs a blocking agent. Replica A calls PostIncidentJSON(ctx, roomID, "watcher: orders: consumer lag (new signal)", nil). B calls syncRoom to fold A's event. The test then expects B's agent Inbox.Wait to return within 500ms.

Command: go test ./internal/room -run TestG1NewIncident -count=1

Output:
INFO room: new incident joined; no agent run here to tell
--- FAIL: TestG1NewIncidentLostWhenOtherReplicaHoldsLease
    zz_g1_repro_test.go:87: B's parked agent never got the new incident: context deadline exceeded
```

## F22. [low, R] G2: Auto-page never fires for a watcher room opened on slot_stall when the first look gives no layer
- Where: `internal/room/service_page.go:195`
- Scenario: The watcher opens a room for the new slot_stall signal (QA G18: a quiet table stalls the slot). No other signal fires, and nobody takes the driver role. The room's table signalLayers has no slot_stall entry, so firstWatcherSignal skips it and alertSignal is "". autoPageDueLocked then falls back to st.snap.Triage.SuspectedLayer. If the triage pass is off (PIPEROOM_TRIAGE_PASS=off), failed, or returned no known layer, the room is skipped forever: one debug line, then nothing. Nobody is paged for an undriven watcher room, and that is the G2 failure. The watcher already puts "layer":"postgres" into every signal's evidence (compose, using SignalLayer, which knows slot_stall) so the room service would not need its own copy, but firstWatcherSignal ignores that field.
- Suggested fix: Read the per-signal "layer" field that the watcher evidence now carries (check it is one of events.Layers) in firstWatcherSignal, and fall back to signalLayers. Or add "slot_stall": "postgres" to signalLayers.
- Verifier: The defect is real at 06a7919. signalLayers in internal/room/service_page.go has no slot_stall entry, so firstWatcherSignal skips a slot_stall signal and returns "". The watcher's slot_stall signal exists in internal/watch/stall.go, and watch.SignalLayer maps it to postgres. autoPageDueLocked then relies on Triage.SuspectedLayer, and with no layer from there it logs one debug line and never pages. The comment on watch.SignalLayer says the room service can page "without its own copy of this table", but the room service keeps its own copy, and that copy is missing the new signal. I found no other handling of slot_stall in internal/room.

Severity is low for three reasons. PIPEROOM_TRIAGE_PASS defaults to on, so in the default config the triage first look will often supply a layer and the page will still fire. It is a missed auto-page for an undriven room only when triage is off, failed or returned no known layer. Nothing is lost or exposed, and a person can still page by hand.

Repro: a throwaway test in package room, since removed, feeds firstWatcherSignal a watcher alert body with a single slot_stall signal. It fails on the current code. It does not go through autoPageDueLocked.
- Repro:
```go
// internal/room/zz_repro_test.go (throwaway, deleted after the run)
package room

import (
	"encoding/json"
	"testing"
)

func TestReproSlotStall(t *testing.T) {
	raw := json.RawMessage(`{"source":"watcher","signals":[{"signal":"slot_stall","evidence":{"layer":"postgres"}}]}`)
	sig, _ := firstWatcherSignal(raw)
	if sig != "slot_stall" {
		t.Fatalf("firstWatcherSignal = %q, want slot_stall", sig)
	}
}
// Run: go test ./internal/room -run TestReproSlotStall
// Result: FAIL: firstWatcherSignal = "", want slot_stall
```
