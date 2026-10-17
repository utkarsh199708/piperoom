# Q4 — QA review round 2 (c4ee725..08ba8ca): confirmed findings
R = reproduced with a failing test.

## R1. [medium, R] G1: A new target of an acked signal (a second connector, consumer group, DLQ topic) is a different incident but stays silent. After an escalation it stays silent until every target clears.
- Where: `internal/watch/state.go:155`
- Scenario: Pipeline orders has a source connector pg-orders and a sink connector wh-sink. pg-orders goes FAILED (sev2) and room r1 opens. The driver closes r1 while pg-orders still fails, so connector_down is acked with ackSev sev2. One minute later wh-sink goes FAILED with an unrelated cause. Its key connector_down:wh-sink was never in r1's incident. Even so, no room opens, nothing is posted and nothing is logged until ESCALATE_AFTER (10 min) after the close. It is never reported if ESCALATE_AFTER=0. Worse case: the signal had already escalated once and the driver closed the escalation room too (pg-orders is a known, parked problem). Then ss.escalated is true, so the signal stays acked until connector_down clears. connector_down is one sigState for all connectors and reads bad while any connector is bad, so wh-sink's outage stays silent for as long as pg-orders stays broken. The same holds for consumer_lag and group_unstable on a second group, and for dlq_growth on a second DLQ topic: they have no severity-rise fast path except sev1. docs/api.md (Q0) says the close-while-firing ack applies only to the room's incident set and that a signal outside it opens a room at once. This is the G1(a) failure shape again: a different incident right after a close is ignored.
- Suggested fix: In step(), or in a pass before viewOf, compare the keys of an acked signal with its cooled keys (ps.cooled, or the keys recorded at ack time). When a key appears that the closed incident did not carry, set phaseFiring, notified=false and bypassCooldown, the way reviveSev1 does, so the new key is posted at once. Alternatively, keep acks per key and not per signal.
- Verifier: Confirmed by code and by a failing test. In sigState.step (state.go), a breach reading merges new targets via addTargets and then returns early when phase != phaseOK, so a new target on an acked signal never changes the phase. viewOf (output.go) treats phaseAcked as hasAny only, so the acked signal is never active or pending, whatever its keys. reviveSev1 un-acks only on a severity rise, and escalateLocked skips signals with escalated=true and measures from ackedAt. The ack path is signal-scoped while the cooldown path is key-scoped, which contradicts the Q0 text in docs/api.md ("a signal outside it opens a room at once"). Reproduction: pipeline orders with connectors [pg-orders, wh-sink]. pg-orders fails, room r1 opens and the driver closes it, so connector_down is acked. wh-sink then fails with an unrelated cause. Escalation is 10 min. Case A (no escalation yet): after 8 min no room opens; the signal stays acked with targets [pg-orders wh-sink], so it is silent until ESCALATE_AFTER from the close, and forever when that is 0. Case B (escalation room also closed): after 12 min the escalation room opens and is closed. wh-sink then fails, and 8 min later no room has opened. The signal is acked, escalated is true, so it stays silent until connector_down clears. Severity is overstated slightly: it needs a second target to fail while the first stays broken after a driver close, and it degrades to a delayed or missed notification with no data loss. It is still a real operator-visible monitoring gap and the same failure shape as G1(a). Because case A also depends on the ESCALATE_AFTER setting, medium seems right.
- Repro:
```
File internal/watch/zz_repro_test.go (deleted after the run). It uses newHarness(opts{escalateAfter: 10*time.Minute, q1Sink: true}), then rebuilds the Watcher with an owners.yaml that lists connectors [pg-orders, wh-sink]. Steps:
  1. h.conn.cfg["wh-sink"] = map[string]string{"connector.class": "x.Sink"}; h.conn.setStatus("wh-sink", running("wh-sink")).
  2. h.breakConnector("orders","boom"); h.steps(4, time.Minute), which opens r1.
  3. h.closeNow(r1). For the second variant also: h.steps(12, time.Minute) (the escalation room opens), then h.closeNow(rooms[1].roomID).
  4. h.conn.setStatus("wh-sink", failed("wh-sink","unrelated")); h.steps(8, time.Minute).
  5. Assert len(h.rooms.created()) increased. It did not.
Result: esc=false phase=acked targets=[pg-orders wh-sink] rooms before=1 after=1. esc=true phase=acked targets=[pg-orders wh-sink] rooms before=2 after=2. Both variants fail with "second connector failure opened no room".
Command: go test ./internal/watch -run TestReproSecondTargetWhileAcked -v
```

## R2. [medium, R] G1: After a restart, a new incident re-attached to the watcher's own still-open room never wakes the agent, and that room skips the join-age rule
- Where: `internal/watch/output.go:497`
- Scenario: Yesterday the watcher opened r1 for connector_down:pg-orders. The signal resolved and the watcher posted 'all signals clear'. r1 stays open as designed, because nobody closed it. The server restarts, or watcher leadership moves to another replica, and the restored agent sits idle (paused since the restart). Today slot_inactive:debezium_orders fires. openRoom gets LastRoomForKey = r1, open, so reuse=true. That skips roomToJoin, so JoinMaxAge and the title-flag rules are never checked for a room a day old. OpenAlertRoom joins r1 with created=false. untold() returns known=false because the new process has no told list. The post goes out with incident = known && len(fresh)>0 = false, which means PostEvidenceJSON, a quiet alert_update. The agent is not woken and no run starts. The new outage sits as one system line in an old room with a paused agent. That is the observed QA G1(b) failure ('an open room absorbs the incident ... the agent, paused since a server restart, was not woken'), now through the watcher's own room instead of a manual one. The order is also backwards: told now holds only slot_inactive's keys. If the original connector_down later re-fires, it is posted as a fresh incident and wakes the agent for the old problem.
- Suggested fix: When reuse is true and the pipeline has no told entry for the room, seed ps.told from RoomSignalKeys(roomID) when the sink has it, then compute fresh against that. Keys the room never carried go through PostIncidentJSON. A true repeat stays a quiet re-attach. Optionally, apply the JoinMaxAge check to the reused room's LastEventAt and post a new-incident wake when the room is stale.
- Verifier: The defect is real and I reproduced it on 08ba8ca. In openRoom, `reuse := found && closedAt == nil` (output.go:398) skips roomToJoin, so JoinMaxAge and the title-flag rules are never checked for the watcher's own open room. After a restart `ps.told` is empty, so `untold()` returns known=false and the post at output.go:497 passes `known && len(fresh) > 0` = false. `post()` then uses PostEvidenceJSON, a quiet alert_update that docs/api.md says never wakes an idle agent. The agent never hears about the new incident.

The Q3 sink that could fix this exists and is wired. `ClosedRoomKeysSink.RoomSignalKeys` reads the folded log whatever the room's status (cmd/server/watch_q3.go). The watcher calls it only in recoverClosed, so for an open room it never learns what the room carried.

In the repro I set the fake room's carried keys to `connector_down:pg-orders` and left `r_before` open with no closedAt. I then made the slot inactive, so `slot_inactive:debezium_orders` fired. The only post was to `r_before`, with via="evidence" and "(re-attached after a restart)". No via="incident" post was made, so the agent is not woken.

What argues against it:
- docs/api.md has a line saying "After a restart the watcher joins a room that is still open but does not wake the agent".
- `TestReattachToTheOwnOpenRoom/after a restart` locks that behaviour in.
- The behaviour was therefore chosen for the case where the watcher could not tell what the room carried.

What argues for it:
- The Q0 incident rule says a signal outside the room's incident wakes the agent.
- QA G1(b) reported exactly this failure ("agent, paused since a server restart, was not woken").
- Q3 now supplies the keys, so the "cannot know" justification no longer holds.

I rate it medium rather than high:
- It needs a restart or a leadership change with the watcher's own room still open.
- The signal is still posted into the room as evidence, and an owner page may still fire.
- Only the agent wake and the join-age gate are lost.

The finding's secondary point also follows from the code. Because `told` holds only the new keys, a later re-fire of the original `connector_down` is treated as a fresh incident and wakes the agent for the old problem.

I did not commit the repro test. A copy is at /tmp/claude-0/-home-user-piperoom/53bb11e1-975e-5bd6-9e2a-762a0f7a558e/scratchpad/zz_repro_test.go. To rerun it, copy it to internal/watch/zz_repro_test.go.
- Repro:
```
File: internal/watch/zz_repro_test.go (package watch). Copy is at /tmp/claude-0/-home-user-piperoom/53bb11e1-975e-5bd6-9e2a-762a0f7a558e/scratchpad/zz_repro_test.go

func TestReproReattachNewSignalWakes(t *testing.T) {
	h := newHarness(t, opts{q3Sink: true})
	h.rooms.byKey["orders|watcher:orders"] = "r_before"
	h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_before"}   // open (no closedAt), as after a restart
	h.q3.setKeys("r_before", "connector_down:pg-orders")                   // what the open room carried
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false }) // NEW signal: slot_inactive
	h.steps(8, time.Minute)
	for _, p := range h.rooms.posts {
		if p.via == "incident" { return }
	}
	t.Fatalf("new signal re-attached to open room posted quietly, not as incident")
}

Command: go test ./internal/watch -run TestReproReattachNewSignalWakes -v
Result: FAIL. The one post to r_before has via=evidence and text "(re-attached after a restart)". Log shows "watch: room joined" for r_before.
```

## R3. [low] G2: Webhook deliveries past 8 in flight are silently dropped, and the once-only auto page is still spent
- Where: `internal/room/service_page.go:630`
- Scenario: Single replica with PIPEROOM_NOTIFY_WEBHOOK_URL set and no Slack. An alert storm (Alertmanager sends 10+ alerts with different dedup keys during a connector outage; alert rooms have no open-room cap) opens 10 rooms within a few seconds. Five minutes later they all fall due in the same 15 s tick. checkAutoPages writes the 10 pages one after another in a few tens of ms. Rooms 1-8 each take a semaphore slot and start a delivery, which takes 100 ms to 11 s (two 5 s attempts plus a 1 s wait). Rooms 9 and 10 hit `default:`: one WARN line and no delivery. The owner.paged event is already in the log, so foldRoomFacts sets autoPaged=true and autoPageDueLocked never retries those rooms. Their owners are never told outside the app. The same burst happens once when this feature is first switched on in a deployment that still has several undriven alert rooms open (they stay open up to 14 days).
- Suggested fix: Queue instead of dropping: a bounded worker pool fed by a buffered channel large enough for a burst, or block or pace the auto-page loop on the semaphore (the loop already runs on its own goroutine). At least do not drop auto pages.
- Verifier: The code does what the finding says, at 08ba8ca. notifyPageLocked (internal/room/service_page.go:625-635) does a non-blocking send on a semaphore of maxPageDeliveries=8. If it is full, it logs one WARN and returns, with no retry or queue. The owner.paged event is already written by then. autoPageDueLocked (same file) returns false once facts.autoPaged is set from the log, so the auto page is never retried for that room. checkAutoPages (same file) pages all due rooms in one serial loop with no pacing. The cap and drop are deliberate and tested: the comment at line 44 and the test at service_page_internal_test.go:554-572 assert that a page past the cap is not sent. docs/api.md says nothing about the drop, so an operator who sets the webhook would not know. Two points weaken it. First, it only fires when 9 or more rooms fall due in the same 15 s tick and the webhook is slower than the serial page writes. Each page write is a few ms to tens of ms, so a fast webhook frees slots early. Second, the page stays in the room and in Slack and the drop is logged, so it is partial loss of an out-of-app channel, not data loss or a security issue. A test would only restate the existing by-design behaviour, so I did not write one. I rate it a real but low-probability gap and lower the severity to low.

## R4. [low, R] G2: Auto-page fallback picks the alert's `layer` label even when nobody owns it, so the room is never paged
- Where: `internal/room/service_page.go:319`
- Scenario: An Alertmanager alert arrives with commonLabels.layer="warehouse" for pipeline orders. owners.yaml lists approvers for kafka-connect and consumer but none for warehouse. Triage abstains or is off, so autoPageDueLocked returns layer "". fallbackLayer returns "warehouse" straight away without checking it has owners. page() is called with needOwners and returns errNobodyToPage. autoPage logs one WARN and returns. Every later tick does the same, so no auto page ever goes out, although kafka-connect and consumer have owners who could be paged. This is the G2 failure again: the room sits undriven and nobody is told. Q3 S4 asked for a fallback 'so someone is paged'. The function's own doc comment ('the first layer ... that has someone to page') applies the owner check only to the second branch. The same dead end applies when the watcher signal's or triage's layer has no owners: there is no fallback after errNobodyToPage.
- Suggested fix: In fallbackLayer, return the label only when pageOwners(label) is non-empty, otherwise go on to the flow-order scan. Optionally, after errNobodyToPage on a signal or triage layer, fall back the same way and say so in the reason.
- Verifier: Confirmed by code and a failing test. fallbackLayer (internal/room/service_page.go:318) returns the alert's layer label without checking that anyone owns it. The owner check applies only when the label is empty. autoPage then calls page() with needOwners, which returns errNobodyToPage. The errNobodyToPage case in autoPage (~line 816) only sets noOwners and logs one WARN. It never retries another layer, so the room is never auto-paged even when other layers have owners. The existing tests miss it: TestAutoPageFallbackLayer uses DefaultUsers, where warehouse has an owner, and TestAutoPageFallbackNobody has no owners at all. The same dead end applies to the watcher's or triage's layer with no owners. Severity is low because it needs a layer label (or signal or triage layer) that has no owners while other layers do. That is a configuration gap, though a plausible one, and the failure is silent: one WARN and no page, which is the G2 outcome.
- Repro:
```
Throwaway test, deleted afterwards, in internal/room (package room), using helpers from service_page_internal_test.go:

func TestReproLabelLayerNoOwners(t *testing.T) {
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.svc.users = mustUsers([]User{{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}}})
	r.load()
	id := openAMRoom(t, r.svc, "a", `{"commonLabels":{"layer":"warehouse"},"alerts":[]}`)
	c.Advance(6 * time.Minute)
	r.tick()
	r.tick()
	if got := autoPages(t, ms, id); len(got) != 1 {
		t.Fatalf("auto pages = %+v, want one to someone (kabir owns consumer)", got)
	}
}

Command: go test ./internal/room -run TestReproLabelLayerNoOwners -count=1
Result: FAIL. Log: "auto page skipped: nobody to page for the layer ... layer=warehouse". Test: "auto pages = [], want one to someone (kabir owns consumer)".
```

## R5. [low] G2: No-driver banner says 'Owners paged: <layer>' when the page named nobody
- Where: `web/src/components/NoDriverBanner.tsx:28`
- Scenario: The agent calls page_owner for postgres, or a member pages it, and the pipeline has no postgres owners. The server writes owner.paged with owners [] (allowed for agent and member pages; only the auto page needs owners). The room's no-driver banner then shows 'Owners paged: postgres', so the room looks handled, while PagesList in the same room says 'paged nobody (no owner is listed)'. The 10-minute window also blocks another page of that layer.
- Suggested fix: Build the list from `state.pages.filter((p) => p.owners.length > 0)`, or say that nobody was reached for that layer.
- Verifier: The banner code is as described. web/src/components/NoDriverBanner.tsx builds `paged` from `state.pages.map(p => p.layer)` with no check on `p.owners.length`. The reducer (web/src/state/reducer.ts) stores `owners: p.owners ?? []`, so an empty list is kept. On the server, `needOwners` is set only for the automatic page (service_page.go:803). Agent and member pages therefore succeed with `owners: []` when the layer has no listed owner. PagesList.tsx and Timeline.q1.test.tsx already show that case as 'paged nobody (no owner is listed)'. The banner would then say 'Owners paged: postgres' while no one was notified, so the room looks handled. The existing banner tests (NoDriverBanner.paged.test.tsx) have no case with empty owners. The defect is only a misleading label in a banner. It needs a layer with no owners plus a manual or agent page. The same room's PagesList shows the correct text, and no security or data-loss risk follows. I did not write a failing test, because the code path is clear from reading it.

## R6. [medium, R] G5: Agent counts only step layers as required: approving the step layer alone makes it think the fix is fully approved, and it then tells the room approval is complete
- Where: `internal/agent/approvals.go:51`
- Scenario: 1) The agent proposes connect_update_connector_config decimal.handling.mode=precise, or schema_register, which is the schema-change demo fix (Q2 F10 made it impact consumer and warehouse). 2) The server sets required_layers = [kafka-connect, consumer, warehouse] (or [schema-registry, consumer, warehouse]). 3) Arjun approves kafka-connect first. 4) The room sends approval_result{layer: kafka-connect, approved: true}. 5) proposalTracker.observe sees every layer in f.required approved, because f.required holds only the step layers. It sets complete=true and waiting=true. 6) awaitStepResults holds the agent's turn and writes agent.status 'working / waiting for step results'. Steers and comments queue up behind it for defaultStepResultWait (45 s). 7) The turn then starts with pendingResultsNote: 'approval is complete for proposal p_…, but not every step result has arrived … do not say approval is missing. Say only that their results are pending.' Consumer and warehouse have not approved, and the executor has not run anything. The agent is told to claim the fix is approved and running. This is the G5 failure (agent prose about approvals is wrong) in a new form, and the room status shows a wait that is not happening. Worse: the server still lets the driver withdraw the proposal (it is not fully approved). The withdrawal's approval_result has approved=false and observe ignores it, so the agent waits on, then reports pending results for a withdrawn proposal.
- Suggested fix: Pass the full approver list to the tracker: record(id, steps, approvers), using the `approvers` slice propose_fix already builds (step layers + in.ImpactedLayers + Impact layers). newExecFlow should use that list as f.required. Also have observe drop the flow (settled=true, waiting=false) on an approval_result with approved=false, which covers both a rejection and a withdrawal.
- Verifier: The defect is real. internal/agent/approvals.go newExecFlow builds f.required only from step layers. tools_room.go calls t.track.record(id, steps) with no impacted layers. The server's applyImpact adds the impacted layers to RequiredLayers, and the server sends a per-layer approval_result. observe therefore sets complete=true and waiting=true once the step layers alone approve. At that point the executor has not run anything, because consumer and warehouse still owe approvals. awaitStepResults then holds the turn for up to 45 s and the turn starts with the "approval is complete, results pending" note. I reproduced it with the real actions registry: a connect_update_connector_config step with decimal.handling.mode=precise has Impact layers [consumer warehouse], and the tracker still reports the proposal as awaiting step results after kafka-connect alone approves. Impact is limited to the agent's prose and a stalled turn (steers queue behind the wait). The executor and the server's approval enforcement are unaffected, so no security or data-loss risk. The withdrawn-proposal variant also follows from the code, because observe ignores approved=false. The ceremony still works, so I rate it medium.
- Repro:
```
File internal/agent/zz_repro_test.go (deleted after the run). The package-level fixtures it relies on are approvalInput and newProposalTracker.

package agent
import ("encoding/json";"testing";"piperoom/internal/actions";"piperoom/internal/events")
func TestReproImpactedLayersNotRequired(t *testing.T) {
	params := json.RawMessage(`{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"precise"}`)
	steps := []events.ProposalStep{{Action: "connect_update_connector_config", Layer: "kafka-connect", Params: params}}
	im := actions.Impact{}
	reg := actions.NewRegistry()
	_ = actions.Register(reg, actions.Deps{})
	if a, ok := reg.Get("connect_update_connector_config"); ok { if i, ok := a.(actions.Impacting); ok { im = i.Impact(params) } }
	t.Logf("server also requires: %v", im.Layers)
	tr := newProposalTracker()
	tr.record("p_1", steps)
	tr.observe(approvalInput("kafka-connect", "p_1"))
	if len(tr.awaiting()) != 0 { t.Fatalf("tracker thinks proposal fully approved after kafka-connect alone; server still needs %v", im.Layers) }
}

Command: go test ./internal/agent -run TestReproImpactedLayersNotRequired -v
Output: "server also requires: [consumer warehouse]" then FAIL "tracker thinks proposal fully approved after kafka-connect alone; server still needs [consumer warehouse]"
```

## R7. [low] G8: Across replicas a proposal.created can land after room.closed: the server never withdraws it, and the web shows 'Proposals 1 open' in a closed room
- Where: `internal/room/service_proposals.go:204`
- Scenario: Setup: shared store, two replicas. 1) The agent runs on replica B and calls propose_fix. B's createProposal runs syncRoom, sees the room open, and starts store.CreateProposal. 2) On replica A a layer owner or the driver closes the room. room.closed is appended, and A's withdrawOnCloseLocked withdraws only what A's openAtClose held. 3) B's plain emitLocked appends proposal.created after room.closed. createProposal does no conditional append. Results: the server snapshot ignores it (the X1 guard in snapshot.go Apply: `if !ok || snap.Status == "closed"`), so no withdrawal is ever written. The proposals row stays status 'open' forever. The web reducer has no such guard (reducer.ts:521, case 'proposal.created'), so every client replaying the log adds an 'open' proposal after room.closed. ProposalsSummary then shows 'Proposals 1 open' and the card lists undecided layers in a closed room, which is the G8 symptom again. The window is narrow (from B's sync read to its append, one DB round trip), but nothing closes it.
- Suggested fix: Append proposal.created with emitCheckedLocked, with a check that the room is still open (on refusal, delete the row as the existing compensation does). Also add the X1 guard to the web reducer: ignore proposal.created when state.status === 'closed'. A replay folds room.closed before any later proposal.created, so this is safe.
- Verifier: The code supports the finding, but I could not reproduce it here. createProposal (internal/room/service_proposals.go) syncs, locks, checks roomOpen on its cached state, then does store.CreateProposal (a DB round trip) and then a plain emitLocked, which is an unconditional store.Append at service.go:727. Every other rule-bound command uses emitCheckedLocked, so this append is not conditional on the last seq. With two replicas on a shared store, another replica can append room.closed between the roomOpen check and the append. Replica A's withdrawOnCloseLocked only covers openAtClose from A's own snapshot, so it never sees the proposal. The proposal.created then lands after room.closed. The server snapshot drops it (snapshot.go:620 guards on Status == "closed"), so no withdrawal is ever written, and the proposals row stays 'open'. The web reducer (reducer.ts:521) has no closed-room guard, so a client replaying the log shows an open proposal in a closed room. That is the G8 symptom. On a single replica, s.mu serializes close and create, so this needs two replicas. I found no mitigation in the code: the compensating DeleteProposal only runs when the append fails.

Impact is limited. ApproveProposal checks roomOpen and returns room_closed, so the orphan proposal cannot be approved or executed, and this is not a safety risk. The effect is stale UI ('Proposals 1 open' in a closed room), and it only shows up on the web's replay, not in the server snapshot. The race window is one DB round trip, and it also needs the agent to call propose_fix at the moment someone closes the room on another replica.

I could not reproduce it. Only the Postgres store implements store.Coordinator, which is the multi-replica path, and this sandbox has no Postgres server and no Docker. An in-memory test would not exercise the cross-replica path. The verdict therefore rests on code reading alone. Severity is low.

## R8. [low, R] G10: Resolution is ignored as fix evidence whenever any executed step or fix note exists, so a stale hand-off's Still open / Risks are shown as current
- Where: `internal/room/postmortem.go:462`
- Scenario: The agent's proposal p_1 runs ok (action.executed), then a hand-off lists Open 'Restart the consumer' and Risks 'lag keeps growing', then Kabir restarts the consumer outside Piperoom and closes the room with resolution 'Fixed: restarted the consumer, lag drained' (no fix note). choosePostmortem picks source 'handoff', because the hand-off is newer than all work. In render, fixEvidence returns the executed step (seq below the hand-off), and the resolution branch only runs when ev.seq == 0. stale is therefore false. I replayed this in a scratch copy: the draft prints 'Resolution: Fixed...' in Root cause and then '## Still open - Restart the consumer' and '## Risks - lag keeps growing' as current lines, with no stale label. The same happens if a fix note was logged before the hand-off and the closing resolution came after it. This is the same contradiction as the QA consumer-lag room, only with a Piperoom step or an earlier fix note in the log.
- Suggested fix: Make the human resolution a candidate evidence on its own: if resolutionIsFix() and closedSeq > ev.seq, use ev = resolution. Also count the human close as work in choosePostmortem, so a hand-off older than a human close with a resolution is not taken as current.
- Verifier: The finding matches the code at 08ba8ca. fixEvidence (internal/room/postmortem.go:453-465) only falls back to the resolution when ev.seq == 0, so an earlier successful executed step or fix note hides a newer human resolution. render then computes stale = p.have && haveEv && ev.seq > p.asOfSeq. Here ev.seq is the old step or note, which is below the hand-off's seq, so stale is false. The hand-off's Still open and Risks lines are printed as current. I found no code or test that handles this combination. Existing tests cover the resolution-only case, the fix-note-after-hand-off case and the system-close case, but not a fix signal before the hand-off followed by a human resolution after it. I reproduced it with a throwaway test, now deleted. It built the log: investigation, then either a successful action.executed or a fix note, then a hand-off with staleSummary(), then a human room.closed with resolution 'Fixed: restarted the consumer, lag drained', then postmortem.drafted with source handoff. Both variants (executed step, earlier fix note) render 'Resolution: Fixed...' next to '## Still open - Restart the consumer' and '## Risks - lag keeps growing', with no 'Earlier, from the' label, so the test failed on both. Severity is low rather than medium. The output is a draft post-mortem for humans, so nothing is lost and no security risk arises. It needs a narrow sequence: a fix signal before the hand-off, no later fix note, and a human closing resolution after it. The effect is a misleading, self-contradicting document. A fix is to make fixEvidence take the newest of the executed step, fix note and human resolution by seq.
- Repro:
```
Throwaway test, internal/room/zz_repro_test.go (package room_test, deleted afterwards). Command: go test ./internal/room -run TestReproResolutionStale -v

func TestReproResolutionStale(t *testing.T) {
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
				t.Fatalf("stale open item shown as current")
			}
		})
	}
}

Result: FAIL for both subtests (exec, fixnote). The draft shows 'Resolution: Fixed...' with '## Still open - Restart the consumer' and '## Risks - lag keeps growing' as current lines.
```

## R9. [low, R] G10: Agent hand-off writer does not know fix notes: they are non-critical, unlabelled comments inside a 30-event tail window
- Where: `internal/agent/prompt.go:434`
- Scenario: A person logs a fix note ('restarted order-sync by hand') in an open room. The agent keeps running (verification, tool calls, messages), and then the driver asks for a hand-off. renderRoomEvent prints the note as a plain 'comment c_..: text' line. isHandoffCritical does not include comments, so the note competes for the 30 newest non-critical lines (handoffTailEvents) and is dropped once enough later events exist. handoffPrompt says nothing about fix notes either. The hand-off then lists 'Restart the consumer' under open. That hand-off is newer than the fix note, so choosePostmortem selects it, and render does not mark it stale (the fix note seq is below the hand-off seq). The post-mortem prints the fix note under What was done and the restart under Still open.
- Suggested fix: Treat comment.added with kind fix_note as hand-off critical, render it as 'fix note by <name>: ...', and add a line to handoffPrompt saying a fix note is a person's account that settles the open items it covers.
- Verifier: The code facts hold at 08ba8ca. isHandoffCritical (internal/agent/prompt.go:434) excludes CommentAdded. renderRoomEvent prints every comment as a plain 'comment <id>: ...' line and ignores p.Kind. renderRoomEvents keeps only the newest 30 non-critical lines (handoffTailEvents). handoffPrompt has no fix-note instruction; only postmortemOutcomeRules mentions fix notes. choosePostmortem (internal/room/service_postmortem.go) counts a fix note as work, but it picks a hand-off whose seq is above the note. The post-mortem render marks open items stale only when the fix evidence seq is above the summary's asOfSeq (postmortem.go:558). A hand-off written after the note is therefore never marked stale, even if it never saw the note or did not treat it as a fix. The post-mortem would then show the note under What was done and the hand-off's 'restart the consumer' under Still open. I reproduced the first half with a throwaway test, since deleted. A fix note followed by 31 agent messages is absent from renderRoomEvents output. handoffPrompt() does not mention fix notes. Both assertions failed on the current code. The severity is low for three reasons. First, the drop needs more than 30 non-critical events after the note, and there is a plausible model-side path where the note survives and the model uses it. Second, the end-to-end contradiction depends on model behavior, which I did not and cannot test deterministically. Third, the spec (tasks/Q1-qa-gaps.md G10) only says 'build from the resolution and the events after the last hand-off; let people log what they did'. It does not require the hand-off writer to treat fix notes specially. It is a real gap and a mild inconsistency, not a data-loss or security problem. The impact is a draft post-mortem document that a person can edit.
- Repro:
```
Throwaway test in internal/agent (deleted afterwards): build evs = [comment.added{CommentID:"c_1", Text:"restarted order-sync by hand", Kind:events.CommentKindFixNote}] followed by 31 agent.message events, then assert renderRoomEvents(evs) contains "restarted order-sync by hand". Second test: assert strings.Contains(strings.ToLower(handoffPrompt()), "fix note"). Command: go test ./internal/agent -run TestRepro. Result: both tests FAIL ("fix note dropped from hand-off window"; "handoff prompt does not mention fix notes").
```

## R10. [low, R] G10: isHandoffNoise misses hand-off bookkeeping lines that end the sentence with the process word
- Where: `internal/room/postmortem.go:415`
- Scenario: hasProcessWord treats '.' as an identifier byte after the word. A line such as 'Tool output was truncated in the hand-off.' or 'Retry the failed handoff.' therefore has the word skipped and the line is not removed. I ran isHandoffNoise on these: 'One handoff failed' and 'Agent summary lines were truncated.' give true, but 'Tool output was truncated in the hand-off.' and 'Retry the failed handoff.' give false. Models usually end list items with a period, so this process noise ('Hand-off noise in the content') still reaches Still open and Risks in some drafts.
- Suggested fix: After the word, treat '.' as part of an identifier only when another identifier character follows it (for example 'orders.summary'); a '.' at the end of the line or before whitespace ends the word.
- Verifier: In internal/room/postmortem.go, hasProcessWord rejects a match when the byte right after the word is an identifier byte, and isIdentByte includes '.'. A process word at the end of a sentence ("...in the hand-off.") is therefore skipped, and isHandoffNoise returns false. The existing tests only cover lines with no trailing period, so they miss this. A scratch test confirmed the failure on 08ba8ca: "Tool output was truncated in the hand-off." and "Retry the failed handoff." are not filtered. "The closing summary was truncated." happens to pass, because the summary word comes before "was". The same gap would hit "...truncated in the summary." The defect is real but narrow. The filter is a best-effort heuristic, the effect is a few bookkeeping lines staying in a draft postmortem's Still open or Risks lists, and there is no security or data-loss risk. A one-line fix would be to treat '.' as an identifier byte only when the next byte after it is also an identifier byte. I deleted the scratch test.
- Repro:
```
// internal/room/zz_repro_internal_test.go (throwaway, deleted)
package room
import "testing"
func TestReproPeriod(t *testing.T) {
	for _, l := range []string{"Tool output was truncated in the hand-off.", "Retry the failed handoff.", "The closing summary was truncated."} {
		if !isHandoffNoise(l) { t.Errorf("not noise: %q", l) }
	}
}
// go test ./internal/room -run TestReproPeriod
// FAIL: not noise: "Tool output was truncated in the hand-off." and "Retry the failed handoff."
```

## R11. [medium, R] G3: connect_worker_log fails when Connect's REST API hangs, which is when the log is most needed
- Where: `internal/tools/kafkatools/worker_log_tool.go:121`
- Scenario: Connect is mid-rebalance or its REST port accepts but does not answer, a common state for a crash-looping worker. Run() creates one 10 s context (cctx) at line 121, then logNames() at line 182 calls Connect.Config(cctx, connector) to learn topic.prefix. Config blocks until cctx expires. The next call, ConnectLog.WorkerLog(cctx, ...), is the file reader. Its first step is `if err := ctx.Err(); err != nil { return nil, ... }`, so it returns context deadline exceeded. The tool returns `kafkatools: connect_worker_log: connect: worker log: context deadline exceeded` after 10 s and the agent gets no log evidence. I reproduced it in a scratch copy of the tree: a fake Connect whose Config waits on ctx.Done() gave `took 10.009s err ... context deadline exceeded`. A related gap: when Config fails, the pipeline TopicPrefix fallback at line 191 runs only if fromDefault is true. With an explicit `connector` argument, which the tool description encourages, the Debezium logical name (Postgres|pg|streaming) is never searched. The tool then answers 'No WARN lines name ...' even though the file holds the error.
- Suggested fix: Give logNames its own short child context (about 2 s) and keep cctx for the file read. Create cctx after logNames returns, or derive the lookup context from ctx and not from cctx. Apply the sc.Pipeline().TopicPrefix fallback whenever the connector belongs to the pipeline, not only when fromDefault is true.
- Verifier: Confirmed at 08ba8ca. Run() creates one 10 s cctx (worker_log_tool.go:121). logNames() calls Connect.Config(cctx, ...) on that same context (line 182). When Connect's REST API hangs, Config blocks until cctx expires. The real reader then fails at WorkerLog's first step, `if err := ctx.Err()` (internal/clients/connect/worker_log.go:79). The tool returns "kafkatools: connect_worker_log: connect: worker log: context deadline exceeded" after 10 s and the agent gets no log evidence. The production Connect client has a 10 s http.Client timeout, the same as cctx, so Config returns at about the deadline and cannot leave useful time for the file read. Nothing in the code refutes this: the Config failure is silently ignored, but the shared deadline is already spent by then. A hung Connect REST API is the state where the worker log matters most, because the log is a file read that does not need Connect.

The second gap is also real. The TopicPrefix fallback runs only when `fromDefault` is true (line 191), so an explicit `connector` argument gets no Debezium logical-name search once Config fails. I did not run a test for this one. Severity stays medium: the tool is degraded only while Connect is unresponsive, and the agent still has other tools. A fix is to give Config its own sub-timeout (a few seconds) and read the file under a fresh or remaining context, and to apply the TopicPrefix fallback whenever Config fails, not only when fromDefault is true.

Repro caveat: the existing fakeLogReader does not check ctx.Err(), so the same test passes (err=nil) with that fake. The test below fails only because its reader checks ctx.Err() first, as the real connect.LogReader does at worker_log.go:79. The throwaway test file has been removed and the worktree is clean.
- Repro:
```
File: internal/tools/kafkatools/zz_repro_test.go (package kafkatools; throwaway, deleted after the run)

func TestReproHungConfig(t *testing.T) {
	at := time.Date(2026, 10, 15, 4, 10, 9, 0, time.UTC)
	d, _ := scopeFakes(t)
	d.Connect = &fakeConnect{
		listFn: func(context.Context) ([]string, error) { return []string{"pg-orders-connector"}, nil },
		configFn: func(ctx context.Context, _ string) (map[string]string, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	d.ConnectLog = &fakeLogReader{fn: func(ctx context.Context, q clients.WorkerLogQuery) ([]clients.LogLine, error) {
		if err := ctx.Err(); err != nil { // same first step as connect.LogReader.WorkerLog
			return nil, err
		}
		return []clients.LogLine{{At: at, Level: "ERROR", Message: "must be owner of table orders"}}, nil
	}}
	d.Now = func() time.Time { return logNow }
	start := time.Now()
	_, err := scopeTool(t, d, "connect_worker_log").Run(roomCtx(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	t.Logf("took %v err=%v", time.Since(start), err)
	if err != nil {
		t.Fatalf("log evidence lost because Config hung: %v", err)
	}
}

Command: go test ./internal/tools/kafkatools -run TestReproHungConfig -v
Result: FAIL after 10.0s, err=kafkatools: connect_worker_log: context deadline exceeded
```

## R12. [low, R] G3: Debezium thread-name match lets a pipeline whose topic.prefix extends another's leak into that room's log
- Where: `internal/clients/connect/worker_log.go:473`
- Scenario: Pipeline A has topic.prefix `pg` and pipeline B on the same worker has `pg-eu`. B's Debezium thread is named `debezium-postgresconnector-pg-eu-change-event-source-coordinator`. For name `pg`, mentionsDebeziumThread accepts a tail that starts with the name and is followed by `-`, so B's thread matches. Any WARN or ERROR entry that names B's thread, such as 'Exception in thread ...', is returned in A's room. The `others` filter in mergeLogLines only drops lines that name another pipeline's connector, not another pipeline's topic.prefix, so the line is not removed. A's room then sees B's errors, table names and messages, and the agent may blame A's pipeline for them.
- Suggested fix: After `<name>-`, require a known Debezium thread suffix (change-event-source-coordinator, SignalProcessor, snapshot, streaming, and so on). Alternatively, make the `others` filter in mergeLogLines also reject lines that name another pipeline's topic.prefix or logical name, using the same thread rule.
- Verifier: The defect is real in worker_log.go (mentionsDebeziumThread). It accepts a tail that starts with the name and is followed by '-'. That is needed for the real thread suffix "-change-event-source-coordinator", but it also matches a longer hyphenated prefix. A throwaway test confirmed it: a log line with thread debezium-postgresconnector-pg-eu-change-event-source-coordinator, read with Connector "pg", returns the line. The pg2 case is correctly rejected, as the finding says. Nothing downstream removes the line. mergeLogLines drops only lines naming another pipeline's connector, from ListConnectors and owners.yaml, and never another pipeline's topic.prefix. The leak is blocked only if the other pipeline's Connect connector is itself named "pg-eu", which is not the usual case (connector names such as pg-orders, prefixes such as pg). It is limited to Debezium thread-name lines, and it needs a shared worker with hyphen-extending topic prefixes. The impact is cross-pipeline log noise in the agent's WARN/ERROR view, not a security issue or data loss. The existing tests only cover "pgx", so they miss this case. Low severity is right. A fix would be to require known thread suffixes after the name, or to add other pipelines' topic prefixes to the `others` list.
- Repro:
```
File internal/clients/connect/zz_probe_test.go (throwaway, deleted after the run; copy at /tmp/claude-0/-home-user-piperoom/53bb11e1-975e-5bd6-9e2a-762a0f7a558e/scratchpad/zz_probe_test.go.txt):

func TestProbePrefixLeak(t *testing.T) {
	log := "2026-10-15 04:10:10,000 ERROR  [debezium-postgresconnector-pg-eu-change-event-source-coordinator]  boom   [io.debezium.X]\n"
	got := readLog(t, log, clients.WorkerLogQuery{Connector: "pg"})
	if len(got) != 0 { t.Fatalf("leak: %+v", got) }
}

Command: go test ./internal/clients/connect -run TestProbePrefixLeak
Result: FAIL, "leak: [{... Message:[debezium-postgresconnector-pg-eu-change-event-source-coordinator] boom}]"
```

## R13. [low, R] G3: pg_publication_tables states PROBLEM for role facts it cannot observe (rds_replication, config-provider user)
- Where: `internal/tools/dbtools/pg_publication.go:319`
- Scenario: (a) On RDS or Aurora the standard Debezium setup is `GRANT rds_replication TO debezium`. That role has pg_roles.rolreplication=false and rolsuper=false, so line 322 emits 'PROBLEM connector user debezium has no REPLICATION attribute ... so it cannot start logical replication'. That is false, yet it is labelled PROBLEM and the message only hedges in a parenthesis. The agent can chase a nonexistent cause or propose a wrong fix. (b) Connect's GET /connectors/{n}/config returns config-provider references unresolved. A connector with `database.user=${secrets:db:user}` gets role `${secrets:db:user}`, PublicationInfo finds no such role, and line 319 asserts 'PROBLEM connector user ${secrets:...} does not exist as a role, so the connector cannot log in'. Before this change the same input raised a tool error; now it produces a confident wrong finding.
- Suggested fix: In PublicationInfo, also report pg_has_role(r.oid, 'rds_replication', 'USAGE') when that role exists, and treat it as replication-capable. Skip the 'does not exist' and 'no REPLICATION' PROBLEM findings, or downgrade them to NOTE, when database.user contains `${` (an unresolved provider reference).
- Verifier: The code does what the finding says. publicationFindings (internal/tools/dbtools/pg_publication.go ~line 319-323) emits a PROBLEM finding whenever pg_roles shows rolreplication=false and rolsuper=false. It also emits a PROBLEM finding when the role name is not in pg_roles. PublicationInfo reads only rolreplication, rolsuper and existence, and never checks membership in rds_replication. I confirmed both cases on commit 08ba8ca with a throwaway test that calls publicationFindings directly with a fake roleFacts. The test failed for both reasons and I deleted it afterwards. (a) An RDS or Aurora role that gets REPLICATION through `GRANT rds_replication` (the standard Debezium setup there) is reported as "PROBLEM ... cannot start logical replication". The only hedge is a parenthetical, so the label is wrong. (b) Connect returns an unresolved config-provider reference such as ${secrets:db:user} as database.user, and the tool reports "PROBLEM ... does not exist as a role, so the connector cannot log in". Before this change, the same input raised a tool error. I found no code that skips these cases and no test for them. The existing tests only cover rolreplication=false on a plain role. Impact is limited. These are advisory text findings from a read-only tool, with no security or data-loss risk and no action executed from them. The hedge in the first message softens (a), and the agent can compare the finding with other evidence such as a connector that is running fine. The effect is a possible misleading lead during an incident on managed Postgres, so I kept the severity at low.
- Repro:
```
File: internal/tools/dbtools/zz_repro_test.go (deleted after the run)

package dbtools
import ("strings";"testing";"piperoom/internal/clients/postgres")
func TestReproRDS(t *testing.T) {
	c := connectorConfig{role: "debezium", publication: "p"}
	f := roleFacts{info: &postgres.PublicationInfo{Role: "debezium", RoleExists: true}}
	for _, g := range publicationFindings(c, nil, nil, f) { if strings.HasPrefix(g, "PROBLEM") { t.Errorf("false PROBLEM for rds_replication member: %s", g) } }
	c.role = "${secrets:db:user}"
	for _, g := range publicationFindings(c, nil, nil, roleFacts{info: &postgres.PublicationInfo{Role: c.role}}) { if strings.HasPrefix(g, "PROBLEM") { t.Errorf("false PROBLEM for unresolved provider: %s", g) } }
}

Command: go test ./internal/tools/dbtools -run TestReproRDS
Result: FAIL, both assertions fire, with the messages "PROBLEM connector user debezium has no REPLICATION attribute..." and "PROBLEM connector user ${secrets:db:user} does not exist as a role...".
```

## R14. [low] G6: A repeat agent.verification adds a second "Verification: recovered" row to the activity feed
- Where: `web/src/state/describe.ts:252`
- Scenario: The last step's action_result is delivered twice, for example a replay or duplicate delivery. The agent correctly emits the Repeat form: verify.go:262 sets Repeat:true, Checks:[] and Verdict "already verified...". reducer.ts:594 keeps the full verdict on the proposal card. But reducer.ts:809 calls describeEvent for every event, and describeEvent's 'agent.verification' case ignores p.repeat. It returns `Verification: recovered`, kind 'decision', and ActivityFeed shows 'decision' rows by default (only 'system' rows are hidden). The activity list now announces the same recovery or "not recovered" twice. The Repeat form exists to stop exactly that, so the web re-creates the G6 symptom "recovery announced several times".
- Suggested fix: In describeEvent, for p.repeat return `kind: 'system'` (hidden by default) with text like "Verification already recorded", or have the reducer skip the activity row for a repeat.
- Verifier: The code confirms the finding. verify.go (~line 255) emits agent.verification with Repeat:true, empty Checks and Recovered copied from the first verdict when a last-step action_result arrives again. The reducer protects the proposal card (reducer.ts:588-597: a repeat never overwrites a full verdict). But applyEvent (reducer.ts:~809) still calls describeEvent for every event. The 'agent.verification' case in describe.ts:252-255 never checks p.repeat, so it returns `Verification: recovered` or `Verification: not recovered` with kind 'decision'. ActivityFeed (ActivityFeed.tsx:48) only hides 'system' rows by default, so a second identical row shows up. The events.go doc and api.md say Repeat exists so the verdict is not announced twice, and the activity feed announces it twice. No test covers the activity path. Severity is low: the cosmetic duplicate row appears only when the last step's action_result is delivered twice, which is rare. The proposal card state is correct, and nothing is lost or exposed. I did not run a test because web/node_modules is not installed in this worktree and there is no Docker. The behaviour follows directly from the code paths above. A one-line fix would be to return kind 'system' or a muted "already verified" text when p.repeat is true.

## R15. [medium, R] G6: Idle-source rule can mark a RUNNING-but-not-capturing connector as "recovered"
- Where: `internal/agent/verify.go:902`
- Scenario: The fix leaves Connect RUNNING with RUNNING tasks and an active slot, so the layer checks pass. Debezium still crash-loops on a retriable error (the G3 case, where the worker log shows it and Connect says RUNNING) or captures nothing, while the source keeps writing. Sinks get no new rows in 3 x freshnessWait. sourceIdle then compares source-topic end offsets against the proposal-time baseline (rec.topicEnds). The stuck connector wrote nothing to the topics, so the offsets are equal and sourceIdle returns true. verifyProposal takes the `!flow.fresh && flow.idle` branch and announces "recovered: every layer check passed; no new source changes to confirm data flow" with recovered=true. The room then parks as done and shows "Pipeline recovered. Close room". The verdict text claims there were no new source changes, but nothing measured the source.
- Suggested fix: Require independent evidence that the source was quiet before returning idle. For example, also require that the slot's retained_wal_bytes did not grow and restart_lsn did not move across the window, or compare the source's current WAL LSN with the slot's confirmed_flush. Otherwise keep "NOT recovered: data flow not verified" when sinks are stale and the connector is not shown to be capturing.
- Verifier: At 08ba8ca, sourceIdle (internal/agent/verify.go:902-933) decides "idle" only from the pipeline's own source data topics. It compares their end offsets against the proposal-time baseline in rec.topicEnds. A connector that is stuck or not capturing writes nothing to those topics, so the offsets stay equal and the function returns true. It reads the source only through the slot fallback, which runs only when the topic read fails. verifyProposal (verify.go:530) then takes the `!flow.fresh && flow.idle` branch. That sets recovered=true with the head "recovered: every layer check passed; no new source changes to confirm data flow."

This is a regression. The earlier slot-fingerprint check (confirmed_flush_lsn, restart_lsn, retained WAL) did catch a source that wrote while the slot stood still. The existing test TestBusySourceStaleWarehouseStaysNotRecovered covers that case, but only on the slot path.

I could not refute it from the surrounding code. Heartbeat topics are excluded on purpose, which also removes the signal that the connector is alive. The idleDetail text still says "replication slot did not move, no new WAL", which the topic path never checked. The Q3 spec for this item (S5) said to compare the source's newest change or slot position, and the topic path does neither.

Reproduction: I wrote a throwaway test (since deleted) using the existing helpers runTopicCase, slotsTool and topicOffsetsTool. The slot stood still with retained WAL growing from 100 to 900, the data topic stayed at 42, Connect was RUNNING with RUNNING tasks, and the sink was stale. The verdict was "recovered: ... no new source changes to confirm data flow", so the assertion that it must not claim recovered failed on the current code.

Severity stays medium. It needs a narrow condition: layer checks pass while Debezium captures nothing. That is exactly the failure the data-flow check exists to catch. The consequence is a false "Pipeline recovered. Close room" verdict. The watcher may later reopen the room on slot_wal or lag signals, which limits the damage.
- Repro:
```
File internal/agent/zz_repro_test.go (deleted after the run):

package agent

import "testing"

func TestReproStuckConnectorTopicsUnchanged(t *testing.T) {
	slots := slotsTool([3]any{"0/5000", "0/4000", 100}, [3]any{"0/5000", "0/4000", 900})
	msgs := runTopicCase(t, slots, topicOffsetsTool(t, 42))
	assertVerdict(t, msgs, false)
}

Command: go test ./internal/agent -run TestReproStuck -count=1
Result: FAIL. The verdict was "recovered: every layer check passed; no new source changes to confirm data flow."
```

## R16. [low, R] G6: Repeat-message guard never resets on new human input or time, so identical answers to later steers are silently dropped
- Where: `internal/agent/msgguard.go:76`
- Scenario: Free-text model replies (run.go:280, no ReplyTo) and canned messages are keyed by text only. The guard remembers just the previous accepted message and has no time or human-input boundary. Driver asks "is the slot still active?" and the agent replies "Yes, the slot is active." Ten minutes later another person asks again, the agent runs a tool and replies with the same words as its only message. The guard returns dropReasonRepeat, run.go:587 logs it at Debug, and the person gets no reply. The same applies to the canned refusal "I cannot proceed with this request: <reason>" and to short answers such as "No." or "Understood." after a different steer, if nothing else was posted in between. The post_message tool at least tells the model; the free-text path does not.
- Suggested fix: Make the repeat window bounded, for example drop only if the previous identical message was accepted within about 60 s, or reset the guard when a human input is drained. A human input should end the "previous message" window, since the QA failure was the agent repeating itself unprompted within 17 s.
- Verifier: The guard is created once per run (run.go:152). It keeps only the last accepted message, and nothing resets it on human input or time (msgguard.go). The text path at run.go:280 emits `AgentMessagePayload{Text: t}` with no ReplyTo. The canned refusal text is keyed the same way. emit() logs a dropped message at Debug (run.go:585-590). The only tests cover repeats within a single run of messages, and none cover a new steer between identical replies.

I reproduced it at run level. Two different humans steer an idle agent, and the model answers both with the same words and nothing else posted in between. The second answer is silently dropped, and the room shows one answer for two steers. I read this failure as the stated cause because the same run posts "done", meaning the loop moved on after the drop.

It is low severity. It needs the model to reply with exactly the same text, as the only agent.message between two steers. That is uncommon, because replies usually vary or the model posts something else in between. The cost is an unanswered steer, with no data loss or security impact, and the person can re-ask. A fix would reset the guard when a non-quiet human input arrives, or set ReplyTo on free-text replies.
- Repro:
```
Throwaway file internal/agent/zz_repro_test.go (deleted afterwards, uncommitted):

```go
func TestReproRepeatAcrossSteers(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	m := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"Yes, the slot is active."}]}`),
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"Yes, the slot is active."}]}`),
	}}
	inbox := &limitedWaitInbox{n: 2, fakeInbox: fakeInbox{drains: [][]HumanInput{
		nil,
		{{Seq: 5, Actor: events.Actor{Kind: "human", Name: "ana"}, Kind: "steer", Text: "is the slot still active?"}},
		{{Seq: 9, Actor: events.Actor{Kind: "human", Name: "bo"}, Kind: "steer", Text: "is the slot still active now?"}},
	}}}
	sink := &fakeSink{}
	r := newTestRunner(t, m, inbox, sink, rec, Options{MaxTurns: 5})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.run(ctx)
	n := 0
	for _, x := range guardPostedTexts(sink) { if x == "Yes, the slot is active." { n++ } }
	if n != 2 { t.Fatalf("agent messages in room = %d, want 2 (one answer per steer)", n) }
}
// limitedWaitInbox embeds fakeInbox; Wait returns nil twice, then context.Canceled.
```

Command: `go test ./internal/agent -run TestReproRepeatAcrossSteers -count=1 -v`

Output: `posted=["Yes, the slot is active." "done"]` then `agent messages in room = 1, want 2 (one answer per steer)` and FAIL.
```

## R17. [low] G11: Step links inside the hand-off dialog scroll the page behind the modal and look dead
- Where: `web/src/components/room/AgentText.tsx:36`
- Scenario: A hand-off is pending and a person opens HandoffDialog, a Radix modal. The summary's what_broke, why, open and risks are rendered through AgentText inside a StepNumbersProvider (HandoffDialog.tsx lines 128 and 285). The agent wrote 'see s_01M4...' and it renders as a 'step 3' link. Clicking it calls goToStep(), which runs scrollIntoView and el.focus() on a timeline row that sits outside the dialog. The overlay hides the scroll. The focus trap also pulls focus back into the dialog. The dialog stays open, so the user sees nothing happen. For hand-offs, which are the main place agent prose cites steps, the link does not do what G11 asks.
- Suggested fix: Let StepNumbersProvider take an optional onFollow callback. HandoffDialog passes onClose, or closes first and then calls goToStep after the dialog has unmounted. Alternatively, render a non-link 'step N' span inside modals.
- Verifier: The code matches the finding. AgentText's onClick calls goToStep(part.id), and goToStep (web/src/lib/triage.ts:65-73) only runs getElementById, scrollIntoView and focus. It has no close callback and no onNavigate. HandoffDialog (web/src/components/HandoffDialog.tsx) wraps its summary fields in StepNumbersProvider and renders AgentText inside a shadcn/Radix modal DialogContent (open, with an overlay). HandoffReceiveDialog does the same. Neither passes anything that closes the dialog. The target row is in the Timeline, outside the dialog. Radix's FocusScope pulls focus back inside the dialog and the overlay hides the page, so the click appears to do nothing and the dialog stays open. I found no refuting code or test. The tests in AgentText.click.test.tsx exercise the link outside a dialog. G11 (tasks/Q1-qa-gaps.md:58) only asks that raw step ids become "step N" links, and the links do render in the dialogs. The defect is that they don't navigate there. I did not write a jsdom test of the focus trap, so reproduced is false. It is a UX gap with no data-loss or security risk. The link is dead only inside modal dialogs, and the user can close the dialog and read the timeline. I rate it low, not medium.

## R18. [low] G11: Step link silently does nothing when the layer filter hides the step
- Where: `web/src/lib/triage.ts:65`
- Scenario: The user has a layer chip selected (layerFilter in RoomScreen). Timeline renders `if (!hit && !live) return null`, so steps on other layers have no <li id=step-...> in the DOM. The hypothesis text or an agent message says 'see s_X', which is a known id and so renders as a 'step 4' link. The step is hidden by the filter, so goToStep() finds no element and returns. The click does nothing, with no feedback and no filter reset. The same happens for the 'based on steps' chips in TriageCard.
- Suggested fix: When the target element is missing, clear the layer filter (a callback via the room context or the provider) and retry on the next frame. Or render a visible 'hidden by filter' hint.
- Verifier: The code matches the finding. Timeline.tsx:509-510 skips rendering a step (`return null`) when a layer filter is active, the step has no tool call on that layer, and it isn't the live step. So hidden steps have no `<li id=step-...>` in the DOM. `goToStep` in web/src/lib/triage.ts:65-67 does `getElementById(...)` and returns silently when the element is missing. It is the only click handler for the step links in AgentText.tsx:33 and TriageCard.tsx:62. Both call `e.preventDefault()` first, so the link does not navigate either. Nothing resets the filter or shows feedback, so clicking a step link for a hidden step does nothing. This is not a security or data-loss problem. It is a small UX gap in an uncommon state: a layer chip is selected and a link points to a step on another layer. The filter banner ("Showing N steps on X · Clear") is visible, which softens it, since the user can clear the filter and click again. I didn't write a test, because the behaviour follows directly from the two code paths above. Low severity.

## R19. [medium, R] regression: Debezium thread-name rule matches a longer logical name, so connect_worker_log returns another pipeline's lines
- Where: `internal/clients/connect/worker_log.go:473`
- Scenario: Pipeline A's connector has topic.prefix "orders". Pipeline B, which may belong to another tenant, runs on the same shared worker with connector "eu-cdc" and topic.prefix "orders-eu". A's room calls connect_worker_log. logNames returns ["<A connector>", "orders"], and the reader filters with mentionsConnector(text, "orders"). B's Debezium lines carry the thread "[debezium-postgresconnector-orders-eu-change-event-source-coordinator]". In mentionsDebeziumThread the tail is "orders-eu-change-...": it starts with "orders" and the next byte is '-', so the line matches. mergeLogLines then drops only lines that name another pipeline's connector name ("eu-cdc"). B's lines name only B's logical name, so they stay. A's room and its model get B's error traces (table names, SQL errors, possibly row data; only secrets are redacted).
- Suggested fix: Match the thread name only when the logical name is followed by a known Debezium thread suffix ("-change-event-source-coordinator", "-snapshot", "-streaming", "-offsets" and similar). Also add the other pipelines' topic.prefix / database.server.name values to `others` in otherConnectors, so lines that name them are dropped. Add tests for prefix pairs like "orders" / "orders-eu".
- Verifier: The code matches the finding. mentionsDebeziumThread (internal/clients/connect/worker_log.go:473) accepts any '-' after the name in a Debezium thread tail. The '-' is needed for the real suffix "-change-event-source-coordinator", but it also accepts "-eu-change-...". logNames adds topic.prefix as a search name. otherConnectors and mergeLogLines drop lines by other pipelines' connector names only, never by their topic.prefix. So a Debezium thread line from pipeline B (prefix "orders-eu") survives in pipeline A's (prefix "orders") result. That is a cross-pipeline, potentially cross-tenant, leak of error text on a shared worker. Only secrets are redacted. It needs one pipeline's logical name to be a hyphen-prefix of another's on the same worker, which is plausible (orders / orders-eu) but not universal, so I rate it medium. The plain-token rule (boundaryAfter) correctly rejects "-eu"; only the thread rule is loose. I reproduced it with a throwaway test in internal/clients/connect, since deleted. The test reads a log line "[debezium-postgresconnector-orders-eu-change-event-source-coordinator] eu pipeline failure ..." with WorkerLogQuery{Connector: "orders"} through the existing readLog helper. It returned that line and failed with "pipeline A (prefix orders) got pipeline B's line". Existing tests only check that "pgx" is rejected, not "pg-eu".
- Repro:
```
File internal/clients/connect/zz_repro_test.go (package connect):

func TestReproPrefixCollision(t *testing.T) {
	log := "2026-10-15 04:10:10,000 ERROR  [debezium-postgresconnector-orders-eu-change-event-source-coordinator]  eu pipeline failure on table eu_customers   [io.debezium.pipeline.ErrorHandler]\n"
	got := readLog(t, log, clients.WorkerLogQuery{Connector: "orders"})
	if len(got) != 0 {
		t.Fatalf("pipeline A (prefix orders) got pipeline B's line: %+v", got)
	}
}

Command: go test ./internal/clients/connect/ -run TestReproPrefixCollision
Result: FAIL (the line from the orders-eu thread is returned for Connector "orders").
```

## R20. [medium, R] regression: Idle-source rule declares a fix 'recovered' when the connector produces nothing while the source keeps writing
- Where: `internal/agent/verify.go:918`
- Scenario: Take the QA G17 case: a connector that reads RUNNING but loops on a decoding error, or any fix after which Debezium stays RUNNING without streaming. The agent proposes and runs a connector restart. The layer check (connect_status RUNNING) passes. The source keeps taking writes, but nothing reaches the data topics, so every sink is stale. sourceIdle reads pipeline_map topics and kafka_topic_offsets. The end offsets equal the proposal-time baseline because the connector produces nothing, so it returns idle=true. verifyProposal then writes agent.verification recovered=true with "recovered: every layer check passed; no new source changes to confirm data flow". Before this change the verdict was "NOT recovered: data flow did not pass". The room, the hand-off and the post-mortem now say a broken pipeline recovered.
- Suggested fix: Count the source as idle only when the data topics did not move AND the source did not write. For example, also require the slot fingerprint (retained WAL, ignoring the LSN movement caused by heartbeats) or pg_current_wal_lsn growth below a small threshold over the window. Otherwise report "inconclusive" with recovered=false instead of recovered=true.
- Verifier: The code does what the finding says. In sourceIdle (internal/agent/verify.go ~902), when the pipeline_map topics and kafka_topic_offsets can be read, it returns true as soon as every data-topic end offset equals the proposal-time baseline. It never looks at the source slot or retained WAL on that path. retained_wal_bytes is only compared on the slot fallback, which runs only when the topics can't be read. A connector that reads RUNNING but produces nothing therefore looks the same as an idle source, even while the source keeps writing. That is the G17 case. The caller then takes `case !flow.fresh && flow.idle`, which sets recovered=true with "recovered: every layer check passed; no new source changes to confirm data flow". Before this change the verdict was "NOT recovered". The existing tests don't cover this. TestHeartbeatMovesSlotButDataTopicsUnchangedIsIdle only covers a slot moved by heartbeats. TestBusySourceStaleWarehouseStaysNotRecovered only covers the slot-fallback path. Reproduced with a throwaway test that was then deleted, so the worktree is clean. It uses the topic path with flat end offsets, a slot that stands still, retained WAL growing from 100 to 900, and a stale warehouse. The run produced "recovered: every layer check passed; no new source changes to confirm data flow." and the test failed because assertVerdict expected not-recovered. Severity stays medium. The failure needs a connector that is RUNNING but silent, and the layer checks must pass. A stuck connector would also stop heartbeats, so the slot would stand still while WAL grows. That is exactly the signal the topic path could have checked and doesn't. The result is a wrong "recovered" verdict that flows into the room, hand-off and post-mortem. It does not cause data loss or a security problem.
- Repro:
```
File internal/agent/zz_repro_test.go (deleted after the run):

package agent

import "testing"

func TestReproStuckConnectorTopicsFlatWALGrows(t *testing.T) {
	// connector RUNNING but producing nothing; source keeps writing: slot stands still, retained WAL grows.
	slots := slotsTool([3]any{"0/5000", "0/4000", 100}, [3]any{"0/5000", "0/4000", 900})
	msgs := runTopicCase(t, slots, topicOffsetsTool(t, 42))
	assertVerdict(t, msgs, false)
}

Command: go test ./internal/agent -run TestReproStuckConnector
Result: FAIL, "must not claim recovered, got [... recovered: every layer check passed; no new source changes to confirm data flow ...]"
```
