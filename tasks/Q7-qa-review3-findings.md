# Q7 — QA review round 3 (08ba8ca..79ae28f): confirmed findings
R = reproduced with a failing test.

## V1. [medium, R] G1: After a server restart, a signal still firing in an alert room the watcher had joined is posted again as a new incident, which restarts the paused agent
- Where: `internal/watch/output.go:419`
- Scenario: An Alertmanager (or recent manual) room R for pipeline orders is open, and the watcher joined it with connector_down:pg-orders; its 'still firing' notes every 15 min keep R's LastEventAt fresh. The server restarts. Load pauses R's agent with 'server restarted — the agent is paused'. After the restart, ps.told is empty and ps.roomID is "". On the first tick where connector_down fires, emitPipeline goes to openRoom. LastRoomForKey(watcher:orders) finds no open watcher room, so reuse=false, and roomToJoin returns R (it is recent and came from an alert). Then `fresh, _ := w.pipes[id].untold(other, v.keys())` returns every key, so post(..., incident=true) calls PostIncidentJSON. The service sees NewIncident, and wakeForIncidentLocked/ensureAgentLocked starts a new agent run. The agent gets '[new alert signal] ... The earlier diagnosis may not cover it: investigate it.', and handleBatch clears recoveredIdle and forces a model turn. This happens for the same incident the room already carried, in every joined room, on every restart.
- Suggested fix: In the join branch, when the pipeline has no told entry for `other`, seed it the same way seedFromRoom does: call ClosedRoomKeysSink.RoomSignalKeys(other) and tell those keys before computing fresh. Add a harness test with q3Sink, a recent alert room that has keys set, and a fresh watcher, and assert the join is posted via 'evidence', not 'incident'.
- Verifier: Confirmed. In openRoom (internal/watch/output.go ~416-427) the join branch computes `fresh` from the pipeline's told list, which is empty after a restart, and never calls seedFromRoom or RoomSignalKeys. Only the own-room reuse path (`!created`) seeds the told list from the room service. So every key looks fresh and post(..., incident=true) goes to PostIncidentJSON. The service does not dedupe: postWatcherNote with newIncident=true writes an incidentNotePayload with NewIncident set and calls newIncidentLocked, which wakes or starts the agent. Service-side RoomSignalKeys reads the folded facts of any room, so the carried key is available and the seeding is possible. docs/api.md says a carried signal after a restart is a quiet repeat. A throwaway test used the q3 harness with an Alertmanager-opened room r_am that the room service says already carries connector_down:pg-orders, and a fresh watcher with no told list. The post into r_am arrived via "incident" instead of "evidence", and the test failed on that assertion. I deleted the test afterwards. Impact: an unnecessary new-incident wake or agent restart in each joined room after every restart, with a misleading '[new alert signal]' prompt. It costs model spend and noise but causes no data loss or security problem, and it happens once per joined room per restart. The test did not exercise the paused-agent state, which comes from the Load behaviour described in the finding. I read the wake path only (newIncidentLocked into wakeForIncidentLocked), so the reported effect on the agent rests on that read. Medium is right, arguably low-to-medium.
- Repro:
```
File internal/watch/zz_repro_test.go (deleted after the run):

func TestReproJoinedRoomAfterRestart(t *testing.T) {
	info := func(now time.Time) RoomInfo {
		return RoomInfo{RoomID: "r_am", AlertSource: "alertmanager", OpenedAt: now.Add(-time.Hour), LastEventAt: now.Add(-time.Minute)}
	}
	h1 := newHarness(t, opts{q3Sink: true})
	h1.q3.setInfo("orders", info(t0.Add(4*time.Minute)))
	h1.breakConnector("orders", "boom")
	h1.steps(4, time.Minute)
	p1 := h1.rooms.postsTo("r_am")
	keys := EvidenceKeys(p1[0].ev) // [connector_down:pg-orders]

	h := newHarness(t, opts{q3Sink: true}) // restarted watcher: no told list
	h.q3.setInfo("orders", info(t0.Add(4*time.Minute)))
	h.q3.setKeys("r_am", keys...) // room service already carries the key
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	posts := h.rooms.postsTo("r_am")
	if len(posts) != 1 { t.Fatalf("posts %+v", posts) }
	if posts[0].via == "incident" { t.Fatalf("carried keys %v re-posted as a new incident after restart", keys) }
}

Command: go test ./internal/watch -run TestReproJoinedRoomAfterRestart -v
Result: FAIL, "carried keys [connector_down:pg-orders] re-posted as a new incident after restart"
```

## V2. [medium, R] G1: A relapse of the same signal after the all-clear, in the still-open watcher room, is a quiet repeat: the agent parked as 'recovered' is never woken
- Where: `internal/watch/output.go:507`
- Scenario: connector_down:pg-orders opens watcher room W, and the agent proposes a fix. It is approved and executed, the verifier reports recovered, and the run parks (recoveredIdle, status done). The signal clears, resolve() posts the all-clear, and W stays open with no driver close yet. Twenty seconds later the same connector fails again for a different reason (e.g. the connector-auth break of QA G1a). connector_down fires again and openRoom runs. LastRoomForKey finds W open, so cooldownOf returns quiet{} and reuse=true. OpenAlertRoom joins W (service writes 'fired again' plus a quiet alert_update). In the watcher, inProcess=true, the keys are already in ps.told (resolve never clears told), so fresh is empty. stale is false because W is recent, so post(..., incident=false) goes through PostEvidenceJSON. That is pushQuiet, which never wakes the agent. Escalation does not apply either, because the signal is notified. The room shows the agent's 'recovered' verdict while the pipeline is down again, and nobody is paged. Only system lines appear, which is the same failure shape as QA G1(b).
- Suggested fix: Treat a signal that fires in a new episode (tFired after a tResolved, i.e. the room got the all-clear) as a new incident for the room: drop the resolved signals' keys from the room's told entry in resolve() (keep them for the close-time cooldown in a separate 'carried' set), or pass incident=true when the room's last watcher note was the all-clear.
- Verifier: Confirmed by code reading and a failing test at 79ae28f. resolve() (output.go ~733) clears the episode and room fields but leaves ps.told[W], as the watch.go comment says. When the same key fires again in the still-open room, openRoom takes the reuse path, and the service joins W with the quiet 'fired again' alert_update. In the watcher, inProcess is true and untold(W, keys) returns an empty list because the key is already in told. stale is false for a recent room. So the incident flag, `(known && len(fresh)>0) || stale`, is false and post() goes through PostEvidenceJSON, which reaches alertUpdateLocked and inbox.pushQuiet. On the agent side, waitForInput treats an alert_update without NewIncident as quiet: it is kept in r.held and never wakes the parked agent, and handleBatch only clears recoveredIdle when NewIncident is set. refireTriageLocked also skips when the agent is not idle or queued. The room therefore still shows 'recovered' while the pipeline is down again, and nobody is paged because the signal is already notified. This is a genuine gap in a plausible scenario: a relapse with a new cause after the all-clear in a room nobody has closed. A test of the existing 'resolved room still open' case covers only a different key re-firing. Severity is medium rather than high because the failing system is still alerting through system lines in the room, the agent wakes on any human steer, and the room stays open, so a person watching can still notice.
- Repro:
```
Throwaway file internal/watch/zz_relapse_test.go (deleted afterwards):

package watch

import (
	"testing"
	"time"
)

func TestRelapseSameKeyAfterAllClear(t *testing.T) {
	h := newHarness(t, opts{q1Sink: true})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	h.fixConnector("orders")
	h.steps(4, time.Minute) // resolved, room stays open
	h.breakConnector("orders", "auth failed")
	h.steps(6, time.Minute)
	posts := h.rooms.postsContaining("fired again in the open room")
	if len(posts) != 1 || posts[0].via != "incident" {
		t.Fatalf("relapse of same signal in open resolved room: posts=%+v, want one incident post", posts)
	}
}

Command: go test ./internal/watch -run TestRelapseSameKeyAfterAllClear -v
Result: FAIL. The one post to r1 ("... (fired again in the open room)") has via:evidence, not incident, and no new room is created.
```

## V3. [low, R] G3: Q5 T10 fix never applies on real Kafka Connect: human or approved task restarts still count as crash-loop restarts
- Where: `internal/watch/crashloop.go:76`
- Scenario: QA's T10 repro is `devctl break` / `devctl fix` three times in 15 minutes. On real Connect a failed Debezium task reads {connector.state: RUNNING, tasks[0].state: FAILED}, because the Connector object does not fail when its task does. devctl fix calls POST /connectors/pg-orders/tasks/0/restart (cmd/devctl/stack.go restartTask), and the approved connect_restart_task action calls the same endpoint (internal/clients/connect/connect.go:103). The connector state therefore stays RUNNING on every tick, so prevConn is RUNNING, `repaired` is false and byPerson is never set. restarts() counts all three FAILED->RUNNING changes, and connector_down fires 'task 0 crash-looping (3 restarts in 15m)' on a healthy RUNNING task right after the third fix. That is exactly the false page T10 reported.
- Suggested fix: Mark restarts that a person or an approved action caused from a source the watcher can see, e.g. the change feed / an action-executed hook for connect_restart_task, or a short 'repair window' after any connector state change. Change the fakes to the real shape (connector RUNNING, task FAILED) for the human-fix case.
- Verifier: The T10 fix in internal/watch/crashloop.go only marks a FAILED->RUNNING task change as byPerson when the connector-level state moves FAILED/PAUSED/STOPPED -> RUNNING. On real Kafka Connect a Debezium task failure leaves connector.state RUNNING. devctl's own waitConnector (cmd/devctl/stack.go:306) waits for Connector.State == "RUNNING" with Tasks[0].State == "FAILED", which confirms that shape. The fix path (devctl fix -> restartTaskSettled -> POST /connectors/x/tasks/0/restart) and the approved connect_restart_task action (connect.go RestartTask) both restart only the task. So `repaired` is never true, byPerson is never set, and every human or approved restart counts.

The unit test TestHumanFixesDoNotCountAsCrashLoop passes only because the fake failed() sets the connector state to FAILED. TestTaskRestartsByConnectStillCount uses the real shape and asserts that 3 restarts count, so it pins the unfixed behaviour. I reproduced the QA scenario with a throwaway test: three cycles of (task FAILED with connector RUNNING, then fixed), then 5 further one-minute ticks. connector_down reaches phase=firing and one room is created, which is the false page T10 reported. The throwaway test file is deleted.

Severity stays low. The result is a false-positive sev2 page on a healthy task after three break/fix or approved restarts inside 15 minutes. Nothing is lost and there is no security impact. It needs three restarts in the window, which is common in QA and demos but rare in production. The finding's second point (one-tick lag between the connector and task state changes) is secondary.
- Repro:
```
File internal/watch/zz_repro_test.go (deleted after the run):

package watch

import (
	"testing"
	"time"

	"piperoom/internal/owners"
)

func TestReproRealConnectHumanFixes(t *testing.T) {
	h := newHarness(t, opts{})
	h.step(0)
	for range 3 {
		h.conn.setStatus("pg-orders", taskFailed("pg-orders", "boom")) // connector RUNNING, task FAILED
		h.step(time.Minute)
		h.fixConnector("orders") // person restarts task 0
		h.step(time.Minute)
	}
	h.steps(5, time.Minute)
	if h.phase("orders", owners.SignalConnectorDown) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatalf("false crash-loop page: phase=%v rooms=%d", h.phase("orders", owners.SignalConnectorDown), len(h.rooms.created()))
	}
}

Command: go test ./internal/watch -run TestReproRealConnectHumanFixes -count=1
Result: FAIL: "false crash-loop page: phase=firing rooms=1"
```

## V4. [low, R] G2: A joined incident on a layer with no owners blocks every later joined incident's auto page
- Where: `internal/room/service_page.go:920`
- Scenario: Room setup: pipeline orders, single replica, PIPEROOM_PAGE_UNDRIVEN_AFTER=5m. owners.yaml has owners for consumer (kabir) and kafka-connect (arjun), but none for postgres.

Steps:
1. The watcher opens a room for consumer_lag.
2. At 5 min, kabir is auto-paged (autoPaged=true).
3. A new incident joins with first signal slot_inactive (layer postgres). joined[0] = postgres.
4. Seconds later another incident joins with connector_down (layer kafka-connect). joined[1] = kafka-connect.

What happens: on every 15 s check, autoPageDue loops over facts.joined. It returns the first joined layer not yet paged, which is postgres. autoPage calls page() with needOwners, and page() returns errNobodyToPage. autoPage logs one WARN (firstNoOwners, once per room) and returns. The next check picks postgres again. joined[1] is never reached, so arjun (kafka-connect) is never auto-paged while the room stays undriven, which can be its whole life (up to 14 days). Q5 T3 asked for exactly this page: 'a joining incident whose layer has not been paged in the room gets its own auto page'.

The same dead end exists one step earlier, and it was left over from Q4 R4's 'optionally' note. If the room's first watcher signal or triage layer has no owners, errNobodyToPage keeps autoPaged false forever. Two things follow:
- The fallback scan (fallbackLayer) never runs, because it is only used when layer == "".
- No joined incident is ever paged, because the joined branch needs autoPaged.

Example: a slot_inactive room with nobody on postgres is never paged, and when connector_down joins later, arjun is not paged either.

Why it matters: this is the G2 outcome ('nobody is told'), and it is silent after one WARN line, even though a layer with an on-record owner is in the room's own evidence.
- Suggested fix: In autoPageDue, skip joined layers that have nobody to page. One way: autoPage passes back the layers that returned errNobodyToPage (for example a per-room set in pagingState, cleared when owners appear), and the loop continues past them. Another way: have autoPage try the next joined entry after errNobodyToPage. For the opening page, after errNobodyToPage on a signal or triage layer, fall back to fallbackLayer as the layer=="" case does, and say so in the reason. Add a test where joined[0] has no owners and joined[1] does.
- Verifier: Confirmed at 79ae28f by reading internal/room/service_page.go and running two throwaway tests. I deleted the test file afterwards and the worktree is clean.

How the code behaves:
- autoPageDue returns the first unpaged joined layer, and autoPage then calls page() with needOwners: true.
- page() returns errNobodyToPage when nobody can be paged. No owner.paged event is written, so pageOf(layer) stays false.
- The next 15 s check picks the same joined entry again. Later joined layers are never reached.
- The opening page has the same dead end. A signal or triage layer with no owners is never paged. autoPaged stays false, so the joined branch never runs. fallbackLayer is only used when layer == "".
- Existing tests (TestAutoPageJoinedIncident, TestAutoPageToNobodyIsNotSpent, TestAutoPageFallbackNobody) cover only layers that have owners, or the case where no layer has anyone.

Reproduction: users are arjun (kafka-connect), meera (warehouse) and kabir (consumer), with no postgres owner. The undriven wait is 5 min.
1. TestReproJoinedNoOwnersBlocks:
   - A consumer_lag room is auto-paged to kabir.
   - A slot_inactive incident joins (postgres, no owners), then connector_down joins (kafka-connect).
   - After 5 more checks only the consumer page exists. The log shows one WARN with layer=postgres. kafka-connect and arjun are never paged.
2. TestReproOpeningNoOwners:
   - A slot_inactive room opens with nobody on postgres, and connector_down joins later.
   - After 5 checks there are zero auto pages, even though arjun owns kafka-connect.

Why only low severity:
- It needs a layer with no on-record owner and an undriven room. That is a configuration gap.
- It fails safe: it does not page the wrong person or lose data. It logs one WARN, and a human can still page manually or take the driver role.
- It does miss the G2 outcome ("nobody is told") and the Q5 T3 promise, in a room that can stay undriven for days. So it is a real defect and not a duplicate of an intended behaviour.
- Repro:
```
File internal/room/zz_repro_internal_test.go (package room; deleted after the run). It uses the existing helpers newPageRig, openWatcherRoom, incidentEvidence, autoPages and mustUsers.

func noPostgresUsers() *Users {
	return mustUsers([]User{
		{ID: "arjun", Name: "Arjun", Layers: []string{"kafka-connect", "kafka"}},
		{ID: "meera", Name: "Meera", Layers: []string{"warehouse"}},
		{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}},
	})
}

func TestReproJoinedNoOwnersBlocks(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.svc.users = noPostgresUsers()
	r.load()
	roomID := openWatcherRoom(t, r.svc, owners.SignalConsumerLag)
	c.Advance(5 * time.Minute)
	r.tick() // pages kabir (consumer)
	c.Advance(10 * time.Second)
	r.svc.PostIncidentJSON(ctx, roomID, "x", incidentEvidence(owners.SignalSlotInactive))   // postgres: no owners
	c.Advance(10 * time.Second)
	r.svc.PostIncidentJSON(ctx, roomID, "y", incidentEvidence(owners.SignalConnectorDown)) // kafka-connect: arjun
	for i := 0; i < 5; i++ { c.Advance(time.Minute); r.tick() }
	if got := autoPages(t, ms, roomID); len(got) != 2 { t.Fatalf("auto pages = %+v, want kafka-connect paged too", got) }
}

func TestReproOpeningNoOwners(t *testing.T) {
	// same rig; openWatcherRoom(..., owners.SignalSlotInactive); tick at 5 min;
	// join connector_down; 5 more ticks; expect len(autoPages) > 0
}

Command: go test ./internal/room -run 'TestRepro' -count=1

Result:
--- FAIL: TestReproJoinedNoOwnersBlocks: auto pages = [{Layer:consumer Owners:[kabir] ...}], want kafka-connect paged too. WARN room: auto page skipped: nobody to page for the layer ... layer=postgres
--- FAIL: TestReproOpeningNoOwners: nobody paged at all, though arjun owns kafka-connect
```

## V5. [medium, R] G9: withdraw_proposal only accepts proposals made in the current run, so a resumed agent cannot withdraw its own earlier open proposal
- Where: `internal/agent/tools_room_q1.go:277`
- Scenario: 1. The agent proposes fix p_X (open, waiting on kafka-connect, consumer and warehouse). 2. The run ends and a new one starts. This happens on a server restart followed by a steer or @agent (ensureAgentLocked -> startAgentLocked with fresh=false), on a replica takeover (adoptRoom -> startAgentLocked), or when the supervisor retries after a run error (attempt > 1 calls runGuarded again, and that builds a new runner). 3. The new run gets the room recap, which lists p_X as open and says 'Proposals with status open or approved are already proposed: do not propose them again'. 4. New evidence shows p_X is wrong, and the prompt (withdrawProposalGuidance) says to withdraw it. 5. withdraw_proposal{proposal_id: p_X} fails with 'proposal "p_X" was not created by you in this run ... (yours: none yet)'. The model may not re-propose and cannot withdraw. Its only move left is to ask approvers in prose to hold, while p_X stays approvable and runs once the last owner clicks Approve. That is the exact G9 failure, on every resumed run.
- Suggested fix: Let the server's byAgent check decide (it returns errNotOwnProposal, which is worded for the model), or seed `own` from the recap: in absorbRecaps, add each open or approved recap proposal that the agent created. The recap would need to carry the proposer, or the room could pass the agent-made open ids with the recap. Also change the tool description so it no longer says 'in this run'.
- Verifier: Confirmed at 79ae28f. newRoomTools (internal/agent/tools_room.go:45) creates a fresh ownProposals for every run, and only propose_fix calls own.add (tools_room.go:618). absorbRecaps (resume.go:165-187) puts recap proposals into the approval tracker (r.proposals) but never into `own`. The recap does carry RecapProposal.ByAgent (resume.go:89, filled from the actor kind in room/service_recap.go:183), so the agent could tell which earlier proposals are its own, but nothing uses it. withdrawProposalTool.Run therefore refuses any proposal made before the current run with 'was not created by you in this run'. The server already enforces the real rule: withdrawLocked with own=true refuses only when !byAgent. So only the agent-side gate blocks the withdrawal. The tool description, the existing test (TestWithdrawProposalRefusesProposalsOfOthers, which says 'made by the driver or by an earlier run') and the doc comment all say 'this run', so the narrow scope was deliberate. Treating 'earlier run' as 'not the agent's own' is the defect. Runs end and restart in normal operation (server restart, replica takeover, supervisor retry, shift change), and the recap tells the resumed agent that p_X is already proposed. With G9 as the spec ('the agent cannot withdraw or hold its own proposal'), the feature does not work after any resume. Impact is limited: the driver can still withdraw through the UI or API, and the agent can post a message asking approvers to hold. Also, a brand-new run is a narrower window than a whole incident, which is why this is medium and not high. Repro: a throwaway test in internal/agent (deleted afterwards) makes run 1 propose p_1, builds a second tool set on the same store (a resumed run), and calls withdraw_proposal for p_1. It fails with 'proposal "p_1" was not created by you in this run ... (yours: none yet)'. The test is a minimal stand-in for the real resume path (it does not go through recap parsing), but the production path leaves `own` empty in the same way.
- Repro:
```
// internal/agent/zz_repro_test.go (throwaway, removed)
package agent
import "testing"
func TestReproResumedRunCanWithdrawEarlierOwnProposal(t *testing.T) {
	store := &withdrawingProposals{}
	run1 := roomToolsWithActions(t, Deps{Proposals: store})
	proposeOne(t, run1, "restart the connector") // p_1, by the agent, run 1
	run2 := roomToolsWithActions(t, Deps{Proposals: store}) // restart / takeover: new run
	if _, err := runTool(t, run2["withdraw_proposal"], `{"proposal_id":"p_1","reason":"slot is gone"}`); err != nil {
		t.Fatalf("resumed run cannot withdraw the agent's own earlier proposal: %v", err)
	}
}
// command: go test ./internal/agent -run TestReproResumed
// result: FAIL: bad tool input: proposal "p_1" was not created by you in this run, and you can withdraw only your own (yours: none yet)
```

## V6. [low, R] G10: Q4 R8 fix only works when the resolution shares a word with the stale open/risk lines; otherwise the stale hand-off is still shown as current
- Where: `internal/room/postmortem.go:657`
- Scenario: The agent's proposal p_1 runs OK, then a hand-off lists Open 'Restart the consumer' and Risks 'lag keeps growing'. Kabir then fixes it another way and closes the room with the resolution 'Fixed by Kabir: raised max.poll.interval.ms, lag drained'. No fix note is logged. choosePostmortem picks source=handoff. fixEvidence returns the executed step, whose seq is below the hand-off's seq. The only upgrade to the resolution is gated on sharesWords(resolution, open+risks), and the two share no word of four or more letters. So stale=false. I replayed this in a scratch copy of the repo; the draft renders 'Resolution: Fixed by Kabir...' and then '## Still open - Restart the consumer' and '## Risks - lag keeps growing' as current lines with no 'Earlier, from the' label. What was done and Verification say nothing about Kabir's fix: Verification only says 'p_1: not verified: the room closed ... before any verification'. This is the original G10 contradiction, and it comes back for any resolution that does not repeat the hand-off's words.
- Suggested fix: Drop the sharesWords gate: when resolutionIsFix() and closedSeq > p.asOfSeq and the newest evidence is older than the summary, use the resolution as the evidence (what: 'resolution'). Also add the human close with a resolution to the 'work' that choosePostmortem compares against the hand-off seq.
- Verifier: At 79ae28f, internal/room/postmortem.go:657-660 only treats a human resolution as newer fix evidence when sharesWords(resolution, Open+Risks) is true. The existing R8 test passes only because its resolution ("restarted the consumer") repeats the word "consumer" from the hand-off's Open line. I took that test and changed the resolution to 'Fixed by Kabir: raised max.poll.interval.ms, lag drained', which shares no word of four or more letters with the Open/Risks lines. I ran it for both variants, an earlier executed step and an earlier fix note. Both fail. The draft prints 'Resolution: Fixed by Kabir...', then '## Still open - Restart the consumer' and '## Risks - lag keeps growing' as current lines, with no 'Earlier, from the' label. Verification only says 'p_1: not verified...' and says nothing about Kabir's fix. This is the contradiction that QA finding R8 (G10) was meant to remove. Nothing in the code or tests refutes it. The merged code narrowed tasks/Q4-qa-review2-findings.md R8's suggested fix (use the resolution whenever closedSeq > ev.seq) to a word-overlap heuristic. The code comment says a bare 'fixed' says nothing about the open lines, but this resolution is not bare: it names a different fix. fixEvidence (line 525) also treats the same resolution as fix evidence when no other evidence exists. The effect is a self-contradicting draft post-mortem for humans, which they can fix by editing. There is no security or data-loss risk, and it needs a narrow sequence: an earlier fix signal, a hand-off, then a human resolution with no shared words. So the severity is low, not medium. I deleted the throwaway test (a copy is at /tmp/claude-0/-home-user-piperoom/53bb11e1-975e-5bd6-9e2a-762a0f7a558e/scratchpad/zz_repro_test.go.txt).
- Repro:
```
Throwaway file internal/room/zz_repro_test.go (package room_test), a copy of internal/room/postmortem_r8_test.go with the test renamed TestZZReproDisjoint and the resolution changed. The only change that matters is the resolution text:

  app(t, st, c, id, events.RoomClosed, pmHuman("kabir", "Kabir"), events.RoomClosedPayload{Resolution: "Fixed by Kabir: raised max.poll.interval.ms, lag drained"})

The rest of the test is the existing R8 test: investigation, then either a successful action.executed for p_1 or a fix note, then handoffLog(h_1, staleSummary()), then room.closed, then postmortem.drafted with source handoff. It fails if Still open contains "- Restart the consumer" and there is no "Earlier, from the" label.

Command: go test ./internal/room -run TestZZReproDisjoint -v
Result: FAIL for both subtests (exec and fixnote) with "stale open item shown as current". The rendered draft contains "## Still open\n\n- Restart the consumer" and "## Risks\n\n- lag keeps growing".
```

## V7. [medium, R] G3: pg_publication_tables states false per-table PROBLEM about a connector user that is an unresolved config-provider reference
- Where: `/home/user/piperoom/internal/tools/dbtools/pg_publication.go:334`
- Scenario: The connector's database.user is `${file:/secrets/db:user}`, which Connect's REST API returns unresolved. The pipeline uses publication.autocreate.mode=filtered, and public.orders is owned by some role X. `PublicationSourceTables` is run with role `${file:...}`, which does not exist in pg_roles, so Owner=X and RoleMemberOfOwner=false. The per-table loop at the top of publicationFindings runs before the `${` guard and emits a `PROBLEM public.orders: connector user ${file:/s:u} is not the owner of this table (owner: X) and is not a member of that role...`. The summary line also prints `selectable=no, role_member_of_owner=no` and `REPLICATION=no, SUPERUSER=no`. Only afterwards does it add the UNKNOWN finding that says nothing can be checked. The agent is told the real connector is broken when the role was never looked up, and may propose or escalate a wrong fix.
- Suggested fix: At the top of publicationFindings, if the role contains `${`, return only the UNKNOWN finding. In publicationSummary and publicationData, omit the selectable, role_member and REPLICATION/SUPERUSER facts for such a role, as the connectDown branch already does.
- Verifier: Confirmed at 79ae28f. In publicationFindings (internal/tools/dbtools/pg_publication.go), the per-table loop runs before the strings.Contains(c.role, "${") guard. The postgres client guards role lookups with EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$2), so an unresolved reference like ${file:...} yields Owner=X and RoleMemberOfOwner=false. The loop then emits a PROBLEM (filtered mode) or NOTE (otherwise) that names the placeholder as the connector user and says it is not the owner. That is a false statement about a role that was never looked up, and it contradicts the file's own rule that "an unread fact is never a finding". The later UNKNOWN finding does not remove it. publicationSummary also prints selectable=no, role_member_of_owner=no and REPLICATION=no, SUPERUSER=no for the placeholder. The existing test passes nil tables, so this path is untested. Connect returning unresolved ${...} references is a normal setup (the code comments say so), so operators with secret-store-backed credentials and filtered autocreate would hit it. The effect is a misleading diagnosis that could drive a wrong fix proposal. It is not a security or data-loss risk, and the UNKNOWN line does follow, so medium is right and not high. I wrote a throwaway test and then removed it; it fails on the current code, and the working tree is clean.
- Repro:
```
File internal/tools/dbtools/zz_repro_test.go (package dbtools):

func TestReproUnresolvedRoleTableProblem(t *testing.T) {
	c := connectorConfig{role: "${file:/s:u}", publication: "dbz", autocreate: "filtered"}
	tables := []postgres.PublicationTable{{Schema: "public", Table: "orders", InPublication: true, Owner: "orders_owner"}}
	f := roleFacts{info: &postgres.PublicationInfo{Role: c.role, Owner: "dba"}}
	got := publicationFindings(c, []string{"public.orders"}, tables, f)
	for _, g := range got {
		if strings.HasPrefix(g, "PROBLEM") || strings.HasPrefix(g, "NOTE") {
			t.Errorf("stated a role fact about an unresolved reference: %s", g)
		}
	}
}

Command: go test ./internal/tools/dbtools -run TestReproUnresolvedRoleTableProblem -v
Result: FAIL. Findings were "PROBLEM public.orders: connector user ${file:/s:u} is not the owner of this table (owner: orders_owner) ..." followed by "UNKNOWN connector user ${file:/s:u} is a config-provider reference ...".
```

## V8. [low, R] G3: With Connect unreachable the tool assumes publication dbz_publication and returns a hard 'publication not found' error when the real publication has another name
- Where: `/home/user/piperoom/internal/tools/dbtools/pg_publication.go:169`
- Scenario: The connector config sets publication.name=pg_orders_pub, which is the usual case outside the dev stack. Connect's REST API times out or is down, so resolveConnector returns connectDownError and the tool falls back to defaultPublication="dbz_publication". `read(...)` then returns clients.ErrNotFound because that publication does not exist. The generic `if err != nil` branch at line ~181 turns it into a tool error, `dbtools: pg_publication_tables: postgres: publication "dbz_publication": not found`. The agent reads that the publication is missing, when Piperoom only guessed the name. This is the exact scenario the Q5 T6 fallback was written for: the room gets an error where it should get the soft 'name assumed' note. When the guess does exist (common in dev), `publication_tables` Data entries still carry `selectable:false` (no omitempty) because the read used role "". Nothing in the summary contradicts that, but the Data does.
- Suggested fix: When conn.connectDown and errors.Is(err, clients.ErrNotFound), return a normal Result whose summary says Connect was unreachable and no publication named dbz_publication exists (it may be named differently in the connector config). Do not return an error. In connectDown mode also drop `selectable`, owner and role fields from publication_tables in Data.
- Verifier: The defect is real in the code. When Connect is down, pg_publication.go falls back to defaultPublication ("dbz_publication") and clears the error. The read then goes to PublicationSourceTables, which returns `postgres: publication "dbz_publication": not found` wrapping clients.ErrNotFound when the real publication has another name. Only isUndefinedObject is special-cased, so ErrNotFound reaches the generic branch and becomes the tool error `dbtools: pg_publication_tables: postgres: publication "dbz_publication": not found`. The existing T6 tests (scope_meta_test.go, pg_publication_unobserved_test.go) only cover the case where the guessed name exists, so the guessed-but-absent case is untested. I reproduced it with a throwaway test and then deleted it.

I rate it low rather than medium for three reasons. It needs two conditions together: Connect unreachable and a non-default publication name. It produces a misleading error, not data loss or a security problem. And the agent still has other tools. The Data claim (`selectable:false` in publication_tables when the guess exists) is secondary and unverified. The soft summary path never states selectable, so that part is only a minor inconsistency. The likely fix is to treat ErrNotFound with connectDown as the soft "name assumed" note instead of a hard error.
- Repro:
```
File (throwaway, deleted after the run): internal/tools/dbtools/zz_repro_test.go

package dbtools

import ("context";"encoding/json";"errors";"fmt";"testing"
	"piperoom/internal/clients"; "piperoom/internal/clients/postgres")

type notFoundPubSource struct{ *pubSQLSource }

func (s *notFoundPubSource) PublicationSourceTables(_ context.Context, pub, _ string, _ []string) ([]postgres.PublicationTable, int, error) {
	return nil, 0, fmt.Errorf("postgres: publication %q: %w", pub, clients.ErrNotFound)
}

func TestReproConnectDownRealPublicationRenamed(t *testing.T) {
	src := &notFoundPubSource{&pubSQLSource{fakeSource: &fakeSource{pubTables: twoPipelinePublication()}}}
	d := metaDeps(src)
	d.Connect = &fakeConnect{configErr: errors.New("connect: connection refused")}
	r, err := scopedTool(t, d, "pg_publication_tables").Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil { t.Fatalf("hard error instead of soft note: %v", err) }
	t.Log(r.Summary)
}

Command: go test ./internal/tools/dbtools -run TestReproConnectDown -count=1
Result: FAIL: hard error instead of soft note: dbtools: pg_publication_tables: postgres: publication "dbz_publication": not found
```

## V9. [low] G13: heartbeat.interval.ms impact and preview say 'writes nothing to the source database' even when heartbeat.action.query is configured, and no postgres approval is required
- Where: `/home/user/piperoom/internal/actions/impact.go:128`
- Scenario: The dev connector (dev/scenarios.md, cmd/devctl/stack_test.go) has `heartbeat.action.query=UPDATE debezium.heartbeat SET beat_at = now() ...` and `heartbeat.interval.ms=10000`. The agent proposes connect_update_connector_config heartbeat.interval.ms=1000, which the allow-list floor accepts. Impact returns Layers=nil and RiskLevel=low, with the reason 'it writes nothing to the source database'. The preview Expected line carries no warning either. Only kafka-connect approves. Once applied, Debezium runs the human-set UPDATE on the source Postgres every second, a ten-fold increase in writes and WAL, with no postgres owner having approved. Proposing 0 silently turns off the heartbeat that fixes the G18 stuck slot.
- Suggested fix: In updateConnectorConfig.Preview, when key==heartbeat.interval.ms and the config has a non-empty heartbeat.action.query, say in Expected that the connector's own SQL then runs on the source database every N ms. Either add the postgres layer to Impact for that case or raise the floor (for example 10000). Drop the 'writes nothing' claim from the static Reason.
- Verifier: The code matches the finding, but the harm is much smaller than claimed. In internal/actions/impact.go (about line 128), configImpact decides from key and value alone. For any non-zero heartbeat.interval.ms it returns no layers, low risk and the reason "...it writes nothing to the source database." The caller (Impact in the same file) never reads the connector config. updateConnectorConfig.Preview in internal/actions/preview.go (line 141) does read the config, but its Expected line is only "key = value; Connect restarts the tasks", with no heartbeat note. The shipped dev connector (dev/connectors/pg-orders-connector.json, line 15, and dev/scenarios.md, line 69) sets heartbeat.action.query to an UPDATE of debezium.heartbeat. On that connector the "writes nothing" sentence is false, because Debezium runs that UPDATE on the source Postgres every interval. humanOnlyKeys only stops the agent from proposing the action.query key. It does not stop an interval change from multiplying a human-set query's frequency. The 1000 ms floor still lets interval=1000 through. The reason text and preview would show that false claim to approvers, and no postgres owner approves. Proposing 0 does have a correct reason string, "turns heartbeats off".

Why it is overstated: the write is one UPDATE of one row in a dedicated heartbeat table. The floor is deliberately 1 s. Heartbeat is designed to run at such rates, so a ten-fold increase is negligible WAL and write load. There is no security or data-loss path. The agent cannot set the query. Approval by the kafka-connect owner is intended for a connector-level setting. What remains is a factually wrong approval-card string and a missing preview note, a wording/approval-completeness flaw that an approver would only occasionally notice. I did not write a failing test. The behaviour is the current design: Impact has no config access, and impact_test.go line 39 asserts the "writes nothing to the source database" text. A test could only assert a different string, which would not demonstrate a real failure.

## V10. [low] G6: Header pill keeps saying "Fix ran · verifying" after a NOT recovered / inconclusive verdict
- Where: `web/src/lib/roomStatus.ts:88`
- Scenario: Replay: the connector fix runs, settle times out or the data-flow check fails, and the agent emits agent.verification {recovered:false}. The reducer stores it on the proposal, and ProposalsPanel shows the card "Verified · not recovered". statusInputFromState does not pass verification to roomStatus. roomStatus therefore still hits `if (approved.executed.length > 0 && agent !== 'done') return info('fixed','Fix ran · verifying','ok')`. On a NOT recovered or inconclusive verdict the agent never goes to state 'done', so the header pill and the other surfaces built on roomStatus keep showing a green "Fix ran · verifying" while the agent investigates or sits idle. The room says it is still verifying when the verdict is already in, and it is a failed verdict.
- Suggested fix: Pass the proposal's verification into StatusProposal. In roomStatus, when the newest approved proposal has a verification with recovered=false, do not return 'fixed'. Return a fail or warn status such as "Fix ran · not recovered", or fall through to the agent state.
- Verifier: The code path is as described, but I could not run a test (web/node_modules is missing and there is no Docker). I confirmed it by reading the code at 79ae28f. roomStatus (web/src/lib/roomStatus.ts:88) returns kind 'fixed' ("Fix ran · verifying", tone ok) whenever the newest approved proposal has executed steps and agent state is not 'done'. RoomStatusInput and statusInputFromState (lines 105-121) do not carry the proposal's verification. The reducer does store agent.verification on the proposal (reducer.ts:616-623). agentPill.ts:26 renders the 'fixed' kind as a green "Fix ran · verifying" pill. In verify.go (around line 382) only a recovered verdict emits AgentStatus done. A not-recovered verdict emits only the agent.verification event, so the pill stays on "Fix ran · verifying" after a failed verdict. The only thing I could not check is the agent's state after a not-recovered verdict. It is probably working or idle, and neither is 'done'. A throwaway test of roomStatus alone could not show the bug, because the input type has no verification field. I did not commit anything.

Severity is low, not medium, for three reasons. The label is stale and misleading, and it is green on a failed verdict. But the ProposalsPanel card does show "Verified · not recovered", and DecisionBar and other surfaces are not affected. Neither roomStatus.ts nor the 'fixed' branch of agentPill.ts changed in 08ba8ca..79ae28f; only a shadow-room line was added to agentPill.ts. So this gap predates the Q-wave change. It is more visible now because G6 is about the verdict.

## V11. [low, R] G6: Repeat-message window is not reset by an accepted suggestion, so the agent's reply to it can be dropped
- Where: `internal/agent/msgguard.go:102`
- Scenario: The agent posts "Task 0 is FAILED on a schema error; I suggest a compatibility change." A member sends a suggestion, and the driver forwards it. internal/room/service.go:1216 delivers it as HumanInput{Kind:"suggestion"}. hasHumanInput treats it as a person speaking, so the model runs. takeHeld -> endsRepeatWindow only recognises Kind "steer", "comment" and a new-incident alert_update, so the guard is not reset. If the model's answer has the same words as its previous message (a short status or recommendation is likely), guardedSink.Emit drops it as dropReasonRepeat. The person who made the suggestion gets no reply. This is the R16 failure the reset was added for, left open for the suggestion kind. post_message tells the model "message not posted: it repeats your previous message".
- Suggested fix: Add in.Kind == "suggestion" to the case that ends the repeat window. Add a test with a suggestion batch followed by an identical reply.
- Verifier: The gap is real. At commit 79ae28f, endsRepeatWindow in internal/agent/msgguard.go recognises only "steer", "comment" and a new-incident alert_update. A "suggestion" is not in that list. internal/room/service.go inputFor produces Kind "suggestion" for a forwarded suggestion. hasHumanInput treats it as a person speaking, so it wakes the model. takeHeld therefore never resets the guard for it. If the model's reply has the same words as its previous message and no ReplyTo, guardedSink.Emit drops it as dropReasonRepeat. The suggester gets no reply, which is the same failure the reset was added to fix.

I did not run the full drop path. My test confirms only that endsRepeatWindow returns false for a suggestion that hasHumanInput accepts. The existing TestOnlyHumanTurnsResetTheGuard has no suggestion case, so nothing covers this.

I rate it low, not medium, for three reasons. The agent must produce byte-identical text after a suggestion, and a reply to a suggestion usually differs from the previous status. The failure is a dropped chat message, and the model is told the post was not accepted. No data loss or security risk is involved. The fix is one word: add "suggestion" to the case.
- Repro:
```
File internal/agent/zz_repro_test.go (throwaway, deleted afterwards):

package agent

import "testing"

func TestSuggestionEndsRepeatWindow(t *testing.T) {
	in := []HumanInput{{Kind: "suggestion", Text: "try compat change"}}
	if !hasHumanInput(in) {
		t.Fatal("suggestion should be human input")
	}
	if !endsRepeatWindow(in) {
		t.Fatal("suggestion does not end the repeat window")
	}
}

Command: go test ./internal/agent -run TestSuggestionEndsRepeatWindow
Result: FAIL, "suggestion does not end the repeat window"
```

## V12. [low] G6: Web labels inconclusive and not-verified verdicts "Verified · not recovered" / "Verification: not recovered"
- Where: `web/src/components/room/VerificationCard.tsx:81`
- Scenario: The agent emits agent.verification with recovered=false and verdict "inconclusive: no source changes observed; connector may not be capturing". It does the same for notVerifiedEarlier (resume.go:371), with verdict "not verified: the proposal was made by an earlier run ... could not load its steps". The card heading renders "Verified · not recovered", and the activity row (describe.ts:263) renders "Verification: not recovered". For inconclusive the layer checks all passed and the agent did not conclude that the pipeline is not recovered. For not-verified no check ran at all. The heading says "Verified" for a case where nothing was verified, and "not recovered" for an unknown. The payload has no field for the third state, so only the verdict prefix can tell them apart.
- Suggested fix: Derive a verdict word on the web from the verdict prefix ("inconclusive", "not verified"). Use neutral headings such as "Inconclusive" and "Not verified", and use the same wording in describe.ts. Alternatively add an explicit field to AgentVerificationPayload.
- Verifier: The code matches the finding at 79ae28f. VerificationCard.tsx:81 and describe.ts:263 map `recovered` to a boolean and never read `verdict`. The agent emits `recovered:false` for the third states. `inconclusive` comes from verify.go (inconclusiveHead/word()). `not verified: ...` comes from resume.go:370-377 (notVerifiedEarlier), which runs no checks and sets DataFlow.Checked=false. The card therefore shows a destructive-styled "Verified · not recovered" heading, and the activity row shows "Verification: not recovered". Those labels are wrong for "no check ran" and "unknown". The inconclusive state and notVerifiedEarlier were added in this diff range. The web files already used this boolean mapping in 08ba8ca ("Verified · ..."), and the diff does not touch them for this case. Mitigations: the full verdict line ("inconclusive: ..." or "not verified: ...") is rendered directly under the heading, and the data-flow line says "Data flow not verified: ...". The "Close room" nudge only shows when ok is true, so it is not triggered wrongly. The defect is a cosmetic or wording mismatch with no security or data-loss risk. I did not write a failing test: that would only assert on the rendered heading text and prove nothing the code reading doesn't already show.

## V13. [low, R] G7: Settled non-incident rooms are cached forever, so a later label never makes them eligible
- Where: `internal/tools/similarincidents/tool.go:527`
- Scenario: A human-fixed room closed with a resolution and no agent hypothesis is first indexed more than 24h after it closed. It fails isIncident and is cached in `kept`. Later a person labels it with POST /api/rooms/{id}/label, which only works on closed rooms. The reuse condition `old.fetched.Sub(*r.ClosedAt) >= settledAfter` is true, so the stale pastRoom is reused on every rebuild. The label, cause and fix_that_worked are never read and isIncident stays false. The room never shows up in similar_incidents or 'Seen before' until the process restarts or the pipeline entry is evicted. The same applies to the label text of rooms that were already included. Before the non-incident filter, only included rooms were cached, so this staleness is now worse.
- Suggested fix: Reuse the cached room only when `old.Events == int(r.LastSeq)` as well as the settled-time check. A new room.labelled bumps last_seq, which forces a re-read.
- Verifier: Confirmed in internal/tools/similarincidents/tool.go build(). The settled-room reuse check (old.fetched.Sub(*r.ClosedAt) >= settledAfter) never compares the cached pastRoom.Events with the current r.LastSeq. A room first indexed more than 24h after it closed, with a resolution but no agent hypothesis and no label, fails isIncident and is cached in `kept`. When a person later labels it (room.labelled appends an event, and labelling is only allowed on closed rooms), the stale copy is reused on every rebuild, so the room never becomes eligible. This lasts until the process restarts or the pipeline entry is evicted. The repro test below fails on the current code. As a control, the same test passes when the first index is built 1h after close (not yet settled), so the failure comes from the settled cache. Severity is low, not medium. The impact is that one tool misses a past incident, with no security or data-loss risk. It needs a narrow case: a human-fixed room with no hypothesis, first indexed more than 24h after close, then labelled. The existing test TestLateLabelIsPickedUpButSettledRoomsAreNotReread shows settled rooms not being re-read is intended, but only unsettled rooms are tested for late labels. Label staleness on already-included settled rooms was pre-existing. The G7 change widens it by also caching rejected rooms. The fix would be to also reuse the cached room only when r.LastSeq equals old.Events. I wrote the repro as a throwaway test and removed it, so the worktree is clean.
- Repro:
```
File internal/tools/similarincidents/zz_repro_test.go (package similarincidents_test, uses existing helpers day, run, scoped, input, hitIDs):

func TestReproLateLabelOnSettledNonIncident(t *testing.T) {
	ctx := context.Background()
	st, _ := fake.NewStore(ctx, fake.Room{ID: "r-bare", Title: "orders bare", ClosedAt: day(9, 1), Resolution: "Restarted the connector.", Steps: 6})
	now := day(9, 1).Add(30 * time.Hour) // first indexed >24h after close
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st), Now: func() time.Time { return now }, TTL: time.Minute})
	res, _ := run(t, tl, scoped("orders"), input("orders bare", 3))   // no hits: not an incident, cached
	st.Append(ctx, "r-bare", events.RoomLabelled, events.Actor{Kind: events.ActorHuman, ID: "a", Name: "A"},
		events.RoomLabelledPayload{Cause: "zzcause stopped task", FixThatWorked: "restart"})
	now = now.Add(5 * time.Minute)
	res, _ = run(t, tl, scoped("orders"), input("zzcause", 3))
	// want hits == ["r-bare"]; got []
}

Command: go test ./internal/tools/similarincidents -run TestReproLate -count=1
Result: FAIL "labelled room not found after label: []". With the first build at +1h instead of +30h the test passes.
```

## V14. [medium, R] G2: Joined-incident auto page (Q5 T3) gets stuck on the first unpaged layer that has no owners, so layers that do have owners are never paged
- Where: `internal/room/service_page.go:919`
- Scenario: Single replica, PIPEROOM_PAGE_UNDRIVEN_AFTER=5m. owners.yaml for pipeline orders lists approvers for consumer and kafka-connect (arjun) but nobody for postgres. The watcher opens room r1 for consumer_lag, and the consumer layer is auto-paged. slot_inactive then joins r1 as a new incident (layer postgres), and later connector_down joins (layer kafka-connect). On every 15 s check, autoPageDue walks facts.joined in log order and returns postgres, the first joined layer with no page. page() resolves pageOwners(postgres) = [] and, with needOwners set, fails with errNobodyToPage. autoPage logs one WARN (firstNoOwners is per room) and returns. The next tick picks postgres again, so kafka-connect is never reached and arjun is never paged while the room has no driver. That is the QA T3 failure again (a connector_down joining an undriven room never pages kafka-connect). A second form: when the room's own first signal points at a layer with no owners, autoPaged never becomes true. The initial branch then retries that layer forever, because there is no fallback when layer != "", and no joined incident is ever paged.
- Suggested fix: In autoPageDue's joined loop, skip layers with no page owners (check pageOwners, or keep a per-room set of layers that answered errNobodyToPage, as noOwners does for rooms). Then continue to the next joined layer. In the initial branch, fall back to fallbackLayer when the signal or triage layer has nobody to page, so the room's first auto page can land and the joined pages are unblocked.
- Verifier: Confirmed in internal/room/service_page.go at 79ae28f. autoPageDue (line 919) returns the first joined layer that has no page, with no check that anyone owns it. autoPage calls page() with needOwners=true. page() returns errNobodyToPage when pageOwners is empty, and autoPage only logs one WARN. Nothing records that the layer has nobody, so every 15 s check picks the same layer again. The page's check closure (line 860) also pins the choice to the first layer. A later joined layer that does have owners is therefore never reached. This is the QA T3 failure again for a room that joins an owner-less layer first. The second form also holds. When the room's first signal layer has no owners, `fallback := layer == ""` is false, so no fallback runs and autoPaged never becomes true. Severity is medium. It needs owners.yaml with a layer nobody owns, which is a misconfiguration, and it only affects undriven rooms. The result is silent: owners of other layers are never paged, and only a single WARN is logged.

Repro: I wrote a throwaway test, used the existing newPageRig helpers and removed it from the worktree. Its users were arjun (kafka-connect, kafka) and kabir (consumer), with nobody for postgres. consumer_lag opens the room, and consumer is auto-paged correctly. Then slot_inactive joins (postgres), then connector_down joins (kafka-connect). After three more ticks only the consumer page exists. The test fails with "auto pages = [consumer ...], want kafka-connect paged". The log shows "auto page skipped: nobody to page for the layer ... layer=postgres". TestAutoPageJoinedIncident does not catch this because it only joins layers that have owners.
- Repro:
```
File: internal/room/zz_repro_test.go (saved copy at /tmp/claude-0/-home-user-piperoom/53bb11e1-975e-5bd6-9e2a-762a0f7a558e/scratchpad/zz_repro_test.go)
Command: go test ./internal/room -run TestReproNoOwnerLayerBlocksJoined -count=1

func TestReproNoOwnerLayerBlocksJoined(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.svc.users = mustUsers([]User{
		{ID: "arjun", Name: "Arjun", Layers: []string{"kafka-connect", "kafka"}},
		{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}},
	})
	r.load()
	roomID := openWatcherRoom(t, r.svc, owners.SignalConsumerLag)
	c.Advance(5 * time.Minute)
	r.tick()
	if got := autoPages(t, ms, roomID); len(got) != 1 || got[0].Layer != "consumer" {
		t.Fatalf("opening page = %+v", got)
	}
	c.Advance(13 * time.Second)
	if err := r.svc.PostIncidentJSON(ctx, roomID, "slot", incidentEvidence(owners.SignalSlotInactive)); err != nil {
		t.Fatal(err)
	}
	c.Advance(13 * time.Second)
	if err := r.svc.PostIncidentJSON(ctx, roomID, "conn", incidentEvidence(owners.SignalConnectorDown)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		c.Advance(15 * time.Second)
		r.tick()
	}
	got := autoPages(t, ms, roomID)
	if len(got) != 2 || got[1].Layer != "kafka-connect" {
		t.Fatalf("auto pages = %+v, want kafka-connect paged", got)
	}
}

Result: FAIL, auto pages = [{Layer:consumer Owners:[kabir] ...}], want kafka-connect paged
```

## V15. [low, R] regression: A forwarded suggestion does not end the message guard's repeat window, so the agent's answer to it can be silently dropped
- Where: `internal/agent/msgguard.go:102`
- Scenario: The agent posts "pg-orders is RUNNING; lag is 0". A watcher then suggests "is pg-orders running now?" and the driver forwards it. The room delivers it as HumanInput Kind "suggestion" (inputFor, service.go, from SteerSentPayload.From). endsRepeatWindow only resets the window for "steer", "comment" and new-incident alert updates. The guard therefore still remembers the last message, and the agent's identical answer is dropped as a repeat. The suggester and the driver get no reply. This is the Q4 R16 failure ("a second person asking the same thing got no reply"). It came back through the new "suggestion" kind that this same diff introduced (d8a617c), because the R16 fix (f0cbcce) does not list that kind.
- Suggested fix: Add `in.Kind == "suggestion"` to endsRepeatWindow's first case.
- Verifier: Confirmed. Before the diff (08ba8ca), a forwarded suggestion reached the agent as Kind "steer", which ends the repeat window. This diff (d8a617c) made inputFor emit Kind "suggestion" when SuggestionID and From are set. endsRepeatWindow (msgguard.go) only resets the window for "steer", "comment" and new-incident alert updates. After a forwarded suggestion, the guard still holds the last message. An identical agent answer, such as "pg-orders is RUNNING; lag is 0", is dropped with "it repeats your previous message". A suggestion carries no CommentID, so ReplyTo is empty and the key matches. The suggester and driver get no reply, which is the Q4 R16 failure returning. Severity stays low. It needs the answer to be word-for-word identical to the previous message, and the model sees a "message not posted" result and can reword. The fix is one line: add `in.Kind == "suggestion"` to the first case of endsRepeatWindow. I reproduced it with a throwaway test (since deleted): drive runner.takeHeld with a "suggestion" HumanInput, then re-emit the same AgentMessage through the guardedSink. The re-emit returns the repeat drop.
- Repro:
```
File internal/agent/zz_repro_test.go (package agent), deleted after the run:

type nopSink struct{ n int }
func (s *nopSink) Emit(ctx context.Context, typ events.Type, payload any) error { s.n++; return nil }

func TestReproSuggestionEndsWindow(t *testing.T) {
	inner := &nopSink{}
	g := newGuardedSink(inner)
	r := &runner{deps: Deps{Sink: g}}
	msg := events.AgentMessagePayload{Text: "pg-orders is RUNNING; lag is 0"}
	if err := g.Emit(context.Background(), events.AgentMessage, msg); err != nil { t.Fatal(err) }
	r.takeHeld([]HumanInput{{Kind: "suggestion", Text: "is pg-orders running now?"}})
	if err := g.Emit(context.Background(), events.AgentMessage, msg); err != nil {
		t.Fatalf("answer to forwarded suggestion dropped: %v", err)
	}
}

Command: go test ./internal/agent -run TestReproSuggestionEndsWindow
Result: FAIL, "answer to forwarded suggestion dropped: agent: message not posted: it repeats your previous message"
```
