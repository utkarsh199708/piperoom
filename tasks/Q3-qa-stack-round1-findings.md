# Q3 — real-stack QA session 1 (on c4ee725): findings

Run on the real dev stack with the puppet model (cmd/qamodel). 15 of 19 G items pass; G11 and the propose_fix
approver list (finding S6) were fixed by wave 2 (Q2 F19, F9) after this run and only need re-checking.
Evidence: /tmp/claude-0/-home-user-piperoom/53bb11e1-975e-5bd6-9e2a-762a0f7a558e/scratchpad/qa1/ (notes.txt, server*.log, hook.log, shots/).

- **S1 (high, G1a)** After a server restart the watcher rebuilds a closed room as an incident with unknown
  signals ("cooldown … incident=\"unknown: closed before the watcher saw it\"") and silences EVERY signal of the
  pipeline for up to the cooldown (1 h); only one escalation after ESCALATE_AFTER gets out. Expected: an unknown
  incident never silences a signal that is firing now and was not firing at the close (or: recover the closed
  room's signals from the room log via the room service, e.g. its watcher evidence events). internal/watch
  incident.go / state.go.
- **S2 (high, G1b)** The escalation path ignores the join rules: it joined manual room r_01M4Z4XWWGJ6HSD4F2Z0ARMNT1
  (POST /api/rooms by arjun) and earlier an alert room whose title is a prompt injection. Expected: the same rules
  as a normal open (RoomInfoSink: recent, alert/watcher origin, or a person's room within JoinMaxAge). internal/watch.
- **S3 (high)** consumer_lag / consumer-group signals never fire in the real server: start-up logs "no
  consumer-group lag reader, consumer_lag … stay unknown"; consumer-lag broken > 3 min (lag > 1000) opened no room.
  cmd/server/tenancy.go watchClients never sets PipelineClients.Lag although kafka.Client.GroupLag exists.
- **S4 (medium, G2)** A webhook/alert room with no suspected layer (triage off or abstaining) is never auto-paged;
  only a debug log (internal/room/service_page.go ~709). Expected: fall back so someone is paged (alert label
  `layer`, else the pipeline's owners in owners.yaml order) and log at WARN when nobody can be paged.
- **S5 (medium, G6)** Idle source: verification says "NOT recovered: no new rows since the fix" while the watcher
  says all signals clear 2 s later. Expected: when the source had no change since the fix (compare the source's
  newest change / slot position), the data-flow check is inconclusive, not failed. internal/agent/verify*.go.
- **S6 (medium)** propose_fix result named only kafka-connect as approver (fixed by Q2 F9; re-check).
- **S7 (medium)** With Connect unreachable the watcher logs "check failed, signal state unchanged" and raises no
  signal, and slot_inactive/slot_wal, pg_replication_slots and pg_publication_tables cannot find the slot because
  they need the connector config. Expected: a Connect-unreachable signal (layer kafka-connect), and slot discovery
  that falls back to owners.yaml `watch.slot` or the pipeline's known slot when Connect is down; add `watch.slot`
  to dev/owners.yaml.
- **S8 (low)** what_changed missed RUNNING→PAUSED one minute after the pause ("no recorded change … 6h").
- **S9 (low, UI)** A closed room still shows "Arjun is driving" and a Driver chip; at 1440 px the pipeline-strip
  cards overlap ("schema-regis", "sumer" clipped); the "Who's here" layer chips overflow the right rail. Also
  (Q2 follow-up) the paged banner must match the viewer's groups against `group:<name>` owners.
- **S10 (low)** The page webhook carries no room link when PIPEROOM_PUBLIC_URL is unset (fall back to the
  server's own address for loopback/dev); the pipeline map shows warehouse FAIL "stale_for=5.5h" on an idle source
  while devctl health counts it OK (the map should not flag staleness when the source has no new changes).
