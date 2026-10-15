# Q5 — real-stack QA session 2 (on 08ba8ca): findings

All G1–G19 and S1–S6, S9 PASS on the real stack (puppet model). No high-severity bug. Evidence:
/tmp/claude-0/-home-user-piperoom/53bb11e1-975e-5bd6-9e2a-762a0f7a558e/scratchpad/qa2/ (notes.txt, server1–5.log, hook.log,
qamodel_turns.json, shots/). Carried over from wave 4: W1 and W2 at the end.

- **T1 (medium)** A fix approved after a server restart executes (action.executed ok) but is never verified (no
  agent.verification; the connector still failed; post-mortem "No verification recorded"). A resumed run (an
  @agent mention) starts with messages_count=1: only the alert + queued inputs; the earlier investigation,
  hypothesis and hand-off are gone. Expected: every executed proposal is verified (by the run that resumes, which
  must verify proposals of earlier runs of the room), or the room records it as "not verified" with the reason; a
  resumed run gets a recap of the room (hypothesis, steps' summaries, open proposals, hand-off) as its first
  context. internal/room/service.go (restart sweep / agent start), internal/agent/verify.go, run.go.
- **T2 (medium, S8)** The change feed is never written in the real server: watch.Deps in cmd/server/main.go (~502)
  never sets Changes, Registry or DDL; pipeline_changes stays empty after pause/resume/owner change/new schema
  version. what_changed then says "no recorded change … (recorded kinds: … connector_state …)", which misleads.
- **T3 (medium)** A second incident that joins an undriven room never pages its own layer: consumer_lag opened the
  room and kabir was auto-paged; connector_down joined 13 s later; kafka-connect (arjun) was not paged for 4 min.
  Expected: a joining incident whose layer has not been paged in the room gets its own auto page (same rate
  limits). internal/room/service_page.go.
- **T4 (low)** Post-mortem says "the fix was made outside Piperoom, per the resolution" for a room closed while its
  signal still fired (acked). The watcher-ack state (or "still firing" at close) must veto "fixed by a person".
- **T5 (low)** Post-mortem timelines omit owner.paged events and server restarts (agent restart notes).
- **T6 (low, S7)** With Connect down, pg_publication_tables still fails (reads the connector config). Fall back to
  owners.yaml/pipeline data and say Connect was unreachable, without showing other pipelines' data.
- **T7 (low, UI)** Watcher evidence overflows at 390 px: web/src/components/room/CollapsibleText.tsx `flex flex-col
  items-start` gives the clamp span its content width (701 px in a 330 px column).
- **T8 (low, security)** Bidi override characters (U+202A–202E, U+2066–2069) are accepted unflagged in titles and
  comments ("orders missing lag" renders reversed). Server: flag them in title_flags (injectscan) and refuse or
  neutralise them in typed text like C0 controls; UI: render titles and comments with the plainText helper.
- **T9 (low)** The notify webhook sends a flagged room title verbatim with no flag: add `title_flags` to the body.
- **T10 (low)** The crash-loop counter counts human fixes: three devctl break/fix cycles in 15 min fired
  "crash-looping (3 restarts in 15m)" on a RUNNING task 12 s after the fix. Restarts caused by approved actions or
  by a connector going from FAILED/PAUSED to RUNNING should not count. internal/watch/crashloop.go.
- **T11 (low)** An instruction meant for the model shows in the room: agent.message "follow-up: … Propose a separate
  fix that restores the level shown before "→"…" (internal/agent/verify.go). Keep model guidance out of room text.
- **T12 (low, UI)** Raw proposal ids (p_…) in agent prose are not linked; step ids are.
- **W1 (wave 4 gap)** The idle-source rule cannot see per-table changes: add a read-only tool (or field) with the
  pipeline tables' change counters (pg_stat_user_tables n_tup_ins/upd/del, scoped to the pipeline's tables) and
  use it in the verifier: tables unchanged since the fix → idle; changed but nothing in the topics → NOT recovered.
- **W2 (wave 4 gap, R7)** The web reducer should ignore a proposal.created after room.closed.
