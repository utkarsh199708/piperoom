# L1 — Learn from closed rooms: labelled rooms become eval cases and worked examples

Owners: **L1a** C6 (`evals/**`) · **L1b** A4 (`internal/agent/**`) · **L1c** C1 (`.github/**`) with C6 (`evals/**`).
Model: L1a Sonnet 5.5 (a mapping from one JSON shape to another, table-driven tests); L1b Opus 5.5 (it puts
text from past rooms into the prompt, so the untrusted-data rules apply); L1c Sonnet 5.5 (one workflow job).

Read `AGENTS.md`, `docs/collab.md`, `evals/README.md` (scenario format, phases, `-baseline`, held-out set),
`internal/tools/similarincidents/room.go` (what a closed room already yields) and `docs/design/shadow.md`.

## Why

A closed room already holds everything an eval case needs: the alert (`room.created`), every tool call with its
exact input (`agent.tool_call`) and result (`agent.tool_result`), the steers, the executed fix and its
verification (`agent.verification`), and two human judgements: `hypothesis.verdict` (confirmed/disputed) and
`room.labelled` (`actual_layer`, `cause`, `fix_that_worked`). Today that record is used only by
`similar_incidents` (a lookup the agent may or may not make) and `agent-stats` (a hit rate). Nothing turns a
room into a regression case, and CI never calls the model, so a prompt or model change ships unmeasured.

Two outcomes: (1) a labelled room becomes a replayable scenario in `evals/scenarios/learned/`, after a person
reviews it; (2) at room start the agent sees the two or three most similar past incidents as worked examples,
with their confirmed cause and fix.

## Guardrails (these decide the design; do not trade them away)

1. **Only human-confirmed rooms teach.** Eligible: closed, `room.created.shadow == false`, last `room.labelled`
   has `cause` and `actual_layer`, and at least one `hypothesis.verdict` is `confirmed`. If a fix was executed,
   its `agent.verification` must say `recovered: true`. A room with only `disputed` verdicts is exported as a
   *hard* case (see L1a) and never as an example.
2. **No customer data in git. This repository is public**, so learned scenarios never live in it, redacted or
   not: table, connector, topic and host names describe a customer's systems. The exporter requires `-out DIR`
   and refuses a DIR inside the git working tree; the learned set lives in a private store (a private repo
   checkout or a local folder) that `evalrun -learned DIR` reads. `evals/scenarios/learned/.gitignore`
   (`*`, `!.gitignore`) stops an accidental commit. Inside the private store the same rules hold: every text the
   exporter writes (summaries, `data`, steers, comments, labels) goes through `redact.Text`; it refuses to
   write a file where a second `redact.Text` pass changes anything; `data` larger than 4 KB is dropped (the
   model reads `summary`, not `data`). Export lands in `<store>/pending/`, which `evalrun` never replays; a
   person moves the file into `<store>/` after reading it. The move is the review.
3. **Examples are data, not instructions.** Past-room text enters the prompt only through the agent's
   untrusted wrapper, and only the fields a person typed or confirmed (`cause`, `fix_that_worked`,
   `actual_layer`, the confirmed hypothesis text). Never the hand-off, never tool-result text.
4. **No leakage between retrieval and evals.** `evalrun` registers no `similar_incidents` tool (wiring only
   adds it when `w.Rooms` is set, which the eval harness never does), so an eval cannot pass by copying its own
   answer. A scenario that wants to test the example path records `similar_incidents` results explicitly.
5. **De-duplicate and cap.** Fingerprint = pipeline id + `actual_layer` + the sorted root-cause terms of
   `cause` (tokenised as `evals/textmatch.go` does). The exporter refuses a fingerprint that already exists
   in `learned/` or `learned/pending/` unless `-force`; at most 10 learned scenarios per pipeline.

## L1a — exporter (C6, `evals/cmd/roomcase/`, `evals/learned.go`)

`go run ./evals/cmd/roomcase -export room.json -owners owners.yaml -out DIR [-force]` reads one room
export (`GET /api/rooms/{id}/export`, the integrity-hashed JSON; the tool verifies the hash first) and writes
`DIR/pending/<pipeline>-<yyyy-mm-dd>-<short room id>.yaml`. DIR is the private store (guardrail 2). No database access: the
export is the input, so this runs anywhere, including on the on-call engineer's laptop.

Mapping (every field of the existing scenario schema; add no new fields):

| scenario field | from |
|---|---|
| `name`, `description` | file name; `room.created.title` + the label's `cause` (redacted) |
| `pipeline` | the `owners.yaml` entry for `room.created.pipeline_id` (`id`, `topic_prefix`, tables, connectors, groups) |
| `alert.title`, `alert.pipeline_map` | `room.created`; the topology text from the first `pipeline_map` tool result |
| `recordings[]` | each `agent.tool_call` joined to its `agent.tool_result` by `tool_use_id`: `tool`, `input` = the call's `Input` (dropped keys: `since`, `limit`, timestamps — anything the model picks freely; the list lives in one table in `learned.go`), `summary`, `data` (≤ 4 KB), `truncated`, `error` when `is_error` |
| `phases`, `transitions`, `inputs[]` (`action_result` with `action`) | when a proposal was executed and verified: `[broken, recovered]`; results after the first successful action step get `phase: recovered`; the executed steps become `action_result` inputs, as README §"Phases, sequences and executed fixes" describes |
| `inputs[]` (`steer`, `comment`) | `steer.sent`, `comment.added` (redacted, `from` = display name only) |
| `expect.outcome` | `found` when a verdict is `confirmed`; `abstain` for a hard case |
| `expect.root_cause_layer` | `room.labelled.actual_layer` |
| `expect.root_cause` | term groups from `cause`: identifiers, numbers and the three most specific words, each group one term; terms that also appear in the alert title are dropped (they are "lazy" matches) |
| `expect.fix` | the executed proposal's action names, when `fix_proposal_id` is set |

Two calls to the same tool with the same `input` in one room keep the sequence (`sequence:` in the
recording), so a stack that changed under the agent replays the same way.

Hard cases: a room whose every verdict is `disputed` exports with `expect.outcome: abstain` and the disputed
hypothesis's root-cause terms as **denied** terms (the README's denial rules), i.e. the agent must not assert
that cause. Such a file names `hard: true` in its `description` line, not in the schema.

Tests (unit, no network): a fixture export under `evals/testdata/rooms/` built with the existing
`evals/gen` fakes; table-driven mapping tests; a test that a planted `password=` in a summary never reaches
the file; a test that the second `redact.Text` pass is a no-op on every written string; the dedup refusal;
the per-pipeline cap; a shadow room and an unlabelled room are refused with a clear error; a hash mismatch is
refused; an `-out` inside the working tree is refused. `evalrun -learned DIR -validate` checks the store and
`-check` also covers `DIR/pending/` (so a pending file is at least well-formed), without ever replaying it.

README: a "Learned scenarios" section: where they live (a private store, never this public repo) and why, how to export, the review checklist (the reviewer reads every
`summary` and `input`, confirms no row values, DSNs or names survive, and that `expect` says what the person
on the room would have said), and the rule that `learned/` is never tuned on by hand: a wrong expectation is
fixed by re-labelling the room and re-exporting.

## L1b — worked examples at room start (A4, `internal/agent/**`)

When the runner starts a room (not the triage pass, not the post-mortem), it calls the registered
`similar_incidents` tool once with the alert title and pipeline id, takes at most three results that have
a `cause` typed by a person and (when present) `fix_that_worked`, and renders them as one `Past incidents on
this pipeline (human-confirmed)` block in the first user turn, each as:

```
- <closed date> · <title> · layer <actual_layer> · cause: <cause> · fix: <fix_that_worked or "none recorded">
```

Rules: every field goes through the untrusted wrapper; the block is capped at 1,200 bytes; a tool error or
no `w.Rooms` yields no block and no log line above debug; the prompt says these are examples of what was
true before, not evidence about this room, and that the agent still has to cite steps from this room. The
hypothesis grounding (F3) is unchanged: a term that only appears in an example is still unbacked.

The triage pass (F6c) gets the same block, capped at 400 bytes, because time-to-first-insight is where a
past case pays most.

Tests: fake `similar_incidents` results with an injection string in `cause` (it must arrive wrapped and
escaped); the cap; the no-tool path; the usage event's token count grows by the block (so cost stays visible).
One eval scenario, `learned-example-used.yaml` (C7 writes it; it records a `similar_incidents` result), checks
the agent reaches the cause in fewer steps with the example than without (`ComparePaired` over the two).

## L1c — evals in CI (C1 `.github/workflows/ci.yml`; C6 `evals/baseline/`)

A new job `evals-live`, separate from the existing jobs, that:

- runs on `pull_request` when the diff touches `internal/agent/**`, `internal/wiring/model.go`,
  `internal/config/config.go` (model defaults), or `evals/scenarios/**`; on `schedule` nightly (the hosted
  model can change without a commit); and on `workflow_dispatch`;
- needs the `ANTHROPIC_API_KEY` repository secret and skips with a visible notice when it is absent (forks);
- runs `go run ./evals/cmd/evalrun -runs 3 -label ci -baseline evals/baseline/default.json -out cand.json`
  and uploads `cand.json` as an artifact;
- fails when `ComparePaired` reports a regression on any scenario at the README's confidence level, or when
  `misses` grew; never fails on cost alone, but prints the cost delta;
- has `concurrency` so a second push cancels the running eval, and `timeout-minutes: 30`.

Public repository rules for this job: never `pull_request_target`; run only when
`github.event.pull_request.head.repo.full_name == github.repository` (fork PRs get no secret anyway, and must not
spend the key); put the job in a GitHub Environment `evals` holding `ANTHROPIC_API_KEY`, with the owner as
required reviewer for PR runs, so no PR spends money without approval (the nightly run is approved by its
schedule); upload only the report of the committed synthetic scenarios (logs and artifacts are public). The
learned set runs from the private store, never in this repository's CI.

Baseline: `evals/baseline/default.json` is the report of `main` on the pinned model, refreshed in the same PR
as any *intended* prompt change (the PR author runs it and commits it; the lead reads the diff of the report,
not just the code). The held-out set stays out of CI and out of the baseline: it is run by hand before a
release and never tuned on.

Cost: C6 measures one full run (`-runs 3` over the default set) and writes the number into the README and
the PR. If it exceeds $5, the job runs the learned and schema-change sets on PRs and the full set nightly.

## Not in this task

Retraining or fine-tuning anything; automatic promotion from `pending/` to `learned/`; using disputed
rooms as examples; exporting from the database directly (the export route is the interface on purpose);
cross-pipeline examples (a past incident on another pipeline is a different system; `similar_incidents`
already scopes by pipeline).

## Dependencies and merge order

L1a and L1b start in parallel: L1b reads `similar_incidents`, not the exported files, so it does not wait for
the exporter. L1c starts when L1a is merged (the baseline report needs `learned/` to exist). L1c's job is added
disabled on PRs (`if: false` on the PR trigger) until the baseline file is in `main`, then one more C1 PR turns it
on. `learned-example-used.yaml` (C7) follows L1b.

## Done when

- A labelled room from the dev stack (`schema-change`, closed and labelled through the UI) exports to a
  pending file in a private store that `-check` accepts, a person moves it, and `evalrun -learned` replays it
  with the recorded cause found in every run. Nothing under `evals/scenarios/learned/` is tracked by git.
- A planted secret in any room text never appears in the exported file (test).
- A room opened on `orders` after that shows the past case in the agent's first turn, wrapped, and the triage
  headline cites the right layer in under 40 s on the dev stack.
- A prompt change that breaks `schema-change` fails the `evals-live` job on its PR with the scenario named.
