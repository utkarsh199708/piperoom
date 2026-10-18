# P2 — The ceremony scales down: derived solo, sole-member driver, unowned layers, policy auto-approve

Owners: P2a A3 · P2b A5 · P2c A6 · P2d A4 · P2e C12.
Model: **Opus 5.5** for P2b (changes who may approve and when a fix runs: the product's safety core) and P2c
(the approval card is the signature interaction); **Sonnet 5.5** for P2a, P2d, P2e.
Plan: `docs/product/fit-2026-10.md` §1.4, §2 row 2. Uses P0 items 2, 3, 4, 6.

## Goal
A two-person data team gets the same record and the same safety, with no theatre:
- when one person owns every layer a proposal needs, the card shows one button, **Approve and run**, that
  records one `approval.given` per layer and runs;
- a room from an alert whose first member walks in is theirs: they are the driver, no "request control";
- a layer nobody owns does not make a proposal un-approvable: the driver approves it, visibly marked;
- `auto_approve: [connect_restart_task, connect_resume_connector]` on a pipeline lets those steps run after a
  60 s countdown that anyone in the room can cancel, and the room log says a policy approved it.

The big-team ceremony is unchanged when several people own the layers.

## Evidence
- `internal/room/service_proposals.go:142-152`: required layers = union of step layers; `ApproveProposal`
  (`:214`) checks one layer per call; `authorizeLayer` (`access.go:278`) already grants `"solo"` for
  `PIPEROOM_SOLO_USER`, with a typed confirm (`docs/design/auth.md` §5.3). One person owning several layers
  already approves them one call at a time: the server allows it, the UI makes it four clicks and four confirms.
- `docs/api.md` row 370: `control/grant` is "anyone if no driver": an alert room has **no** driver until someone
  requests and grants to themselves.
- `owners.Owners.Layers` (`internal/owners/owners.go:95`): a layer absent from `layers:` and from a pipeline's
  `owners:` has no approver, so a proposal that needs it can never run. `CanApprove` returns false.
- End-to-end run 2026-10-18: every approval, grant and hand-off flow needed a second browser context.

## Scope — what to build

1. **P2a (A3): owners schema.** `internal/owners`: `Pipeline.AutoApprove []string yaml:"auto_approve"` (P0 item 3),
   validated against a name set the caller passes to `Load` (`LoadWithActions(path, actionNames)`; plain `Load`
   accepts any snake_case name so the file loads before the registry exists, and the server re-validates at start).
   `Owners.IsOwned(pipelineID, layer string) bool` (false when no `layers:` entry and no pipeline `owners:` entry
   names a principal); `Owners.OwnsAll(pipelineID string, layers []string, userID string, groups []string) bool`.
   `dev/owners.yaml` is A1's: ask in thread for an `auto_approve` example on `orders` and a second pipeline with an
   unowned `consumer` layer for the demo.
2. **P2b (A5): the room service.**
   - **Sole-member driver.** `CreateRoomFromAlert` leaves `Driver` empty as today; on the first WebSocket join
     (or first steer/comment/approve) of a room with no driver and no `control.requested` pending, the service
     grants control to that member with `control.granted{reason: "sole_member"}` (P0 item 4). A second member
     joining does not change it. Shadow rooms excepted. A viewer (`isViewer`) never becomes driver.
   - **Unowned layer fallback.** In `createProposal`, a required layer with `!owners.IsOwned` is recorded in
     `ProposalCreated.FallbackLayers []string`; `authorizeLayer` grants `"fallback"` for such a layer to the
     **current driver** only (checked at append time like every driver action). The agent's proposal text is not
     changed; the UI shows the badge (P2c).
   - **approve-all** (`POST /api/proposals/{pid}/approve-all`, P0 item 4): `OwnsAll` (or solo user, or fallback
     for the driver) over `required_layers`, else `403 not_layer_owner` naming the first missing layer and
     nothing recorded; then one `approval.given` per layer in `required_layers` order inside one `s.mu` hold,
     `grant` per layer (`owner`/`solo`/`fallback`), same `params_hash` rule, same executor hand-off as the last
     `approve` today. Solo confirm stays required once per call (`confirm: "<pipeline_id>/*"`).
   - **Policy auto-approve.** After `proposal.previewed` (F4c) lands, if every step's action is in the pipeline's
     `auto_approve`, every preview succeeded with `DryRun.OK != false`, write mode is on, the room is open and not
     shadow, and `cfg.AutoApproveDelay > 0`: emit `proposal.scheduled{run_at: now+delay, cancel_until: run_at}`
     and start a timer (service lifetime ctx; survives nothing: a restart drops it and the proposal is simply
     pending, logged once). At `run_at`, unless `proposal.cancelled` or any human `approval.*` arrived: one
     `approval.given{grant: "policy", policy: "auto_approve", actor: systemActor}` per required layer, then the
     executor as usual. `POST /api/proposals/{pid}/cancel` by the driver or any required-layer owner emits
     `proposal.cancelled` and stops the timer. A manual approve-all before `run_at` cancels the timer silently.
   - Snapshot: `mode.solo` (derived: `OwnsAll` over `events.LayersOf(kind)` for the caller, or solo user),
     `mode.auto_approve`, `open_proposals[].scheduled`, `open_proposals[].fallback_layers`.
   - Metrics: `piperoom_approvals_total{grant}` gains the new grants.
3. **P2c (A6): the card and the room.**
   - `ProposalsPanel.tsx`: when `mode.solo` or the caller owns every `required_layers` entry → one primary button
     **Approve and run** (confirm dialog lists the layers it will record, the fingerprint, and "your approval is
     void if the steps change" as today), calling `approve-all`. Otherwise today's per-layer rows.
   - A `fallback_layers` entry shows the row as "no owner for `consumer` · driver approves" with the driver's
     name; non-drivers see it greyed.
   - `scheduled`: the card header becomes a countdown ("Runs in 0:42 · policy auto_approve · Cancel"); Cancel
     calls `cancel`; the feed gets one line at `proposal.scheduled` and one at run or cancel. `aria-live=polite`
     for the countdown's last 10 s only.
   - `RoomScreen.tsx`: "Request control" is hidden when the caller is the driver (already) **and** when the room
     has one member and no driver (the server grants on join; show "You are the driver" instead). Hand-off is
     offered only when `people.length > 1`; the postmortem draft on close stays for a room of one.
   - `GET /api/me` `solo` banner (exists) is kept; the derived solo gets no banner, only the single button.
4. **P2d (A4): the agent knows the mode.** The runner reads `mode.write` and `mode.auto_approve` from the room
   (via `Deps`/the snapshot the service hands it; no new tool): with write mode off its proposal message says
   "proposed; write mode is off, so this will not run from here" once per room, not per proposal; with a policy
   match it says "this fix is on the pipeline's auto-approve list; it runs in 60 s unless cancelled". Prompt
   wording only, in `internal/agent/prompt*.go`; the evals' scripted scenarios must not change outcome.
5. **P2e (C12): Slack.** `proposal.scheduled` mirrors as one message in the room thread with the countdown and
   the cancel link (`PIPEROOM_PUBLIC_URL` deep link); `proposal.cancelled` and the policy `approval.given` as one
   line each. Nothing inbound.

### Design decisions from review (2026-10-18)
Accepted by the leads in the round 1 and round 2 reviews (P0 follow-up 1); also on the P2b-3 and P2b-4 rows of
`tasks/P-board.md`.
- **P2b-3 (fallback):** one proposal-aware `authorizeProposalLayer(st, p, user, layer, confirm, needConfirm)` that both `approve` and approve-all call (`authorizeLayer` first, then layer ∈ `fallback_layers` && driver == user → `"fallback"`); `approvalGrant` gains the fallback case.
- **P2b-4 (policy auto-approve):** the timer's fire path is authoritative (under the room lock: open, no `proposal.cancelled`, no human `approval.*` after `proposal.scheduled`); the explicit stop lives in `appendApprovalLocked` and the reject path, not as a third copy; `appendApprovalLocked` takes the actor and optional identity so policy approvals reuse it; `mode.solo` = `authorizeAllLayers(pid, user, pipeline.Layers())`; `auto_approve` is validated against the FULL proposal registry, never an executor registry emptied by write-mode-off; a `schema_*` entry is refused.

## Out of scope
- Auto-approve for anything whose preview is missing, failed or `DryRun.OK == false`; for `schema_*` actions;
  for driver-authored proposals (P3). The list is per pipeline, never global.
- Changing `PIPEROOM_SOLO_USER` semantics: it still implies solo for every pipeline and still needs the confirm.

## Ownership and files
| Sub-task | Agent | Files |
|---|---|---|
| P2a | A3 | `internal/owners/**` |
| P2b | A5 | `internal/room/**`, `internal/httpapi/**`, `internal/store/**` (nothing new in the schema is expected; a timer needs no table), `cmd/server/**` (re-validate `auto_approve` against the registry) |
| P2c | A6 | `web/**` |
| P2d | A4 | `internal/agent/**` (not `contract.go`), `internal/wiring/**` |
| P2e | C12 | `internal/slackbridge/**` |

## Dependencies and merge order
P0 → **P2a** → **P2b** → P2c, P2d, P2e in any order. P2c may be built against a fake snapshot before P2b lands.
P2b waits for P1a only for the `mode.write` field; it reads `cfg.WriteMode` directly otherwise.

## Done when
- [ ] Fixture `owners.yaml` with one user on every layer: a two-layer proposal is approved and run with one
      `approve-all` call; the log holds two `approval.given` with `grant: "owner"`; a user who owns one layer gets
      `403 not_layer_owner` naming the other and no event is written.
- [ ] A room from an alert, one member joins: `control.granted{reason: "sole_member"}` within one event of the
      join; a second member joining changes nothing; a viewer joining first gets nothing.
- [ ] A pipeline whose `consumer` layer is unowned: the proposal carries `fallback_layers: ["consumer"]`; the
      driver's approval records `grant: "fallback"`; a non-driver owner of another layer cannot approve it.
- [ ] `auto_approve: [connect_restart_task]`, delay 2 s in test: `proposal.scheduled` after the preview, then
      exactly one `approval.given{grant: "policy"}` per required layer, then `action.executed`; a `cancel`
      at 1 s leaves the proposal pending with `proposal.cancelled`; a failed preview schedules nothing; write
      mode off schedules nothing.
- [ ] Web: Vitest for the single button, the fallback row, the countdown and cancel, the hidden request-control;
      the 2026-10-18 end-to-end script passes with **one** browser context for the whole approve-and-run path.

## Tests
- A3: table tests for `IsOwned`, `OwnsAll`, `auto_approve` validation (unknown action name, duplicate, empty).
- A5: memstore unit tests for every bullet above, with a fake clock for the timer; `-race` on `internal/room`;
  an integration run of the `schema-change` scenario with `auto_approve` set.
- A4: prompt snapshot tests; the eval suite's pass rate must not move (`make evals`).

## Effort
P2a 1 day · P2b 4 days · P2c 3 days · P2d 1 day · P2e 0.5 day.
