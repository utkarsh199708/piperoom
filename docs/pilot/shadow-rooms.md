# Shadow rooms

A **shadow room** is a normal incident room whose agent runs **blind**: it investigates with the same
read-only tools as in a live room, but it proposes nothing, says nothing to the people in the room, and
nobody sees what it concluded until the incident is closed and a person has written down what really
happened. Then you compare. The point is a measured number, "how often was the agent right", before you
let it into a live incident.

Design: [docs/design/shadow.md](../design/shadow.md). API: `docs/api.md` ("Shadow mode" and the shadow
rows of the route table). This page says what an on-call engineer needs. Each statement was checked
against `main` at `b5bbbc2`; where the design and the code differ, this page follows the code.

> **Not the same as the watcher's `log` mode.** The built-in watcher has a default mode called `log`
> (`PIPEROOM_WATCH_MODE`, [setup.md](setup.md#1-configure-the-environment)). In `log` mode **no agent
> runs at all**: the watcher only writes log lines and opens no room. Older notes called that "shadow
> mode". Shadow rooms are the blind **agent** runs described here. The two are independent. See the
> [comparison](#shadow-rooms-and-the-watchers-log-mode).

## Turn it on, per pipeline

Shadow is a block in `owners.yaml`, next to `watch:`. It is per pipeline. There is no global switch
(`internal/owners/shadow.go`).

```yaml
pipelines:
  - id: orders
    # ...topic_prefix, source_tables, connectors, as in setup.md section 2...
    shadow:
      enabled: true     # no block, or enabled: false, means a live agent
```

- The file is read at start-up: **restart the server** after editing it. A misspelled key inside the
  block (for example `enable: true`) stops the start, so it cannot silently leave the pipeline live.
- **Every alert route** opens a shadow room for a shadow pipeline: the signed routes
  `POST /api/alerts/{source}`, the unsigned dev webhook, and the watcher in mode `rooms`
  (`docs/api.md`, Shadow mode). **A room opened by hand** (`POST /api/rooms`) is never a shadow room,
  because a person who opens a room wants the agent. So for shadow rooms you need real alerts coming in:
  wire up [alerts](setup.md#6-wire-up-alerts), or set `PIPEROOM_WATCH_MODE=rooms`. With the watcher
  in its default `log` mode no room opens, so nothing is shadowed.
- A room is shadow or not **when it opens**, and the flag is stored in its log. Removing the block and
  restarting makes *new* rooms live. Rooms already open stay shadow and still get revealed.
- `GET /api/pipelines` shows `shadow: true` for such a pipeline.
- The room store must be able to hold shadow runs. The Postgres store and the in-memory demo store can;
  the server refuses to start if a pipeline is shadow and the store cannot (`internal/room/service.go`,
  `Load`).
- Token, turn and grace settings are in [Budgets and settings](#budgets-and-settings).

## What people see, and when

| When | In the room | Agent output |
|---|---|---|
| Alert arrives | A room opens as usual. Engineers comment, take control, hand off and close it exactly as in a live room | The agent starts once, as soon as an agent slot is free |
| While open | The room log holds only human and system events. The agent's status stays `idle`; it writes no message, no hypothesis, no tool step. A steer, a suggestion or an `@agent` comment is saved in the log but never starts or wakes the agent | Written to a separate store (`shadow_events`), which no WebSocket, no `GET /events`, no export and no Slack mirror reads |
| The driver closes the room | A **resolution is required** (an empty one is `400 validation`): it is what the agent is graded against | The run is cancelled at close, unless you set a grace period |
| After the run has ended and the room is closed | The system adds one `shadow.revealed` event: status, three times, tool-call count and token count. **Numbers only, no hypothesis** | Still hidden |
| After the blind label | The run can be read | `GET /api/rooms/{id}/shadow` returns the hypothesis, the steps and (with `include=data`) the tool data |

Other rules you will notice:

- The room's cost chip stays off and its usage is zeros, because the agent's spend is not in the room
  log (`docs/api.md`, room snapshot).
- **Slack:** the mirror stops at `room.closed` and never posts shadow output (`docs/api.md`, Shadow mode).
  It does post the human part of the room as usual.
- The hand-off summary is written from the human events, since the room log holds no agent events
  (`docs/design/shadow.md` §3.4).
- A shadow room does not carry the agent's fixes: `propose_fix` has no actions to offer, so nothing is
  proposed, approved or executed (`docs/design/shadow.md` §3.3, §4).

## Label and grade after the incident

The order is enforced by the server: **resolution at close → blind label → reveal visible → grade.**
It is meant to stop anyone from grading the agent after reading its answer. The two **writing** routes,
`shadow/blind` and `shadow/grade`, are for a **member of the room's pipeline**: an approver or backup for
any layer of that pipeline in `owners.yaml`, or the solo user (in dev mode also a `users.yaml` user whose
`layers:` grant one). Anyone else gets `403 not_allowed`, and a viewer gets `403 viewer`. Being in
`users.yaml` alone is not enough (`docs/api.md`, `shadow/blind` and `shadow/grade` rows). The reading
route, `GET .../shadow`, is open to any logged-in user, viewers included, once the blind label exists.

**The web app has no screen for these three steps yet.** There is no blind-label form, no run viewer and
no grade form for shadow rooms in `web/src`. The app shows no shadow indicator at all; it only uses the
flag to hide the cost chip, the opened timer and the insight line. Use the API. Every `POST` needs
`Content-Type: application/json`. Set these once in your shell:

```sh
BASE=https://piperoom.example.com   # your PIPEROOM_PUBLIC_URL; http://127.0.0.1:8090 for a local dev server
read -rs SESSION                    # paste the cookie value; -s keeps it out of the screen and the history
AUTH="Cookie: __Host-piperoom_session=$SESSION"   # oidc mode
# dev mode only:  AUTH='X-User: <user id>'   (or  Cookie: piperoom_session=...)
```

In `oidc` mode the session cookie is `HttpOnly`, so copy its value from the browser's developer tools
(the storage or application panel, cookie `__Host-piperoom_session`). It is a bearer credential: anyone
who has it acts as you until it expires (`PIPEROOM_SESSION_MAX`), so keep it out of shell history,
tickets and chat (`internal/auth/settings.go`, `internal/httpapi/auth_oidc.go`). The general "Add label" form of the room
page posts to `POST /api/rooms/{id}/label`, which on a shadow room answers `409 not_labelled` until the
blind label below has been written (`docs/api.md`, `label` row).

1. **Blind label**, once per room (a second call is `409 already_decided`):

   ```sh
   curl -X POST -H 'Content-Type: application/json' -H "$AUTH" \
     -d '{"actual_layer":"kafka-connect","root_cause_terms":["schema change","orders.total"]}' \
     "$BASE/api/rooms/<room id>/shadow/blind"
   ```

   `actual_layer` is one of the six layers (`postgres`, `kafka-connect`, `schema-registry`, `kafka`,
   `consumer`, `warehouse`). `root_cause_terms` is 1 to 20 short strings (1 to 200 characters each) that
   name the real cause in your own words. Only a **closed** room can be labelled (`409 room_open`).
   Before this call `GET .../shadow` answers `409 not_labelled`.

2. **Read the run** once the label exists (any logged-in user, viewers included):

   ```sh
   curl -H "$AUTH" "$BASE/api/rooms/<room id>/shadow?include=data"
   ```

   You get `{run, events}`: the run's status (`queued`, `running`, `hypothesised`, `no_hypothesis`,
   `error`, `budget` or `interrupted`), the model, the times, the tool-call and token counts, the final
   hypothesis and the agent's steps.

3. **Grade it**, after `shadow.revealed` (before that, `409 not_revealed`; a run still going during a
   grace period is not revealed yet). Re-grading is allowed, the last grade wins, and every grade is
   kept in the log:

   ```sh
   curl -X POST -H 'Content-Type: application/json' -H "$AUTH" \
     -d '{"layer_correct":true,"root_cause":"partial","notes":"right layer, missed the column"}' \
     "$BASE/api/rooms/<room id>/shadow/grade"
   ```

   `layer_correct` is true or false. `root_cause` is `yes`, `partial` or `no`. `notes` is up to 2,000
   characters. The label and the grade are stored with the labeller's identity (`identity` on
   `shadow.labelled` and `shadow.graded`).

`GET /api/rooms/{id}/export?include=shadow` adds the shadow run to a closed room's export, under the
usual export rule (approver, backup or auditor) and only after the blind label.

## Measure it: `GET /api/shadow/metrics`

```sh
curl -H "$AUTH" "$BASE/api/shadow/metrics?pipeline=orders&since=2026-10-01T00:00:00Z"
```

Any logged-in user may read it. `pipeline` and `since` (RFC 3339, compared with the close time) are
optional. It counts **every closed shadow room** that matches, whatever happened to its run:

| Field | Meaning (`docs/api.md`, `docs/design/shadow.md` §3.6) |
|---|---|
| `rooms`, `labelled`, `graded` | closed shadow rooms; how many have a blind label; how many have a grade |
| `layer_correct` | graded rooms whose grade said the layer was right |
| `root_cause` | counts of `yes`, `partial` and `no` |
| `by_status` | rooms per run status |
| `median_time_to_first_s`, `median_time_to_final_s`, `median_human_mttr_s` | seconds from the room opening to the agent's first and final hypothesis, and the humans' own time to close; `-1` when no room has the value |
| `total_tokens`, `tokens_available` | tokens spent; false when every run reports zero (demo mode) |
| `per_room` | one row per room, with `alert_source`, times, status, grade and tokens |

"N of M matched" is `layer_correct` out of `rooms`. Only the human grade decides a match: a room counts when
its grade says `layer_correct: true`, whatever the run's status. A room that was never graded always
counts as not matched, and so does one graded `false`. A run that ended in `error`, `budget`,
`interrupted` or `no_hypothesis` can still be graded (a revealed run accepts a grade), and the grader
should normally mark such a run wrong. The denominator is every closed shadow room, which is
deliberately the lower, honest number. Ungraded rooms pull the rate down, so label and grade every
closed room (`shadowMetrics` in `internal/room/shadow.go`).

How many graded rooms are enough, and what hit rate means "go live", is **not decided**. The pilot
decision proposes 20 closed, labelled rooms with `layer_correct` in at least 70% as a starting bar and
says that the 70% is not derived from data (`docs/pilot/decision.md` §5.6).

## Budgets and settings

| Setting | Shadow run | Live room | Source |
|---|---|---|---|
| Tokens per room | `PIPEROOM_SHADOW_TOKEN_BUDGET`, default **500,000**. `0` means the live room budget (`PIPEROOM_ROOM_TOKEN_BUDGET`), which is itself uncapped if you set that to `0` | `PIPEROOM_ROOM_TOKEN_BUDGET`, default 2,000,000 | `internal/config/config.go` (`DefaultShadowTokenBudget`), [setup.md](setup.md#1a-triage-model-and-the-token-budget) |
| Model turns | `PIPEROOM_SHADOW_MAX_TURNS`, default **20** | 40 | `internal/config/config.go`, `internal/agent/options.go` |
| Grace after close | `PIPEROOM_SHADOW_GRACE`, a Go duration, default `0`: the run is cancelled at close. A positive value lets a run that is mid-way finish, holding its agent slot | not applicable | `docs/api.md`, Shadow mode |

- A shadow run is **one episode**: it ends at the agent's first final answer or at the turn or token
  limit, and it takes no input. So it never sits parked in a room.
- Shadow runs and watcher-opened rooms take agent slots from the same `PIPEROOM_MAX_AGENTS` cap as live
  rooms, and may queue ([setup.md](setup.md#supported-scale)).
- A run that hits its token budget ends with status `budget`, and is not retried.
- The values are read at start-up; a bad one stops the start and names the variable. The start-up log
  has one line, `server: shadow mode`, with the pipelines and these numbers (`cmd/server/main.go`).
- **Docker Compose:** `deploy/docker-compose.pilot.yml` passes none of the three `PIPEROOM_SHADOW_*`
  variables, so the defaults apply there. The `shadow:` block itself lives in the mounted `owners.yaml`
  and works. The Helm chart passes all three (`deploy/helm/piperoom/values.yaml`). That chart's comment
  on `PIPEROOM_SHADOW_TOKEN_BUDGET` says "0 = no cap"; the code says `0` means the live room budget
  (`cmd/server/shadow.go`).
- `GET /api/usage?pipeline=` includes shadow runs in a pipeline's spend for the day (`docs/api.md`).

## Restarts and failures

- A restart does **not** restart a shadow run. A run that was `queued` or `running` is marked
  `interrupted`, and a closed room whose run was finished but not revealed gets its reveal
  (`docs/design/shadow.md` §3.7; `internal/room/service.go`, `Load`). With several replicas, a replica
  that takes over a shadow room does the same (`internal/room/replicas.go`, `adoptRoom`).
- An interrupted run is in the metrics denominator. It counts as a match only if a person grades it
  `layer_correct: true`; ungraded, it counts as not matched.
- A failing run is retried up to 3 attempts while the room is open, then ends `error`. After the room
  is closed it is not retried. Failures are written to the shadow store, not to the room.
- Shadow data is deleted with its room, by retention (`PIPEROOM_RETENTION_DAYS`) or by an admin delete
  (`internal/store/retention.go`; `docs/api.md`, `DELETE /api/rooms/{id}`).

## What the model and the customer see

A shadow run sends the model the same kind of data as a live room: alert text, the pipeline map, tool
summaries (which can include keys and timestamps), per [data-handling.md](data-handling.md). Nothing the
agent writes is shown in the room, in Slack or in the export before the label. The tools are scoped to
the room's pipeline and read-only, as in a live room (F11 in [README.md](README.md)). Ask the customer
the model-data question in [decision.md](decision.md) §5 before turning shadow on, because shadow does
not remove it.

## Shadow rooms and the watcher's `log` mode

| | Watcher `log` mode (`PIPEROOM_WATCH_MODE=log`) | Shadow rooms (`shadow:` in `owners.yaml`) |
|---|---|---|
| Runs the agent | No | Yes, once per room, with read tools |
| Opens rooms | No: it only writes `watch:` log lines | Yes, from any alert route or the watcher in mode `rooms`; humans work them |
| Set by | one environment variable for the whole server | a block per pipeline in `owners.yaml` |
| Output | log lines for the operator | a hidden run, revealed after close and a blind label |
| Result | you check logs against what happened | labels, grades and `GET /api/shadow/metrics` |
| Cost | none | model tokens, 500,000 per room by default |

They combine: the watcher in `rooms` mode on a shadow pipeline opens shadow rooms
(`docs/design/shadow.md` §3.1).

## Known gaps

- No web screens for the blind label, the run viewer or the grade (use the API above).
- No Docker Compose pass-through for the `PIPEROOM_SHADOW_*` numbers (the Helm chart has it).
- The 70% bar is a proposal, not a measured threshold.
