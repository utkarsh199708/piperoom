# Post-mortem: Incident

Status: closed

| | |
|---|---|
| Severity | sev2 |
| Pipeline | orders |
| Opened (UTC) | 2026-10-11 10:00:00 |
| Closed (UTC) | 2026-10-11 10:07:30 |
| Duration | 7 min 30 s |
| Closed by | Priya |
| Resolution | fixed |
| Room id | `ROOM` |

## Timeline

| time (UTC) | +mm:ss | what |
|---|---|---|
| 10:00:00 | +00:00 | room opened (opened by hand) |
| 10:02:30 | +02:30 | hypothesis (high): order-sync is stuck on a poison record |
| 10:03:00 | +03:00 | proposal `p_1` proposed: Restart task 0 of orders-cdc |
| 10:03:30 | +03:30 | proposal `p_2` proposed: Reset the consumer offsets |
| 10:04:00 | +04:00 | approval given for `p_1`: Arjun (user:arjun, dev) (kafka-connect) |
| 10:04:30 | +04:30 | `p_1` step 1 ok |
| 10:05:30 | +05:30 | verification: `p_1` recovered; recovered: every layer check passed and data is flowing again |
| 10:06:30 | +06:30 | hand-off requested by Arjun to Priya |
| 10:07:00 | +07:00 | hand-off `h_1` created: Arjun to Priya |
| 10:07:30 | +07:30 | room closed by Priya |
| 10:08:00 | +08:00 | proposal `p_2` withdrawn by Piperoom: room closed |

## Root cause

Final hypothesis (high confidence): order-sync is stuck on a poison record

Why: the task hit a transient error

Resolution: fixed

## What was done

### `p_1` Restart task 0 of orders-cdc

1. `connect_restart_task` · kafka-connect · restart the failed task

Step 1 params:

```json
{"connector":"orders-cdc","task":0}
```

Approvals:
- kafka-connect — Arjun (user:arjun, dev) at 10:04:00

Outcome: executed

### `p_2` Reset the consumer offsets

1. `reset_offsets` · consumer · rewind one hour

Approvals: none

Outcome: withdrawn (room closed)

## Verification

- `p_1` recovered; recovered: every layer check passed and data is flowing again

## Still open

- watch the lag for an hour

## Risks

- a second failure needs a schema check

## Ruled out

- broker health

## Agent

- steps: 1
- tool calls: 1 (0 failed)
- first hypothesis after 150 s
- model calls: 2
- tokens: 2000 in, 500 out
- cost: $0.12

Drafted by Piperoom dev from 19 events; summary source: handoff `h_1`
