# Post-mortem: Incident

Status: open (draft so far)

| | |
|---|---|
| Severity | sev2 |
| Pipeline | orders |
| Opened (UTC) | 2026-10-11 10:00:00 |
| Room id | `ROOM` |

## Timeline

| time (UTC) | +mm:ss | what |
|---|---|---|
| 10:00:00 | +00:00 | room opened (opened by hand) |
| 10:02:30 | +02:30 | hypothesis (high): order-sync is stuck on a poison record |
| 10:03:00 | +03:00 | proposal `p_1` proposed: Restart the consumer |
| 10:03:30 | +03:30 | Kabir commented: looking at it |

## Root cause

Final hypothesis (high confidence): order-sync is stuck on a poison record

## What was done

### `p_1` Restart the consumer

1. `restart_consumer` · consumer · restart order-sync

Approvals: none

Outcome: waiting for approval

## Verification

No verification recorded.

## Still open

- proposal `p_1` is still waiting for approval

## Risks

none recorded

## Ruled out

none recorded

## Agent

- steps: 1
- tool calls: 1 (0 failed)
- first hypothesis after 150 s
- model calls: 1
- tokens: 1200 in, 300 out
- cost: $0.07

Drafted by Piperoom dev from 9 events; summary source: none
