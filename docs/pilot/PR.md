# [C14] Pilot onboarding docs: least-privilege permissions, setup, data handling

## What
Adds `docs/pilot/` (docs only, no code): `README.md` (pilot limits), `permissions.md`
(least-privilege grants per system), `setup.md` (env vars, owners/users files, start, verify),
`data-handling.md` (what data goes where, retention, controls).

## Why
Pilot teams need to connect Piperoom to a real pipeline without over-granting access or being
surprised by what is sent to the model and stored in the room log.

## How I tested
Docs only. Facts checked against `main` @ `origin/main`: env vars vs `internal/config/config.go`,
tool and action names vs `internal/tools/*` and `internal/actions/kafka_actions.go`, SQL objects
read vs `internal/clients/postgres`, redaction vs `dbtools` (`redactRow`, `redactWhRow`),
`connect_config` redaction, API routes vs `docs/api.md`, Snowflake grants vs
`internal/clients/snowflake/snowflake.go` comment.

## Not done / known gaps
- `tasks/C14-pilot-onboarding.md` is not on `main` (my copy of the task came from the prompt); please check "Done when" against it.
- `cmd/server`, `migrations/`, `internal/httpapi` are not on `main`; `setup.md` says so and marks the steps that need them.
- `docs/design/*` and `internal/clients/netsec` are not on `main`. `setup.md` references
  `docs/design/webhook-auth.md` as "if present"; `permissions.md` tells readers to confirm TLS/SASL support in their build.
- The Kafka ACL table and Connect/Registry endpoint lists are derived from reading the clients/actions, not tested against a secured cluster.
- `docs/security/findings.md` still lists F3/F4 as open; the code on `main` fixes them. The pilot docs describe the code. Findings doc is not mine to edit.

## New dependencies
None.

## Contract change request
None.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01G2Hdht3ExCPHQyYeba8CHw
