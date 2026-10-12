# Piperoom pilot onboarding

For the platform/data engineer at a pilot team who will connect Piperoom to a real pipeline.

| Doc | Read it to |
|---|---|
| [permissions.md](permissions.md) | create **least-privilege** credentials for each system Piperoom touches |
| [setup.md](setup.md) | configure Piperoom against your pipeline, start the room service and verify it |
| [data-handling.md](data-handling.md) | know exactly what data Piperoom reads, where it goes, and who sees it |

## Read this first: pilot limits

Piperoom is pre-1.0. These are known, open gaps (see `docs/security/findings.md`). Plan your
pilot around them instead of discovering them in an incident:

- **No real login (F1).** Identity is an `X-User` header checked against `users.yaml`
  (`internal/httpapi/server.go:125-142`). Anyone who can reach the HTTP API can act as any
  user, **including approving fixes as any layer owner**. The real login is still only a design
  (C9). The server listens on `PIPEROOM_HTTP_ADDR`, default `127.0.0.1:8090`
  (`internal/config/config.go:10`, used at `cmd/server/main.go:172`). **There is no check that
  the address is loopback**: if you set it to `0.0.0.0` the server starts without a warning.
  Keep it on loopback (SSH tunnel / VPN, or a reverse proxy that does real authentication).
- **Approvers see a description, not parameters (F2).** The approval UI shows the model's
  free-text description of a fix, not the exact connector/subject it will act on. Before
  approving, ask the agent (or read the `proposal.created` event) for the exact `params`.
- **The alert webhook is not authenticated as an alert sender.** `POST /api/alerts/webhook`
  sits behind the same `X-User` check as every other route (`internal/httpapi/server.go:69-70,81`),
  so Alertmanager and similar senders must send a valid `X-User` header, and nothing checks a
  signature or token. `internal/alertin` (which can verify senders) is not used by the server
  (no import in `cmd/server`, `internal/room` or `internal/httpapi`). The same alert sent twice
  opens two rooms (`internal/room/service_room.go:102`: a new id every time, no dedup). Keep
  the endpoint reachable only from your alerting system.
- **Not wired yet:** the Slack bridge (`internal/slackbridge` is not used by `cmd/server`).
- **A restart restarts the agents but not the work.** See
  [setup.md](setup.md#what-happens-on-a-restart).
- **Write actions are scoped to the room's pipeline** (F10, fixed on `main`:
  `actions.Scoped.CheckScope`). Still use narrow Connect/Registry credentials as defence in
  depth; the credential, not the code, should be the last line.
- **Row values are redacted, but identifiers are not.** `pg_row` / `warehouse_row` return only
  the key and timestamp values to the model and the room log (F3, fixed on `main`); other
  columns appear as names only. Keys, topic names and timestamps still appear. See
  [data-handling.md](data-handling.md).
- **One credential set per process (F7).** Run one Piperoom process per team/pipeline group.
  Do not share one process between teams with different data access.

If any of these is a blocker for your environment, start with a non-production replica or
staging pipeline. Check `docs/security/findings.md` for the current status; this list may be
out of date as findings are fixed.
