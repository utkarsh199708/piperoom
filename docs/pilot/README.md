# Piperoom pilot onboarding

For the platform/data engineer at a pilot team who will connect Piperoom to a real pipeline.

| Doc | Read it to |
|---|---|
| [permissions.md](permissions.md) | create **least-privilege** credentials for each system Piperoom touches |
| [setup.md](setup.md) | configure and start Piperoom against your pipeline, then verify it |
| [data-handling.md](data-handling.md) | know exactly what data Piperoom reads, where it goes, and who sees it |

## Read this first: pilot limits

Piperoom is pre-1.0. These are known, open gaps (see `docs/security/findings.md`). Plan your
pilot around them instead of discovering them in an incident:

- **No real login (F1).** Identity is an `X-User` header checked against `users.yaml`. Anyone
  who can reach the HTTP API can act as any user, **including approving fixes as any layer
  owner**. The server binds to loopback by default. Keep it that way (SSH tunnel / VPN
  or a reverse proxy that does real authentication) and do not expose it on a shared network.
- **Approvers see a description, not parameters (F2).** The approval UI shows the model's
  free-text description of a fix, not the exact connector/subject it will act on. Before
  approving, ask the agent (or read the `proposal.created` event) for the exact `params`.
- **Room service not on `main` yet.** `cmd/server`, the HTTP API, the store and the executor
  (A5) have not merged. Until they do, only `cmd/investigate` runs, and [setup.md](setup.md)
  marks the steps that need the server.
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
