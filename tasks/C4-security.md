# C4 — Security review and threat model (Claude cloud agent, Sonnet)

Read `AGENTS.md`, `docs/api.md`, all `tasks/*.md` and the code on `main` first. You own `docs/security/**`.
You **do not change code** — you write findings for the leads, who turn them into tasks.

## Goal
Piperoom lets an AI agent read production systems and propose writes that run after human approval.
Find the ways this can go wrong before customers use it.

## Work
1. `docs/security/threat-model.md`: assets (customer data in records, credentials, approval authority), actors
   (room members, other tenants, a malicious record value / log line, compromised Slack/webhook, the model itself),
   trust boundaries (tool output → model, Slack/webhook → room, browser → API, approvals → executor), data flows.
2. Review the code on `main` (and open PR branches, read-only) for:
   - prompt injection via tool output, record values, error traces, comments (is every path wrapped as untrusted?)
   - approval bypass: can anyone execute an action without every required layer owner? idempotency? replay?
   - auth: `X-User` dev auth — what must change for production (SSO, per-tenant isolation)
   - secrets: DSNs, API keys in logs, events, Slack, UI
   - read-only guarantees of tools; SQL/identifier injection; SSRF in HTTP clients; WebSocket origin checks
   - denial of service: unbounded scans, large payloads, event log growth
3. `docs/security/findings.md`: one entry per finding — id, severity (critical/high/medium/low), file:line or design
   area, scenario (how an attacker uses it), fix proposal, owner (which agent's folder).

## Done when
Both files exist; every finding has a concrete scenario and fix; no code changed.
