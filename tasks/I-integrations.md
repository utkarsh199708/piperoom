# I — Integrations and secret managers: remaining work

> **Status (lead, 2026-10-14):** integrated on `claude/gifted-planck-gb2o6c`, **not committed**. The whole tree is green
> without Docker: `go build ./...`, `go vet ./...` (also with `-tags integration`), `gofmt -l .` empty,
> `go test ./...`; in `web/`: `npm run build`, `npm run lint`, `npx vitest run` (527 tests).
> Specs: `docs/design/integrations.md`, `docs/design/integrations-ux.md`, `docs/design/secret-providers.md`.

Owners: I1 = `internal/secrets/**`, I2 = `internal/integrations/**` (AGENTS.md §2). Items marked A2–A7 go to those
owners.

## What is built

| Area | Where |
|---|---|
| Sealing (AES-256-GCM, key rotation), host guard, catalog, phase-1 kinds, service | `internal/integrations` |
| Secret-manager cards (Vault/OpenBao, AWS Secrets Manager, SSM, Kubernetes, Azure Key Vault, Google), setup scripts | `internal/integrations/secretkinds` (added by the lead during integration) |
| `secret://` parser, registry, cache, leases, providers | `internal/secrets`, `internal/secrets/{azure,gcp}` |
| No-restart swap | `internal/wiring/live.go` |
| Migration and stores | `migrations/00010_integrations.sql`, `internal/store/integrations.go`, `internal/store/memstore/integrations.go` |
| API, admin rule, room notices, setup mode | `internal/httpapi/integrations.go`, `internal/room/integrations.go`, `cmd/server/integrations.go` |
| Integrations page | `web/src/components/integrations/**`, `web/src/lib/integrations.ts`, `web/src/api/integrations.ts` |

Lead fixes made during integration:

- Added the `secret_manager` catalog cards. Neither track built them, so a secret manager could not be created from the UI.
- Registered the Google factory in `cmd/server` (only Azure was registered).
- Env-declared providers now win. A saved row named like a `PIPEROOM_SECRET_PROVIDERS` id (or `files`) gets `managed_by_env` and can no longer replace or unregister it.
- `integrations.ParseRef` now delegates to `secrets.ParseRef`, so the API refuses at save time what would fail at resolve time.
- The web page shows `restart_required` after an alert secret is rotated.
- `docs/api.md` now documents the rotate response's `restart_required`, PagerDuty's `400`, and that `PUT` replaces the whole non-secret form.
- Corrected secret-providers.md §2: `Ref.Provider` is the row `name`, `Check` returns `spec.Check`, and `Value` has lease fields.

## End-to-end review fixes (E1–E23, lead, 2026-10-14)

Each item has a regression test (unit with fakes; the Postgres items also an integration test,
`TestIntegrationPostgresSetupScriptAndColumnGrants`, run against a scratch PG 16 via `PIPEROOM_IT_PG_ADMIN_URL`).

| # | Fixed | Where |
|---|---|---|
| E1 | Reserved references are compared by what they read (`Registry.SecretIdentity`: a files ref by its real path, so symlinked roots and `..data` links match), and a reference that resolves to any process secret value (`ReservedValues`: keys, alert tokens, URL passwords, constant-time compare) is refused in test, save, enable and restore | `integrations/service.go`, `secrets/files.go`, `secrets/registry.go`, `cmd/server/integrations.go` |
| E2 | `read_only:<table>` also fails on `TRUNCATE`, `REFERENCES`, `TRIGGER` and column `INSERT`/`UPDATE` grants; the fix names the columns | `integrations/kind_postgres.go`, integrations.md §4.2 table |
| E3 | `cmd/server` passes `OnChange`; `Service.SecretChanged` rebuilds the rows that read a changed file or a lost lease; a failed renewal is retried with backoff; the log says what happens | `cmd/server/integrations.go`, `integrations/service.go`, `secrets/renew.go`, `secrets/registry.go` |
| E4 | A row restore or a live apply could not build is `failing` with a `live` check (not stored) | `integrations/service.go` (`MarkNotLive`), `cmd/server` restore |
| E5 | `ErrOpen` is `spec.ErrSecretKeyMismatch` → `409 secret_key_mismatch`; `PUT` that retypes the typed secrets repairs the row, a kept one → `400 must be entered again`; stored resolve errors keep their cause (`*spec.SecretRefError`) → 400/403/404/502 | `integrations/service.go`, `spec/service.go`, `httpapi/integrations.go` |
| E6 | Delete or disable of a secret manager with enabled dependents → `409 in_use` with `dependents` (the drawer shows the message) | `integrations/service.go`, `httpapi/integrations.go` |
| E7 | Optional audit grants run in a `DO` block only when `piperoom_audit.ddl_log` exists | `kind_postgres.go`, `docs/pilot/permissions.md` |
| E8 | The script creates the role without a password literal and says to `\password` it | same |
| E9 | `spec.Field.AllowRef`: the Postgres `user` may be a `secret://` reference (one lease gives user and password); `SOURCE_DB_PASSWORD` / `WAREHOUSE_DB_PASSWORD` added to `internal/config` (**contract file, lead change**) and spliced into the URLs | `spec/spec.go`, `values.go`, `config.go`, `cmd/server`, web |
| E10 | Picker refusals map to 400/403/404 with `field` instead of 500. Vault already wrapped a 404 as `ErrNotFound` (existing test `TestVaultKV2`); the 500 came from the missing mapping | `cmd/server` `browseErr`, `httpapi` |
| E11 | Vault `Keys` honours the provider prefix | `secrets/vault.go` |
| E12 | The least-privilege pass names the one probe it made and says other paths are not checked | `secrets/vault.go` |
| E13 | Files refuse a non-regular file before opening it, and open with `O_NONBLOCK` (Unix) | `secrets/files.go`, `secrets/open_*.go` |
| E14 | The env provider names the real refusal reason | `secrets/env.go` |
| E15 | A refused TLS request is detected with an SSLRequest probe → `tls` fail "the server did not use TLS" | `kind_postgres.go` |
| E16 | `slots_visible` fix is `GRANT SELECT ON pg_catalog.pg_replication_slots` | `kind_postgres.go` |
| E17 | For a superuser the table checks fail with the not-superuser fix, not a REVOKE | `kind_postgres.go` |
| E18 | Env rows count as connected in the checklist (web); documented | `web/src/lib/integrations.ts`, `docs/api.md` |
| E19 | Repeating enable/disable writes no version, audit row or notice (enable still rebuilds the client) | `integrations/service.go` |
| E20 | `If-Match: 0` reaches the service, so env rows answer `409 managed_by_env` | `httpapi/integrations.go` |
| E21 | A `secret://` database URL in the env view is not parsed as a URL | `integrations/env.go` |
| E22 | `GET /api/integrations/status` → `secret_key_set`; the drawer explains and offers references only | `httpapi`, web |
| E23 | `docs/api.md` documents `env_alerts_<source>`, `200 []` audit for unknown ids, and the new codes | `docs/api.md` |

Left open from this review:

- **`RetryAfterAuthFailure` is still unused** (E3, rest of S4): a static secret rotated in a manager that sends no
  change notice is picked up on enable or restart, not on the first `28P01` / SASL / 401. Needs the wrap in each
  client login (A2, A3, A4).
- Process variables resolved from references at start (`PIPEROOM_DB_URL`, `SOURCE_DB_URL` …) are not rebuilt on a
  change; a restart applies them.
- The `in_use` check does not know env-declared providers, so a page row shadowed by an env provider of the same name
  is also refused while rows reference that name.
- Vault least-privilege still probes one path (E12 made the text honest; deeper probing or
  `sys/internal/ui/resultant-acl` remains I1 work).
- The `live` state is in memory: a second server replica does not see it.

## Before merge

1. Run `go test -tags integration ./internal/store/ ./internal/integrations/...` against the dev stack. The Postgres
   store suite was not re-run after its last fix, including the `CASE` expression in the `secret_manager` unique index.
   The Kafka `DescribeGroups` step and the integrations integration tests have never run.
2. Build and test on Windows (AGENTS.md §3). `cmd/server` drops the non-absolute default `PIPEROOM_SECRET_FILE_ROOTS`
   there; check that it starts.
3. Start `cmd/server` for real against the dev stack, once in setup mode (no data-stack env) and once with env. Save
   one row per card and check that the client swaps live.
4. Code review by `[LEAD-REVIEW]`. `internal/integrations/secretkinds` was written by the lead and has had no review.
5. Split into the PRs in integrations.md §11 and secret-providers.md §8 (each < 600 lines where possible).

## Phase-1 gaps (integrations-ux.md §8, phase 1)

| # | Gap | Owner | Needs |
|---|---|---|---|
| 1 | A saved Anthropic key does not reach the running agent (the card's Test warns `live_swap`) | A4 | `agent.EndpointConfig.APIKey netsec.Secret`; the anthropic kind's Build then returns `*agent.Endpoint` |
| 2 | A Slack row saved on the page is stored but not used; mirroring still reads `SLACK_BOT_TOKEN`/`SLACK_CHANNEL_ID` | A5 | applier builds `slackbridge.New` from the kind's `slackbridge.Options` and swaps it |
| 3 | A rotated or saved alert secret takes effect only after a restart | A5 | swappable signed-alert config in `internal/httpapi/alerts.go` (`atomic.Pointer[alertin.Config]`) |
| 4 | A pasted CA is refused for Postgres source and warehouse (Test fails `tls_ca`) | A2 + A3 | integrations.md PR 2 (`netsec.TLS` accepts PEM) and a `*pgxpool.Config` constructor |
| 5 | Postgres, warehouse and Snowflake Build only pre-check the host; their own dial skips the guard (DNS rebinding) | A3 | dialer option on `postgres.New`, `warehouse.New`, `snowflake.New` |
| 6 | Kafka built from the page has no Schema Registry client, so it cannot decode Avro keys | A2 | `kafka.New` takes a `clients.SchemaRegistryClient` interface (the live wrapper) |
| 7 | The warehouse SQL dialect is read once; a live Postgres→Snowflake swap keeps the old dialect | A3 | `dbtools.Deps` reads the kind through a func (`wiring.Live.WarehouseKind`) |
| 8 | Sample room and **Send a test alert** (ux §3.8) are not built | A5 + A6 | route that opens a room on a chosen pipeline; button on the checklist |
| 9 | Mixing env and page for the data stack is refused at start | A4 | per-category opener in `internal/wiring` |
| 10 | `owners.yaml` `admins:` is not read; only `PIPEROOM_ADMINS`, groups and the solo user | A3 | `owners.Admins` + `(*Owners).IsAdmin`; pass as `httpapi.Integrations.IsAdmin` |
| 11 | A secret-manager name that clashes with an env provider is saved and then reports a warn `apply` check | I2 | refuse on create/update with `409 managed_by_env` (service asks the applier or env first) |
| 12 | `spec.AuditTested` is never written; there is no background re-test | I2 | periodic re-test calling `SaveLastTest` |
| 13 | Write actions whose client is not configured fail at run time, not at proposal time | A4 | check `Live.Configured()` when proposing |
| 14 | In setup mode the watcher and `dlq_peek` log "not connected" errors each tick | A7 | `errors.Is(err, wiring.ErrNotConfigured)` / `errors.ErrUnsupported` → log once |
| 15 | Web fixture mode has no Integrations data | A6 | fixtures in `web/src/test/integrationsData.ts` → fixture API |
| 16 | ~~`cmd/server` `storedValues` duplicates the service's unseal/defaults/reference logic~~ **Done** (E4/E5: `(*Service).OpenRow`) | I2 | – |
| 17 | `azure/httpx.go` and `gcp/httpx.go` copy about 230 lines of helpers from `internal/secrets` | I1 | export the HTTP and check helpers from `internal/secrets` |

## Secret managers: open work (secret-providers.md)

- **S4, rotation without restart (A4).** `OnChange` is wired (E3): a changed file or a lost lease rebuilds the rows
  that read it. Still open: wrap client logins with `RetryAfterAuthFailure` (fetch once more, reconnect). Make every `netsec.Secret` field in wiring go
  through `secrets`. Done when a rotated password is picked up without a restart (unit test with fakes).
- **§9 canary test (A4 + I1).** Run every agent tool against fakes holding a dummy secret; fail if it appears in any
  `Result`, event or log line. Today only the `internal/secrets`, `internal/integrations` and `secretkinds` outputs
  are swept.
- **Mounted-files card (I1).** The `files` provider is always on, but there is no card with the CSI
  `SecretProviderClass` / External Secrets YAML setup script (§5 table).
- **Vault auth (I1).** AWS IAM, Azure and GCP auth methods are not built (token, AppRole, Kubernetes, JWT are).
- **Least-privilege depth (I1).** Vault probes one fixed path outside the prefix (its pass text now says so). AWS and Azure report `skipped`.
  Kubernetes checks the namespace, not `resourceNames`.
- **Smaller limits (I1).** These are documented in the code:
  - Vault List is one level deep, and KV v2 mounts are configured rather than detected.
  - The Vault internal-path guard is a denylist.
  - Any 403 costs one extra Vault login.
  - Files change detection uses path, size and mtime.
  - Kubernetes values that are not UTF-8 may be altered.
  - Every redirect is refused.
  - Azure App Service managed identity is not supported.
  - Google List scans at most 5000 names.
- **No integration tests** against a real Vault, Kubernetes, AWS, Azure or Google (I1).

## Open contract requests (lead decisions)

| Request | From | Decision |
|---|---|---|
| `spec.Applier` gains `Configured() []Category` (or a `LiveStatus` interface) | live | **Deferred** until gap 13 or the agent prompt needs it; `wiring.Live.Configured()` exists |
| `spec.View.VerifiedReadOnly bool` (or a fixed check id) so the badge is not a client rule | web | **Accepted, not done**: I2 sets it from the read-only checks; A6 reads it |
| Non-admins may read secret-free row status | web | **Product decision** needed; today non-admins see only the catalog |
| `config.DefaultSecretFileRoots` is not absolute on Windows | api | **Accepted, not done**: OS-specific default in `internal/config` (lead); `cmd/server` works around it now |
| Hosted workspaces reaching operator-declared env providers | secrets | **Decided: no.** Env providers stay in the default workspace; hosted reaches managers only through per-workspace rows (secret-providers.md §7) |
| Per-room token budgets survive `SwapModel` | live | **Accepted** → A4 (`agent.Endpoint` hands its budget state to its replacement) |
| A2, A3 and A4 client changes | integrations, api | **Accepted**: gaps 1, 4–7, 9 and 10 above |

Done at integration (no longer open):

- `SecretBrowser` (in `cmd/server`)
- `ParseRef` unified
- `docs/api.md` PagerDuty and `PUT` semantics, and `restart_required`
- secret-providers.md §2 wording
- Google factory registration

## Phase 2 (integrations-ux.md §8)

- **Ask someone** hand-off.
  - Links work only behind the customer's SSO.
  - A contractor is first added to the customer's identity provider as a guest.
  - An outsider who opens the link sees "Ask <name> to add you to <org>'s sign-in".
  - Adds the "Waiting on <person>" status.
- Stack tiles with discovery, pick pipelines (integrations.md §4.6, PR 8), and owners from SSO groups.
- Health page: expiry, drift, the "static token: prefer workload identity" warning, and `secrets.Registry.Entries`.
- Environments, the data-access summary PDF, and four-eyes approval for integration changes.
- **Hosted workspaces.**
  - Take the workspace from the session (today it is always `default`).
  - Give each workspace its own sealing key.
  - Restore and notify for every workspace.
  - AWS: cross-account `AssumeRole` with an external ID.
  - Azure: federated identity or an injectable token source.
  - Google: service-account impersonation.
  - Never long-lived keys; access the customer can revoke.
- **Piperoom Link**: the relay inside the customer network that only connects outward. Hosted customers with private
  databases, Vault, Kubernetes or file mounts need it.
- Slack OAuth install and channel picker (integrations.md §6.1, PR 6).
- Pasting a connection string splits it into fields, so the password can be a reference.

## Phase 3

- AWS one-role discovery and the role card with a CloudFormation template (§6.2, PR 7), plus MSK IAM, Bedrock and Vertex cards.
- Terraform provider and YAML export (references exported as is, typed-in values as `<set me>`).
- Tier-2 secret managers (S6, one PR each): CyberArk Conjur, Akeyless, 1Password, Doppler, Infisical, Delinea,
  Bitwarden.
- Strict hosted mode: Link fetches secrets and opens the connections itself. Needs its own security review.
- SAML and SCIM. More than one credential set (integrations.md §9). MCP servers as agent tools (§10).

## Done when

- Every row in *Before merge* and *Phase-1 gaps* is closed or moved to a later phase by the product owner.
- The tree stays green, and the integration suites pass on the dev stack and on Windows.
