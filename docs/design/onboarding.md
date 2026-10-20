# Design: self-serve onboarding in the app (connections, discovery, people, integrations)

Status: **proposed**, 2026-10-17, against `main` @ `a3acf492`. Every file and symbol named below was read
on that tree. Nothing here is built. Decisions marked **(decide)** are the owner's.

## 1. Problem

Products people compare Piperoom with (Fivetran, Airbyte, Monte Carlo, Datadog, Grafana) onboard in the
app: pick a tile, fill a short form, press **Test connection**, see a green tick or an exact fix, let the
product discover what is there, assign people from a list, done. Value shows on the first screen.

Piperoom onboards through files and restarts:

- ~83 environment variables (`deploy/pilot.env.example`), read once by `config.Load`
  (`internal/config/config.go:223`).
- `owners.yaml` and `users.yaml`, read once at start (`cmd/server/main.go:187`, `owners.Load`,
  `internal/owners/owners.go:105`). Changing an owner means editing a file and restarting.
- Client sets built once at start (`wiring.OpenStacks`, `cmd/server/main.go:364`).
- `cmd/discover` drafts an owners file on the command line; a person copies it in by hand.
- Slack needs a hand-made Slack app and two pasted values; alert sources need hand-written webhooks.
- Docs: `docs/pilot/setup.md` (704 lines), `deploy/README.md` (747), `docs/pilot/permissions.md` (329).

Result: days before a pilot sees anything. Goal: **a useful finding within 30 minutes of first opening
the app**, without weakening any security property Piperoom has today.

## 2. What already exists and makes this cheaper

| Piece | Where | Why it matters |
|---|---|---|
| Per-pipeline connection settings | `owners.Pipeline.Connections`, `owners.ConnRef{Env, File, Value}` (`internal/owners/tenancy.go:64`) | A pipeline can already name its own DSN, brokers, logins. `owners.yaml` never holds a secret: `env:` and `file:` say where it is, `value:` is allowed only for non-secret keys. |
| Per-pipeline client sets | `wiring.Stack`, `wiring.Stacks` (`internal/wiring/stacks.go`), resolved by `pipelineConfig` (`internal/wiring/connections.go:109`) | Isolation by construction: a pipeline's tools hold only its own clients. |
| Clients looked up per use | `room.Deps.StackFor` (`internal/room/service.go:55`), `watch.Deps.ClientsFor` via `roomStacks` / `watchClients` (`cmd/server/tenancy.go:72`) | Rooms and the watcher ask for a pipeline's clients each time, so a client set can be **swapped live** without touching `internal/room` or the watcher's signal code. |
| Discovery | `internal/discover` (`Deps`, `Options`, `Result{Pipelines, Skipped, Warnings}`, `internal/discover/api.go`) | The hard part of "we found 3 pipelines" is written; it needs an API, not a CLI. |
| Admin role | `admin: true` in `users.yaml` (`docs/api.md:32`) | A role for managing connections already exists. |
| Cross-replica signalling | `LISTEN`/`NOTIFY` in `internal/store/hub.go` | The pattern to tell every replica "config changed". |
| Read-only permission recipe | `docs/pilot/permissions.md` | The exact SQL and ACLs a connection test should check and print. |

## 3. Goals and non-goals

Goals:
1. A **setup wizard** on first run and a **Settings** area afterwards: connections, pipelines, people,
   integrations, all editable in the app by an admin.
2. **Test connection** for each connection: reachability, login, and every permission Piperoom needs,
   with the exact fix (one SQL line, one ACL command) when one is missing.
3. **Discovery in the app**: pick the pipelines found, fill the few fields discovery cannot know, assign
   owners from a people list.
4. **Changes apply without a restart**, on every replica.
5. **GitOps stays possible**: export the whole config as YAML, import it, keep file-only mode.
6. Integrations by copy-paste or a button: Slack (manifest link, channel picker), alert sources (webhook
   URL and secret generated).

Non-goals (v1):
- OIDC configuration in the UI (it is how people log in; it stays in env, §6.6).
- A vendor-hosted Slack OAuth broker (§6.9 explains why, and the v2 sketch).
- Model provider / API key in the UI (stays in env; one key per deployment).
- Multi-customer SaaS. One deployment is still one customer (`docs/design/tenancy.md` §2 model (a)).

## 4. Design overview

```
                 ┌──────────────── web (A6) ────────────────┐
                 │ /setup wizard · /settings/{connections,   │
                 │ pipelines, people, integrations}          │
                 └───────────────┬──────────────────────────┘
                                 │ /api/admin/*  (admin only)
                 ┌───────────────▼──────────────────────────┐
                 │ httpapi (A5): config API, connection test,│
                 │ discovery, people                         │
                 └──────┬───────────────┬───────────────────┘
       validate + save  │               │ probe (no save)
  ┌─────────────────────▼───┐   ┌───────▼─────────────────────┐
  │ store (A5):             │   │ connection probes (A2/A3):   │
  │  config_versions (doc)  │   │  build a throwaway client,   │
  │  secrets (encrypted)    │   │  run permission checks       │
  │  config_audit           │   └──────────────────────────────┘
  └──────────┬──────────────┘
             │ NOTIFY piperoom_config (version)
  ┌──────────▼──────────────────────────────────────────────────┐
  │ every replica: reloader (A5 + A4)                            │
  │  load doc → owners.Parse (same validation as owners.Load)    │
  │  → resolve refs (env/file/value/secret) → build changed      │
  │  stacks → atomic swap → close old clients after a grace      │
  └──────────────────────────────────────────────────────────────┘
```

Three ideas carry the design:

1. **The config is one versioned document**, the same YAML shape `owners.yaml` + `users.yaml` have today,
   stored in the room database. Every save is a new version. Validation is the existing parser, so the
   UI cannot create a config the file path would refuse. Diff, rollback and YAML export come free.
2. **Secrets are a fourth kind of `ConnRef`**: `{secret: <id>}`, encrypted in their own table, resolved
   in `pipelineConfig` like `env:` and `file:`. Clients do not change. `env:` and `file:` stay allowed,
   so a security team can keep every secret in its own secret manager.
3. **Reload swaps client sets behind the lookup functions** rooms and the watcher already call.

## 5. Config source modes

`PIPEROOM_CONFIG_SOURCE` **(new, contract `internal/config/config.go`)**:

| Mode | Source of truth | UI | For |
|---|---|---|---|
| `file` (default for existing installs) | `OWNERS_FILE`, `USERS_FILE`, env, as today | Settings pages are **read-only**, connection tests and discovery work, "Export YAML" works | Helm/GitOps users |
| `db` (default for new installs via the image, **(decide)**) | `config_versions` in the room database | Fully editable | Everyone who wants the onboarding flow |

One mode at a time, never both: dual writes would make "which owners.yaml is real?" unanswerable.
`file` → `db` migration: on first start in `db` mode with no version stored, import the files as
version 1 (author `system`, comment `imported from OWNERS_FILE/USERS_FILE`). `db` → `file`: "Export YAML",
then set the mode back. A secret reference (`{secret: …}`) has no meaning in file mode, so export writes
it as `{env: PIPEROOM_SECRET_<ID>}` and lists the variables to set.

## 6. Components

### 6.1 Storage (A5, new migration `00017_config.sql`)

```sql
CREATE TABLE config_versions (
  version     bigint PRIMARY KEY,           -- 1, 2, 3 …
  created_at  timestamptz NOT NULL DEFAULT now(),
  author      text NOT NULL,                -- user id, or 'system'
  comment     text NOT NULL DEFAULT '',
  document    jsonb NOT NULL,               -- {owners: <owners.yaml as JSON>, users: <users.yaml as JSON>}
  doc_sha256  text NOT NULL
);
CREATE TABLE config_current (               -- one row: which version is live
  id int PRIMARY KEY CHECK (id = 1),
  version bigint NOT NULL REFERENCES config_versions(version)
);
CREATE TABLE secrets (
  id          text PRIMARY KEY,             -- 'sec_…', referenced as {secret: sec_…}
  label       text NOT NULL,                -- "orders source DB password"
  ciphertext  bytea NOT NULL,               -- AES-256-GCM(data key, value)
  wrapped_key bytea NOT NULL,               -- data key wrapped by the master key
  key_id      text NOT NULL,                -- which master key wrapped it (rotation)
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  updated_by  text NOT NULL
);
CREATE TABLE config_audit (                 -- append-only, content-free for secrets
  at timestamptz NOT NULL DEFAULT now(), actor text NOT NULL,
  action text NOT NULL,                     -- version.saved | version.rolled_back | secret.set | secret.deleted | test.run
  target text NOT NULL, detail jsonb NOT NULL
);
```

A save runs in one transaction: insert the version, move `config_current`, insert the audit row, then
`NOTIFY piperoom_config, '<version>'`. Retention: keep every version (they are small); a secret row is
deleted only when no version still references it.

### 6.2 Secrets (A5 store, A4 resolver; security review C4/C9)

- **Master key**: `PIPEROOM_SECRET_KEY` (env) or `PIPEROOM_SECRET_KEY_FILE` (path), 32 random bytes,
  base64 **(new, contract)**. v2: a KMS key id (AWS KMS / GCP KMS) instead, for envelope wrapping by the
  cloud. Without a master key, `db` mode refuses to store secrets and the UI says so; `env:`/`file:`
  references still work.
- **Envelope encryption**: one random data key per secret, AES-256-GCM with the secret id as additional
  data (a ciphertext moved to another row fails to decrypt). The master key wraps the data key.
- **Write-only API**: a secret is set or replaced, never read back. The UI shows "set · changed by
  priya, 3 days ago". No API, log, export, error or event ever contains a secret value; errors name the
  secret id and label only, as `pipelineConfig` errors name a variable today.
- **Resolution**: `owners.ConnRef` gains `Secret string \`yaml:"secret"\``; `checkKeys` accepts it;
  `connSource` (`internal/wiring/connections.go:54`) gains a `secret(id)` reader backed by the store.
  `refName` prints `secret:<id>`. `CheckSharedRefs` treats `secret:<id>` like `env:` / `file:`, so two
  tenants sharing one stored credential are caught exactly like two tenants sharing one variable.
- **Rotation**: `POST /api/admin/secrets/rotate-key` rewraps every data key under the new master key
  (old key still in `PIPEROOM_SECRET_KEY_OLD` during the rotation).

### 6.3 Connections as objects (document shape, A3 owners)

Today connection settings live inside each pipeline. The UI needs reusable named connections ("prod
Kafka", "orders source DB") used by several pipelines. The document gets a top-level `connections:` map
and pipelines refer to it:

```yaml
connections:
  prod-kafka:
    kind: kafka                        # kafka | connect | schema_registry | source_db | warehouse
    settings:
      kafka_brokers:            {value: "b-1.prod:9096,b-2.prod:9096"}
      kafka_security_protocol:  {value: SASL_SSL}
      kafka_sasl_username:      {secret: sec_01JA…}
      kafka_sasl_password:      {secret: sec_01JB…}
  orders-db:
    kind: source_db
    settings:
      source_db_url:            {secret: sec_01JC…}
pipelines:
  - id: orders
    uses: [prod-kafka, prod-connect, prod-registry, orders-db, dw]
    # … the rest of the pipeline exactly as today
```

`owners.Parse` **flattens** `uses:` into the pipeline's existing `Connections` map (same keys as
`owners.ConnectionKeys()`), so everything downstream (`pipelineConfig`, stacks, `CheckSharedRefs`,
tenancy) is unchanged. A key set twice through two connections is a validation error. A pipeline may
still carry an inline `connections:` block (file users); `uses:` and inline keys may not overlap.

Scale note: in `db` mode every pipeline has connections, so every pipeline gets its own client set
(tenancy §5 isolation). That is right for a pilot (a handful of pipelines). Sharing one set among
pipelines with identical references is a later optimisation and needs a tenancy decision first.

### 6.4 Connection test (A2: Kafka, Connect, Schema Registry probes; A3: Postgres, warehouse probes; A5: route)

`POST /api/admin/connections/test` with `{kind, settings}` (unsaved values allowed, so the form can test
before saving) or `{connection: "prod-kafka"}` (saved). The server builds a **throwaway** client with the
same constructors `wiring` uses, runs the probes with a 10 s timeout each, closes the client, and
returns:

```json
{"ok": false, "checks": [
  {"id": "reach",  "ok": true,  "ms": 41},
  {"id": "login",  "ok": true},
  {"id": "read_slots", "ok": false,
   "problem": "role piperoom_ro cannot read pg_replication_slots",
   "fix": "GRANT pg_read_all_stats TO piperoom_ro;"},
  {"id": "read_only", "ok": true, "detail": "default_transaction_read_only = on"}]}
```

Probes per kind (each from `docs/pilot/permissions.md`, which becomes their single source):

| Kind | Checks |
|---|---|
| source_db | reach, login, `default_transaction_read_only = on`, `statement_timeout` set, read `pg_replication_slots`, read `pg_publication_tables`, `SELECT` on each source table; **warns** if the role can write |
| warehouse | reach, login, read the sink table's timestamp column |
| kafka | reach (metadata), login, describe topics and groups, list offsets; **warns** if the principal has Write/Alter ACLs it does not need |
| connect | `GET /connectors`, `GET /connectors/{c}/status`, `/config` |
| schema_registry | `GET /subjects`, read one subject's versions and compatibility |

Security (this route makes the server connect where an admin says):
- Admin only; viewers and members get `403`.
- **SSRF guard**: refuse link-local and metadata addresses (`169.254.0.0/16`, `fd00:ec2::254`,
  `metadata.google.internal`) and loopback unless `PIPEROOM_CONNECT_ALLOW_LOOPBACK=1` **(new)**; optional
  allow-list `PIPEROOM_CONNECT_ALLOW_CIDRS` **(new)**. Resolve the host first and check the resolved
  address, so DNS cannot rebind past the guard. The same guard applies to saved connections.
- Responses carry check ids, booleans, timings and fixed problem/fix text with object names only, never
  a server's raw error body or a secret. Rate-limited per admin. Every run writes a `test.run` audit row
  (kind and host, no settings).

### 6.5 Discovery in the app (C10 discover, A5 route, A6 UI)

`POST /api/admin/discover` with `{uses: [connection names]}` runs `discover.Run` (`internal/discover`)
over clients built from those connections and returns `discover.Result` plus, per drafted pipeline, the
fields still missing (`title`, `sink_tables`, `sink_ts_column`, owners). The UI shows a checklist:

```
☑ orders        pg-orders-connector · 1 table · group order-sync     [title____] [sink: analytics.orders ▾]
☑ payments      pg-payments-connector · 3 tables                     [title____] [sink: … ▾]
☐ legacy-crm    skipped: connector is a sink, not a Debezium source
```

Sink table suggestions come from the warehouse connection (list tables whose name matches the source
table). Save writes a new config version with the chosen pipelines. Discovery stays read-only, as the
CLI is today.

### 6.6 People and roles (A5)

- In `db` mode the `users:` part of the document replaces `users.yaml`; same fields (`id`, `name`,
  `layers`, `admin`), same validation.
- **OIDC sign-ins appear as pending people**: today an IdP user who maps to no entry cannot log in
  (`not_provisioned`, `docs/api.md` errors). In `db` mode the login is still refused, but the person is
  recorded in a `pending_people` list (id, name, email, first seen) that an admin can accept with one
  click and give layers to. No one gets access without an admin.
- **Owners are picked from people** in the pipeline form (per layer: approver, backup), writing the same
  `layers`/`owners` structures `owners.yaml` has.
- OIDC settings themselves stay in env: they are how anyone logs in, so a broken UI edit could lock
  everyone out. v2 could add them behind a "test login before saving" step.

### 6.7 First run (A5 + A6)

"The first visitor becomes admin" is how Grafana and Metabase start, and it is unsafe for a server that
may be reachable before setup. Instead:

1. On start in `db` mode with **no admin**, the server prints a one-time **setup token** to its log
   (`setup token: …`, valid 24 h, single use), the pattern Jenkins uses.
2. `/setup` asks for the token, then for the first admin: in `oidc` mode, "sign in now and become
   admin" (the IdP identity is bound to the admin entry); in `dev`/solo mode, a name.
3. The wizard: connections → test → discovery → owners → integrations → health report.
4. The token is burned when the first admin is saved.

### 6.8 Live reload (A5 reloader, A4 wiring swap, A7 watcher)

On `NOTIFY piperoom_config` (and on a 60 s poll as a backstop, like the hub's poll when `LISTEN` is
down), each replica:

1. Loads the current version; skips if `doc_sha256` equals the live one.
2. Parses it with the same code as `owners.Load` (split `owners.Load` into read-file + `owners.Parse`),
   then runs every check the start-up path runs (`validate`, `validateTenancy`, `CheckSharedRefs`,
   libpq-variable refusal). **The API already ran these before saving**; a failure here (e.g. a secret
   key missing on one replica) keeps the old config live and turns `/readyz` amber with the reason.
3. Builds client sets **only for pipelines whose resolved settings changed** (compare a hash of each
   pipeline's resolved, flattened settings).
4. Swaps a `stacksHolder` (`atomic.Pointer[wiring.Stacks]`). `roomStacks` and `watchClients` read
   through the holder, so the next lookup gets the new set. No change in `internal/room`.
5. Closes replaced client sets after a grace period (default 5 min) so a tool call in flight finishes.
   A running agent turn that fetched its registry before the swap finishes on the old clients; its next
   turn gets the new ones.
6. Gives the watcher the new `*owners.Owners` (`watch.Deps.Owners` is read at `watch.New`,
   `internal/watch/watch.go:151` and `:310`): add `Watcher.SetOwners`, applied between ticks.
7. Logs `server: config reloaded version=N changed_pipelines=[…]` (names only).

What reload does **not** change without a restart: env settings (model, OIDC, listen address, Slack
token when it comes from env). The UI says "restart required" next to anything env-backed.

### 6.9 Integrations (C12 Slack, C5 alert intake, A6 UI)

**Slack.** "Add to Slack" with one click needs a Slack app whose OAuth redirect URL is the
deployment's own URL; Slack registers redirect URLs per app and allows no wildcards, so one public app
cannot serve every self-hosted install. v1:
1. A **"Create Slack app" link** that opens Slack's create-from-manifest page with Piperoom's manifest
   (name, bot user, the single `chat:write` scope, plus `channels:read`/`groups:read` only if the
   channel picker is wanted, **(decide)**).
2. Paste the bot token into a secret field (write-only, §6.2).
3. **Channel picker** (needs the read scopes) or paste a channel id, then **Send test message**.
4. Save writes `slack: {token: {secret: …}, channel: C…}` into the document; the bridge reloads.

v2 (SaaS-style customers): a Piperoom-operated OAuth broker that completes the OAuth dance and hands the
token to the customer's instance over a one-time code. It puts the vendor in the token's path, so it is
opt-in and needs its own security review.

**Alert sources.** Per source (Alertmanager, Grafana, Datadog, PagerDuty, Opsgenie, already accepted by
`internal/alertin`): the UI generates the signing secret (stored as a secret), shows the webhook URL and
a copy-paste snippet for that tool, and a **Send test alert** button that opens a shadow-safe test room
(closed automatically).

### 6.10 The first screen: health report

The wizard ends on a read-only health report per pipeline, built from checks Piperoom already has:
slot activity and retained WAL with a growth estimate, connector and task states, `config_risk`
(silent-drop settings), DLQ presence and growth, schema compatibility per subject, consumer lag,
warehouse freshness. It answers "is anything wrong right now?" before any incident and is the reason to
keep going. It is a separate feature (its own small design) that this flow links to.

## 7. API (contract changes in `docs/api.md`, all admin-only)

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/admin/config` | current version: document (secrets as `{secret: id}`), version, author, time |
| GET | `/api/admin/config/versions` / `…/{v}` / `…/{v}/diff?from=` | history and diffs |
| POST | `/api/admin/config` | save a new version `{document, comment, base_version}`; `409 stale` if `base_version` is not current (two admins); `400 validation` with the parser's message |
| POST | `/api/admin/config/rollback` | `{version}` → new version equal to an old one |
| GET / POST | `/api/admin/config/export` / `import` | YAML out (secret refs become `env:` names) / YAML in (validated, saved as a version) |
| PUT / DELETE | `/api/admin/secrets/{id}` | set (body: `{label, value}`) / delete if unreferenced; never returns the value |
| GET | `/api/admin/secrets` | ids, labels, timestamps, which versions reference them |
| POST | `/api/admin/connections/test` | §6.4 |
| POST | `/api/admin/discover` | §6.5 |
| GET / POST | `/api/admin/people/pending`, `…/{id}/accept` | §6.6 |
| POST | `/api/admin/slack/test`, `/api/admin/alerts/{source}/test` | §6.9 |
| POST | `/setup/token`, `/setup/admin` | §6.7, only while no admin exists |

## 8. Web (A6)

New routes in `web/src/App.tsx` (today: `/`, `/rooms/:roomId`, `/shadow`): `/setup`,
`/settings/connections`, `/settings/pipelines`, `/settings/people`, `/settings/integrations`,
`/settings/history`. A **Settings** item in the shell for admins only. Each connection is a tile with a
form, **Test** button and the check list of §6.4. The pipeline form shows a live pipeline-map preview
(topology build over the draft's connections). History shows versions with a diff and **Roll back**.
In `file` mode every form is read-only with a note "managed by OWNERS_FILE; export to edit in the app".

## 9. Security summary (for C4/C9 review)

| Risk | Control |
|---|---|
| Piperoom now stores credentials | Envelope encryption, write-only API, master key outside the database, `env:`/`file:` still allowed, audit rows, content-free logs |
| SSRF through connection tests or saved connections | Admin only, metadata/link-local/loopback refused by resolved address, optional CIDR allow-list, no raw error bodies echoed, rate limit |
| Admin account takeover = config takeover | OIDC for admins, setup token for first admin, every save audited with diff, rollback, `base_version` check |
| A bad save breaks the pipeline's tools | Same validation as start-up before commit; replicas keep the old config if they cannot apply; per-pipeline rebuild only |
| Tenant isolation | `uses:` flattens into the existing per-pipeline `Connections`; `CheckSharedRefs` covers `secret:` refs |
| Read-only promise | Probes only read; the test warns when a credential has more rights than needed |

## 10. Contract changes (lead PR first)

- `internal/config/config.go`: `PIPEROOM_CONFIG_SOURCE`, `PIPEROOM_SECRET_KEY`, `PIPEROOM_SECRET_KEY_FILE`,
  `PIPEROOM_SECRET_KEY_OLD`, `PIPEROOM_CONNECT_ALLOW_LOOPBACK`, `PIPEROOM_CONNECT_ALLOW_CIDRS`.
- `docs/api.md`: §7 routes, `config_reloaded` in health output, new error codes (`stale`, `setup_closed`).
- `internal/owners` (A3, not a contract file but shared): `ConnRef.Secret`, top-level `connections:` and
  `uses:`, `owners.Parse` split out of `owners.Load`.
- `internal/events`: none (config changes are not room events).

## 11. Delivery plan

| Phase | Ships | Owners | Size |
|---|---|---|---|
| **1. See and test** (no storage change) | Settings pages read-only over today's files and env; **Test connection** for every configured connection; YAML export; health report link | A2, A3 (probes), A5 (routes), A6 (UI) | ~1 week |
| **2. Edit in the app** | `db` mode, config versions, secrets, reload and swap, setup token and wizard skeleton, history and rollback | lead (contract), A5, A4, A3, A7, A6; C4/C9 review | ~2 weeks |
| **3. Discover and assign** | discovery in the app, pending people, owners from a list | C10, A5, A6 | ~1 week |
| **4. Integrations** | Slack manifest + channel picker + test; alert source snippets + test alert | C12, C5, A6 | ~3–4 days |

Phase 1 alone fixes the worst of today's experience (people can see what is configured and what is
missing, with exact fixes) without the riskiest change (storing credentials). It is the right first PR
batch whatever is decided about phase 2.

## 12. Decisions for the owner

1. **Default mode for new installs** (§5): `db` (app-first, recommended) or `file`.
2. **Secrets in Piperoom's database** (§6.2): allow (recommended, with a master key outside the DB) or
   require an external secret manager (`env:`/`file:` only, UI stores references).
3. **Slack channel picker** (§6.9): add the `channels:read`/`groups:read` scopes for a picker, or keep the
   single `chat:write` scope and paste a channel id.
4. **Phase 1 first** (§11): ship read-only settings and connection tests before any storage change.

## 13. Known gaps

- Per-pipeline client sets in `db` mode mean one set per pipeline even when pipelines share clusters
  (§6.3); fine for pilots, revisit with tenancy before large installs.
- Env-backed settings still need a restart (§6.8).
- MSK Connect and other backends plug into the same connection kinds when they exist
  (`docs/design/msk.md`); this design adds no new backend.
- The health report (§6.10) needs its own design.
