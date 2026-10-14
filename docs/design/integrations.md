# Design: plug-and-play integrations (rev 1, draft for the leads)

Base: `main` at the head of branch `claude/gifted-planck-gb2o6c`. File and line references were read from that tree.
Nothing here is built yet. Every library call named below is marked **verify with `go doc`** where it was not checked
(AGENTS.md §4 rule 9).

## 1. Problem

Today a team connects Piperoom to its pipeline by hand:

1. Edit about 15–40 environment variables in `deploy/.env` (`deploy/pilot.env.example`, `docs/pilot/setup.md` §1).
2. Write `owners.yaml` and `users.yaml` by hand. `cmd/discover` can draft `owners.yaml`, but only from a shell.
3. Restart. Every client is built once at start-up: `wiring.Open` (`internal/wiring/wiring.go:159`), called from
   `cmd/server/main.go:253`. A wrong URL or a missing grant shows up only as a start-up error or the first failing tool
   call. Connect and Schema Registry are not contacted at all at start-up (`docs/pilot/setup.md` §0).

The goal is to make this work the way Claude's connectors do. A person opens **Settings → Integrations**, picks an app
from a catalog, fills a short form (or clicks "Connect" for apps that support OAuth or a cloud role), sees a checklist
that proves the connection works **and is read-only**, and saves. It takes effect with no restart.

## 2. Goals and non-goals

Goals (v1):

- A catalog of integrations, each a card: logo, one line, status (`not connected`, `connected`, `failing`, `managed by
  environment`).
- A form for each card, generated from a schema on the server. The web app holds no per-app code.
- **Test connection** before save: a list of checks with pass, warn or fail, for example "can reach host",
  "login works", "role cannot INSERT", "can read `pg_replication_slots`".
- Save without a restart: the new client replaces the old one while the server runs.
- Secrets are encrypted at rest, write-only through the API, never logged, and never sent back to the browser.
- A server with **no** data-stack settings still starts, in setup mode, and shows the first-run wizard.
- Pipelines can be added from the UI: run discovery on the connected apps, pick the pipelines, name the layer owners.
- Environment variables keep working. A pilot that is configured today does not change.

Non-goals (v1):

- Write access set up from the UI. Integrations are read-only by default. Write actions stay off unless the owner of
  that layer turns them on, one layer at a time (same rule as `docs/pilot/setup.md` §7).
- One credential set per pipeline (F7). v1 keeps one set per process. §9 shows how the data model allows more later.
- A hosted multi-tenant Piperoom. This design is for one self-hosted install. §6 says what changes if Piperoom is
  hosted.
- Inbound Slack (approvals in Slack). Slack stays outbound only (`internal/slackbridge/bridge.go:3-4`).

## 3. The catalog

What "a few clicks" means depends on what the app supports. Kafka and Postgres have no OAuth, so for them the fastest
honest path is: paste the host and the login, click **Test**, click **Save**. Piperoom closes the gap by generating
the exact grant script or cloud template, so the person does not have to write one.

| Category | Card | Connect flow | Clicks after the account exists |
|---|---|---|---|
| Source database | PostgreSQL (self-hosted, RDS, Aurora, Cloud SQL) | Form: host, port, db, user, password, TLS mode. **"Copy grant script"** button shows the read-only `CREATE ROLE … GRANT SELECT` SQL from `docs/pilot/permissions.md`, filled with the chosen tables | Paste → Test → Save |
| Kafka | Self-hosted / Strimzi | Form: brokers, protocol, SASL mechanism, user, password, CA | Paste → Test → Save |
| Kafka | Confluent Cloud | Form: bootstrap, API key, API secret. Protocol fixed to `SASL_SSL` + `PLAIN` | 3 fields → Test → Save |
| Kafka | AWS MSK (IAM) | **"Launch stack in AWS"** button → CloudFormation quick-create link that makes a read-only IAM role with an external ID; paste the role ARN back (§6.2). Uses `AWS_MSK_IAM` (`docs/design/msk.md`) | Launch → Create → paste ARN → Save |
| Kafka Connect | Connect REST | Form: URL, user, password, CA | Paste → Test → Save |
| Kafka Connect | MSK Connect | Same AWS role as MSK (`docs/design/msk.md` §3a) | 0 extra if MSK is connected |
| Schema Registry | Confluent / Apicurio / Karapace | Form: URL, API key, secret | Paste → Test → Save |
| Warehouse | PostgreSQL / Redshift | Form, same as source, plus a grant script | Paste → Test → Save |
| Warehouse | Snowflake | Form: account, user, role, warehouse, db, schema; key-pair or password. **"Copy setup SQL"** creates a read-only role | Paste → Test → Save |
| AI model | Anthropic | API key field | 1 field → Test → Save |
| AI model | AWS Bedrock | Region, model id; reuses the AWS role | 2 fields |
| AI model | Google Vertex AI | Project, region; service-account JSON upload or workload identity | Upload → Save |
| Alerts | Alertmanager, Grafana, PagerDuty, Datadog, generic | **No form.** Piperoom generates the secret, shows the webhook URL and a copy-paste snippet for that tool (Alertmanager `receivers:` YAML, a Grafana contact point, and so on) | Copy → paste in the tool |
| Chat | Slack | **"Add to Slack"**: OAuth v2 install (§6.1), then pick a channel from a list | Click → Allow → pick channel |
| Agent tools | Runbooks (built in), later MCP servers (§10) | — | — |

Each card also shows **what Piperoom will read** from that app (taken from `docs/pilot/data-handling.md`) before the
person saves. People should know what they are agreeing to.

## 4. Architecture

```
web: Settings → Integrations ──► /api/integrations/*  (admin only)
                                     │
                                     ▼
                         internal/integrations
              ┌──────────────┬───────────────┬──────────────────┐
              │ Kind registry│  Store (DB,   │   Tester         │
              │ (schema,     │  AES-GCM      │   (checks with   │
              │  build, test)│  secrets)     │    deadlines)    │
              └──────┬───────┴───────┬───────┴──────────────────┘
                     │ build         │ change notify
                     ▼               ▼
              internal/wiring.Live  ── swaps clients behind stable interfaces
                     │
     tools, actions, topology, watcher, discover (unchanged: they hold the stable wrappers)
```

### 4.1 Kinds

New package `internal/integrations` (new owner row, §11). A **kind** is one card, and its code registers itself:

```go
// Kind describes one integration card: its form, how to test it and how to build its client.
type Kind struct {
	ID       string   // "postgres-source", "kafka-confluent-cloud", "slack", ...
	Category Category // source_db | kafka | connect | registry | warehouse | model | alerts | chat
	Title    string
	Summary  string
	Reads    []string // what Piperoom reads, shown before save
	Fields   []Field
	// Test runs the checks on unsaved values. It never writes to the target.
	Test func(ctx context.Context, v Values) []Check
	// Build constructs the client. The returned close func releases pools and connections.
	Build func(ctx context.Context, v Values) (client any, close func(), err error)
}

// Field is one form input. Secret fields are write-only through the API.
type Field struct {
	Name, Label, Help string
	Type              FieldType // text | secret | number | select | bool | pem | json_file
	Required          bool
	Default           string
	Options           []string // for select
	Pattern           string   // validated on the server; the browser check is only a hint
}

// Check is one line of the test checklist.
type Check struct {
	ID     string // "reach", "login", "read_only", "slots_visible", ...
	Title  string
	Status string // pass | warn | fail | skipped
	Detail string // short; never contains a secret or a full record value
}
```

The web app renders the form from `GET /api/integrations/catalog`. Adding a kind is server-only work.

`Build` reuses the constructors that exist today: `postgres.New`, `openWarehouse`, `connect.NewSecure`,
`schemareg.NewSecure` and `kafka.NewSecure` with `netsec` settings (`internal/wiring/wiring.go:88-155`). The kinds are
thin presets over them. For example, Confluent Cloud is `kafka.NewSecure` with the protocol fixed to `SASL_SSL` and the
mechanism fixed to `PLAIN`.

### 4.2 Checks that prove read-only

The test is the part that saves the most time. It replaces "start it and read the error". Each check has its own
deadline (default 5 s, the whole test 20 s). The checks never write:

| Kind | Checks |
|---|---|
| Postgres source | TCP reach; TLS handshake; login; `server_version` ≥ 13 (watcher, `docs/pilot/setup.md` §1); `pg_is_in_recovery()` is false (watcher needs the primary); `SELECT` on `pg_replication_slots`; for each listed table `has_table_privilege(…, 'SELECT')` is true **and** `'INSERT'`, `'UPDATE'`, `'DELETE'` are false; `default_transaction_read_only` or a `statement_timeout` is set (warn if not) |
| Warehouse | the same privilege checks on the sink tables; Snowflake: `SHOW GRANTS TO ROLE` has no write privilege (**verify** the result shape) |
| Kafka | metadata request lists brokers; `DescribeGroups` works; warn when the principal is allowed to produce (ACL describe, when the cluster allows it; otherwise `skipped` with the reason) |
| Connect | `GET /connectors` returns 200; warn if no Debezium Postgres connector is found |
| Registry | `GET /subjects` returns 200 |
| Model | one tiny request (max 1 output token) to the chosen model; shows the model id that answered |
| Slack | `auth.test`, then `conversations.info` on the channel; the bot is a member |

`has_table_privilege` is a standard Postgres function, so the read-only check needs no extra grant. "Role cannot write"
becomes a green check instead of a manual step in `docs/pilot/setup.md` §5.3.

### 4.3 Storage

New migration (A5): `migrations/000NN_integrations.sql`.

```sql
CREATE TABLE integrations (
  id          text PRIMARY KEY,          -- "int_<random>"
  category    text NOT NULL,
  kind        text NOT NULL,
  scope       text NOT NULL DEFAULT '*', -- '*' = whole process; a pipeline id later (§9)
  config      jsonb NOT NULL,            -- non-secret fields only
  secrets     bytea,                     -- AES-256-GCM: nonce || ciphertext, over a JSON object
  key_id      text,                      -- which key sealed `secrets`
  version     bigint NOT NULL DEFAULT 1, -- optimistic lock (If-Match)
  enabled     boolean NOT NULL DEFAULT true,
  last_test   jsonb,                     -- []Check from the last test, no secrets
  updated_by  text NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (category, scope)               -- v1: one active integration per category
);

CREATE TABLE integration_audit (
  seq         bigserial PRIMARY KEY,
  integration text NOT NULL,
  action      text NOT NULL,             -- created | updated | tested | enabled | disabled | deleted | secret_rotated
  actor       text NOT NULL,
  changed     text[] NOT NULL,           -- field NAMES only, never values
  at          timestamptz NOT NULL DEFAULT now()
);
```

Secrets are sealed with `crypto/aes` + `crypto/cipher` (GCM) from the standard library, with the row id and the
category as additional data, so a ciphertext copied onto another row does not open. The key is
`PIPEROOM_SECRET_KEY` (32 bytes, base64), injected from the customer's secret manager. Without a key, the Integrations
page is read-only, and it tells the admin what is missing. Key rotation: `PIPEROOM_SECRET_KEY_PREVIOUS` is tried on
read, and each row is re-sealed on its next save. `key_id` is the first 8 hex characters of SHA-256 of the key.

PEM files (CA, client certificate, key) are stored as secret fields. `netsec.TLS` takes file **paths** today
(`internal/clients/netsec/netsec.go:61-66`). That needs a contract-adjacent change (§11, PR 2): add PEM-bytes fields to
`netsec.TLS`, so nothing secret is written to disk.

### 4.4 Precedence with environment variables

For each category: **if any of its environment variables is set, the environment wins**. The card shows
`managed by environment` and is read-only. Existing pilots keep working, and an operator who wants everything in a
secret manager can still do that. A start-up log line names the source of each category, for example
`integrations: kafka from env, connect from db, source_db not configured`. The line contains no values.

### 4.5 No-restart swap

Tools, actions, the topology builder and the watcher receive their clients once, at registration
(`wiring.RegisterTools`, `internal/wiring/wiring.go:223`). Instead of rebuilding all of them, `wiring` gains a `Live`
holder (A4) with one **swappable wrapper per client interface**. Each wrapper implements the contract interface from
`internal/clients/clients.go` and forwards each call to the current client, held in an `atomic.Pointer`:

```go
// swapConnect is a clients.ConnectClient that forwards to the current client.
type swapConnect struct{ cur atomic.Pointer[connectSlot] }

type connectSlot struct {
	c     clients.ConnectClient // nil = not configured
	close func()
	inUse sync.WaitGroup
}

func (s *swapConnect) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	slot := s.cur.Load()
	if slot == nil || slot.c == nil {
		return clients.ConnectorStatus{}, ErrNotConfigured // wraps a sentinel; the tool says "Kafka Connect is not connected"
	}
	slot.inUse.Add(1)
	defer slot.inUse.Done()
	return slot.c.Status(ctx, name)
}
```

(The `Add` after `Load` races with the drain; the real code takes a reference count under the slot's own mutex. The
sketch shows the shape only.)

On save: `Build` the new client, `Test` it, store the new slot, then close the old one when its in-flight calls end,
or after 30 s, whichever comes first. A failed build leaves the old client in place, and the API returns the failed
checks. Calls in progress finish on the old client. A room in progress sees the new connection on its next tool call,
and its timeline gets a system message: `Kafka Connect integration updated by priya`.

`ErrNotConfigured` lets the server start with nothing connected (setup mode). Tools for a missing category answer
"not connected" instead of failing the run. The agent's prompt lists the connected categories, so it does not plan
around tools it cannot use. Write actions on a client that is not configured are refused at proposal time.

The model endpoint (`wiring.NewAgentEndpoint`, `cmd/server/main.go:245`) is swapped the same way, behind its `Run` and
`WriteHandoff` funcs. The per-room token counts move to the new endpoint, so a key change does not reset the budget.

### 4.6 Pipelines from the UI

After the Connect and Kafka cards are green, the wizard offers **Discover pipelines**. It runs `discover.Discover`
(`internal/discover`, read-only by type) through the live clients and shows the draft pipelines as a list of cards
with checkboxes: connector, topic prefix, tables, consumer groups. The admin ticks the ones to watch, picks the sink
tables from a list (read from the warehouse's `information_schema`), and picks an owner per layer from the user list.

Storing pipelines is phase 3. `*owners.Owners` is loaded once at start (`internal/owners/owners.go:56`) and shared by
the room service, tools, topology, watcher and alerts, so live edits need the same swap pattern and an owner's sign-off
(A3). Until then, the wizard's last step is **Download owners.yaml** with a restart notice. That is still far fewer
steps than writing the file by hand.

## 5. API (contract change to `docs/api.md`)

All routes are under `/api/integrations` and need the **admin** role (§7). The JSON shapes follow §4.1.

| Route | Does |
|---|---|
| `GET /catalog` | every kind: fields, category, reads, logo id |
| `GET /` | configured integrations: config without secrets, `secrets_set: ["password"]`, status, `last_test`, `source: db \| env` |
| `POST /test` | `{kind, config, secrets}` → `[]Check`. Tests unsaved values; stores nothing |
| `POST /` | create. Runs the test first; refuses with `422` and the checks if any check is `fail` (warns are allowed) |
| `PUT /{id}` | update with `If-Match: <version>`. An empty secret field keeps the stored value; `null` clears it |
| `POST /{id}/enable`, `/disable` | disable keeps the row and swaps in "not configured" |
| `DELETE /{id}` | removes the row and the client |
| `GET /{id}/audit` | the audit rows |
| `POST /discover` | runs discovery on the live clients, returns the draft pipelines |
| `GET /alerts/{source}/setup` | the webhook URL and the snippet; a `rotate` POST makes a new secret, shown **once** |
| `GET /slack/install`, `GET /slack/callback` | OAuth v2 (§6.1) |

Every response passes through one encoder that drops secret fields, and a unit test proves that no secret value ever
appears in any route's output.

## 6. Flows that use OAuth or a cloud role

### 6.1 Slack ("Add to Slack")

Self-hosted Piperoom cannot ship one Slack app shared by every customer, because the OAuth redirect must reach each
install. So:

1. The card has **Create Slack app**. It opens `https://api.slack.com/apps?new_app=1&manifest_json=<url-encoded
   manifest>`, prefilled with the bot scopes `chat:write` and `channels:read`/`groups:read` (for the channel picker) and
   the redirect URL `<PIPEROOM_PUBLIC_URL>/api/integrations/slack/callback`. (**Verify** the `manifest_json` query
   parameter against current Slack docs before building.)
2. The admin pastes the app's client id and client secret into the card, then clicks **Add to Slack**. That is a
   standard OAuth v2 install (`oauth.v2.access`) with a `state` bound to the admin's session, reusing the login-state
   table pattern from `internal/store/loginstates.go`.
3. Piperoom stores the bot token as a secret and shows the channels the bot can see. The admin picks one.

If Piperoom is ever hosted by us, step 1 goes away: one published Slack app, and it is one click.

### 6.2 AWS (MSK, MSK Connect, Bedrock)

**Launch stack in AWS** opens a CloudFormation quick-create URL
(`https://console.aws.amazon.com/cloudformation/home#/stacks/quickcreate?templateURL=…&param_ExternalId=…`) with a
template that creates one IAM role. The role has the read-only policy from `docs/design/msk.md` §2(f) and a trust
policy for the Piperoom host's own role, with an `sts:ExternalId` condition. Piperoom generates the external ID per
install and shows it. The admin pastes the role ARN back. Piperoom then uses `sts:AssumeRole` through the shared
`aws.Config` (`wiring.LoadAWS`, `internal/wiring/aws.go`), with `stscreds.NewAssumeRoleProvider` (**verify** with
`go doc github.com/aws/aws-sdk-go-v2/credentials/stscreds`). The template is served by the server, so it matches the
running version.

This does not remove the need for the Piperoom host to have AWS credentials of its own (an instance or task role).
The card checks for them and says so.

### 6.3 Snowflake and Google

Snowflake: key-pair login. The card generates an RSA key pair on the server, stores the private key as a secret, and
shows `ALTER USER … SET RSA_PUBLIC_KEY='…'` with the read-only role SQL to paste into Snowflake. Google Vertex AI:
upload the service-account JSON, or choose "workload identity" when Piperoom runs on GKE.

## 7. Who may change integrations

There is no admin role today. The roles are viewer, member and auditor (`docs/design/auth.md` §2.1). The new role:

- **admin**: a principal listed in `admins:` at the top of `owners.yaml` (`user:` or `group:`, like `auditors:`),
  or in `PIPEROOM_ADMINS` (comma list), for a first install that has no `owners.yaml` yet.
- In **dev** auth mode anyone can pick any user (F1). So in dev mode the integration write routes work **only** when
  the server listens on loopback (the check in `cmd/server/listen.go`); otherwise they answer `403 admin_needs_oidc`.
  Reading the catalog is always allowed.
- In **solo** mode the solo user is the admin.
- Every write is logged to `integration_audit` and posted as a system message in every open room. Slack gets the same
  notice when it is connected, with field names and never values.

## 8. Security

| Risk | Mitigation |
|---|---|
| Secret leak through the API | write-only fields; one response encoder strips secrets; test over every route (§5) |
| Secret leak through logs | values held as `netsec.Secret` from parse to use (`internal/clients/netsec/netsec.go:34-50`); checks report field names only |
| Secret at rest | AES-256-GCM, key outside the database, row-bound additional data (§4.3) |
| SSRF through **Test** (the server dials any host a user types) | admin only; per-check deadlines; at most 10 tests a minute per admin; response bodies are never echoed, only status codes and short errors; optional `PIPEROOM_INTEGRATION_ALLOWED_HOSTS` (CIDR or host list) for strict installs; link-local `169.254.0.0/16` and `fd00:ec2::254` are blocked always (cloud metadata) |
| Changing a credential mid-incident | the swap posts a system message in open rooms; in-flight calls finish on the old client; the audit row names the actor |
| A role that can write slips through | the read-only checks fail the save for sources and warehouses (§4.2); a person can override them only with `enabled_write_layers`, which is a separate, audited toggle per layer |
| Stolen database dump | secrets cannot be opened without `PIPEROOM_SECRET_KEY` |
| CSRF | the existing same-origin and content-type rules apply (`docs/pilot/setup.md` §4) |

C4/C9 (security) should review this section before PR 1 merges.

## 9. Later: more than one credential set (F7)

The `scope` column is `'*'` in v1. Later, `scope = '<pipeline id>'` lets one pipeline use its own source database or
Kafka cluster. `Live` then keeps a map of wrappers per scope, and the tool's scope check (`internal/tools/scope.go`)
picks the client from the room's pipeline. The table needs no change for this. Only the `UNIQUE (category, scope)`
rule and the wiring grow.

## 10. Later: MCP servers as agent tools

Claude Code's connectors are MCP servers. The same idea fits here as one more card, **Custom tool (MCP)**: a URL and
an auth header. Piperoom lists the server's tools, and the admin ticks which ones the agent may call. Rules: only
tools marked read-only by the admin are offered; every result goes through the agent's untrusted-data wrapper
(AGENTS.md §7); results are capped at `tools.MaxSummaryBytes`; MCP tools never take part in `propose_fix`. This is
phase 4 and needs its own design, because it lets outside code put text into the prompt.

## 11. Contract changes and PR split

Contract changes (lead commit, PR 0):

- `internal/config/config.go`: `SecretKey` (`PIPEROOM_SECRET_KEY`), `SecretKeyPrevious`
  (`PIPEROOM_SECRET_KEY_PREVIOUS`), `Admins` (`PIPEROOM_ADMINS`), `IntegrationAllowedHosts`
  (`PIPEROOM_INTEGRATION_ALLOWED_HOSTS`).
- `docs/api.md`: the routes in §5, `/api/me` gains `admin: bool`, new errors `403 admin_needs_oidc`,
  `409 version_conflict`, `422 checks_failed`.
- `internal/events/events.go`: a `SystemMessage` reason `integration_changed` (or reuse the plain system message, so
  there is no contract change; the lead decides).
- AGENTS.md §2: a new owner row for `internal/integrations/**`.

| PR | Owner | What | Done when |
|---|---|---|---|
| 0 | Lead | contract changes above | `go build ./...` |
| 1 | new owner | `internal/integrations`: `Kind`, `Field`, `Check`, sealing (AES-GCM, rotation), registry, kinds for postgres source, warehouse (postgres, snowflake), kafka (generic, confluent cloud), connect, registry, anthropic; testers with fakes and `httptest` | unit tests for every check's pass, fail and timeout path; no secret in any `String()`, `%v` or JSON |
| 2 | A2 | `netsec.TLS` accepts PEM bytes as well as paths | table tests; paths still work |
| 3 | A4 | `wiring.Live`: swappable wrappers for the five clients and the model endpoint; `ErrNotConfigured`; start with nothing configured; env-wins precedence | race test (`go test -race`): swap during calls, old client closed exactly once, no goroutine leak |
| 4 | A5 | migration, store, `/api/integrations/*`, admin role, audit, system message on change, setup mode in `cmd/server` | API tests for the admin rule, `If-Match`, secret stripping, `422` on failed checks |
| 5 | A6 | Settings → Integrations: catalog grid, schema-driven form, live checklist, first-run wizard, alert snippets | Vitest for the form renderer and the checklist; `npm run build` and `npm run lint` |
| 6 | A5 + A6 | Slack OAuth install and channel picker (§6.1) | state is single use and bound to the session; tested with `httptest` |
| 7 | A4 | AWS role card with the CloudFormation template (§6.2); after MSK PR 1 | fake STS test; template passes `cfn-lint` if available |
| 8 | A3 + A5 + A6 | Discover pipelines in the UI, owners download (phase 2); live owners in the DB (phase 3, separate design review) | — |

PRs 1, 2 and 5 can start at once against the contracts. PR 3 needs PR 1's interfaces. PR 4 needs PRs 1 and 3.

## 12. Open questions for the product owner

1. **Self-hosted only, or will we host Piperoom?** Hosting turns Slack (§6.1) and AWS (§6.2) into true one-click flows,
   but it changes the data-handling story (`docs/pilot/data-handling.md`). This design assumes self-hosted.
2. **Should a failed read-only check block the save, or only warn?** The draft blocks the save for databases and only
   warns for Kafka (where ACLs often cannot be read).
3. **Which cards ship first?** Suggested: Postgres, Confluent Cloud, Connect REST, Schema Registry, Anthropic, Slack,
   alert snippets. Then Snowflake and the AWS role.
