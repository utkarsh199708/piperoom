# Tenancy — design rev 2 (main @ 09473b2, 2026-10-13)

Closes security finding F7 (one process-wide credential set for every pipeline,
`docs/security/findings.md`) and the visibility gap next to it (any signed-in user can list and open
every room). The decisions in this document are LEAD-ARCH's; they are not open for review here.
Implementation: PR `[T1] tenancy: per-pipeline client sets and tenant visibility` (#112), stacked on
the contract PR `[LEAD] contract: tenancy` (#111) that holds this file, `PIPEROOM_TENANCY_MODE` in
`internal/config/config.go` and the `docs/api.md` lines. #112 also carries the DLQ watcher (#102) and
`dlq_peek` (#103) ported to per-pipeline client sets, so it merges after them.

**Rev 2 (review of #111/#112).** A null `tenants:`, `connections:` or `tenant:` is refused (3.1, 3.2).
The four literal keys are checked before a client sees them, so a wrong reference never echoes a
secret (3.2). TLS key and certificate files count by their resolved path, wherever they were named, and
CA bundles are not references (3.3). The libpq `PG*` variables and home files stop the start when a
pipeline has its own connections (3.5). An unknown or empty pipeline always gets the empty set (5).
DLQ topic names are compared per Kafka cluster (5). New known gaps in section 8.

## 1. Problem

- `internal/config/config.go` has one `SOURCE_DB_URL`, `WAREHOUSE_DB_URL`, `KAFKA_BROKERS`,
  `CONNECT_URL` and `SCHEMA_REGISTRY_URL` (and one set of Kafka, Connect and Schema Registry logins)
  for the whole process. `wiring.Open` builds one client set from them and every pipeline in
  `owners.yaml` uses it: the read tools, the topology builder, the watcher and the write actions.
  F11 scoped what a tool may *name*; the credentials behind it still reach every pipeline's systems.
- Every signed-in user sees every room: `GET /api/rooms` lists all of them and every
  `/api/rooms/{id}/...` route opens any of them.

## 2. Supported models

| Model | When | Boundary |
|---|---|---|
| **(a) One deployment per tenant** | separate customers or legal entities | **The only supported hard boundary.** Separate process, room database, model key and cost, identity-provider client, Slack app, alert secrets and AWS/GCP identity. |
| **(b) In-process tenants** | teams within one organisation that trust the same operator | One deployment. A tenant is a named set of pipelines. Each pipeline can have its own credentials, and a user sees only the rooms of their tenants. Not a hard boundary: one process holds every tenant's credentials, and one room database holds every tenant's rooms. |

**Model (a) in practice.** Nothing in the code is shared between two deployments, so nothing in this
document is needed for it: give each deployment its own `owners.yaml` with only that tenant's pipelines.

- Helm (`deploy/helm/piperoom`): install the chart once per tenant, each release in its own namespace
  with its own Secret (`secrets.existingSecret`), values file, ingress host, OIDC client and
  NetworkPolicy:
  `helm install piperoom-<tenant> deploy/helm/piperoom -n piperoom-<tenant> --create-namespace -f <tenant>-values.yaml --set-file files.owners=<tenant>-owners.yaml --set-file files.users=<tenant>-users.yaml`.
- Terraform (`deploy/terraform/gcp`): one `terraform apply` per tenant, each with its own state and
  its own `project_id` (the variable already asks for a dedicated project) and `name_prefix`.
- Docker (`deploy/docker-compose.pilot.yml`): one compose project per tenant (`-p piperoom-<tenant>`)
  with its own `deploy/.env`, room database and published port.

The rest of this document is model (b).

## 3. `owners.yaml`

```yaml
tenants:                         # optional; present = the tenant filter is on
  payments:
    members: [group:payments-eng, user:li]
  growth:
    members: [group:growth-data]

pipelines:
  - id: payments-cdc
    tenant: payments             # optional; default "default"
    connections:                 # optional; absent = the process-wide settings
      source_db_url:       {env: PAYMENTS_SOURCE_DB_URL}
      warehouse_db_url:    {file: /run/secrets/payments/warehouse-dsn}
      kafka_brokers:       {env: PAYMENTS_KAFKA_BROKERS}
      connect_url:         {env: PAYMENTS_CONNECT_URL}
      schema_registry_url: {env: PAYMENTS_SCHEMA_REGISTRY_URL}
      kafka_security_protocol: {value: SASL_SSL}
      kafka_sasl_mechanism:    {value: SCRAM-SHA-512}
      kafka_sasl_username: {env: PAYMENTS_KAFKA_USER}
      kafka_sasl_password: {file: /run/secrets/payments/kafka-password}
      kafka_tls_ca_file:   {file: /run/secrets/payments/kafka-ca.pem}
    shared_connections: false    # optional; see 3.3
    # topic_prefix, source_tables, connectors, owners, watch, shadow ... as before
```

### 3.1 `tenant:` and `tenants:`

- A tenant is a name (`^[a-zA-Z0-9_.-]+$`, like a pipeline id). A pipeline with no `tenant:` is in
  tenant `default`.
- `tenants:` maps a tenant name to `{members: [principals]}`; principals are `user:<id>` or
  `group:<name>` as in `layers:`. `members:` may be empty or missing: the tenant then holds only the
  owners of its pipelines (3.4).
- With a `tenants:` block, a pipeline's `tenant:` must be a key of it (`default` may be left out).
  Without one, a pipeline may only say `tenant: default`: naming another tenant without declaring
  it is refused, so a forgotten block cannot silently show that tenant's rooms to everyone.
- An empty `tenants: {}` is refused (write no block for a single tenant), and so is a null one:
  `tenants:` with no value, `tenants: ~`, or every entry commented out. A null `tenant:` on a pipeline
  is refused too (it would quietly mean `default`). YAML decoders skip a null value, so these are found
  on the parsed node tree (`owners.checkNullBlocks`). Unknown keys inside a tenant are refused, like
  every other `owners.yaml` block.
- An error never quotes a key or a tenant name that is not a plain name (`^[A-Za-z0-9_.-]{1,64}$`): a
  DSN pasted in the wrong place must not reach the start-up log.
- Tenant members are principals that `owners.yaml` names, so solo mode (`PIPEROOM_SOLO_USER`)
  refuses to start when `tenants:` names anyone else.

### 3.2 `connections:`

`connections:` names where each of the pipeline's connection settings comes from. **It never holds a
secret.** Each key is the lower-case name of the process-wide variable it replaces; each value is
exactly one reference:

| Reference | Meaning |
|---|---|
| `{env: NAME}` | the value of environment variable `NAME` (`^[A-Za-z_][A-Za-z0-9_]*$`). It must be set and not empty at start. |
| `{file: /abs/path}` | an absolute path. For the `*_file` keys (TLS material) it is the path of that PEM file. For every other key the value is the file's content, trimmed of surrounding white space, at most 64 KiB; it must be readable and not empty at start. |
| `{value: X}` | a literal. Allowed only for the non-secret keys marked "literal" below. |

| Key | Replaces | Notes |
|---|---|---|
| `source_db_url` | `SOURCE_DB_URL` | required |
| `source_db_write_url` | `SOURCE_DB_WRITE_URL` | optional, `cdc` pipelines only (P1a-3b, P1a-3c; #665, #673): the source Postgres as its separate write role, for the pg write actions. `{env}` or `{file}` only, never a literal. `owners.Load` refuses it when it is the same reference as `source_db_url`; `wiring` resolves it at start and refuses it when it logs in as the same role (same user, host and port) as `source_db_url`. Both checks run in either write mode, so the mistake stops the start; the writer is built from it only with `PIPEROOM_WRITE_MODE=on` (off, the pipeline's set drops it). Without it the pg write actions refuse at Execute ("no write credentials") |
| `warehouse_db_url` | `WAREHOUSE_DB_URL` | required |
| `warehouse_kind` | `WAREHOUSE_KIND` | literal allowed; empty = `postgres` |
| `kafka_brokers` | `KAFKA_BROKERS` | required |
| `kafka_security_protocol` | `KAFKA_SECURITY_PROTOCOL` | literal allowed |
| `kafka_sasl_mechanism` | `KAFKA_SASL_MECHANISM` | literal allowed |
| `kafka_sasl_username`, `kafka_sasl_password` | `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | |
| `kafka_tls_ca_file`, `kafka_tls_cert_file`, `kafka_tls_key_file` | `KAFKA_TLS_*_FILE` | path |
| `kafka_tls_server_name` | `KAFKA_TLS_SERVER_NAME` | literal allowed |
| `connect_url` | `CONNECT_URL` | required |
| `connect_username`, `connect_password` | `CONNECT_USERNAME`, `CONNECT_PASSWORD` | |
| `connect_tls_ca_file`, `connect_tls_cert_file`, `connect_tls_key_file` | `CONNECT_TLS_*_FILE` | path |
| `schema_registry_url` | `SCHEMA_REGISTRY_URL` | required |
| `schema_registry_username`, `schema_registry_password` | `SCHEMA_REGISTRY_USERNAME`, `SCHEMA_REGISTRY_PASSWORD` | |
| `schema_registry_tls_ca_file`, `schema_registry_tls_cert_file`, `schema_registry_tls_key_file` | `SCHEMA_REGISTRY_TLS_*_FILE` | path |

- **No inheritance.** A pipeline with `connections:` uses only what it lists. A key it leaves out is
  empty (PLAINTEXT, no login, the system CA roots), never the process-wide value: one tenant's broker
  must never be sent another tenant's password.
- A pipeline with no `connections:` uses the process-wide settings, which keeps today's behaviour. A
  null `connections:` (no value, `~`, or every entry commented out) is refused, not read as "no
  block": it would silently fall back to the process-wide credentials, in teams mode too.
- `owners.Load` checks the shape (known key, exactly one reference, literal only where allowed,
  absolute path, variable-name syntax, the five required keys). `wiring` resolves the references at
  start. Any failure stops the start with an error that names the pipeline, the key and the
  reference (variable name or path), **never the value**.
- "Never the value" covers the four literal keys too when they come from `{env}` or `{file}`: a
  reference to the wrong variable may hold a DSN or a password, and the client's own error would quote
  it. `wiring` checks them before any client sees them, against what the clients accept:
  `warehouse_kind` `postgres`|`snowflake`; `kafka_security_protocol`
  `PLAINTEXT`|`SSL`|`SASL_PLAINTEXT`|`SASL_SSL`; `kafka_sasl_mechanism`
  `PLAIN`|`SCRAM-SHA-256`|`SCRAM-SHA-512`|`AWS_MSK_IAM` (both without regard to case); and
  `kafka_tls_server_name` a host name. The refusal names the key and the reference only.
- MSK IAM (`kafka_sasl_mechanism` = `AWS_MSK_IAM`) uses the process's one AWS identity; see 3.3.

### 3.3 Shared references

Two tenants must not share a credential by accident. At start, every reference is collected per
tenant:

- `{env: NAME}` and `{file: path}` are references; `{value: X}` is not. Variable names compare
  without regard to case (Windows), paths after `filepath.Clean`, also without regard to case.
- A pipeline with no `connections:` references the process-wide variables: `env:SOURCE_DB_URL`,
  `env:KAFKA_SASL_PASSWORD` and so on, one per key in 3.2 except the CA bundles.
- **TLS keys and certificates** (`*_tls_key_file`, `*_tls_cert_file`) also count by the path a client
  really opens: the resolved value, made absolute and cleaned, with symbolic links followed when the
  file exists. So a key reached through two names (a process-wide `KAFKA_TLS_KEY_FILE` and another
  tenant's `{file:}`, or one tenant's `{env:}` holding the path and another's `{file:}`, or a link) is
  one shared credential. This is added at resolution, for the process-wide settings too.
- **CA bundles are excluded** (`*_tls_ca_file`, LEAD-ARCH decision): a CA is public, and one corporate CA
  for every team is normal.
- A pipeline whose resolved `kafka_sasl_mechanism` is `AWS_MSK_IAM` references the process AWS
  identity; so does every pipeline without `connections:` when the process-wide mechanism is
  `AWS_MSK_IAM`.

When one reference is used by pipelines of two or more tenants, the start fails unless **every**
pipeline that uses it sets `shared_connections: true`. The error names the reference, the tenants and
the pipelines. Pipelines of one tenant may share freely. With no `tenants:` block every pipeline is in
`default`, so nothing is ever refused (today's files keep working). The check compares names, not
values: two variables that hold the same DSN pass (section 8).

### 3.4 Membership

A session (user id + IdP groups) is in tenant `T` when either:

1. it matches a principal in `tenants.T.members`; or
2. it may approve any layer of any pipeline of `T` (`owners.CanApprove`: approver or backup in the
   pipeline's `owners:` block, or in top-level `layers:` for a layer the pipeline does not override).

Consequences, all deliberate:

- A top-level `layers:` owner owns that layer in every pipeline that does not override it, so they are
  in every such pipeline's tenant. For separated teams, give each pipeline an `owners:` block for
  every layer. The server logs one warning at start per pipeline that inherits a layer while the
  tenant filter is on.
- `users.yaml` `layers:`, `auditors:` and `PIPEROOM_AUTH_VIEWER_GROUPS` give **no** tenant
  membership. An auditor of two tenants is listed in both `members:`.
- `tenants:` decides which rooms a person sees, not whether they may log in or which role they have:
  a tenant member must still pass the login gate (`users.yaml`, a principal in `layers:`/`owners:`,
  `auditors:`, or a viewer group), and the role rules are unchanged.
- The solo user sees every tenant (they approve every layer).
- A room whose pipeline is not in `owners.yaml` (an alert with an unknown `pipeline_id`) belongs to
  no tenant: with the filter on, nobody sees it, and the server logs a warning with the room id when
  it opens. Fix the alert's pipeline, or set `DEFAULT_ALERT_PIPELINE` for alerts that name none.

### 3.5 libpq defaults

pgx (`pgconn.ParseConfig`) fills anything a DSN leaves out from the libpq environment and home files:
`PGPASSWORD`, `PGPASSFILE` and `~/.pgpass`, `PGSERVICE`/`PGSERVICEFILE` and `~/.pg_service.conf`,
`PGSSLCERT`/`PGSSLKEY`/`PGSSLPASSWORD` and `~/.postgresql/postgresql.key`, `PGUSER`, `PGHOST`,
`PGDATABASE`. With per-pipeline DSNs, one tenant's DSN that omits its password, user, host or client
key would silently log in with the process's one. So, **when any opened pipeline has `connections:`,
the start is refused** while any of those variables is set (not empty) or any of those home files
exists (home = `os.UserHomeDir`, as pgconn uses). The error names the variables and files, never a
value. Put everything a pipeline needs in its own DSN. Without any `connections:` block nothing changes:
the process-wide DSN may rely on libpq defaults as before.

## 4. Mode

`PIPEROOM_TENANCY_MODE` (`internal/config/config.go`, `config.ParseTenancyMode`):

| Value | Meaning |
|---|---|
| `auto` (default) | a `tenants:` block turns the filter on (`teams`); no block = `single` |
| `single` | the start fails if `owners.yaml` has a `tenants:` block |
| `teams` | the start fails if `owners.yaml` has no `tenants:` block |

Set `teams` in a model (b) deployment, so that a lost block (a ConfigMap rollback, say) stops the start
instead of showing every room to everyone. The start-up log prints the chosen mode, the tenants, the
number of client sets, and which pipelines use their own connections and which the process-wide ones.
No value is ever logged.

## 5. Client sets

- `wiring.OpenStacks` builds **one client set per pipeline with `connections:`**, once at start, and
  **one process-wide set** shared by the pipelines without `connections:`. The process-wide set is
  opened only when at least one pipeline uses it, so a deployment where every pipeline has its own
  connections never opens the process-wide defaults.
- `StackOptions.Prepare(w)` runs for every opened set after its clients exist and before its tools are
  registered, with the set's own `Wired`. The server uses it to hand each set what only the server
  owns (the room store reader of `similar_incidents`, #100).
- Every set gets its own read-tool registry, action registry and topology builder, built over its own
  clients only, and an `owners` view holding only the pipelines it serves. A tool cannot reach another
  pipeline's client because it holds none, and it cannot name another pipeline because `tools.ScopeFor`
  resolves the room's pipeline id against that view (`ErrOutOfScope`). Isolation is by construction,
  not by a check inside each tool.
- Lookup is keyed by pipeline id:
  - **room service**: `room.Deps.StackFor(pipelineID)` gives the tools of the live and shadow agent runs
    and the actions of `CreateProposal` (`Validate`, `CheckScope`) and of the executor (`Validate`,
    `CheckScope`, `Execute`), all for the room's pipeline;
  - **topology**: the stacks value is a `topology.Builder` that sends `Build` and `Trace` to the set of
    the given pipeline (`GET /api/pipelines/{pid}/topology`, the room's first prompt, `POST .../trace`);
  - **watcher**: `watch.Deps.ClientsFor(pipelineID)`; each tick groups pipelines by client set and reads
    each set once (one `pg_replication_slots` query per set; a connector name is read on each set's own
    Connect cluster, so two clusters may both have a connector of that name). The DLQ signals (#102)
    read DLQ offsets once per set from the set's own Kafka (`PipelineClients.Kafka`), and a topic's
    sample ring is keyed by set and topic: two clusters may both have a DLQ topic of one name;
  - **`cmd/investigate`** opens only the set of the pipeline it investigates.
- A room whose pipeline is unknown or empty gets the **empty set**, whatever other sets exist:
  `runbook_lookup` only, no actions, topology refuses. A room never reaches a client by naming a
  pipeline nobody configured.
- Topic names are unique per Kafka cluster, so `owners.yaml` checks on DLQ topic names (a DLQ declared
  by two pipelines, a DLQ named like a CDC topic) compare them per `kafka_brokers` reference (the
  process-wide one for pipelines without `connections:`).
- Each set is opened in turn with the existing `openTimeout` (10 s); one unreachable source or
  warehouse stops the start, as today. `Close` closes every set.

## 6. Visibility

With the filter on, every route that names a room, proposal, hand-off or pipeline answers as if the
object did not exist when it is outside the caller's tenants. **404, never 403**, so ids do not leak.

| Route | Outside your tenants |
|---|---|
| `GET /api/rooms` | the room is not listed |
| `GET /api/rooms/{id}`, `/events`, `/export`, `/shadow`; every `POST /api/rooms/{id}/...` | `404 not_found` "not found", the answer for a missing room, checked before anything else on the route (after the login and viewer guards) |
| `GET /api/rooms/{id}/ws` | `404 not_found` before the upgrade: no subscribe |
| `POST /api/proposals/{pid}/approve`, `/reject` | `404 not_found`, the answer for a missing proposal, before `already_decided` |
| `POST /api/handoffs/{hid}/accept` | `404 not_found`, the answer for a missing hand-off, before `409`/`403` |
| `GET /api/pipelines/{pid}/topology` | `404 not_found` "pipeline not found" |
| `GET /api/shadow/metrics` | only rooms of your tenants are counted; `?pipeline=` outside them counts nothing, as an unknown pipeline does |
| `GET /api/fleet` (X5-0) | the pipeline has no row and is in no count; `?tenant=` naming a tenant you are not in, or none, answers `[]` (not `403`/`404`) |
| `GET /api/fleet/changes` (X5-0) | the pipeline's changes are not listed; `?pipeline=` outside them answers an empty page, as an unknown pipeline does |
| `GET /api/pipelines/{pid}/connectors/{name}/config-versions` (X5-0, also `?diff=`) | `404 not_found` "pipeline not found", before the connector, hash and role checks |
| `GET /api/pipelines/{pid}/trends` (X3-0) | `404 not_found` "pipeline not found", before the metric check (X3a's ticket said `403`; this rule wins) |
| `POST /api/pipelines/{pid}/connectors/{name}/accept-config` (X3-0) | `404 not_found` "pipeline not found", before the connector, role and watcher checks |
| `POST /api/rooms/{id}/page-team` (X4-0) | the room-id rule above; a `team` of another tenant is refused like an unknown team (`400 validation`) |
| `POST /api/changes/webhook` (X4-0, signed) | no caller tenant: the secret is shared by every tenant, so the reply carries `stored` only; an unknown pipeline is `404 not_found` |
| `POST /api/rooms` | `400 validation` "unknown pipeline", the answer for an unknown pipeline |
| `POST /api/alerts/webhook` (dev) | `404 not_found` for a pipeline outside your tenants, unknown pipelines included |
| `control/grant`, `handoffs` (create) | a target who cannot see the room is refused like an unknown person (`400 validation`) |
| `POST /api/alerts/{source}` (signed) | the reply is `{"room_id", "created"}` instead of the `RoomSnapshot`: a source secret is shared by every tenant, so the sender learns nothing about the room's content |

The check is one function, `room.Service.CanSeePipeline(user, pipelineID)`, over
`owners.CanSeePipeline`. The HTTP layer wraps every `/rooms/{id}` route with it; the approve, reject
and accept paths call it inside the service after loading the row. The check reads the room's
pipeline id from the cache or the rooms row; it clones and rebuilds nothing. A route test walks the chi
router and fails unless every route is classified: a room-id route (with its 404 case), an aggregate
that filters by tenant, or public. With the filter off (no `tenants:` block), nothing changes.

## 7. Migration

None. No database change, no event change, no `RoomSnapshot` field. An `owners.yaml` without
`tenants:`, `tenant:` and `connections:` loads and behaves exactly as before.

## 8. Known gaps (model (b) only)

- **Model key and cost are not per tenant** (out of scope): one `ANTHROPIC_API_KEY` / Bedrock / Vertex
  identity, one `PIPEROOM_MAX_AGENTS` pool and one token budget setting for every tenant. One tenant's
  storm queues the others' agents.
- One process and one room database hold every tenant's credentials and rooms.
- The Slack mirror posts every tenant's rooms to one channel. The server warns at start when the
  filter is on and Slack is enabled.
- `GET /api/people` lists every person the server knows, across tenants.
- Signed alert secrets are per source, not per tenant: whoever holds one can open a room for any
  pipeline (the reply no longer shows its content).
- MSK IAM and Bedrock use the process's one AWS identity (MSK IAM counts as a shared reference, 3.3).
- The shared-reference check compares names, not values or file contents: two variables that hold the
  same DSN pass (TLS key and certificate paths are the exception, 3.3). The same holds for DLQ topic
  names per `kafka_brokers` reference (5).
- pgx reads the libpq variables and home files (3.5); the start refuses them when a pipeline has its
  own connections, but a process-wide DSN may still use them.
- A `users.yaml` `layers:` entry still grants that layer on every pipeline in dev mode; the filter
  stops it at 404 before the approval is checked.
- **Membership is not re-checked on a live WebSocket.** The subscribe is checked once, before the
  upgrade; the gateway's tick checks the session, not the tenant. Membership cannot change without a
  restart today (owners.yaml and IdP groups are read at start or at login). Any owners.yaml hot reload
  or group refresh must add a tenant re-check to the WebSocket tick and close connections that lost it.
- **A grant or hand-off target who is a member only through an IdP group is refused** (fails closed).
  `LookupPerson` returns a `users.yaml` user without groups, so a `users.yaml` user who is in the tenant
  only through `group:` cannot be given control or a hand-off; list them by `user:` in `members:`.
  (An IdP-only user's live session carries its groups and works.)
