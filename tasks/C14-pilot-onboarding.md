# C14 — Pilot customer onboarding guide (Claude cloud agent, Sonnet) — docs only

> **Status (lead, 2026-10-12 00:30 IST):** in flight as PR #13. Facts are as of `main` @ 6283c9c (C8 clients and the
> 18 security fields merged); the call site that uses them is `internal/wiring` (branch `a4/wiring`). Label the
> Confluent CLI ACL example "Confluent Cloud only"; make the `kafka-acls.sh` example a full
> `--bootstrap-server … --command-config … --add` command. When C12 merges, list Slack as a data egress (private channel).


Read `AGENTS.md`, `docs/collab.md`, `docs/api.md`, `docs/security/findings.md`, `docs/security/threat-model.md` and `tasks/C8-secure-clients.md` first. Then read every file named under **Facts**.
You own a **new** folder, `docs/pilot/**`. You change **no code**, no contract file and no other doc. Open one GitHub PR titled `[C14] Pilot onboarding guide` into `main`. Your session's own `claude/...` branch is fine. Never touch `main`.

The cloud has no Docker, no dev stack, no Postgres or Kafka, and no `ANTHROPIC_API_KEY`. You cannot run the SQL or ACL examples. So every example may use only syntax that you read on the official page you cite. This task needs no contract change.

## Goal
A pilot customer's platform team must be able to connect Piperoom to their real pipeline safely, with the least access possible. Write:
- `docs/pilot/permissions.md`: least-privilege access per system. **READ** (always needed) is kept apart from **WRITE** (only if they turn on fixes).
- `docs/pilot/setup.md`: env vars, `owners.yaml` and `users.yaml`, the network list, how to run, and smoke checks.
- `docs/pilot/data-handling.md`: what Piperoom reads, what it stores, what it sends to the Claude API, and the retention status.
- `docs/pilot/faq.md`.
- Optional: `docs/pilot/README.md`, an index of at most 30 lines.

**Accuracy beats length.** Every permission names the `file:line` that needs it. Every external claim (Postgres role, Kafka ACL, Confluent flag, Anthropic policy) links the official page you read. If you cannot verify something, write "not verified, check <official page>". Never guess.

## Line numbers
The facts below were checked on `main` @ `7c7bdac` (C8 merged), plus the 18 security fields that the lead is adding to `internal/config/config.go`. A4 (`internal/wiring`) and A5 (room server) may merge before you start and move lines. Re-read each file on your base commit and cite the lines **you** see. Put "Checked against main @ <hash>" at the top of `permissions.md`.

## Facts checked by the lead (verify each one again)

### Source Postgres: `internal/clients/postgres/postgres.go`
| Lines | What runs | What it needs |
|---|---|---|
| 30-36 | `SET default_transaction_read_only = on` on every new connection | Nothing. It is defense in depth only: a session can change it, so the grants are the real limit. |
| 62-69 | `pg_replication_slots` (slot_name, plugin, active, restart_lsn, confirmed_flush_lsn, wal_status) + `pg_current_wal_lsn()` + `pg_wal_lsn_diff()` | The view page documents no privilege. The WAL functions "do not require superuser permissions" (functions-admin). `wal_status` is missing from the PG 12 page, so the minimum is **PostgreSQL 13**. `pg_current_wal_lsn()` "cannot be executed during recovery", so `SOURCE_DB_URL` must be the **primary**, where the Debezium slot lives. |
| 114, 121-134 | `to_regclass('piperoom_audit.ddl_log')`, then `SELECT at, command_tag, object_identity, query, username FROM piperoom_audit.ddl_log` | `USAGE` on schema `piperoom_audit` and `SELECT` on the table. The table and event trigger come from `dev/postgres-init/02-ddl-audit.sql`, and "Only superusers can create event triggers" (sql-createeventtrigger). This is optional: when it is missing, `pg_recent_ddl` returns "DDL audit trigger is not installed" (117-119). |
| 161-171 | `pg_stat_activity` (pid, usename, state, xact_start, `left(query, 500)`) | `pg_read_all_stats`. Without it, rows for other sessions have many null columns (monitoring-stats, "Ordinary users can only see all the information about their own sessions"). The filter `xact_start IS NOT NULL` then drops those rows, so the tool **silently** reports no long transactions. `pg_monitor` also works, but it includes more (`pg_read_all_settings`, `pg_stat_scan_tables`). |
| 196-230, 281-292 (`rowQuery`) | `SELECT * FROM <schema.table> WHERE <pk> = $1 LIMIT 1` | `USAGE` on the schema and table-level `SELECT` on each `source_tables` entry. Column grants are not enough, because the query is `SELECT *`. |
| 235-259 (SQL 244-254) | PK lookup in `information_schema.table_constraints` + `key_column_usage` | `table_constraints` shows only tables the user owns "or has some privilege other than SELECT on". A SELECT-only role gets no row, so `internal/topology/trace.go:86-89` falls back to the column `id`. Do **not** tell customers to grant more. Document the limit: the trace works when the PK column is `id`. |
| all | connecting | `LOGIN` on the role and `CONNECT` on the database |

### Warehouse: `internal/clients/warehouse/warehouse.go` (Postgres only today)
- Lines 31-37 set the same read-only flag. Line 68 runs `SELECT max(<ts_column>) FROM <schema.table>`. Line 89 runs `SELECT * ... WHERE <pk> = $1 LIMIT 1`.
- Grants: `CONNECT`, `USAGE` on the schema, and `SELECT` on each `sink_tables` entry only. `warehouse_freshness` accepts **any** table (`internal/tools/dbtools/wh.go:14-59` has no pipeline check), so the grants are the only boundary. Say this.
- Snowflake: `internal/clients/snowflake` exists, but no config field and no binary selects it (grep for it). Write "not selectable yet". Its grant advice is in the package doc (`snowflake.go:1-10`).

### Kafka: `internal/clients/kafka/kafka.go` (franz-go `kadm` v1.19.0)
Map each call to its Kafka request and to its row in the table "Operations and Resources on Protocols" at https://kafka.apache.org/41/security/authorization-and-acls/.
| Lines | Call | Request(s) | ACL |
|---|---|---|---|
| 81 | `kadm.Client.ListTopics` | METADATA | Describe Topic |
| 98, 102, 205, 209 | `ListStartOffsets`, `ListEndOffsets`, `ListOffsetsAfterMilli` | LIST_OFFSETS (+ METADATA) | Describe Topic |
| 145 | `ListGroups` | LIST_GROUPS | Describe Group. Only authorized groups come back; Describe Cluster is not needed. |
| 156 | `Lag` → `DescribeGroups`, `FetchManyOffsets`, `ListStartOffsets`/`ListEndOffsets` (kadm `groups.go`, func `Lag`) | DESCRIBE_GROUPS, FIND_COORDINATOR, OFFSET_FETCH, LIST_OFFSETS | Describe Group, Describe Topic |
| 236, 307 | short-lived `kgo` consumers made with `kgo.ConsumePartitions` (no group) | FETCH (+ OFFSET_FOR_LEADER_EPOCH) | Read Topic (+ Describe Topic) |
- Not needed (say so): DescribeConfigs (no `DescribeTopicConfigs` or `DescribeBrokerConfigs` call anywhere; check with grep), Write, Create, Alter, Delete, and Read on Group. Piperoom never joins a group and never commits offsets: `kgo.ConsumerGroup` appears only in `cmd/devconsumer`.
- Scope: the topics are the pipeline's Debezium topics `<topic.prefix>.<schema>.<table>` (`internal/topology/trace.go:172`, `builder.go:87`), so use one **prefixed** ACL on `<topic.prefix>.`. The groups are the `consumer_groups` entries (literal names).
- Read Topic lets the principal fetch whole records. The code decodes only keys (`decodeKey`: call at 259, body at 337-362) and keeps no values. Say this plainly in `data-handling.md`.
- Key decoding calls Schema Registry `GET /schemas/ids/{id}` (371 → `schemareg.go:103`).
- **Auth:** C8 added TLS and SASL (`kafka.NewSecure`, `internal/clients/netsec/kafka.go`: PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL; PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512; PLAIN without TLS is refused at line 105). On `7c7bdac`, though, `cmd/investigate/wire.go:100-102` still calls the plain `New` constructors, so the binary **ignores** the new env vars. Check the call sites on your base commit (`cmd/investigate/wire.go` or `internal/wiring`). If they still call `New`, state it as a known limit. MSK IAM and OAUTHBEARER are not supported (C8 task file). Do not name the broker's anonymous principal unless you verify it in the Kafka docs.

### Kafka Connect REST: `internal/clients/connect/connect.go` (self-managed Connect only)
- READ: `GET /connectors` (48), `GET /connectors/{name}/status` (70-71), `GET /connectors/{name}/config` (91-92).
- WRITE (actions only): `POST /connectors/{name}/tasks/{task}/restart` (100-101, `connect_restart_task`) and `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true` (106-107, `connect_restart_connector`).
- The Apache Kafka Connect guide (https://kafka.apache.org/41/kafka-connect/user-guide/) lists these endpoints and the HTTPS listener settings, but it describes no per-endpoint authorization. So "read-only Connect" means a network rule or reverse proxy that allows only the three GET paths. Say this. Do not invent a Connect permission.
- `GET .../config` returns the whole config. Secrets are included unless the customer uses config providers (https://kafka.apache.org/41/configuration/configuration-providers/: `config.providers`, `FileConfigProvider`, `EnvVarConfigProvider`). Recommend them, and claim only what that page says. Piperoom shows only an allow-list and masks keys that contain `password`, `secret`, `token` or `.key` (`internal/tools/kafkatools/connect_tools.go:155-170`, `sensitiveKey` 193-202). Topology reads only `topic.prefix`, `table.include.list` and `slot.name` (`builder.go:87, 205, 307`; `trace.go:171-172`).
- Login: `connect.NewSecure` uses basic auth plus TLS. `internal/clients/netsec/http.go` refuses credentials in the URL, basic auth over `http://`, and half a login. Confluent Cloud fully managed connectors use a different API and are not supported.

### Schema Registry: `internal/clients/schemareg/schemareg.go`
- READ: `GET /subjects` (63), `GET /subjects/{s}/versions` (70-73), `GET /subjects/{s}/versions/{n|latest}` (80-92), `GET /schemas/ids/{id}` (103), and `GET /config/{s}` (115) with a fallback to `GET /config` (122).
- WRITE (actions only): `PUT /config/{s}` with body `{"compatibility": ...}` (129-135, `schema_set_compatibility`) and `POST /subjects/{s}/versions` (139-148, `schema_register`). The endpoints were checked on https://docs.confluent.io/platform/current/schema-registry/develop/api.html.
- Confluent Cloud RBAC (https://docs.confluent.io/cloud/current/security/access-control/rbac/predefined-rbac-roles.html): DeveloperRead gives read-only access to subjects. DeveloperManage "can change global and subject level compatibility mode" but cannot register schemas. The page does not say what registering needs: write "not verified" unless you find it on an official Confluent page. Note that DeveloperRead on a consumer group can also reset offsets when it is bound together with a topic, so prefer Kafka ACLs for groups.
- Self-managed Schema Registry: verify on docs.confluent.io what per-subject authorization exists without a commercial plugin. If you find none, give the same GET-only proxy advice as for Connect.

### Writes and their scope: `internal/actions/kafka_actions.go`
- `actions.Register` (37-50) is called **unconditionally** in `cmd/investigate/wire.go:123-128`. There is **no switch to turn fixes off**. "Fixes off" means: do not grant the WRITE rows. An approved fix then fails at the target, and the error shows in the room.
- `Validate` only reads (`connectorExists` 98-110 → `GET /connectors`; `subjectExists` 112-124 → `GET /subjects`). `Execute` writes and then re-reads (`Status` at 175 and 233, `Compatibility` at 297).
- `CheckScope`: the connector must be in the pipeline's `connectors` (60-68). The subject must be `<topic>-key` or `<topic>-value`, with the topic ending in `.` + a `source_tables` entry (82-96). Cite `docs/api.md` §Proposals for when the server calls it.
- `cmd/investigate` never executes anything: it auto-rejects every proposal (`cmd/investigate/proposals.go:13-25`).

### Config, identity and data (for setup.md and data-handling.md)
- `internal/config/config.go` has the 11 base fields and the 18 security fields; the env var name is in each field's comment. `get` falls back to the **dev default when a var is unset or empty**: for example `dev/users.yaml`, `dev/owners.yaml`, or a DSN with the dev password. `setup.md` must tell customers to set every var.
- The SDK reads `ANTHROPIC_API_KEY` itself (`internal/agent/model.go:43`, `anthropic.NewClient()`). `go doc github.com/anthropics/anthropic-sdk-go.DefaultClientOptions` shows that it also honors `ANTHROPIC_BASE_URL`. The default host is `https://api.anthropic.com/` (`option.WithEnvironmentProduction`). Never print the key.
- DSNs: `pgxpool.ParseConfig` accepts `sslmode=verify-full` and `pool_max_conns`. When the URL has no password, it reads `.pgpass` (`go doc github.com/jackc/pgx/v5/pgconn.ParseConfig` and `.../pgxpool.ParseConfig`). Recommend a small `pool_max_conns` and no password in the URL.
- `owners.yaml`: the fields of `owners.Pipeline` (`internal/owners/owners.go:17-25`), `layers:` (28-31), the rules in `validate` (65-91), and the layer names in `events.Layers` (`internal/events/events.go:267-268`). `dev/owners.yaml` may also have `topic_prefix` (A1). `yaml.Unmarshal` ignores unknown keys, so document `topic_prefix` only if `owners.Pipeline` has that field on your base commit.
- `users.yaml`: the format and the "must agree with owners.yaml" rule are in `docs/api.md` §Auth.
- Room server (`cmd/server`, A5) is **not on GitHub**. If it is on `main` when you start, document its real flags and migrations from the code. If not, write "Running the room server" as "ships with A5", using only facts from `docs/api.md` and `config.go`. Then document what runs today: `go run ./cmd/investigate --pipeline <id> --title "<text>"` (flags at `cmd/investigate/main.go:24-29`). If `main.go:71-76` still puts a fixed `orders` pipeline map into the first prompt, say so.
- Alert webhooks: `internal/alertin` (C5) parses and verifies Alertmanager, Grafana, PagerDuty, Datadog and generic alerts. The HTTP route ships with A5. The secret config is in `docs/design/webhook-auth.md` (in design, C9).
- What goes to the model: the first message holds the alert title, pipeline id, room id and pipeline map, but not the raw alert body (`internal/agent/prompt.go:55-78`). Each tool result sends only `Summary`, wrapped as untrusted (`internal/agent/run.go:556-570`). `Data` goes only into the room event (`run.go:526-533`, `557-566`). Human input goes through `formatHumanInput` (`prompt.go:100-130`). The hand-off uses `renderRoomEvents` (`prompt.go:142` onward) and never `Data`. Requests use prompt caching and server-side compaction (`run.go:262-287`).
- What the room log stores (`internal/events/events.go`): the raw alert (`RoomCreatedPayload.Alert`, 101-106), tool inputs (126-131), tool `Summary` + `Data` (134-143), comments, steers, proposal params, action output and hand-offs. Row tools keep only key and timestamp values (`dbtools/pg.go:196-229`, `wh.go:116-138`; F3 is fixed). **Not redacted:** SQL text from `pg_long_transactions` (`Data` up to 500 chars, `postgres.go:166`; `Summary` 200 runes, `pg.go:130`) and from `pg_recent_ddl` (full DDL text; `Summary` 300 runes, `pg.go:88`), and FAILED task traces in `connect_status` (up to 2000 chars, `connect_tools.go:115-116`). These can hold literal values. Say so.
- Retention: the room log has no retention yet (F6). Link `docs/design/event-retention.md` as **"in design (C9, PR #10)"**. For Anthropic, link https://platform.claude.com/docs/en/manage-claude/api-and-data-retention and the commercial retention article it links. Do **not** quote a number of days, because sources disagree. You may say that this page lists prompt caching and compaction as ZDR-eligible.

## Work
1. `permissions.md`
   - At the top: "Checked against main @ <hash>", a summary of 5 lines, and the pilot advice: **start read-only**. Real login is not built yet (F1; `docs/design/auth.md` is in design), so treat approvals as unauthenticated and grant no WRITE until login ships.
   - One section per system: source Postgres, warehouse, Kafka, Kafka Connect, Schema Registry, Claude API. Each section has a READ table and a WRITE table; WRITE is "none" for Postgres, the warehouse and Kafka. Columns: Piperoom tool or action → client method (file:line) → SQL, request or endpoint → permission → official doc link.
   - Ready-to-paste examples, each under the heading "Example — adapt before use":
     - Source Postgres: `CREATE ROLE ... LOGIN ... CONNECTION LIMIT n`, `ALTER ROLE ... SET statement_timeout`, `ALTER ROLE ... SET default_transaction_read_only = on`, `GRANT pg_read_all_stats`, and `GRANT CONNECT`, `USAGE` and `SELECT` for each table, plus `USAGE`/`SELECT` on `piperoom_audit`. Warehouse: the same pattern without the stats role. Check the syntax on the CREATE ROLE, ALTER ROLE and GRANT pages.
     - Optional DDL audit install: point to `dev/postgres-init/02-ddl-audit.sql` (needs a superuser). Do not copy it.
     - Self-managed Kafka: `kafka-acls.sh` with `--allow-principal`, `--operation Describe --operation Read --topic <prefix>. --resource-pattern-type prefixed`, and `--operation Describe --group <group>`. Check every flag on the Kafka page.
     - Confluent Cloud: `confluent kafka acl create --allow --service-account <sa> --operations describe,read --topic <prefix>. --prefix` and `--operations describe --consumer-group <group>`. The flags are from https://docs.confluent.io/confluent-cli/current/command-reference/kafka/acl/confluent_kafka_acl_create.html.
     - Read-only Connect and Schema Registry: the list of methods and paths that a proxy must allow. Give no proxy product config.
   - A "What Piperoom never needs" list: superuser, the replication privilege, DescribeConfigs, Read on Group, Create/Alter/Delete, and any warehouse write.
   - A "Known limits" list: the PK falls back to `id`; long transactions are missed without `pg_read_all_stats`; the primary only; PG 13 or newer; `warehouse_freshness` is not scoped; there is no fixes-off switch; the state of Kafka/SR/Connect login in the binaries.
2. `setup.md`
   - An env var table built from `config.go` on your base commit: name, dev default (show the dev password as `<dev>`), what a pilot must set, and secret yes/no. Add `ANTHROPIC_API_KEY` and `ANTHROPIC_BASE_URL` (SDK). Warn about the empty-means-default rule.
   - Secure connections: Confluent Cloud (SASL_SSL + PLAIN with the cluster API key, and a separate Schema Registry key), MSK (SCRAM-SHA-512), and self-managed TLS or mTLS, all with the C8 env vars. If the binary does not pass them yet, say so at the top of this section.
   - `owners.yaml` and `users.yaml`: a field table and one full example for a made-up pipeline (not `orders`), with every rule from `validate` and from `docs/api.md` §Auth.
   - Network list. Outbound: the source Postgres primary, the warehouse, the room DB (`PIPEROOM_DB_URL`, Piperoom's own; keep it off the source DB), every broker address the cluster advertises and not only the seed list (cite `advertised.listeners` on https://kafka.apache.org/41/configuration/broker-configs/), Connect REST, Schema Registry, and the Claude API host. Inbound: `PIPEROOM_HTTP_ADDR` (loopback by default because of F1; if it is exposed, put it behind the customer's own SSO proxy).
   - Running: the command that works today (`cmd/investigate`) and the status of the room server (see Facts).
   - Smoke checks that change nothing, run as the Piperoom identities: `has_table_privilege('<table>', 'INSERT')` returns false, `pg_has_role('pg_read_all_stats', 'USAGE')` returns true, a `SELECT` from `pg_replication_slots`, `to_regclass('piperoom_audit.ddl_log')`, `curl -s $CONNECT_URL/connectors`, `curl -s $SCHEMA_REGISTRY_URL/subjects` (with the login if set), a Kafka group describe with the Piperoom principal, and then one `cmd/investigate` run (it auto-rejects proposals). For each check, give the expected output and what a failure means. Check the function signatures on https://www.postgresql.org/docs/current/functions-info.html.
3. `data-handling.md`: four tables. (a) What Piperoom reads, per system. (b) What it keeps only in memory, such as Kafka record values. (c) What it stores in the room log, per event type, with the redaction status and file:line. (d) What it sends to the Claude API, and what it never sends. Then cover retention (room log: none yet, F6, with the design link; Anthropic: links and the ZDR note), logs (the AGENTS.md §4/§7 policy and the F5 status), and customer controls: narrow grants; Connect config providers; leaving out `pg_read_all_stats` if query text must not leave the database, and what that costs (missed long transactions).
4. `faq.md`: 10 to 15 short questions and answers. It must cover:
   - Can the agent change production by itself? (No: it can only propose. Explain the approval gate and the F1 caveat.)
   - Can we run read-only? (Yes, and how.)
   - Does Piperoom join our consumer groups or move offsets? (No.)
   - Can we point it at a read replica? (No: it needs the primary. Explain why.)
   - Can managed Postgres (RDS, Cloud SQL) install the event trigger? Link provider docs only if you verified them; otherwise say "check your provider".
   - Confluent Cloud, MSK? (Answer per the C8 and wiring status.)
   - Snowflake? (Not selectable yet.)
   - What goes to Anthropic?
   - How long is data kept?
   - Multi-tenant? (One process per customer, F7.)
   - What if the DDL audit table is missing?
   - Why does `trace_record` assume `id`?
5. Write in simple English: short sentences and plain words. Prefer tables to long text. No marketing text.

## Tests
There is no Go code. Before you open the PR:
- Check every `path:line` you cite. Open the file on your base commit and confirm that the line shows what you claim. Put a "Sources checked" table at the end of `permissions.md` (file:line → the exact SQL, call or endpoint you saw there).
- Check that every relative link resolves on your branch. The exception is `docs/design/*.md` links: mark them "(in design, C9 PR #10)".
- Check that every external URL opens and says what you cite. Quote at most one short phrase from each page.
- `go build ./...` still passes, and `git diff --stat origin/main...HEAD` lists only `docs/pilot/**`.

## Done when
- The four pages (plus the optional README) exist under `docs/pilot/`, about 400-900 lines in total, and nothing else changed.
- Every READ and WRITE permission traces to a file:line and to an official doc URL. READ and WRITE are kept apart everywhere.
- Every example is marked "Example — adapt before use" and uses only syntax from the page it cites.
- The limits and open items are stated, not hidden: login in the binaries (wiring), A5 not merged, the PK fallback, no fixes-off switch, SQL text that is not redacted, and no retention.
- The PR description lists everything you could not verify, plus the follow-ups you suggest for code owners. Examples: A3, look up the PK through `pg_catalog`; A4, pass the C8 settings in the wiring; the leads, a fixes-off config flag. No contract change is needed.
