# Governance: what Piperoom can and cannot attest

For the data governance lead, platform lead or security reviewer at a pilot team who is asked "can we rely
on this?". Checked against the integration branch after the X wave's last batch (X1 drift, X3 config drift, X7
lineage, PII tags, retention evidence and the OpenLineage emitter). Each claim names the file or function it comes from, so a reviewer
can spot-check it. If the code changes, change the claim and its citation in the same PR.

Piperoom is an incident room, not a data catalog and not a compliance system. The X wave puts four governance
facts next to the incident, where the on-call engineer already looks: drift between source and sink, column
lineage, PII tags and retention evidence. This page says what each one proves, how to turn it on, and where it
stops. The design is `docs/design/lineage.md`; the API shapes are in `docs/api.md` ("Watcher alerts", "Fleet
view", "X7: column lineage and governance tags"); what each reader touches is in
[data-handling.md](data-handling.md).

## 1. At a glance

| Question | Piperoom can tell you | Piperoom cannot tell you |
|---|---|---|
| Does the sink hold as many rows as the source? | `count_drift`: source and sink row counts over the last closed UTC hour, shifted by the sink's lag, compared against a tolerance (default 0.5 %) on 2 checks in a row | which rows differ; anything about rows outside that hour |
| Are sampled rows there, with the same times? | `row_drift`: up to 50 of the newest settled source keys looked up on the sink, up to 8 timestamp columns compared | whether any non-timestamp value (`amount`, `status`, `email`) is right |
| Is the topic duplicated or reordered? | `dup_keys`, `order_skew`: a scan of the records produced since the previous scan, from Debezium Postgres `source.lsn` | duplicates that exist only in the sink table, or whose two copies fall in different scans; sources other than Debezium Postgres |
| Did a connector's config change outside an approved fix? | `config_drift`: which keys changed (names only), the old and new hash of the redacted config | who changed it, or whether the change was wrong |
| Where does a column go? | column maps on the CDC hops (source table, topic, sink table), matched by name through the connector's SMTs | what a consumer or a SQL job does to it on the way; BI tools; batch pipelines |
| Which columns look personal? | a `pii` tag per table: `column:class` pairs from column names and types | that a column holds personal data, or that a column with a harmless name does not |
| Is Kafka retention as promised? | per topic: configured `retention.ms` / `cleanup.policy` against your `declared_retention`, both directions | how old the data really is; the retention of a warehouse table (no reader exists yet) |
| Is it in our lineage catalog? | with `OPENLINEAGE_URL` set: the CDC datasets, their column lineage, PII tags and owners, and each executed fix step, sent to your Marquez or DataHub (section 7) | anything the map does not hold (section 4); whether every event arrived (delivery is best effort) |

Piperoom only reads your systems for all of this. It writes nothing to Kafka, Postgres or the warehouse to
produce these facts, and every fact carries names, classes, counts and durations, never a row value
(`docs/design/lineage.md`, "Every X7 shape carries names, types, classes and durations only").

## 2. Turn it on

The drift signals, `config_drift` and the retention check belong to the watcher, so the pipeline needs a
watcher: `PIPEROOM_WATCH_MODE` is `log` or `rooms` (`config.ParseWatch`; default `log`, which only writes log
lines and updates `GET /api/fleet`; `rooms` also opens a room, [setup.md](setup.md#1-configure-the-environment)).
A `kind: batch` pipeline has no watcher and no column lineage (`internal/owners/kind.go` `validateBatch`
refuses `watch:`; `internal/topology/builder.go` builds a batch map without `collectLineage`).

This `owners.yaml` loads on this commit (`owners.Load`; the keys are in [setup.md](setup.md#2b-x-wave-keys-in-ownersyaml)):

```yaml
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders, public.customers]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders, analytics.customers]
    sink_ts_column: updated_at         # the drift checks window and compare on this column, on both sides
    declared_retention:                # what the data owner promised
      topics:
        pg.public.orders: 7d
      tables:
        analytics.orders: 400d         # recorded and validated; no warehouse can be judged yet (section 6)
    pii:                               # corrections to the name classifier
      include: [public.customers.tax_ref:national_id, notes]
      exclude: [phone_verified]
    watch:
      reconcile_interval: 15m
      count_tolerance: 0.5%
      tables:
        analytics.customers: {reconcile: off}   # a table the sink filters on purpose
```

Every key is validated at load and a typo stops the start (`internal/owners/pii.go`,
`declared_retention.go`, `reconcile.go`; `owners.go` decodes with `KnownFields(true)`). The signals are on by
default with action `room` (`internal/owners/watch.go`, `DefaultWatch`); `watch.signals.<name>: {enabled: false}`
or `{action: log}` turns one down, for `count_drift`, `row_drift`, `dup_keys` and `order_skew` only.

What the checks need from your systems:

| Check | Needs | Without it |
|---|---|---|
| `count_drift`, `row_drift` | a source and a sink that implement `clients.Reconciler`: the source Postgres and the Postgres warehouse (`WAREHOUSE_KIND=postgres`) only (`internal/clients/postgres/reconcile.go`, `internal/clients/warehouse/reconcile.go`); `SELECT` on the pipeline's tables; a `sink_ts_column` that exists on both | the check is off for the pipeline, one INFO log `watch: drift check off for this pipeline` (`internal/watch/drift.go` `driftOff`). Snowflake, ClickHouse, BigQuery and Databricks sinks get no drift check |
| `dup_keys`, `order_skew` | the Kafka client's `DupScan` (`internal/clients/kafka/dupscan.go`): `Read` on the CDC topics | off, one INFO log |
| Column lineage, PII tags | `clients.ColumnReader` on the source Postgres and the Postgres warehouse: it reads `pg_catalog` only (`internal/clients/pgconv/columns.go`); a registry for the topic hop (`SCHEMA_REGISTRY_URL` not `off`) | the hop has no `columns`, or its maps are marked `mapping: inferred` |
| Retention | `DescribeConfigs` on the pipeline's topics ([permissions.md](permissions.md#kafka)) | one INFO log `watch: retention check cannot read a config, no finding for it`, no finding (`internal/watch/retention.go`) |

## 3. Drift: silent corruption becomes a symptom

The four signals open or join the pipeline's room like `slot_wal` does (`internal/watch/drift.go`,
`checkDrift`); their default severities are `events.SignalSeverity` (`count_drift` and `row_drift` sev2,
`dup_keys` and `order_skew` sev3). The evidence is a `DriftSignal`: table or topic, a time window, counts and one
line of detail, never a key or a row (`internal/events/events.go`). Read the shapes in `docs/api.md`.

- **`count_drift`** counts source and sink rows by `sink_ts_column` for the last closed UTC hour, moved back by
  the sink's measured lag (at most 24 h; an idle sink is read at its newest row). It fires when the difference
  is over `watch.count_tolerance` on 2 checks in a row (`countVerdict`, `countDriftChecks`). It pairs tables by
  position when there is one source and one sink table, otherwise by the part after the schema (`pairSink`); a
  table with no partner by that rule is not checked. A sink that filters, aggregates or soft-deletes **on
  purpose** will fire it: set `reconcile: off` on that table.
- **`row_drift`** takes up to 50 newest source keys old enough to have reached the sink (`driftSampleKeys`,
  `rowSettle`), and checks that each exists on the sink and that up to 8 date or timestamp columns agree within
  one second (`timeColumns`, `driftTimeTolerance`). It also catches rows that landed with `sink_ts_column`
  NULL, sink rows holding Debezium's unavailable-value placeholder in a TOAST column, and a column the sink lost
  (`internal/watch/drift_landed.go`). It needs a single-column primary key on both tables; a composite key turns
  the check off for that table (`rowParts`).
- **PII guard.** `row_drift` never reads a table whose key column, or whose `sink_ts_column`, looks like
  personal data, and never compares a time column whose name does (`piiColumn` calls `redact.PersonalColumn`,
  and an `owners.yaml` `pii.include` entry). The guard classifies the column list itself, not the `pii` tag, so
  a tag cut for length cannot let a column through (`docs/design/lineage.md` section 3).
- **`dup_keys` and `order_skew`** are one `DupScan` per CDC topic over `(key, source.lsn)`, of the records
  produced since that topic's previous scan (the first scan: the last `reconcile_interval`). A run scans at
  most 8 topics, rotating, with a budget of 50 000 records shared among them and at least 1 000 each
  (`checkTopics`, `clients.DupScanMaxScan`). Two copies of a record that fall in different scans are not
  matched. Records it cannot decode count as `undecodable` and fire nothing. The finding holds counts only; the
  key fingerprints stay in the tool `kafka_dup_keys` ([data-handling.md](data-handling.md#2-what-each-tool-exposes)).
- **`config_drift`** compares the redacted connector config with the last approved one: the baseline moves when
  the server starts (the first read of each leadership term, so also when another replica takes over), when an
  executed step names the connector, and on
  `POST /api/pipelines/{pid}/connectors/{name}/accept-config` (`internal/watch/config_drift.go`). The signal has
  key names and two hashes; a key that looks like a credential reads `<secret key changed>`. The baseline is in
  memory: a change made while the server was down, or before a replica took over, becomes the baseline and is
  never reported.

All of it is statistical or structural evidence of *a difference*. None of it proves the data is right
(section 8).

## 4. Column lineage

`GET /api/pipelines/{pid}/topology` returns `edges[].columns` on the data edges into a dataset node: for each
column its source column, target column and `transform` (`rename`, `unwrap`, `dropped`, `added`; empty = same
name). "orders.discount is not in the sink" is a `dropped` entry on the edge. The agent sees the same facts in
`pipeline_map` as one `columns:` line per edge (`internal/topology/render.go`).

How it is built (`internal/topology/lineage.go`, `lineage_smt.go`): the source and sink column lists come from
the catalogs, the topic's fields from the latest `<topic>-value` Avro schema in Schema Registry, and the two are
matched **by name**, through the connector's `ExtractNewRecordState` and `ReplaceField$Value` settings. Any
other SMT, a registry that is off, or a sink written by a consumer group instead of a sink connector makes the
maps `mapping: inferred`: a name match, not a proof. Lists are capped at 1000 per edge (`columns_omitted`
counts the rest).

Covered: the hops `connect:<source> -> topic` and `<writer> -> sink table` of a CDC pipeline, with the source
Postgres and a Postgres warehouse. Not covered: Snowflake, ClickHouse, BigQuery and Databricks sinks (no
`ColumnReader`), ClickHouse engine tables, dbt models and views, and `kind: batch` pipelines.

## 5. PII tags

`redact.ClassifyColumn` (`internal/redact/columns.go`) puts a column in one of `email`, `phone`, `name`,
`address`, `national_id`, `card`, `ip`, `dob` from its **name and catalog type only**. It reads words
(`customer_email`, `customerEmail`), not substrings (`emailed_at` is not an email), and lets a word that makes
the column about the thing cancel the match (`email_verified`, `ip_count`). A type rules a class out (a boolean
`phone_verified`, an integer `ip_count`). `owners.yaml` `pii.include` adds a column, with an optional class
(`public.customers.tax_ref:national_id`, default `other`), and `pii.exclude` removes a hit. The result is the
`pii` tag of the source and sink table nodes, `column:class` pairs sorted by column, at most 2048 bytes, with
`pii_omitted` counting what was cut (`internal/topology/pii.go`). Read it in the topology JSON:

```
curl -s -H 'X-User: priya' http://127.0.0.1:8090/api/pipelines/orders/topology \
  | jq '.nodes[] | select(.tags.pii) | {id, pii: .tags.pii}'
```

(`X-User` works in dev auth mode only; with OIDC use your session, [README.md](README.md).)

**It is an inventory aid, not a classification.** Known gaps, from the code's own comments: words run together
beyond the listed spellings are missed (`customeremail` matches only the spellings in `columnRules`); a bare
`name` counts only alone or after a person word, so `product_name` is not flagged; free-text columns
(`notes`, `comment`) are not flagged unless you `include` them; a user-defined type whose name starts with
`int`, `uint`, `float`, `decimal` or `time` is read as that kind and may be ruled out. A column
that holds personal data under a neutral name is a false negative, and nothing reads values to find it.
The tag exists only where a `ColumnReader` does: the source Postgres and the Postgres warehouse.

## 6. Retention evidence

The watcher reads the configuration of every CDC topic of the pipeline and of every DLQ topic you give a
`declared_retention` (`retention.ms`, `retention.bytes`, `cleanup.policy`), and compares retention with your
promise **in both directions** wherever you made one: shorter loses data you promised to keep (`retention_shorter_than_declared`),
longer, `-1` included, keeps data you promised to delete (`retention_longer_than_declared`). A CDC topic with
`cleanup.policy=compact` and no `delete` is `compact_cdc_topic` whether or not you declared anything, because
deletes are compacted away and a replay cannot see them. A compact-only topic counts as `infinite`. Findings
are entries of the `config_risk` signal's `risks` list (`internal/watch/retention.go`, `events.RetentionRisk`),
severity sev4: they make `config_risk` fire (so it shows in `GET /api/fleet` `signals_firing` and the log) but
open no room alone; they ride along in the evidence when another signal opens or joins a room. The agent reads
them as `retention_risk=<topic>:<code>` with the two durations. Each object is re-read at most every 15 minutes,
16 reads a tick.

What this proves: the topic's **configuration** matches your declaration at the last read. What it does not:
that old records are gone. Kafka deletes by segment, so data older than `retention.ms` can remain until the
segment rolls; a size cap (`retention.bytes`) can cut it shorter, and the finding names the cap. The check does
not read a record's age.

The warehouse side is **not implemented**: `clients.TableRetentionReader` has no implementation, so a declared
`tables:` entry is validated and shown in the config but never judged, and `Tags.retention` (the declared value
as a node tag) is defined in `internal/topology/graph.go` but nothing sets it. The watcher keeps no
history of retention settings: `watch_status` is replaced each tick and `watch_samples` keeps 7 days of
numbers, not topic configs.

## 7. OpenLineage export

Optional and off by default: set `OPENLINEAGE_URL` to your lineage server's endpoint (Marquez
`http://marquez:5000/api/v1/lineage`, DataHub `.../openapi/openlineage/api/v1/lineage`; the emitter appends no
path), optionally `OPENLINEAGE_NAMESPACE` (the job namespace, default `piperoom`) and `OPENLINEAGE_API_KEY` (sent
as `Authorization: Bearer`, only over https or http to a loopback host; `config.ParseOpenLineage` refuses the
rest and a bad value stops the start). Unset, no goroutine starts and nothing is sent
(`internal/discover/openlineage/emitter.go`, `Start`; wired in `cmd/server/main.go`).

What it sends (`internal/discover/openlineage/event.go`, mapping in `docs/design/lineage.md` section 6):

- **The pipeline map**, as one `RunEvent` (job `piperoom.pipeline.<id>`, `COMPLETE`): source tables as inputs,
  topics and sink tables as outputs, with the `schema` (column names), `columnLineage`, `tags` (the `pii`
  classes per column; `retention` when a node carries it, which none does yet, section 6) and `ownership`
  (the layer owner's user id) facets. At most one per pipeline every 15 minutes, and only when the room service
  builds the map on the leader (an agent or shadow run starting, a fix preview, the map page
  `GET /api/pipelines/{pid}/topology`); a quiet pipeline nobody opens sends nothing.
- **Each executed fix step**, as one `RunEvent` (job `piperoom.<action>`; `COMPLETE`, `FAIL`, or `OTHER` when
  the outcome is unknown) with the pipeline and proposal ids and the step index, and as outputs the datasets of
  that pipeline's last map that the step's topic or table params name. The step's output is cleared before the
  emitter sees it (`room.Deps.OnActionResult`).

Never a row or record value, a config value, node health or facts, room content, other step params or the API
key. Datasets are named `<database>.<schema>.<table>` in namespaces built from the process-wide connections
(`postgres://host:port`, `kafka://host:port`, the warehouse's), never with a user, password or DSN parameter
(`internal/discover/openlineage/namespaces.go`).

Where it stops:

- **Leader only, best effort.** It runs on the elected leader replica; a map built or a step executed on
  another replica that the leader does not fold is not sent. A POST has 10 s; a network error, 429 or 5xx is
  retried 3 times with backoff, then the event is dropped with one warning; at most 64 events wait (the oldest
  is dropped). A missing event in your catalog proves nothing about the pipeline.
- **Left out:** a pipeline with its own `connections:` block (its datasets are not the process-wide ones), MySQL
  sources, dbt models, and sink tables of a warehouse other than `WAREHOUSE_KIND`. An event over 1 MiB is sent
  without its column facets, or dropped.
- **One namespace per server.** Every pipeline lands in `OPENLINEAGE_NAMESPACE`; with several tenants,
  `docs/design/lineage.md` section 7 says to run one lineage server per tenant or leave it off.
- **Egress.** An outbound POST from the server, through a dialer that refuses link-local and cloud-metadata
  addresses (`connprobe.Guard`), with no proxy and no redirect: add it to your egress review
  ([data-handling.md](data-handling.md#2-what-each-tool-exposes), row "OpenLineage emitter").

## 8. What Piperoom cannot do

State these to your auditor as they are.

1. **It cannot verify value-level correctness.** The only values it compares are row counts and timestamp
   columns, and it samples at most 50 keys per table and run. A wrong `amount`, a truncated string, a wrong
   currency or a changed enum passes `count_drift` and `row_drift`. `dup_keys` looks at the topic, so duplicates
   introduced after it are invisible.
2. **It cannot validate a business metric.** It does not know what revenue, an active user or a refund is, and
   does not recompute any figure. A `fct_revenue` that is wrong because the SQL is wrong is outside every check.
3. **It cannot attest erasure.** Piperoom holds no list of data subjects, has no action that deletes a Kafka
   record or a warehouse row, and cannot tell you whether a person's data is gone. The recovery actions copy
   or re-emit data (`kafka_replay_range` copies whole records into the pipeline's `reprocess_topic`;
   `pg_resnapshot_rows` re-emits source rows) and consult no erasure list. Its own copies are deleted by room
   retention or an admin delete, not by data subject ([data-handling.md](data-handling.md#4-retention-and-deletion));
   data already sent to the model provider or posted to Slack is outside Piperoom.
4. **It cannot prove data was deleted on time.** Section 6: it compares configuration, not data age, and not
   a warehouse's retention.
5. **It does not discover personal data.** Section 5: names and types, with the gaps listed there.
6. **Its lineage is not complete.** Section 4: CDC hops, by name, for Postgres, no SQL parsing, no downstream
   models or dashboards. The OpenLineage feed carries the same maps, sent best effort from the leader only, so
   your catalog can miss a pipeline or a fix step (section 7).
7. **It is not an audit log or a signed report.** There is no governance report. Signals are in the room they
   opened, in `GET /api/fleet`, and in the log; room exports carry a SHA-256 over the JCS form
   (`X-Piperoom-Export-SHA256`), which detects a change only if you keep the hash elsewhere, and are not signed.
   Rooms are deleted by retention (`PIPEROOM_RETENTION_DAYS`); config versions and the change log by
   `PIPEROOM_CHANGES_RETENTION_DAYS`.
8. **It has no guarantee of detection.** Drift checks run every `reconcile_interval` (default 15 min), up to 10
   table pairs and 8 topics per run in rotation, and the count check judges the last closed UTC hour and needs
   two failing checks in a row, so a fault can be an hour old or more before it is reported. A check that
   cannot run is off, with one INFO log, not an alert: read the log for `drift check off`.

## 9. Evidence you can hand over

| Evidence | Where | Who may read it | Kept |
|---|---|---|---|
| What the watcher sees now, per pipeline | `GET /api/fleet` (`signals_firing`, `forecasts`, `last_verdict`) | any signed-in user, filtered by tenant | the current status only (`watch_status` is replaced each tick) |
| Connector config history | `GET /api/pipelines/{pid}/connectors/{name}/config-versions`; `?diff=a..b` for redacted values | list: anyone who can see the pipeline; diff: approvers, backups, the solo user, auditors | `PIPEROOM_CHANGES_RETENTION_DAYS` (default 30), newest per connector always |
| What changed across pipelines | `GET /api/fleet/changes` | any signed-in user, filtered by tenant | the same setting |
| The incident itself | `GET /api/rooms/{id}/export`, `GET /api/rooms/{id}/postmortem.md` | per `docs/api.md` | until room retention or an admin delete |
| Deletions of rooms | `room_deletions` table: room id, pipeline, who, when, reason, event count | the database | with the database |

## 10. Verify it yourself

No Docker is needed for the unit tests; each of these passes on this commit:

```
go test ./internal/redact -run Column
go test ./internal/owners -run 'PII|DeclaredRetention'
go test ./internal/topology -run 'Columns|PIITags|X7'
go test ./internal/watch -run 'CountDrift|RowDrift|DupKeys|ConfigDrift|Retention'
go test ./internal/config -run X7
go test ./internal/discover/openlineage
```

With the dev stack (`docker compose -f dev/docker-compose.yml up -d`, [setup.md](setup.md)), `devctl` makes the
silent faults on demand; nothing fails and `devctl health` stays green, and only the drift signals see them
(`dev/scenarios.md` section 14 to 17, the signal each expects is in `cmd/devctl/scenarios.go`):

```
go run ./cmd/devctl break sink-filtered-rows --owner me    # count_drift
go run ./cmd/devctl break sink-dropped-column --owner me   # row_drift
go run ./cmd/devctl break consumer-duplicates --owner me   # dup_keys
go run ./cmd/devctl break toast-placeholder --owner me     # row_drift
go run ./cmd/devctl fix <same name> --owner me
```

Set `reconcile_interval: 1m` in a local owners file to see them within minutes. The integration tests
(`go test -tags integration ./internal/...`) need that stack and were not run for this guide. There is no
`devctl` break and no eval scenario for a retention mismatch or a PII tag.

## 11. Status and known gaps at this commit

- **OpenLineage is best effort and leader only** (section 7). **Warehouse retention reader missing** and
  `Tags.retention` never set (section 6). **Column catalogs only on Postgres** (sections 4, 5).
- **The web UI does not show** `tags`, `columns` or the retention entries (`web/src/api/types.ts` has no such
  fields). They are in the topology JSON, in the agent's `pipeline_map` text and in watcher evidence.
- **Eval scenarios** for the four drift faults (`evals/scenarios/sink-filtered-rows.yaml`,
  `sink-dropped-column.yaml`, `consumer-duplicates.yaml`, `toast-placeholder.yaml`) are synthesised from the
  code paths (header `RECORDED: NO`), not recorded on a stack; the CDC baseline has not been re-recorded with
  them (`evals/scenarios/README.md`).
- **Settings that do not exist.** `owners.yaml` has no `vendor_status_url` and no `watch.db_size_limit`, and
  `watch.signals` accepts neither `forecast` nor `config_drift`: the loader refuses each (checked with
  `owners.Load`). Forecasts and `config_drift` run on fixed settings. Only `slot_wal_bytes` can forecast
  today, as no client implements `DBSizeReader` or `UnreadAgeReader`.
- **A restart** loses the drift cooldown memory and the `config_drift` baseline (sections 3, 8): a drift room
  can reopen inside its cooldown.
