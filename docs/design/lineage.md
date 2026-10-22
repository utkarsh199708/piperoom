# Lineage, PII tags and retention evidence (X7)

Status: contract X7-0 (KAN-447), leads. Implementation: X7a column lineage (A3, KAN-448), X7b OpenLineage emitter
(C10, KAN-449), X7c PII tags (A3 + N5, KAN-450), X7d retention evidence (A7, KAN-451), X7e governance guide (C14,
KAN-452).

Governance asks three questions of a CDC pipeline: where does each column go, which columns hold personal data, and
is data kept as long as promised. Piperoom already holds the pipeline map; X7 puts the answers on that map, next to
the incident, instead of in a separate spreadsheet, and exports them to an OpenLineage server (Marquez, DataHub).

Every X7 shape carries **names, types, classes and durations only: never a row value, a sample, a default
expression, a record or a config value.** Who may read them is who may read the topology: the pipeline's tenants;
for anyone else the pipeline is `404 not_found` (`docs/design/tenancy.md`).

## 1. Contract shapes (X7-0)

| Shape | File | Used by |
|---|---|---|
| `Edge.Columns []ColumnMap`, `Edge.ColumnsOmitted`, `ColumnMap{Input, From, To, Transform, Mapping}`, `ColumnTransform` (`""`, `rename`, `unwrap`, `dropped`, `added`), `IsDataset`, `LimitColumns`, `MaxEdgeColumns` (1000) | `internal/topology/graph.go` | X7a fills, X7b exports, A6 shows |
| `Node.Tags`, `TagPII`, `TagPIIOmitted`, `TagRetention`, `ValidTag`, `MaxNodeTags` (16), `MaxTagValueBytes` (2048), `PIIClass` + `PIIClasses()`, `FormatPIITag`/`ParsePIITag`, `ParseRetention`/`FormatRetention`, `RetentionInfinite` | `internal/topology/graph.go` | X7c and X7a fill, X7b exports, X7d formats evidence, A6 shows |
| `ColumnReader` + `ColumnInfo`, `TableRetentionReader` + `TableRetention`, `MaxTableColumns` (1600), `CatalogTimeout` (10 s) | `internal/clients/clients.go` | X7a, X7c (postgres, warehouse clients), X7d |
| `TopicConfigReader` + `TopicConfig` (moved from `internal/clients/kafka`, which keeps an alias) | `internal/clients/clients.go` | X7d |
| `RetentionRisk`, `RiskRetentionShorter`, `RiskRetentionLonger`, `RiskCompactCDCTopic`, `RetentionRiskSeverity` (`sev4`) | `internal/events/events.go` | X7d writes, A4 renders, A6 shows |
| `OPENLINEAGE_URL`, `OPENLINEAGE_NAMESPACE`, `OPENLINEAGE_API_KEY`, `ParseOpenLineage`, the `OpenLineage*` limits | `internal/config/config.go` | X7b |
| Topology JSON (`tags`, `edges[].columns`, `columns_omitted`), `config_risk` retention entries | `docs/api.md` | web, agent |

Everything is additive and omitted when empty: a graph, an edge or a node from before X7 has the same JSON bytes and
the same `Render` text (`internal/topology/graph_x7_test.go`, `TestRenderGolden`). `Edge` now holds a slice, so it is
no longer comparable with `==`; compare the fields you need (the topology tests use `containsEdge`).

The readers are optional, asserted at use time like `clients.CountReader`: no existing client or fake has to grow,
and a client that cannot answer returns `clients.ErrNotSupported`, which turns that part off for the pipeline with one
INFO log.

## 2. Column lineage (X7a)

`Edge.Columns` sits on the **data edge into a dataset node** (`IsDataset`: `source_table`, `topic`, `dlq_topic`,
`sink_table`, `model`, `kafka_engine`, `materialized_view`). `To` names a column of that node. `Input` names the
upstream dataset node the `From` column belongs to:

- the edge's `From` when that node is a dataset (ClickHouse `topic → kafka_engine → materialized_view → sink_table`,
  `model → sink_table`);
- else a dataset with a data edge into the edge's `From`, the writer's input (`pg:public.orders → connect:src →
  topic:pg.public.orders`: the map on `connect:src → topic:…` has `Input` `pg:public.orders`);
- when that dataset's fields are unknown (no Schema Registry: `SCHEMA_REGISTRY_URL=off`, or the registry fails), the
  nearest dataset further upstream whose columns are known, with `Mapping: "inferred"` on each map. The topic hop then
  has no `Columns` (X7a TC3).

Field names: a table column by its catalog name (`clients.ColumnReader`); a topic value field by its path in the
latest value schema with dots (`after.amount` inside a Debezium envelope, `amount` once unwrapped). Matching is by name
after the connector's SMTs as configured, in chain order, where known:

| SMT (class) | Effect on the maps |
|---|---|
| `io.debezium.transforms.ExtractNewRecordState` | `after.<f>` → `<f>`, `Transform: "unwrap"`; fields its `add.fields` adds (`__op`, `__table`, …) are `added` |
| `org.apache.kafka.connect.transforms.ReplaceField$Value` `renames` (`amount:total`) | `amount` → `total`, `Transform: "rename"` (Kafka Connect has no separate RenameField SMT) |
| the same, `exclude` (older `blacklist`) or `include` (older `whitelist`) | an excluded or not-included field is `dropped` |
| any other SMT on the chain | the maps after it are `Mapping: "inferred"` (Piperoom does not model it) |

Unmatched upstream columns are `dropped` (`To` `""`): "orders.discount not in sink" is a fact on the edge, and
`Render` shows it. Unmatched downstream columns are `added` (`From` and `Input` `""`). Order: the downstream column
order, then the dropped columns in upstream order, so the same graph always gives the same list; `LimitColumns` keeps
the first 1000 and `ColumnsOmitted` counts the rest. A Schema Registry or catalog error leaves that hop without
`Columns` and never fails the build (the `HealthBudget` of the builder still applies).

`Render` (contract in `Builder.Render`): a graph with no `Columns` and no `Tags` renders byte for byte as before. An
edge with `Columns` adds, under its `To` node, one `columns:` line naming only the changes (renamed, unwrapped,
dropped, added; same-name columns counted, not listed); a node's `Tags` follow its facts in key order. All under the
16 KB render cap.

## 3. PII tags (X7c)

Columns are classed from **names and types only** (`ColumnInfo.Name`, `ColumnInfo.Type`), never values: `email`,
`phone`, `name`, `address`, `national_id`, `card`, `ip`, `dob` (`topology.PIIClasses()`; `other` only from an
`owners.yaml` include without a class). The type only rules columns out (`phone_verified boolean`, `ip_count integer`
are not PII). The classifier lives in the N5 folder (`internal/redact`), next to the secret patterns, and is fuzzed
with the same harness.

The result goes on the table's node as `Tags.pii`, `FormatPIITag`'s sorted `column:class` pairs
(`customer_email:email,phone_number:phone`), on `source_table` and `sink_table` nodes. The value is capped at 2048
bytes; `pii_omitted` counts what was cut. **The tag is a label for display and export, not a safety list**: X1b's
sampling classifies the column list itself (it never requests a PII-classed timestamp column in `RowTimes`, and skips
`SampleKeys` on a table whose key column is PII-classed), so a cut tag cannot let a PII column through.

## 4. `owners.yaml` additions (A3 loads and validates)

Both blocks are optional, per pipeline:

```yaml
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    sink_tables: [analytics.orders]
    declared_retention:              # X7: what the data owner promised
      topics:
        pg.public.orders: 7d         # a topic of this pipeline (CDC topic or a watch.dlq_topics topic)
      tables:
        analytics.orders: 400d       # one of this pipeline's sink_tables or source_tables
    pii:                             # X7c: overrides of the name classifier
      include: [public.orders.notes, public.customers.tax_ref:national_id]
      exclude: [public.orders.emailed_at, phone_verified]
```

Validation (a load error names the pipeline and the entry):

- `declared_retention` values parse with `topology.ParseRetention`: a whole number ≥ 1 with `d`, `h`, `m` or `s`
  (`7d`, `36h`), or `infinite`; at most 100 years. A `tables` key must be one of the pipeline's `source_tables` or
  `sink_tables`; a `topics` key must be a valid Kafka topic name (whether it is one of the pipeline's topics is
  checked at run time: X7d judges only topics in the pipeline's graph and logs once for a declared topic it never
  finds). At most 200 entries per pipeline.
- `pii.include` entries are `<schema>.<table>.<column>` with an optional `:<class>` (default `other`), and `<table>`
  one of the pipeline's tables; or a bare `<column>`, meaning that column in every table of the pipeline. `pii.exclude`
  takes the same forms without a class. Exclude removes a classifier hit; include adds one. A column in both lists, an
  unknown class, or a two-part entry (`orders.email`) is an error. At most 500 entries per list.

## 5. Retention evidence (X7d)

For each topic of the pipeline with a declaration, the watcher reads `retention.ms`, `retention.bytes` and
`cleanup.policy` (`clients.TopicConfigReader`; `retention.ms=-1` is infinite) and compares the time with the
declaration; for a sink table with a declaration it does the same when the warehouse client is a
`clients.TableRetentionReader` (none is in X7-0: until one is, only topics are judged). A difference **in either
direction** is a finding: shorter loses data the owner promised to keep, longer (infinite included) keeps data the
owner promised to delete. Equal, or no declaration, is no finding. A CDC topic (`<topic_prefix>.<source table>`) with
`cleanup.policy` `compact` and without `delete` is a `compact_cdc_topic` finding whether or not retention is declared:
deletes are compacted away after `delete.retention.ms`, so a replay cannot see them.

Findings are entries of the existing **`config_risk`** signal's `risks` list (`events.RetentionRisk`, `sev` `sev4`),
not a new signal:

```json
{ "code": "retention_shorter_than_declared", "sev": "sev4", "topic": "pg.public.orders",
  "configured": "1d", "declared": "7d", "text": "pg.public.orders retains 1d; declared 7d" }
```

A retention finding makes `config_risk` fire (so it is in `/api/fleet` `signals_firing` and in the log) but opens no
room on its own, whatever `config_risk`'s action; when another signal of the pipeline (a `forecast`, for example)
opens or joins a room, the finding rides along in the evidence. An unreadable topic config (no `DescribeConfigs` ACL)
gives one INFO log per pipeline and no finding, and never resolves a finding by itself.

## 6. OpenLineage mapping (X7b)

Spec versions, from the schema files on OpenLineage `main` as of 2026-10-22 (each file's `$id`). X7b vendors these
files under its `testdata/` and validates every event it builds against them (KAN-449 TC1); if a vendored file's
version differs from this table, the vendored file wins and this table is updated in the same PR.

| Schema | Version (`$id`) |
|---|---|
| Event (`RunEvent`, `Run`, `Job`, `InputDataset`, `OutputDataset`, `BaseFacet`) | `https://openlineage.io/spec/2-0-2/OpenLineage.json` |
| `JobTypeJobFacet` | `https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json` |
| `DatasourceDatasetFacet` | `https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json` |
| `SchemaDatasetFacet` | `https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json` |
| `ColumnLineageDatasetFacet` | `https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json` |
| `TagsDatasetFacet` | `https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json` |
| `OwnershipDatasetFacet` | `https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json` |
| Naming conventions (dataset namespaces and names) | OpenLineage docs, `website/docs/spec/naming.md` on `main` |

### 6.1 Events

**Topology event.** At most one per pipeline per `OpenLineageEmitInterval` (15 min): the first build after the
interval sends, builds in between send nothing (KAN-449 TC5). One `RunEvent` per build (TC1):

| Field | Value | Spec |
|---|---|---|
| `eventType` | `COMPLETE`: each event is one finished observation run, so Marquez and DataHub record the lineage without an open run waiting for an end | 2-0-2 `RunEvent.eventType` |
| `eventTime` | the build time, RFC 3339 UTC | 2-0-2 `BaseEvent.eventTime` |
| `producer` | `urn:piperoom:<version.Version>` | 2-0-2 `BaseEvent.producer` (format `uri`) |
| `schemaURL` | `https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent` | 2-0-2 `BaseEvent.schemaURL` |
| `run.runId` | a new UUID v7 per event (time-ordered, as the OpenLineage clients make them; built from `crypto/rand` and the clock, no new module) | 2-0-2 `Run.runId` (format `uuid`) |
| `job.namespace` | `OPENLINEAGE_NAMESPACE` (default `piperoom`) | 2-0-2 `Job.namespace` |
| `job.name` | `piperoom.pipeline.<pipeline_id>` | 2-0-2 `Job.name` |
| `job.facets.jobType` | `processingType` `STREAMING`, `integration` `PIPEROOM`, `jobType` `PIPELINE`, `emissionPattern` `{eventTrigger: PERIODIC, eventContentMode: COMPLETE_SNAPSHOT, windowDuration: 900}` | JobTypeJobFacet 2-0-4 |
| `inputs` | every dataset node with no upstream dataset in the graph (the source tables) | 2-0-2 `RunEvent.inputs` |
| `outputs` | every other dataset node (topics, DLQ topics, sink tables, ClickHouse engine tables and views, models) | 2-0-2 `RunEvent.outputs` |

One event per pipeline rather than one per connector or group: the column-lineage facet already carries every hop
(an output's `inputFields` may name a topic that is itself an output), and one event per build is what the emitter's
rate limit and tests count. Piperoom does not use Debezium's own job name (`{topic.prefix}.{taskId}`), so it never
overwrites the runs of Debezium's built-in OpenLineage integration.

**Action event.** On each `action.executed`, one `RunEvent` for the job `piperoom.<action>` (`action` the step's
action name, e.g. `piperoom.connect_restart_task`) in the same namespace: `eventType` `COMPLETE` when `ok`, `FAIL`
when not, `OTHER` when `outcome_unknown`; a new UUID v7 run id; `jobType` `{processingType: BATCH, integration:
PIPEROOM, jobType: ACTION}`; `outputs` the dataset nodes the step targets when it names a topic or a table (none for
a connector or a group); and the custom run facet `piperoom_action` `{_producer, _schemaURL, pipeline_id,
proposal_id, step_index}` (2-0-2 `RunFacet`, which allows custom facets; its `_schemaURL` points at a schema file X7b
vendors). The step's `output` text is never sent: it is untrusted and may quote values.

### 6.2 Datasets and namespaces

Per the OpenLineage naming conventions. The host and port come from the configured address with **user, password and
query parameters removed** (a DSN's credentials never reach a namespace); a missing port gets the system's default.

| Node | `namespace` | `name` |
|---|---|---|
| `source_table` (Postgres source, `SOURCE_DB_URL`) | `postgres://{host}:{port}` | `{database}.{schema}.{table}` |
| `source_table` (MySQL source, Q8) | `mysql://{host}:{port}` | `{database}.{table}` |
| `topic`, `dlq_topic` | `kafka://{host}:{port}` of the first `KAFKA_BROKERS` entry | `{topic}` |
| `sink_table`, `WAREHOUSE_KIND=postgres` | `postgres://{host}:{port}` (`WAREHOUSE_DB_URL`) | `{database}.{schema}.{table}` |
| `sink_table`, `snowflake` | `snowflake://{organization name}-{account name}` | `{database}.{schema}.{table}` |
| `sink_table`, `bigquery` | `bigquery` | `{project id}.{dataset name}.{table name}` |
| `sink_table`, `databricks` | `unitycatalog://{host}` (the Unity Catalog row) | `{catalog}.{schema}.{table}` |
| `sink_table`, `kafka_engine`, `materialized_view` on `clickhouse` | `clickhouse://{host}:{port}`: **not in the naming table**, Piperoom's choice in the same pattern | `{database}.{table}` |
| `model` | the warehouse's namespace | the model's relation as the dbt client reports it; a model without one is left out |

Dataset facets (the `_producer` and `_schemaURL` of 2-0-2 `BaseFacet` on each):

| Facet | Content | Spec |
|---|---|---|
| `dataSource` | `name` and `uri` = the namespace | DatasourceDatasetFacet 1-0-1 |
| `schema` | `fields[].name` (and `ordinal_position` when known) from the `Columns` maps; no `type`: the graph carries names only | SchemaDatasetFacet 1-2-0 |
| `columnLineage` | on an output with `Columns`: `fields.<To>.inputFields[] = {namespace, name (of Input), field: From, transformations: [{type: DIRECT, subtype: IDENTITY, description, masking: false}]}`; `description` is `rename`, `unwrap` or empty, plus `inferred (name match)` for `Mapping: inferred`; an `added` column has `inputFields: []`; a `dropped` column has no slot in the facet (it maps output fields only) and stays in Piperoom | ColumnLineageDatasetFacet 1-2-0 (subtypes from the facet's docs page) |
| `tags` | from `Node.Tags`: one `{key: "pii", value: <class>, field: <column>, source: "PIPEROOM"}` per `pii` pair; `{key: "pii_omitted", value: "<n>"}`; `{key: "retention", value: "7d", source: "PIPEROOM"}` | TagsDatasetFacet 1-0-0 |
| `ownership` | `owners: [{name: "user:<owner id>", type: "MAINTAINER"}]` from `Node.Owner` | OwnershipDatasetFacet 1-0-1 |

**Never sent:** node health and `facts`, watcher evidence, room content, proposals' params, an action's `output`, any
row or record value, any config value, credentials, the API key.

### 6.3 Transport

- `OPENLINEAGE_URL` is the full endpoint the events are POSTed to (`application/json`); the emitter appends no path.
  Marquez serves `POST /api/v1/lineage`; DataHub's OpenLineage endpoint is `/openapi/openlineage/api/v1/lineage`. X7b
  confirms both in its notes. Empty URL = off: no goroutine starts and nothing is sent (TC3).
- `OPENLINEAGE_API_KEY`, when set, is sent as `Authorization: Bearer <key>` and only over https or to a loopback host
  (`config.ParseOpenLineage` refuses the rest at start). It is never logged and never in a body (TC4).
- One `*http.Client`; each POST has `OpenLineageTimeout` (10 s); a response is read to at most 4 MB and discarded. A
  network error, `429` or `5xx` is retried `OpenLineageMaxRetries` (3) times with backoff, then one WARN log (pipeline,
  job, status, attempts; never the body) and the event is dropped (TC2). Any other `4xx` is not retried.
- An event over `OpenLineageMaxEventBytes` (1 MiB) is sent without its `columnLineage` and `schema` facets, and
  dropped with a WARN if still over.
- The emitter never blocks a room: the server hands it the graph or the executed step and returns; a bounded queue
  (drop-oldest, counted in the WARN) feeds one goroutine that stops when the server's context ends. X7b exposes the two
  calls; the server's wiring (A5's file) adds one line each, asked in thread.

## 7. Tenancy, data handling, tests

- **Who reads it.** Topology `tags` and `columns` follow the topology route's rule: `404 not_found` for an unknown
  pipeline and for one outside the caller's tenants, then any logged-in user. The agent sees them in `pipeline_map`'s
  render. The OpenLineage server sees every pipeline of the deployment in one namespace: a multi-tenant deployment
  that must keep tenants apart in lineage runs one server per tenant or leaves the emitter off.
- **Data handling.** `docs/pilot/data-handling.md` has a row for each new reader (column catalog, table retention, the
  watcher's topic config read) and for the OpenLineage egress.
- **X7-0 tests.** TC1 `TestRenderGolden` (unchanged golden file) and `TestX7OldGraphUnchanged`; TC2
  `TestX7GraphRoundTrip` (strict decode, zero values, unknown fields refused); TC3 the table in section 6, each row
  citing its schema version; TC4 `go build`, `go vet`, `go test ./...` and the web `NODE_KINDS` test (no node kind
  was added). E4 caps: `TestX7LimitColumns`, `TestX7ValidTag`, `TestX7PIITag`, `TestX7Retention`; E5 fakes:
  `internal/clients/x7_test.go` (`fakeCatalog`, and `*kafka.Client` asserted as a `TopicConfigReader`); config:
  `internal/config/x7_test.go`; evidence: `internal/events/x7_test.go`.

## 8. Follow-ups named by this contract

- A3: load and validate `declared_retention` and `pii` (section 4); implement `ColumnReader` on the source Postgres and
  the warehouse clients (X7a/X7c); fill `Columns`, `Tags.pii` and `Tags.retention`; render them (section 2).
- A7: retention findings in `config_risk` (section 5); the sev4 no-room rule.
- A4: `agent.AlertEvidenceText` allow-list for the retention entry's keys: names `topic`, `table`; durations
  `configured`, `declared`; number `retention_bytes`; word `cleanup_policy`; `code`, `sev`, `text` as for the other
  `config_risk` entries.
- A6: `web/src/api/types.ts` `Node.tags`, `Edge.columns`, `Edge.columns_omitted`; a PII badge on lane nodes; the
  retention entries of `config_risk`.
- C3 (and A3 for Postgres): `TableRetentionReader` where the warehouse has a retention setting (BigQuery expiration,
  ClickHouse TTL); none is required for X7d to ship.
- C1/C14: `OPENLINEAGE_*` in `deploy/pilot.env.example`, the Helm values and `docs/pilot/setup.md`.
