# A2 — Kafka, Kafka Connect and Schema Registry: clients, tools, actions

Read `AGENTS.md` first. You own: `internal/clients/connect/**`, `internal/clients/schemareg/**`,
`internal/clients/kafka/**`, `internal/tools/kafkatools/**`, `internal/actions/**` (not `action.go`).

## Goal

Give the agent accurate, compact facts about the Kafka side of a pipeline, and give the room
service safe write actions for approved fixes.

## 1. Clients (implement the interfaces in `internal/clients/clients.go`)

### `connect` → `clients.ConnectClient`
`New(baseURL string, httpClient *http.Client) *Client`. Kafka Connect REST:
`GET /connectors`, `GET /connectors/{name}/status`, `GET /connectors/{name}/config`,
`POST /connectors/{name}/tasks/{id}/restart`, `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true`.
404 → wrap `clients.ErrNotFound`. Path-escape names.

### `schemareg` → `clients.SchemaRegistryClient`
`New(baseURL string, httpClient *http.Client) *Client`. Confluent Schema Registry REST,
`Accept: application/vnd.schemaregistry.v1+json`:
`GET /subjects`, `GET /subjects/{s}/versions`, `GET /subjects/{s}/versions/{v|latest}`,
`GET /config/{s}` (404 with error_code 40408 = no subject level → then `GET /config` for global),
`PUT /config/{s}`, `POST /subjects/{s}/versions`, `GET /schemas/ids/{id}`.
Also expose `SchemaByID(ctx, id int) (string, error)` on the concrete type (used for decoding).
Parse the registry error body `{"error_code":..,"message":..}` into a typed error.

### `kafka` → `clients.KafkaAdmin`
`New(brokers []string, sr *schemareg.Client) (*Client, error)` with `franz-go` (`kgo` + `kadm`); `Close()`.
- `Topics`: skip internal topics (`__*`, `_connect_*`, `_schemas`).
- `TopicOffsets`: start + end per partition, and `LastTimestamp` = timestamp of record `End-1`
  (fetch exactly that offset with a short-lived consumer; zero time if the partition is empty).
- `GroupLag`: state, member count, per-partition committed/end/lag, total. A partition with no
  commit has `Committed = -1` and lag = end − start.
- `FindByKey`: start at the first offset with timestamp ≥ `since` (ListOffsetsAfterMilli), scan up
  to `maxScan` records per partition, decode the key (Confluent Avro via the registry; if the key
  is a record with one field, compare that field's value as a string; otherwise compare JSON).
- Verify every franz-go symbol with `go doc` before use (AGENTS.md rule 9).

## 2. Tools (`internal/tools/kafkatools`) — all `tools.KindRead`

Provide `func Register(reg *tools.Registry, d Deps) error` where `Deps` holds the three
interfaces (not the concrete types), so tests use fakes.

| Name | Input | Summary must say |
|---|---|---|
| `connect_list_connectors` | – | each connector: name, type, state, failed task count |
| `connect_status` | `connector` | connector state; each task state; for FAILED tasks the **exception class and message lines + every `Caused by:` line**, not the full stack (≤ 2000 chars per task) |
| `connect_config` | `connector` | key settings: class, table include list, topic prefix, slot name, converters, decimal mode. **Redact** any key containing `password`, `secret`, `token`, `key` (value → `***`) |
| `schema_registry_subjects` | `prefix?` | matching subjects |
| `schema_registry_versions` | `subject` | versions, latest id, compatibility level and whether it is subject-level or global |
| `schema_registry_diff` | `subject`, `from_version`, `to_version?` (default latest), `schema?` (Avro JSON; diff this instead of `to_version`) | field-level diff for Avro record schemas: added / removed / type-changed fields (e.g. `amount: int → bytes(decimal 12,2)`) |

**Important (observed on the live stack):** a schema that Schema Registry **rejects is never registered**, so a
version-to-version diff shows nothing. The attempted schema appears only inside the FAILED task trace
(`Error registering Avro schema{…}`). Therefore `connect_status` must parse that schema from the trace, diff it
against the subject's latest version, and print the field changes — that is how the agent finds the real cause.
| `kafka_topic_offsets` | `topic` | per partition start/end, total records, newest record time and "N min ago" |
| `kafka_list_groups` | `topic?` | groups (optionally only those reading the topic) with state |
| `kafka_consumer_lag` | `group` | state, members, total lag, worst 5 partitions |
| `kafka_find_by_key` | `topic`, `key`, `since` (RFC 3339), `max_scan?` (default 5000) | found or not; where (partition/offset/time) |

Every tool: validate input (wrap `tools.ErrBadInput`), 10 s timeout, `tools.Truncate` on the summary,
structured `Data`. Summaries are short, factual, include units and times — the model reads them.
Example `connect_status` summary:
```
pg-orders-connector (source): RUNNING. Tasks: 0=FAILED on 172.19.0.5:8083.
Task 0 error: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data ...
  Caused by: ... Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value" ...
```

## 3. Actions (`internal/actions`) — write operations, run only after approval

Each implements `actions.Action`. Provide `func Register(reg *actions.Registry, d Deps) error`.

| Name | Layer | Params |
|---|---|---|
| `connect_restart_task` | `kafka-connect` | `connector`, `task` |
| `connect_restart_connector` | `kafka-connect` | `connector` (failed tasks only) |
| `schema_set_compatibility` | `schema-registry` | `subject`, `level` (BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE) |
| `schema_register` | `schema-registry` | `subject`, `schema` (Avro JSON string) |

`Validate` checks params and that targets exist (read calls only). `Execute` returns a short
result like `restarted pg-orders-connector task 0 (RUNNING)` — re-read the state after the write.

## Tests

- Unit: `httptest` servers returning **real** response bodies (capture them from the dev stack
  into `testdata/`), including error bodies and a FAILED task trace. Fake `KafkaAdmin` for tool tests.
- Integration (tag `integration`, use `internal/testenv` when A1 lands; until then skip if
  `localhost:8083` is unreachable): create a temp topic + group, produce, check offsets and lag;
  read the real `pg-orders-connector` status; register a schema under a temp subject and diff versions.

## Done when

On the `schema-change` scenario, calling `connect_status`, `schema_registry_versions` and
`schema_registry_diff` gives a human the full root cause in three short summaries.
