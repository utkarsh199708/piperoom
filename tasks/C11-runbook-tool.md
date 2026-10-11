# C11 — `runbook_lookup`: sourced CDC/Kafka runbooks for the agent (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-12 00:30 IST):** launched as a claude.ai/code session. Corrections: per-file cap
> 8192 bytes (4096 does not fit the sources); truncation test builds `[]Runbook` directly and calls `New(rbs)`.
> If the PR passes ~1000 lines, ship 5 runbooks (slot-wal, connector-failed, schema-incompatible, consumer-lag,
> source-ddl) and move poison-pill and retention-loss to a follow-up. The tool is pure (no clients) — say in the
> PR that C6 evals can run it live without replay recordings.


Read these first: `AGENTS.md`, `docs/collab.md`, `internal/tools/tool.go` (contract), `internal/tools/dbtools/dbtools.go`,
`internal/tools/kafkatools/kafkatools.go`, `internal/actions/kafka_actions.go`, `internal/agent/toolparams.go` and
`runReadTool` in `internal/agent/run.go` (how a tool's schema reaches the model and how its output is wrapped), and
`dev/scenarios.md` (symptoms we really saw).

You own a **new** package, `internal/tools/runbooks/**`. It holds the Go code and the markdown in `internal/tools/runbooks/library/`.
Do not edit dbtools, kafkatools, actions, agent, wiring, evals or any contract file. This is not `docs/runbook.md`, which is C1's
dev-stack how-to.

You run in the cloud: no Docker, no network in tests, no `ANTHROPIC_API_KEY`. Use the standard library only and add no new
modules. Open one GitHub PR titled `[C11] runbook_lookup tool` from your `claude/...` branch into `main`, and never touch `main`.
**Do not wire the tool.** After this merges, A4 adds `runbooks.Register(reg)` to `internal/wiring` (`RegisterTools`, on branch
`a4/wiring`, not merged yet). Say this in the PR.

## Goal
Give the agent a read-only tool that returns short, sourced runbook sections for common CDC/Kafka failure modes. Each section
names the real Piperoom tools that reveal the problem, the safe checks, and which fixes are Piperoom actions and which are manual.

## Work
1. **API.** Use exactly these names:
   ```go
   const ToolName = "runbook_lookup"
   type Section struct{ Heading, Text string }
   type Runbook struct {
       ID, Title string
       Keywords  []string
       Sections  []Section // Symptoms … Risks, in file order (Sources is not a section)
       Tools     []string  // from "Find it with Piperoom tools"
       Actions   []string  // from Fixes items that start with: Action `name`
       Sources   []string  // https URLs from "Sources"
   }
   func Load(fsys fs.FS) ([]Runbook, error) // every *.md at the root of fsys, sorted by file name
   func Embedded() ([]Runbook, error)       // //go:embed library/*.md, then fs.Sub(…, "library")
   func New(rbs []Runbook) *Tool            // *Tool implements tools.Tool
   func Register(reg *tools.Registry) error // Embedded -> New -> reg.Register
   ```
   Parse once in `New`/`Register`. `Run` does no I/O. The `embed.FS` var is read-only, so it does not break AGENTS.md rule 5.
2. **Runbook file format.** `id` must equal the file name without `.md`. Sections use exactly these `## ` headings, in this order:
   ```markdown
   ---
   id: slot-wal
   title: Replication slot inactive or WAL retention growing
   keywords: replication slot, wal, retained, restart_lsn, inactive, wal_status, lost, long transaction
   ---
   ## Symptoms
   ## Find it with Piperoom tools
   - `pg_replication_slots` — active=false, or retained WAL keeps rising
   ## Safe checks
   ## Fixes
   - Action `connect_restart_task` — only after the cause is fixed …
   - Manual — end the blocking transaction with `pg_terminate_backend(pid)` (DBA) …
   ## Risks
   ## Sources
   - https://www.postgresql.org/docs/current/view-pg-replication-slots.html — slot columns, wal_status values
   ```
   `Load` returns a wrapped error (`runbooks: <file>: …`) when any of these is true:
   - `id`, `title` or `keywords` is missing, or `id` is a duplicate.
   - A section is missing, out of order or extra.
   - A "Find it" item does not start with a backticked name.
   - A Fixes item starts with neither ``Action `name` `` nor `Manual`.
   - Sources is empty, or a source is not `https` on `www.postgresql.org`, `debezium.io`, `kafka.apache.org` or `docs.confluent.io`.
   - The file is larger than 4096 bytes.

   `Load` also accepts CRLF line endings.
3. **Library.** Write 7 files, each 35–60 lines. Each one lists at least the tools below. The only Piperoom actions are the 4 in
   `internal/actions/kafka_actions.go`. Everything else is `Manual`.

   | file | covers | tools | Fixes |
   |---|---|---|---|
   | `slot-wal.md` | slot inactive (connector FAILED, PAUSED or missing), WAL growth, a long transaction pinning `restart_lsn`, `wal_status` `lost`, `max_slot_wal_keep_size`, `idle_replication_slot_timeout`, heartbeats | `pg_replication_slots`, `pg_long_transactions`, `connect_status`, `connect_list_connectors`, `pipeline_map` | Action `connect_restart_task` / `connect_restart_connector`; Manual: resume a paused connector, `pg_terminate_backend`, heartbeat config, drop the slot (last resort) |
   | `connector-failed.md` | Debezium task FAILED: auth, REPLICATION/LOGIN, `pg_hba.conf`, CREATE for publications, a table missing from the publication, `publication.autocreate.mode` | `connect_list_connectors`, `connect_status`, `connect_config`, `pg_replication_slots` | Action `connect_restart_task` / `connect_restart_connector` (only failed tasks are restarted: `onlyFailed=true` in `internal/clients/connect`), only after the cause is fixed; Manual: grants, `ALTER PUBLICATION … ADD TABLE` |
   | `schema-incompatible.md` | Registry rejects a schema: levels, BACKWARD is the default, the verbose compatibility test, the demo `schema-change` | `connect_status`, `schema_registry_subjects`, `schema_registry_versions`, `schema_registry_diff`, `pg_recent_ddl` | Action `schema_set_compatibility` (temporary, then restore the old level), `schema_register`, `connect_restart_task` |
   | `consumer-lag.md` | lag growth, slow processing, rebalance storms, `max.poll.interval.ms`, `max.poll.records`, `group.instance.id`, CooperativeStickyAssignor, `group.protocol` | `kafka_list_groups`, `kafka_consumer_lag`, `kafka_topic_offsets`, `warehouse_freshness`, `connect_status` | all Manual (there are no consumer actions) |
   | `poison-pill.md` | one bad record blocks a partition, `RecordDeserializationException`, Connect `errors.tolerance` + DLQ, skipping one offset | `kafka_consumer_lag`, `kafka_list_groups`, `kafka_find_by_key`, `trace_record`, `pg_row`, `connect_status` | Action `connect_restart_task` (sink connector, after a config fix); Manual: fix the consumer, offset reset with the group stopped, DLQ config |
   | `retention-loss.md` | `retention.ms`/`retention.bytes` deleted unread records, `auto.offset.reset`, backfill with an incremental snapshot | `kafka_topic_offsets`, `kafka_consumer_lag`, `kafka_list_groups`, `warehouse_freshness`, `trace_record`, `warehouse_row` | all Manual: raise retention, the Debezium `execute-snapshot` signal, offset reset |
   | `source-ddl.md` | dropped, renamed or retyped column; the Postgres connector emits no DDL events; REPLICA IDENTITY; `pg_recent_ddl` needs the DDL audit trigger (`piperoom_audit.ddl_log`) | `pg_recent_ddl`, `schema_registry_versions`, `schema_registry_diff`, `connect_status`, `pg_row`, `warehouse_row`, `trace_record` | Action `schema_set_compatibility`, `schema_register`, `connect_restart_task`; Manual: consumer or warehouse migration, revert the DDL |

   Sources. Use these and only these. Each one was checked on 2026-10-11. Open each again with WebFetch before you cite it.
   - slot-wal: postgresql.org/docs/current/{view-pg-replication-slots, runtime-config-replication, functions-admin, monitoring-stats, runtime-config-client}.html;
     debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-wal-disk-space; kafka.apache.org/41/kafka-connect/user-guide/
   - connector-failed: the Debezium postgresql.html with `#postgresql-permissions` and `#postgresql-publication-autocreate-mode`;
     postgresql.org/docs/current/{logical-replication-security, sql-createpublication, sql-alterpublication, view-pg-publication-tables, view-pg-roles}.html;
     docs.confluent.io/platform/current/connect/references/restapi.html
   - schema-incompatible: docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html and …/schema-registry/develop/api.html;
     debezium.io/documentation/reference/stable/configuration/avro.html; the Debezium postgresql.html with `#postgresql-property-decimal-handling-mode`
   - consumer-lag: kafka.apache.org/41/configuration/consumer-configs/; kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html;
     kafka.apache.org/41/operations/basic-kafka-operations/
   - poison-pill: kafka.apache.org/41/kafka-connect/user-guide/ ("Error Reporting in Connect"); kafka.apache.org/41/configuration/kafka-connect-configs/;
     kafka.apache.org/41/javadoc/org/apache/kafka/common/errors/RecordDeserializationException.html; basic-kafka-operations (above)
   - retention-loss: kafka.apache.org/41/configuration/topic-configs/; consumer-configs and basic-kafka-operations (above);
     debezium.io/documentation/reference/stable/configuration/signalling.html
   - source-ddl: the Debezium postgresql.html (it says logical decoding does not support DDL; see `#postgresql-replica-identity`);
     postgresql.org/docs/current/sql-altertable.html (`#SQL-ALTERTABLE-DESC-SET-DATA-TYPE`, `-DROP-COLUMN`, `-RENAME`, `#SQL-ALTERTABLE-REPLICA-IDENTITY`);
     the Confluent schema-evolution page

   Use the pinned Kafka 4.1 URLs. `kafka.apache.org/documentation/` is only a redirect page.
4. **Content rules.**
   - Back every config name, default, view, column, REST path and SQL function with one of that runbook's Sources. If you cannot
     find it there, leave it out.
   - Quote error text only from `dev/scenarios.md` or a source page. Never invent error strings.
   - Add one "Seen on the Piperoom dev stack" line where `dev/scenarios.md` has the case. Examples: the registry reports
     `MISSING_UNION_BRANCH`, not the changed field; a slot stays active while its connector is PAUSED; `restart_lsn` gets stuck
     behind a long transaction.
   - Write for an engineer, not as orders to the model. Tool output reaches the model wrapped as untrusted data.
   - Dropping a slot, deleting topics or data, and resetting offsets are always `Manual`, each with a Risks line. The system
     prompt (`internal/agent/prompt.go`) forbids proposing slot drops or data deletion.
5. **Tool behavior.**
   - Name `runbook_lookup`. Kind `tools.KindRead`: it reads only embedded text and never touches customer systems.
   - Description, at most 400 characters: it covers the 7 failure modes, returns matching sections with official doc links, and is
     reference text, not live data, so the agent must confirm with the read tools.
   - Schema, exactly:
     `{"type":"object","properties":{"query":{"type":"string",…},"max_results":{"type":"integer","description":"How many sections, 1-5. Default 3."}},"additionalProperties":false,"required":["query"]}`.
   - Do not use `minimum`, `maximum`, `multipleOf`, `minLength` or `maxLength`. Strict tool use rejects them with a 400
     (https://platform.claude.com/docs/en/build-with-claude/structured-outputs).
   - Do not add a property named `why`. The agent adds `why` to every registry tool and removes it before `Run`.
   - Input: decode with `json.Decoder` + `DisallowUnknownFields`. Return an error that wraps `tools.ErrBadInput` when the input
     is empty, has an unknown field or a wrong type, when `query` is blank, longer than 1000 bytes or not valid UTF-8, or when
     `max_results` < 0.
   - `max_results` of 0 means 3, and any value above 5 becomes 5. If ctx is already done, return the ctx error.
   - Scoring, one unit per section:
     - Tokens: lowercase, then `strings.FieldsFunc`. Split on every rune that is not a letter, a digit, `_` or `.`, and trim
       `.` from both ends of each token.
     - Drop tokens shorter than 2 characters and a small stopword list. When a token contains `_` or `.`, also add its parts.
     - Use at most 32 distinct query terms.
     - For each term the score adds: 3 if the term is in the runbook's title or keywords, plus 2 if it is in the section
       heading, plus its count in the section text, capped at 3.
     - Drop units that score 0. Sort with `slices.SortStableFunc` by score (high first), then file order, then section order.
   - Output:
     - Summary: one block per hit, `[n] <title> › <section> (score N)`, then the section text cut to 1500 bytes without splitting
       a rune, then `Sources: url; url`. Pass the whole Summary through `tools.Truncate` and set `Truncated`.
     - Data: `{"query":…,"hits":[{"runbook","title","section","score","text","sources"}]}` with the full text. `hits` is never `null`.
     - No match is not an error. Return the Summary `No runbook section matched "<q>". Runbooks: <titles>` and empty `hits`.
6. **Tests** (unit, offline):
   - (a) `Embedded()` returns the 7 ids, and each file is within the size cap.
   - (b) Real names. Build the registries the way C6's `evals/replay.go` `LiveTools` does (that PR is open, not on `main`):
     `kafkatools.Register(reg, kafkatools.Deps{})`, `dbtools.Register(reg, dbtools.Deps{Owners: &owners.Owners{}})`,
     `actions.Register(areg, actions.Deps{})`. Every `Tools` entry must resolve with `reg.Get`, and every `Actions` entry with
     `areg.Get`. Then scan each whole file: every backticked token matching `^(connect|kafka|schema|warehouse|pipeline|trace)_[a-z0-9_]+$`
     must be a real tool or action.
   - (c) Every runbook has at least 1 source, and every source is https on an allowed host (check with `net/url.Parse`, no fetching).
   - (d) `Load` rejects bad files. Use a `fstest.MapFS` table with one case per rule in item 2.
   - (e) Ranking. Each query must return a top hit from the named runbook:

     | query | runbook |
     |---|---|
     | "replication slot inactive wal growing" | slot-wal |
     | "connector task failed permission publication" | connector-failed |
     | "incompatible BACKWARD 409 subject" | schema-incompatible |
     | "rebalance max.poll.interval.ms" | consumer-lag |
     | "deserialization dead letter queue" | poison-pill |
     | "retention.ms deleted offset out of range" | retention-loss |
     | "dropped column renamed type change" | source-ddl |

     Put these query words in each runbook's keywords. If a case fails, fix the keywords, not the weights. The same query gives
     the same order every time. A stopword-only query returns no hits and no error.
   - (f) Schema strictness: the keys are exactly `query` and `max_results`, `required` is exactly `["query"]`, there is no `why`,
     and none of the forbidden keywords appear. Also test every `ErrBadInput` path with `errors.Is`, and that `max_results` 0 gives
     3 hits and 99 gives 5.
   - (g) Truncation. A broad query with `max_results` 5 keeps Summary ≤ `tools.MaxSummaryBytes`. A MapFS runbook with 20 KB
     sections gives `Truncated` true and a valid UTF-8 Summary.
   - (h) A cancelled ctx gives an error. A second `Register` on the same registry fails as a duplicate.

## Done when
`var _ tools.Tool = (*Tool)(nil)` compiles. `gofmt -l .` is empty, `go vet ./...` is clean, and `go test ./...` passes offline.
The PR lists which source pages backed each runbook. It also states the follow-ups: A4 wires the tool and may name it in the
system prompt, and the evals owner adds it to `LiveTools`.
