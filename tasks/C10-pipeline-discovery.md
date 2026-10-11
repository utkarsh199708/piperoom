# C10 — Pipeline auto-discovery for pilot onboarding (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-12 00:30 IST):** launched as a claude.ai/code session (own `claude/...` branch is fine).
> Corrections: C8 is merged — build clients with `kafka.NewSecure`, `schemareg.NewSecure`, `connect.NewSecure`
> from the `KAFKA_*` / `SCHEMA_REGISTRY_*` / `CONNECT_*` config fields, the same way `internal/wiring`
> (`buildA2Clients`, branch `a4/wiring`) does. For skipped **sink** connectors, read only the non-secret `topics`
> key and add a note when it names a pipeline topic (owners `connectors` is the CheckScope list for
> `connect_restart_*`). Note in the PR that topology matches include lists literally (A3 follow-up). Up to
> ~1100 changed lines is accepted if ~300 are tests/golden data; else move the `-source-db` slot check to a follow-up.


Read `AGENTS.md`, `docs/collab.md`, `tasks/A1-devstack.md` §2 (owners.yaml format) and `tasks/A3-db-tools-topology.md` §2–3
first, then `internal/clients/clients.go`, `internal/owners/owners.go`, `internal/topology/builder.go` (`deriveTopics`,
`slotNameFor`) and `cmd/investigate/wire.go` (how clients are built). You own two **new** folders: `internal/discover/**` and
`cmd/discover/**`. Edit nothing else: not `internal/owners/**` (A3), `internal/clients/**` (A2/A3/C8), `internal/wiring/**` (A4),
`dev/**` (A1), and no contract file.

No Docker, no dev stack and no `ANTHROPIC_API_KEY` in the cloud: unit tests with fakes only. Open one GitHub PR titled
`[C10] discover: draft owners file from a live cluster` into `main`.

## Goal
Onboarding a pilot customer today means writing their `owners.yaml` by hand. Instead, one read-only command
`go run ./cmd/discover -out owners.draft.yaml` reads Kafka Connect, Kafka and Schema Registry (and, with `-source-db`, the source
Postgres) and writes a **draft** in the `dev/owners.yaml` shape: one pipeline per Debezium Postgres source connector. A human then
fills the TODOs (titles, sink tables, layer owners) and copies it into `OWNERS_FILE`.

## Work
1. **Read-only by type.** In `internal/discover`, define narrow interfaces. Copy the method signatures exactly from `clients.go`:
   `ConnectReader` (`ListConnectors`, `Config`), `KafkaReader` (`Topics`, `ConsumerGroups`, `GroupLag`), `SubjectLister` (`Subjects`),
   `SlotLister` (`ReplicationSlots`). Assert `var _ ConnectReader = clients.ConnectClient(nil)` (same for the other three), so no
   write method can be called from this package.
   `Deps{Connect ConnectReader; Kafka KafkaReader; Registry SubjectLister; Source SlotLister /* nil = skip */}`,
   `Options{CallTimeout time.Duration /* default 10 s */; MaxGroups int /* default 1000 */}`.
2. **`Discover(ctx, Deps, Options) (Result, error)`.** Each client call gets its own `context.WithTimeout(ctx, CallTimeout)`. Call `Topics`,
   `Subjects` and `ConsumerGroups` once each, and `GroupLag` at most once per group (cache the result). Go through connectors in sorted order:
   1. `ListConnectors`, then `Config(name)`. Keep only `connector.class == "io.debezium.connector.postgresql.PostgresConnector"`. All
      others go to `Result.Skipped` with a short reason: `not a Debezium Postgres source (class X)`, `deleted during discovery`
      (`clients.ErrNotFound`), or `config read failed`.
   2. Topic prefix: `topic.prefix`. If it is empty, use the Debezium 1.x key `database.server.name` and add a note. If both are empty, skip
      the connector. If two connectors share a prefix, add a warning.
   3. Topic-derived tables: a topic `T` from `Topics()` counts when it starts with `prefix + "."` and the rest is exactly two
      non-empty dot parts (`schema.table`). This drops `<prefix>.transaction` and the topics of a longer prefix (`pg.eu.public.x` under `pg`).
   4. `source_tables`:
      - `table.include.list` set: split on `,` and trim. Each entry is a Debezium regex that must match the **whole** `schema.table` id.
        Compile it as `^(?:entry)$` with `regexp.Compile` and keep the matching topic-derived tables. If the entry is a plain
        `schema.table` (only `[A-Za-z0-9_]` plus one `.`), also add it literally, even when it has no topic yet (note `no topic yet for X`).
        If the entry does not compile (Java-only syntax), add the note `include entry "X" is not RE2 — add its tables by hand`.
      - Not set: take all topic-derived tables. Drop those that match `table.exclude.list`, and filter the schema part with
        `schema.include.list` / `schema.exclude.list` (same anchored match). Add the note `source_tables guessed from existing topics`.
      - If the result is empty, keep the pipeline with `source_tables: []` and a TODO note. `owners.Load` will reject the file until a
        human fills it in. This is intended.
   5. Pipeline topics = `prefix + "." + table` for each source table whose topic exists in `Topics()`.
   6. Naming overrides: add the note `custom topic naming (<keys>): check topic names by hand` when `transforms` is not empty,
      `topic.delimiter` is set and is not `.`, or `topic.naming.strategy` is set and is not `io.debezium.schema.SchemaTopicNamingStrategy`.
      Name the keys only, never their values.
   7. Schema Registry: for each pipeline topic, if `<topic>-value` is not in `Subjects()`, add the note
      `no subject <topic>-value (JSON converter or non-default subject name strategy?)`. If `Subjects` fails, add one global warning and skip this step.
   8. Consumer groups: a group belongs to a pipeline when `GroupLag(group).Partitions` has an entry whose `Topic` is a pipeline topic
      and whose `Committed >= 0` (in the contract, `-1` means nothing committed). On `clients.ErrNotFound`, skip the group quietly. On any
      other error, add a warning that names the group. A group in state `Empty` or `Dead` gets the note
      `group X state Empty at discovery — maybe retired`. If there are more than `MaxGroups` groups, check the first `MaxGroups` in sorted order and warn how many were not checked.
   9. Slot check, only when `Deps.Source != nil`: the slot is `slot.name`, default `debezium`. If it is not in `ReplicationSlots()`, add the note
      `slot X not found in SOURCE_DB_URL (connector may point at another database)`. If it is found but `Active == false`, add the note `slot X inactive`.

   Only errors from `ListConnectors` and `Topics` are fatal (`fmt.Errorf("discover: list connectors: %w", err)`). Error details go to
   `slog` on stderr. Notes in the YAML hold fixed text plus names only.
3. **IDs and order.** `id` = the connector name in lower case, with each run of `[^a-z0-9]` turned into `-`, then trimmed, and a
   trailing `-connector` removed (`pg-orders-connector` → `pg-orders`). An empty result becomes `pipeline`. Duplicates get `-2`, `-3` in sorted order.
   Sort pipelines by `id`. Sort and de-duplicate every list. The same cluster state must give a byte-identical file (no timestamps, no map order in the output).
4. **`Render(w io.Writer, r Result) error`.** Build `gopkg.in/yaml.v3` `yaml.Node` trees (`MappingNode`; `SequenceNode` with
   `FlowStyle`; `HeadComment` / `LineComment` / `FootComment`) and write them with `yaml.NewEncoder`, `SetIndent(2)` and `Close`.
   Do not use string templates: the encoder must quote names. Target shape:
   ```yaml
   # DRAFT generated by go run ./cmd/discover. Review every TODO, then copy into OWNERS_FILE by hand.

   pipelines:
     # note: source_tables guessed from existing topics
     - id: pg-orders
       title: pg-orders-connector # TODO: human-friendly title
       topic_prefix: pg
       source_tables: [public.orders]
       connectors: [pg-orders-connector]
       consumer_groups: [order-sync]
       # TODO: warehouse tables this pipeline loads (schema.table), then set sink_ts_column
       sink_tables: []
       sink_ts_column: ""
   layers:
     # TODO: replace each TODO with a user id from USERS_FILE (the server checks they agree)
     postgres: TODO
     kafka-connect: TODO
     schema-registry: TODO
     kafka: TODO
     consumer: TODO
     warehouse: TODO

   # skipped connectors:
   #   <name>: not a Debezium Postgres source (class <connector.class>)
   ```
   - Keys and their order follow `tasks/A1-devstack.md` §2. Write `topic_prefix` even though `owners.Pipeline` has no `TopicPrefix` field
     yet (A3 adds it). `owners.Load` uses `yaml.Unmarshal`, which ignores unknown keys, so the draft still loads.
   - `layers` lists `events.Layers` in that order, each set to `TODO`.
   - In any text that goes into a comment, replace control characters (< 0x20 and 0x7f).
   - **No secrets.** Read only these config keys: `connector.class`, `topic.prefix`, `database.server.name`,
     `table.include.list`, `table.exclude.list`, `schema.include.list`, `schema.exclude.list` and `slot.name`. For `transforms`,
     `topic.delimiter` and `topic.naming.strategy`, check only whether they are set. No result type holds a config map. No URL or DSN goes into the file.
5. **`cmd/discover/main.go`.**
   - Flags: `-out` (required; `-` means stdout), `-force` (overwrite; without it, open with `os.O_WRONLY|os.O_CREATE|os.O_EXCL`),
     `-source-db` (also run the slot check), `-timeout` (whole run, default 2m).
   - Read addresses only through `config.Load()`: `ConnectURL`, `SchemaRegistryURL`, `KafkaBrokers` (split on `,` and trim) and `SourceDBURL`.
     Add no new env vars.
   - Build clients like `cmd/investigate/wire.go`: one `&http.Client{Timeout: 10 * time.Second}`, then `connect.New(cfg.ConnectURL, hc)`,
     `schemareg.New(cfg.SchemaRegistryURL, hc)` and `kafka.New(brokers, registry)` (`defer Close()`). With `-source-db`, also
     `postgres.New(ctx, cfg.SourceDBURL)` (`defer Close()`). Do not import `internal/wiring` (A4, not merged).
   - Never print a URL or DSN. In errors, name the env var instead.
   - Print a summary to stderr: number of pipelines, skipped connectors and warnings, and `N pipelines have no source_tables`.
   - Exit codes: 0 ok, 1 error, 2 bad flags. Put the logic in `run(ctx, args, stdout, stderr) int` so it can be tested.
6. **PR description.** Be honest about the gaps: `kafka.New` has no SASL/TLS options, so a pilot cluster that needs auth cannot be read
   yet (C8 owns `internal/clients/kafka`). Connect and Registry auth are not handled. Only Debezium Postgres connectors are supported.
   Cite https://debezium.io/documentation/reference/stable/connectors/postgresql.html (properties) and
   https://debezium.io/blog/2022/10/17/debezium-2-0-final-released/ (`database.server.name` renamed to `topic.prefix`).

## Tests
- Put fakes for the four interfaces in `_test.go`. They return lists **unsorted**, to prove the ordering code works.
- Table tests:
  - prefix (`topic.prefix`; `database.server.name`; neither set)
  - class filter (Postgres kept; another `io.debezium.connector.*` class skipped; a sink connector skipped)
  - include list as a literal, as a regex (`public\.order.*`), and as a non-RE2 entry (`public\.(?!tmp).*`)
  - no include list, with `table.exclude.list` and `schema.include.list`
  - `pg.transaction` and `pg.eu.public.x` are not taken as tables of `pg`
  - the naming-override note names keys only
- Consumer groups:
  - committed offset on a pipeline topic → included
  - only `Committed == -1` → not included
  - offsets on other topics only → not included
  - `ErrNotFound` → skipped quietly
  - other error → warning, and the run continues
  - `MaxGroups` cap works
- Registry and slot:
  - a missing `-value` subject adds a note
  - a `Subjects` error adds a warning, and the file is still written
  - slot found, missing, and inactive
  - `Source == nil` → no slot notes
- Errors:
  - a `ListConnectors` error is wrapped and passes `errors.Is`
  - a `Config` `ErrNotFound` skips that connector
  - a fake that blocks until ctx is done, with `CallTimeout` set to 50 ms, gives a warning or an error and does not hang
- Golden file `internal/discover/testdata/draft.golden.yaml` for a 3-connector fixture. Rendering twice gives identical bytes.
- Secrets: the fixture config holds `database.password: s3cr3t-pw`, `database.user` and `database.hostname`. None of their values appear in the output.
- Round trip: render the happy fixture into `t.TempDir()`, run `owners.Load` on it, and check the pipelines, connectors and groups.
- A connector name that contains `\n` and `#` stays on one comment line, and the output still parses with `yaml.Unmarshal`.
- `cmd/discover`: a missing `-out` gives exit 2. An existing file without `-force` gives an error and the file stays unchanged.
- Integration test (tag `integration`). You cannot run it, so say so in the PR. The lead runs it after `go run ./cmd/devctl up`. It builds real
  clients from `config.Load()` and checks that the pipeline for `pg-orders-connector` has `topic_prefix: pg`, that `source_tables` contains
  `public.orders`, and that `consumer_groups` contains `order-sync`.

## Done when
- `gofmt -l` is empty, and `go vet ./...` and `go build ./...` pass.
- `go test ./internal/discover/... ./cmd/discover/...` passes with no network.
- The golden draft loads with `owners.Load`.
- `internal/discover` cannot reach any client write method (checked at compile time).
- The PR (~600–900 changed lines) lists the gaps and the Debezium doc links above.
