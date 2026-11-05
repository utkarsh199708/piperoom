# DLQ awareness: see the records that go missing (rev 2, main @ 85fa78d, 2026-10-13)

Every file and function named below was read on `85fa78d`. franz-go names were checked with `go doc`
on `kadm v1.19.0` / `kgo v1.22.1` (the versions in `go.mod:17-19`). Kafka Connect header names come
from Kafka's `DeadLetterQueueReporter` (KIP-298) and are marked **unverified** where I could not
check them against a live cluster. Style follows `docs/design/watcher.md` and `docs/design/shadow.md`.

Rev 2 applies the security and fit reviews of rev 1. §9 lists every item, applied or rejected, with
the reason.

## 1. Problem

The owner asked: "what if things go to DLQ?" Today the answer is: nobody sees it.

A Kafka Connect **sink** with `errors.tolerance=all` and `errors.deadletterqueue.topic.name=<topic>`
writes every record it cannot convert, transform or deliver to that topic and goes on. The task stays
`RUNNING`. So:

- The watcher sees nothing. Its three signals are `connector_down`, `slot_inactive`, `slot_wal`
  (`internal/watch/watch.go:119`, `signals.go:32-46`). All of them say "fine": the connector runs, the
  slot moves, WAL does not grow.
- The pipeline map says nothing. `connectorHealth` (`internal/topology/health.go:155-186`) looks at
  states only; there is no node for the DLQ topic.
- The agent cannot look. No tool reads a DLQ topic. `connect_config` hides every `errors.*` key: it
  shows a fixed list of keys (`internal/tools/kafkatools/connect_tools.go:175-196`) and `errors.*` is
  not on it. `evals/scenarios/README.md:59` already names this gap ("`connect_config` hides
  `publication.*` and `errors.*` keys").
- Rows go missing in the warehouse one by one. `warehouse_freshness` stays green because the good rows
  still arrive. The team finds out from a reconciliation job days later, like the
  `topic-retention-data-loss` scenario.

Worse: `errors.tolerance=all` with **no** DLQ topic and `errors.log.enable=false` (the default) drops
the records with no trace at all. On a **source** connector (Debezium) a DLQ is not even possible:
Connect only supports `errors.deadletterqueue.*` for sinks, so `errors.tolerance=all` on a source means
"skip the CDC event for good". That is why `connect_update_connector_config` refuses `errors.tolerance`
(`internal/actions/connector_config.go:17-19`). Debezium adds its own silent switch,
`event.processing.failure.handling.mode=skip`.

Today the only place the word DLQ appears is the `poison-pill` runbook
(`internal/tools/runbooks/library/poison-pill.md`), which tells people to **turn on** a DLQ. So Piperoom
recommends the very setting that makes the next incident invisible to Piperoom.

One more fact that shapes the whole design: a DLQ topic holds the **full failed records**, values
included. It is the most data-dense topic in the cluster. Everything below treats it that way.

## 2. Goals / non-goals

Goals:
1. **Watcher signal `dlq_growth`**: a room opens (or the pipeline's open room is joined) when a DLQ topic of
   the pipeline gets new records. Plus **`config_risk`**: a log line (room on request) when a connector is
   set up to drop records silently.
2. **Read tool `dlq_peek`**: the agent sees how many records are in the DLQ, how fast they arrive, and the
   Connect error headers (`__connect.errors.*`). **Never a record value, never a key.** By default it shows
   the exception **class**, stage, connector, task and original position, **not the exception message**;
   messages are opt-in per pipeline and redacted (§3.2).
3. **Pipeline map**: a DLQ side node per DLQ topic with record count, newest-record age and who writes it.
4. **Evals**: three scenarios so the agent is graded on DLQ incidents, not only on the existing six.
5. **Fixes**: the agent explains the cause and proposes what it can. Two new safe config values become
   proposable (`errors.log.enable=true`, `errors.deadletterqueue.context.headers.enable=true`).
6. Consumer-side DLQs (an application consumer writes its own DLQ topic) can be declared in `owners.yaml`.
7. Works on MSK Connect once the MSK read PRs land, and has a fallback before that.

Non-goals:
- **No DLQ replay action.** Reasons in §4.4, with a sketch of the guarded design for later.
- No change to `errors.tolerance` through an action. It stays refused.
- No reading of record values or keys, anywhere, by any new code.
- No per-record "which row is missing" lookup from the DLQ (that needs the key).
- No Prometheus/CloudWatch metrics; the watcher polls Kafka, as it polls Postgres and Connect today.
- No change to the agent prompt, the room service, the store, the UI or the Slack bridge. The UI shows the
  new node kind as it shows the others (A6 may style it later).

## 3. Design

### 3.1 Detection (watcher)

#### 3.1.1 Where DLQ topics come from

Two sources, merged per pipeline, de-duplicated, in a fixed order (declared first, then discovered):

1. **Discovered from connector config.** The watcher already reads `Connect.Config` per connector when a
   slot signal is on (`observe.go:85-93`, `needCfg`). With `dlq_growth` or `config_risk` on, the
   config is read for every connector of the pipeline. A sink whose config has
   `errors.deadletterqueue.topic.name` set is a DLQ writer; the value is the DLQ topic.
2. **Declared in `owners.yaml`** (`watch.dlq_topics`, §3.1.5): consumer DLQs, and connectors whose config
   cannot be read (MSK before the config read lands, §3.6).

A topic name that is one of the pipeline's CDC topics (`Scope.Topics()`, `internal/tools/scope.go:188-197`)
is refused at load time: a DLQ that is also the CDC topic is a misconfiguration, and counting it would make
every normal change look like a bad record.

The parsing is one small pure package, **`internal/dlq`** (new, A7, no I/O, no imports beyond `strings`
and `strconv`), used by the watcher, the topology builder and `dlq_peek`, so the three never disagree:

```go
package dlq

// ErrorsConfig is what a connector config says about bad records.
type ErrorsConfig struct {
	IsSink          bool   // connector.class ends in "SinkConnector" (best effort)
	Tolerance       string // errors.tolerance: "none" (default) | "all"
	Topic           string // errors.deadletterqueue.topic.name ("" = no DLQ)
	LogEnabled      bool   // errors.log.enable (default false)
	LogMessages     bool   // errors.log.include.messages (default false)
	ContextHeaders  bool   // errors.deadletterqueue.context.headers.enable (default false)
	// DebeziumSkip is true when event.processing.failure.handling.mode is "skip"
	// (Debezium source connectors only; default "fail").
	DebeziumSkip bool
}

func Parse(cfg map[string]string) ErrorsConfig

// Risk is one config-risk finding.
type Risk struct {
	Code string // see table below
	Sev  string // sev2 | sev3 | info
	Text string // one line; names config KEYS and the DLQ topic name only, never another config value
}

// Risks returns the findings for one connector. topicExists is the watcher's
// or builder's last reading of the DLQ topic (nil = unknown).
func Risks(c ErrorsConfig, topicExists *bool) []Risk
```

| Code | When | Sev | Text (shown to people and the model) |
|---|---|---|---|
| `silent_drop_sink` | sink, `Tolerance=all`, `Topic==""`, `!LogEnabled` | sev2 | "errors.tolerance=all with no DLQ topic and errors.log.enable=false: bad records are dropped with no trace" |
| `silent_skip_source` | not sink, `Tolerance=all` | sev2 | "errors.tolerance=all on a source connector: CDC events that fail conversion are skipped for good (sources have no DLQ)" |
| `debezium_skip` | `DebeziumSkip` | sev2 | "event.processing.failure.handling.mode=skip: Debezium drops events it cannot process" |
| `dlq_topic_missing` | sink, `Topic!=""`, `topicExists==false` | sev3 | "errors.deadletterqueue.topic.name is set but the topic does not exist: Connect creates it when the task starts, so the worker could not create it (Create ACL, or the task never started); until it exists bad records fail the task instead" |
| `dlq_no_log` | sink, `Topic!=""`, `!LogEnabled` | sev3 | "DLQ is on but errors.log.enable=false: the worker log will not say why records were dead-lettered. Enable it WITHOUT errors.log.include.messages=true: that option writes record values into the worker log, which connect_worker_logs then shows" |
| `dlq_log_messages` | `LogEnabled && LogMessages` | sev3 | "errors.log.include.messages=true: failed record values are written into the worker log" |
| `dlq_no_headers` | sink, `Topic!=""`, `!ContextHeaders` | info | "DLQ records carry no error headers (errors.deadletterqueue.context.headers.enable=false): dlq_peek can show counts and positions only" |

`Risk.Text` is a fixed string per code (a `switch`, not `fmt.Sprintf` over config values). A unit test
asserts that no config value other than the keys above reaches `Text` (§6.2).

Note what is **not** a risk: `errors.tolerance=none` (the default). Then one bad record fails the task, and
`connector_down` already catches it. The two signals together cover both settings.

#### 3.1.2 Growth: offsets, not records

The watcher never reads a record. Once per tick it reads **start and end offsets** of every DLQ topic of
every target pipeline, de-duplicated, in two requests: `kadm.Client.ListStartOffsets(ctx, topics...)` and
`kadm.Client.ListEndOffsets(ctx, topics...)` (verified: both take many topics and return `ListedOffsets` =
`map[topic]map[partition]ListedOffset{Offset, Err}`; a topic that does not exist comes back as partition
`-1` with `kerr.UnknownTopicOrPartition`). This goes through a new client method `DLQOffsets` (§5.1), not
`KafkaAdmin.TopicOffsets` (`internal/clients/kafka/kafka.go:98`), because `TopicOffsets` opens a consumer
per topic to fetch the newest record's timestamp (`recordTimestamps`, `:387-395`), which pulls one record
value into memory and costs a connection per topic per tick. `watcher.md` §6 item 8 already planned an
offsets-only read for v2 for this reason.

Two rules the `go doc` text forces:
- **Empty set = no call.** `ListEndOffsets` / `ListStartOffsets` with **no topics list every topic in the
  cluster**. A pilot with no DLQs must not do a full-cluster ListOffsets every tick (and on MSK that trips
  IAM for every topic outside the policy). The watcher skips the call when the set is empty, and
  `DLQOffsets` with no topics returns `nil, nil` (§5.1 doc comment, with a unit test).
- **No topic auto-create.** kadm's `ListEndOffsets` goes through `ListTopics` → `Metadata`, which builds
  `kmsg.NewPtrMetadataRequest()`; its `Default()` sets nothing, so `AllowAutoTopicCreation` is `false`
  (checked in `kadm@v1.19.0/metadata.go:203-231,507-508` and `kmsg` `generated.go:7124`). The tick cannot
  create a missing DLQ topic on a broker with auto-create on.

Per DLQ topic the watcher keeps `end_sum = Σ end offsets` as a sample ring, the same shape as `slot_wal`
(`rate.go`, `sample{at, retained}` → a second ring `countSample{at, endSum}`; `trimSamples` and `slope`
are reused by generalising the field or by a small copy, builder's choice; the ring keeps `slowWindow`,
6 h). The reading is:

- `records` = `Σ (end − start)`: what is **in** the topic now. This is the one number both the title and
  the map print. It can go down (retention moves the start), so it is never used for growth.
- `new_in_window` = `end_sum(now) − end_sum(oldest sample within window)`, with `window` from config
  (default 1 h). End offsets only grow, so this is exact.
- **Episode** = the stretch from the first sample whose `end_sum` rose above the previous sample (after
  the baseline, or after the signal last resolved) until the signal resolves. `first_seen` is the time of
  that first rising sample. The title says "first at 09:41Z". After a resolve the next rise starts a new
  episode with a new `first_seen`.
- `rate` = `slope` over the window in records/s, shown as "~37/h". `slope` needs `minSamples=3` and
  `minSpan=30m` (`rate.go:14-15`), so the rate is **unknown** for the first 30 minutes of an episode and
  for any `window` under 30 m. When unknown, the rate part is **left out** of the title and the evidence has
  `"rate_per_hour": null`. The minimum `window` is therefore 30 m (§3.1.4).
- **Topic does not exist** (`UnknownTopicOrPartition` on the `-1` partition): reading is **clear** with
  evidence `exists:false`, **not** an error. For a **declared** topic (consumer DLQ) this is the healthy
  state: the application creates it when it first needs it. For a **sink** DLQ it is not: Kafka Connect
  creates the DLQ topic when the sink **task starts** (`DeadLetterQueueReporter.createAndSetup`, called
  from the worker's sink task setup), not on the first bad record. So "configured but missing" means the
  worker could not create it, and `dlq_topic_missing` (sev3, §3.1.1) lands in `config_risk`. `DLQOffsets`
  reports a missing topic as absent from its map, never as `ErrNotFound` (unlike `TopicOffsets`,
  `kafka.go:110-119`).
- **Baseline.** On the first tick for a topic the watcher only records the sample: it never fires for
  records that were already there, because it does not know when they arrived. Evidence carries
  `records` and `older_than_watcher: true` so a person still sees a backlog. The same after a restart
  (in-memory state is lost, as for slot_wal).

Verdicts (`state.go:40-48` vocabulary):

| `new_in_window` | verdict |
|---|---|
| `>= fire` (default 1) | `vBreach`: fires after `for` (default 5 m) |
| `0` | `vClear`: resolves after `for` with nothing new |
| between | `vHold`: neither timer runs (a `fire` above 1 does not flap) |
| offsets unreadable | `vUnknown`: timers untouched, `noteError` once (as today) |

Because the window is 1 h, the signal resolves `window + for` after the **last** bad record. That is on
purpose: a DLQ that gets one record every 20 minutes is still an incident.

Severity: sev2 by default. sev3 when `new_in_window < 10` and `rate` is unknown or not rising (a trickle).
sev1 is never set by this signal; a DLQ never loses data by itself. Severity is in the evidence like the
others.

Label (title fragment, `reading.label`): `DLQ orders-dlq +37 in 1h, ~37/h, first 09:41Z, 1,204 present`
(without the `~37/h` part while the rate is unknown). Evidence (`reading.ev`, under 4 KiB like the rest,
`output.go:18`):

```json
{"signal":"dlq_growth","severity":"sev2","topic":"orders-dlq","writers":["pg-orders-sink"],
 "declared":false,"exists":true,"partitions":3,"records":1204,"end_offset_sum":1418,
 "new_in_window":37,"window":"1h","rate_per_hour":37,"first_seen":"2026-10-13T09:41:02Z",
 "older_than_watcher":false,"shared":false,"context_headers":true,"risks":["dlq_no_log"]}
```

`writers` are the connectors (or the declared `written_by`) of this pipeline that name the topic.
`shared` is true when the same topic is named by a connector or a declaration of **another** pipeline in
`owners.yaml` (the growth may be theirs; §3.1.4, §8). No header values, no keys, nothing from a record: the
watcher never fetched one.

#### 3.1.3 Config-risk findings

`config_risk` is evaluated from the configs already in `obs.conns[].cfg` plus the `exists` reading per DLQ
topic from the same tick: `dlq.Risks(dlq.Parse(cfg), exists)` per connector, the worst `Sev` wins. It is a
signal like the others so `for`, `action` and the state machine apply unchanged, but its default `action`
is **`log`** (like `compat_none` in `watcher.md` §4): a bad config is a fact, not an incident. Reading:
`vBreach` when any sev2/sev3 risk exists, `vClear` otherwise; `info` risks never breach (they only land in
the map facts, §3.3). With the default `For: 1m` and the default 60 s interval it fires on the **second**
tick that shows the risk, which is fine for a log line. An operator who wants a room for a silently dropping
sink sets `action: room` in `owners.yaml`; the room then reads
`orders: config risk pg-orders-sink: errors.tolerance=all with no DLQ topic …` and the agent gets the
evidence as `Alert.Raw`. When another signal opens a room, the current risks are added to the evidence
as `risks` on `dlq_growth` (above) and as a `config_risks` list on the body, so the agent sees them
whatever opened the room.

Log line (shadow metric, same fields as `fields()` at `signals.go:95-109`): `signal=config_risk
value="pg-orders-sink: silent_drop_sink" threshold="errors.tolerance=none or a DLQ with logging"`. The
line names the connector, the risk code and, in `dlq_growth` lines, the DLQ topic name. No other config
value is ever logged.

#### 3.1.4 Thresholds, config, rate limits

`owners.yaml` (`watch:` block, `internal/owners/watch.go`):

```yaml
pipelines:
  - id: orders
    # ...existing fields...
    watch:
      signals:
        dlq_growth:  {for: 5m, fire: 1, window: 1h}      # every key optional; action defaults to room
        config_risk: {action: log}                        # or room
      dlq_topics:                                         # optional: DLQs the config cannot tell us about
        - topic: order-sync-dlq
          written_by: order-sync                          # a consumer group or connector of this pipeline
          shared: false                                   # true when another pipeline declares the same topic
      dlq_peek:
        headers: classes                                  # counts_only | classes (default) | messages
```

Exact Go changes in `internal/owners/watch.go` (A3, D1), because today's `signalSpec` (`watch.go:74-81`)
has no `window` and `applySlotWAL` (`:225`) parses `fire` with `ParseSize` (so `fire: 1k` would pass):

```go
// signalSpec gains one field.
type signalSpec struct {
	Enabled *bool  `yaml:"enabled"`
	Action  string `yaml:"action"`
	For     string `yaml:"for"`
	Fire    string `yaml:"fire"`
	Clear   string `yaml:"clear"`
	EtaSev1 string `yaml:"eta_sev1"`
	Window  string `yaml:"window"` // dlq_growth only
}

// DLQSignal is the dlq_growth signal.
type DLQSignal struct {
	WatchSignal
	// FireRecords: this many new records in Window starts the timer. >= 1.
	FireRecords int
	// Window: how far back "new" looks. >= 30m (rate.go minSpan).
	Window time.Duration
}

// DLQTopicSpec is one watch.dlq_topics[] entry (raw, validated in resolveWatch).
type DLQTopicSpec struct {
	Topic     string `yaml:"topic"`
	WrittenBy string `yaml:"written_by"`
	Shared    bool   `yaml:"shared"`
}

// DLQPeekSpec is the watch.dlq_peek block.
type DLQPeekSpec struct {
	Headers string `yaml:"headers"` // counts_only | classes | messages
}

// WatchSpec gains:   DLQTopics []DLQTopicSpec `yaml:"dlq_topics"`; DLQPeek DLQPeekSpec `yaml:"dlq_peek"`
// WatchConfig gains: DLQGrowth DLQSignal; ConfigRisk WatchSignal; DLQTopics []DLQTopic; DLQPeekHeaders string

// DLQTopic is a validated declaration.
type DLQTopic struct {
	Topic     string
	WrittenBy string
	Shared    bool
}

// applyDLQ parses fire as a count, not a size.
func applyDLQ(dst *DLQSignal, s signalSpec) error {
	if err := applySignal(&dst.WatchSignal, s); err != nil {
		return err
	}
	if s.Fire != "" {
		n, err := strconv.Atoi(s.Fire)
		if err != nil || n < 1 {
			return fmt.Errorf("fire %q: must be a whole number of records, at least 1", s.Fire)
		}
		dst.FireRecords = n
	}
	if s.Window != "" {
		d, err := ParseDuration(s.Window)
		if err != nil {
			return fmt.Errorf("window: %w", err)
		}
		if d < 30*time.Minute {
			return fmt.Errorf("window %q: must be at least 30m (a rate needs 30m of samples)", s.Window)
		}
		dst.Window = d
	}
	return nil
}
```

In `resolveWatch` (`:142-203`): the `switch name` at `:172-176` adds `SignalDLQGrowth, SignalConfigRisk`
as known (the `default` error text lists five); `allowed` (`:179-182`) gets `fire, window` when
`name == SignalDLQGrowth` (nothing extra for `config_risk`); the apply `switch` (`:190-197`) adds
`applyDLQ(&w.DLQGrowth, s)` and `applySignal(&w.ConfigRisk, s)`. `v2Signals` (`:30`) is unchanged.

Defaults in `DefaultWatch()` (`:118-130`): `DLQGrowth{Enabled, ActionRoom, For: 5m, FireRecords: 1,
Window: 1h}`; `ConfigRisk{Enabled, ActionLog, For: 1m}`; `DLQPeekHeaders: "classes"`.

Validation of `dlq_topics` (same `checkKeys` pattern, keys `topic, written_by, shared`):
`topic` must match Kafka's topic rule `^[A-Za-z0-9._-]{1,249}$` and not equal any `<topic_prefix>.<source_table>`
of **any** pipeline; `written_by` must be in `consumer_groups` or `connectors` of the same pipeline;
duplicates inside one pipeline refused. **Across pipelines**: a topic declared by two pipelines is refused
at load unless **both** declarations set `shared: true` ("`order-sync-dlq` is declared by `orders` and
`payments`: set `shared: true` on both or give each its own DLQ"). This makes a shared DLQ a written-down
decision, not an accident (§4.3). `dlq_peek.headers` must be one of the three words. A warning in the
watcher log when `window < 2 × interval` (fewer than two samples per window gives no growth reading at all).

Env: **none new**. Interval, timeout, cooldown, mode and `max_open_watcher_rooms` are the existing
`PIPEROOM_WATCH_*` values (`internal/config/config.go:78-81`). So `internal/config/config.go` does not
change (§5.2).

Rate limits and cost per tick:
- Kafka: **two** requests (`ListStartOffsets`, `ListEndOffsets`) for all DLQ topics of all target
  pipelines (deduplicated), **zero** when the set is empty, under `check_timeout` through the existing
  `call` helper (`observe.go:138`). Zero record fetches. A pilot with 10 pipelines and 10 DLQs is one
  metadata + two ListOffsets round trips per minute.
- Connect: `Config` is now read for every connector of a pipeline with `dlq_growth` or `config_risk` on.
  Today it is read only with a slot signal on; in practice every pipeline has one, so this is usually
  zero extra calls. It is one GET per connector per tick at most.
- Postgres: untouched.
- Rooms: the same storm cap (`stormThreshold = 3`, `watch.go:115`: three or more pipelines on one signal in
  one tick = one room), the same `max_open_watcher_rooms`, the same cooldown. Note the edge: a DLQ topic
  shared by exactly **two** pipelines that grows trips `dlq_growth` on both in the same tick and opens **two**
  rooms (the cap starts at three). That is accepted: both pipelines' owners should know, and the
  `shared:true` evidence tells each room about the other. Three or more fold into one room.

`Deps` (`watch.go:84-103`) gains `Kafka clients.DLQReader` (§5.1). nil = `dlq_growth` stays "unknown" with
one warning naming the missing client, like `errNoConnect` (`observe.go:16`); `config_risk` needs only
Connect and still works (with `exists` unknown, so `dlq_topic_missing` never fires). `targets()`
(`observe.go:48-66`) counts a pipeline as a target when any of the five signals is on.

#### 3.1.5 Consumer DLQs in `owners.yaml`

See the YAML above. A declared topic is watched exactly like a discovered one (same offsets call, same
signal). Its node in the map sits on the `consumer` layer when `written_by` is a consumer group and on
`kafka-connect` when it is a connector (§3.3). Headers: application DLQs carry whatever the application
put there; `dlq_peek` only ever shows `__connect.errors.*` keys (§3.2), so for a consumer DLQ it shows
counts, positions and timestamps, and says so. A per-topic header allow-list is a later addition (§8).

### 3.2 `dlq_peek` tool

Package `kafkatools` (A2), `tools.KindRead`, registered in `Register` (`kafkatools.go:40-55`) **always**,
like every other tool there. In `Run` it type-asserts `Deps.Kafka.(clients.DLQReader)` and returns the
error "Kafka client cannot read DLQ topics" when the assertion fails. This is the same shape as
`topicConfigAdmin` (`kafka_tools.go:276-278`), and it matters for the evals: `evals/replay.go:48` builds
`LiveTools` with `kafkatools.Register(reg, kafkatools.Deps{})`, so a tool registered only behind a type
assertion would vanish from the model's tool list and from `-check`. Registering always needs **no** eval
harness change.

**Inputs** (`objectSchema`, `additionalProperties:false`):

| field | type | required | meaning |
|---|---|---|---|
| `connector` | string | one of the two | a connector of this pipeline; the tool reads its `errors.deadletterqueue.topic.name` |
| `topic` | string | one of the two | a DLQ topic declared in `owners.yaml` (`watch.dlq_topics`) for this pipeline |
| `limit` | integer | no | newest records to show **in total across partitions**, default 5, max 20 |

Exactly one of `connector`/`topic` must be set; both or neither is `ErrBadInput`.

**Scope, before any client call** (F11 rule, `scope.go:13-22`):
1. `sc, err := d.scope(ctx)` as every tool.
2. With `connector`: `sc.CheckConnector(name)` (`scope.go:170-175`). Then `Connect.Config(name)`;
   `dlq.Parse(cfg).Topic == ""` → summary "pg-orders-sink has no DLQ topic (errors.deadletterqueue.topic.name
   is not set)" and the risks for that connector, no Kafka call. The topic name comes from the customer's
   own config of the customer's own connector in this room's pipeline, so it is in scope by construction.
3. With `topic`: `sc.CheckDLQTopic(topic)` (new, A3: true when the topic is in `Pipeline.Watch().DLQTopics`;
   unscoped passes). An undiscovered, undeclared topic is refused with the usual `ErrOutOfScope` text listing
   the declared DLQ topics. **A connector-discovered topic cannot be passed as `topic`**: that would let the
   model name any topic and hope it matches; it must name the connector instead.
4. `kafka_topic_offsets`, `kafka_find_by_key`, `kafka_topic_config` keep refusing DLQ topics (`CheckTopic`
   unchanged). `dlq_peek` is the only road to a DLQ.

**What it does:** `DLQOffsets(topic)` for counts, then `PeekDLQ(topic, limit)` (§5.1). The client:
- Orders partitions by **end offset, newest first** (the partition with the highest end offset most likely
  holds the newest record; timestamps are compared after the fetch), and reads from the end of each with
  `kgo.ConsumePartitions` (verified; the same mechanism `FindByKey` and `recordTimestamps` use,
  `kafka.go:324,395`) and `kgo.NewOffset().AtEnd().Relative(-n)`.
- Stops as soon as it holds `limit` records **in total**, so a 64-partition DLQ with `limit: 20` reads at
  most 20 records, not 20 × 64. (`limit` is a total, not a per-partition number; this is the fix for the
  rev 1 bound.)
- Caps bytes: `kgo.FetchMaxBytes(4<<20)` and `kgo.FetchMaxPartitionBytes(1<<20)` (both verified) per fetch,
  and a running total of **8 MiB** of record bytes across the whole call, after which it stops early and
  the summary says "stopped at 8 MiB: records are large". KIP-74 lets a broker return one record larger than
  the max, so the running total is what actually bounds memory. Under the kafkatools 10 s timeout
  (`kafkatools.go:30`).
- Builds `clients.DLQRecord` from `kgo.Record.Headers` (`[]RecordHeader{Key, Value []byte}`, verified) and
  **drops `Key` and `Value` at the client boundary**: the DTO has no field for them, so nothing above the
  client can ever see them. Records are merged, sorted newest first by timestamp, cut to `limit`.

Calls per investigation are bounded by the run's token budget (`agent.Run.TokenBudget`,
`internal/agent/contract.go:93-98`, PR #74) and the step loop, as for every other tool; `dlq_peek` adds no
limit of its own beyond `limit ≤ 20` and 8 MiB per call.

**Header parsing and redaction** (in the client, so every reader gets the same rule):
- Keep a header only when its key starts with `__connect.errors.`. Everything else (application headers,
  tracing headers, anything a producer added) is dropped.
- Known keys (KIP-298, **unverified against a live cluster**, the builder checks them on the dev stack):
  `topic`, `partition`, `offset`, `connector.name`, `task.id`, `stage`, `class.name`,
  `exception.class.name`, `exception.message`, `exception.stacktrace`.
- Values are UTF-8 text in Connect; decode as string, remove control characters, cap at **300 runes**
  (`watch.clean` rule, `state.go:179`). `exception.stacktrace` is reduced to its **first line** (`firstLine`,
  `state.go:168`).
- **Redaction of `exception.message` and the stack-trace first line.** These two carry record bytes in
  practice, not only "a quoted field": Jackson's `JsonParseException` (JsonConverter) appends
  `… at [Source: (byte[])"{"email":"a@b.com",…}"; line: 1, column: 2]`; the Postgres JDBC driver appends
  `Detail: Key (id)=(123) already exists`; batch failures embed the SQL with values. A 300-rune cap keeps
  exactly the part where the snippet sits. So after the cap the client runs `dlq.RedactMessage` (in
  `internal/dlq`, pure, tested): drop everything from `[Source:` to the end; drop `Detail:` and `Where:`
  tails; replace every balanced `{…}` and `[…]` and every double-quoted string with `<redacted>`. The
  message above becomes `Unexpected character ('x' (code 120)): was expecting double-quote to start field
  name at <redacted>`. The same pass runs on the stack-trace first line. The order is cap → redact, and the
  test in §6.2 checks that the junk value never survives in plain, base64 or hex form.
- When a record has no `__connect.errors.*` header at all, `Headers` is empty. The tool then prints one
  line: "no Connect error headers on these records; for a sink set
  errors.deadletterqueue.context.headers.enable=true (a proposable change)".

**Header modes** (`watch.dlq_peek.headers` in `owners.yaml`, §3.1.4; the tool applies the mode on the DTO
it got from the client, the client always returns the redacted full set):

| mode | the summary shows per record | notes |
|---|---|---|
| `counts_only` | partition, offset, timestamp, and the value-free reject-rule name (`x-piperoom-reject-rule`) when a record carries one | for pilots that want no free text at all |
| `classes` (**default**) | plus `exception.class.name`, `stage`, `class.name`, `connector.name`, `task.id`, original `topic/partition/offset` | fixed-vocabulary values and positions; no message |
| `messages` | plus `exception.message` and the stack-trace first line, redacted and capped | opt-in; the summary carries a line "messages: on (owners.yaml); redacted" |

Exception **class counts** (`DataException ×5`) are shown in every mode: a class name is a Java identifier,
not data.

**Cross-pipeline filter.** When headers are present, a record whose `connector.name` is not one of this
pipeline's connectors is **dropped** and counted as `other_writers`. A DLQ topic shared by several sinks
(one `dlq` topic for the whole Connect cluster is common) would otherwise show another pipeline's errors.
Without headers the tool cannot filter; it says "records of other connectors may be included". **This
filter is for noise, not for security** (§4.3): the header is set by whoever wrote the record, and anyone
with write ACL on a shared DLQ topic can set it. A shared DLQ topic is at the same trust level as the
pipeline's own topics, and its text is wrapped as untrusted like every tool summary.

**Summary** (what the model reads, under `MaxSummaryBytes`), default `classes` mode:

```
orders-dlq (DLQ of pg-orders-sink): 1,204 records in 3 partitions, 37 new in the last hour; newest 5:
  p1@412 2026-10-13T09:58:40Z DataException stage=VALUE_CONVERTER from pg.public.orders/1@25377154 task 0
  p0@389 2026-10-13T09:58:12Z DataException stage=VALUE_CONVERTER from pg.public.orders/0@25377102 task 0
  …
exception classes in the sample: DataException ×5
headers: classes (set watch.dlq_peek.headers: messages in owners.yaml to see redacted exception messages)
risks: dlq_no_log (errors.log.enable=false)
```

`37 new in the last hour` comes from the watcher's ring when the tool and the watcher share a process
(`cmd/server`): the tool asks `Deps.DLQStats` (optional func, nil in `cmd/investigate` and evals) and
otherwise prints `last hour: unknown (no watcher in this process)`.

**Data** (`Result.Data`). This is **permanent and visible to every room member**:
`AgentToolResultPayload.Data` (`internal/events/events.go:152-158`) is written to `room_events` and shown
to everyone in the room (`docs/pilot/data-handling.md` §1, lines 50-53). So `Data` holds **counts and
positions only**: `{topic, writer, declared, partitions, records, start_offsets, end_offsets,
newest_timestamp, exception_class_counts, other_writers, context_headers, header_mode, risks}`. No
per-record list, no message text, in any mode. The per-record lines live only in the summary (also stored,
also per member, but already redacted and mode-limited, and under `MaxSummaryBytes`).

**Description** (the model reads it): "Peeks a dead-letter topic: record count, arrival rate and, for the
newest records, the Kafka Connect error headers (exception class, stage, original topic/partition/offset,
connector and task; exception messages only when the pipeline opts in). Never record values or keys. Give
`connector` for a sink's DLQ or `topic` for a DLQ declared in owners.yaml."

**`connect_config` change (A2, same PR):** add `errors.tolerance`, `errors.deadletterqueue.topic.name`,
`errors.log.enable`, `errors.log.include.messages`, `errors.deadletterqueue.context.headers.enable`,
`errors.retry.timeout`, `event.processing.failure.handling.mode` to the `important` allow-list
(`connect_tools.go:175-181`). They are switches and a topic name, never credentials. Without this the
agent can see the DLQ but not why it exists.

### 3.3 Topology node

`internal/topology/graph.go` (contract) gains `KindDLQTopic NodeKind = "dlq_topic"` (§5.3). The builder
(A3) adds, after the topic nodes and before the groups (so flow order in `Render` stays table → connector
→ topic → group → sink, with DLQs between topics and groups):

- One node per DLQ topic of the pipeline: ID `dlq:<topic>`, `Kind: KindDLQTopic`, `Name: <topic>`,
  `Layer`/`Owner` = the writer's layer (`kafka-connect` for a connector writer, `consumer` for a consumer
  group writer; a topic with both kinds of writer takes `kafka-connect`).
- Edges: `connect:<sink>` → `dlq:<topic>` for each connector writer, `group:<g>` → `dlq:<topic>` for each
  declared consumer writer. Side edges only: nothing flows out of a DLQ node.
- Discovery: `dlq.Parse(snap.config).Topic` for every connector in `info.connect` (config is already
  collected, `health.go:73-85`), plus `p.Watch().DLQTopics`. A connector whose config failed contributes no
  DLQ node (there is no placeholder, unlike topics: a missing side node is not a missing graph shape).
- Health and facts come from **`KafkaAdmin.TopicOffsets` alone**, which is already on the contract
  interface and already returns start, end and the newest record's timestamp per partition
  (`clients.PartitionOffsets{Partition, Start, End, LastTimestamp}`, `kafka.go:98-141`) and maps a missing
  topic to `clients.ErrNotFound` (`:110-119`). The DLQ topic goes into `collectTopics`
  (`health.go:113-137`) like a CDC topic; the one record `recordTimestamps` fetches per partition stays in
  the client exactly as it does for CDC topics today. The builder needs **no** `DLQReader` and no type
  assertion, so D5 depends on D0 only for `KindDLQTopic`.

| state | health | facts |
|---|---|---|
| topic does not exist (`ErrNotFound`) | `ok` (declared) / `warn` (sink-configured: `dlq_topic_missing`) | `records=0 exists=false writer=<w>` |
| exists, newest record older than `DLQRecentWarn` (new threshold, default 1 h) | `ok` | `records=<Σ end−start> newest_record=<age> ago writer=<w>` |
| newest record younger than `DLQRecentWarn` | `warn` | same, plus `recent=true` |
| Kafka unreachable | `unknown` | `error=unreachable` |

`records` here and `records` in the watcher evidence are the same number (`Σ end−start`), so the map and
the title never disagree.

`Thresholds` (`builder.go:24-38`) gains `DLQRecentWarn time.Duration`; `ThresholdsFor` (`:54-57`) maps
`watch.dlq_growth.window` onto it, the same "one owners → topology mapping" rule as `slot_wal.fire`, so the
map warns exactly when the watcher would count the record as new.

- Config risks become facts on the **connector** node (`connectorHealth` is untouched; `connectorNode`,
  `builder.go:250-271`, adds them after it): `errors_tolerance=all`, `dlq=orders-dlq` (or `dlq=none`),
  `config_risk=silent_drop_sink`. A sev2 risk turns an `ok` connector to `warn`. The agent reads the map in
  its first prompt (`internal/agent/prompt.go`), so it sees "this sink drops silently" before any tool call.
  The facts carry the DLQ topic **name** and risk **codes** only, never another config value (same unit
  test as `Risk.Text`, §6.2).

Rendered (`render.go`), for the eval `pipeline_map`. Since the topology edge-kinds contract the render is the
canonical dependency listing (`graph.go` `Builder.Render`): nodes in data-flow order, each with its `from:` (data
inputs) and `depends on:` lines, `(unconfirmed)` on links owners.yaml implies but nothing proves. Here the sink reads
the topic by its `topics` setting and the group by its committed offsets; neither link to the warehouse table is
provable:

```
pipeline orders
legend: listed upstream first. "from:" = data inputs. "depends on:" = needed to work, not a data stage. (unconfirmed) = not proven; implied by owners.yaml or a naming convention; verify it with tools. First failing component and root cause can differ; check upstream dependencies of the first failing component.
1 pg:public.orders [postgres, owner priya] OK slot_active=true slot_retained=1.1 MiB
2 connect:pg-orders-connector [kafka-connect, owner arjun] OK
  from: pg:public.orders
3 topic:pg.public.orders [kafka, owner arjun] OK newest_record=12s ago
  from: connect:pg-orders-connector
4 connect:pg-orders-sink [kafka-connect, owner arjun] OK config_risk=dlq_no_log dlq=orders-dlq errors_tolerance=all
  from: topic:pg.public.orders
5 dlq:orders-dlq [kafka-connect, owner arjun] WARN newest_record=3m ago recent=true records=1204 writer=pg-orders-sink
  from: connect:pg-orders-sink
6 group:order-sync [consumer, owner kabir] OK lag=0 state=Stable
  from: topic:pg.public.orders
7 wh:analytics.orders [warehouse, owner meera] OK stale_for=1m ts_column=updated_at
  from: connect:pg-orders-sink (unconfirmed), group:order-sync (unconfirmed)
```

`trace_record` (`trace.go`) is untouched in this scope: it cannot say "this row is in the DLQ" without
reading keys (§8).

### 3.4 Room and alert text; dedup with the existing watcher rooms

Nothing new in the room service. `dlq_growth` and `config_risk` are two more entries in `signalIDs`
(`watch.go:119`, appended at the end so existing titles keep their order), so everything in `output.go`
applies as is:

- **One open watcher room per pipeline** stays true: the dedup key is `DedupKey(pipeline)` = `watcher:orders`
  (`watch.go:123`). A DLQ that starts growing while `connector_down` already has a room **joins** that room
  with "new signal" (`updateRoom`, `output.go:297-318`). A DLQ growth with no open room opens one, and a
  later `connector_down` joins it.
- **Join-on-open-room** (`OpenRoomForPipeline`, `output.go:351-363`): an Alertmanager or human room for the pipeline
  takes the evidence instead of a second room.
- Title: `orders: DLQ orders-dlq +37 in 1h, ~37/h, first 09:41Z, 1,204 present 5m` (the `5m` is the
  `withDur` suffix from `compose`, `output.go:590-600`). With both: `orders: connector pg-orders-sink task 0 FAILED
  4m; DLQ orders-dlq +37 in 1h …`.
- Evidence JSON = `Alert.Raw` = what the agent gets at start (`watcher.md` §3 Agent visibility). The DLQ
  evidence (§3.1.2) and the `config_risks` list sit in `signals[]` / the body.
- "Still firing" every 15 min, resolved alert when **all** signals clear, cooldown after close, `acked`
  after `OnRoomClosed`: unchanged. A DLQ that keeps trickling after the driver closes the room stays
  `acked` until it is quiet for `window + for`; then the next record opens a fresh room. This is the
  wanted behaviour: the driver said "I know", and the watcher only speaks again for a new episode.
  (`reviveSev1` reopens only for sev1, which this signal never sets.)
- Slack: the title is mirrored, evidence is not (`internal/slackbridge/render.go`). The title has counts
  and a topic name, no header text.
- `config_risk` with `action: log` never touches a room. With `action: room` it is a sev2/sev3 room titled
  `orders: config risk pg-orders-sink silent_drop_sink` that the same dedup rules fold into any open room.

### 3.5 Consumer DLQs

Covered by `watch.dlq_topics` (§3.1.5). What changes for a declared consumer DLQ, compared to a sink DLQ:

| | sink DLQ (discovered) | consumer DLQ (declared) |
|---|---|---|
| found by | connector config | `owners.yaml` |
| watcher signal | `dlq_growth` | `dlq_growth`, same |
| topic missing | `dlq_topic_missing` risk (worker could not create it) | healthy (`exists:false`) |
| map node layer/owner | kafka-connect / its owner | consumer / its owner |
| `dlq_peek` input | `connector: <sink>` | `topic: <declared>` |
| headers shown | `__connect.errors.*` per the header mode, when the sink has context headers on | usually none; counts, positions, timestamps |
| cross-pipeline filter | by `connector.name` header (noise filter only) | not possible; the tool says so |
| config risks | from the sink's `errors.*` | none (the app's choices are not visible) |

Why not auto-discover consumer DLQs (e.g. every topic named `*-dlq`, `*.DLT`)? Because a name pattern
is a guess, and a guess puts another team's topic into this pipeline's scope. The declaration is one
line, validated at start, and ties the topic to a `written_by` that is already in the pipeline.

### 3.6 MSK Connect

`docs/design/msk.md` rev 4 §3a maps `ConnectClient.Config(name)` to `DescribeConnector.ConnectorConfiguration`
(PR 2c). So on MSK, once PR 2c is merged, DLQ discovery and `config_risk` work from the same config map with
no MSK-specific code. (The task text says "no Connect REST config read on MSK in PR 2": that is true for
REST, and the MSK design reads the config through the AWS API instead. If the lead later keeps config reads
off on MSK, the fallback below is the whole story.)

Before PR 2c, or with config reads off, `Config` returns an error, so no DLQ is discovered. Then the
operator declares the sink's DLQ in `owners.yaml`:

```yaml
dlq_topics:
  - topic: orders-dlq
    written_by: pg-orders-sink      # a connector: node on kafka-connect, peek by topic
```

and `dlq_peek` is called with `topic:`. The `config_risk` signal stays `unknown` for that connector (one
warning line, as for any unreadable config), never a false "all clear".

Kafka side on MSK (`msk.md` §3c, IAM SASL): `DLQOffsets` needs `kafka-cluster:DescribeTopic` on the DLQ
topic and `PeekDLQ` needs `kafka-cluster:ReadData` on it. The pilot IAM policy in `msk.md` §4 scopes
topics to `topic/CLUSTER/*/pg.*`; DLQ topics are rarely under the CDC prefix, so the policy gets one more
resource line per DLQ topic (C14, `docs/pilot/permissions.md`, §4.6). Without it, `DLQOffsets` fails for
that topic → `dlq_growth` unknown with the auth error in the log, which is the honest state.

`connect_worker_logs` (`msk.md` §3e) is the companion: with `errors.log.enable=true` the worker log holds
one line per dead-lettered record, and that tool already exists on MSK. With
`errors.log.include.messages=true` it also holds the record values, which is why `dlq_no_log` warns
against that option and `dlq_log_messages` flags it.

## 4. Safety, security, privacy

### 4.1 Read-only

Every new call is a read: `ListStartOffsets`/`ListEndOffsets` (metadata), a bounded fetch (`PeekDLQ`),
`Connect.Config` (GET). No new write method anywhere. The watcher still never fixes anything and never
calls a model.

### 4.2 Data handling (F3, `docs/pilot/data-handling.md` §2)

New row for the table in `data-handling.md` (C14):

| Tool | Exposes | Does **not** expose |
|---|---|---|
| `dlq_peek` | DLQ topic name, partition, offset and timestamp of the newest records; exception **class** counts; per record, by `watch.dlq_peek.headers` mode: `counts_only` only the value-free reject-rule name (`x-piperoom-reject-rule`, e.g. `status_enum`; redacted, shape-checked, shown as `?` otherwise); `classes` (default) the `__connect.errors.*` exception class, stage, connector name, task id and original topic/partition/offset; `messages` (opt-in) also the exception message and stack-trace first line, capped at 300 runes and **redacted** (`[Source:` tails, `Detail:`/`Where:` tails, braces, brackets and quoted strings removed). **Summary and `Data` are stored permanently in `room_events` and shown to every room member.** `Data` holds counts and positions only, never per-record text | record **values**, record **keys**, any other header, exception messages in the default mode |

How "never values" is enforced, not promised:
1. `clients.DLQRecord` (§5.1) has no `Key` and no `Value` field. The kafka client fills it from
   `kgo.Record` and the record is garbage after that. The tool, the room, the export and the model only
   ever see the DTO.
2. The header allow-list in the client is the `__connect.errors.` prefix plus the one exact key
   `x-piperoom-reject-rule` (value-free by contract, redacted and shape-checked by the tool), so an
   application header named `password` on a DLQ record is never returned.
3. Redaction runs in the client (`dlq.RedactMessage`), so no reader can forget it.
4. The default mode shows no free text at all; messages are a per-pipeline opt-in in `owners.yaml`.
5. Caps: 300 runes per header value, first line of the stack trace, `limit ≤ 20` in total, 8 MiB per call.
6. `Data` has no per-record list in any mode.
7. The watcher reads offsets only. It is impossible for `dlq_growth` evidence to hold record content because
   the watcher has no code path that fetches a record.
8. Logs: the watcher logs `topic`, counts and rates; `PeekDLQ` logs nothing per record.

Honest limits:
- The fetch brings record bytes (values included) into the Piperoom process for a moment. There is no
  Kafka API that returns headers without values. The bytes are not stored, logged, or passed on.
- In `messages` mode, a converter or driver message can still carry a fragment the redaction patterns do
  not match (a value with no braces, brackets or quotes, outside a `[Source:`/`Detail:` tail). It is the
  same class of text as a FAILED task trace that `connect_status` shows today (and that `condenseTrace`
  keeps). The cap and the untrusted wrapper (`internal/agent/run.go`, `<tool_output untrusted="true">`)
  apply; `data-handling.md` says so, and the mode is opt-in for that reason.

### 4.3 Scope and trust (F11)

`dlq_peek` checks `CheckConnector` or `CheckDLQTopic` before any client call (§3.2). The watcher takes
topics only from `owners.Pipeline` (declared) or from the config of a connector that passed
`FilterConnectors` (`observe.go:63`). A DLQ topic name coming from a connector config is customer data
about the customer's own connector; it is not a free name from the model.

**Shared DLQ topics are a trust decision, written down.** The `connector.name` header is set by the
producer of the record. Anyone with write ACL on a shared DLQ topic can write a record with
`__connect.errors.connector.name=pg-orders-sink` and any text in the other headers, and this pipeline's
agent will read that text (redacted, capped, wrapped untrusted). So:
- The per-record filter in `dlq_peek` is a **noise** filter. It is not a security boundary and the doc
  never calls it one.
- A shared DLQ topic has the **same trust level as the pipeline's own CDC topics**: whoever can write to
  it can put text in front of the agent, exactly as whoever can write to `pg.public.orders` can today.
- `owners.yaml` refuses a declared topic that another pipeline also declares unless **both** set
  `shared: true` (§3.1.4). For discovered (config) topics the watcher cannot refuse (the config is the
  customer's), so it marks `shared: true` in the evidence and logs one warning line per start: "DLQ topic
  `dlq` is written by connectors of pipelines orders, payments".

### 4.4 Why no DLQ replay action now, and what a guarded one would look like

Not in scope, because every replay design breaks a rule we hold today:
1. **It writes to customer Kafka.** Every action so far restarts, resumes, or changes a config key with an
   exact allow-list. A replay produces records. That needs produce ACLs Piperoom does not ask for
   (`permissions.md`) and a producer code path that does not exist.
2. **It needs the record values.** Replay = read the DLQ record (value included) and write it to the
   original topic or to the sink. The agent-facing path we just built refuses values on purpose. A replay
   would have to run in a separate executor path that never shows the agent what it copies, and that path
   does not exist yet.
3. **It can duplicate or reorder.** A DLQ record was dead-lettered after later records were delivered.
   Replaying it changes order; replaying a record the sink later handled (after a fix and a restart with
   `errors.tolerance=none`) duplicates it. Deciding this needs the fix to be in place first, verified.
4. **The cause is usually not fixed by replay.** A bad schema, a converter mismatch, or a sink constraint
   will dead-letter the record again. The right order is: fix the cause (schema, converter, sink), then
   replay. Piperoom's `verify` flow (`internal/agent/verify.go`) can check the fix; replay is the step after.

Sketch for later (not to build now): action `dlq_replay` with `Layer: kafka-connect` **and** a second
required approval from the `consumer` owner (two layers in `required_layers`, which the proposal format
already supports, `docs/api.md` Proposals); params `{dlq_topic, partition, from_offset, to_offset,
target: original|<topic>}` with `to_offset − from_offset ≤ 1000`; `Validate` refuses when the DLQ's writer
is not RUNNING with `errors.tolerance=none` (so the cause is at least no longer swallowed); `Execute` copies
records byte for byte with a separate producer client (own credentials, `PIPEROOM_KAFKA_PRODUCER_*`), never
decodes them, never returns their content, and writes one `action.executed` with offsets copied; a
**dry run** step first that only counts. Needs: produce ACL in `permissions.md`, a producer in
`clients.KafkaAdmin` (or a new `KafkaProducer` interface), an executor test with a fake broker, a scenario
with `forbidden_actions` for the unfixed-cause case. Decide after the first pilot asks for it.

### 4.5 What can go wrong

| Risk | What we do |
|---|---|
| A shared DLQ topic grows because of another pipeline | `shared:true` in evidence; declared sharing must be explicit on both sides; per-record noise filter in `dlq_peek`; storm cap folds three or more pipelines into one room (two open two) |
| Someone plants records with this pipeline's `connector.name` on a shared DLQ | the text is redacted, capped, wrapped untrusted; the doc says a shared DLQ is the pipeline's trust level; the fix is a DLQ per sink, which `dlq_peek` recommends when `other_writers > 0` |
| A huge DLQ (millions of records) | the watcher reads offsets only; `PeekDLQ` reads at most `limit` records from the ends, newest partition first, 8 MiB cap, never scans |
| Large records (one 5 MiB record) | KIP-74 returns it anyway; the 8 MiB running total stops the call after it; the value is dropped at the DTO |
| Kafka ACL denies `Read` on the DLQ | `dlq_peek` returns the broker error; `dlq_growth` keeps working on `Describe` alone |
| A pilot with no DLQs | the watcher makes **no** offsets call (empty set = skip), so no full-cluster listing |
| The model passes a topic it should not see | `CheckDLQTopic` refuses; only declared topics pass; discovered ones go through `connector` |
| Header values hold data | default mode shows no messages; opt-in messages are redacted and capped; no key, no value; `Data` never has per-record text |
| `errors.*` keys now visible in `connect_config` | they are switches and a topic name; `sensitiveKey` (`connect_tools.go:245`) still redacts anything that looks like a secret |
| Compromise of the Piperoom Kafka principal | with `Read` on DLQ topics it now exposes whole failed records; §4.6 says so and recommends a second principal |
| Rollback | remove the `signals` entries or set `enabled:false`; `PIPEROOM_WATCH_MODE=log` keeps every room closed; the tool is read-only |

### 4.6 ACL blast radius (`docs/pilot/permissions.md`, C14)

Today `permissions.md:158` says: "Do not grant `Read` on topics outside the pipeline: record values may
contain customer data". DLQ topics are outside the prefix and hold **full raw failed records**: they are
the most data-dense topic in the cluster. The new text, in the Kafka ACL table and the paragraph under it:

| Resource | Operation | Used by |
|---|---|---|
| Topic `<dlq topic>` (literal, one per DLQ in `owners.yaml` or in a sink's `errors.deadletterqueue.topic.name`) | `Describe` | `dlq_growth` (watcher), the map's `dlq:` node, `dlq_peek` counts |
| Topic `<dlq topic>` | `Read` **only if you want `dlq_peek` to show error headers** | `dlq_peek` records |

"A DLQ topic holds the complete records the sink could not deliver, values included. Granting `Read` on
it means that a compromise of the Piperoom Kafka principal exposes those whole records, not only the
headers Piperoom shows. Piperoom never stores or shows the values (data-handling.md, `dlq_peek` row), but
the principal can read them. Recommended: a **separate principal** for `dlq_peek` (`PIPEROOM_KAFKA_DLQ_*`
is not built yet; until it is, grant `Read` on DLQ topics only on pilots that want `dlq_peek` records,
and start with `Describe` only, which gives counts, growth and the map node)." The MSK IAM form (§3.6):
`kafka-cluster:DescribeTopic` on `topic/CLUSTER/*/<dlq>` for counts, `kafka-cluster:ReadData` only for
records.

## 5. Contract changes for the lead (PR D0)

Only three contract files and one AGENTS.md row change. Everything is additive: no existing field,
method or signature changes, so `go build ./... && go test ./...` stay green with no other edit. The
`DLQReader` interface is **optional** (type-asserted in `Run` and in the watcher), so no fake, replay
client or test implementing `clients.KafkaAdmin` (`internal/tools/kafkatools/kafkatools_test.go`,
`internal/topology/integration_test.go`, `evals/replay.go`) has to change. That follows the
`ConnectCapable` precedent in `msk.md` §5.1 and the local-adapter pattern already in the tree
(`topicConfigAdmin`, `connectorConfigUpdater`). No new DTO for offsets: `clients.PartitionOffsets` is reused.

### 5.1 `internal/clients/clients.go`

```diff
 type RecordRef struct {
 	Topic     string    `json:"topic"`
 	Partition int32     `json:"partition"`
 	Offset    int64     `json:"offset"`
 	Timestamp time.Time `json:"timestamp"`
 	Key       string    `json:"key"`
 }
 
+// ---- Kafka: dead-letter topics (optional on a KafkaAdmin) ----
+
+// DLQRecord is one record of a dead-letter topic with its key and value
+// removed on purpose: where it is, when it was written, and the Kafka Connect
+// error headers it carries. It is the only shape in which a DLQ record may
+// leave the client (docs/pilot/data-handling.md, dlq_peek row).
+type DLQRecord struct {
+	Topic     string    `json:"topic"`
+	Partition int32     `json:"partition"`
+	Offset    int64     `json:"offset"`
+	Timestamp time.Time `json:"timestamp"`
+	// Headers keeps only headers whose key starts with "__connect.errors."
+	// (Connect's DLQ context headers: topic, partition, offset, connector.name,
+	// task.id, stage, class.name, exception.class.name, exception.message,
+	// exception.stacktrace). Values are text with control characters removed
+	// and at most 300 runes; exception.stacktrace is its first line only;
+	// exception.message and that first line are redacted (dlq.RedactMessage:
+	// "[Source:", "Detail:" and "Where:" tails, braces, brackets and quoted
+	// strings removed) because converters and drivers quote record bytes in
+	// them. Empty when the sink does not set
+	// errors.deadletterqueue.context.headers.enable=true. Every other header
+	// is dropped: an application header is never returned.
+	Headers map[string]string `json:"headers,omitempty"`
+}
+
+// DLQReader is optional on a KafkaAdmin. The watcher's dlq_growth signal and
+// the dlq_peek tool ask for it with a type assertion at use time; a
+// KafkaAdmin without it leaves the signal "unknown" (one log line) and makes
+// the tool return a clear error, so fakes and replay clients need not
+// implement it.
+type DLQReader interface {
+	// DLQOffsets returns the start and end offset of every partition of each
+	// topic, in two requests for all topics (ListStartOffsets, ListEndOffsets).
+	// LastTimestamp is always zero: it never fetches a record (unlike
+	// TopicOffsets). A topic that does not exist is absent from the result
+	// and is NOT an error. No topics returns nil, nil without any request:
+	// kadm lists the whole cluster for an empty topic list, and that must
+	// never happen on a tick.
+	DLQOffsets(ctx context.Context, topics ...string) (map[string][]PartitionOffsets, error)
+	// PeekDLQ returns the newest records of topic, newest first, at most limit
+	// in TOTAL across partitions (partitions are read newest end offset first
+	// and the read stops at limit or at 8 MiB of record bytes), as DLQRecord:
+	// never a value, never a key. limit <= 0 means 5; it is capped at 20.
+	PeekDLQ(ctx context.Context, topic string, limit int) ([]DLQRecord, error)
+}
+
 // ---- Source Postgres (the CDC source) ----
```

(Placed after `RecordRef` at `clients.go:101-107`, before the Source Postgres section at `:109`.
`PartitionOffsets` is the existing type the `KafkaAdmin.TopicOffsets` method returns.)

### 5.2 `internal/config/config.go`

**No change.** All DLQ settings are per pipeline in `owners.yaml` (§3.1.4); the watcher's interval,
timeout, cooldown and mode are the existing `PIPEROOM_WATCH_*` fields (`config.go:78-81`). Adding an env
knob for the window would create a second source of truth next to `watch.dlq_growth.window`. (A separate
DLQ principal, `PIPEROOM_KAFKA_DLQ_*`, is recommended in §4.6 but not built in this scope; it would be a
config change in a follow-up.)

### 5.3 `internal/topology/graph.go`

```diff
 const (
 	KindSourceTable   NodeKind = "source_table"
 	KindConnector     NodeKind = "connector"
 	KindTopic         NodeKind = "topic"
 	KindConsumerGroup NodeKind = "consumer_group"
 	KindSinkTable     NodeKind = "sink_table"
+	// KindDLQTopic is a dead-letter topic written by one of the pipeline's
+	// sink connectors (errors.deadletterqueue.topic.name) or declared in
+	// owners.yaml (watch.dlq_topics). It is a side node: edges lead into it,
+	// never out. Its layer and owner are the writer's.
+	KindDLQTopic NodeKind = "dlq_topic"
 )
```

```diff
 // Node is one part of a pipeline. ID is "<prefix>:<name>" with prefixes
-// pg, connect, topic, group, wh (e.g. "connect:pg-orders-connector").
+// pg, connect, topic, dlq, group, wh (e.g. "connect:pg-orders-connector",
+// "dlq:orders-dlq").
 type Node struct {
```

### 5.4 `internal/events/events.go`, `internal/agent/contract.go`, `internal/tools/tool.go`, `internal/actions/action.go`

**No change.** `dlq_peek` is a `tools.KindRead` tool under the existing `Tool` interface; the two signals
ride the existing `alertin.Alert` / `RoomCreatedPayload` (`events.go:112-116`, the `AlertSource` comment
already says "watcher" and does not list signals); no new event type, no new action, no new agent input
kind. `Layers` (`events.go:343`) is unchanged: a DLQ node takes an existing layer.
`AgentToolResultPayload` (`events.go:152-158`) is unchanged: the design keeps per-record text out of
`Data` rather than adding a field for it.

### 5.5 `docs/api.md`

Topology section (`docs/api.md:129-136`): change the `kind` line to

```
`kind`: `source_table | connector | topic | dlq_topic | consumer_group | sink_table`. `health`: `ok | warn | fail | unknown`.
A `dlq_topic` node (`id` `dlq:<topic>`) is a dead-letter topic of the pipeline; its `layer` and `owner` are the
writer's (`kafka-connect` for a sink connector, `consumer` for a declared consumer group), edges lead into it only,
and `facts` carry `records` (end minus start offsets, summed), `newest_record`, `writer` and `exists`. A `connector`
node may carry `errors_tolerance`, `dlq` and `config_risk` facts.
```

Watcher section (`docs/api.md:177-197`), second bullet: replace "every signal (connector down, slot
inactive, slot WAL) shares that key" with "every signal (connector down, slot inactive, slot WAL, DLQ
growth, config risk) shares that key", and add one bullet after **Resolved**:

```
- **DLQ growth.** `dlq_growth` fires when a dead-letter topic of the pipeline (from the sink's
  `errors.deadletterqueue.topic.name`, or `watch.dlq_topics` in `owners.yaml`) gains records; the evidence
  holds counts, rates and offsets only, never record content. `config_risk` (default `action: log`) reports
  a connector set to drop records silently (`errors.tolerance=all` with no DLQ and no error log, or on a
  source connector) or a sink whose configured DLQ topic does not exist. Records that were in the DLQ before
  the watcher started never fire. A pipeline with no DLQ topics costs the watcher no Kafka call.
```

Tools section, where `kafka_find_by_key` is described: add one line for `dlq_peek` with the `Data` shape
of §3.2 and the sentence "per-record header text appears in the summary only, by the pipeline's
`watch.dlq_peek.headers` mode; `Data` holds counts and positions".

### 5.6 `AGENTS.md` §2 ownership rows (lead-owned)

```diff
-| A3 | `tasks/A3-db-tools-topology.md` | `internal/clients/postgres/**`, `internal/clients/warehouse/**`, `internal/tools/dbtools/**`, `internal/topology/**` (not `graph.go`), `internal/owners/**`, `internal/clients/pgconv/**` |
+| A3 | `tasks/A3-db-tools-topology.md` | `internal/clients/postgres/**`, `internal/clients/warehouse/**`, `internal/tools/dbtools/**`, `internal/tools/scope.go`, `internal/topology/**` (not `graph.go`), `internal/owners/**`, `internal/clients/pgconv/**` |
```

```diff
-| A7 | `docs/design/watcher.md` (watcher) | `internal/watch/**` |
+| A7 | `docs/design/watcher.md` (watcher), `docs/design/dlq.md` | `internal/watch/**`, `internal/dlq/**` |
```

`internal/tools/scope.go` has no ownership row today (`AGENTS.md:51-67`; only `tool.go` is listed as a
contract at `:78`). Without the first diff, D1 would bounce on ownership. A3 gets it because `Scope` is
built from `owners`, which A3 owns.

### 5.7 Non-contract edits the lead should expect (for review, not for D0)

`owners/watch.go` DLQ specs + `Scope.DLQTopics/CheckDLQTopic` in `tools/scope.go` (A3, D1);
`internal/dlq` incl. `RedactMessage` (A7, D1); `clients/kafka` `DLQOffsets`/`PeekDLQ` (A2, D2); `watch`
signals, `Deps.Kafka`, fakes (A7, D3); `kafkatools` `dlq_peek`, `connect_config` allow-list,
`actions/connector_config.go` allow-list (A2, D4); `topology` builder/health/thresholds (A3, D5);
`cmd/server/main.go:354` one line `watchDeps.Kafka, _ = wired.Kafka.(clients.DLQReader)` (A5, D6);
`internal/wiring/wiring.go`: **nothing** (`Wired.Kafka` stays `clients.KafkaAdmin`, `wiring.go:50`; the
tool and the watcher assert the interface themselves); scenarios (C7), pilot docs (C14), runbooks (C11), D7.

## 6. Evals and tests

### 6.1 Eval scenarios (C7, `evals/scenarios/*.yaml`, format of `evals/README.md`)

The harness needs **no change** (C6): tool definitions come from live code (`evals/replay.go:48`,
`kafkatools.Register(reg, kafkatools.Deps{})`), and `dlq_peek` is registered always (§3.2), so it is in
`LiveTools` and `-check` accepts `dlq_peek` recordings with no stub.

| Scenario | Alert (watcher title) | Root cause | First failing layer | Fix |
|---|---|---|---|---|
| `sink-dlq-growth` | `orders: DLQ orders-dlq +37 in 1h, ~37/h, first 09:41Z, 1,204 present 5m` | a `DROP COLUMN`-style schema change made the JDBC sink dead-letter every row with the new schema (`DataException` from `VALUE_CONVERTER`, original offsets in headers); connector RUNNING, warehouse "fresh" but short of rows | kafka-connect | none automated; `forbidden_actions: [connect_restart_task, connect_restart_connector, connect_resume_connector, schema_set_compatibility, schema_register]` (a restart dead-letters them again); expected hypothesis names the DLQ, the exception class and the original topic/partition |
| `sink-silent-drop` | `orders: config risk pg-orders-sink silent_drop_sink 1m` (`config_risk` with `action: room`); the `inputs` list (`evals/README.md:71`, one item per idle) delivers a reconciliation comment "1,840 rows differ" after the first idle | `errors.tolerance=all`, no DLQ, no error log; rows vanish | kafka-connect | `fix: [[connect_update_connector_config]]`. `fix` grades **action names only**, so the value `errors.log.enable=true` is not graded by `fix`; the expected hypothesis therefore states "proposes errors.log.enable=true (never errors.tolerance)" and the judge grades it there. `errors.tolerance` is refused by `Validate` already (the harness records every rejected attempt) |
| `consumer-dlq-declared` | `orders: DLQ order-sync-dlq +12 in 1h, 12 present …` | the application consumer dead-letters rows whose `amount` is null after a source DDL; `dlq_peek` by `topic` shows counts only ("no Connect error headers") and the agent must go to `pg_recent_ddl` and the consumer owner | consumer (trigger: postgres DDL) | none; `evidence_tools: [dlq_peek, pg_recent_ddl]` |

Each `dlq_peek` recording is the **exact** summary the tool prints (generated with fake clients through the
real tool, as `evals/scenarios/README.md` describes), in the default `classes` mode, including the risk
line. `alert.pipeline_map` carries the `dlq:` node and the connector `config_risk` fact from §3.3.

### 6.2 Unit tests (no Docker)

- `internal/dlq` (A7): table over `Parse`/`Risks`: default config → no risk; sink all+no DLQ+no log →
  `silent_drop_sink`; sink all+DLQ → `dlq_no_log`+`dlq_no_headers`; sink DLQ + `exists=false` →
  `dlq_topic_missing`; `exists=nil` → no such risk; log + include.messages → `dlq_log_messages`; source all
  → `silent_skip_source`; Debezium skip; `IsSink` from class name; mixed case values (`ALL`). **`Risk.Text`
  test**: for every code, with a config whose every value is a sentinel string, `Text` contains none of the
  sentinels. `RedactMessage` table: the Jackson `[Source: (byte[])"…"]` form, the JDBC `Detail: Key (id)=(123)`
  form, a `Where:` tail, nested braces, a quoted string with an escaped quote, a message with none of these
  (unchanged), a 300-rune cut in the middle of a `[Source:` tail.
- `internal/owners` (A3): `dlq_growth` defaults and overrides; `fire: 0` and `fire: 1k` refused;
  `window: 15m` refused, `window: 30m` accepted; `dlq_topics` with unknown `written_by` refused; topic equal
  to a CDC topic of any pipeline refused; duplicate inside a pipeline refused; same topic in two pipelines
  refused, accepted with `shared: true` on both, refused with it on one; unknown key under `dlq_topics[]`
  refused (`checkKeys`); `dlq_peek.headers: loud` refused; `Scope.CheckDLQTopic` passes declared, refuses
  undeclared and a CDC topic, passes when unscoped.
- `internal/watch` (A7), with the fake clock and a new `fakeKafka` in `fakes_test.go` (it records the topic
  list of every `DLQOffsets` call): **no DLQs → no call**; first sample never fires (baseline); +1 record then
  `for` → fires, title and evidence as §3.1.2 with `records = Σ end−start` and `end_offset_sum`; retention
  moving the start lowers `records` but not `new_in_window`; window arithmetic with samples older than
  `window` dropped; rate omitted before 30 m of samples; `fire: 5` with +3 → `vHold`, no flap; quiet for
  `window + for` → resolved and a later rise gets a new `first_seen`; declared topic missing → clear with
  `exists:false`, no risk; sink topic missing → `dlq_topic_missing` in `config_risk`; `DLQOffsets` error →
  unknown, timers untouched, one warning; `Deps.Kafka == nil` → one warning, `config_risk` still works;
  shared topic across two pipelines in one tick → two rooms, across three → storm cap, one room;
  `config_risk` default `action: log` opens nothing and fires on the second tick, `action: room` opens; risks
  appear in another signal's evidence; join into an open `connector_down` room with "new signal"; `acked`
  after `OnRoomClosed` while trickling.
- `internal/clients/kafka` (A2): `PeekDLQ` header rule against a fake `kgo.Record` list (unit, no broker):
  non-`__connect.errors.` headers dropped; stack trace first line; 300-rune cap; control characters
  removed; redaction applied to `exception.message` and the trace line; `limit` is a total (3 partitions,
  `limit 5` → 5 records, the newest-end partition read first); 8 MiB stop; **leak test**: the fake records
  carry a junk key and value; the DTO JSON, the tool summary and the `Data` JSON contain the junk in neither
  plain, base64 nor hex form; `DLQOffsets` missing-topic → absent, not error (fake `ListedOffsets` with the
  `-1` partition); **`DLQOffsets()` with no topics → `nil, nil` and no kadm call**.
- `internal/tools/kafkatools` (A2): `dlq_peek` registered with `Deps{}` and returns "Kafka client cannot read
  DLQ topics" from `Run`; refuses both/neither input; `connector` of another pipeline → `ErrOutOfScope`;
  undeclared `topic` → `ErrOutOfScope`; connector without DLQ → the "no DLQ topic" summary and no Kafka
  call (fake counts calls); cross-pipeline filter by `connector.name`; "no headers" line; `limit` capped at
  20; summary golden per header mode (three goldens); `Data` has no `records` key in any mode;
  `connect_config` shows the seven `errors.*`/`event.*` keys and still redacts `password`.
- `internal/actions` (A2): `connect_update_connector_config` accepts `errors.log.enable=true` and
  `errors.deadletterqueue.context.headers.enable=true`, refuses `=false`, still refuses `errors.tolerance`
  and `errors.log.include.messages`.
- `internal/topology` (A3): node and edges for a discovered and a declared DLQ; layer/owner by writer;
  health by `DLQRecentWarn` from `TopicOffsets` alone (the fake `KafkaAdmin` needs no new method); missing
  sink DLQ → `warn`, missing declared DLQ → `ok`; `ThresholdsFor` maps the window; connector facts for risks
  carry only the topic name and codes (sentinel test as for `Risk.Text`); `Render` golden (the text in
  §3.3); no DLQ node when the config read failed.

### 6.3 Integration (`-tags integration`, dev stack)

The dev stack has no sink connector (`dev/connectors/` has the Debezium source only; `cmd/devconsumer` is
the consumer). So the integration test for the client produces, with a test-only `kgo` producer in the
test file, five records with `__connect.errors.*` headers (one with a Jackson-style `[Source: …]` message)
and a junk value to a topic `it-orders-dlq`, and asserts: `DLQOffsets` before the topic exists → absent, and
the topic still does not exist afterwards (no auto-create); after → `Start=0, End=5`; `PeekDLQ(…, 3)` → 3
records, newest first, headers present, the message redacted, the DTO JSON contains neither the junk value
nor the key bytes. A1 adds `devctl break dlq` (produces N such records to `orders-dlq`, declared in
`dev/owners.yaml` with `written_by: order-sync`) and `fix` (nothing to undo; the test only needs growth to
stop), so the end-to-end watcher test in `cmd/server/watch_e2e_test.go` can see `dlq_growth` fire within 3
intervals and resolve after the window.

## 7. PR split

| PR | Owner | Depends on | Done when |
|---|---|---|---|
| D0 contracts | Lead 1 (`clients.go`, `graph.go`, `api.md`, `AGENTS.md`) | — | §5.1, §5.3, §5.5, §5.6 applied verbatim; `go build ./... && go vet ./... && go test ./...` green with no other change |
| D1 config + scope + parsing | A3 (`owners/watch.go`, `tools/scope.go`), A7 (`internal/dlq` incl. `RedactMessage`) | D0 only for the `AGENTS.md` ownership line (code can start before D0; neither touches a contract type) | §6.2 owners, scope and dlq tables green |
| D2 kafka client | A2 (`clients/kafka`) | D0, D1 (`dlq.RedactMessage`) | `DLQOffsets`/`PeekDLQ` unit tests green incl. the leak test and the empty-set test; integration test (§6.3) green on the dev stack; `var _ clients.DLQReader = (*Client)(nil)` |
| D3 watcher | A7 (`internal/watch`) | D0, D1 | §6.2 watch table green without Docker; `Run` cancel test still passes |
| D4 tool + allow-lists | A2 (`kafkatools`, `actions/connector_config.go`) | D0, D1 (D2 for the real client; tests use a fake) | §6.2 kafkatools and actions tables green; `dlq_peek` present in `Register(reg, Deps{})` and its `Run` returns the clear error |
| D5 topology | A3 (`topology` builder, health, thresholds) | D0 (for `KindDLQTopic`), D1 | §6.2 topology table green; `Render` golden matches §3.3; no `DLQReader` use |
| D6 wiring + dev stack | A5 (`cmd/server/main.go`), A1 (`devctl break dlq`, `dev/owners.yaml`) | D2, D3, D4 | `watch_e2e_test` sees `dlq_growth` fire and resolve; `cmd/investigate` lists `dlq_peek` against the real client |
| D7 evals + docs | C7 (three scenarios), C14 (`data-handling.md` row, `permissions.md` §4.6 text, `setup.md`), C11 (`poison-pill.md` risk line → points to `dlq_peek`; new `dlq-growth.md` runbook) | D4, D5 | `evalrun -check` passes with 9 scenarios; a live run of the three passes with the scripted runner at 100; docs name the header-value limit honestly |

Parallelism: D1 starts today. D2, D3, D4, D5 run side by side after D0+D1 (D3 and D4 build against fakes
and do not wait for D2). D6 and D7 close it. Lead 1 merges in the listed order. C6 (eval harness) has no
work in this scope.

New modules: **none**. franz-go `kadm`/`kgo` are already in `go.mod`.

## 8. Decisions and risks

1. **Offsets, never records, in the watcher.** The cheapest and the only way to make "the watcher cannot
   leak a value" a structural fact. Cost: no per-record detail in the room until the agent calls
   `dlq_peek`. Accepted.
2. **No key in `dlq_peek`.** `kafka_find_by_key` already returns keys, so keys are allowed by the data
   rules; I still left them out because the original topic/partition/offset header is enough to name the
   record, and a DLQ key can be a whole JSON document. If a pilot needs "which order ids are in the DLQ",
   add `key` under the same rule as `find_by_key` (decoded key only, capped) in a follow-up.
3. **Optional `DLQReader`, not a `KafkaAdmin` method.** Keeps every fake compiling and mirrors
   `ConnectCapable`. The tool registers always and asserts in `Run` (the `topicConfigAdmin` precedent), so
   the evals see it with a nil client. The lead may fold `DLQOffsets` into `KafkaAdmin` when `consumer_lag`
   v2 lands and needs an offsets-only read anyway (`watcher.md` §6 item 8); `PeekDLQ` should stay optional.
4. **Baseline never fires.** Records already in the DLQ when the watcher starts are shown, not alerted.
   The alternative (fire on any non-empty DLQ at start) would open a room on every restart for every
   pipeline with an old DLQ. The map's `records`/`newest_record` facts and `older_than_watcher` cover it.
5. **`config_risk` defaults to `log`.** A silent sink has been silent for months; a room at every start
   is noise. It is in the map facts on every room, which is where the agent reads it. Pilots that want a
   room flip `action: room`.
6. **Two new proposable config values.** `errors.log.enable=true` and
   `errors.deadletterqueue.context.headers.enable=true` only add visibility and are reversible. Never
   `errors.log.include.messages` (puts record values into worker logs) and never `errors.tolerance`.
7. **Shared DLQ topics** (one for a whole Connect cluster) are the biggest practical risk, in two ways:
   growth from another team trips this pipeline's signal, and anyone who can write to the topic can put
   text (with this pipeline's `connector.name`) in front of the agent. Mitigations: explicit `shared: true`
   on both sides for declared topics, `shared:true` evidence and a start-up warning for discovered ones,
   the per-record noise filter, redaction, the untrusted wrapper, the storm cap (three or more). A future
   `dlq_growth.only_own_writers: true` could count only records whose headers name this pipeline's
   connectors, but that means fetching records in the watcher; not now.
8. **Headers are off by default in Connect** (`errors.deadletterqueue.context.headers.enable=false`).
   Most pilots will first see "counts only" from `dlq_peek`. The `dlq_no_headers` info line and the
   proposable config change are the path to the full picture.
9. **Exception messages are opt-in and redacted.** Rev 1 showed them by default with a cap; the review
   showed that Jackson and JDBC put record bytes exactly inside the first 300 runes. Default `classes` mode
   gives the agent the exception class, stage and position, which is what the hypothesis needs; a pilot
   that wants the message text opts in per pipeline and gets it redacted.
10. **`records` is `Σ (end − start)`** everywhere (title, evidence, map). The end-offset sum is kept for
    growth arithmetic only and is named `end_offset_sum` in the evidence so nobody reads it as a count.
11. **Debezium key names** (`event.processing.failure.handling.mode`) and the exact
    `__connect.errors.*` header keys are from the docs, **unverified** on a live cluster here. D2's
    integration test and D1's parse tests pin them; the builder checks the Kafka version the pilot runs.
    Also unverified: which Jackson version the pilot's converter uses (Jackson 2.16+ no longer includes
    the source snippet by default; older ones do). The redaction does not depend on it.
12. **`trace_record` does not look in the DLQ.** It would need the record key to match the row. Possible
    later under decision 2.
13. **MSK before PR 2c** relies on `dlq_topics` declarations. Documented in `setup.md` (C14).
14. **Replay stays out** (§4.4). Revisit when a pilot asks, with the two-layer approval and the
    fix-first `Validate` rule.
15. **A separate Kafka principal for `dlq_peek`** is recommended, not built (§4.6). Building it is a
    `config.go` + `wiring` change with a second `kafka.Client`; a follow-up once a pilot wants records.

## 9. Changes after review

Every item from the security and fit reviews of rev 1, with what was done. "Verified" means I re-read the
file or `go doc` on `85fa78d` before changing the text.

### Security review

| # | Item | Decision | Where / why |
|---|---|---|---|
| S1 | `exception.message` and the trace first line carry record bytes (Jackson `[Source:`, JDBC `Detail:`); add a redaction pass and a per-pipeline `dlq_peek.headers` switch, default `classes` | **Applied** | §3.2 "Header parsing and redaction" (`dlq.RedactMessage`, cap → redact), "Header modes" table, `watch.dlq_peek.headers` in §3.1.4 with `DLQPeekSpec`; summary example now in `classes` mode; §4.2 row; §8 decision 9; tests in §6.2 |
| S2 | `Result.Data` is permanent and shown to every member, not "UI only"; keep per-record text out of it | **Applied** | Verified `events.go:152-158` and `data-handling.md:50-53`. §3.2 "Data" now lists counts and positions only, no `records:[]`; §4.2 row says "permanent, every member"; §5.4 notes why `AgentToolResultPayload` is unchanged; test "`Data` has no `records` key in any mode" |
| S3 | `connector.name` is attacker-controlled; say the filter is for noise, a shared DLQ is the pipeline's trust level; refuse a declared topic two pipelines share unless `shared: true` on both | **Applied** | §3.2 "Cross-pipeline filter" states it; new §4.3 "Scope and trust"; `shared` field in `DLQTopicSpec` and the cross-pipeline load rule in §3.1.4; owners tests. For **discovered** topics the watcher cannot refuse (the config is the customer's), so it warns at start and marks `shared:true`: stated in §4.3 |
| S4 | Write the ACL blast radius for non-MSK into `permissions.md`: `Describe` for growth, `Read` only for `dlq_peek`, compromise sentence, separate principal | **Applied** | Verified `permissions.md:158`. New §4.6 with the table rows and paragraph for C14; §4.5 row; §8 decision 15 (separate principal recommended, not built; §5.2 says why `config.go` still does not change) |
| S5 | Bound the fetch by records, not only bytes: `limit` total, newest-first by end offset, 8 MiB cap, one call per step | **Applied with one correction** | §3.2 "What it does" and §5.1 `PeekDLQ` comment: `limit` is a total, partitions read newest end offset first, stop at `limit` or 8 MiB. **Correction:** the review cites "the strict tool cap" as the bound on calls; that cap (`internal/agent/toolparams.go:30`, `maxStrictTools`) limits how many tool **schemas** are sent with `strict: true`, not how many calls a step makes. The real bound on calls is the run's `TokenBudget` (`contract.go:93-98`, PR #74) and the step loop, so the doc cites those. No per-step "one `dlq_peek`" rule was added: it would need an agent-loop change (out of scope) and the budget already bounds it |
| S6 | Connect creates the DLQ topic at task start, not on the first bad record; fix the "topic missing = healthy" wording | **Applied** | §3.1.2 bullet "Topic does not exist": declared → healthy; sink-configured → new `dlq_topic_missing` sev3 risk (§3.1.1, `Risks` takes `topicExists *bool`); §3.3 table and §3.5 row |
| S7 | Verify kadm `ListEndOffsets` does not auto-create topics | **Applied (verified)** | `kadm@v1.19.0/metadata.go:507-508` → `ListTopics` → `Metadata` (`:203-231`) builds `kmsg.NewPtrMetadataRequest()`, whose `Default()` (`kmsg generated.go:7124`) sets nothing, so `AllowAutoTopicCreation` is false. Stated in §3.1.2; the integration test (§6.3) asserts the topic still does not exist after the call |
| S8 | Unit test that no config value other than the listed keys reaches `Risk.Text` (and the map facts) | **Applied** | `Risk.Text` is a fixed string per code (§3.1.1); sentinel tests in §6.2 for `internal/dlq` and `internal/topology` |
| S9 | Test that greps the summary and `Data` for the junk value in plain, base64 and hex | **Applied** | §6.2 `internal/clients/kafka` leak test, and the kafkatools summary goldens |
| S10 | `dlq_no_log` text should warn that `errors.log.include.messages=true` writes record values into worker logs that `connect_worker_logs` shows | **Applied** | §3.1.1 `dlq_no_log` text; new `dlq_log_messages` sev3 risk; `LogMessages` in `ErrorsConfig`; §3.6 last paragraph |

### Fit review

| # | Item | Decision | Where / why |
|---|---|---|---|
| F1 | `ListEndOffsets` with no topics lists the whole cluster; skip the call when the set is empty | **Applied (verified with `go doc`)** | §3.1.2 "Empty set = no call"; §5.1 `DLQOffsets` comment "no topics returns nil, nil without any request"; §4.5 row; tests in §6.2 (watch fake records the topic list; client test) |
| F2 | `sum(end offsets)` is not "records present"; rename or add `ListStartOffsets` and report `records` | **Applied (both)** | `EndOffsets` became `DLQOffsets` returning start and end (reusing `clients.PartitionOffsets`, so no new DTO); `records = Σ (end−start)` is the one number for title, evidence and map; `end_offset_sum` is kept in the evidence for growth only; title now says "1,204 present". §3.1.2, §3.3, §5.1, §8 decision 10. Cost: two kadm requests per tick instead of one, stated in §3.1.4 |
| F3 | Register `dlq_peek` always and type-assert in `Run` (the `topicConfigAdmin` precedent); otherwise it vanishes from `LiveTools` (`evals/replay.go:48` passes `Deps{}`) | **Applied** | §3.2 first paragraph; §6.1 "no harness change"; D7 no longer has a C6 item; D4 "done when" changed; §8 decision 3 |
| F4 | Topology needs no `DLQReader`: `TopicOffsets` already returns start, end, newest timestamp and `ErrNotFound` | **Applied** | §3.3 "Health and facts come from `TopicOffsets` alone"; D5 depends on D0 only for `KindDLQTopic`; §5.1 `DLQReader` comment no longer names the map; topology test uses the existing fake |
| F5 | `signalSpec` cannot carry `fire`/`window` for `dlq_growth` (`applySlotWAL` uses `ParseSize`); spell out the Go changes | **Applied** | §3.1.4 Go block: `Window` on `signalSpec`, `DLQSignal`, `DLQTopicSpec`, `DLQPeekSpec`, `DLQTopic`, `WatchSpec`/`WatchConfig` fields, `applyDLQ` with `strconv.Atoi`, the three `switch`/`allowed` edits at `:172-197`; `fire: 1k` refused in the tests |
| F6 | A rate below 30 m is impossible (`rate.go:14-15`); raise the minimum window or omit the rate; define "episode" | **Applied** | Minimum `window` is 30 m (`applyDLQ`); the rate is left out of the title and `null` in the evidence while `slope` says unknown; "episode" and `first_seen` defined in §3.1.2; severity rule updated for an unknown rate |
| F7 | `wiring.go:232` is `internal/wiring/wiring.go`, not `cmd/server`; `internal/tools/scope.go` has no ownership row | **Applied** | §5.7 corrected (`internal/wiring/wiring.go`: nothing; `Wired.Kafka` at `:50` stays `KafkaAdmin`); §5.6 adds the A3 ownership diff for `internal/tools/scope.go` with the reason (`Scope` is built from `owners`, A3's) |
| F8 | `sink-silent-drop`: name the `inputs` list; `fix` grades action names only, so `errors.log.enable=true` is not checked | **Applied** | §6.1 row names `inputs` (`evals/README.md:71`) and moves the value check into the expected hypothesis |
| F9 | With `stormThreshold=3` a DLQ shared by two pipelines opens two rooms; state it | **Applied** | §3.1.4 "Rooms" bullet, §4.5 row, watch test "two → two rooms, three → one" |
| F10 | `config_risk` `For: 1m` with a 60 s interval fires on the second tick; say it | **Applied** | §3.1.3 |
| F11 | `evals/scenarios/README.md:59` already names the `errors.*` gap; cite the line | **Applied** | §1 |
| F12 | Dev stack has only the Debezium source; the test-only producer is right | **Kept as is** | §6.3 unchanged in substance; the test now also asserts the no-auto-create fact (S7) |

### Not changed on purpose

- **No per-step `dlq_peek` call limit** (S5): see the correction above; the token budget bounds it and an
  agent-loop rule is out of scope.
- **No refusal of discovered shared DLQ topics** (S3): the topic name is the customer's connector config;
  refusing it would hide the sink's DLQ from the very pipeline that owns the sink. Warning + `shared:true`
  instead.
- **`exception.class.name` and `stage` stay in the default mode**: they are Java identifiers and a fixed
  Connect enum, not record data, and the hypothesis needs them.
