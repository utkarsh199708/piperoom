package evals

import (
	"context"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/clickhouse"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/tools"
	"piperoom/internal/tools/logtools"
)

// W4o (KAN-498). The 14 scenarios that passed 3/3 in run 39994729845 still
// had 68 replay misses: reads on the prompt's standard path that no recording
// answered. Each of those calls now resolves to a recording, and each new
// recording has the shape of the real tool's summary
// (internal/tools/{kafkatools,dbtools,logtools,nettools}).

// w4oMiss is one distinct miss: the scenario, the call (tool and its input as
// the model sent it) and how many of the 3 runs missed it.
type w4oMiss struct {
	scenario string
	tool     string
	input    string
	runs     int
}

var w4oMisses = []w4oMiss{
	// Generated from reviews/evals-2026-10-23/misses.json (W4-0, PR #943): the misses of the
	// 14 passing scenarios of run 39994729845, grouped by scenario, tool and input; runs is how many
	// of the 3 runs missed that exact call.
	{"watcher-start", "connect_source_offsets", `{"connector":"pg-orders-connector"}`, 3},
	{"watcher-start", "connect_worker_logs", `{"connector":"pg-orders-connector","since":"30m"}`, 3},
	{"watcher-start", "source_blocking_sessions", `{"older_than_minutes":5}`, 1},
	{"watcher-start", "source_settings", `{}`, 2},
	{"watcher-start", "pg_streaming_liveness", `{"pipeline_id":"orders"}`, 1},
	{"watcher-start", "net_probe", `{"target":"source"}`, 1},
	{"db-unreachable-from-connect", "connect_worker_logs", `{"connector":"pg-orders-connector","limit":30,"since":"1h"}`, 1},
	{"db-unreachable-from-connect", "connect_worker_info", `{}`, 3},
	{"db-unreachable-from-connect", "net_probe", `{"target":"connect"}`, 2},
	{"db-unreachable-from-connect", "connect_worker_logs", `{"connector":"pg-orders-connector","limit":50,"since":"3h"}`, 1},
	{"db-unreachable-from-connect", "connect_worker_logs", `{"connector":"pg-orders-connector","limit":40,"since":"2h"}`, 1},
	{"db-unreachable-from-connect", "pg_streaming_liveness", `{"pipeline_id":"orders"}`, 1},
	{"recurring-connector-paused", "connect_source_offsets", `{"connector":"pg-orders-connector"}`, 3},
	{"recurring-connector-paused", "pg_streaming_liveness", `{"pipeline_id":"orders","window_seconds":8}`, 1},
	{"recurring-connector-paused", "pg_streaming_liveness", `{"pipeline_id":"orders"}`, 1},
	{"recurring-connector-paused", "connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":60,"since":"10h"}`, 1},
	{"recurring-connector-paused", "pg_streaming_liveness", `{"pipeline_id":"orders","window_seconds":5}`, 1},
	{"recurring-connector-paused", "reconcile", `{"since":"2026-10-11T03:45:00Z","table":"public.orders"}`, 1},
	{"clickhouse-mv-schema-change", "similar_incidents", `{"pipeline_id":"orders","query":"analytics.orders stale; ClickHouse materialized view insert failing"}`, 2},
	{"clickhouse-mv-schema-change", "reconcile", `{"since":"2026-10-20T08:00:00Z","table":"public.orders"}`, 2},
	{"clickhouse-mv-schema-change", "reconcile", `{"since":"2026-10-20T08:10:00Z","table":"public.orders"}`, 1},
	{"clickhouse-mv-schema-change", "similar_incidents", `{"query":"ClickHouse materialized view insert failing code 47 unknown identifier after ADD COLUMN"}`, 1},
	{"register-rejected-renamed-key", "connect_worker_logs", `{"connector":"pg-orders-connector","since":"2h"}`, 3},
	{"register-rejected-renamed-key", "connect_source_offsets", `{"connector":"pg-orders-connector"}`, 3},
	{"missed-table", "pg_connector_scope_gap", `{"pipeline_id":"orders"}`, 3},
	{"missed-table", "pg_streaming_liveness", `{"pipeline_id":"orders"}`, 2},
	{"ambiguous-lag-spike", "kafka_throughput", `{"window":"30m"}`, 3},
	{"ambiguous-lag-spike", "warehouse_load_errors", `{"table":"analytics.orders"}`, 1},
	{"sink-filtered-rows", "dlq_peek", `{"topic":"pg.public.orders.dlq"}`, 1},
	{"sink-filtered-rows", "warehouse_load_errors", `{"since":"2026-10-21T08:25:00Z","table":"analytics.orders"}`, 3},
	{"consumer-bad-deploy", "warehouse_load_errors", `{"since":"2026-10-18T08:40:00Z","table":"analytics.orders"}`, 1},
	{"consumer-bad-deploy", "warehouse_load_errors", `{"since":"2026-10-18T08:00:00Z","table":"analytics.orders"}`, 1},
	{"consumer-bad-deploy", "warehouse_load_errors", `{"since":"2026-10-18T08:45:00Z","table":"analytics.orders"}`, 1},
	{"publication-permission-denied", "source_role_grants", `{"pipeline_id":"orders"}`, 3},
	{"setup/missing-grant", "connect_source_offsets", `{"connector":"pg-orders-connector"}`, 3},
	{"unclear-driver-intent", "source_role_grants", `{"pipeline_id":"orders"}`, 3},
	{"source-column-dropped", "kafka_throughput", `{"window":"1h"}`, 2},
	{"dlq-backlog", "kafka_throughput", `{"table":"public.orders","window":"2h"}`, 1},
}

// realNoLogs runs the production connect_worker_logs on sc's pipeline with no
// Loki reader, as the eval harness has: the not-configured answer.
func realNoLogs(t *testing.T, sc Scenario, input string) string {
	t.Helper()
	own, err := loadOwnersFor(sc)
	if err != nil {
		t.Fatal(err)
	}
	reg := tools.NewRegistry()
	if err := logtools.Register(reg, logtools.Deps{Owners: own}); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get(logtools.Name)
	if !ok {
		t.Fatalf("%s is not registered", logtools.Name)
	}
	r, err := tl.Run(tools.WithPipeline(context.Background(), sc.Pipeline), json.RawMessage(input))
	if err != nil {
		t.Fatal(err)
	}
	return r.Summary
}

// w4oScenarioPath maps a scenario name to its file ("setup/x" is setup-x.yaml).
func w4oScenarioPath(name string) string {
	return filepath.Join("scenarios", strings.ReplaceAll(name, "/", "-")+".yaml")
}

// W4o 2: the table is the 68 misses of the 14 scenarios, and every one of them
// resolves to a recording of its scenario now. A call that matches nothing is
// the "no data available" miss the report counts.
func TestW4oEveryMissResolvesToARecording(t *testing.T) {
	perScenario := map[string]int{}
	total := 0
	for _, m := range w4oMisses {
		perScenario[m.scenario] += m.runs
		total += m.runs
	}
	if total != 68 || len(perScenario) != 14 {
		t.Fatalf("table has %d misses in %d scenarios, want 68 in 14", total, len(perScenario))
	}
	want := map[string]int{
		"watcher-start": 11, "db-unreachable-from-connect": 9, "recurring-connector-paused": 8,
		"clickhouse-mv-schema-change": 6, "register-rejected-renamed-key": 6, "missed-table": 5,
		"ambiguous-lag-spike": 4, "sink-filtered-rows": 4, "consumer-bad-deploy": 3,
		"publication-permission-denied": 3, "setup/missing-grant": 3, "unclear-driver-intent": 3,
		"source-column-dropped": 2, "dlq-backlog": 1,
	}
	for name, n := range want {
		if perScenario[name] != n {
			t.Errorf("%s: table has %d misses, W4-0 counted %d", name, perScenario[name], n)
		}
	}
	for _, m := range w4oMisses {
		sc := loadScenario(t, w4oScenarioPath(m.scenario))
		if sc.Name != m.scenario {
			t.Fatalf("%s: scenario name = %q", w4oScenarioPath(m.scenario), sc.Name)
		}
		r, err := newScenarioReplayer(sc)
		if err != nil {
			t.Fatalf("%s: %v", m.scenario, err)
		}
		rec, ok := r.lookup(m.tool, json.RawMessage(m.input))
		if !ok {
			t.Errorf("%s: %s %s still misses", m.scenario, m.tool, m.input)
			continue
		}
		if len(rec.obs) == 0 || (rec.obs[0].summary == "" && rec.obs[0].err == "") {
			t.Errorf("%s: %s %s resolves to an empty recording", m.scenario, m.tool, m.input)
		}
	}
}

var (
	w4oStamp    = `\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ`
	w4oOffsetRE = regexp.MustCompile(`^pg-orders-connector( partition server=pg: lsn=\d+, lsn_proc=\d+, ts_usec=\d+, txId=\d+|: no stored offsets)$`)
	w4oLiveRE   = regexp.MustCompile(`^(quiet: no source writes in \d+s \(slot "[a-z_]+", confirmed_flush_lsn [0-9A-F]+/[0-9A-F]+ unchanged, active=(true|false)\); not a stall|stuck: confirmed_flush_lsn did not move while the source wrote \d+ bytes in \d+s \(slot "[a-z_]+", active=(true|false), wal_status=\w+, retains [\d.]+ [KMG]iB\)); connector \S+ state [A-Z]+(; the slot is INACTIVE, so no walsender is streaming it)?\n`)
	w4oNetRE    = regexp.MustCompile(`^net_probe (source|connect) \([^)]+\): all checks ok: dns ok, tcp ok(, connect_view ok)?\n- `)
	w4oThruRE   = regexp.MustCompile(`^orders last (30m|1h|2h): no skew found\npg\.public\.orders: \d+ records \([\d.]+/s\) over 3 partitions; median [\d.]+/s, max p\d [\d.]+/s → even\norder-sync \((Stable|Empty), \d+ members\): lag \d+(; pg\.public\.orders p\d \d+% \(\d+\))?\nno consumed rate or lag trend: Kafka keeps no committed-offset history and this process has no group_lag watch samples \(the watcher does not run here\)$`)
	w4oLoadRE   = regexp.MustCompile(`^postgres warehouse has no load history\. analytics\.orders: \d+ rows inserted since stats reset, \d+ dead tuples, last autovacuum (never|` + w4oStamp + `), 0 session\(s\) waiting on a lock$`)
	w4oBucketRE = regexp.MustCompile(`^(\d\d:\d\d) \| (\d+) \| (\d+) \| (\d+) \| ([+-]\d+)$`)
	w4oKeysRE   = regexp.MustCompile(`^keys not compared: (\d+ source / \d+ sink keys exceed the 2000-key cap; narrow since/until for a key diff|the warehouse client cannot list keys)$`)
	w4oNoteRE   = regexp.MustCompile(`^note: rows newer than \d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{9}Z \(the last 2m0s\) are in flight and not judged$`)
)

// W4o 3: each new recording has the shape the real tool prints. The checks are
// structural (the values are composed); the formats are the Sprintf strings of
// the tools, and the trailing newline follows the tool's own builder: dbtools'
// pg_* and source_* summaries end with one, kafkatools', net_probe's, the
// worker log's and reconcile's do not.
func TestW4oRecordingsHaveTheRealToolsShape(t *testing.T) {
	checked := map[string]int{}
	for _, m := range w4oMisses {
		sc := loadScenario(t, w4oScenarioPath(m.scenario))
		r, err := newScenarioReplayer(sc)
		if err != nil {
			t.Fatal(err)
		}
		rec, ok := r.lookup(m.tool, json.RawMessage(m.input))
		if !ok {
			t.Fatalf("%s: %s %s misses", m.scenario, m.tool, m.input)
		}
		s := rec.obs[0].summary
		where := m.scenario + " " + m.tool + " " + m.input
		checked[m.tool]++
		wantNL := false
		switch m.tool {
		case "connect_source_offsets":
			if !w4oOffsetRE.MatchString(s) {
				t.Errorf("%s: summary %q", where, s)
			}
		case "connect_worker_logs":
			// No Loki runs in a replay, so the real tool answers with its not-configured text.
			if want := realNoLogs(t, sc, m.input); s != want {
				t.Errorf("%s: summary %q", where, s)
			}
		case "source_blocking_sessions":
			wantNL = true
			if s != "(no blocking sessions visible to this role; other sessions' transactions need pg_read_all_stats, and prepared transactions are not listed)\n" {
				t.Errorf("%s: summary %q", where, s)
			}
		case "source_settings":
			wantNL = true
			if s != "wal_level=logical\nmax_replication_slots=10\nmax_wal_senders=10\nmax_slot_wal_keep_size=-1 MB\nwal_sender_timeout=60000 ms\n" {
				t.Errorf("%s: summary %q", where, s)
			}
		case "pg_streaming_liveness":
			wantNL = true
			if !w4oLiveRE.MatchString(s) {
				t.Errorf("%s: summary %q", where, s)
			}
			if strings.HasPrefix(s, "stuck:") && !strings.HasSuffix(s, "no open writer transaction was found; the slot is held back by something else\n") {
				t.Errorf("%s: a stuck verdict ends with the transaction line", where)
			}
		case "net_probe":
			if !w4oNetRE.MatchString(s) {
				t.Errorf("%s: summary %q", where, s)
			}
		case "connect_worker_info":
			for _, w := range []string{"Connect worker groups: 1\n- group connect-cluster (Stable): 2 workers; runs the pipeline's connectors\n", "Internal topics (name match):\n", "\nPlugins: Kafka ", "\nLog levels: root INFO"} {
				if !strings.Contains(s, w) {
					t.Errorf("%s: summary lacks %q", where, w)
				}
			}
		case "pg_connector_scope_gap":
			wantNL = true
			for _, w := range []string{
				"connector pg-orders-connector, publication dbz_publication (connector user piperoom)\n",
				"public.devctl_refunds: declared=yes, in_connector=yes, in_publication=no\n",
				"public.devctl_signal: declared=no, in_connector=yes, in_publication=yes (in connector, outside declared scope)\n",
				"public.devctl_signal is streamed by the connector but owners.yaml does not declare it: ask the pipeline owner to add it to source_tables before proposing a fix for it.\n",
			} {
				if !strings.Contains(s, w) {
					t.Errorf("%s: summary lacks %q", where, w)
				}
			}
		case "source_role_grants":
			wantNL = true
			if s != "connector pg-orders-connector role piperoom: exists=yes login=yes replication=yes\nselectable tables: 0 of 1\npublic.orders: selectable=no\n" {
				t.Errorf("%s: summary %q", where, s)
			}
		case "kafka_throughput":
			if !w4oThruRE.MatchString(s) {
				t.Errorf("%s: summary %q", where, s)
			}
		case "warehouse_load_errors":
			if !w4oLoadRE.MatchString(s) {
				t.Errorf("%s: summary %q", where, s)
			}
		case "similar_incidents":
			if s != "No similar closed incident found on pipeline orders (3 past rooms searched)." {
				t.Errorf("%s: summary %q", where, s)
			}
		case "reconcile":
			checkReconcileRecording(t, where, s)
		case "dlq_peek":
			// The not-found answer is the real scope refusal, not a hand-written text.
			if want := dlqRefusal(t, sc, "pg.public.orders.dlq"); rec.obs[0].err != want {
				t.Errorf("%s: error %q, the real tool refuses with %q", where, rec.obs[0].err, want)
			}
			continue
		default:
			t.Errorf("%s: no shape check for this tool", where)
		}
		if got := strings.HasSuffix(s, "\n"); got != wantNL {
			t.Errorf("%s: summary ends with a newline = %v, the real tool's = %v", where, got, wantNL)
		}
	}
	for _, tool := range []string{"connect_source_offsets", "connect_worker_logs", "source_blocking_sessions", "source_settings", "pg_streaming_liveness",
		"net_probe", "connect_worker_info", "pg_connector_scope_gap", "source_role_grants", "kafka_throughput", "warehouse_load_errors",
		"similar_incidents", "reconcile", "dlq_peek"} {
		if checked[tool] == 0 {
			t.Errorf("%s was not checked", tool)
		}
	}
}

// dlqRefusal is what dlq_peek returns for a topic the pipeline does not
// declare: tools.Scope.CheckDLQTopic, called on the scenario's pipeline.
func dlqRefusal(t *testing.T, sc Scenario, topic string) string {
	t.Helper()
	scope, err := tools.ScopeFor(tools.WithPipeline(context.Background(), sc.Pipeline), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = scope.CheckDLQTopic(topic)
	if err == nil {
		t.Fatalf("%s declares %s", sc.Name, topic)
	}
	return err.Error()
}

// checkReconcileRecording re-derives a reconcile recording's own arithmetic:
// the sink-source column, the verdict's missing count and event counts, and the
// key-cap and in-flight lines of the tool's footer.
func checkReconcileRecording(t *testing.T, where, s string) {
	t.Helper()
	lines := strings.Split(s, "\n")
	if len(lines) < 5 || !strings.HasPrefix(lines[0], "reconcile public.orders → pg.public.orders → analytics.orders by updated_at, buckets of ") ||
		lines[1] != "bucket | source rows | topic events | sink rows | sink−source" {
		t.Errorf("%s: header %q", where, lines[:min(2, len(lines))])
		return
	}
	type row struct{ src, topic, sink int }
	var rows []row
	i := 2
	for ; i < len(lines); i++ {
		g := w4oBucketRE.FindStringSubmatch(lines[i])
		if g == nil {
			break
		}
		var r row
		r.src, _ = strconv.Atoi(g[2])
		r.topic, _ = strconv.Atoi(g[3])
		r.sink, _ = strconv.Atoi(g[4])
		if d, _ := strconv.Atoi(g[5]); d != r.sink-r.src {
			t.Errorf("%s: %s: sink−source = %d, want %d", where, g[1], d, r.sink-r.src)
		}
		rows = append(rows, r)
	}
	if len(rows) == 0 || len(rows) > 12 {
		t.Errorf("%s: %d buckets, want 1 to 12", where, len(rows))
		return
	}
	rest := lines[i:]
	if len(rest) != 3 {
		t.Errorf("%s: footer = %q, want verdict, keys and note lines", where, rest)
		return
	}
	first, last := -1, -1
	for j, r := range rows {
		if gap := r.src - r.sink; gap >= 1 && float64(gap) > 0.02*float64(r.src) {
			if first < 0 {
				first = j
			}
			last = j
		}
	}
	if first < 0 {
		t.Errorf("%s: no bucket is short, so the recording shows no loss", where)
		return
	}
	var missing, src, topic int
	for _, r := range rows[first : last+1] {
		missing += r.src - r.sink
		src += r.src
		topic += r.topic
	}
	if want := "verdict: sink is missing " + strconv.Itoa(missing) + " rows in "; !strings.HasPrefix(rest[0], want) {
		t.Errorf("%s: verdict %q does not start with %q", where, rest[0], want)
	}
	tail := " change events for " + strconv.Itoa(src) + " source rows"
	if !strings.Contains(rest[0], "the topic carried "+strconv.Itoa(topic)+tail) && !strings.Contains(rest[0], "the topic carried only "+strconv.Itoa(topic)+tail) {
		t.Errorf("%s: verdict %q does not count %d topic events for %d source rows", where, rest[0], topic, src)
	}
	if !w4oKeysRE.MatchString(rest[1]) {
		t.Errorf("%s: keys line %q", where, rest[1])
	}
	if !w4oNoteRE.MatchString(rest[2]) {
		t.Errorf("%s: note line %q", where, rest[2])
	}
}

// w4oKeyLister is the optional reader reconcile's key diff needs
// (dbtools/reconcile_keys.go windowKeysReader).
type w4oKeyLister interface {
	WindowKeys(ctx context.Context, table, keyColumn, tsColumn string, since, until time.Time, limit int) ([]string, int64, error)
}

// The key line follows the warehouse's client: the Postgres warehouse lists
// keys, so a window over the cap gets the key-cap note; the ClickHouse client
// does not, so reconcile on clickhouse-mv-schema-change says the warehouse
// client cannot list keys. If ClickHouse ever gains WindowKeys this fails and
// the recordings must be redone with the key diff.
func TestW4oReconcileKeyLineFollowsTheWarehouseClient(t *testing.T) {
	if _, ok := any((*warehouse.DB)(nil)).(w4oKeyLister); !ok {
		t.Fatal("the Postgres warehouse client no longer lists keys: the key-cap recordings need redoing")
	}
	if _, ok := any((*clickhouse.Client)(nil)).(w4oKeyLister); ok {
		t.Fatal("the ClickHouse client lists keys now: redo clickhouse-mv-schema-change's reconcile recordings")
	}
	sc, err := LoadScenario(w4oScenarioPath("clickhouse-mv-schema-change"))
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, rec := range sc.Recordings {
		if rec.Tool != "reconcile" || !strings.Contains(rec.Summary, "source rows | topic events") {
			continue
		}
		seen++
		if !strings.Contains(rec.Summary, "\nkeys not compared: the warehouse client cannot list keys\n") {
			t.Errorf("reconcile %v on a ClickHouse sink: key line is not the client's note:\n%s", rec.Input, rec.Summary)
		}
	}
	if seen != 2 {
		t.Errorf("checked %d current-format reconcile recordings, want 2", seen)
	}
}
