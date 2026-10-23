package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeWorkerKafka adds the optional ConnectClusterReader to fakeKafka.
type fakeWorkerKafka struct {
	*fakeKafka
	groups    []kafka.ConnectWorkerGroup
	groupsErr error
	topics    func(names []string) ([]kafka.InternalTopic, error)
	gotNames  []string
}

func (f *fakeWorkerKafka) ConnectWorkerGroups(context.Context) ([]kafka.ConnectWorkerGroup, error) {
	return f.groups, f.groupsErr
}

func (f *fakeWorkerKafka) InternalTopics(_ context.Context, names []string) ([]kafka.InternalTopic, error) {
	f.gotNames = names
	return f.topics(names)
}

// fakeWorkerConnect is a plugin/version Connect fake that also reads loggers.
type fakeWorkerConnect struct {
	*fakePluginsConnect
	loggersFn func(context.Context) ([]connect.LoggerLevel, error)
}

func (f *fakeWorkerConnect) Loggers(ctx context.Context) ([]connect.LoggerLevel, error) {
	return f.loggersFn(ctx)
}

func healthyTopics([]string) ([]kafka.InternalTopic, error) {
	return []kafka.InternalTopic{
		{Name: "connect-analytics-configs", Role: "config", Partitions: 1, ReplicationFactor: 3, CleanupPolicy: "compact", Brokers: 3},
		{Name: "connect-analytics-offsets", Role: "offset", Partitions: 25, ReplicationFactor: 3, CleanupPolicy: "compact", Brokers: 3},
		{Name: "connect-analytics-status", Role: "status", Partitions: 5, ReplicationFactor: 3, CleanupPolicy: "compact", Brokers: 3},
		{Name: "connect-prod-configs", Role: "config", Partitions: 1, ReplicationFactor: 3, CleanupPolicy: "compact", MinInSyncReplicas: 2, Brokers: 3},
		{Name: "connect-prod-offsets", Role: "offset", Partitions: 25, ReplicationFactor: 3, CleanupPolicy: "compact", Brokers: 3},
		{Name: "connect-prod-status", Role: "status", Partitions: 5, ReplicationFactor: 3, CleanupPolicy: "compact", Brokers: 3},
	}, nil
}

func twoGroups() []kafka.ConnectWorkerGroup {
	return []kafka.ConnectWorkerGroup{
		{GroupID: "connect-prod", State: "Stable", Workers: []kafka.ConnectWorker{
			{MemberID: "m1", Host: "10.0.0.5", URL: "http://10.0.0.5:8083/", Leader: true, Connectors: []string{"pg-orders-connector", "secret-billing-sink"}},
			{MemberID: "m2", Host: "10.0.0.6", Connectors: []string{"other-source"}},
		}},
		{GroupID: "connect-analytics", State: "Stable", Workers: []kafka.ConnectWorker{
			{MemberID: "m9", Host: "10.9.9.9", URL: "http://10.9.9.9:8083/", Connectors: []string{"analytics-sink"}},
		}},
	}
}

func workerInfoTool(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get("connect_worker_info")
	if !ok {
		t.Fatal("connect_worker_info not registered")
	}
	return tool
}

func ordersCtx() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders-connector"}})
}

func TestWorkerInfoScopedShowsOnlyTheRoomsGroup(t *testing.T) {
	fk := &fakeWorkerKafka{fakeKafka: &fakeKafka{}, groups: twoGroups(), topics: healthyTopics}
	fc := &fakeWorkerConnect{
		fakePluginsConnect: newPluginsFake(t, allPluginKinds()),
		loggersFn: func(context.Context) ([]connect.LoggerLevel, error) {
			return []connect.LoggerLevel{{Logger: "root", Level: "INFO"}, {Logger: "io.debezium", Level: "DEBUG"}, {Logger: "org.apache.kafka", Level: "INFO"}}, nil
		},
	}
	tool := workerInfoTool(t, Deps{Kafka: fk, Connect: fc})
	r, err := tool.Run(ordersCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"group connect-prod (Stable): 2 workers; runs the pipeline's connectors",
		"worker 10.0.0.5 http://10.0.0.5:8083/ leader; runs pg-orders-connector; 1 other connectors",
		"worker 10.0.0.6 (REST URL not advertised); 1 other connectors",
		"config connect-prod-configs: 1 partitions, RF 3, cleanup compact, min ISR 2",
		"status connect-prod-status: 5 partitions",
		"Kafka 3.6.1; 6 plugins",
		"Log levels: root INFO; DEBUG io.debezium",
		"1 other Connect worker group not shown",
		"3 other Connect internal topics not shown",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	// Other pipelines' connector names and the other group never show.
	b, _ := json.Marshal(r.Data)
	for _, leak := range []string{"secret-billing-sink", "other-source", "analytics", "10.9.9.9"} {
		if strings.Contains(r.Summary, leak) || strings.Contains(string(b), leak) {
			t.Errorf("%q leaked into the result", leak)
		}
	}
	if strings.Contains(r.Summary, "Findings") {
		t.Errorf("healthy topics flagged:\n%s", r.Summary)
	}
	if fk.gotNames != nil {
		t.Errorf("no CONNECT_INTERNAL_TOPICS: names = %v, want discovery", fk.gotNames)
	}
}

// devStackTwoClusters is the dev stack with its staging profile: two Connect
// clusters on one Kafka, the default one with Debezium-image-style generic
// topic names and the staging one with suffixed names.
func devStackTwoClusters() *fakeWorkerKafka {
	topic := func(name, role string) kafka.InternalTopic {
		return kafka.InternalTopic{Name: name, Role: role, Partitions: 1, ReplicationFactor: 1, CleanupPolicy: "compact", Brokers: 1}
	}
	return &fakeWorkerKafka{fakeKafka: &fakeKafka{},
		groups: []kafka.ConnectWorkerGroup{
			{GroupID: "piperoom-connect", State: "Stable", Workers: []kafka.ConnectWorker{{MemberID: "m1", Host: "connect", Connectors: []string{"pg-orders-connector"}}}},
			{GroupID: "piperoom-connect-staging", State: "Stable", Workers: []kafka.ConnectWorker{{MemberID: "s1", Host: "connect-staging", Connectors: []string{"staging-billing-source"}}}},
		},
		topics: func([]string) ([]kafka.InternalTopic, error) {
			return []kafka.InternalTopic{
				topic("_connect_configs", "config"), topic("_connect_configs_staging", "config"),
				topic("_connect_offsets", "offset"), topic("_connect_offsets_staging", "offset"),
				topic("_connect_status", "status"), topic("_connect_status_staging", "status"),
			}, nil
		},
	}
}

func TestWorkerInfoTwoConnectClustersDoNotLeak(t *testing.T) {
	fk := devStackTwoClusters()
	tool := workerInfoTool(t, Deps{Kafka: fk, Connect: &fakeConnect{}})

	for _, tc := range []struct {
		name      string
		connector string
		want      []string
		leaks     []string
	}{
		{"default cluster's room", "pg-orders-connector",
			[]string{"group piperoom-connect (Stable)", "config _connect_configs:", "offset _connect_offsets:", "status _connect_status:"},
			[]string{"staging"}},
		{"staging cluster's room", "staging-billing-source",
			[]string{"group piperoom-connect-staging (Stable)", "config _connect_configs_staging:", "status _connect_status_staging:"},
			[]string{"_connect_configs:", "_connect_offsets:", "_connect_status:", "\"_connect_configs\"", "pg-orders"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tools.WithPipeline(context.Background(), owners.Pipeline{ID: "p", Connectors: []string{tc.connector}})
			r, err := tool.Run(ctx, json.RawMessage(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range append(tc.want, "1 other Connect worker group not shown", "3 other Connect internal topics not shown") {
				if !strings.Contains(r.Summary, want) {
					t.Errorf("summary lacks %q:\n%s", want, r.Summary)
				}
			}
			b, _ := json.Marshal(r.Data)
			for _, leak := range tc.leaks {
				if strings.Contains(r.Summary, leak) || strings.Contains(string(b), leak) {
					t.Errorf("%q leaked:\n%s\n%s", leak, r.Summary, b)
				}
			}
			if d := r.Data.(workerInfoData); len(d.InternalTopics) != 3 || d.TopicsNotShown != 3 {
				t.Errorf("topics = %+v, not shown = %d", d.InternalTopics, d.TopicsNotShown)
			}
		})
	}

	// Unscoped sees both clusters' topics.
	r := runTool(t, tool, `{}`)
	if d := r.Data.(workerInfoData); len(d.InternalTopics) != 6 || d.TopicsNotShown != 0 {
		t.Errorf("unscoped topics = %+v, not shown = %d", d.InternalTopics, d.TopicsNotShown)
	}

	// CONNECT_INTERNAL_TOPICS is the operator's word: shown as named.
	named := workerInfoTool(t, Deps{Kafka: fk, Connect: &fakeConnect{}, ConnectInternalTopics: []string{"_connect_configs_staging"}})
	fk.topics = func(names []string) ([]kafka.InternalTopic, error) {
		return []kafka.InternalTopic{{Name: names[0], Partitions: 1, ReplicationFactor: 1, CleanupPolicy: "compact", Brokers: 1}}, nil
	}
	r, err := named.Run(ordersCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "config _connect_configs_staging: 1 partitions") || strings.Contains(r.Summary, "internal topic not shown") {
		t.Errorf("configured topics scoped away:\n%s", r.Summary)
	}
}

func TestWorkerInfoTopicsHiddenWhenUntied(t *testing.T) {
	// Groups unreadable: no topic can be tied to the room, so all are a count.
	fk := devStackTwoClusters()
	fk.groupsErr = errors.New("broker down")
	tool := workerInfoTool(t, Deps{Kafka: fk, Connect: &fakeConnect{}})
	r, err := tool.Run(ordersCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"6 other Connect internal topics not shown", "no Connect internal topic is tied to this room's worker group by name; set CONNECT_INTERNAL_TOPICS"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	if strings.Contains(r.Summary, "_connect_") {
		t.Errorf("topic names shown without a tie:\n%s", r.Summary)
	}

	// A stopped cluster's topics (no group of that name) stay hidden.
	fk.groupsErr = nil
	fk.groups = fk.groups[:1]
	fk.topics = func([]string) ([]kafka.InternalTopic, error) {
		return []kafka.InternalTopic{{Name: "_connect_configs", Role: "config", Partitions: 1}, {Name: "legacy-billing-configs", Role: "config", Partitions: 1}}, nil
	}
	r, err = tool.Run(ordersCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "config _connect_configs:") || strings.Contains(r.Summary, "legacy-billing") || !strings.Contains(r.Summary, "1 other Connect internal topic not shown") {
		t.Errorf("summary:\n%s", r.Summary)
	}
}

func TestWorkerInfoUnscopedShowsAll(t *testing.T) {
	fk := &fakeWorkerKafka{fakeKafka: &fakeKafka{}, groups: twoGroups(), topics: healthyTopics}
	tool := workerInfoTool(t, Deps{Kafka: fk, Connect: &fakeConnect{}})
	r := runTool(t, tool, `{}`)
	if !strings.Contains(r.Summary, "connect-analytics") || !strings.Contains(r.Summary, "secret-billing-sink") || strings.Contains(r.Summary, "not shown (not part") {
		t.Errorf("unscoped summary:\n%s", r.Summary)
	}
	// A Connect client with neither plugin nor logger reader says so.
	for _, want := range []string{"this Connect client does not list plugins", "this Connect client does not read GET /admin/loggers"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
}

func TestWorkerInfoGroupAttribution(t *testing.T) {
	idle := []kafka.ConnectWorkerGroup{{GroupID: "connect-x", State: "PreparingRebalance", Workers: []kafka.ConnectWorker{{MemberID: "m1", Host: "h1"}}}}
	fk := &fakeWorkerKafka{fakeKafka: &fakeKafka{}, groups: idle, topics: healthyTopics}
	tool := workerInfoTool(t, Deps{Kafka: fk, Connect: &fakeConnect{}})
	r, err := tool.Run(ordersCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "group connect-x (PreparingRebalance): 1 worker; only Connect group in the cluster") {
		t.Errorf("single group not attributed:\n%s", r.Summary)
	}

	// Two groups, neither running the pipeline's connectors: none shown.
	fk.groups = append(idle, kafka.ConnectWorkerGroup{GroupID: "connect-y", State: "Stable"})
	r, err = tool.Run(ordersCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "No Connect worker group runs this pipeline's connectors right now") || !strings.Contains(r.Summary, "2 other Connect worker groups not shown") {
		t.Errorf("summary:\n%s", r.Summary)
	}

	// A group id the pipeline declares is the room's.
	ctx := tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders-connector"}, ConsumerGroups: []string{"connect-y"}})
	r, err = tool.Run(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "group connect-y (Stable): 0 workers; declared group") {
		t.Errorf("declared group not shown:\n%s", r.Summary)
	}

	// No connect group at all.
	fk.groups = []kafka.ConnectWorkerGroup{}
	r, _ = tool.Run(ordersCtx(), json.RawMessage(`{}`))
	if !strings.Contains(r.Summary, "No Connect worker group (protocol type connect) on this Kafka cluster") {
		t.Errorf("summary:\n%s", r.Summary)
	}
}

func TestWorkerInfoFindings(t *testing.T) {
	fk := &fakeWorkerKafka{fakeKafka: &fakeKafka{}, groups: twoGroups(), topics: func([]string) ([]kafka.InternalTopic, error) {
		return []kafka.InternalTopic{
			{Name: "connect-configs", Role: "config", Partitions: 3, ReplicationFactor: 1, CleanupPolicy: "compact", Brokers: 3},
			{Name: "connect-offsets", Role: "offset", Partitions: 25, ReplicationFactor: 3, CleanupPolicy: "delete", Brokers: 3},
			{Name: "connect-status", Role: "status", Partitions: 5, ReplicationFactor: 1, CleanupPolicy: "compact,delete", Brokers: 1},
		}, nil
	}}
	tool := workerInfoTool(t, Deps{Kafka: fk, Connect: &fakeConnect{}})
	r := runTool(t, tool, `{}`)
	data := r.Data.(workerInfoData)
	want := []string{
		"config topic connect-configs has 3 partitions; Connect requires exactly 1, or workers can read connector configs out of order",
		"connect-configs has replication factor 1 on a 3-broker cluster; losing one broker loses it",
		"connect-offsets has cleanup.policy=delete; Connect's internal topics must be compacted, or configs, offsets and status are deleted after retention",
	}
	if !slices.Equal(data.Findings, want) {
		t.Errorf("findings = %q\nwant %q", data.Findings, want)
	}
	for _, f := range want {
		if !strings.Contains(r.Summary, f) {
			t.Errorf("summary lacks %q", f)
		}
	}
}

func TestWorkerInfoConfiguredTopics(t *testing.T) {
	fk := &fakeWorkerKafka{fakeKafka: &fakeKafka{}, groups: twoGroups(), topics: func(names []string) ([]kafka.InternalTopic, error) {
		out := []kafka.InternalTopic{}
		for _, n := range names {
			it := kafka.InternalTopic{Name: n, Partitions: 1, ReplicationFactor: 1, CleanupPolicy: "compact", Brokers: 1}
			if n == "cfg-missing" {
				it = kafka.InternalTopic{Name: n, Missing: true, Brokers: 1}
			}
			out = append(out, it)
		}
		return out, nil
	}}
	d := Deps{Kafka: fk, Connect: &fakeConnect{}, ConnectInternalTopics: ParseInternalTopics(" store_a , store_b,,cfg-missing ")}
	r := runTool(t, workerInfoTool(t, d), `{}`)
	if !slices.Equal(fk.gotNames, []string{"store_a", "store_b", "cfg-missing"}) {
		t.Fatalf("names = %q", fk.gotNames)
	}
	for _, want := range []string{
		"Internal topics (CONNECT_INTERNAL_TOPICS):",
		"- config store_a: 1 partitions",
		"- offset store_b: 1 partitions",
		"- status cfg-missing: missing",
		"cfg-missing (from CONNECT_INTERNAL_TOPICS) does not exist on this cluster",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	// One broker: replication factor 1 is no finding.
	if strings.Contains(r.Summary, "replication factor 1") {
		t.Errorf("RF 1 flagged on a one-broker cluster:\n%s", r.Summary)
	}
}

func TestWorkerInfoPartialFailures(t *testing.T) {
	down := errors.New("broker down")
	fk := &fakeWorkerKafka{fakeKafka: &fakeKafka{}, groupsErr: down, topics: healthyTopics}
	fc := &fakeWorkerConnect{
		fakePluginsConnect: newPluginsFake(t, nil),
		loggersFn: func(context.Context) ([]connect.LoggerLevel, error) {
			return nil, clients.ErrNotSupported
		},
	}
	fc.pluginsFn = func(context.Context) ([]connect.Plugin, error) { return nil, errors.New("connect down\n- forged line") }
	tool := workerInfoTool(t, Deps{Kafka: fk, Connect: fc})
	r := runTool(t, tool, `{}`)
	for _, want := range []string{
		"could not list Connect worker groups: broker down",
		"config connect-prod-configs",
		"could not list plugins: connect down - forged line",
		"Log levels: this Connect has no GET /admin/loggers (Kafka Connect 2.4+)",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	if strings.Contains(r.Summary, "\n- forged line") {
		t.Errorf("an error forged a summary line:\n%s", r.Summary)
	}

	// Topics fail, groups work: still an answer.
	fk.groupsErr = nil
	fk.groups = twoGroups()
	fk.topics = func([]string) ([]kafka.InternalTopic, error) { return nil, down }
	fc.loggersFn = func(context.Context) ([]connect.LoggerLevel, error) { return nil, errors.New("500") }
	r = runTool(t, tool, `{}`)
	if !strings.Contains(r.Summary, "could not read the internal topics: broker down") || !strings.Contains(r.Summary, "could not read log levels: 500") {
		t.Errorf("summary:\n%s", r.Summary)
	}

	// Both Kafka reads fail: an error.
	fk.groupsErr = down
	if _, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`)); !errors.Is(err, down) {
		t.Errorf("err = %v, want both failures", err)
	}
}

func TestWorkerInfoNeedsScopeAndReader(t *testing.T) {
	fk := &fakeWorkerKafka{fakeKafka: &fakeKafka{}, groups: twoGroups(), topics: healthyTopics}
	tool := workerInfoTool(t, Deps{Kafka: fk, Connect: &fakeConnect{}})
	if _, err := tool.Run(context.Background(), json.RawMessage(`{}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Errorf("no scope: err = %v", err)
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: &fakeKafka{}, Connect: &fakeConnect{}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("connect_worker_info"); ok {
		t.Error("registered for a Kafka client without ConnectClusterReader")
	}
}

func TestWorkerInfoLoggerCap(t *testing.T) {
	var ls []connect.LoggerLevel
	ls = append(ls, connect.LoggerLevel{Logger: "root", Level: "WARN"})
	for i := range 15 {
		ls = append(ls, connect.LoggerLevel{Logger: "l" + strings.Repeat("a", i), Level: "TRACE"})
	}
	line := loggersLine(workerInfoData{Loggers: func() []workerInfoLogger {
		var out []workerInfoLogger
		for _, l := range ls {
			out = append(out, workerInfoLogger{Logger: l.Logger, Level: l.Level})
		}
		return out
	}()})
	if !strings.HasPrefix(line, "Log levels: root WARN; TRACE l") || !strings.HasSuffix(line, ", 5 more") {
		t.Errorf("line = %q", line)
	}
}
