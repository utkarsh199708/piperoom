package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeWorkerKafka is a KafkaAdmin that also implements the optional Q3a
// ConnectClusterReader and BrokerCounter.
type fakeWorkerKafka struct {
	*fakeKafka
	groupsFn    func(context.Context) ([]kafka.ConnectWorkerGroup, error)
	topicsFn    func(context.Context, []string) ([]kafka.InternalTopic, error)
	brokersFn   func(context.Context) (int, error)
	requested   []string
	groupCalls  int
	topicCalls  int
	brokerCalls int
}

func (f *fakeWorkerKafka) ConnectWorkerGroups(ctx context.Context) ([]kafka.ConnectWorkerGroup, error) {
	f.groupCalls++
	return f.groupsFn(ctx)
}

func (f *fakeWorkerKafka) InternalTopics(ctx context.Context, names []string) ([]kafka.InternalTopic, error) {
	f.topicCalls++
	f.requested = append([]string(nil), names...)
	return f.topicsFn(ctx, names)
}

func (f *fakeWorkerKafka) BrokerCount(ctx context.Context) (int, error) {
	f.brokerCalls++
	if f.brokersFn == nil {
		return 0, errors.New("fakeWorkerKafka: broker count not configured")
	}
	return f.brokersFn(ctx)
}

// fakeWorkerConnect is a Connect client with the optional Q3a inventory reads.
type fakeWorkerConnect struct {
	*fakeConnect
	pluginsFn func(context.Context) ([]connect.Plugin, error)
	levelsFn  func(context.Context) (map[string]string, error)
}

func (f *fakeWorkerConnect) Plugins(ctx context.Context) ([]connect.Plugin, error) {
	return f.pluginsFn(ctx)
}

func (f *fakeWorkerConnect) LoggerLevels(ctx context.Context) (map[string]string, error) {
	return f.levelsFn(ctx)
}

func workerGroups() []kafka.ConnectWorkerGroup {
	return []kafka.ConnectWorkerGroup{
		{GroupID: "connect-cluster", State: "Stable", Workers: []kafka.ConnectWorker{
			{MemberID: "worker-2", Host: "10.0.0.2", URL: "http://10.0.0.2:8083"},
			{MemberID: "worker-1", Host: "10.0.0.1", URL: "http://10.0.0.1:8083"},
		}},
		{GroupID: "connect-billing", State: "Empty"},
	}
}

// workerTopics answers every requested name with a plausible shape: the
// config topic compact with one partition, the others compact with more.
func workerTopics(_ context.Context, names []string) ([]kafka.InternalTopic, error) {
	out := make([]kafka.InternalTopic, 0, len(names))
	for _, name := range names {
		it := kafka.InternalTopic{Name: name, ReplicationFactor: 3, CleanupPolicy: "compact", MinInSyncReplicas: 2}
		switch {
		case strings.HasSuffix(name, "-configs"):
			it.Role, it.Partitions = "config", 1
		case strings.HasSuffix(name, "-offsets"):
			it.Role, it.Partitions = "offset", 25
		case strings.HasSuffix(name, "-status"):
			it.Role, it.Partitions = "status", 5
		default:
			it.Partitions = 1
		}
		out = append(out, it)
	}
	return out, nil
}

func workerKafka(groups []kafka.ConnectWorkerGroup, topics func(context.Context, []string) ([]kafka.InternalTopic, error)) *fakeWorkerKafka {
	return &fakeWorkerKafka{
		fakeKafka: &fakeKafka{},
		groupsFn:  func(context.Context) ([]kafka.ConnectWorkerGroup, error) { return groups, nil },
		topicsFn:  topics,
		brokersFn: func(context.Context) (int, error) { return 3, nil },
	}
}

func registerWorkerInfo(t *testing.T, d Deps) (tools.Tool, *fakeWorkerKafka) {
	t.Helper()
	fk, ok := d.Kafka.(*fakeWorkerKafka)
	if !ok {
		t.Fatalf("Kafka is %T, want *fakeWorkerKafka", d.Kafka)
	}
	if d.Connect == nil {
		d.Connect = &fakeConnect{}
	}
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get("connect_worker_info")
	if !ok {
		t.Fatalf("connect_worker_info not registered for %T", d.Kafka)
	}
	return tool, fk
}

func workerInventoryConnect() *fakeWorkerConnect {
	return &fakeWorkerConnect{
		fakeConnect: &fakeConnect{},
		pluginsFn: func(context.Context) ([]connect.Plugin, error) {
			return []connect.Plugin{
				{Class: "io.debezium.connector.postgresql.PostgresConnector", Type: "source", Version: "3.0.0.Final"},
				{Class: "io.confluent.connect.avro.AvroConverter", Type: "converter", Version: "7.6.1"},
			}, nil
		},
		levelsFn: func(context.Context) (map[string]string, error) {
			return map[string]string{
				"org.apache.kafka.connect.runtime.Worker":   "INFO",
				"io.debezium.connector.postgresql.Postgres": "DEBUG",
				"root": "WARN",
			}, nil
		},
	}
}

// unsetInternalTopics clears CONNECT_INTERNAL_TOPICS so a suffix test does not
// depend on the developer's environment.
func unsetInternalTopics(t *testing.T) {
	t.Helper()
	t.Setenv(EnvConnectInternalTopics, "")
}

func TestConnectWorkerInfoReportsGroupsTopicsAndFlags(t *testing.T) {
	unsetInternalTopics(t)
	fk := workerKafka(workerGroups(), workerTopics)
	tool, _ := registerWorkerInfo(t, Deps{Kafka: fk, Connect: workerInventoryConnect()})

	r := runTool(t, tool, `{}`)
	for _, want := range []string{
		"2 Connect worker groups",
		"connect-cluster: Stable, 2 workers:",
		"worker-1 (10.0.0.1) http://10.0.0.1:8083",
		"worker-2 (10.0.0.2) http://10.0.0.2:8083",
		"connect-billing: Empty, no workers",
		"internal topics: 6 of 6 found",
		"connect-billing-configs (config): 1 partitions, RF 3, cleanup.policy compact, min.insync.replicas 2",
		"flags: none",
		"plugins: 2 (1 source, 1 converter)",
		"logger levels: 3 loggers (1 DEBUG, 1 INFO, 1 WARN)",
		"not INFO: io.debezium.connector.postgresql.Postgres=DEBUG, root=WARN",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, r.Summary)
		}
	}
	// The six requested names are each group's three default suffixes, sorted.
	wantReq := []string{
		"connect-billing-configs", "connect-billing-offsets", "connect-billing-status",
		"connect-cluster-configs", "connect-cluster-offsets", "connect-cluster-status",
	}
	if strings.Join(fk.requested, ",") != strings.Join(wantReq, ",") {
		t.Fatalf("requested %v, want %v", fk.requested, wantReq)
	}
	data, ok := r.Data.(connectWorkerInfoData)
	if !ok {
		t.Fatalf("data %T, want connectWorkerInfoData", r.Data)
	}
	if data.GroupCount != 2 || len(data.Groups) != 2 || len(data.InternalTopics) != 6 {
		t.Fatalf("data groups/topics: %+v", data)
	}
	if len(data.Flags) != 0 || data.BrokerCount != 0 {
		t.Fatalf("unexpected flags or broker read: flags=%v brokers=%d", data.Flags, data.BrokerCount)
	}
	if fk.brokerCalls != 0 {
		t.Fatalf("broker count read %d times without an RF-1 topic", fk.brokerCalls)
	}
	if data.PluginCount != 2 || data.PluginsByType["source"] != 1 || data.LoggerCount != 3 {
		t.Fatalf("plugin/logger data: %+v", data)
	}
	if r.Truncated {
		t.Fatal("uncapped result marked truncated")
	}
}

func TestConnectWorkerInfoUsesConfiguredTopics(t *testing.T) {
	unsetInternalTopics(t)
	fk := workerKafka(workerGroups(), workerTopics)
	tool, _ := registerWorkerInfo(t, Deps{
		Kafka: fk, Connect: workerInventoryConnect(),
		ConnectInternalTopics: []string{"custom-configs", "not a topic", "custom-offsets"},
	})
	r := runTool(t, tool, `{}`)
	if strings.Join(fk.requested, ",") != "custom-configs,custom-offsets" {
		t.Fatalf("requested %v, want the configured names in order", fk.requested)
	}
	if !strings.Contains(r.Summary, "1 configured topic names ignored") {
		t.Fatalf("ignored config name not reported:\n%s", r.Summary)
	}
	// The role still comes from the suffix, so the configured config topic is
	// labelled as one even though it is not a default name.
	if !strings.Contains(r.Summary, "- custom-configs (config):") {
		t.Fatalf("summary %q", r.Summary)
	}
}

func TestConnectWorkerInfoReadsEnvTopics(t *testing.T) {
	t.Setenv(EnvConnectInternalTopics, " env-configs , env-1-offsets ,,")
	fk := workerKafka(workerGroups(), workerTopics)
	tool, _ := registerWorkerInfo(t, Deps{Kafka: fk, Connect: workerInventoryConnect()})
	runTool(t, tool, `{}`)
	if strings.Join(fk.requested, ",") != "env-configs,env-1-offsets" {
		t.Fatalf("requested %v, want the environment list", fk.requested)
	}
}

func TestConnectWorkerInfoNoGroups(t *testing.T) {
	unsetInternalTopics(t)
	fk := workerKafka(nil, func(context.Context, []string) ([]kafka.InternalTopic, error) {
		t.Fatal("InternalTopics called with no group and no configured list")
		return nil, nil
	})
	tool, _ := registerWorkerInfo(t, Deps{Kafka: fk, Connect: workerInventoryConnect()})
	r := runTool(t, tool, `{}`)
	for _, want := range []string{
		"no Connect worker groups found",
		"internal topics: none (no connect group names one and CONNECT_INTERNAL_TOPICS is not set)",
		"flags: none",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, r.Summary)
		}
	}
	if fk.topicCalls != 0 {
		t.Fatalf("InternalTopics called %d times", fk.topicCalls)
	}
}

func TestConnectWorkerInfoFlags(t *testing.T) {
	unsetInternalTopics(t)
	tests := []struct {
		name     string
		topic    kafka.InternalTopic
		brokers  int
		wantFlag string // "" = no flag
		absent   string
	}{
		{
			name:     "config topic with two partitions",
			topic:    kafka.InternalTopic{Name: "c-configs", Role: "config", Partitions: 2, ReplicationFactor: 3, CleanupPolicy: "compact"},
			brokers:  3,
			wantFlag: "c-configs (config) has 2 partitions; a Connect config topic must have exactly 1",
		},
		{
			name:    "config topic with one partition",
			topic:   kafka.InternalTopic{Name: "c-configs", Role: "config", Partitions: 1, ReplicationFactor: 3, CleanupPolicy: "compact"},
			brokers: 3,
			absent:  "partitions; a Connect config topic",
		},
		{
			name:     "offsets topic with delete",
			topic:    kafka.InternalTopic{Name: "c-offsets", Role: "offset", Partitions: 25, ReplicationFactor: 3, CleanupPolicy: "delete"},
			brokers:  3,
			wantFlag: `c-offsets (offset) cleanup.policy is "delete"; Connect internal topics need compact`,
		},
		{
			name:    "compact and delete still compacts",
			topic:   kafka.InternalTopic{Name: "c-offsets", Role: "offset", Partitions: 25, ReplicationFactor: 3, CleanupPolicy: "compact,delete"},
			brokers: 3,
			absent:  "cleanup.policy is",
		},
		{
			name:     "cleanup policy not reported",
			topic:    kafka.InternalTopic{Name: "c-status", Role: "status", Partitions: 5, ReplicationFactor: 3},
			brokers:  3,
			wantFlag: "c-status (status) reports no cleanup.policy; Connect internal topics need compact",
		},
		{
			name:     "replication factor 1 on three brokers",
			topic:    kafka.InternalTopic{Name: "c-status", Role: "status", Partitions: 5, ReplicationFactor: 1, CleanupPolicy: "compact"},
			brokers:  3,
			wantFlag: "c-status (status) has replication factor 1 on a 3-broker cluster",
		},
		{
			name:    "replication factor 1 on one broker",
			topic:   kafka.InternalTopic{Name: "c-status", Role: "status", Partitions: 5, ReplicationFactor: 1, CleanupPolicy: "compact"},
			brokers: 1,
			absent:  "replication factor 1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fk := workerKafka(workerGroups(), func(context.Context, []string) ([]kafka.InternalTopic, error) {
				return []kafka.InternalTopic{tc.topic}, nil
			})
			fk.brokersFn = func(context.Context) (int, error) { return tc.brokers, nil }
			tool, _ := registerWorkerInfo(t, Deps{
				Kafka: fk, Connect: workerInventoryConnect(),
				ConnectInternalTopics: []string{tc.topic.Name},
			})
			r := runTool(t, tool, `{}`)
			if tc.wantFlag != "" && !strings.Contains(r.Summary, tc.wantFlag) {
				t.Fatalf("summary missing flag %q:\n%s", tc.wantFlag, r.Summary)
			}
			if tc.wantFlag == "" && strings.Contains(r.Summary, "flags:\n") {
				t.Fatalf("unexpected flag:\n%s", r.Summary)
			}
			if tc.absent != "" && strings.Contains(r.Summary, tc.absent) {
				t.Fatalf("summary carries %q unexpectedly:\n%s", tc.absent, r.Summary)
			}
		})
	}
}

// A requested topic the cluster does not have is named, never quietly left
// out: a missing internal topic means Connect cannot start.
func TestConnectWorkerInfoMissingTopic(t *testing.T) {
	unsetInternalTopics(t)
	fk := workerKafka(workerGroups(), func(context.Context, []string) ([]kafka.InternalTopic, error) {
		return nil, nil
	})
	tool, _ := registerWorkerInfo(t, Deps{
		Kafka: fk, Connect: workerInventoryConnect(),
		ConnectInternalTopics: []string{"gone-configs"},
	})
	r := runTool(t, tool, `{}`)
	if !strings.Contains(r.Summary, "internal topics: none of the 1 expected topics exist") {
		t.Fatalf("summary %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "missing internal topics: gone-configs") ||
		!strings.Contains(r.Summary, "gone-configs does not exist") {
		t.Fatalf("missing topic not flagged:\n%s", r.Summary)
	}
	data := r.Data.(connectWorkerInfoData)
	if len(data.MissingTopics) != 1 || data.MissingTopics[0] != "gone-configs" {
		t.Fatalf("missing topics %v", data.MissingTopics)
	}
}

func TestConnectWorkerInfoWithoutInventoryReaders(t *testing.T) {
	unsetInternalTopics(t)
	fk := workerKafka(workerGroups(), workerTopics)
	tool, _ := registerWorkerInfo(t, Deps{Kafka: fk, Connect: &fakeConnect{}})
	r := runTool(t, tool, `{}`)
	for _, want := range []string{
		"plugins: not available (this Connect client cannot list plugins)",
		"logger levels: not available (this Connect client cannot read /admin/loggers)",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, r.Summary)
		}
	}
}

func TestConnectWorkerInfoInventoryReadFailuresDegrade(t *testing.T) {
	unsetInternalTopics(t)
	fc := &fakeWorkerConnect{
		fakeConnect: &fakeConnect{},
		pluginsFn:   func(context.Context) ([]connect.Plugin, error) { return nil, errors.New("connect: 500") },
		levelsFn:    func(context.Context) (map[string]string, error) { return nil, errors.New("connect: 500") },
	}
	fk := workerKafka(workerGroups(), workerTopics)
	tool, _ := registerWorkerInfo(t, Deps{Kafka: fk, Connect: fc})
	r := runTool(t, tool, `{}`)
	for _, want := range []string{
		"plugins: unavailable (the worker's plugin endpoint did not answer)",
		"logger levels: unavailable (the worker's /admin/loggers did not answer)",
		"2 Connect worker groups",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, r.Summary)
		}
	}
}

func TestConnectWorkerInfoBrokerCountFailureDegrades(t *testing.T) {
	unsetInternalTopics(t)
	fk := workerKafka(nil, func(context.Context, []string) ([]kafka.InternalTopic, error) {
		return []kafka.InternalTopic{{Name: "c-status", Role: "status", Partitions: 5, ReplicationFactor: 1, CleanupPolicy: "compact"}}, nil
	})
	fk.brokersFn = func(context.Context) (int, error) { return 0, errors.New("kafka: broker metadata: EOF") }
	tool, _ := registerWorkerInfo(t, Deps{
		Kafka: fk, Connect: workerInventoryConnect(), ConnectInternalTopics: []string{"c-status"},
	})
	r := runTool(t, tool, `{}`)
	if !strings.Contains(r.Summary, "broker count unknown (the broker metadata read failed): the replication-factor-1 check is skipped") {
		t.Fatalf("summary %q", r.Summary)
	}
}

func TestConnectWorkerInfoPropagatesErrors(t *testing.T) {
	unsetInternalTopics(t)
	boom := errors.New("kafka: list connect groups: EOF")
	cases := []struct {
		name string
		fk   *fakeWorkerKafka
	}{
		{"groups", &fakeWorkerKafka{
			fakeKafka: &fakeKafka{},
			groupsFn:  func(context.Context) ([]kafka.ConnectWorkerGroup, error) { return nil, boom },
			topicsFn:  workerTopics,
		}},
		{"topics", &fakeWorkerKafka{
			fakeKafka: &fakeKafka{},
			groupsFn:  func(context.Context) ([]kafka.ConnectWorkerGroup, error) { return workerGroups(), nil },
			topicsFn:  func(context.Context, []string) ([]kafka.InternalTopic, error) { return nil, boom },
		}},
		{"deadline", &fakeWorkerKafka{
			fakeKafka: &fakeKafka{},
			groupsFn:  func(context.Context) ([]kafka.ConnectWorkerGroup, error) { return nil, context.DeadlineExceeded },
			topicsFn:  workerTopics,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tool, _ := registerWorkerInfo(t, Deps{Kafka: tc.fk, Connect: workerInventoryConnect()})
			_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
			want := boom
			if tc.name == "deadline" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) {
				t.Fatalf("err %v, want %v", err, want)
			}
		})
	}
}

func TestConnectWorkerInfoNotRegisteredWithoutReader(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: &fakeConnect{}, Kafka: &fakeKafka{}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("connect_worker_info"); ok {
		t.Fatal("tool registered for a Kafka client without the optional reader")
	}
}

func TestConnectWorkerInfoScope(t *testing.T) {
	unsetInternalTopics(t)
	fk := workerKafka(workerGroups(), workerTopics)
	tool, _ := registerWorkerInfo(t, Deps{Kafka: fk, Connect: workerInventoryConnect()})
	if _, err := tool.Run(context.Background(), json.RawMessage(`{}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no-room err %v, want ErrNoScope", err)
	}
	if fk.groupCalls != 0 {
		t.Fatalf("client reached without a room: %d calls", fk.groupCalls)
	}
	ctx := tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders-connector"}})
	if _, err := tool.Run(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("room call: %v", err)
	}
	if fk.groupCalls != 1 {
		t.Fatalf("room call read %d times, want 1", fk.groupCalls)
	}
}

// Every name in the answer is untrusted cluster text: a line break, an ANSI
// escape or a bidi override must not forge or reorder summary lines, and a
// huge group list is capped and says so.
func TestConnectWorkerInfoSanitizesAndCaps(t *testing.T) {
	unsetInternalTopics(t)
	groups := make([]kafka.ConnectWorkerGroup, maxWorkerGroupRows+10)
	for i := range groups {
		groups[i] = kafka.ConnectWorkerGroup{
			GroupID: fmt.Sprintf("connect-%03d", i),
			State:   "Stable\x1b[31m",
			Workers: []kafka.ConnectWorker{
				{MemberID: "w\nforged", Host: "10.0.0.1", URL: "http://10.0.0.1:8083\x1b\u202e"},
			},
		}
	}
	// A hostile group id is not a valid topic name, so it yields no internal
	// topic; it must still render sanitized as a group.
	groups[0].GroupID = "connect-000\nforged"
	fk := workerKafka(groups, func(context.Context, []string) ([]kafka.InternalTopic, error) { return nil, nil })
	fc := &fakeWorkerConnect{
		fakeConnect: &fakeConnect{},
		pluginsFn:   func(context.Context) ([]connect.Plugin, error) { return nil, nil },
		levelsFn: func(context.Context) (map[string]string, error) {
			return map[string]string{"logger\nforged": "INFO\u202e", "root": "DEBUG"}, nil
		},
	}
	tool, _ := registerWorkerInfo(t, Deps{Kafka: fk, Connect: fc})
	r := runTool(t, tool, `{}`)

	if len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary %d bytes", len(r.Summary))
	}
	for _, banned := range []string{"\x1b", "\u202e", "\nforged"} {
		if strings.Contains(r.Summary, banned) {
			t.Fatalf("summary carries %q:\n%s", banned, r.Summary)
		}
	}
	data := r.Data.(connectWorkerInfoData)
	if len(data.Groups) != maxWorkerGroupRows || data.GroupsOmitted != 10 {
		t.Fatalf("groups %d omitted %d, want %d/%d", len(data.Groups), data.GroupsOmitted, maxWorkerGroupRows, 10)
	}
	if data.GroupCount != len(groups) {
		t.Fatalf("group count %d, want %d", data.GroupCount, len(groups))
	}
	if data.TopicsNotRead == 0 {
		t.Fatal("the requested-topic cap did not leave a TopicsNotRead count")
	}
	if !r.Truncated {
		t.Fatal("capped result not marked truncated")
	}
	for _, row := range data.Groups {
		if strings.ContainsAny(row.GroupID, "\x1b\n\u202e") || strings.ContainsAny(row.State, "\x1b\n\u202e") {
			t.Fatalf("control characters kept in data: %+v", row)
		}
	}
}
