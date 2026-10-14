package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeKafkaConfig serves DescribeTopicConfigs on top of the plain fakeKafka
// (which deliberately lacks the method): the local adapter the tool asserts.
type fakeKafkaConfig struct {
	*fakeKafka
	cfgFn func(context.Context, string) (kafka.TopicConfig, error)
}

func (f *fakeKafkaConfig) DescribeTopicConfigs(ctx context.Context, topic string) (kafka.TopicConfig, error) {
	return f.cfgFn(ctx, topic)
}

var (
	_ clients.KafkaAdmin = (*fakeKafkaConfig)(nil)
	_ topicConfigAdmin   = (*fakeKafkaConfig)(nil)
)

// topicConfigOwners mirrors dev/owners.yaml: one single-table pipeline and one
// with two source tables (where the table parameter is required).
func topicConfigOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}},
		{ID: "multi", TopicPrefix: "billing", SourceTables: []string{"public.orders", "public.payments"}},
	}}
}

// echoTopicConfig returns the fixed configs for whatever topic is asked, so
// the test checks the topic the tool resolved.
func echoTopicConfig() *fakeKafkaConfig {
	return &fakeKafkaConfig{
		fakeKafka: &fakeKafka{},
		cfgFn: func(_ context.Context, topic string) (kafka.TopicConfig, error) {
			return kafka.TopicConfig{
				Topic: topic, Partitions: 3, ReplicationFactor: 1,
				RetentionMS: "259200000", RetentionBytes: "-1",
				CleanupPolicy: "delete", MinInsyncReplicas: "2",
			}, nil
		},
	}
}

func TestKafkaTopicConfig(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: echoTopicConfig(), Owners: topicConfigOwners()}); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get("kafka_topic_config")
	if !ok {
		t.Fatal("kafka_topic_config not registered")
	}
	if tool.Kind() != tools.KindRead {
		t.Fatalf("kind = %s, want read", tool.Kind())
	}

	tests := []struct {
		name      string
		input     string
		wantTopic string
	}{
		{"single source table defaults", `{"pipeline_id":"orders"}`, "pg.public.orders"},
		{"explicit table", `{"pipeline_id":"orders","table":"public.orders"}`, "pg.public.orders"},
		{"multi-table pipeline picks the named table", `{"pipeline_id":"multi","table":"public.payments"}`, "billing.public.payments"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := runTool(t, tool, tt.input)
			want := kafka.TopicConfig{
				Topic: tt.wantTopic, Partitions: 3, ReplicationFactor: 1,
				RetentionMS: "259200000", RetentionBytes: "-1",
				CleanupPolicy: "delete", MinInsyncReplicas: "2",
			}
			got, ok := r.Data.(kafka.TopicConfig)
			if !ok {
				t.Fatalf("data type %T, want kafka.TopicConfig", r.Data)
			}
			if got != want {
				t.Fatalf("data = %+v, want %+v", got, want)
			}
			for _, want := range []string{tt.wantTopic, "retention.ms=259200000", "retention.bytes=-1", "cleanup.policy=delete", "min.insync.replicas=2", "3 partitions", "replication factor 1"} {
				if !strings.Contains(r.Summary, want) {
					t.Fatalf("summary missing %q:\n%s", want, r.Summary)
				}
			}
		})
	}
}

func TestKafkaTopicConfigErrors(t *testing.T) {
	notFoundKafka := &fakeKafkaConfig{
		fakeKafka: &fakeKafka{},
		cfgFn: func(_ context.Context, topic string) (kafka.TopicConfig, error) {
			return kafka.TopicConfig{}, clients.ErrNotFound
		},
	}
	tests := []struct {
		name     string
		deps     Deps
		input    string
		wantKind error  // nil means "not this sentinel"; see wantContains
		wantText string // substring the error must carry
	}{
		{
			name:     "missing pipeline_id",
			deps:     Deps{Kafka: echoTopicConfig(), Owners: topicConfigOwners()},
			input:    `{}`,
			wantKind: tools.ErrBadInput,
			wantText: "pipeline_id is required",
		},
		{
			name:     "unknown pipeline",
			deps:     Deps{Kafka: echoTopicConfig(), Owners: topicConfigOwners()},
			input:    `{"pipeline_id":"nope"}`,
			wantKind: tools.ErrBadInput,
			wantText: `pipeline "nope" not found`,
		},
		{
			name:     "pipeline without topic_prefix",
			deps:     Deps{Kafka: echoTopicConfig(), Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "noprefix", SourceTables: []string{"public.orders"}}}}},
			input:    `{"pipeline_id":"noprefix"}`,
			wantKind: tools.ErrBadInput,
			wantText: "no topic_prefix",
		},
		{
			name:     "table outside the pipeline",
			deps:     Deps{Kafka: echoTopicConfig(), Owners: topicConfigOwners()},
			input:    `{"pipeline_id":"orders","table":"public.payments"}`,
			wantKind: tools.ErrBadInput,
			wantText: "not a source table",
		},
		{
			name:     "ambiguous table",
			deps:     Deps{Kafka: echoTopicConfig(), Owners: topicConfigOwners()},
			input:    `{"pipeline_id":"multi"}`,
			wantKind: tools.ErrBadInput,
			wantText: "table is required",
		},
		{
			name:     "owners not configured",
			deps:     Deps{Kafka: echoTopicConfig()},
			input:    `{"pipeline_id":"orders"}`,
			wantText: "owners not configured",
		},
		{
			name:     "client does not support topic configs",
			deps:     Deps{Kafka: &fakeKafka{}, Owners: topicConfigOwners()},
			input:    `{"pipeline_id":"orders"}`,
			wantText: "does not support DescribeTopicConfigs",
		},
		{
			name:     "unknown topic on the broker",
			deps:     Deps{Kafka: notFoundKafka, Owners: topicConfigOwners()},
			input:    `{"pipeline_id":"orders"}`,
			wantKind: clients.ErrNotFound,
			wantText: "pg.public.orders",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := tools.NewRegistry()
			if err := Register(reg, tt.deps); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("kafka_topic_config")
			_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(tt.input))
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if tt.wantKind != nil && !errors.Is(err, tt.wantKind) {
				t.Fatalf("err = %v, want it to wrap %v", err, tt.wantKind)
			}
			if tt.wantKind == nil && (errors.Is(err, tools.ErrBadInput) || errors.Is(err, clients.ErrNotFound)) {
				t.Fatalf("err = %v, want neither ErrBadInput nor ErrNotFound", err)
			}
			if !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.wantText)
			}
		})
	}
}

// TestKafkaTopicConfigBadJSON covers malformed input, which must surface as
// ErrBadInput, not an internal error.
func TestKafkaTopicConfigBadJSON(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: echoTopicConfig(), Owners: topicConfigOwners()}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("kafka_topic_config")
	_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"pipeline_id":`))
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("err = %v, want ErrBadInput", err)
	}
}
