// Package kafkatools registers the read-only Kafka/Connect/Schema Registry
// investigation tools. All tools are tools.KindRead: they never change the
// systems they inspect.
package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Deps holds the three client interfaces the tools read through (so tests use fakes).
type Deps struct {
	Connect  clients.ConnectClient
	Registry clients.SchemaRegistryClient
	Kafka    clients.KafkaAdmin
	// Owners resolves the room's pipeline id (tools.WithPipelineID) to its
	// connectors, topics, groups and tables, and pipeline_id + source table to
	// the pipeline's topic ("<topic_prefix>.<source_table>") for
	// kafka_topic_config. The wiring must pass it; every tool reports a clear
	// error when it is missing and the context carries only a pipeline id.
	Owners *owners.Owners
	// DLQStats is optional: how many records the watcher saw arrive in a DLQ
	// topic within its window. It is set only when the watcher runs in this
	// process (cmd/server); nil in cmd/investigate and the evals, where
	// dlq_peek then says the arrival rate is unknown.
	DLQStats func(topic string) (newInWindow int64, window time.Duration, ok bool)
}

const timeout = 10 * time.Second

// scope resolves the room's pipeline scope for one tool call (finding F11).
// A tool calls it first, and checks every name it got against the result
// before it uses a client.
func (d Deps) scope(ctx context.Context) (tools.Scope, error) {
	return tools.ScopeFor(ctx, d.Owners)
}

// Register adds every kafkatools tool to reg.
func Register(reg *tools.Registry, d Deps) error {
	for _, t := range []tools.Tool{
		&connectList{d: d},
		&connectStatus{d: d},
		&connectConfig{d: d},
		&registrySubjects{d: d},
		&registryVersions{d: d},
		&registryDiff{d: d},
		&topicOffsets{d: d},
		&listGroups{d: d},
		&consumerLag{d: d},
		&findByKey{d: d},
		&topicConfig{d: d},
		&dlqPeek{d: d},
	} {
		if err := reg.Register(t); err != nil {
			return err
		}
	}
	return nil
}

// withTimeout derives a timeout context from the caller's context so in-flight
// calls stop when the room closes or the agent stops.
func withTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

// parse decodes input into T, wrapping ErrBadInput on failure.
func parse[T any](raw json.RawMessage) (T, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	return v, nil
}

// appendLine adds line to sb on a line of its own; an empty line adds nothing.
func appendLine(sb *strings.Builder, line string) {
	if line == "" {
		return
	}
	if sb.Len() > 0 {
		sb.WriteString("\n")
	}
	sb.WriteString(line)
}

// res builds a Result with the summary truncated.
func res(summary string, data any) tools.Result {
	s, truncated := tools.Truncate(summary)
	return tools.Result{Summary: s, Data: data, Truncated: truncated}
}

// objectSchema builds a JSON Schema object with the given properties and required list.
func objectSchema(props map[string]any, required ...string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
		"required":             required,
	}
}

func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}
