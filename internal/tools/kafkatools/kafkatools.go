// Package kafkatools registers the read-only Kafka/Connect/Schema Registry
// investigation tools. All tools are tools.KindRead: they never change the
// systems they inspect.
package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
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
	// Owners resolves pipeline_id + source table to the pipeline's topic
	// ("<topic_prefix>.<source_table>") for kafka_topic_config. The wiring
	// must pass it; the tool reports a clear error when it is missing.
	Owners *owners.Owners
}

const timeout = 10 * time.Second

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
