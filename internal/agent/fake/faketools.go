// Package fake provides read-only tools that replay the dev-stack
// "schema-change" scenario, so the agent loop and cmd/investigate can run
// without the real A2/A3 clients. Used by unit tests and the claude-tagged
// integration test.
package fake

import (
	"context"
	"encoding/json"
	"fmt"

	"piperoom/internal/tools"
)

// Register adds the schema-change replay tools to reg.
func Register(reg *tools.Registry) error {
	for _, t := range []tools.Tool{
		connectStatus{},
		registryVersions{},
		connectConfig{},
		consumerLag{},
	} {
		if err := reg.Register(t); err != nil {
			return err
		}
	}
	return nil
}

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

func parse(raw json.RawMessage, v any) error {
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	return nil
}

// connectStatus returns the connector/task state that the schema-change
// scenario produces: task 0 failed on a schema incompatibility.
type connectStatus struct{}

func (connectStatus) Name() string { return "connect_status" }
func (connectStatus) Kind() tools.Kind {
	return tools.KindRead
}
func (connectStatus) Description() string {
	return "Returns the state of a Kafka Connect connector and its tasks, including any failure trace."
}
func (connectStatus) InputSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}
func (connectStatus) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Connector string `json:"connector"`
	}
	if err := parse(input, &p); err != nil {
		return tools.Result{}, err
	}
	if p.Connector == "" {
		return tools.Result{}, fmt.Errorf("%w: connector is required", tools.ErrBadInput)
	}
	return tools.Result{
		Summary: fmt.Sprintf(`connector %s: state RUNNING, 1 task, task 0 FAILED
task 0 error: org.apache.kafka.connect.errors.ConnectException: Error registering Avro schema{...} — incompatible with the latest schema due to an incompatible type change. Caused by: org.apache.kafka.common.config.ConfigException: Failed to access Avro data ... MISSING_UNION_BRANCH ... oldSchemaVersion: 1 ... compatibility: 'BACKWARD' ... error code: 409
Rejected schema vs pg.public.orders-value v1: amount int -> bytes(decimal 12,2)`, p.Connector),
		Data: map[string]any{
			"connector": p.Connector,
			"state":     "RUNNING",
			"tasks": []map[string]any{
				{"id": 0, "state": "FAILED"},
			},
		},
	}, nil
}

// registryVersions returns the subject's registered versions.
type registryVersions struct{}

func (registryVersions) Name() string { return "schema_registry_versions" }
func (registryVersions) Kind() tools.Kind {
	return tools.KindRead
}
func (registryVersions) Description() string {
	return "Lists the schema versions registered under a subject."
}
func (registryVersions) InputSchema() map[string]any {
	return objectSchema(map[string]any{"subject": strProp("subject name")}, "subject")
}
func (registryVersions) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Subject string `json:"subject"`
	}
	if err := parse(input, &p); err != nil {
		return tools.Result{}, err
	}
	return tools.Result{
		Summary: fmt.Sprintf("subject %s: versions [1]", p.Subject),
		Data:    map[string]any{"subject": p.Subject, "versions": []int{1}},
	}, nil
}

// connectConfig returns the connector configuration (with secrets redacted).
type connectConfig struct{}

func (connectConfig) Name() string { return "connect_config" }
func (connectConfig) Kind() tools.Kind {
	return tools.KindRead
}
func (connectConfig) Description() string {
	return "Returns a connector's configuration, with passwords redacted."
}
func (connectConfig) InputSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}
func (connectConfig) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Connector string `json:"connector"`
	}
	if err := parse(input, &p); err != nil {
		return tools.Result{}, err
	}
	return tools.Result{
		Summary: fmt.Sprintf("connector %s config: connector.class=io.debezium.connector.postgresql.PostgresConnector, database.hostname=postgres, database.user=piperoom, database.password=***, topic.prefix=pg", p.Connector),
		Data: map[string]any{
			"connector": p.Connector,
			"config": map[string]string{
				"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
			},
		},
	}, nil
}

// consumerLag returns a consumer group's lag.
type consumerLag struct{}

func (consumerLag) Name() string { return "kafka_consumer_lag" }
func (consumerLag) Kind() tools.Kind {
	return tools.KindRead
}
func (consumerLag) Description() string {
	return "Returns the lag of a Kafka consumer group."
}
func (consumerLag) InputSchema() map[string]any {
	return objectSchema(map[string]any{"group": strProp("consumer group name")}, "group")
}
func (consumerLag) Run(_ context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Group string `json:"group"`
	}
	if err := parse(input, &p); err != nil {
		return tools.Result{}, err
	}
	return tools.Result{
		Summary: fmt.Sprintf("group %s: state Stable, members 1, total lag 0", p.Group),
		Data:    map[string]any{"group": p.Group, "total_lag": 0},
	}, nil
}
