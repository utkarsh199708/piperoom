package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hamba/avro/v2"

	"piperoom/internal/clients"
)

const timeout = 10 * time.Second

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

// Deps holds the client interfaces the actions write through.
type Deps struct {
	Connect  clients.ConnectClient
	Registry clients.SchemaRegistryClient
}

// Register adds the Kafka Connect and Schema Registry write actions to reg.
func Register(reg *Registry, d Deps) error {
	for _, a := range []Action{
		&restartTask{d: d},
		&restartConnector{d: d},
		&setCompatibility{d: d},
		&registerSchema{d: d},
	} {
		if err := reg.Register(a); err != nil {
			return err
		}
	}
	return nil
}

// connectorExists checks a connector exists (read-only, for validation).
func (d Deps) connectorExists(ctx context.Context, name string) error {
	names, err := d.Connect.ListConnectors(ctx)
	if err != nil {
		return err
	}
	for _, n := range names {
		if n == name {
			return nil
		}
	}
	return fmt.Errorf("connector %q not found", name)
}

// subjectExists checks a subject exists (read-only, for validation).
func (d Deps) subjectExists(ctx context.Context, subject string) error {
	subjects, err := d.Registry.Subjects(ctx)
	if err != nil {
		return err
	}
	for _, s := range subjects {
		if s == subject {
			return nil
		}
	}
	return fmt.Errorf("subject %q not found", subject)
}

// ---- connect_restart_task ----

type restartTask struct{ d Deps }

func (a *restartTask) Name() string  { return "connect_restart_task" }
func (a *restartTask) Layer() string { return "kafka-connect" }
func (a *restartTask) Describe() string {
	return "Restarts one task of a Kafka Connect connector."
}
func (a *restartTask) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{
		"connector": strProp("connector name"),
		"task":      map[string]any{"type": "integer", "description": "task id"},
	}, "connector", "task")
}

type restartTaskParams struct {
	Connector string `json:"connector"`
	Task      int    `json:"task"`
}

func (a *restartTask) Validate(raw json.RawMessage) error {
	var p restartTaskParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" || p.Task < 0 {
		return fmt.Errorf("connector and non-negative task are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.d.connectorExists(ctx, p.Connector)
}

func (a *restartTask) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p restartTaskParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := a.d.Connect.RestartTask(ctx, p.Connector, p.Task); err != nil {
		return "", err
	}
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return "", err
	}
	state := "UNKNOWN"
	for _, t := range st.Tasks {
		if t.ID == p.Task {
			state = t.State
		}
	}
	return fmt.Sprintf("restarted %s task %d (state now %s)", p.Connector, p.Task, state), nil
}

// ---- connect_restart_connector ----

type restartConnector struct{ d Deps }

func (a *restartConnector) Name() string  { return "connect_restart_connector" }
func (a *restartConnector) Layer() string { return "kafka-connect" }
func (a *restartConnector) Describe() string {
	return "Restarts only the failed tasks of a Kafka Connect connector."
}
func (a *restartConnector) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}

type restartConnectorParams struct {
	Connector string `json:"connector"`
}

func (a *restartConnector) Validate(raw json.RawMessage) error {
	var p restartConnectorParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Connector == "" {
		return fmt.Errorf("connector is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.d.connectorExists(ctx, p.Connector)
}

func (a *restartConnector) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p restartConnectorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := a.d.Connect.RestartConnector(ctx, p.Connector); err != nil {
		return "", err
	}
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("restarted failed tasks of %s (connector state %s)", p.Connector, st.State), nil
}

// ---- schema_set_compatibility ----

var compatLevels = map[string]bool{
	"BACKWARD": true, "BACKWARD_TRANSITIVE": true, "FORWARD": true,
	"FORWARD_TRANSITIVE": true, "FULL": true, "FULL_TRANSITIVE": true, "NONE": true,
}

type setCompatibility struct{ d Deps }

func (a *setCompatibility) Name() string  { return "schema_set_compatibility" }
func (a *setCompatibility) Layer() string { return "schema-registry" }
func (a *setCompatibility) Describe() string {
	return "Sets a schema registry subject's compatibility level."
}
func (a *setCompatibility) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{
		"subject": strProp("subject name"),
		"level":   strProp("BACKWARD | BACKWARD_TRANSITIVE | FORWARD | FORWARD_TRANSITIVE | FULL | FULL_TRANSITIVE | NONE"),
	}, "subject", "level")
}

type setCompatibilityParams struct {
	Subject string `json:"subject"`
	Level   string `json:"level"`
}

func (a *setCompatibility) Validate(raw json.RawMessage) error {
	var p setCompatibilityParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Subject == "" {
		return fmt.Errorf("subject is required")
	}
	if !compatLevels[p.Level] {
		return fmt.Errorf("level %q is not a valid compatibility level", p.Level)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.d.subjectExists(ctx, p.Subject)
}

func (a *setCompatibility) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p setCompatibilityParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := a.d.Registry.SetCompatibility(ctx, p.Subject, p.Level); err != nil {
		return "", err
	}
	level, _, err := a.d.Registry.Compatibility(ctx, p.Subject)
	if err != nil {
		return fmt.Sprintf("%s compatibility now %s (set; re-read failed: %v)", p.Subject, p.Level, err), nil
	}
	return fmt.Sprintf("%s compatibility now %s", p.Subject, level), nil
}

// ---- schema_register ----

type registerSchema struct{ d Deps }

func (a *registerSchema) Name() string  { return "schema_register" }
func (a *registerSchema) Layer() string { return "schema-registry" }
func (a *registerSchema) Describe() string {
	return "Registers a new Avro schema under a subject."
}
func (a *registerSchema) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{
		"subject": strProp("subject name"),
		"schema":  strProp("Avro schema JSON string"),
	}, "subject", "schema")
}

type registerSchemaParams struct {
	Subject string `json:"subject"`
	Schema  string `json:"schema"`
}

func (a *registerSchema) Validate(raw json.RawMessage) error {
	var p registerSchemaParams
	if err := json.Unmarshal(raw, &p); err != nil || p.Subject == "" || p.Schema == "" {
		return fmt.Errorf("subject and schema are required")
	}
	if _, err := avro.Parse(p.Schema); err != nil {
		return fmt.Errorf("schema is not valid Avro: %v", err)
	}
	return nil
}

func (a *registerSchema) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p registerSchemaParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	id, err := a.d.Registry.Register(ctx, p.Subject, p.Schema)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("registered %s as schema id %d", p.Subject, id), nil
}
