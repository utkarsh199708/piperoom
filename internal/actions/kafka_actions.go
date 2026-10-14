package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hamba/avro/v2"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
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
		&resumeConnector{d: d},
		&updateConnectorConfig{d: d},
		&setCompatibility{d: d},
		&registerSchema{d: d},
	} {
		if err := reg.Register(a); err != nil {
			return err
		}
	}
	return nil
}

// Compile-time checks that every write action is scope-checked (F10).
var (
	_ Scoped = (*restartTask)(nil)
	_ Scoped = (*restartConnector)(nil)
	_ Scoped = (*resumeConnector)(nil)
	_ Scoped = (*setCompatibility)(nil)
	_ Scoped = (*registerSchema)(nil)
)

// inScopeConnector returns an error when name is not one of p.Connectors.
func inScopeConnector(p owners.Pipeline, name string) error {
	for _, c := range p.Connectors {
		if c == name {
			return nil
		}
	}
	return fmt.Errorf("actions: connector %q is not part of pipeline %q", name, p.ID)
}

// subjectTopic strips the "-key"/"-value" suffix from a Schema Registry
// subject, returning the topic it names.
func subjectTopic(subject string) (string, bool) {
	if strings.HasSuffix(subject, "-key") {
		return strings.TrimSuffix(subject, "-key"), true
	}
	if strings.HasSuffix(subject, "-value") {
		return strings.TrimSuffix(subject, "-value"), true
	}
	return "", false
}

// inScopeSubject returns an error when subject is not derived from one of
// p.SourceTables. A subject names "<topic>-key" or "<topic>-value", and the
// topic must be exactly "<p.TopicPrefix>.<sourceTable>": a suffix match would
// accept another pipeline's topic that merely ends with the same table name,
// e.g. "billing.public.orders-value" for the "pg" prefix.
func inScopeSubject(p owners.Pipeline, subject string) error {
	// owners.Load refuses an empty prefix, but a Pipeline built in code
	// (demo mode, tests) skips that check; with an empty prefix the rule
	// would accept any subject starting with a bare ".".
	if p.TopicPrefix == "" {
		return fmt.Errorf("actions: pipeline %q has no topic_prefix", p.ID)
	}
	topic, ok := subjectTopic(subject)
	if !ok {
		return fmt.Errorf("actions: subject %q is not a -key or -value subject", subject)
	}
	for _, st := range p.SourceTables {
		if topic == p.TopicPrefix+"."+st {
			return nil
		}
	}
	return fmt.Errorf("actions: subject %q is not part of pipeline %q", subject, p.ID)
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
	return fmt.Errorf("actions: connector %q not found", name)
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
	return fmt.Errorf("actions: subject %q not found", subject)
}

// pollEvery returns the poll interval: the test override when set, otherwise
// resumePollInterval.
func pollEvery(override time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	return resumePollInterval
}

// awaitRunning calls read until it reports RUNNING or ctx ends. read returns ""
// while the state is not known yet (for example a task id that is not in the
// status list yet), and the loop keeps polling. It returns the last state seen
// and whether RUNNING was reached. A read error is returned unless ctx has
// already ended, which is reported as a timeout instead.
func awaitRunning(ctx context.Context, interval time.Duration, read func(context.Context) (string, error)) (string, bool, error) {
	last := ""
	for {
		state, err := read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return last, false, nil
			}
			return last, false, err
		}
		if state != "" {
			last = state
		}
		if state == "RUNNING" {
			return state, true, nil
		}
		select {
		case <-ctx.Done():
			return last, false, nil
		case <-time.After(interval):
		}
	}
}

// taskState returns the state of task id in st, or "" when the status does
// not list it yet.
func taskState(st clients.ConnectorStatus, id int) string {
	for _, t := range st.Tasks {
		if t.ID == id {
			return t.State
		}
	}
	return ""
}

func orUnknown(state string) string {
	if state == "" {
		return "UNKNOWN"
	}
	return state
}

// ---- connect_restart_task ----

type restartTask struct {
	d Deps
	// pollInterval overrides resumePollInterval in tests; zero means the default.
	pollInterval time.Duration
}

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
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("actions: decode params: %w", err)
	}
	if p.Connector == "" || p.Task < 0 {
		return fmt.Errorf("actions: connector and non-negative task are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.d.connectorExists(ctx, p.Connector)
}

func (a *restartTask) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params restartTaskParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeConnector(p, params.Connector)
}

func (a *restartTask) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p restartTaskParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", fmt.Errorf("actions: decode params: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := a.d.Connect.RestartTask(ctx, p.Connector, p.Task); err != nil {
		return "", fmt.Errorf("actions: restart %s task %d: %w", p.Connector, p.Task, err)
	}
	// Connect applies a restart asynchronously. Once the restart call has
	// succeeded, every later problem is a non-error result: an error would tell
	// the agent the restart failed and invite a retry.
	state, running, err := awaitRunning(ctx, pollEvery(a.pollInterval), func(ctx context.Context) (string, error) {
		st, err := a.d.Connect.Status(ctx, p.Connector)
		if err != nil {
			return "", err
		}
		return taskState(st, p.Task), nil
	})
	switch {
	case err != nil:
		return fmt.Sprintf("restart accepted for %s task %d; status read failed: %v", p.Connector, p.Task, err), nil
	case running:
		return fmt.Sprintf("restarted %s task %d (RUNNING)", p.Connector, p.Task), nil
	}
	return fmt.Sprintf("restart accepted for %s task %d; state now %s (Connect applies restarts asynchronously)", p.Connector, p.Task, orUnknown(state)), nil
}

// ---- connect_restart_connector ----

type restartConnector struct {
	d Deps
	// pollInterval overrides resumePollInterval in tests; zero means the default.
	pollInterval time.Duration
}

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
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("actions: decode params: %w", err)
	}
	if p.Connector == "" {
		return fmt.Errorf("actions: connector is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.d.connectorExists(ctx, p.Connector)
}

func (a *restartConnector) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params restartConnectorParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeConnector(p, params.Connector)
}

func (a *restartConnector) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p restartConnectorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", fmt.Errorf("actions: decode params: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := a.d.Connect.RestartConnector(ctx, p.Connector); err != nil {
		return "", fmt.Errorf("actions: restart %s: %w", p.Connector, err)
	}
	// The call uses onlyFailed=true, so it restarts the connector and its
	// failed tasks. Success needs the connector and every listed task RUNNING:
	// a RUNNING connector with a FAILED task is not a fix.
	taskCount := 0
	state, running, err := awaitRunning(ctx, pollEvery(a.pollInterval), func(ctx context.Context) (string, error) {
		st, err := a.d.Connect.Status(ctx, p.Connector)
		if err != nil {
			return "", err
		}
		desc, n, ok := connectorAndTasks(st)
		taskCount = n
		if ok {
			return "RUNNING", nil
		}
		return desc, nil
	})
	switch {
	case err != nil:
		return fmt.Sprintf("restart accepted for %s; status read failed: %v", p.Connector, err), nil
	case running:
		return fmt.Sprintf("restarted failed tasks of %s (connector and %d tasks RUNNING)", p.Connector, taskCount), nil
	}
	return fmt.Sprintf("restart accepted for %s; %s (Connect applies restarts asynchronously)", p.Connector, orUnknown(state)), nil
}

// connectorAndTasks describes a connector status for connect_restart_connector.
// ok is true only when the connector and every listed task are RUNNING and at
// least one task is listed. Otherwise desc names each part that is not
// RUNNING, for example "connector RUNNING, task 0 FAILED".
func connectorAndTasks(st clients.ConnectorStatus) (desc string, n int, ok bool) {
	parts := []string{"connector " + orUnknown(st.State)}
	allRunning := st.State == "RUNNING" && len(st.Tasks) > 0
	for _, t := range st.Tasks {
		if t.State != "RUNNING" {
			allRunning = false
			parts = append(parts, fmt.Sprintf("task %d %s", t.ID, orUnknown(t.State)))
		}
	}
	if len(st.Tasks) == 0 {
		parts = append(parts, "no tasks listed")
	}
	if allRunning {
		return "RUNNING", len(st.Tasks), true
	}
	return strings.Join(parts, ", "), len(st.Tasks), false
}

// ---- connect_resume_connector ----

// connectorResumer is the local adapter for ConnectClient.Resume until the
// method lands on clients.ConnectClient (contract change request in the PR):
// the concrete connect client (internal/clients/connect) and the test fakes
// implement it, and the action reports a clear error when the injected client
// does not.
type connectorResumer interface {
	Resume(ctx context.Context, name string) error
}

type resumeConnector struct{ d Deps }

func (a *resumeConnector) Name() string  { return "connect_resume_connector" }
func (a *resumeConnector) Layer() string { return "kafka-connect" }
func (a *resumeConnector) Describe() string {
	return "Resumes a paused or stopped Kafka Connect connector."
}
func (a *resumeConnector) ParamsSchema() map[string]any {
	return objectSchema(map[string]any{"connector": strProp("connector name")}, "connector")
}

type resumeConnectorParams struct {
	Connector string `json:"connector"`
}

// resumePollInterval is how often Execute re-reads the connector state while
// it waits for Connect to apply the resume.
const resumePollInterval = 250 * time.Millisecond

func (a *resumeConnector) Validate(raw json.RawMessage) error {
	var p resumeConnectorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("actions: decode params: %w", err)
	}
	if p.Connector == "" {
		return fmt.Errorf("actions: connector is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := a.d.connectorExists(ctx, p.Connector); err != nil {
		return err
	}
	// Resume only moves PAUSED or STOPPED connectors. Refusing the rest here
	// keeps the agent from "resuming" a RUNNING connector or a FAILED one,
	// which a resume does not fix (restart is the action for FAILED tasks).
	st, err := a.d.Connect.Status(ctx, p.Connector)
	if err != nil {
		return fmt.Errorf("actions: resume %s: read status: %w", p.Connector, err)
	}
	if st.State != "PAUSED" && st.State != "STOPPED" {
		return fmt.Errorf("actions: connector %q is %s; resume only applies to PAUSED or STOPPED", p.Connector, st.State)
	}
	return nil
}

func (a *resumeConnector) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params resumeConnectorParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeConnector(p, params.Connector)
}

func (a *resumeConnector) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p resumeConnectorParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", fmt.Errorf("actions: bad params: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	rc, ok := a.d.Connect.(connectorResumer)
	if !ok {
		return "", fmt.Errorf("actions: connect client does not support Resume")
	}
	if err := rc.Resume(ctx, p.Connector); err != nil {
		return "", fmt.Errorf("actions: resume %s: %w", p.Connector, err)
	}
	// Connect applies a resume asynchronously. Once Resume has succeeded the
	// write is done, so every later failure is reported as a non-error result:
	// an error here would tell the agent the resume failed and invite a retry.
	state := ""
	for {
		st, err := a.d.Connect.Status(ctx, p.Connector)
		if err != nil {
			return fmt.Sprintf("resume accepted for %s; status read failed: %v", p.Connector, err), nil
		}
		state = st.State
		if state == "RUNNING" {
			return fmt.Sprintf("resumed %s (RUNNING)", p.Connector), nil
		}
		select {
		case <-ctx.Done():
			return fmt.Sprintf("resume accepted for %s; state now %s (Connect applies resume asynchronously)", p.Connector, state), nil
		case <-time.After(resumePollInterval):
		}
	}
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
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("actions: decode params: %w", err)
	}
	if p.Subject == "" {
		return fmt.Errorf("actions: subject is required")
	}
	if !compatLevels[p.Level] {
		return fmt.Errorf("actions: level %q is not a valid compatibility level", p.Level)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.d.subjectExists(ctx, p.Subject)
}

func (a *setCompatibility) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params setCompatibilityParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeSubject(p, params.Subject)
}

func (a *setCompatibility) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p setCompatibilityParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", fmt.Errorf("actions: decode params: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// Read the level before writing it: after a fix that loosened the subject
	// to NONE, the agent must know which level to restore. A failed read does
	// not block the write; the old level is then reported as unknown.
	old := "unknown"
	if before, subjectLevel, err := a.d.Registry.Compatibility(ctx, p.Subject); err == nil && before != "" {
		old = levelText(before, subjectLevel)
	}
	if err := a.d.Registry.SetCompatibility(ctx, p.Subject, p.Level); err != nil {
		return "", fmt.Errorf("actions: set compatibility %s: %w", p.Subject, err)
	}
	level, subjectLevel, err := a.d.Registry.Compatibility(ctx, p.Subject)
	if err != nil {
		return fmt.Sprintf("%s compatibility %s → %s (set; re-read failed: %v)", p.Subject, old, p.Level, err), nil
	}
	return fmt.Sprintf("%s compatibility %s → %s", p.Subject, old, levelText(level, subjectLevel)), nil
}

// levelText renders a compatibility level for the result. A level inherited
// from the global default is marked "(global)", so the agent can tell that the
// subject itself had no level of its own.
func levelText(level string, subjectLevel bool) string {
	if subjectLevel {
		return level
	}
	return level + " (global)"
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
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("actions: decode params: %w", err)
	}
	if p.Subject == "" || p.Schema == "" {
		return fmt.Errorf("actions: subject and schema are required")
	}
	if _, err := avro.Parse(p.Schema); err != nil {
		return fmt.Errorf("actions: schema is not valid Avro: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.d.subjectExists(ctx, p.Subject)
}

func (a *registerSchema) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	var params registerSchemaParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return fmt.Errorf("actions: bad params: %w", err)
	}
	return inScopeSubject(p, params.Subject)
}

func (a *registerSchema) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	var p registerSchemaParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", fmt.Errorf("actions: decode params: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	id, err := a.d.Registry.Register(ctx, p.Subject, p.Schema)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("registered %s as schema id %d", p.Subject, id), nil
}
