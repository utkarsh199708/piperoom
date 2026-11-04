package k8stools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"piperoom/internal/clients/kubernetes"
	"piperoom/internal/tools"
	"piperoom/internal/tools/kafkatools"
)

// ---- strimzi_connector_status ----

type connectorTool struct{ d Deps }

func (t *connectorTool) Name() string     { return ConnectorStatusName }
func (t *connectorTool) Kind() tools.Kind { return tools.KindRead }
func (t *connectorTool) Description() string {
	return "Reads a pipeline connector's Strimzi KafkaConnector resource: its Ready condition with reason and message, the connector and " +
		"task states Strimzi copied from Kafka Connect (with condensed traces), spec.state/pause, the strimzi.io/pause-reconciliation " +
		"annotation, whether the operator has reconciled the latest generation, and whether the owning KafkaConnect is Ready and manages " +
		"KafkaConnector resources. Use when Connect runs on Kubernetes with Strimzi and a connector is missing, not ready, or ignores a change."
}
func (t *connectorTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "Connector name from the room's pipeline (the KafkaConnector's metadata.name)."},
		},
		"required":             []string{"connector"},
		"additionalProperties": false,
	}
}

// ConnectorTask is one task from .status.connectorStatus.
type ConnectorTask struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id,omitempty"`
	Trace    string `json:"trace,omitempty"` // untrusted, condensed, redacted
}

// ConnectorStatus is strimzi_connector_status's Data.
type ConnectorStatus struct {
	Connector string `json:"connector"`
	Namespace string `json:"namespace"`
	Found     bool   `json:"found"`
	// Generation and ObservedGeneration differ while the operator has not
	// reconciled the latest change (or cannot).
	Generation         int64       `json:"generation"`
	ObservedGeneration int64       `json:"observed_generation"`
	Reconciled         bool        `json:"reconciled"`
	Conditions         []Condition `json:"conditions"`
	// SpecState is spec.state (running, paused, stopped); SpecPause is the
	// older spec.pause.
	SpecState            string `json:"spec_state,omitempty"`
	SpecPause            *bool  `json:"spec_pause,omitempty"`
	ReconciliationPaused bool   `json:"reconciliation_paused"`
	TasksMax             *int   `json:"tasks_max,omitempty"`
	AutoRestarts         *int   `json:"auto_restarts,omitempty"`
	// ConnectorState, WorkerID and Trace come from .status.connectorStatus.
	ConnectorState string          `json:"connector_state,omitempty"`
	WorkerID       string          `json:"worker_id,omitempty"`
	Trace          string          `json:"trace,omitempty"` // untrusted, condensed, redacted
	Tasks          []ConnectorTask `json:"tasks,omitempty"`
	// Cluster is the owning KafkaConnect (strimzi.io/cluster label).
	Cluster        string     `json:"connect_cluster,omitempty"`
	ClusterFound   bool       `json:"connect_found"`
	ClusterReady   *Condition `json:"connect_ready,omitempty"`
	ClusterManages *bool      `json:"connect_uses_connector_resources,omitempty"`
	ClusterReadErr string     `json:"connect_read_error,omitempty"`
	Findings       []string   `json:"findings,omitempty"`
}

type connectorStatusJSON struct {
	strimziStatus
	TasksMax    *int `json:"tasksMax"`
	AutoRestart *struct {
		Count *int `json:"count"`
	} `json:"autoRestart"`
	ConnectorStatus *struct {
		Connector struct {
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Trace    string `json:"trace"`
		} `json:"connector"`
		Tasks []struct {
			ID       int    `json:"id"`
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Trace    string `json:"trace"`
		} `json:"tasks"`
	} `json:"connectorStatus"`
}

func (t *connectorTool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	var input struct {
		Connector string `json:"connector"`
	}
	if err := decodeInput(in, &input); err != nil {
		return tools.Result{}, err
	}
	if input.Connector == "" {
		return tools.Result{}, fmt.Errorf("%w: connector is required", tools.ErrBadInput)
	}
	sc, err := tools.ScopeFor(ctx, t.d.Owners)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckConnector(input.Connector); err != nil {
		return tools.Result{}, err
	}
	if !kubernetes.ValidName(input.Connector) {
		return tools.Result{}, fmt.Errorf("%w: connector %q is not a valid Kubernetes object name, so it has no KafkaConnector", tools.ErrBadInput, input.Connector)
	}
	ns := t.d.Settings.Namespace
	st := ConnectorStatus{Connector: input.Connector, Namespace: ns}
	r, err := t.d.Kube.CustomResource(ctx, kubernetes.KindKafkaConnector, ns, input.Connector)
	if errors.Is(err, kubernetes.ErrNotFound) {
		st.Findings = []string{"no KafkaConnector resource of this name in the namespace"}
		return result(fmt.Sprintf("KafkaConnector %s/%s not found: Strimzi does not manage this connector in namespace %s (it may be created through the Connect REST API, or live in another namespace).", ns, input.Connector, ns), st), nil
	}
	if err != nil {
		return tools.Result{}, kubeError(t.Name(), "the KafkaConnector", err)
	}
	st.Found = true
	fillConnector(&st, r)
	t.owningConnect(ctx, &st)
	findConnectorProblems(&st)
	return result(renderConnector(st), st), nil
}

// fillConnector copies what the tool reports from the resource.
func fillConnector(st *ConnectorStatus, r kubernetes.Resource) {
	st.Generation = r.Metadata.Generation
	st.Cluster = r.Metadata.Labels[clusterLabel]
	st.ReconciliationPaused = r.Metadata.Annotations[pauseAnnotation] == "true"
	var state string
	if json.Unmarshal(r.Spec["state"], &state) == nil {
		st.SpecState = clean(state, 32)
	}
	var pause bool
	if json.Unmarshal(r.Spec["pause"], &pause) == nil {
		st.SpecPause = &pause
	}
	var s connectorStatusJSON
	if len(r.Status) > 0 && json.Unmarshal(r.Status, &s) != nil {
		st.Findings = append(st.Findings, "the resource's status does not decode")
	}
	st.ObservedGeneration = s.ObservedGeneration
	st.Reconciled = s.ObservedGeneration >= r.Metadata.Generation
	st.Conditions = cleanConditions(s.Conditions)
	st.TasksMax = s.TasksMax
	if s.AutoRestart != nil {
		st.AutoRestarts = s.AutoRestart.Count
	}
	if cs := s.ConnectorStatus; cs != nil {
		st.ConnectorState = clean(cs.Connector.State, 32)
		st.WorkerID = clean(cs.Connector.WorkerID, 128)
		if cs.Connector.Trace != "" {
			st.Trace = cleanTrace(kafkatools.CleanTaskTrace(cs.Connector.Trace))
		}
		for _, tk := range cs.Tasks {
			ct := ConnectorTask{ID: tk.ID, State: clean(tk.State, 32), WorkerID: clean(tk.WorkerID, 128)}
			if tk.Trace != "" {
				ct.Trace = cleanTrace(kafkatools.CleanTaskTrace(tk.Trace))
			}
			st.Tasks = append(st.Tasks, ct)
		}
	}
}

// owningConnect reads the connector's KafkaConnect. A failure is recorded,
// not returned: the connector's own status is already known.
func (t *connectorTool) owningConnect(ctx context.Context, st *ConnectorStatus) {
	if st.Cluster == "" || !kubernetes.ValidName(st.Cluster) {
		return
	}
	r, err := t.d.Kube.CustomResource(ctx, kubernetes.KindKafkaConnect, st.Namespace, st.Cluster)
	switch {
	case errors.Is(err, kubernetes.ErrNotFound):
		return
	case err != nil:
		st.ClusterReadErr = clean(err.Error(), 200)
		return
	}
	st.ClusterFound = true
	var s strimziStatus
	_ = json.Unmarshal(r.Status, &s)
	if c, ok := ready(cleanConditions(s.Conditions)); ok {
		st.ClusterReady = &c
	}
	manages := r.Metadata.Annotations[connectorResourcesAnnotation] == "true"
	st.ClusterManages = &manages
}

// findConnectorProblems lists the causes the status shows, most direct first.
func findConnectorProblems(st *ConnectorStatus) {
	if c, ok := ready(st.Conditions); ok && c.Status != "True" {
		st.Findings = append(st.Findings, "KafkaConnector "+conditionLine(c))
	} else if !ok {
		st.Findings = append(st.Findings, "no Ready condition yet: the operator has not reconciled this KafkaConnector")
	}
	if st.ReconciliationPaused {
		st.Findings = append(st.Findings, "annotation "+pauseAnnotation+"=true: the operator ignores every change to this KafkaConnector until it is removed")
	}
	if !st.Reconciled {
		st.Findings = append(st.Findings, fmt.Sprintf("generation %d not reconciled (observedGeneration %d): the latest spec change has not reached Kafka Connect", st.Generation, st.ObservedGeneration))
	}
	switch {
	case st.SpecState == "paused" || st.SpecState == "stopped":
		st.Findings = append(st.Findings, "spec.state="+st.SpecState+": the connector is "+st.SpecState+" on purpose; Strimzi keeps it so")
	case st.SpecState == "" && st.SpecPause != nil && *st.SpecPause:
		st.Findings = append(st.Findings, "spec.pause=true: the connector is paused on purpose; Strimzi keeps it so")
	}
	if st.ConnectorState == "FAILED" {
		st.Findings = append(st.Findings, "connector FAILED in Kafka Connect")
	}
	for _, tk := range st.Tasks {
		if tk.State == "FAILED" {
			st.Findings = append(st.Findings, fmt.Sprintf("task %d FAILED", tk.ID))
		}
	}
	switch {
	case st.Cluster == "":
		st.Findings = append(st.Findings, "no "+clusterLabel+" label: no KafkaConnect will run this connector")
	case st.ClusterReadErr != "":
		st.Findings = append(st.Findings, "KafkaConnect "+st.Cluster+" could not be read: "+st.ClusterReadErr)
	case !st.ClusterFound:
		st.Findings = append(st.Findings, "the label names KafkaConnect "+st.Cluster+", which does not exist in the namespace")
	default:
		if st.ClusterManages != nil && !*st.ClusterManages {
			st.Findings = append(st.Findings, "KafkaConnect "+st.Cluster+" lacks "+connectorResourcesAnnotation+"=true: the operator ignores its KafkaConnector resources")
		}
		if st.ClusterReady != nil && st.ClusterReady.Status != "True" {
			st.Findings = append(st.Findings, "KafkaConnect "+st.Cluster+" "+conditionLine(*st.ClusterReady))
		}
	}
}

func renderConnector(st ConnectorStatus) string {
	var b strings.Builder
	fmt.Fprintf(&b, "KafkaConnector %s/%s", st.Namespace, st.Connector)
	if c, ok := ready(st.Conditions); ok {
		fmt.Fprintf(&b, ": Ready=%s", c.Status)
	}
	fmt.Fprintf(&b, "; generation %d, observed %d", st.Generation, st.ObservedGeneration)
	if st.SpecState != "" {
		fmt.Fprintf(&b, "; spec.state=%s", st.SpecState)
	}
	if st.Cluster != "" {
		fmt.Fprintf(&b, "; KafkaConnect %s", st.Cluster)
		if st.ClusterReady != nil {
			fmt.Fprintf(&b, " Ready=%s", st.ClusterReady.Status)
		}
	}
	b.WriteString("\n")
	if st.ConnectorState != "" {
		fmt.Fprintf(&b, "Connect status: connector %s", st.ConnectorState)
		for _, tk := range st.Tasks {
			fmt.Fprintf(&b, ", task %d %s", tk.ID, tk.State)
		}
		b.WriteString("\n")
	}
	if len(st.Findings) == 0 {
		b.WriteString("No problem in the resource: Ready, reconciled, not paused.\n")
	} else {
		b.WriteString("Findings:\n")
		for _, f := range st.Findings {
			b.WriteString("- " + f + "\n")
		}
	}
	if st.Trace != "" {
		b.WriteString("Connector trace:\n" + st.Trace + "\n")
	}
	for _, tk := range st.Tasks {
		if tk.Trace != "" {
			fmt.Fprintf(&b, "Task %d trace:\n%s\n", tk.ID, tk.Trace)
		}
	}
	b.WriteString(untrustedNote)
	return b.String()
}
