package k8stools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"piperoom/internal/clients/kubernetes"
	"piperoom/internal/tools"
)

// ---- strimzi_connect_status ----

type connectTool struct{ d Deps }

func (t *connectTool) Name() string     { return ConnectStatusName }
func (t *connectTool) Kind() tools.Kind { return tools.KindRead }
func (t *connectTool) Description() string {
	return "Reads the Strimzi KafkaConnect cluster that runs the pipeline's connectors: its conditions (Ready, NotReady with reason and " +
		"message), desired replicas against ready pods, each Connect pod's phase, restart count, current problem (CrashLoopBackOff, " +
		"ImagePullBackOff) and last termination (OOMKilled, exit code), and its REST URL. Use when connectors on Strimzi are missing or " +
		"unassigned, the Connect REST API does not answer, or workers restart. Without connect, the cluster is found from the pipeline's KafkaConnectors."
}
func (t *connectTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connect": map[string]any{"type": "string", "description": "KafkaConnect name (the strimzi.io/cluster label of the pipeline's KafkaConnectors). Optional."},
		},
		"required":             []string{},
		"additionalProperties": false,
	}
}

// ConnectStatus is strimzi_connect_status's Data.
type ConnectStatus struct {
	Connect            string      `json:"connect"`
	Namespace          string      `json:"namespace"`
	Found              bool        `json:"found"`
	Generation         int64       `json:"generation"`
	ObservedGeneration int64       `json:"observed_generation"`
	Conditions         []Condition `json:"conditions"`
	DesiredReplicas    *int        `json:"desired_replicas,omitempty"`
	StatusReplicas     *int        `json:"status_replicas,omitempty"`
	ReadyPods          int         `json:"ready_pods"`
	Pods               []PodState  `json:"pods"`
	URL                string      `json:"url,omitempty"`
	// UsesConnectorResources is the strimzi.io/use-connector-resources
	// annotation: without it the operator ignores KafkaConnector resources.
	UsesConnectorResources bool     `json:"uses_connector_resources"`
	ReconciliationPaused   bool     `json:"reconciliation_paused"`
	PodsReadError          string   `json:"pods_read_error,omitempty"`
	Findings               []string `json:"findings,omitempty"`
}

type connectStatusJSON struct {
	strimziStatus
	URL      string `json:"url"`
	Replicas *int   `json:"replicas"`
}

func (t *connectTool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	var input struct {
		Connect string `json:"connect"`
	}
	if len(in) == 0 {
		in = json.RawMessage(`{}`)
	}
	if err := decodeInput(in, &input); err != nil {
		return tools.Result{}, err
	}
	sc, err := tools.ScopeFor(ctx, t.d.Owners)
	if err != nil {
		return tools.Result{}, err
	}
	ns := t.d.Settings.Namespace
	name := input.Connect
	if name != "" {
		if !kubernetes.ValidName(name) {
			return tools.Result{}, fmt.Errorf("%w: connect %q is not a valid Kubernetes object name", tools.ErrBadInput, name)
		}
		if err := t.d.checkConnect(ctx, sc, name); err != nil {
			if errors.Is(err, tools.ErrOutOfScope) {
				return tools.Result{}, err
			}
			return tools.Result{}, kubeError(t.Name(), "the pipeline's KafkaConnectors", err)
		}
	} else {
		if sc.Unscoped() {
			return tools.Result{}, fmt.Errorf("%w: connect is required without a room pipeline", tools.ErrBadInput)
		}
		clusters, err := t.d.connectClusters(ctx, sc)
		if err != nil {
			return tools.Result{}, kubeError(t.Name(), "the pipeline's KafkaConnectors", err)
		}
		if len(clusters) == 0 {
			return result(fmt.Sprintf("No KafkaConnector of this pipeline (%s) exists in namespace %s, so its KafkaConnect is unknown: pass connect, or check that the connectors are managed by Strimzi.",
				strings.Join(sc.Pipeline().Connectors, ", "), ns), ConnectStatus{Namespace: ns}), nil
		}
		name = clusters[0]
	}
	st := ConnectStatus{Connect: name, Namespace: ns, Pods: []PodState{}}
	r, err := t.d.Kube.CustomResource(ctx, kubernetes.KindKafkaConnect, ns, name)
	if errors.Is(err, kubernetes.ErrNotFound) {
		return result(fmt.Sprintf("KafkaConnect %s/%s not found: the pipeline's KafkaConnectors name a cluster that does not exist, so no worker runs them.", ns, name), st), nil
	}
	if err != nil {
		return tools.Result{}, kubeError(t.Name(), "the KafkaConnect", err)
	}
	st.Found = true
	st.Generation = r.Metadata.Generation
	st.UsesConnectorResources = r.Metadata.Annotations[connectorResourcesAnnotation] == "true"
	st.ReconciliationPaused = r.Metadata.Annotations[pauseAnnotation] == "true"
	var desired int
	if json.Unmarshal(r.Spec["replicas"], &desired) == nil {
		st.DesiredReplicas = &desired
	}
	var s connectStatusJSON
	_ = json.Unmarshal(r.Status, &s)
	st.ObservedGeneration = s.ObservedGeneration
	st.Conditions = cleanConditions(s.Conditions)
	st.StatusReplicas = s.Replicas
	st.URL = clean(s.URL, 200)

	pods, err := t.d.Kube.Pods(ctx, ns, connectPodSelector(name))
	if err != nil {
		st.PodsReadError = clean(kubeError(t.Name(), "the Connect pods", err).Error(), 300)
	} else {
		st.Pods = podStates(pods)
	}
	for _, p := range st.Pods {
		if p.Ready {
			st.ReadyPods++
		}
	}
	findConnectProblems(&st)
	return result(renderConnect(st), st), nil
}

func findConnectProblems(st *ConnectStatus) {
	st.Findings = append(st.Findings, notReadyLines(st.Conditions)...)
	if _, ok := ready(st.Conditions); !ok {
		st.Findings = append(st.Findings, "no Ready condition yet: the operator has not reconciled this KafkaConnect")
	}
	if st.ObservedGeneration < st.Generation {
		st.Findings = append(st.Findings, fmt.Sprintf("generation %d not reconciled (observedGeneration %d)", st.Generation, st.ObservedGeneration))
	}
	if st.ReconciliationPaused {
		st.Findings = append(st.Findings, "annotation "+pauseAnnotation+"=true: the operator ignores changes to this KafkaConnect")
	}
	if !st.UsesConnectorResources {
		st.Findings = append(st.Findings, "no "+connectorResourcesAnnotation+"=true annotation: KafkaConnector resources are ignored for this cluster")
	}
	if st.DesiredReplicas != nil && st.PodsReadError == "" && st.ReadyPods < *st.DesiredReplicas {
		st.Findings = append(st.Findings, fmt.Sprintf("%d of %d desired Connect pods ready", st.ReadyPods, *st.DesiredReplicas))
	}
	if st.PodsReadError != "" {
		st.Findings = append(st.Findings, st.PodsReadError)
	}
	st.Findings = append(st.Findings, podHints(st.Pods)...)
}

func renderConnect(st ConnectStatus) string {
	var b strings.Builder
	fmt.Fprintf(&b, "KafkaConnect %s/%s", st.Namespace, st.Connect)
	if c, ok := ready(st.Conditions); ok {
		fmt.Fprintf(&b, ": Ready=%s", c.Status)
	}
	if st.DesiredReplicas != nil {
		fmt.Fprintf(&b, "; %d/%d pods ready", st.ReadyPods, *st.DesiredReplicas)
	} else {
		fmt.Fprintf(&b, "; %d pods ready", st.ReadyPods)
	}
	if st.URL != "" {
		fmt.Fprintf(&b, "; REST %s", st.URL)
	}
	b.WriteString("\n")
	for _, p := range st.Pods {
		b.WriteString("pod " + podLine(p) + "\n")
	}
	if len(st.Findings) == 0 {
		b.WriteString("No problem in the resource or its pods.\n")
	} else {
		b.WriteString("Findings:\n")
		for _, f := range st.Findings {
			b.WriteString("- " + f + "\n")
		}
	}
	b.WriteString(untrustedNote)
	return b.String()
}
