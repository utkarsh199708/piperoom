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

// ---- strimzi_operator_status ----

type operatorTool struct{ d Deps }

func (t *operatorTool) Name() string     { return OperatorStatusName }
func (t *operatorTool) Kind() tools.Kind { return tools.KindRead }
func (t *operatorTool) Description() string {
	return "Reads the Strimzi cluster operator's Deployment: desired, ready and available replicas, its conditions, and its pods' " +
		"restarts and last terminations (OOMKilled, CrashLoopBackOff). Use when KafkaConnector or KafkaConnect changes are not applied " +
		"(observedGeneration behind generation, no Ready condition), before blaming Kafka Connect itself."
}
func (t *operatorTool) InputSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{},
		"required":             []string{},
		"additionalProperties": false,
	}
}

// OperatorStatus is strimzi_operator_status's Data.
type OperatorStatus struct {
	Deployment          string      `json:"deployment"`
	Namespace           string      `json:"namespace"`
	Found               bool        `json:"found"`
	DesiredReplicas     int32       `json:"desired_replicas"`
	ReadyReplicas       int32       `json:"ready_replicas"`
	AvailableReplicas   int32       `json:"available_replicas"`
	UnavailableReplicas int32       `json:"unavailable_replicas"`
	Generation          int64       `json:"generation"`
	ObservedGeneration  int64       `json:"observed_generation"`
	Conditions          []Condition `json:"conditions"`
	Pods                []PodState  `json:"pods"`
	PodsReadError       string      `json:"pods_read_error,omitempty"`
	Findings            []string    `json:"findings,omitempty"`
}

func (t *operatorTool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	var input struct{}
	if len(in) == 0 {
		in = json.RawMessage(`{}`)
	}
	if err := decodeInput(in, &input); err != nil {
		return tools.Result{}, err
	}
	// The operator is shared, but the tool still runs only in a room (or an
	// explicit unscoped CLI run).
	if _, err := tools.ScopeFor(ctx, t.d.Owners); err != nil {
		return tools.Result{}, err
	}
	ns, name := t.d.Settings.OperatorNamespace, t.d.Settings.OperatorDeployment
	st := OperatorStatus{Deployment: name, Namespace: ns, Pods: []PodState{}}
	dep, err := t.d.Kube.Deployment(ctx, ns, name)
	if errors.Is(err, kubernetes.ErrNotFound) {
		return result(fmt.Sprintf("Deployment %s/%s not found: no Strimzi cluster operator there (set strimzi_operator_deployment and strimzi_operator_namespace if it runs elsewhere). Without an operator no KafkaConnector or KafkaConnect change is applied.", ns, name), st), nil
	}
	if err != nil {
		return tools.Result{}, kubeError(t.Name(), "the operator Deployment", err)
	}
	st.Found = true
	st.DesiredReplicas, st.ReadyReplicas, st.AvailableReplicas, st.UnavailableReplicas = dep.DesiredReplicas, dep.ReadyReplicas, dep.AvailableReplicas, dep.UnavailableReplicas
	st.Generation, st.ObservedGeneration = dep.Generation, dep.ObservedGeneration
	st.Conditions = cleanConditions(dep.Conditions)
	if len(dep.Selector) > 0 {
		pods, err := t.d.Kube.Pods(ctx, ns, dep.Selector)
		if err != nil {
			st.PodsReadError = clean(kubeError(t.Name(), "the operator pods", err).Error(), 300)
		} else {
			st.Pods = podStates(pods)
		}
	}
	findOperatorProblems(&st)
	return result(renderOperator(st), st), nil
}

func findOperatorProblems(st *OperatorStatus) {
	if st.DesiredReplicas == 0 {
		st.Findings = append(st.Findings, "scaled to 0 replicas: no operator runs, so no Strimzi resource is reconciled")
	} else if st.AvailableReplicas < st.DesiredReplicas {
		st.Findings = append(st.Findings, fmt.Sprintf("%d of %d replicas available", st.AvailableReplicas, st.DesiredReplicas))
	}
	for _, c := range st.Conditions {
		if c.Status != "True" {
			st.Findings = append(st.Findings, "Deployment "+conditionLine(c))
		}
	}
	if st.PodsReadError != "" {
		st.Findings = append(st.Findings, st.PodsReadError)
	}
	for _, p := range st.Pods {
		if p.Restarts > 0 && p.LastTermination != "" {
			st.Findings = append(st.Findings, fmt.Sprintf("pod %s restarted %d times, last: %s", p.Name, p.Restarts, p.LastTermination))
		}
	}
	st.Findings = append(st.Findings, podHints(st.Pods)...)
}

func renderOperator(st OperatorStatus) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Strimzi operator Deployment %s/%s: %d desired, %d ready, %d available\n",
		st.Namespace, st.Deployment, st.DesiredReplicas, st.ReadyReplicas, st.AvailableReplicas)
	for _, p := range st.Pods {
		b.WriteString("pod " + podLine(p) + "\n")
	}
	if len(st.Findings) == 0 {
		b.WriteString("The operator is available and its pods are not restarting.\n")
	} else {
		b.WriteString("Findings:\n")
		for _, f := range st.Findings {
			b.WriteString("- " + f + "\n")
		}
	}
	b.WriteString(untrustedNote)
	return b.String()
}
