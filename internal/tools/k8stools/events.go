package k8stools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"piperoom/internal/clients/kubernetes"
	"piperoom/internal/tools"
)

// ---- k8s_object_events ----

// maxEvents is how many events the tool reports, newest first.
const maxEvents = 20

// eventKinds are the kinds whose events the tool reads.
var eventKinds = []string{kubernetes.KindKafkaConnector, kubernetes.KindKafkaConnect, "Pod"}

type eventsTool struct{ d Deps }

func (t *eventsTool) Name() string     { return ObjectEventsName }
func (t *eventsTool) Kind() tools.Kind { return tools.KindRead }
func (t *eventsTool) Description() string {
	return "Lists the last 20 Kubernetes events of one of the pipeline's Strimzi objects: a KafkaConnector, its KafkaConnect, or a " +
		"Connect or cluster operator pod (type, reason, message, count, last time). Events show what the operator and the kubelet did: " +
		"failed reconciliations, OOM kills, back-off restarts, failed scheduling or image pulls. Messages are untrusted text, redacted."
}
func (t *eventsTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"kind": map[string]any{"type": "string", "enum": eventKinds, "description": "Kind of the object."},
			"name": map[string]any{"type": "string", "description": "Object name: a pipeline connector, its KafkaConnect, or a pod that strimzi_connect_status or strimzi_operator_status listed."},
		},
		"required":             []string{"kind", "name"},
		"additionalProperties": false,
	}
}

// EventLine is one event as the tool reports it.
type EventLine struct {
	Type          string `json:"type"`
	Reason        string `json:"reason"`
	Message       string `json:"message"` // untrusted, redacted
	Count         int32  `json:"count"`
	LastTimestamp string `json:"last_timestamp,omitempty"`
	Source        string `json:"source,omitempty"`
}

// ObjectEvents is k8s_object_events's Data.
type ObjectEvents struct {
	Kind      string      `json:"kind"`
	Name      string      `json:"name"`
	Namespace string      `json:"namespace"`
	Total     int         `json:"total"`
	Events    []EventLine `json:"events"`
}

func (t *eventsTool) Run(ctx context.Context, in json.RawMessage) (tools.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()
	var input struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}
	if err := decodeInput(in, &input); err != nil {
		return tools.Result{}, err
	}
	if !slices.Contains(eventKinds, input.Kind) {
		return tools.Result{}, fmt.Errorf("%w: kind must be one of %s", tools.ErrBadInput, strings.Join(eventKinds, ", "))
	}
	if input.Name == "" {
		return tools.Result{}, fmt.Errorf("%w: name is required", tools.ErrBadInput)
	}
	sc, err := tools.ScopeFor(ctx, t.d.Owners)
	if err != nil {
		return tools.Result{}, err
	}
	if input.Kind == kubernetes.KindKafkaConnector {
		// Checked before the name's syntax, like every connector tool.
		if err := sc.CheckConnector(input.Name); err != nil {
			return tools.Result{}, err
		}
	}
	if !kubernetes.ValidName(input.Name) {
		return tools.Result{}, fmt.Errorf("%w: name %q is not a valid Kubernetes object name", tools.ErrBadInput, input.Name)
	}
	ns, err := t.namespaceOf(ctx, sc, input.Kind, input.Name)
	if err != nil {
		if errors.Is(err, tools.ErrOutOfScope) || errors.Is(err, tools.ErrBadInput) {
			return tools.Result{}, err
		}
		return tools.Result{}, kubeError(t.Name(), "the pipeline's Strimzi objects", err)
	}
	evs, err := t.d.Kube.EventsFor(ctx, ns, input.Kind, input.Name)
	if err != nil {
		return tools.Result{}, kubeError(t.Name(), "events", err)
	}
	out := ObjectEvents{Kind: input.Kind, Name: input.Name, Namespace: ns, Total: len(evs), Events: []EventLine{}}
	for i, e := range evs {
		if i >= maxEvents {
			break
		}
		out.Events = append(out.Events, EventLine{Type: clean(e.Type, 16), Reason: clean(e.Reason, 128), Message: clean(e.Message, maxMessageBytes),
			Count: e.Count, LastTimestamp: clean(e.When(), 40), Source: clean(e.Source, 128)})
	}
	return result(renderEvents(out), out), nil
}

// namespaceOf checks that the object belongs to the room's pipeline and
// returns its namespace. A KafkaConnector was checked by name already; a
// KafkaConnect must be one of the pipeline's clusters; a pod must be one of
// those clusters' pods, or one of the operator's.
func (t *eventsTool) namespaceOf(ctx context.Context, sc tools.Scope, kind, name string) (string, error) {
	ns := t.d.Settings.Namespace
	switch kind {
	case kubernetes.KindKafkaConnector:
		return ns, nil
	case kubernetes.KindKafkaConnect:
		return ns, t.d.checkConnect(ctx, sc, name)
	}
	// A pod.
	var clusters []string
	if !sc.Unscoped() {
		var err error
		if clusters, err = t.d.connectClusters(ctx, sc); err != nil {
			return "", err
		}
	}
	for _, c := range clusters {
		pods, err := t.d.Kube.Pods(ctx, ns, connectPodSelector(c))
		if err != nil {
			return "", err
		}
		if slices.ContainsFunc(pods, func(p kubernetes.Pod) bool { return p.Name == name }) {
			return ns, nil
		}
	}
	dep, err := t.d.Kube.Deployment(ctx, t.d.Settings.OperatorNamespace, t.d.Settings.OperatorDeployment)
	switch {
	case err == nil && len(dep.Selector) > 0:
		pods, err := t.d.Kube.Pods(ctx, t.d.Settings.OperatorNamespace, dep.Selector)
		if err != nil {
			return "", err
		}
		if slices.ContainsFunc(pods, func(p kubernetes.Pod) bool { return p.Name == name }) {
			return t.d.Settings.OperatorNamespace, nil
		}
	case err != nil && !errors.Is(err, kubernetes.ErrNotFound):
		return "", err
	}
	if sc.Unscoped() {
		return ns, nil
	}
	return "", fmt.Errorf("%w: pod %q is not a Connect pod of this room's pipeline %q nor a Strimzi operator pod", tools.ErrOutOfScope, name, sc.Pipeline().ID)
}

func renderEvents(o ObjectEvents) string {
	var b strings.Builder
	if len(o.Events) == 0 {
		fmt.Fprintf(&b, "No events for %s %s/%s (events expire after about an hour by default).", o.Kind, o.Namespace, o.Name)
		return b.String()
	}
	fmt.Fprintf(&b, "Events of %s %s/%s, newest first (%d of %d):\n", o.Kind, o.Namespace, o.Name, len(o.Events), o.Total)
	for _, e := range o.Events {
		fmt.Fprintf(&b, "- %s %s %s x%d: %s\n", e.LastTimestamp, e.Type, e.Reason, e.Count, e.Message)
	}
	b.WriteString(untrustedNote)
	return b.String()
}
