package plan

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// The Strimzi target's ladder (row U12b, docs/design/setup-strimzi.md). The
// Connect cluster is the operator's: the room reads the KafkaConnector
// custom resource through the in-cluster runner's k8s_get, so rungs 1 and 2
// judge the CR's status (the Ready condition and the connectorStatus the
// operator copies from Connect's REST API) instead of connect_status. Rungs 3
// to 8 read Kafka, the registry and the sink exactly as Ladder's do.

// ReadK8sGet is the runner read rungs 1 and 2 issue on a Strimzi target, with
// {"kind":"KafkaConnector","name":<connector>}.
const ReadK8sGet = "k8s_get"

// kafkaConnector is the part of a KafkaConnector object the predicates read.
type kafkaConnector struct {
	Kind   string `json:"kind"`
	Status *struct {
		Conditions []struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"conditions"`
		ConnectorStatus *struct {
			Connector struct {
				State string `json:"state"`
				Trace string `json:"trace"`
			} `json:"connector"`
			Tasks []struct {
				ID    int    `json:"id"`
				State string `json:"state"`
				Trace string `json:"trace"`
			} `json:"tasks"`
		} `json:"connectorStatus"`
	} `json:"status"`
}

func decodeKafkaConnector(data json.RawMessage) (kafkaConnector, string) {
	var k kafkaConnector
	if err := json.Unmarshal(data, &k); err != nil || k.Kind != "KafkaConnector" {
		return kafkaConnector{}, "the read did not return a KafkaConnector"
	}
	if k.Status == nil {
		return kafkaConnector{}, "the operator has not reconciled the KafkaConnector yet (no status)"
	}
	return k, ""
}

// StrimziLadder returns the eight rungs of a setup on a Strimzi target whose
// Connect REST API the room cannot reach (the usual case: it is a ClusterIP
// service). Ladder(source, "strimzi", sink) stays for a room that can; this
// one differs in rungs 1 and 2 only, which need the connector's name in their
// read input, supplied by the room's ladder runner per target.
func StrimziLadder(source, sink string) ([]Rung, error) {
	if !slices.Contains(ladderSources, source) {
		return nil, fmt.Errorf("%w: unknown source %q", ErrLadder, cleanText(source, 32))
	}
	rungs, err := Ladder(source, "strimzi", sink)
	if err != nil {
		return nil, err
	}
	rungs[0].Reads, rungs[0].Predicate = []string{ReadK8sGet}, strimziConnectorRunning
	rungs[1].Reads, rungs[1].Predicate = []string{ReadK8sGet}, strimziTasksRunning
	return rungs, nil
}

// strimziConnectorRunning: the CR is Ready and the connector RUNNING. A Ready
// condition that is not True explains the failure better than the state, so
// its reason and message come first.
func strimziConnectorRunning(data json.RawMessage) (bool, string) {
	k, why := decodeKafkaConnector(data)
	if why != "" {
		return false, why
	}
	ready := false
	for _, c := range k.Status.Conditions {
		switch {
		case c.Type == "Ready" && c.Status == "True":
			ready = true
		case c.Type == "NotReady" && c.Status == "True", c.Type == "Ready" && c.Status != "True":
			return false, "the KafkaConnector is not Ready: " + cleanText(strings.TrimSpace(c.Reason+" "+c.Message), 200)
		}
	}
	if !ready {
		return false, "the KafkaConnector has no Ready condition yet"
	}
	cs := k.Status.ConnectorStatus
	if cs == nil || cs.Connector.State == "" {
		return false, "the KafkaConnector status carries no connector state yet"
	}
	if !strings.EqualFold(cs.Connector.State, "RUNNING") {
		return false, "the connector is " + note(cs.Connector.State) + traceNote(cs.Connector.Trace)
	}
	return true, "the KafkaConnector is Ready and the connector is RUNNING"
}

func strimziTasksRunning(data json.RawMessage) (bool, string) {
	k, why := decodeKafkaConnector(data)
	if why != "" {
		return false, why
	}
	cs := k.Status.ConnectorStatus
	if cs == nil || len(cs.Tasks) == 0 {
		return false, "the KafkaConnector status lists no tasks yet"
	}
	for _, t := range cs.Tasks {
		if !strings.EqualFold(t.State, "RUNNING") {
			return false, fmt.Sprintf("task %d is %s%s", t.ID, note(t.State), traceNote(t.Trace))
		}
	}
	return true, fmt.Sprintf("all %d tasks are RUNNING", len(cs.Tasks))
}

// traceNote is the first line of a Connect trace: the exception, never the
// stack, and clipped (it is untrusted text).
func traceNote(trace string) string {
	first, _, _ := strings.Cut(strings.TrimSpace(trace), "\n")
	if first == "" {
		return ""
	}
	return ": " + cleanText(first, 160)
}
