package agent

import (
	"encoding/json"
	"regexp"
)

// Q6b (KAN-193): when Kafka Connect reports a connection, DNS or TLS failure,
// the model tends to blame the host, though the host is often up and only
// Connect's network position, resolver or trust store differs from
// Piperoom's. net_probe (Q6a) probes the same target from Piperoom and asks
// Connect to validate the source connector's config, so the two views can be
// compared. As with the Q3e skip rule (workerskip.go), the CDC prompt has no
// room for the rule, so it sits beside the error that makes it apply.

// netRuleNote is fixed agent text appended after a connector-state read whose
// connector or task error is a network or TLS failure. It carries no tool
// data, so no trace text can forge or break it.
const netRuleNote = "[agent rule] Connect reports a network or TLS failure above. Before blaming the host, run net_probe on that target to compare Piperoom's view with Connect's, and cite both."

// netRuleTool is the tool netRuleNote names; the note is added only when the
// room offers it.
const netRuleTool = "net_probe"

// networkFailure matches the connection, DNS and TLS failures Kafka Connect
// and its JDBC and Kafka clients put in a task or connector trace. It is a
// hint for a fixed rule, never a verdict: a miss only leaves the rule out.
var networkFailure = regexp.MustCompile(`(?i)connection refused|connection timed out|connect timed out|no route to host|network is unreachable|unknownhostexception|unknown host|name or service not known|could not translate host name|temporary failure in name resolution|communications link failure|the connection attempt failed|sslhandshakeexception|pkix path building failed|certificate (?:has )?expired|certificateexception|unable to find valid certification path|handshake_failure`)

// networkFailureRead reports whether a connector-state read's Data holds a
// connector or task error that is a network or TLS failure. The error text is
// already condensed and sanitized by the tool; Data is round-tripped through
// JSON so the tool's map and a fake's are read the same way.
func networkFailureRead(tool string, data any) bool {
	if tool != "connect_status" || data == nil {
		return false
	}
	b, err := json.Marshal(data)
	if err != nil {
		return false
	}
	var d struct {
		Error string `json:"error"`
		Tasks []struct {
			Error string `json:"error"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return false
	}
	if networkFailure.MatchString(d.Error) {
		return true
	}
	for _, task := range d.Tasks {
		if networkFailure.MatchString(task.Error) {
			return true
		}
	}
	return false
}
