package mskconnect

import (
	"encoding/json"
	"fmt"
	"strings"
)

// KafkaConnectService is the one principal a service execution role may
// trust.
const KafkaConnectService = "kafkaconnect.amazonaws.com"

// maxTrustPolicy bounds the trust policy the runner parses; IAM's own limit
// is 2048 characters by default and 4096 at most.
const maxTrustPolicy = 16 << 10

// TrustProblem reads a role's trust policy (IAM's AssumeRolePolicyDocument,
// already URL-decoded) and says why it does not trust MSK Connect alone, or
// "" when it does: every Allow statement names only the Service principal
// kafkaconnect.amazonaws.com and only the action sts:AssumeRole, with no
// NotPrincipal or NotAction; at least one Allow statement exists. Deny
// statements only narrow the policy, and conditions (aws:SourceAccount,
// aws:SourceArn, the confused-deputy guards AWS recommends) are kept as
// they are. A role that another account, a user or a second service can
// assume would let the connector's permissions be used outside MSK Connect.
func TrustProblem(doc string) string {
	if len(doc) > maxTrustPolicy {
		return "the trust policy is too long to read"
	}
	var pol struct {
		Statement json.RawMessage `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(doc), &pol); err != nil {
		return "the trust policy is not a JSON policy document"
	}
	var stmts []map[string]json.RawMessage
	if err := json.Unmarshal(pol.Statement, &stmts); err != nil {
		var one map[string]json.RawMessage
		if err := json.Unmarshal(pol.Statement, &one); err != nil || one == nil {
			return "the trust policy has no statements"
		}
		stmts = []map[string]json.RawMessage{one}
	}
	allows := 0
	for i, st := range stmts {
		var effect string
		_ = json.Unmarshal(st["Effect"], &effect)
		switch effect {
		case "Deny":
			continue
		case "Allow":
		default:
			return fmt.Sprintf("statement %d has effect %q", i+1, clip(effect, 16))
		}
		allows++
		if _, ok := st["NotPrincipal"]; ok {
			return fmt.Sprintf("statement %d allows a NotPrincipal", i+1)
		}
		if _, ok := st["NotAction"]; ok {
			return fmt.Sprintf("statement %d allows a NotAction", i+1)
		}
		if p := onlyService(st["Principal"]); p != "" {
			return fmt.Sprintf("statement %d %s", i+1, p)
		}
		actions, ok := stringOrList(st["Action"])
		if !ok || len(actions) == 0 {
			return fmt.Sprintf("statement %d has no action list", i+1)
		}
		for _, a := range actions {
			if !strings.EqualFold(a, "sts:AssumeRole") {
				return fmt.Sprintf("statement %d allows %s, not only sts:AssumeRole", i+1, clip(a, 64))
			}
		}
	}
	if allows == 0 {
		return "the trust policy allows no one"
	}
	return ""
}

// onlyService says why a statement's Principal is not exactly the MSK
// Connect service, or "".
func onlyService(raw json.RawMessage) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		// "*" or a missing principal.
		return "trusts a principal other than " + KafkaConnectService
	}
	for k, v := range m {
		if k != "Service" {
			return "trusts a principal of type " + clip(k, 32)
		}
		svcs, ok := stringOrList(v)
		if !ok || len(svcs) == 0 {
			return "has an empty Service principal"
		}
		for _, s := range svcs {
			if s != KafkaConnectService {
				return "trusts the service " + clip(s, 64)
			}
		}
	}
	if _, ok := m["Service"]; !ok {
		return "trusts no service"
	}
	return ""
}

// stringOrList decodes an IAM policy value that is a string or a list of
// strings.
func stringOrList(raw json.RawMessage) ([]string, bool) {
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		return []string{one}, true
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err == nil {
		return many, true
	}
	return nil, false
}
