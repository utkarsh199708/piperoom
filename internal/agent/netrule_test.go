package agent

import (
	"strings"
	"testing"

	"piperoom/internal/tools"
)

func TestNetworkFailureRead(t *testing.T) {
	task := func(e string) map[string]any {
		return map[string]any{"name": "c", "state": "RUNNING", "tasks": []map[string]any{{"id": 0, "state": "FAILED", "error": e}}}
	}
	for _, tt := range []struct {
		name string
		tool string
		data any
		want bool
	}{
		{"task refused", "connect_status", task("org.postgresql.util.PSQLException: Connection to db:5432 refused. Check that the hostname and port are correct: Connection refused"), true},
		{"task dns", "connect_status", task("java.net.UnknownHostException: db.internal"), true},
		{"task tls", "connect_status", task("javax.net.ssl.SSLHandshakeException: PKIX path building failed"), true},
		{"mysql link", "connect_status", task("Communications link failure"), true},
		{"connector error", "connect_status", map[string]any{"name": "c", "state": "FAILED", "error": "Connect timed out"}, true},
		{"schema error", "connect_status", task("org.apache.kafka.connect.errors.DataException: unknown magic byte"), false},
		{"healthy", "connect_status", map[string]any{"name": "c", "state": "RUNNING", "tasks": []map[string]any{{"id": 0, "state": "RUNNING"}}}, false},
		{"other tool", "connect_config", task("Connection refused"), false},
		{"nil data", "connect_status", nil, false},
		{"bad shape", "connect_status", []string{"Connection refused"}, false},
	} {
		if got := networkFailureRead(tt.tool, tt.data); got != tt.want {
			t.Errorf("%s: networkFailureRead = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestNetRuleNoteIsFixedText(t *testing.T) {
	for _, want := range []string{"[agent rule]", "net_probe", "Piperoom's view with Connect's"} {
		if !strings.Contains(netRuleNote, want) {
			t.Errorf("net rule note lacks %q: %s", want, netRuleNote)
		}
	}
	if strings.ContainsAny(netRuleNote, "%<>") {
		t.Errorf("net rule note looks formatted or nests markup: %s", netRuleNote)
	}
}

// TestNetRuleReachesTheModel: a connect_status read with a network failure
// gets the net rule after its wrapper when the room offers net_probe.
func TestNetRuleReachesTheModel(t *testing.T) {
	status := func(e string) *recordingTool {
		return readTool("connect_status", tools.Result{
			Summary: "pg-orders-connector (source): RUNNING\nTask 0: FAILED",
			Data: map[string]any{"name": "pg-orders-connector", "state": "RUNNING",
				"tasks": []map[string]any{{"id": 0, "state": "FAILED", "error": e}}},
		})
	}
	probe := readTool("net_probe", tools.Result{Summary: "p"})
	for _, tt := range []struct {
		name  string
		tools []tools.Tool
		want  bool
	}{
		{"network failure, probe offered", []tools.Tool{status("Connection refused"), probe}, true},
		{"other failure, probe offered", []tools.Tool{status("DataException"), probe}, false},
		{"network failure, no probe", []tools.Tool{status("Connection refused")}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			content := firstToolResultText(t, &fakeSink{}, "connect_status", `{"connector":"pg-orders-connector"}`, tt.tools...)
			if got := strings.Contains(content, netRuleNote); got != tt.want {
				t.Fatalf("rule in tool_result = %v, want %v; content: %s", got, tt.want, content)
			}
			if tt.want && !strings.Contains(content, "</tool_output>\n"+netRuleNote) {
				t.Fatalf("the rule must follow the untrusted wrapper, got: %s", content)
			}
		})
	}
}
