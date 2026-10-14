package kafkatools

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func TestExcClassNameNeedsPackageAndLength(t *testing.T) {
	long := strings.Repeat("a", 60) + "." + strings.Repeat("b", 60) + ".XException" // 133 bytes
	tests := []struct{ in, want string }{
		{"org.apache.kafka.connect.errors.DataException", "org.apache.kafka.connect.errors.DataException"},
		{"a.XError", "a.XError"},
		{"ZZMARKError", "?"},
		{"xWlpNQVJLException", "?"},
		{"DataException", "?"},
		{long, "?"},
	}
	for _, tc := range tests {
		if got := excClassName(tc.in); got != tc.want {
			t.Errorf("excClassName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestOperatorClassName(t *testing.T) {
	tests := []struct{ in, want string }{
		{"org.apache.kafka.connect.json.JsonConverter", "org.apache.kafka.connect.json.JsonConverter"},
		{"org.apache.kafka.connect.transforms.ExtractField$Value", "org.apache.kafka.connect.transforms.ExtractField$Value"},
		{"a.lowercase", "?"},
		{"a._Under", "?"},
		{"a.1Digit", "?"},
		{"WlpNQVJL", "?"},
		{"a.", "?"},
		{".A", "?"},
		{"4111-1111", "?"},
	}
	for _, tc := range tests {
		if got := operatorClassName(tc.in); got != tc.want {
			t.Errorf("operatorClassName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNumOrQBounds(t *testing.T) {
	const card = "4111111111111111"
	tests := []struct {
		name string
		in   string
		max  uint64
		want string
	}{
		{"task 0", "0", maxTaskID, "0"},
		{"task max", "9999", maxTaskID, "9999"},
		{"task over", "10000", maxTaskID, "?"},
		{"task card number", card, maxTaskID, "?"},
		{"task negative", "-1", maxTaskID, "?"},
		{"task text", "12a", maxTaskID, "?"},
		{"partition max", "2147483647", maxPartition, "2147483647"},
		{"partition over", "2147483648", maxPartition, "?"},
		{"partition card number", card, maxPartition, "?"},
		{"offset 16 digits unchanged", card, maxOffset, card},
		{"offset int64 max", "9223372036854775807", maxOffset, "9223372036854775807"},
		{"offset over int64", "9223372036854775808", maxOffset, "?"},
		{"spaces trimmed", " 7 ", maxTaskID, "7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := numOrQ(tc.in, tc.max); got != tc.want {
				t.Errorf("numOrQ(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRecordLineNumericHeaders(t *testing.T) {
	h := hdr("org.apache.kafka.connect.errors.DataException", "m",
		"__connect.errors.task.id", "4111111111111111", "__connect.errors.partition", "4111111111111111")
	line := recordLine(clients.DLQRecord{Timestamp: ts(1), Headers: h}, "classes", tools.Scope{})
	if strings.Contains(line, "4111") || !strings.Contains(line, "task=?") || !strings.Contains(line, "/?@25377154") {
		t.Fatalf("line = %s", line)
	}
}

func TestConnectorNameScoped(t *testing.T) {
	sc, err := tools.ScopeFor(tools.WithPipelineID(context.Background(), "orders"), dlqOwners(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string]string{
		"pg-orders-sink": "pg-orders-sink",
		"bill-sink":      "?",
		"PG-ORDERS-SINK": "?",
		"":               "?",
		"4111-1111":      "?",
	} {
		if got := connectorName(in, sc); got != want {
			t.Errorf("scoped connectorName(%q) = %q, want %q", in, got, want)
		}
	}
	un, _ := tools.ScopeFor(tools.WithUnscoped(context.Background()), nil)
	if connectorName("any-name", un) != "any-name" || connectorName("has space", un) != "?" {
		t.Error("unscoped connectorName falls back to the name shape")
	}
}
