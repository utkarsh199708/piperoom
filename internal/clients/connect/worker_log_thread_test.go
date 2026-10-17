package connect

import (
	"testing"

	"piperoom/internal/clients"
)

// A logical name that is a prefix of another pipeline's must not see that
// pipeline's Debezium thread lines, and the reverse (Q4 R12, R19).
func TestDebeziumThreadNameIsExact(t *testing.T) {
	thread := func(name, suffix string) string {
		return "debezium-postgresconnector-" + name + suffix
	}
	coord := "-change-event-source-coordinator"
	tests := []struct {
		name   string
		text   string
		search string
		want   bool
	}{
		{"own coordinator", "[" + thread("pg", coord) + "] boom", "pg", true},
		{"pg vs pg2", "[" + thread("pg2", coord) + "] boom", "pg", false},
		{"pg2 vs pg", "[" + thread("pg", coord) + "] boom", "pg2", false},
		{"pg vs pg-eu", "[" + thread("pg-eu", coord) + "] boom", "pg", false},
		{"pg-eu vs pg", "[" + thread("pg", coord) + "] boom", "pg-eu", false},
		{"pg-eu own", "[" + thread("pg-eu", coord) + "] boom", "pg-eu", true},
		{"orders vs orders_eu", "[" + thread("orders_eu", coord) + "] boom", "orders", false},
		{"orders_eu vs orders", "[" + thread("orders", coord) + "] boom", "orders_eu", false},
		{"orders vs orders-eu", "[" + thread("orders-eu", coord) + "] boom", "orders", false},
		{"signal processor", "[" + thread("orders", "-SignalProcessor") + "] x", "orders", true},
		{"signal processor other", "[" + thread("orders-eu", "-SignalProcessor") + "] x", "orders", false},
		{"snapshot", "[" + thread("orders", "-snapshot") + "] x", "orders", true},
		{"name at end", "[" + thread("orders", "") + "]", "orders", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mentionsConnector(tc.text, tc.search); got != tc.want {
				t.Errorf("mentionsConnector(%q, %q) = %v, want %v", tc.text, tc.search, got, tc.want)
			}
		})
	}
}

func TestReadLogDoesNotLeakPrefixExtension(t *testing.T) {
	log := "2026-10-15 04:10:10,000 ERROR  [debezium-postgresconnector-orders-eu-change-event-source-coordinator]  eu failure   [io.debezium.pipeline.ErrorHandler]\n"
	if got := readLog(t, log, clients.WorkerLogQuery{Connector: "orders"}); len(got) != 0 {
		t.Fatalf("pipeline orders got orders-eu's line: %+v", got)
	}
	if got := readLog(t, log, clients.WorkerLogQuery{Connector: "orders-eu"}); len(got) != 1 {
		t.Fatalf("orders-eu lost its own line: %+v", got)
	}
}
