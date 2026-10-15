package kafkatools

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// Debezium logs its own lines with the logical name (topic.prefix), so the
// tool reads the log for that name too (Q2 F14) and keeps those lines.
func TestConnectWorkerLogMatchesDebeziumLogicalName(t *testing.T) {
	at := time.Date(2026, 10, 15, 4, 10, 9, 0, time.UTC)
	d, _ := scopeFakes(t)
	d.Connect = &fakeConnect{
		listFn: func(context.Context) ([]string, error) { return []string{"pg-orders-connector"}, nil },
		configFn: func(context.Context, string) (map[string]string, error) {
			return map[string]string{"topic.prefix": "dbzlogical"}, nil
		},
	}
	fl := &fakeLogReader{fn: func(_ context.Context, q clients.WorkerLogQuery) ([]clients.LogLine, error) {
		if q.Connector == "dbzlogical" {
			return []clients.LogLine{{At: at, Level: "ERROR", Message: "Postgres|dbzlogical|streaming Producer failure\nCaused by: ERROR: must be owner of table orders"}}, nil
		}
		return nil, nil
	}}
	d.ConnectLog = fl
	d.Now = func() time.Time { return logNow }
	r := runLog(t, scopeTool(t, d, "connect_worker_log"), `{}`)
	if !strings.Contains(r.Summary, "must be owner of table orders") {
		t.Fatalf("Debezium's own line was dropped; queries %+v\n%s", fl.queries, r.Summary)
	}
	var names []string
	for _, q := range fl.queries {
		names = append(names, q.Connector)
	}
	if !strings.Contains(strings.Join(names, ","), "pg-orders-connector") {
		t.Fatalf("the connector itself was not read: %v", names)
	}
}

// An entry that names the room's connector and another pipeline's is not
// returned (Q2 F17): the shared worker must not show other pipelines.
func TestConnectWorkerLogDropsEntriesNamingOtherConnectors(t *testing.T) {
	at := time.Date(2026, 10, 15, 4, 10, 9, 0, time.UTC)
	d, _ := scopeFakes(t) // Connect lists pg-orders-connector, bill-connector, other-connector
	fl := &fakeLogReader{fn: func(context.Context, clients.WorkerLogQuery) ([]clients.LogLine, error) {
		return []clients.LogLine{
			{At: at, Level: "WARN", Message: "assignment connectorIds=[pg-orders-connector, bill-connector] taskIds=[bill-connector-0]"},
			{At: at.Add(-time.Second), Level: "WARN", Message: "pg-orders-connector-0 retrying"},
		}, nil
	}}
	d.ConnectLog = fl
	d.Now = func() time.Time { return logNow }
	r := runLog(t, scopeTool(t, d, "connect_worker_log"), `{"min_level":"WARN"}`)
	if strings.Contains(r.Summary, "bill-connector") {
		t.Fatalf("another pipeline's connector leaked:\n%s", r.Summary)
	}
	if !strings.Contains(r.Summary, "retrying") {
		t.Fatalf("the room's own line was dropped:\n%s", r.Summary)
	}
}
