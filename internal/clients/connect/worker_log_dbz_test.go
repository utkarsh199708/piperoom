package connect

import (
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// Debezium logs with its logical name (topic.prefix), not the Connect
// connector name: the MDC and the task thread carry "pg". The layout is the
// Debezium image's; lines are written by hand (no real capture was available).
func TestWorkerLogDebeziumLogicalName(t *testing.T) {
	log := "2026-10-15 04:10:09,123 ERROR  Postgres|pg|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]\n" +
		"org.apache.kafka.connect.errors.ConnectException: Unable to update filtered publication\n" +
		"Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders\n" +
		"2026-10-15 04:10:10,000 WARN  [debezium-postgresconnector-pg-change-event-source-coordinator]  retriable   [io.debezium.pipeline.ChangeEventSourceCoordinator]\n" +
		"2026-10-15 04:10:11,000 WARN  pgbouncer and pg_hba noise, topic pg.public.orders, pg-orders-eu failed   [x.Y]\n" +
		"2026-10-15 04:10:12,000 WARN  [debezium-postgresconnector-pgx-change-event-source-coordinator]  other   [x.Y]\n"
	got := readLog(t, log, clients.WorkerLogQuery{Connector: "pg"})
	if len(got) != 2 {
		t.Fatalf("got %d lines, want the MDC and the thread line: %+v", len(got), got)
	}
	if !strings.Contains(got[1].Message, "must be owner of table orders") {
		t.Errorf("cause lost: %q", got[1].Message)
	}
}

func TestMentionsName(t *testing.T) {
	if !MentionsName("x pg-orders-0 y", "pg-orders") || MentionsName("x pg-payments y", "pg-orders") || MentionsName("pg", "") {
		t.Fatal("MentionsName wrong")
	}
}
