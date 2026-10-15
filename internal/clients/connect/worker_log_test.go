package connect

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
)

// debeziumLog is the connector-auth break as the Debezium image logs it: the
// task is RUNNING for Connect while it crash-loops on a permission error.
const debeziumLog = `2026-10-15 04:00:01,100 INFO   ||  Kafka Connect started   [org.apache.kafka.connect.runtime.Connect]
2026-10-15 04:09:58,500 INFO   Postgres|pg-orders-connector|streaming  Starting streaming   [io.debezium.pipeline.ChangeEventSourceCoordinator]
2026-10-15 04:10:09,123 ERROR  Postgres|pg-orders-connector|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]
org.apache.kafka.connect.errors.ConnectException: Unable to update filtered publication dbz_publication for "public"."orders"
	at io.debezium.connector.postgresql.connection.PostgresReplicationConnection.initPublication(PostgresReplicationConnection.java:219)
	at io.debezium.connector.postgresql.connection.PostgresReplicationConnection.createReplicationSlot(PostgresReplicationConnection.java:300)
	at io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.execute(PostgresStreamingChangeEventSource.java:105)
Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders
	at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2713)
	at org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2401)
	... 12 more
2026-10-15 04:10:09,300 WARN   Postgres|pg-orders-connector|streaming  Retrying after a retriable error   [io.debezium.connector.common.BaseSourceTask]
2026-10-15 04:10:10,000 ERROR  Postgres|pg-payments-connector|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]
2026-10-15 04:10:11,000 INFO   Postgres|pg-orders-connector|streaming  Restarted   [io.debezium.pipeline.ChangeEventSourceCoordinator]
`

// kafkaLog is the same break in the Apache Kafka layout.
const kafkaLog = `[2026-10-15 04:00:01,100] INFO Kafka Connect started (org.apache.kafka.connect.runtime.Connect:57)
[2026-10-15 04:10:09,123] ERROR [pg-orders-connector|task-0] WorkerSourceTask{id=pg-orders-connector-0} Task threw an uncaught and unrecoverable exception (org.apache.kafka.connect.runtime.WorkerTask:234)
org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java:260)
Caused by: org.postgresql.util.PSQLException: ERROR: compressed lz4 data is corrupt
	... 5 more
[2026-10-15 04:10:09,400] WARN [pg-orders-connector|task-0] Reconnecting (org.apache.kafka.connect.runtime.WorkerSourceTask:80)
[2026-10-15 04:10:10,000] ERROR [pg-ordersx|task-0] another connector (org.apache.kafka.connect.runtime.WorkerTask:234)
`

func writeLog(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "connect.log")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func readLog(t *testing.T, content string, q clients.WorkerLogQuery) []clients.LogLine {
	t.Helper()
	got, err := NewLogReader(writeLog(t, content)).WorkerLog(context.Background(), q)
	if err != nil {
		t.Fatalf("WorkerLog: %v", err)
	}
	return got
}

func levels(ls []clients.LogLine) string {
	var parts []string
	for _, l := range ls {
		parts = append(parts, l.Level)
	}
	return strings.Join(parts, ",")
}

func TestWorkerLogDebeziumLayout(t *testing.T) {
	got := readLog(t, debeziumLog, clients.WorkerLogQuery{Connector: "pg-orders-connector"})
	// WARN is the default floor, newest first, the other connector's line and
	// the INFO lines are out.
	if levels(got) != "WARN,ERROR" {
		t.Fatalf("levels = %s, want WARN,ERROR: %+v", levels(got), got)
	}
	errLine := got[1]
	if want := time.Date(2026, 10, 15, 4, 10, 9, 123_000_000, time.UTC); !errLine.At.Equal(want) {
		t.Errorf("At = %v, want %v", errLine.At, want)
	}
	if errLine.Logger != "io.debezium.pipeline.ErrorHandler" {
		t.Errorf("Logger = %q", errLine.Logger)
	}
	// The MDC stays (it names the connector and the phase); the logger does not.
	if !strings.HasPrefix(errLine.Message, "Postgres|pg-orders-connector|streaming Producer failure\n") {
		t.Errorf("Message head = %q", errLine.Message)
	}
	for _, want := range []string{
		"ConnectException: Unable to update filtered publication dbz_publication",
		"Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders",
	} {
		if !strings.Contains(errLine.Message, want) {
			t.Errorf("Message lacks %q:\n%s", want, errLine.Message)
		}
	}
	// Three frames under the exception, then two frames and "... 12 more" (one
	// line) under the cause.
	if n := strings.Count(errLine.Message, "[3 stack frames omitted]"); n != 2 {
		t.Errorf("frame markers = %d, want 2:\n%s", n, errLine.Message)
	}
	if strings.Contains(errLine.Message, "QueryExecutorImpl") {
		t.Errorf("a stack frame was kept:\n%s", errLine.Message)
	}
}

func TestWorkerLogKafkaLayout(t *testing.T) {
	got := readLog(t, kafkaLog, clients.WorkerLogQuery{Connector: "pg-orders-connector"})
	if levels(got) != "WARN,ERROR" {
		t.Fatalf("levels = %s: %+v", levels(got), got)
	}
	e := got[1]
	if e.Logger != "org.apache.kafka.connect.runtime.WorkerTask" {
		t.Errorf("Logger = %q (the :line must go)", e.Logger)
	}
	if !strings.HasPrefix(e.Message, "[pg-orders-connector|task-0] WorkerSourceTask{id=pg-orders-connector-0} Task threw") {
		t.Errorf("Message = %q", e.Message)
	}
	if !strings.Contains(e.Message, "Caused by: org.postgresql.util.PSQLException: ERROR: compressed lz4 data is corrupt") {
		t.Errorf("root cause lost: %q", e.Message)
	}
}

func TestWorkerLogTimeForms(t *testing.T) {
	cases := []struct {
		name, line string
		want       time.Time
	}{
		{"comma ms", "2026-10-15 04:10:09,123 ERROR c-0 x", time.Date(2026, 10, 15, 4, 10, 9, 123e6, time.UTC)},
		{"dot ms", "2026-10-15 04:10:09.5 ERROR c-0 x", time.Date(2026, 10, 15, 4, 10, 9, 500e6, time.UTC)},
		{"no ms", "2026-10-15 04:10:09 ERROR c-0 x", time.Date(2026, 10, 15, 4, 10, 9, 0, time.UTC)},
		{"T and Z", "2026-10-15T04:10:09,123Z ERROR c-0 x", time.Date(2026, 10, 15, 4, 10, 9, 123e6, time.UTC)},
		{"offset to UTC", "2026-10-15T06:10:09+02:00 ERROR c-0 x", time.Date(2026, 10, 15, 4, 10, 9, 0, time.UTC)},
		{"bracketed", "[2026-10-15 04:10:09,123] ERROR c-0 x", time.Date(2026, 10, 15, 4, 10, 9, 123e6, time.UTC)},
		{"docker prefix", "2026-10-15T04:10:09.999999999Z 2026-10-15 04:10:09,123 ERROR c-0 x", time.Date(2026, 10, 15, 4, 10, 9, 123e6, time.UTC)},
		{"month 13 has no time", "2026-13-15 04:10:09,123 ERROR c-0 x", time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := readLog(t, tc.line+"\n", clients.WorkerLogQuery{Connector: "c"})
			if len(got) != 1 || !got[0].At.Equal(tc.want) {
				t.Fatalf("got %+v, want At %v", got, tc.want)
			}
		})
	}
}

func TestWorkerLogLevelFilter(t *testing.T) {
	log := strings.Join([]string{
		"2026-10-15 04:00:01,000 DEBUG c d",
		"2026-10-15 04:00:02,000 INFO c i",
		"2026-10-15 04:00:03,000 WARN c w",
		"2026-10-15 04:00:04,000 ERROR c e",
		"2026-10-15 04:00:05,000 FATAL c f",
	}, "\n") + "\n"
	cases := []struct{ min, want string }{
		{"", "FATAL,ERROR,WARN"},
		{"WARN", "FATAL,ERROR,WARN"},
		{"warning", "FATAL,ERROR,WARN"},
		{"INFO", "FATAL,ERROR,WARN,INFO"},
		{"error", "FATAL,ERROR"},
	}
	for _, tc := range cases {
		t.Run("min="+tc.min, func(t *testing.T) {
			got := readLog(t, log, clients.WorkerLogQuery{Connector: "c", MinLevel: tc.min})
			if levels(got) != tc.want {
				t.Fatalf("levels = %s, want %s", levels(got), tc.want)
			}
		})
	}
	_, err := NewLogReader(writeLog(t, log)).WorkerLog(context.Background(), clients.WorkerLogQuery{Connector: "c", MinLevel: "DEBUG"})
	if err == nil || !strings.Contains(err.Error(), "want INFO, WARN or ERROR") {
		t.Fatalf("DEBUG floor: err = %v", err)
	}
}

func TestWorkerLogSince(t *testing.T) {
	since := time.Date(2026, 10, 15, 4, 10, 0, 0, time.UTC)
	got := readLog(t, debeziumLog, clients.WorkerLogQuery{Connector: "pg-orders-connector", Since: since, MinLevel: "INFO"})
	if levels(got) != "INFO,WARN,ERROR" {
		t.Fatalf("levels = %s: the 04:09:58 INFO line is older than Since", levels(got))
	}
	// Since is compared as an instant, whatever zone it carries.
	zone := time.FixedZone("x", 5*3600)
	got = readLog(t, debeziumLog, clients.WorkerLogQuery{Connector: "pg-orders-connector", Since: since.In(zone)})
	if len(got) != 2 {
		t.Fatalf("zoned Since: %d lines", len(got))
	}
	got = readLog(t, debeziumLog, clients.WorkerLogQuery{Connector: "pg-orders-connector", Since: since.Add(time.Hour)})
	if len(got) != 0 {
		t.Fatalf("future Since: %+v", got)
	}
}

func TestWorkerLogConnectorMatching(t *testing.T) {
	const logTmpl = "2026-10-15 04:00:00,000 ERROR %s\n"
	cases := []struct {
		name, connector, text string
		want                  bool
	}{
		{"debezium MDC", "pg-orders", "Postgres|pg-orders|streaming boom", true},
		{"kafka context", "pg-orders", "[pg-orders|task-0] boom", true},
		{"task id suffix", "pg-orders", "WorkerSourceTask{id=pg-orders-0} boom", true},
		{"multi digit task", "pg-orders", "task pg-orders-12 failed", true},
		{"end of sentence", "pg-orders", "Connector pg-orders.", true},
		{"start of text", "pg-orders", "pg-orders failed", true},
		{"longer name after", "pg-orders", "pg-orders-eu failed", false},
		{"longer name before", "pg-orders", "legacy-pg-orders failed", false},
		{"suffix with word", "pg-orders", "pg-orders_backup failed", false},
		{"dotted longer name", "pg-orders", "pg-orders.v2 failed", false},
		{"dotted prefix", "pg-orders", "x.pg-orders failed", false},
		{"other connector", "pg-orders", "pg-payments failed", false},
		{"task suffix then name", "pg-orders", "pg-orders-0-1 failed", false},
		{"later occurrence matches", "pg-orders", "pg-orders-eu then pg-orders|task-0", true},
		{"case sensitive", "pg-orders", "PG-ORDERS failed", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := readLog(t, fmt.Sprintf(logTmpl, tc.text), clients.WorkerLogQuery{Connector: tc.connector})
			if (len(got) == 1) != tc.want {
				t.Fatalf("matched = %v, want %v (%+v)", len(got) == 1, tc.want, got)
			}
		})
	}
}

// The connector name may be only in the trace: the entry still belongs to it.
func TestWorkerLogConnectorNameInFoldedLine(t *testing.T) {
	log := "2026-10-15 04:00:00,000 ERROR something failed   [a.B]\nCaused by: x: task pg-orders-0 died\n"
	if got := readLog(t, log, clients.WorkerLogQuery{Connector: "pg-orders"}); len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestWorkerLogMaxLines(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&sb, "2026-10-15 04:00:00,%03d ERROR c-0 line %d\n", i%1000, i)
	}
	cases := []struct{ max, want int }{{0, 50}, {-5, 50}, {1, 1}, {120, 120}, {200, 200}, {5000, 200}}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.max), func(t *testing.T) {
			got := readLog(t, sb.String(), clients.WorkerLogQuery{Connector: "c", MaxLines: tc.max})
			if len(got) != tc.want {
				t.Fatalf("got %d lines, want %d", len(got), tc.want)
			}
			if !strings.HasSuffix(got[0].Message, "line 299") {
				t.Fatalf("newest first: first = %q", got[0].Message)
			}
		})
	}
}

func TestWorkerLogMessageCutAtRuneBoundary(t *testing.T) {
	// 3-byte runes: a cut at 2048 bytes would land inside one.
	long := strings.Repeat("日", 2000)
	got := readLog(t, "2026-10-15 04:00:00,000 ERROR c-0 "+long+"\n", clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	m := got[0].Message
	if len(m) > 2048 {
		t.Fatalf("Message is %d bytes, want <= 2048", len(m))
	}
	if !utf8.ValidString(m) || !strings.HasSuffix(m, "…") {
		t.Fatalf("Message not cut cleanly: valid=%v tail=%q", utf8.ValidString(m), m[len(m)-6:])
	}
	// Ordinary ASCII is cut at the limit too.
	got = readLog(t, "2026-10-15 04:00:00,000 ERROR c-0 "+strings.Repeat("a", 5000)+"\n", clients.WorkerLogQuery{Connector: "c"})
	if len(got[0].Message) != 2048 {
		t.Fatalf("ASCII message is %d bytes, want exactly 2048", len(got[0].Message))
	}
}

// A long trace must not push the root cause out of the 2 KiB window: frames
// are counted, not kept.
func TestWorkerLogLongTraceKeepsCausedBy(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("2026-10-15 04:00:00,000 ERROR c-0 Producer failure   [a.B]\nx.Top: top\n")
	for i := 0; i < 400; i++ {
		fmt.Fprintf(&sb, "\tat some.very.long.package.name.Class%d.method(Class%d.java:%d)\n", i, i, i)
	}
	sb.WriteString("Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders\n")
	for i := 0; i < 400; i++ {
		fmt.Fprintf(&sb, "\tat other.package.Class%d.method(Class%d.java:%d)\n", i, i, i)
	}
	got := readLog(t, sb.String(), clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 || !strings.Contains(got[0].Message, "must be owner of table orders") {
		t.Fatalf("root cause lost: %+v", got)
	}
	if !strings.Contains(got[0].Message, "[400 stack frames omitted]") {
		t.Fatalf("frame count missing: %q", got[0].Message)
	}
}

// A runaway entry is bounded while it is folded.
func TestWorkerLogEntryIsBounded(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("2026-10-15 04:00:00,000 ERROR c-0 head\n")
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&sb, "Caused by: line number %d with some padding to be long enough\n", i)
	}
	got := readLog(t, sb.String(), clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 || len(got[0].Message) > 2048 {
		t.Fatalf("got %d lines, first %d bytes", len(got), len(got[0].Message))
	}
}

func TestWorkerLogLevelOnlyFallback(t *testing.T) {
	log := strings.Join([]string{
		"INFO  Postgres|c|streaming  fine   [io.debezium.X]",
		"ERROR  Postgres|c|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]",
		"org.apache.kafka.connect.errors.ConnectException: nope",
		"\tat a.b.C.d(C.java:1)",
		"ERROR: this is a trace line, not a header",
		"WARN  Postgres|other|streaming  other connector",
	}, "\n") + "\n"
	got := readLog(t, log, clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	e := got[0]
	if !e.At.IsZero() || e.Level != "ERROR" || e.Logger != "io.debezium.pipeline.ErrorHandler" {
		t.Errorf("entry = %+v", e)
	}
	if !strings.Contains(e.Message, "ConnectException: nope") || !strings.Contains(e.Message, "ERROR: this is a trace line") {
		t.Errorf("continuation not folded: %q", e.Message)
	}
	// With no time, Since cannot drop anything.
	got = readLog(t, log, clients.WorkerLogQuery{Connector: "c", Since: time.Now()})
	if len(got) != 1 {
		t.Fatalf("Since dropped a line with no time: %+v", got)
	}
}

// In a log with times, a trace line that starts with a level word is a
// continuation, not a header.
func TestWorkerLogLevelWordInTraceIsNotAHeader(t *testing.T) {
	log := "2026-10-15 04:00:00,000 ERROR c-0 head\nERROR also part of the trace\n"
	got := readLog(t, log, clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 || !strings.Contains(got[0].Message, "also part of the trace") {
		t.Fatalf("got %+v", got)
	}
}

func TestWorkerLogDockerTimestamps(t *testing.T) {
	log := strings.Join([]string{
		"2026-10-15T04:10:09.100000000Z 2026-10-15 04:10:09,100 ERROR  Postgres|c|streaming  boom   [a.B]",
		"2026-10-15T04:10:09.100000001Z org.x.Exc: bad",
		"2026-10-15T04:10:09.100000002Z \tat a.b.C.d(C.java:1)",
		"2026-10-15T04:10:09.100000003Z Caused by: y.Z: root",
	}, "\n") + "\n"
	got := readLog(t, log, clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	m := got[0].Message
	if strings.Contains(m, "2026-10-15T04:10:09") || !strings.Contains(m, "Caused by: y.Z: root") || !strings.Contains(m, "[1 stack frame omitted]") {
		t.Fatalf("Message = %q", m)
	}
}

func TestWorkerLogDockerPrefixOnLevelOnlyLog(t *testing.T) {
	log := "2026-10-15T04:10:09.5Z ERROR  Postgres|c|streaming  boom   [a.B]\n"
	got := readLog(t, log, clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 || !got[0].At.Equal(time.Date(2026, 10, 15, 4, 10, 9, 500e6, time.UTC)) {
		t.Fatalf("got %+v", got)
	}
}

func TestWorkerLogCRLFAndControlChars(t *testing.T) {
	log := "2026-10-15 04:00:00,000 ERROR c-0 \x1b[31mred\x1b[0m text\x00 \xff end\r\nCaused by: x\r\n"
	got := readLog(t, log, clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	m := got[0].Message
	if strings.ContainsAny(m, "\x1b\x00\r") || !utf8.ValidString(m) || !strings.Contains(m, "Caused by: x") {
		t.Fatalf("Message = %q", m)
	}
}

func TestWorkerLogUnparseableAndEmpty(t *testing.T) {
	for name, content := range map[string]string{
		"empty":       "",
		"json lines":  `{"timestamp":"2026-10-15T04:00:00Z","level":"ERROR","message":"c-0 failed"}` + "\n",
		"plain text":  "hello\nworld\n",
		"only blanks": "\n\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			got := readLog(t, content, clients.WorkerLogQuery{Connector: "c"})
			if len(got) != 0 {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

// Only the last 4 MiB are read.
func TestWorkerLogReadsOnlyTheTail(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("2026-10-15 03:00:00,000 ERROR c-0 OLD-MARKER before the tail\n")
	pad := "2026-10-15 03:30:00,000 INFO c-0 " + strings.Repeat("p", 1000) + "\n"
	for sb.Len() < workerLogTailBytes+10_000 {
		sb.WriteString(pad)
	}
	sb.WriteString("2026-10-15 04:00:00,000 ERROR c-0 NEW-MARKER\n")
	got := readLog(t, sb.String(), clients.WorkerLogQuery{Connector: "c"})
	if len(got) != 1 || !strings.Contains(got[0].Message, "NEW-MARKER") {
		t.Fatalf("got %+v", got)
	}
}

// The window starts at size-4MiB. When that is inside a line the rest of the
// line is dropped, even when it looks like a header; when it is exactly on a
// line start the line is kept.
func TestWorkerLogTailCutPoint(t *testing.T) {
	const keep = "2026-10-15 04:00:00,000 ERROR c-0 KEEP\n"
	// build returns head + filler + keep, exactly workerLogTailBytes+cut bytes
	// long, so the window starts cut bytes into head. The filler is one long
	// non-header line.
	build := func(head string, cut int) string {
		fill := workerLogTailBytes + cut - len(head) - len(keep)
		return head + strings.Repeat("x", fill-1) + "\n" + keep
	}
	t.Run("inside a line", func(t *testing.T) {
		// 19 filler bytes, then a header: the window starts one byte into it
		// (cut=20), and the byte read before the window starts at the header.
		head := strings.Repeat("j", 19) + "2026-10-15 03:00:00,000 ERROR c-0 PARTIAL\n"
		data := build(head, 20)
		tail, err := readTail(writeLog(t, data))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(string(tail), "xxxx") {
			t.Fatalf("the tail starts with %q, want the line after the cut one", tail[:40])
		}
		got := readLog(t, data, clients.WorkerLogQuery{Connector: "c"})
		if len(got) != 1 || !strings.Contains(got[0].Message, "KEEP") {
			t.Fatalf("a header-looking tail of a cut line was read: %+v", got)
		}
	})
	t.Run("on a line start", func(t *testing.T) {
		old := "2026-10-15 03:00:00,000 ERROR c-0 OLDLINE\n"
		head := old + "2026-10-15 03:30:00,000 ERROR c-0 EDGE\n"
		data := build(head, len(old))
		tail, err := readTail(writeLog(t, data))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(string(tail), "2026-10-15 03:30:00,000 ERROR c-0 EDGE\n") {
			t.Fatalf("the tail starts with %q, want the line at the cut", tail[:40])
		}
		got := readLog(t, data, clients.WorkerLogQuery{Connector: "c"})
		var msgs []string
		for _, l := range got {
			msgs = append(msgs, l.Message)
		}
		all := strings.Join(msgs, "|")
		if !strings.Contains(all, "EDGE") || strings.Contains(all, "OLDLINE") || !strings.Contains(all, "KEEP") {
			t.Fatalf("got %q", all)
		}
	})
	t.Run("one giant line", func(t *testing.T) {
		got := readLog(t, strings.Repeat("y", workerLogTailBytes+100), clients.WorkerLogQuery{Connector: "c"})
		if len(got) != 0 {
			t.Fatalf("got %+v", got)
		}
	})
}

func TestWorkerLogErrors(t *testing.T) {
	ctx := context.Background()
	q := clients.WorkerLogQuery{Connector: "c"}

	t.Run("no log source", func(t *testing.T) {
		for _, r := range []*LogReader{NewLogReader(""), NewLogReader("  "), nil} {
			_, err := r.WorkerLog(ctx, q)
			if !errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "CONNECT_WORKER_LOG_FILE") {
				t.Fatalf("err = %v, want ErrNotSupported naming the setting", err)
			}
		}
	})
	t.Run("missing file", func(t *testing.T) {
		_, err := NewLogReader(filepath.Join(t.TempDir(), "nope.log")).WorkerLog(ctx, q)
		if !errors.Is(err, fs.ErrNotExist) || !strings.Contains(err.Error(), "CONNECT_WORKER_LOG_FILE") {
			t.Fatalf("err = %v, want a wrapped not-exist naming the setting", err)
		}
		if errors.Is(err, clients.ErrNotSupported) {
			t.Fatalf("a missing file is a failure, not 'not supported': %v", err)
		}
	})
	t.Run("directory", func(t *testing.T) {
		_, err := NewLogReader(t.TempDir()).WorkerLog(ctx, q)
		if err == nil || !strings.Contains(err.Error(), "directory") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("connector required", func(t *testing.T) {
		_, err := NewLogReader(writeLog(t, debeziumLog)).WorkerLog(ctx, clients.WorkerLogQuery{})
		if err == nil || !strings.Contains(err.Error(), "connector is required") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("cancelled before the read", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		// The file does not exist: a cancelled context is reported first, so
		// the read never starts.
		_, err := NewLogReader(filepath.Join(t.TempDir(), "nope.log")).WorkerLog(cctx, q)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
	t.Run("deadline already passed", func(t *testing.T) {
		cctx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		_, err := NewLogReader(writeLog(t, debeziumLog)).WorkerLog(cctx, q)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
	})
}

func TestMentionsConnectorEmptyAndUnicode(t *testing.T) {
	if mentionsConnector("anything", "zzz") {
		t.Error("absent name matched")
	}
	if !mentionsConnector("コネクタ pg-orders ok", "pg-orders") {
		t.Error("name next to non-ASCII text not found")
	}
	if mentionsConnector("éxpg-orders", "pg-orders") {
		t.Error("a letter before the name is a longer name")
	}
}
