package watch

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// secretTrace is a Connect task trace whose first line carries a record value
// and a connection password.
const secretTrace = "org.apache.kafka.connect.errors.ConnectException: jdbc:postgresql://app:S3cretPw@db:5432/orders " +
	"failed for jane.doe@example.com password=hunter2\n\tat io.debezium.Foo.bar(Foo.java:1)"

var secretParts = []string{"S3cretPw", "jane.doe", "hunter2"}

func assertNoSecret(t *testing.T, where, s string) {
	t.Helper()
	for _, p := range secretParts {
		if strings.Contains(s, p) {
			t.Errorf("%s holds %q: %s", where, p, s)
		}
	}
}

// Task traces reach the room evidence, the room note and the change log; the
// first line was only cut and cleaned, never redacted.
func TestTaskTraceIsRedacted(t *testing.T) {
	c := newChangesHarness(t)
	c.step(0)
	c.breakConnector("orders", secretTrace)
	c.steps(4, time.Minute)
	rooms := c.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("want one room, got %d\n%s", len(rooms), c.logs)
	}
	if !strings.Contains(string(rooms[0].alert.Raw), "ConnectException") {
		t.Fatalf("the trace line is gone from the evidence: %s", rooms[0].alert.Raw)
	}
	assertNoSecret(t, "evidence", string(rooms[0].alert.Raw))
	assertNoSecret(t, "title", rooms[0].alert.Title)
	for _, row := range c.ch.all() {
		assertNoSecret(t, "change row", row.Summary+string(row.Before)+string(row.After))
	}
	assertNoSecret(t, "logs", c.logs.String())
}

// A crash-looping task reads RUNNING; its evidence shows the last FAILED trace,
// which must be redacted too.
func TestCrashLoopTraceIsRedacted(t *testing.T) {
	h := newHarness(t, opts{})
	h.step(0)
	for range 3 {
		h.breakConnector("orders", secretTrace)
		h.conn.mu.Lock()
		st := h.conn.status["pg-orders"]
		st.State = "RUNNING" // Connect keeps the connector RUNNING when a task fails
		h.conn.status["pg-orders"] = st
		h.conn.mu.Unlock()
		h.step(time.Minute)
		h.fixConnector("orders")
		h.step(time.Minute)
	}
	h.steps(4, time.Minute)
	var raw string
	for _, o := range h.rooms.created() {
		raw += string(o.alert.Raw)
	}
	if !strings.Contains(raw, "crash_loop") || !strings.Contains(raw, "ConnectException") {
		t.Fatalf("no crash-loop evidence with a trace: %s\n%s", raw, h.logs)
	}
	assertNoSecret(t, "crash-loop evidence", raw)
}

// A failed check is logged with the client's error text, which can hold a
// response body from Kafka Connect.
func TestCheckErrorLogIsRedacted(t *testing.T) {
	h := newHarness(t, opts{})
	h.conn.setStatusErr("pg-orders", errors.New(`connect: GET /connectors/pg-orders/status: status 500: {"message":"bad config connection.url=jdbc:postgresql://app:S3cretPw@db/orders password=hunter2"}`))
	h.step(0)
	if !strings.Contains(h.logs.String(), "check failed") {
		t.Fatalf("no warning\n%s", h.logs)
	}
	assertNoSecret(t, "logs", h.logs.String())
}

// A task trace whose first line is longer than the cap: the quoted record value
// runs past the cut, so its closing quote is gone.
func TestLongTraceQuoteCutIsRedacted(t *testing.T) {
	line := "org.apache.kafka.connect.errors.DataException: Failed to convert value for customer 'Jane Q. Doe, 12 Elm St, " +
		strings.Repeat("filler ", 50) + "' at offset 5"
	got := traceLine(line + "\n\tat io.debezium.Foo.bar(Foo.java:1)")
	if strings.Contains(got, "Jane") || strings.Contains(got, "Elm St") || !strings.Contains(got, "DataException") {
		t.Fatalf("traceLine = %q", got)
	}
	if n := len([]rune(got)); n > maxTraceChars {
		t.Fatalf("traceLine is %d runes, cap %d", n, maxTraceChars)
	}
}
