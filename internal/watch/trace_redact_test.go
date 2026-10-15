package watch

import (
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
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

// A value that straddles the 300-rune cut of a trace line is masked whole.
// Before, the evidence kept "jane.doe@exam" (#392).
func TestTraceLineCutKeepsNoPartialEmail(t *testing.T) {
	trace := "org.apache.kafka.connect.errors.ConnectException: " + strings.Repeat("x", 203) +
		" failed to write row for customer jane.doe@example.com: constraint\n\tat org.foo.Bar"
	got := traceLine(trace)
	if strings.Contains(got, "jane.doe") || !strings.HasPrefix(got, "org.apache.kafka.connect.errors.ConnectException") {
		t.Fatalf("traceLine = %q", got)
	}
}

// DDL text goes into the change log: literals in it are masked as in a DLQ
// message, and the statement keeps its shape. Before, only redact.Text ran:
// the card and the SSN were stored and the e-mail kept its domain (#392).
func TestDDLSummaryMasksPII(t *testing.T) {
	h := newChangesHarness(t)
	h.step(0)
	q := "ALTER TABLE public.t ADD CONSTRAINT c CHECK (email <> 'jane.doe@example.com' AND card <> '4111111111111111' AND ssn <> '123-45-6789')"
	h.ddl.rows = []clients.DDLChange{{At: t0.Add(30 * time.Second), CommandTag: "ALTER TABLE", ObjectIdentity: "public.t", Query: q, Username: "app_migrator"}}
	h.step(time.Minute)
	rows := h.ch.all()
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want one", rows)
	}
	for _, leak := range []string{"jane.doe", "example.com", "4111111111111111", "123-45-6789"} {
		if strings.Contains(rows[0].Summary, leak) {
			t.Errorf("summary keeps %q: %q", leak, rows[0].Summary)
		}
	}
	if !strings.Contains(rows[0].Summary, "ADD CONSTRAINT c CHECK (email <> '") {
		t.Errorf("summary lost the statement: %q", rows[0].Summary)
	}
}

// TestOneLineCorpus pins what the change log keeps and masks (org.md §12):
// keep rows come back unchanged, as on main; mask rows lose the value.
func TestOneLineCorpus(t *testing.T) {
	for _, keep := range []string{
		"ALTER TABLE public.events ALTER COLUMN token TYPE varchar(64) USING token::varchar(64)",
		"ALTER TABLE public.t ALTER COLUMN access_token TYPE text USING access_token::text",
		"COMMENT ON TABLE public.t IS 'bearer token rotation policy'",
		"ALTER TABLE public.t ALTER COLUMN ts SET DEFAULT 'UTC+1 2026-10-15 18:00'",
		"ConnectException: lag +12500000 behind",
		"CREATE POLICY tenant ON public.sessions USING (session_token = current_setting('app.session')::text)",
		"CREATE INDEX users_token_idx ON public.users (token) WHERE token = NULL",
		"COMMENT ON TABLE public.t IS 'requires Bearer authentication.'",
		"ALTER TABLE public.t ALTER COLUMN l SET DEFAULT 'lag +12.345.678'",
		"ALTER TABLE public.t ALTER COLUMN d SET DEFAULT '+1 15.10.2026'",
	} {
		if got := oneLine(keep); got != keep {
			t.Errorf("oneLine(%q) = %q, want it unchanged", keep, got)
		}
	}
	for in, gone := range map[string]string{
		"ALTER TABLE public.t ADD CONSTRAINT c CHECK (card <> '2223 0031 2200 3222')": "2223",
		"ALTER ROLE app SET my.password = hunter2":                                    "hunter2",
	} {
		if got := oneLine(in); strings.Contains(got, gone) {
			t.Errorf("oneLine(%q) = %q, still has %q", in, got, gone)
		}
	}
	// URL userinfo is internal/redact's: the password goes whole, as on main,
	// and the e-mail mask never takes the '@' (round 3 of #435).
	for in, want := range map[string]string{
		"ALTER TABLE public.t ADD COLUMN backup_dsn text DEFAULT 'postgres://app:S3cr!t@db.example.com:5432/orders'": "ALTER TABLE public.t ADD COLUMN backup_dsn text DEFAULT 'postgres://app:[redacted 8]@db.example.com:5432/orders'",
		"x mysql://root:pa$$w0rd@mysql.example.net:3306/shop":                                                        "x mysql://root:[redacted 8]@mysql.example.net:3306/shop",
		"ALTER SUBSCRIPTION s CONNECTION 'postgresql://rep:Tr0ub4dor&3@primary.example.com/orders'":                  "ALTER SUBSCRIPTION s CONNECTION 'postgresql://rep:[redacted 16]@primary.example.com/orders'",
		"CREATE SUBSCRIPTION s CONNECTION 'postgresql://rep:Xy7*q9@primary.example.com:5432/orders' PUBLICATION p":   "CREATE SUBSCRIPTION s CONNECTION 'postgresql://rep:[redacted 8]@primary.example.com:5432/orders' PUBLICATION p",
		"COMMENT ON TABLE t IS 'source mysql://root:pa$$w0rd@mysql.example.net:3306/shop'":                           "COMMENT ON TABLE t IS 'source mysql://root:[redacted 8]@mysql.example.net:3306/shop'",
		"ALTER TABLE t ADD COLUMN u text DEFAULT 'https://user:p4ss#w0rd@api.example.com/x'":                         "ALTER TABLE t ADD COLUMN u text DEFAULT 'https://user:[redacted 16]@api.example.com/x'",
		"ALTER TABLE t ADD COLUMN u text DEFAULT 'amqp://guest:g^uest@rabbit.example.io/'":                           "ALTER TABLE t ADD COLUMN u text DEFAULT 'amqp://guest:[redacted 8]@rabbit.example.io/'",
		"ALTER TABLE t ADD COLUMN u text DEFAULT 'redis://:p!ss@cache.example.com:6379'":                             "ALTER TABLE t ADD COLUMN u text DEFAULT 'redis://:[redacted 8]@cache.example.com:6379'",
		"ALTER SERVER fdw OPTIONS (SET dsn 'postgres://app:S3cr!t@db.example.com/x')":                                "ALTER SERVER fdw OPTIONS (SET dsn 'postgres://app:[redacted 8]@db.example.com/x')",
		"postgres://app:hunter2@db.example.com/orders":                                                               "postgres://app:[redacted 8]@db.example.com/orders",
		"mongodb://app:a.b.c@mongo.example.com/db":                                                                   "mongodb://app:[redacted 8]@mongo.example.com/db",
		"postgres://app:p%40ss@db.example.com/x contact jane.doe@example.com":                                        "postgres://app:[redacted 8]@db.example.com/x contact <redacted>",
		"smtp://jane.doe@example.com:SmtpPw1@mail.example.com:587":                                                   "smtp://jane.doe@example.com:[redacted 8]@mail.example.com:587",
		"imaps://jane@example.com:ImapPw2@imap.example.com":                                                          "imaps://jane@example.com:[redacted 8]@imap.example.com",
		"https://api.example.com?u=jane@example.com&k=v":                                                             "https://api.example.com?u=<redacted>&k=v",
		"see http://a.example.com/ email:jane@example.com":                                                           "see http://a.example.com/ email:<redacted>",
	} {
		if got := oneLine(in); got != want {
			t.Errorf("oneLine(%q) = %q, want %q", in, got, want)
		}
	}
	// Numeric literals the PII masks take for a phone, card or SSN: main
	// kept them. Pinned at today's output (no name or type is hidden); see
	// "Not done" in #435.
	for in, want := range map[string]string{
		"ALTER TABLE public.t ADD CONSTRAINT c CHECK (amount < 555-123-4567)": "ALTER TABLE public.t ADD CONSTRAINT c CHECK (amount < <redacted>)",
		"ALTER TABLE public.t ALTER COLUMN n SET DEFAULT 4111111111111111":    "ALTER TABLE public.t ALTER COLUMN n SET DEFAULT <redacted>",
		"CREATE SEQUENCE public.s START WITH 4111111111111111":                "CREATE SEQUENCE public.s START WITH <redacted>",
		"ALTER TABLE public.t ALTER COLUMN c SET DEFAULT '202-61-0150'":       "ALTER TABLE public.t ALTER COLUMN c SET DEFAULT '<redacted>'",
		"ALTER TABLE public.t ADD CONSTRAINT c CHECK (n IN (555.123.4567))":   "ALTER TABLE public.t ADD CONSTRAINT c CHECK (n IN <redacted>))",
		// The host after a userinfo that is itself an e-mail, or that
		// internal/redact masked with a space in it: no secret, a host lost.
		"postgres://jane.doe@example.com@db.example.com/x":                                                "postgres://[redacted 8]@<redacted>/x",
		"s3://AKIA" + "IOSFODNN7EXAMPLE:wJalr" + "XUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY@bucket.example.com/x": "s3://AKIA…[redacted 24]…LE:<redacted>/x",
	} {
		if got := oneLine(in); got != want {
			t.Errorf("known over-mask changed: oneLine(%q) = %q, want %q", in, got, want)
		}
	}
	// #426 is split out of #435 (org.md §12): international phones are kept,
	// as on main.
	gap := "ALTER TABLE public.t ADD CONSTRAINT c CHECK (phone <> '+44 20 7946 0958')"
	if got := oneLine(gap); got != gap {
		t.Errorf("gap (#426) changed: oneLine(%q) = %q (fixed? flip the row)", gap, got)
	}
}

// #440: a Connect trace whose first line carries a PostgreSQL key detail
// without a "Detail:" label keeps the columns and loses the values.
func TestTraceLineMasksKeyValues(t *testing.T) {
	for in, want := range map[string]string{
		"org.apache.kafka.connect.errors.ConnectException: ERROR: duplicate key value violates unique constraint \"u_name\" Key (full_name)=(Jane Doe) already exists.\n\tat io.debezium.Foo.bar(Foo.java:1)": "org.apache.kafka.connect.errors.ConnectException: ERROR: duplicate key value violates unique constraint <redacted> Key (full_name)=<redacted> already exists.",
		"org.apache.kafka.connect.errors.ConnectException: Key (tax_id, region)=(AB123456, EU) is not present in table t.":                                                                                    "org.apache.kafka.connect.errors.ConnectException: Key (tax_id, region)=<redacted> is not present in table t.",
	} {
		if got := traceLine(in); got != want {
			t.Errorf("traceLine(%q) = %q, want %q", in, got, want)
		}
	}
}
