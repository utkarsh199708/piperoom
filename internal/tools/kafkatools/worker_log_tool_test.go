package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeLogReader implements clients.ConnectLogReader and remembers the queries.
type fakeLogReader struct {
	fn      func(context.Context, clients.WorkerLogQuery) ([]clients.LogLine, error)
	queries []clients.WorkerLogQuery
}

func (f *fakeLogReader) WorkerLog(ctx context.Context, q clients.WorkerLogQuery) ([]clients.LogLine, error) {
	f.queries = append(f.queries, q)
	return f.fn(ctx, q)
}

var logNow = time.Date(2026, 10, 15, 4, 30, 0, 0, time.UTC)

// logTool registers connect_worker_log over a reader that returns lines.
func logTool(t *testing.T, lines []clients.LogLine, readErr error) (tools.Tool, *fakeLogReader) {
	t.Helper()
	fl := &fakeLogReader{fn: func(context.Context, clients.WorkerLogQuery) ([]clients.LogLine, error) {
		return lines, readErr
	}}
	d, _ := scopeFakes(t)
	d.ConnectLog = fl
	d.Now = func() time.Time { return logNow }
	return scopeTool(t, d, "connect_worker_log"), fl
}

func runLog(t *testing.T, tool tools.Tool, input string) tools.Result {
	t.Helper()
	r, err := tool.Run(roomCtx(), json.RawMessage(input))
	if err != nil {
		t.Fatalf("connect_worker_log %s: %v", input, err)
	}
	return r
}

// authLog is what the connector-auth break leaves in the worker log: the same
// error every few seconds, newest first, and one WARN.
func authLog(n int) []clients.LogLine {
	var out []clients.LogLine
	at := time.Date(2026, 10, 15, 4, 10, 9, 0, time.UTC)
	out = append(out, clients.LogLine{At: at, Level: "WARN", Message: "Retrying after a retriable error"})
	for i := 0; i < n; i++ {
		out = append(out, clients.LogLine{
			At: at.Add(-time.Duration(i+1) * 10 * time.Second), Level: "ERROR", Logger: "io.debezium.pipeline.ErrorHandler",
			Message: "Postgres|pg-orders-connector|streaming Producer failure\n" +
				"org.apache.kafka.connect.errors.ConnectException: Unable to update filtered publication dbz_publication\n" +
				"[3 stack frames omitted]\n" +
				"Caused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders\n" +
				"[2 stack frames omitted]",
		})
	}
	return out
}

func TestConnectWorkerLogSummary(t *testing.T) {
	tool, fl := logTool(t, authLog(35), nil)
	r := runLog(t, tool, `{}`)

	for _, want := range []string{
		"Connect worker log for pg-orders-connector (the pipeline's source connector)",
		"last 30 min, WARN and above: 36 lines (ERROR 35, WARN 1)",
		"Newest ERROR/WARN", // see below: the newest line here is the WARN
		"Distinct messages, newest first (2 of 2)",
		"- x35 ERROR 2026-10-15T04:04:19Z to 2026-10-15T04:09:59Z",
		"root cause: org.postgresql.util.PSQLException: ERROR: must be owner of table orders",
	} {
		if want == "Newest ERROR/WARN" {
			want = "Newest WARN at 2026-10-15T04:10:09Z: Retrying after a retriable error"
		}
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	if len(r.Summary) > tools.MaxSummaryBytes || len(r.Summary) > 2000 {
		t.Errorf("summary is %d bytes for 36 lines of one message; groups should keep it short", len(r.Summary))
	}
	// 35 identical errors are one group, not 35 lines of summary.
	if strings.Count(r.Summary, "Producer failure") != 1 {
		t.Errorf("the repeated error is listed %d times", strings.Count(r.Summary, "Producer failure"))
	}

	data := r.Data.(map[string]any)
	if lines := data["lines"].([]clients.LogLine); len(lines) != 36 {
		t.Errorf("Data has %d lines, want 36", len(lines))
	}
	if c := data["counts"].(map[string]int); c["ERROR"] != 35 || c["WARN"] != 1 {
		t.Errorf("counts = %v", c)
	}
	if data["connector"] != "pg-orders-connector" || data["connector_source"] == nil {
		t.Errorf("data = %v", data)
	}

	// The defaults went to the reader.
	q := fl.queries[0]
	if q.Connector != "pg-orders-connector" || q.MinLevel != "WARN" || q.MaxLines != 50 || !q.Since.Equal(logNow.Add(-30*time.Minute)) {
		t.Errorf("query = %+v", q)
	}
}

func TestConnectWorkerLogNewestErrorIsNamed(t *testing.T) {
	lines := []clients.LogLine{
		{At: logNow.Add(-time.Minute), Level: "INFO", Message: "restarting"},
		{At: logNow.Add(-2 * time.Minute), Level: "ERROR", Message: "first line\nCaused by: x.Y: the root"},
	}
	tool, _ := logTool(t, lines, nil)
	r := runLog(t, tool, `{"min_level":"info"}`)
	if !strings.Contains(r.Summary, "Newest ERROR at 2026-10-15T04:28:00Z: first line | root cause: x.Y: the root") {
		t.Errorf("summary = %s", r.Summary)
	}
	if !strings.Contains(r.Summary, "INFO and above: 2 lines (ERROR 1, INFO 1)") {
		t.Errorf("summary = %s", r.Summary)
	}
}

func TestConnectWorkerLogMasksSecrets(t *testing.T) {
	lines := []clients.LogLine{{
		At: logNow, Level: "ERROR",
		Message: "Connection failed for jdbc url postgres://debezium:hunter2-secret-pw@db:5432/orders password=correct-horse-battery\n" +
			"Caused by: org.postgresql.util.PSQLException: FATAL: password authentication failed for user \"debezium\"",
	}}
	tool, _ := logTool(t, lines, nil)
	r := runLog(t, tool, `{}`)
	blob, _ := json.Marshal(r)
	for _, secret := range []string{"hunter2-secret-pw", "correct-horse-battery"} {
		if strings.Contains(string(blob), secret) {
			t.Errorf("%q reached the result:\n%s", secret, blob)
		}
	}
	// The evidence around the secret stays.
	if !strings.Contains(r.Summary, "password authentication failed for user") {
		t.Errorf("summary lost the error text: %s", r.Summary)
	}
}

func TestConnectWorkerLogSummaryStaysSmall(t *testing.T) {
	// 200 distinct 2 KiB lines: the worst case the reader allows.
	var lines []clients.LogLine
	for i := 0; i < 200; i++ {
		lines = append(lines, clients.LogLine{
			At: logNow.Add(-time.Duration(i) * time.Second), Level: "ERROR",
			Message: fmt.Sprintf("failure %d ", i) + strings.Repeat("z", 2000),
		})
	}
	tool, _ := logTool(t, lines, nil)
	r := runLog(t, tool, `{"max_lines":200}`)
	if len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary is %d bytes", len(r.Summary))
	}
	if n := strings.Count(r.Summary, "\n- "); n != workerLogMaxGroups {
		t.Errorf("summary lists %d groups, want %d", n, workerLogMaxGroups)
	}
	// Each listed line is clipped, not the whole 2 KiB.
	if len(r.Summary) > 3000 {
		t.Errorf("summary is %d bytes; a distinct 2 KiB line must be clipped", len(r.Summary))
	}
	data := r.Data.(map[string]any)
	shown := data["lines"].([]clients.LogLine)
	omitted, _ := data["omitted_older"].(int)
	if len(shown)+omitted != 200 || omitted == 0 || !r.Truncated {
		t.Errorf("shown %d, omitted %d, truncated %v: Data must be bounded and say so", len(shown), omitted, r.Truncated)
	}
	if b, _ := json.Marshal(data); len(b) > workerLogMaxDataBytes+8<<10 {
		t.Errorf("Data is %d bytes", len(b))
	}
	// The newest lines are the ones kept.
	if !strings.HasPrefix(shown[0].Message, "failure 0 ") {
		t.Errorf("first kept line = %.20q", shown[0].Message)
	}
}

func TestConnectWorkerLogNoLines(t *testing.T) {
	tool, _ := logTool(t, nil, nil)
	r := runLog(t, tool, `{"since_minutes":10,"min_level":"ERROR"}`)
	if !strings.Contains(r.Summary, "No ERROR or higher lines name pg-orders-connector (the pipeline's source connector) in the last 10 min") ||
		!strings.Contains(r.Summary, "does not show the connector is healthy") {
		t.Errorf("summary = %s", r.Summary)
	}
}

func TestConnectWorkerLogBounds(t *testing.T) {
	tool, fl := logTool(t, authLog(1), nil)
	r := runLog(t, tool, `{"connector":"pg-orders-connector","since_minutes":99999,"max_lines":9999,"min_level":"error"}`)
	q := fl.queries[0]
	if q.MaxLines != 200 || q.MinLevel != "ERROR" || !q.Since.Equal(logNow.Add(-1440*time.Minute)) {
		t.Errorf("query = %+v", q)
	}
	if !strings.Contains(r.Summary, "since_minutes capped at 1440; max_lines capped at 200") {
		t.Errorf("the clamp is not told: %s", r.Summary)
	}
	if strings.Contains(r.Summary, "the pipeline's source connector") {
		t.Errorf("an explicit connector was described as the default: %s", r.Summary)
	}

	// "warning" is WARN; omitted values take the defaults.
	runLog(t, tool, `{"min_level":"warning"}`)
	if fl.queries[len(fl.queries)-1].MinLevel != "WARN" || fl.queries[len(fl.queries)-1].MaxLines != 50 {
		t.Errorf("query = %+v", fl.queries[len(fl.queries)-1])
	}
}

func TestConnectWorkerLogFullQueryIsFull(t *testing.T) {
	// A log of exactly max_lines lines says the older ones are not shown.
	var lines []clients.LogLine
	for i := 0; i < 3; i++ {
		lines = append(lines, clients.LogLine{At: logNow, Level: "ERROR", Message: fmt.Sprintf("e%d", i)})
	}
	tool, _ := logTool(t, lines, nil)
	r := runLog(t, tool, `{"max_lines":3}`)
	if !strings.Contains(r.Summary, "Showing the newest 3; older lines are not shown.") {
		t.Errorf("summary = %s", r.Summary)
	}
}

func TestConnectWorkerLogBadInput(t *testing.T) {
	tool, fl := logTool(t, authLog(1), nil)
	for name, in := range map[string]string{
		"negative since": `{"since_minutes":-1}`,
		"negative lines": `{"max_lines":-5}`,
		"bad level":      `{"min_level":"DEBUG"}`,
		"junk level":     `{"min_level":"x\u001b[31m"}`,
		"not json":       `{`,
		"wrong type":     `{"since_minutes":"ten"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := tool.Run(roomCtx(), json.RawMessage(in))
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err = %v, want ErrBadInput", err)
			}
		})
	}
	if len(fl.queries) != 0 {
		t.Errorf("the log was read for bad input: %+v", fl.queries)
	}
	// An absent or null input is the same as {}.
	for _, in := range []string{``, `null`, ` `} {
		if _, err := tool.Run(roomCtx(), json.RawMessage(in)); err != nil {
			t.Errorf("input %q: %v", in, err)
		}
	}
}

func TestConnectWorkerLogDefaultConnector(t *testing.T) {
	two := &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"},
		Connectors: []string{"orders-sink", "pg-orders-connector"},
	}}}
	classes := map[string]string{
		"orders-sink":         "io.confluent.connect.jdbc.JdbcSinkConnector",
		"pg-orders-connector": "io.debezium.connector.postgresql.PostgresConnector",
	}
	build := func(cfgErr error) (tools.Tool, *fakeLogReader) {
		fl := &fakeLogReader{fn: func(context.Context, clients.WorkerLogQuery) ([]clients.LogLine, error) { return nil, nil }}
		d := Deps{
			Owners:     two,
			ConnectLog: fl,
			Connect: &fakeConnect{listFn: func(context.Context) ([]string, error) { return nil, nil }, configFn: func(_ context.Context, n string) (map[string]string, error) {
				if cfgErr != nil {
					return nil, cfgErr
				}
				return map[string]string{"connector.class": classes[n]}, nil
			}},
		}
		return scopeTool(t, d, "connect_worker_log"), fl
	}

	t.Run("source among several", func(t *testing.T) {
		tool, fl := build(nil)
		runLog(t, tool, `{}`)
		if got := fl.queries[0].Connector; got != "pg-orders-connector" {
			t.Fatalf("connector = %q, want the Debezium source", got)
		}
	})
	t.Run("cannot tell", func(t *testing.T) {
		tool, fl := build(errors.New("connect down"))
		_, err := tool.Run(roomCtx(), json.RawMessage(`{}`))
		if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "orders-sink, pg-orders-connector") {
			t.Fatalf("err = %v, want bad input naming the choices", err)
		}
		if len(fl.queries) != 0 {
			t.Fatal("the log was read without a connector")
		}
	})
	t.Run("a named sink is fine", func(t *testing.T) {
		tool, fl := build(nil)
		runLog(t, tool, `{"connector":"orders-sink"}`)
		if fl.queries[0].Connector != "orders-sink" {
			t.Fatalf("query = %+v", fl.queries[0])
		}
	})
	t.Run("unscoped needs a name", func(t *testing.T) {
		tool, _ := build(nil)
		_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
		if !errors.Is(err, tools.ErrBadInput) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestConnectWorkerLogReaderErrors(t *testing.T) {
	t.Run("no log source is a result, not an error", func(t *testing.T) {
		tool, _ := logTool(t, nil, fmt.Errorf("connect: worker log: CONNECT_WORKER_LOG_FILE is not set: %w", clients.ErrNotSupported))
		r := runLog(t, tool, `{}`)
		if !strings.Contains(r.Summary, "CONNECT_WORKER_LOG_FILE is not set") {
			t.Errorf("summary = %s", r.Summary)
		}
	})
	t.Run("a failing file is an error naming the tool", func(t *testing.T) {
		cause := errors.New("the file named by CONNECT_WORKER_LOG_FILE does not exist")
		tool, _ := logTool(t, nil, cause)
		_, err := tool.Run(roomCtx(), json.RawMessage(`{}`))
		if !errors.Is(err, cause) || !strings.HasPrefix(err.Error(), "kafkatools: connect_worker_log: ") {
			t.Fatalf("err = %v", err)
		}
		if errors.Is(err, tools.ErrBadInput) {
			t.Fatalf("a target failure must not be bad input: %v", err)
		}
	})
	t.Run("the deadline reaches the reader", func(t *testing.T) {
		var deadline bool
		fl := &fakeLogReader{fn: func(ctx context.Context, _ clients.WorkerLogQuery) ([]clients.LogLine, error) {
			_, deadline = ctx.Deadline()
			return nil, ctx.Err()
		}}
		d, _ := scopeFakes(t)
		d.ConnectLog = fl
		ctx, cancel := context.WithCancel(roomCtx())
		cancel()
		_, err := scopeTool(t, d, "connect_worker_log").Run(ctx, json.RawMessage(`{}`))
		if !errors.Is(err, context.Canceled) || !deadline {
			t.Fatalf("err = %v, deadline set = %v", err, deadline)
		}
	})
}

func TestConnectWorkerLogRegistration(t *testing.T) {
	d, _ := scopeFakes(t)
	d.ConnectLog = nil
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("connect_worker_log"); ok {
		t.Fatal("registered without a log source")
	}

	d.ConnectLog = &fakeLogReader{fn: func(context.Context, clients.WorkerLogQuery) ([]clients.LogLine, error) { return nil, nil }}
	reg = tools.NewReadOnlyRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatalf("a read-only registry refused the tool set: %v", err)
	}
	tool, ok := reg.Get("connect_worker_log")
	if !ok || tool.Kind() != tools.KindRead {
		t.Fatalf("tool = %v, ok = %v", tool, ok)
	}
	schema := tool.InputSchema()
	if schema["additionalProperties"] != false {
		t.Errorf("schema = %v", schema)
	}
	props := schema["properties"].(map[string]any)
	for _, k := range []string{"connector", "since_minutes", "min_level", "max_lines"} {
		if _, ok := props[k]; !ok {
			t.Errorf("schema lacks %s", k)
		}
	}
	if req, _ := schema["required"].([]string); len(req) != 0 {
		t.Errorf("required = %v, every parameter is optional", req)
	}
}

// The tool refuses to run when constructed with no reader at all.
func TestConnectWorkerLogNilReader(t *testing.T) {
	r, err := (&connectWorkerLog{d: Deps{}}).Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
	if err != nil || !strings.Contains(r.Summary, "CONNECT_WORKER_LOG_FILE") {
		t.Fatalf("r = %+v, err = %v", r, err)
	}
}

func TestLogHeadline(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"one line", "plain", "plain"},
		{"empty", "", ""},
		{"markers dropped", "head\n[4 stack frames omitted]\n…", "head"},
		{"exception line", "head\nx.Exc: bad\n[1 stack frame omitted]", "head | x.Exc: bad"},
		{"deepest cause", "head\nx.Exc: bad\nCaused by: a.B: mid\n[2 stack frames omitted]\nCaused by: c.D: root", "head | x.Exc: bad | root cause: c.D: root"},
		{"cause right after head", "head\nCaused by: c.D: root", "head | root cause: c.D: root"},
		{"clipped", strings.Repeat("a", 500), strings.Repeat("a", 200) + "…"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := logHeadline(tc.in); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
