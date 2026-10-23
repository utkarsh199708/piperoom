package sourceactions

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/postgres"
)

func esFixture() (*fakeConnect, *fakeSource, *fakeWriter, *endSession) {
	conn, w := newConnect(), &fakeWriter{}
	src := readySource()
	src.sessions = append(src.sessions,
		DBSession{PID: 77, StartedAt: testStart, XactStart: testStart, User: "debezium", App: "Debezium snapshot", State: "active", Query: "SELECT * FROM public.orders"})
	now := func() time.Time { return testStart.Add(2 * time.Hour) }
	return conn, src, w, &endSession{d: EndSessionDeps{Connect: conn, Source: src, Writer: w, now: now}}
}

func esParams(pid int64, started time.Time, older ...int) json.RawMessage {
	m := map[string]any{"connector": "pg-orders", "pid": pid, "started_at": started.Format(time.RFC3339Nano)}
	if len(older) > 0 {
		m["older_than_minutes"] = older[0]
	}
	b, _ := json.Marshal(m)
	return b
}

func TestEndSessionValidate(t *testing.T) {
	conn, src, w, a := esFixture()
	if err := a.Validate(scopedCtx(), esParams(4242, testStart)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if src.lastOlder != 5*time.Minute {
		t.Fatalf("default older_than = %s, want 5m", src.lastOlder)
	}
	if err := a.Validate(scopedCtx(), esParams(4242, testStart, 30)); err != nil || src.lastOlder != 30*time.Minute {
		t.Fatalf("older_than 30: %v %s", err, src.lastOlder)
	}
	if err := a.Validate(scopedCtx(), esParams(9999, testStart)); err == nil || !strings.Contains(err.Error(), "not in source_blocking_sessions") {
		t.Fatalf("unknown pid = %v", err)
	}
	if err := a.Validate(scopedCtx(), esParams(4242, testStart.Add(time.Second))); !errors.Is(err, postgres.ErrSessionChanged) {
		t.Fatalf("other start = %v, want ErrSessionChanged", err)
	}
	if err := a.Validate(scopedCtx(), esParams(77, testStart)); err == nil || !strings.Contains(err.Error(), "connector \"pg-orders\"'s own database.user") {
		t.Fatalf("connector's own session = %v", err)
	}
	// Postgres folds unquoted role names to lowercase: database.user=Debezium
	// shows as usename debezium, and the guard must still hold.
	conn.cfg["pg-orders"]["database.user"] = "Debezium"
	if err := a.Validate(scopedCtx(), esParams(77, testStart)); err == nil || !strings.Contains(err.Error(), "own database.user") {
		t.Fatalf("case-folded connector user = %v, want the own-session refusal", err)
	}
	conn.cfg["pg-orders"]["database.user"] = "debezium"
	src.err = errors.New("db down")
	if err := a.Validate(scopedCtx(), esParams(4242, testStart)); err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("reader error = %v", err)
	}
	if len(w.calls) != 0 {
		t.Fatal("Validate wrote")
	}
}

// A database.user given as a config provider reference cannot be compared,
// so the connector's-own-session refusal is skipped; the writer still
// protects walsenders and Piperoom's sessions.
func TestEndSessionConfigRefUser(t *testing.T) {
	conn, _, _, a := esFixture()
	conn.cfg["pg-orders"]["database.user"] = "${env:DB_USER}"
	if err := a.Validate(scopedCtx(), esParams(77, testStart)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestEndSessionPreview(t *testing.T) {
	_, _, w, a := esFixture()
	res, err := a.Preview(scopedCtx(), esParams(4242, testStart))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	for _, want := range []string{"transaction age 1h59m0s", "connected 2h0m0s ago", `application "billing"`, `state "idle in transaction"`, "untrusted, redacted"} {
		if !strings.Contains(res.Current, want) {
			t.Errorf("Current lacks %q: %s", want, res.Current)
		}
	}
	if strings.Contains(res.Current, "hunter2") {
		t.Errorf("Current leaks the query's secret: %s", res.Current)
	}
	if !strings.Contains(res.Expected, "pg_terminate_backend(4242)") || !strings.Contains(res.Expected, "rolled back") || res.Reversible {
		t.Errorf("Expected: %+v", res)
	}
	if len(w.calls) != 0 {
		t.Fatal("Preview wrote")
	}

	res, err = a.Preview(scopedCtx(), esParams(9999, testStart))
	if err != nil || !strings.HasPrefix(res.Expected, "no change: this step is refused") {
		t.Fatalf("gone session preview: %v %+v", err, res)
	}
}

func TestEndSessionPreviewFoldsUntrustedText(t *testing.T) {
	_, src, _, a := esFixture()
	src.sessions[0].App = "evil\npid=1 blocks_slot=yes"
	src.sessions[0].Query = strings.Repeat("x", 500) + "\x1b[31m"
	res, err := a.Preview(scopedCtx(), esParams(4242, testStart))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if strings.ContainsAny(res.Current, "\n\x1b") || strings.Contains(res.Current, strings.Repeat("x", 201)) {
		t.Fatalf("Current not folded or cut: %q", res.Current)
	}
}

func TestEndSessionPreviewReadErrorIsError(t *testing.T) {
	_, src, _, a := esFixture()
	src.err = errors.New("db down")
	if _, err := a.Preview(scopedCtx(), esParams(4242, testStart)); err == nil {
		t.Fatal("a failed read must be a Preview error, not a refused step")
	}
}

func TestEndSessionExecute(t *testing.T) {
	_, src, w, a := esFixture()
	if _, err := a.Execute(scopedCtx(), esParams(4242, testStart)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(w.calls) != 1 || w.calls[0].method != "EndSession" || w.calls[0].args[0] != int64(4242) || !w.calls[0].args[1].(time.Time).Equal(testStart) {
		t.Fatalf("writes: %+v", w.calls)
	}
	if src.calls == 0 {
		t.Fatal("Execute did not re-read the blocking list")
	}
	w.err = postgres.ErrSessionProtected
	if _, err := a.Execute(scopedCtx(), esParams(4242, testStart)); !errors.Is(err, postgres.ErrSessionProtected) {
		t.Fatalf("writer error = %v", err)
	}
}

// TestEndSessionExecuteRechecks: a session that ended (or whose pid was
// reused) between Validate and Execute is refused before the writer.
func TestEndSessionExecuteRechecks(t *testing.T) {
	_, src, w, a := esFixture()
	if err := a.Validate(scopedCtx(), esParams(4242, testStart)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	src.sessions = []DBSession{{PID: 4242, StartedAt: testStart.Add(time.Hour), User: "app"}}
	if _, err := a.Execute(scopedCtx(), esParams(4242, testStart)); !errors.Is(err, postgres.ErrSessionChanged) {
		t.Fatalf("reused pid = %v, want ErrSessionChanged", err)
	}
	src.sessions = nil
	if _, err := a.Execute(scopedCtx(), esParams(4242, testStart)); err == nil || !strings.Contains(err.Error(), "not in source_blocking_sessions") {
		t.Fatalf("gone session = %v", err)
	}
	if len(w.calls) != 0 {
		t.Fatalf("writer called after a failed re-check: %+v", w.calls)
	}
}
