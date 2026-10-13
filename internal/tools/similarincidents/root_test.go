package similarincidents

import "testing"

// TestRootIsTheLastClassCauseNotATrailingLogLine: a log line after the root
// must not become the root fingerprint (textnorm marks such lines Log).
func TestRootIsTheLastClassCauseNotATrailingLogLine(t *testing.T) {
	root := "Caused by: org.a.b.BarException: root cause here"
	plain, _, ok1 := analyze("org.a.b.FooException: outer\n" + root)
	withLog, _, ok2 := analyze("org.a.b.FooException: outer\n" + root + "\n[2026-10-11 09:41:07] INFO Worker stopped")
	only, ok3 := fingerprint(root)
	if !ok1 || !ok2 || !ok3 {
		t.Fatal("no fingerprint")
	}
	if plain != withLog {
		t.Errorf("a trailing log line changed the fingerprints: %+v vs %+v", plain, withLog)
	}
	if plain.Root != only.Root {
		t.Errorf("root = %s, want the fingerprint of the root cause alone %s", plain.Root, only.Root)
	}
}

// TestRootSkipsDetailLineAndFrame: a Postgres trace ends in a "Detail:" line
// (a classless cause) and a stack frame. The root is the PSQLException above
// them, not the last cause. With a "last cause" rule the root would be the
// Detail line with its frame and a query quoting the PSQLException line would
// not match.
func TestRootSkipsDetailLineAndFrame(t *testing.T) {
	trace := "org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded\n" +
		"Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"orders_pkey\"\n" +
		"  Detail: Key (email)=(a@b.c) already exists.\n" +
		"\tat org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2713)"
	line := "Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"orders_pkey\""
	full, _, ok1 := analyze(trace)
	only, ok2 := fingerprint(line)
	if !ok1 || !ok2 {
		t.Fatal("no fingerprint")
	}
	if full.Root != only.Root {
		t.Errorf("root = %s, want the PSQLException alone %s", full.Root, only.Root)
	}
	if full.Full == full.Root {
		t.Error("full and root must differ for a chain")
	}
}

// TestRootFallsBackToTheLastNonLogCause: with no class-bearing cause the root
// is the last cause that is not a log line, even when a log line (with a
// frame) comes after it.
func TestRootFallsBackToTheLastNonLogCause(t *testing.T) {
	msg := "ERROR: duplicate key value violates unique constraint \"orders_pkey\""
	withLog, _, ok1 := analyze(msg + "\n[2026-10-11 09:41:07] INFO Worker stopped\n\tat org.a.B.m(B.java:1)")
	only, ok2 := fingerprint(msg)
	if !ok1 || !ok2 {
		t.Fatal("no fingerprint")
	}
	if withLog.Root != only.Root {
		t.Errorf("root = %s, want the ERROR line alone %s (a trailing log line took the root)", withLog.Root, only.Root)
	}
}
