package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/config"
)

func TestLastWorkerLogError(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		ok      bool
		count   int
		message string // substring
		cause   string // exact
	}{
		{
			name: "Kafka layout, root cause is the last Caused by",
			text: `[2026-10-15 00:22:16,917] INFO Processing messages (io.debezium.X)
[2026-10-15 00:22:16,938] ERROR Producer failure (io.debezium.pipeline.ErrorHandler)
org.apache.kafka.connect.errors.RetriableException: An exception occurred in the change event producer.
	at io.debezium.pipeline.ErrorHandler.setProducerThrowable(ErrorHandler.java:67)
Caused by: org.postgresql.util.PSQLException: Database connection failed
	... 12 more
Caused by: org.postgresql.util.PSQLException: ERROR: compressed lz4 data is corrupt
	at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2734)
`,
			ok: true, count: 1, message: "Producer failure",
			cause: "org.postgresql.util.PSQLException: ERROR: compressed lz4 data is corrupt",
		},
		{
			name: "Debezium image layout (ISO8601, MDC, logger in brackets)",
			text: `2026-10-15 00:22:16,938 ERROR  Postgres|pg|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]
org.postgresql.util.PSQLException: ERROR: must be owner of table orders
	at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2734)
2026-10-15 00:22:17,001 INFO   Postgres|pg|streaming  Connected metrics set to 'false'   [io.debezium.X]
`,
			ok: true, count: 1, message: "Postgres|pg|streaming  Producer failure",
			cause: "org.postgresql.util.PSQLException: ERROR: must be owner of table orders",
		},
		{
			name: "newest of several errors, counted",
			text: `[2026-10-15 00:00:01,000] ERROR first (a.B)
java.lang.IllegalStateException: one
[2026-10-15 00:00:02,000] WARN in between (a.B)
[2026-10-15 00:00:03,000] ERROR second (a.B)
`,
			ok: true, count: 2, message: "second", cause: "",
		},
		{
			name: "a trace after a WARN does not belong to the earlier ERROR",
			text: `[2026-10-15 00:00:01,000] ERROR the error (a.B)
java.lang.IllegalStateException: its cause
[2026-10-15 00:00:02,000] WARN a warning (a.B)
Caused by: java.io.IOException: the warning's cause
`,
			ok: true, count: 1, message: "the error", cause: "java.lang.IllegalStateException: its cause",
		},
		{
			name: "CRLF line ends",
			text: "[2026-10-15 00:00:01,000] ERROR crlf (a.B)\r\njava.lang.Exception: x\r\n",
			ok:   true, count: 1, message: "crlf", cause: "java.lang.Exception: x",
		},
		{
			name: "no error",
			text: "[2026-10-15 00:00:01,000] INFO all good (a.B)\nUsing BOOTSTRAP_SERVERS=kafka:9092\n",
			ok:   false,
		},
		{name: "empty", text: "", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, ok := lastWorkerLogError(tt.text)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v (%+v)", ok, tt.ok, e)
			}
			if !ok {
				return
			}
			if e.Count != tt.count {
				t.Errorf("Count = %d, want %d", e.Count, tt.count)
			}
			if !strings.Contains(e.Message, tt.message) {
				t.Errorf("Message = %q, want it to contain %q", e.Message, tt.message)
			}
			if e.Cause != tt.cause {
				t.Errorf("Cause = %q, want %q", e.Cause, tt.cause)
			}
		})
	}
}

func TestClip(t *testing.T) {
	long := strings.Repeat("é", maxReportLine) // two bytes per rune
	got := clip(long)
	if len(got) > maxReportLine+len("…") {
		t.Errorf("clip kept %d bytes, want at most %d", len(got), maxReportLine+len("…"))
	}
	if !strings.HasSuffix(got, "…") || strings.ContainsRune(strings.TrimSuffix(got, "…"), '�') {
		t.Errorf("clip cut inside a rune or lost the ellipsis: %q", got[len(got)-8:])
	}
	if clip("short") != "short" {
		t.Error("clip changed a short line")
	}
}

func TestReadWorkerLogSince(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "connect.log")
	if err := os.WriteFile(path, []byte("old line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mark := workerLogMark(path)
	appendFile(t, path, "new line\n")

	got, err := readWorkerLogSince(path, mark)
	if err != nil || got != "new line\n" {
		t.Errorf("after mark = %q, %v; want only the new line", got, err)
	}

	// Rotated: the file is now smaller than the mark, so it is read from the start.
	if err := os.WriteFile(path, []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := readWorkerLogSince(path, mark+100); err != nil || got != "x\n" {
		t.Errorf("rotated file = %q, %v; want the whole new file", got, err)
	}

	// Capped: only the last maxWorkerLogRead bytes are read.
	big := strings.Repeat("a", maxWorkerLogRead) + "TAIL"
	if err := os.WriteFile(path, []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = readWorkerLogSince(path, 0)
	if err != nil || len(got) != maxWorkerLogRead || !strings.HasSuffix(got, "TAIL") {
		t.Errorf("capped read: %d bytes, err %v; want the last %d bytes", len(got), err, maxWorkerLogRead)
	}

	// Error path: no file.
	if _, err := readWorkerLogSince(filepath.Join(dir, "missing.log"), 0); err == nil {
		t.Error("missing file: want an error")
	}
	if workerLogMark(filepath.Join(dir, "missing.log")) != 0 || workerLogMark("") != 0 {
		t.Error("mark of a missing or disabled log should be 0")
	}
}

func TestWorkerLogFinding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "connect.log")
	s := &stack{root: dir, cfg: config.Config{ConnectWorkerLogFile: "connect.log"}}
	if got := s.workerLogPath(); got != path {
		t.Fatalf("workerLogPath = %q, want it resolved against the repo root: %q", got, path)
	}

	appendFile(t, path, "[2026-10-15 00:00:00,000] INFO started (a.B)\n")
	if finding, corrupt := s.workerLogFinding(0); corrupt || !strings.Contains(finding, "no ERROR since the restart") {
		t.Errorf("clean log: %q corrupt=%v", finding, corrupt)
	}

	appendFile(t, path, g17LogExcerpt)
	finding, corrupt := s.workerLogFinding(0)
	if !corrupt {
		t.Errorf("the G17 excerpt was not recognised: %q", finding)
	}
	for _, want := range []string{"1 ERROR record(s)", "compressed lz4 data is corrupt", "publication.autocreate.mode", "Known issues (G17)"} {
		if !strings.Contains(finding, want) {
			t.Errorf("finding does not contain %q: %s", want, finding)
		}
	}

	// From a mark after the error, the same file is clean again.
	if finding, corrupt := s.workerLogFinding(workerLogMark(path)); corrupt || strings.Contains(finding, "ERROR record") {
		t.Errorf("after the mark: %q corrupt=%v, want nothing", finding, corrupt)
	}

	// Off, and unreadable: say so, never fail.
	off := &stack{root: dir}
	if finding, corrupt := off.workerLogFinding(0); corrupt || !strings.Contains(finding, "worker log off") {
		t.Errorf("log off: %q", finding)
	}
	gone := &stack{root: dir, cfg: config.Config{ConnectWorkerLogFile: filepath.Join(dir, "nope.log")}}
	if finding, _ := gone.workerLogFinding(0); !strings.Contains(finding, "worker log unreadable") {
		t.Errorf("missing log: %q", finding)
	}
}
