package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

// The Connect worker log on the host (dev/.logs/connect/connect.log, written by
// dev/connect/worker-log.sh; QA G3). Kafka Connect keeps a task RUNNING while
// Debezium retries an error it calls retriable, so this log is often the only
// place that says why a fix did not take. Fixes read it to say so.

// maxWorkerLogRead caps one read of the log (AGENTS.md rule 7 spirit: the file
// is rotated at 10 MiB, a fix only needs what was written since its restart).
const maxWorkerLogRead = 1 << 20

// maxReportLine caps one log line in a report: Debezium traces embed whole Avro
// schemas, several kB per line.
const maxReportLine = 300

// logRecord matches the first line of a log4j record in the two layouts the dev
// stack produces: the Debezium image's
//
//	2026-10-15 00:22:16,938 ERROR  Postgres|pg|streaming  Producer failure   [io.debezium.pipeline.ErrorHandler]
//
// and Kafka's default, which the local cp-kafka-connect stand-in prints:
//
//	[2026-10-15 00:22:16,938] ERROR Producer failure (io.debezium.pipeline.ErrorHandler)
var logRecord = regexp.MustCompile(`^\[?\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[,.]\d{3}\]?\s+([A-Z]+)\s+(.*)$`)

// decodeCorrupt is what Postgres answers when logical decoding reads a row with
// the wrong row type (QA G17, dev/scenarios.md "Known issues").
var decodeCorrupt = regexp.MustCompile(`compressed [a-z0-9]+ data is corrupt`)

// g17Hint explains a decodeCorrupt error to the person running the fix.
const g17Hint = "Postgres cannot decode the WAL written before the ALTER: the connector opened public.orders on its " +
	"replication connection before streaming (publication.autocreate.mode=filtered), so the walsender reads old rows " +
	"with the new row type. Register the dev connector config again (`devctl up`; it uses publication.autocreate.mode=" +
	"disabled) and rerun the fix; see dev/scenarios.md, Known issues (G17)"

// workerLogPath is CONNECT_WORKER_LOG_FILE resolved against the repo root, or ""
// when the log source is off.
func (s *stack) workerLogPath() string {
	p := s.cfg.ConnectWorkerLogFile
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(s.root, p)
}

// workerLogMark is the log's current size; a later readWorkerLogSince starts
// there. 0 when there is no log (yet).
func workerLogMark(path string) int64 {
	if path == "" {
		return 0
	}
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// readWorkerLogSince returns what was appended after mark, at most the last
// maxWorkerLogRead bytes of it. A file now smaller than mark was rotated (the
// worker restarted, or the copy passed its size cap), so it is read from the start.
func readWorkerLogSince(path string, mark int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("devctl: open worker log: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("devctl: stat worker log: %w", err)
	}
	from := mark
	if fi.Size() < mark {
		from = 0
	}
	if fi.Size()-from > maxWorkerLogRead {
		from = fi.Size() - maxWorkerLogRead
	}
	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return "", fmt.Errorf("devctl: seek worker log: %w", err)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxWorkerLogRead))
	if err != nil {
		return "", fmt.Errorf("devctl: read worker log: %w", err)
	}
	return string(b), nil
}

// workerLogError is the newest ERROR record of a log excerpt.
type workerLogError struct {
	Message string // the record's message (MDC and logger included, as logged)
	Cause   string // the last "Caused by:" of its stack trace, else the exception line
	Count   int    // ERROR records in the excerpt
}

func (e workerLogError) String() string {
	if e.Cause == "" {
		return clip(e.Message)
	}
	return clip(e.Message) + " — caused by " + clip(e.Cause)
}

// lastWorkerLogError finds the newest ERROR record in text and the root cause of
// its stack trace. ok is false when text holds no ERROR record.
func lastWorkerLogError(text string) (e workerLogError, ok bool) {
	inError, sawException := false, false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if m := logRecord.FindStringSubmatch(line); m != nil {
			inError = m[1] == "ERROR"
			if inError {
				e.Count++
				e.Message, e.Cause, sawException = strings.TrimSpace(m[2]), "", false
			}
			continue
		}
		if !inError {
			continue
		}
		t := strings.TrimSpace(line)
		switch {
		case t == "" || strings.HasPrefix(t, "at ") || strings.HasPrefix(t, "..."):
		case strings.HasPrefix(t, "Caused by: "):
			e.Cause = strings.TrimPrefix(t, "Caused by: ")
		case !sawException:
			// The first line of the trace names the thrown exception.
			sawException = true
			if e.Cause == "" {
				e.Cause = t
			}
		}
	}
	return e, e.Count > 0
}

// clip shortens one line for a terminal report, on a rune boundary.
func clip(s string) string {
	if len(s) <= maxReportLine {
		return s
	}
	cut := maxReportLine
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// workerLogFinding reads the log since mark and says what it shows: the newest
// ERROR record and, when the excerpt has the G17 signature, the hint for it.
// corrupt reports that signature.
func (s *stack) workerLogFinding(mark int64) (finding string, corrupt bool) {
	path := s.workerLogPath()
	if path == "" {
		return "worker log off (CONNECT_WORKER_LOG_FILE=off); see `docker logs piperoom-dev-connect-1`", false
	}
	text, err := readWorkerLogSince(path, mark)
	if err != nil {
		return fmt.Sprintf("worker log unreadable (%v); see `docker logs piperoom-dev-connect-1`", err), false
	}
	corrupt = decodeCorrupt.MatchString(text)
	e, ok := lastWorkerLogError(text)
	switch {
	case !ok:
		finding = fmt.Sprintf("worker log %s: no ERROR since the restart", path)
	default:
		finding = fmt.Sprintf("worker log %s: %d ERROR record(s) since the restart, newest: %s", path, e.Count, e)
	}
	if corrupt {
		finding += ". " + g17Hint
	}
	return finding, corrupt
}

// connectDiagnosis says why task 0 has not recovered: what Connect reports, and
// what the worker log shows since mark.
func (s *stack) connectDiagnosis(ctx context.Context, mark int64) string {
	var parts []string
	if ctx.Err() != nil {
		// Cancelled (Ctrl+C) or out of time: no more requests, the log is local.
		parts = append(parts, "connector status not read: "+ctx.Err().Error())
	} else if st, err := s.connectorStatus(ctx); err != nil {
		parts = append(parts, "connector status unavailable: "+err.Error())
	} else {
		line := "connector " + st.Connector.State
		for _, t := range st.Tasks {
			line += fmt.Sprintf(", task %d %s", t.ID, t.State)
			if first, _, _ := strings.Cut(strings.TrimSpace(t.Trace), "\n"); first != "" {
				line += " (" + clip(first) + ")"
			}
		}
		parts = append(parts, line)
	}
	finding, _ := s.workerLogFinding(mark)
	parts = append(parts, finding)
	return strings.Join(parts, "; ")
}
