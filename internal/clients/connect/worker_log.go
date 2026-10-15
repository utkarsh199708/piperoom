package connect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
)

// Limits of the worker log reader. The file is the Connect worker's own log, so
// it can be gigabytes: only its end is read, and every entry is bounded.
const (
	// workerLogTailBytes is how much of the end of the file is read.
	workerLogTailBytes = 4 << 20
	// defaultWorkerLogLines and maxWorkerLogLines bound WorkerLogQuery.MaxLines.
	defaultWorkerLogLines = 50
	maxWorkerLogLines     = 200
	// maxLogMessageBytes is the contract limit of clients.LogLine.Message.
	maxLogMessageBytes = 2048
	// maxEntryBytes bounds the text kept while one entry's trace is folded in,
	// so a 4 MiB trace cannot turn into one 4 MiB string.
	maxEntryBytes = 16 << 10

	// workerLogSetting names the setting in errors, so the person reading one
	// knows what to change.
	workerLogSetting = "CONNECT_WORKER_LOG_FILE"
)

// LogReader reads the Kafka Connect worker log from a file and implements
// clients.ConnectLogReader. Debezium logs a retriable failure there (a missing
// privilege, a corrupt WAL read) while Connect still reports the task RUNNING.
//
// Two log4j layouts are understood, each also with an ISO 'T' in the time, a
// time zone suffix or a '.' before the milliseconds:
//
//	2026-10-15 04:10:09,123 ERROR  Postgres|pg-orders|streaming  message   [logger]   (Debezium image)
//	[2026-10-15 04:10:09,123] ERROR [pg-orders|task-0] message (logger:123)             (Apache Kafka)
//
// A time without a zone is read as UTC (a container's default). A log whose
// lines carry no time at all but start with the level (ERROR message) is read
// too; its lines have a zero At and are never dropped by Since. A leading
// `docker logs --timestamps` prefix is ignored. JSON logs are not understood:
// they give an empty result, never an error.
type LogReader struct{ path string }

var _ clients.ConnectLogReader = (*LogReader)(nil)

// NewLogReader returns a reader of the worker log at path. An empty path (the
// setting is unset or "off") gives a reader whose WorkerLog returns
// clients.ErrNotSupported, so the caller needs no special case.
func NewLogReader(path string) *LogReader { return &LogReader{path: strings.TrimSpace(path)} }

// WorkerLog returns the entries of the last 4 MiB of the log that name
// q.Connector (or one of its tasks), are at least q.MinLevel and not older
// than q.Since, newest first, at most q.MaxLines. A stack trace is folded into
// the entry before it, with its frames collapsed to a count so the exception
// and every "Caused by:" line stay inside the 2 KiB Message.
func (r *LogReader) WorkerLog(ctx context.Context, q clients.WorkerLogQuery) ([]clients.LogLine, error) {
	if r == nil || r.path == "" {
		return nil, fmt.Errorf("connect: worker log: %s is not set: %w", workerLogSetting, clients.ErrNotSupported)
	}
	if q.Connector == "" {
		return nil, errors.New("connect: worker log: connector is required")
	}
	floor, err := levelFloor(q.MinLevel)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("connect: worker log: %w", err)
	}
	data, err := readTail(r.path)
	if err != nil {
		return nil, err
	}
	// A file read cannot be interrupted; check again so a call that was
	// cancelled during the read does not go on to build a result nobody wants.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("connect: worker log: %w", err)
	}
	return parseWorkerLog(data, q, floor), nil
}

// readTail returns the last workerLogTailBytes of the file, starting on a line
// start: when the cut falls inside a line, that partial line is dropped.
func readTail(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, logFileError(path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, logFileError(path, err)
	}
	if st.IsDir() {
		return nil, fmt.Errorf("connect: worker log: %s names a directory (%q), not the Connect worker log file", workerLogSetting, path)
	}
	size := st.Size()
	start := int64(0)
	if size > workerLogTailBytes {
		start = size - workerLogTailBytes
	}
	// One byte before the cut says whether the cut is on a line start.
	off := start
	if off > 0 {
		off--
	}
	buf, err := io.ReadAll(io.NewSectionReader(f, off, size-off))
	if err != nil {
		return nil, logFileError(path, err)
	}
	if off < start && len(buf) > 0 {
		if buf[0] == '\n' {
			return buf[1:], nil
		}
		nl := bytes.IndexByte(buf, '\n')
		if nl < 0 {
			return nil, nil // the whole tail is one partial line
		}
		return buf[nl+1:], nil
	}
	return buf, nil
}

// logFileError wraps a file error and names the setting.
func logFileError(path string, err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("connect: worker log: the file named by %s does not exist; set it to the Kafka Connect worker log (the dev stack writes dev/.logs/connect/connect.log): %w", workerLogSetting, err)
	case errors.Is(err, fs.ErrPermission):
		return fmt.Errorf("connect: worker log: the file named by %s cannot be read (permission denied): %w", workerLogSetting, err)
	}
	return fmt.Errorf("connect: worker log: reading the file named by %s (%q): %w", workerLogSetting, path, err)
}

// Level ranks. Levels below INFO are parsed (they are valid log4j) but never
// asked for.
func levelRank(level string) int {
	switch level {
	case "TRACE", "DEBUG":
		return 0
	case "INFO":
		return 1
	case "WARN", "WARNING":
		return 2
	case "ERROR", "FATAL":
		return 3
	}
	return 1
}

// levelFloor maps WorkerLogQuery.MinLevel to a rank: WARN when empty.
func levelFloor(s string) (int, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "":
		return 2, nil
	case "INFO":
		return 1, nil
	case "WARN", "WARNING":
		return 2, nil
	case "ERROR":
		return 3, nil
	}
	return 0, fmt.Errorf("connect: worker log: min level %q: want INFO, WARN or ERROR", s)
}

const (
	tsPattern    = `\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[,.]\d{1,9})?(?:Z|[+-]\d{2}:?\d{2})?`
	levelPattern = `TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL`
)

var (
	// tsHeaderRE: "2026-… LEVEL rest" and "[2026-…] LEVEL rest".
	tsHeaderRE = regexp.MustCompile(`^\[?(` + tsPattern + `)\]?\s+(` + levelPattern + `)(?:\s+(.*))?$`)
	// levelHeaderRE is the fallback for a log with no time: "LEVEL rest".
	levelHeaderRE = regexp.MustCompile(`^\[?(` + levelPattern + `)\]?\s+(.*)$`)
	// dockerPrefixRE is the prefix `docker logs --timestamps` adds to a line.
	// Exactly one space follows it: the next character may be the tab that
	// marks a stack frame.
	dockerPrefixRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z `)
	// A logger name has at least one dot, so "(true)" or "[1]" is not one.
	loggerBracketRE = regexp.MustCompile(`\s+\[([A-Za-z_$][\w$]*(?:\.[\w$]+)+)\]\s*$`)
	loggerParenRE   = regexp.MustCompile(`\s+\(([A-Za-z_$][\w$]*(?:\.[\w$]+)+)(?::\d+)?\)\s*$`)
	// emptyMDCRE is Debezium's context prefix when nothing is set: "||".
	emptyMDCRE = regexp.MustCompile(`^\|\|\s*`)
	// frameRE and moreRE are the lines of a stack trace that carry no cause.
	frameRE = regexp.MustCompile(`^\s+at\s+\S`)
	moreRE  = regexp.MustCompile(`^\s*\.\.\.\s+\d+\s+(?:more|common frames omitted)\s*$`)
)

// header is one parsed first line of an entry.
type header struct {
	at     time.Time
	level  string
	logger string
	msg    string
}

// entry is an entry being built: its header and the lines folded into it.
type entry struct {
	header
	extra   []string
	frames  int // stack frames skipped since the last kept line
	size    int
	clipped bool
}

// startsWithTime is a cheap test that line can begin with a time: a digit or '['.
func startsWithTime(line string) bool {
	return line != "" && (line[0] == '[' || (line[0] >= '0' && line[0] <= '9'))
}

// stripDocker removes a `docker logs --timestamps` prefix. A line that is
// already a header ("2026-10-15T04:10:09Z ERROR ...") keeps its time: in a log
// with no time of its own that is the docker one, and it is the best At there is.
func stripDocker(line string) string {
	// Both a docker prefix and a header with a time start with a digit or '['.
	// Most lines of a busy log are stack frames and blanks: skip the regexes.
	if !startsWithTime(line) {
		return line
	}
	if tsHeaderRE.MatchString(line) {
		return line
	}
	if loc := dockerPrefixRE.FindStringIndex(line); loc != nil {
		return line[loc[1]:]
	}
	return line
}

// parseHeader reads line as the first line of an entry. withTime selects the
// layout: with times (the normal case) or the level-only fallback.
func parseHeader(line string, withTime bool) (header, bool) {
	if withTime {
		if !startsWithTime(line) {
			return header{}, false
		}
		m := tsHeaderRE.FindStringSubmatch(line)
		if m == nil {
			return header{}, false
		}
		h := header{at: parseLogTime(m[1]), level: m[2]}
		h.msg, h.logger = splitMessage(m[3])
		return h, true
	}
	if line == "" || (line[0] != '[' && (line[0] < 'A' || line[0] > 'Z')) {
		return header{}, false
	}
	m := levelHeaderRE.FindStringSubmatch(line)
	if m == nil {
		return header{}, false
	}
	h := header{level: m[1]}
	h.msg, h.logger = splitMessage(m[2])
	return h, true
}

// splitMessage cuts the trailing logger name off the rest of a header line and
// tidies the message: no empty MDC, runs of blanks collapsed.
func splitMessage(rest string) (msg, logger string) {
	rest = strings.TrimRight(rest, " \t")
	if m := loggerBracketRE.FindStringSubmatchIndex(rest); m != nil {
		logger = rest[m[2]:m[3]]
		rest = rest[:m[0]]
	} else if m := loggerParenRE.FindStringSubmatchIndex(rest); m != nil {
		logger = rest[m[2]:m[3]]
		rest = rest[:m[0]]
	}
	rest = emptyMDCRE.ReplaceAllString(strings.TrimLeft(rest, " \t"), "")
	return cleanLine(strings.Join(strings.Fields(rest), " ")), logger
}

// cleanLine drops control characters (an ANSI escape must not reach a UI or a
// prompt) and repairs invalid UTF-8.
func cleanLine(s string) string {
	s = strings.ToValidUTF8(s, "�")
	return strings.Map(func(r rune) rune {
		if r == '\t' {
			return ' '
		}
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
}

// parseLogTime reads the time of a header; a time without a zone is UTC. A
// time that does not parse gives the zero time.
func parseLogTime(s string) time.Time {
	s = strings.Replace(s, "T", " ", 1)
	for _, layout := range []string{"2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05Z0700", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// add folds a non-header line into the entry. A stack frame is only counted;
// the run is written as one "[N stack frames omitted]" line when the next
// kept line (or the end of the entry) comes, so "Caused by:" lines, which hold
// the root cause, stay near the front instead of after kilobytes of frames.
func (e *entry) add(line string) {
	if strings.TrimSpace(line) == "" {
		return
	}
	if frameRE.MatchString(line) || moreRE.MatchString(line) {
		e.frames++
		return
	}
	e.flushFrames()
	e.push(cleanLine(strings.TrimSpace(line)))
}

func (e *entry) flushFrames() {
	if e.frames == 0 {
		return
	}
	e.push(fmt.Sprintf("[%d stack %s omitted]", e.frames, plural(e.frames)))
	e.frames = 0
}

func plural(n int) string {
	if n == 1 {
		return "frame"
	}
	return "frames"
}

func (e *entry) push(line string) {
	if e.size+len(line) > maxEntryBytes {
		e.clipped = true
		return
	}
	e.size += len(line) + 1
	e.extra = append(e.extra, line)
}

// text is the whole retained text of the entry.
func (e *entry) text() string {
	e.flushFrames()
	t := e.msg
	if len(e.extra) > 0 {
		t += "\n" + strings.Join(e.extra, "\n")
	}
	if e.clipped {
		t += "\n…"
	}
	return t
}

// parseWorkerLog turns the tail of a log into the lines q asks for, newest
// first. floor is levelFloor(q.MinLevel).
func parseWorkerLog(data []byte, q clients.WorkerLogQuery, floor int) []clients.LogLine {
	lines := strings.Split(string(data), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(stripDocker(strings.TrimSuffix(l, "\r")), " \t")
	}
	// The layout is decided once for the whole tail: a line that merely starts
	// with "ERROR" inside a stack trace must not open an entry in a log whose
	// entries all carry a time.
	withTime := false
	for _, l := range lines {
		if tsHeaderRE.MatchString(l) {
			withTime = true
			break
		}
	}

	limit := q.MaxLines
	if limit <= 0 {
		limit = defaultWorkerLogLines
	}
	limit = min(limit, maxWorkerLogLines)

	var matched []clients.LogLine
	var cur *entry
	flush := func() {
		if cur == nil {
			return
		}
		e := cur
		cur = nil
		if levelRank(e.level) < floor {
			return
		}
		if !q.Since.IsZero() && !e.at.IsZero() && e.at.Before(q.Since) {
			return
		}
		text := e.text()
		if !mentionsConnector(text, q.Connector) {
			return
		}
		matched = append(matched, clients.LogLine{At: e.at, Level: e.level, Logger: e.logger, Message: cutMessage(text)})
	}
	for _, l := range lines {
		if h, ok := parseHeader(l, withTime); ok {
			flush()
			cur = &entry{header: h}
			continue
		}
		// Lines before the first header are the rest of an entry whose start
		// was cut off by the 4 MiB limit: nothing to attach them to.
		if cur != nil {
			cur.add(l)
		}
	}
	flush()

	// File order is time order; the result is newest first.
	out := make([]clients.LogLine, 0, min(len(matched), limit))
	for i := len(matched) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, matched[i])
	}
	return out
}

// cutMessage keeps s within maxLogMessageBytes, cut on a rune boundary, with
// an ellipsis that is inside the limit.
func cutMessage(s string) string {
	if len(s) <= maxLogMessageBytes {
		return s
	}
	const ell = "…"
	cut := maxLogMessageBytes - len(ell)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ell
}

// mentionsConnector reports whether text names the connector or one of its
// tasks: the name as a whole token, optionally followed by "-<task id>". The
// name must not be part of a longer name ("pg-orders" is not "pg-orders-eu")
// but may sit inside Debezium's "Postgres|pg-orders|streaming" or Connect's
// "[pg-orders|task-0]" and "WorkerSourceTask{id=pg-orders-0}".
func mentionsConnector(text, name string) bool {
	for from := 0; from < len(text); {
		i := strings.Index(text[from:], name)
		if i < 0 {
			return false
		}
		start := from + i
		end := start + len(name)
		if boundaryBefore(text, start) && boundaryAfter(text, end) {
			return true
		}
		from = start + 1
	}
	return false
}

func isNameRune(r rune) bool {
	return r == '_' || r == '-' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// boundaryBefore is true when the name starting at i is not the tail of a
// longer name. A '.' joins names ("x.pg-orders") only when a name rune is
// behind it, so the end of a sentence is a boundary.
func boundaryBefore(s string, i int) bool {
	if i == 0 {
		return true
	}
	r, size := utf8.DecodeLastRuneInString(s[:i])
	if isNameRune(r) {
		return false
	}
	if r == '.' && i-size > 0 {
		pr, _ := utf8.DecodeLastRuneInString(s[:i-size])
		return !isNameRune(pr)
	}
	return true
}

// boundaryAfter is true when the name ending at i is not the head of a longer
// name; "-<digits>" (a task id) is allowed once.
func boundaryAfter(s string, i int) bool {
	if i < len(s) && s[i] == '-' {
		j := i + 1
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j == i+1 {
			return false // "-eu", "-": a longer name
		}
		return plainAfter(s, j)
	}
	return plainAfter(s, i)
}

func plainAfter(s string, i int) bool {
	if i >= len(s) {
		return true
	}
	r, size := utf8.DecodeRuneInString(s[i:])
	if isNameRune(r) {
		return false
	}
	if r == '.' && i+size < len(s) {
		nr, _ := utf8.DecodeRuneInString(s[i+size:])
		return !isNameRune(nr)
	}
	return true
}
