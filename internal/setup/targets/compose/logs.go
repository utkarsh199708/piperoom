package compose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Caps of container_logs (row U5a; design §2.5: "container logs are an
// injection vector", so they are capped, redacted and, on the server,
// injection-scanned and wrapped as untrusted).
const (
	// LogLines is how many of the newest lines one read asks docker for.
	LogLines = 200
	// maxLogLineBytes caps one line. A longer line is replaced by a marker
	// (capLine): a cut anywhere would leave a fragment of a secret that
	// literal redaction can no longer recognise.
	maxLogLineBytes = 4 << 10
	// maxLogDataBytes caps the JSON-encoded lines in Data. It sits under
	// protocol.MaxDataBytes (64 KiB) so the runner's literal redaction, which
	// replaces a stored value by ${secret:name} and can grow the text, still
	// fits the cap; the oldest lines go first.
	maxLogDataBytes = 56 << 10
	// maxLogSummaryBytes caps the lines quoted in Summary, under
	// tools.MaxSummaryBytes for the same reason: the runner's truncation cuts
	// the END of a summary, which is where the newest lines are.
	maxLogSummaryBytes = 6 << 10
)

// ErrNotInProject: container_logs named a service that is not in the
// project's validated compose file. Nothing ran: not even `compose ps`.
var ErrNotInProject = errors.New("compose: service is not in the compose project")

// LogsParams are container_logs' typed params: one service of the rendered
// project. The line count is fixed (LogLines); no field reaches docker except
// the service name, which only selects a container id from `compose ps -q`
// (running) or `compose ps -a -q` (stopped too).
type LogsParams struct {
	Service string `json:"service"`
	File    string `json:"file,omitempty"`
}

// containerLogsData is container_logs' Data. Lines are oldest first, newest
// last, redacted by pattern here and literally by the runner after.
type containerLogsData struct {
	Service    string `json:"service"`
	File       string `json:"file"`
	Container  string `json:"container"`
	Containers int    `json:"containers"`
	// Stopped: the service has no running container, so a stopped one
	// (`compose ps -a`) was read: the logs of a crash.
	Stopped bool     `json:"stopped,omitempty"`
	Lines   []string `json:"lines"`
	Count   int      `json:"count"`
	// Tail is the --tail the kept read used: LogLines, or less when 200
	// lines did not fit the 64 KiB the command runner keeps.
	Tail          int `json:"tail"`
	DroppedOldest int `json:"dropped_oldest,omitempty"`
	OmittedLong   int `json:"omitted_long,omitempty"`
	// Masked counts the values masked in the whole read, lines later
	// dropped to fit the caps included.
	Masked    int  `json:"masked,omitempty"`
	Truncated bool `json:"truncated,omitempty"`
}

// container_logs (T0) reads the newest lines of one service's container.
type containerLogsOp struct{ d *Driver }

func (containerLogsOp) Name() string   { return "container_logs" }
func (containerLogsOp) Target() string { return targetKind }
func (containerLogsOp) Tier() string   { return protocol.TierT0 }
func (containerLogsOp) Layer() string  { return layerName }
func (containerLogsOp) Describe() string {
	return "read the last 200 log lines of one service's container in the project (capped, redacted, newest last)"
}

func (containerLogsOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"service"},
		"properties": map[string]any{
			"service": map[string]any{"type": "string", "minLength": 1, "maxLength": 63},
			"file":    map[string]any{"type": "string"},
		},
	}
}

func (containerLogsOp) Validate(params json.RawMessage, env ops.Env) error {
	_, err := decodeLogs(params)
	return err
}

func (o containerLogsOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, err := decodeLogs(params)
	if err != nil {
		return ops.Result{}, err
	}
	clean, err := stackFiles(env, p.File)
	if err != nil {
		return ops.Result{}, err
	}
	start := now(env)
	// The service is checked against the validated snapshot docker will be
	// given, before any docker call: a name outside the project never reaches
	// `compose ps`, and the id `docker logs` gets is always one that ps
	// printed for this project (-p pins it).
	snap, project, cleanup, err := stackSnapshot(env, clean, o.d.classOrDefault())
	if err != nil {
		return ops.Result{}, err
	}
	defer cleanup()
	services, err := snapshotServices(snap)
	if err != nil {
		return ops.Result{}, err
	}
	if !containsName(services, p.Service) {
		return ops.Result{}, fmt.Errorf("%w: %s is not a service of %s (services: %s)", ErrNotInProject, q(p.Service), q(clean), strings.Join(services, ", "))
	}
	// A crashed container is the main reason to read logs, and `compose ps
	// -q` lists running containers only: a running one is preferred, and
	// with none the stopped ones (`ps -a -q`) are read instead.
	ids, stopped := []string(nil), false
	for _, flags := range [][]string{{"-q"}, {"-a", "-q"}} {
		psArgv, err := stackArgv(env.Confine.Dir(), p.File, "ps", flags, []string{p.Service})
		if err != nil {
			return ops.Result{}, err
		}
		if psArgv, err = snapshotArgv(psArgv, snap, project); err != nil {
			return ops.Result{}, err
		}
		psRes, err := dockerCommand(ctx, env, psArgv)
		if err != nil {
			return ops.Result{}, err
		}
		if err := ctx.Err(); err != nil {
			return ops.Result{}, fmt.Errorf("compose: container_logs: %w", err)
		}
		if psRes.code != 0 {
			line, output := logFailure(psRes)
			return ops.Result{
				Summary:   failureLine("compose ps "+strings.Join(flags, " ")+" "+p.Service, psRes.code, line),
				Data:      map[string]any{"service": p.Service, "argv": psArgv, "exit_code": psRes.code, "output": output},
				Truncated: psRes.truncated,
				Receipt:   events.SetupReceipt{Kind: "exec", Argv: psArgv, Target: clean, At: start, ExitCode: psRes.code},
			}, fmt.Errorf("%w: compose ps exited %d", ops.ErrExec, psRes.code)
		}
		if ids, _ = parseContainerIDs(psRes.out); len(ids) > 0 {
			break
		}
		stopped = true
	}
	if len(ids) == 0 {
		return ops.Result{}, fmt.Errorf("%w: service %s has no container, running or stopped, in %s; run compose_up first", ErrStack, q(p.Service), q(clean))
	}
	// Several containers (a scaled service): the first one ps printed is
	// read, and the summary says "first of N".
	id := ids[0]

	read, err := readDockerLogs(ctx, env, id)
	if err != nil {
		return ops.Result{}, err
	}
	if read.res.code != 0 {
		line, output := logFailure(read.res)
		return ops.Result{
			Summary:   failureLine("docker logs", read.res.code, line),
			Data:      map[string]any{"service": p.Service, "argv": read.argv, "exit_code": read.res.code, "output": output},
			Truncated: read.res.truncated,
			Receipt:   events.SetupReceipt{Kind: "exec", Argv: read.argv, Target: clean, At: start, ExitCode: read.res.code},
		}, fmt.Errorf("%w: docker logs exited %d", ops.ErrExec, read.res.code)
	}

	shaped := shapeLog(read.res.out, read.cut, true)
	data := containerLogsData{
		Service:       p.Service,
		File:          clean,
		Container:     shortID(id),
		Containers:    len(ids),
		Stopped:       stopped,
		Lines:         shaped.lines,
		Count:         len(shaped.lines),
		Tail:          read.tail,
		DroppedOldest: shaped.dropped,
		OmittedLong:   shaped.omitted,
		Masked:        shaped.masked,
	}
	data.Truncated = read.tail < LogLines || shaped.dropped > 0 || shaped.omitted > 0
	head := fmt.Sprintf("%s logs (container %s", p.Service, shortID(id))
	if stopped {
		head += ", not running"
	}
	if len(ids) > 1 {
		head += fmt.Sprintf(", first of %d", len(ids))
	}
	head += ")"
	return ops.Result{
		Summary:   logSummary(head, shaped),
		Data:      data,
		Truncated: data.Truncated,
		Receipt:   events.SetupReceipt{Kind: "exec", Argv: read.argv, Target: clean, At: start},
	}, nil
}

// decodeLogs decodes and validates container_logs params without I/O.
func decodeLogs(params json.RawMessage) (LogsParams, error) {
	var p LogsParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if !serviceNameRE.MatchString(p.Service) {
		return p, fmt.Errorf("%w: service %s is not a service name", ErrParams, q(p.Service))
	}
	if p.File == "" {
		p.File = DefaultFile
	}
	if err := ops.CheckName(p.File); err != nil {
		return p, fmt.Errorf("%w: file: %v", ErrPath, err)
	}
	if !isComposeName(p.File) {
		return p, fmt.Errorf("%w: file %s is not a compose file name", ErrParams, q(p.File))
	}
	return p, nil
}

// snapshotServices lists the services of the validated snapshot, sorted.
// stackSnapshot has already validated those exact bytes.
func snapshotServices(snap string) ([]string, error) {
	doc, err := os.ReadFile(snap)
	if err != nil {
		return nil, fmt.Errorf("compose: reading the compose snapshot: %w", err)
	}
	var head struct {
		Services map[string]yaml.Node `yaml:"services"`
	}
	if err := yaml.Unmarshal(doc, &head); err != nil {
		return nil, ruleError(RuleYAML, "", "the document is not valid YAML")
	}
	names := make([]string, 0, len(head.Services))
	for name := range head.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func containsName(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// logRead is the kept docker logs invocation.
type logRead struct {
	argv []string
	res  commandResult
	tail int
	// cut: the output filled the 64 KiB the command runner keeps, so its
	// last line may be partial.
	cut bool
}

// readDockerLogs runs `docker logs --timestamps --tail N <id>` with N = 200,
// halving N while the output fills the command runner's 64 KiB. The runner
// keeps the HEAD of a child's output (ops.runCommand), which for a log is the
// oldest lines; a smaller tail is how the newest lines survive the cap.
func readDockerLogs(ctx context.Context, env ops.Env, id string) (logRead, error) {
	if !containerIDRE.MatchString(id) {
		return logRead{}, fmt.Errorf("%w: %s is not a container id", ErrParams, q(id))
	}
	tail := LogLines
	for {
		argv := []string{"docker", "logs", "--timestamps", "--tail", strconv.Itoa(tail), id}
		res, err := dockerCommand(ctx, env, argv)
		if err != nil {
			return logRead{}, err
		}
		// A killed child exits non-zero without an error; the deadline is
		// the cause to report.
		if cerr := ctx.Err(); cerr != nil {
			return logRead{}, fmt.Errorf("compose: docker logs: %w", cerr)
		}
		cut := len(res.out) >= maxCommandOutput
		if res.code != 0 || !cut || tail == 1 {
			return logRead{argv: argv, res: res, tail: tail, cut: cut}, nil
		}
		tail /= 2
	}
}

// shapedLog is captured log output made safe to return.
type shapedLog struct {
	lines   []string // oldest first, newest last
	dropped int      // older lines dropped to fit maxLogDataBytes or LogLines
	omitted int      // lines too long to keep (partial at the capture cap, or over the line cap)
	masked  int      // values masked anywhere in the read, dropped lines included
}

// ansiRE matches the terminal escape sequences (colours, cursor moves) many
// images print; they are noise to the model and can hide text from a person.
var ansiRE = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)`)

// dockerStampRE is the RFC 3339 timestamp and blank `docker logs
// --timestamps` puts before every line.
var dockerStampRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2}) `)

// shapeLog turns captured output into at most LogLines lines, newest last:
// invalid UTF-8 and escape sequences out, redacted by pattern, control
// characters replaced, over-long lines omitted, and the oldest lines dropped
// until the JSON-encoded lines fit maxLogDataBytes. Redaction runs over the
// messages with their timestamps taken off and joined in capture order, so a
// value that spans lines (a PEM block, a quoted value) is one span again, and
// every PEM block is masked line by line first (maskPEMLines), so an
// interleaved line cannot split one. byTime then sorts by the timestamp:
// docker writes the container's stdout and stderr to two pipes, so their
// lines can arrive out of order.
func shapeLog(raw []byte, cut, byTime bool) shapedLog {
	var out shapedLog
	// "?" never grows the text, so it stays inside redact.MaxScan.
	text := strings.ToValidUTF8(string(raw), "?")
	if cut {
		// The capture stopped mid-line; that line is partial (and could end
		// in the first bytes of a secret), so it goes.
		i := strings.LastIndexByte(text, '\n')
		text = text[:i+1]
		out.omitted++
	}
	text = ansiRE.ReplaceAllString(text, "")
	var stamps, msgs []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		stamp := dockerStampRE.FindString(line)
		stamps = append(stamps, stamp)
		msgs = append(msgs, line[len(stamp):])
	}
	out.masked = maskPEMLines(msgs)
	redacted, n := redactLines(stamps, msgs)
	out.masked += n

	var lines []string
	for _, line := range redacted {
		line, long := capLine(controlFree(line))
		if long {
			out.omitted++
		}
		lines = append(lines, line)
	}
	if byTime {
		sortByTimestamp(lines)
	}
	if len(lines) > LogLines {
		out.dropped += len(lines) - LogLines
		lines = lines[len(lines)-LogLines:]
	}
	size := 2 // the brackets of the JSON array
	keep := len(lines)
	for i := len(lines) - 1; i >= 0; i-- {
		n := jsonLen(lines[i]) + 1
		if size+n > maxLogDataBytes {
			break
		}
		size += n
		keep = i
	}
	out.dropped += keep
	out.lines = append([]string{}, lines[keep:]...)
	return out
}

// pemMask replaces a line, or the rest of one, inside a PEM block.
const pemMask = "[redacted PEM block line]"

// maskPEMLines masks, in place and in capture order, every message from the
// one that opens a PEM block ("-----BEGIN ") through the one that closes it
// ("-----END "), or to the last message when none closes it, and returns how
// many blocks it masked. A block opened and closed within one message (a
// JSON string with \n escapes) is left to redact.Text, which masks it there.
// This does not depend on redact.Text seeing the block as one span, which a
// line prefix or a line interleaved from the other stream would break; the
// cost is that a line interleaved inside a block is masked with it.
func maskPEMLines(msgs []string) int {
	blocks, open := 0, false
	for i, msg := range msgs {
		wasOpen := open
		open = pemOpenAfter(msg, open)
		switch {
		case wasOpen:
			msgs[i] = pemMask
		case open:
			blocks++
			msgs[i] = msg[:strings.Index(msg, "-----BEGIN ")] + pemMask
		}
	}
	return blocks
}

// pemOpenAfter reports whether a PEM block is still open after msg, given
// whether one was open before it.
func pemOpenAfter(msg string, open bool) bool {
	for {
		mark := "-----BEGIN "
		if open {
			mark = "-----END "
		}
		i := strings.Index(msg, mark)
		if i < 0 {
			return open
		}
		msg, open = msg[i+len(mark):], !open
	}
}

// redactLines runs redact.Text over the messages joined in order and puts
// each prefix back on its message. When a mask spanned a line break, so the
// redacted messages no longer line up with their prefixes, or when a prefix
// itself holds something redact.Text masks, it redacts the prefixed lines
// instead. It returns the lines and how many values it masked.
func redactLines(prefixes, msgs []string) ([]string, int) {
	text, st := redact.Text(strings.Join(msgs, "\n"))
	joined := strings.Join(prefixes, "\n")
	clean, _ := redact.Text(joined)
	if lines := strings.Split(text, "\n"); len(lines) == len(msgs) && clean == joined {
		for i := range lines {
			lines[i] = prefixes[i] + lines[i]
		}
		return lines, st.Total
	}
	whole := make([]string, len(msgs))
	for i := range msgs {
		whole[i] = prefixes[i] + msgs[i]
	}
	text, st = redact.Text(strings.Join(whole, "\n"))
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines, st.Total
}

// controlFree replaces control and format characters other than tab (C0,
// DEL, C1, the Unicode line and paragraph separators, and format characters
// such as bidi overrides and zero-width spaces), so a log line cannot move a
// terminal cursor, forge a line break in a summary or hide text from a
// person.
func controlFree(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\t' {
			return r
		}
		if unicode.In(r, unicode.Cc, unicode.Cf, unicode.Zl, unicode.Zp) {
			return '?'
		}
		return r
	}, s)
}

// capLine replaces a line longer than maxLogLineBytes by a marker, keeping
// its timestamp, and reports whether it did. Nothing of the line is kept: a
// cut anywhere, even at a blank, could leave the head of a secret (a stored
// value can hold blanks and be longer than the cap) that the runner's
// literal pass can no longer match.
func capLine(s string) (string, bool) {
	if len(s) <= maxLogLineBytes {
		return s, false
	}
	return dockerStampRE.FindString(s) + fmt.Sprintf("[a %d-byte line omitted: over the %d-byte line cap]", len(s), maxLogLineBytes), true
}

// logFailure is a failed command's output made safe to return: shaped like a
// log (redacted, control-free, whole lines only, so no line starts or ends
// mid-token) in capture order. It returns the last line, for the summary,
// and the newest lines that fit maxFailureOutput, for Data.
func logFailure(res commandResult) (string, string) {
	shaped := shapeLog(res.out, len(res.out) >= maxCommandOutput, false)
	if len(shaped.lines) == 0 {
		return "", ""
	}
	keep, size := len(shaped.lines)-1, len(shaped.lines[len(shaped.lines)-1])
	for keep > 0 && size+len(shaped.lines[keep-1])+1 <= maxFailureOutput {
		keep--
		size += len(shaped.lines[keep]) + 1
	}
	return shaped.lines[len(shaped.lines)-1], strings.Join(shaped.lines[keep:], "\n")
}

// maxFailureLine bounds the output line a failure summary quotes. A longer
// line is not cut (a cut could split a secret); Data holds it whole.
const maxFailureLine = 512

// failureLine is the summary of a failed command: what ran, its exit code
// and its last shaped output line.
func failureLine(what string, code int, line string) string {
	switch {
	case line == "":
		return fmt.Sprintf("%s failed (exit %d)", what, code)
	case len(line) > maxFailureLine:
		return fmt.Sprintf("%s failed (exit %d); the output is in data", what, code)
	}
	return fmt.Sprintf("%s failed (exit %d): %s", what, code, line)
}

// sortByTimestamp stable-sorts lines by their leading RFC 3339 timestamp. A
// line without one keeps the time of the line before it, so a continuation
// stays with its first line.
func sortByTimestamp(lines []string) {
	type keyed struct {
		t    time.Time
		line string
	}
	ks := make([]keyed, len(lines))
	var last time.Time
	for i, line := range lines {
		if sp := strings.IndexByte(line, ' '); sp > 0 {
			if t, err := time.Parse(time.RFC3339Nano, line[:sp]); err == nil {
				last = t
			}
		}
		ks[i] = keyed{t: last, line: line}
	}
	sort.SliceStable(ks, func(a, b int) bool { return ks[a].t.Before(ks[b].t) })
	for i := range ks {
		lines[i] = ks[i].line
	}
}

// jsonLen is the length of s as a JSON string.
func jsonLen(s string) int {
	b, err := json.Marshal(s)
	if err != nil {
		return len(s) * 6
	}
	return len(b)
}

// logSummary is the head line plus the newest lines that fit
// maxLogSummaryBytes, newest last; Data holds every kept line.
func logSummary(head string, s shapedLog) string {
	var b strings.Builder
	b.WriteString(head)
	if len(s.lines) == 0 {
		b.WriteString(": no log lines")
	} else {
		fmt.Fprintf(&b, ": %d lines, newest last", len(s.lines))
	}
	if s.dropped > 0 {
		fmt.Fprintf(&b, "; %d older lines dropped to fit the cap", s.dropped)
	}
	if s.omitted > 0 {
		fmt.Fprintf(&b, "; %d over-long or partial lines omitted", s.omitted)
	}
	if s.masked > 0 {
		fmt.Fprintf(&b, "; %d values masked in the read", s.masked)
	}
	shown := 0
	size := 0
	for i := len(s.lines) - 1; i >= 0; i-- {
		if size+len(s.lines[i])+1 > maxLogSummaryBytes {
			break
		}
		size += len(s.lines[i]) + 1
		shown++
	}
	if shown < len(s.lines) {
		fmt.Fprintf(&b, " (the last %d here, all %d in data)", shown, len(s.lines))
	}
	if shown > 0 {
		b.WriteString(":")
	}
	for _, line := range s.lines[len(s.lines)-shown:] {
		b.WriteString("\n")
		b.WriteString(line)
	}
	return b.String()
}
