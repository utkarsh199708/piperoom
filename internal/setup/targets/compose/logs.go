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
	// maxLogLineBytes caps one line. A longer line is cut at a blank so no
	// token is split (a cut through a secret would leave a fragment that
	// literal redaction can no longer recognise); a line with no blank
	// before the cap is replaced by a marker.
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
// the service name, which only selects a container id from `compose ps -q`.
type LogsParams struct {
	Service string `json:"service"`
	File    string `json:"file,omitempty"`
}

// containerLogsData is container_logs' Data. Lines are oldest first, newest
// last, redacted by pattern here and literally by the runner after.
type containerLogsData struct {
	Service    string   `json:"service"`
	File       string   `json:"file"`
	Container  string   `json:"container"`
	Containers int      `json:"containers"`
	Lines      []string `json:"lines"`
	Count      int      `json:"count"`
	// Tail is the --tail the kept read used: LogLines, or less when 200
	// lines did not fit the 64 KiB the command runner keeps.
	Tail          int  `json:"tail"`
	DroppedOldest int  `json:"dropped_oldest,omitempty"`
	OmittedLong   int  `json:"omitted_long,omitempty"`
	Masked        int  `json:"masked,omitempty"`
	Truncated     bool `json:"truncated,omitempty"`
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
	psArgv, err := stackArgv(env.Confine.Dir(), p.File, "ps", []string{"-q"}, []string{p.Service})
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
		return ops.Result{
			Summary:   failureSummary("compose ps -q "+p.Service, psRes),
			Data:      map[string]any{"service": p.Service, "argv": psArgv, "exit_code": psRes.code, "output": tailText(psRes.out, maxFailureOutput)},
			Truncated: psRes.truncated,
			Receipt:   events.SetupReceipt{Kind: "exec", Argv: psArgv, Target: clean, At: start, ExitCode: psRes.code},
		}, fmt.Errorf("%w: compose ps exited %d", ops.ErrExec, psRes.code)
	}
	ids, _ := parseContainerIDs(psRes.out)
	if len(ids) == 0 {
		return ops.Result{}, fmt.Errorf("%w: service %s has no container in %s; run compose_up first", ErrStack, q(p.Service), q(clean))
	}
	id := ids[0]

	read, err := readDockerLogs(ctx, env, id)
	if err != nil {
		return ops.Result{}, err
	}
	if read.res.code != 0 {
		return ops.Result{
			Summary:   failureSummary("docker logs", read.res),
			Data:      map[string]any{"service": p.Service, "argv": read.argv, "exit_code": read.res.code, "output": tailText(read.res.out, maxFailureOutput)},
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
		Lines:         shaped.lines,
		Count:         len(shaped.lines),
		Tail:          read.tail,
		DroppedOldest: shaped.dropped,
		OmittedLong:   shaped.omitted,
		Masked:        shaped.masked,
	}
	data.Truncated = read.tail < LogLines || shaped.dropped > 0 || shaped.omitted > 0
	head := fmt.Sprintf("%s logs (container %s", p.Service, shortID(id))
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
	omitted int      // lines too long to keep (partial at the capture cap, or no blank to cut at)
	masked  int      // values redact.Text masked
}

// ansiRE matches the terminal escape sequences (colours, cursor moves) many
// images print; they are noise to the model and can hide text from a person.
var ansiRE = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)`)

// shapeLog turns captured output into at most LogLines lines, newest last:
// invalid UTF-8 and escape sequences out, redacted by pattern over the whole
// text (a PEM block or a multi-line JSON value spans lines), control
// characters replaced, long lines cut at a blank, and the oldest lines
// dropped until the JSON-encoded lines fit maxLogDataBytes. byTime sorts by
// the leading RFC 3339 timestamp `docker logs --timestamps` prints: docker
// writes the container's stdout and stderr to two pipes, so their lines can
// arrive out of order.
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
	text, st := redact.Text(text)
	out.masked = st.Total

	var lines []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
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

// controlFree replaces control characters other than tab, so a log line
// cannot move a terminal cursor or forge a line break in a summary.
func controlFree(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\t' {
			return r
		}
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return '?'
		}
		return r
	}, s)
}

// capLine cuts a line longer than maxLogLineBytes at its last blank before
// the cap, and reports whether it cut. A line with no blank in the second
// half of the cap is replaced by a marker: cutting through a token could
// leave the head of a secret the runner's literal pass cannot match.
func capLine(s string) (string, bool) {
	if len(s) <= maxLogLineBytes {
		return s, false
	}
	i := strings.LastIndexAny(s[:maxLogLineBytes], " \t")
	if i < maxLogLineBytes/2 {
		return fmt.Sprintf("[a %d-byte line omitted: no break before the %d-byte line cap]", len(s), maxLogLineBytes), true
	}
	return s[:i] + fmt.Sprintf(" …[%d bytes cut]", len(s)-i), true
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
		fmt.Fprintf(&b, "; %d over-long lines cut or omitted", s.omitted)
	}
	if s.masked > 0 {
		fmt.Fprintf(&b, "; %d values masked", s.masked)
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
