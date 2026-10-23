package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// Caps of journal_tail (row U5a; design §2.5: logs are an injection vector,
// so they are capped, redacted on the runner and, on the server,
// injection-scanned and wrapped as untrusted). They match container_logs.
const (
	// JournalLines is how many of the newest entries one read asks for.
	JournalLines = 200
	// maxJournalCapture is what the command runner keeps of a child's
	// output (ops.runCommand keeps the head, 64 KiB); output that fills it
	// may end in a partial line.
	maxJournalCapture = 64 << 10
	// maxJournalLineBytes caps one line; a longer one is cut at a blank so no
	// token is split, or replaced by a marker when it has none.
	maxJournalLineBytes = 4 << 10
	// maxJournalDataBytes caps the JSON-encoded lines in Data, under
	// protocol.MaxDataBytes so the runner's literal redaction still fits.
	maxJournalDataBytes = 56 << 10
	// maxJournalSummaryBytes caps the lines quoted in Summary, under
	// tools.MaxSummaryBytes: a later cut there would drop the newest lines.
	maxJournalSummaryBytes = 6 << 10

	journalTarget = "host"
	journalLayer  = "environment"
)

// ErrUnit: journal_tail named a unit outside the allow pattern.
var ErrUnit = errors.New("host: unit not allowed")

// unitPrefixes are the services a CDC pipeline runs on a plain host: the
// Connect worker and Kafka (Apache, Confluent, Redpanda), Debezium Server,
// the source databases of the U wave, and the units this project installs
// (piperoom-*). A unit name must start with one of them followed by a
// separator, so journal_tail cannot read sshd, sudo or a desktop session's
// journal. When U8a's installed-unit record exists the op can narrow to it.
var unitPrefixes = []string{
	"piperoom", "kafka", "connect", "confluent", "redpanda", "zookeeper",
	"schema-registry", "debezium", "postgresql", "postgres", "mysql", "mysqld",
	"mariadb", "mongod", "mongodb", "mssql-server",
}

// unitRE is the strict shape of a unit name: lowercase, no glob characters
// (journalctl -u expands them), no backslash escapes, no leading '-' (it
// would be read as a flag), and the explicit .service suffix.
var unitRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_.@-]{0,95}\.service$`)

// JournalParams are journal_tail's typed params. The unit is the only text
// that reaches journalctl, as its own argv element after -u.
type JournalParams struct {
	// Unit is a systemd service name, e.g. "kafka-connect.service".
	Unit string `json:"unit"`
	// User reads the per-user journal (journalctl --user), for a user-level
	// unit.
	User bool `json:"user,omitempty"`
}

// journalData is journal_tail's Data. Lines are oldest first, newest last,
// redacted by pattern here and literally by the runner after.
type journalData struct {
	Unit  string   `json:"unit"`
	User  bool     `json:"user,omitempty"`
	Lines []string `json:"lines"`
	Count int      `json:"count"`
	// Tail is the -n the kept read used: JournalLines, or less when 200
	// lines did not fit the 64 KiB the command runner keeps.
	Tail          int  `json:"tail"`
	DroppedOldest int  `json:"dropped_oldest,omitempty"`
	OmittedLong   int  `json:"omitted_long,omitempty"`
	Masked        int  `json:"masked,omitempty"`
	Truncated     bool `json:"truncated,omitempty"`
}

// JournalTailOp is journal_tail (T0): the newest lines of one systemd unit's
// journal, as `journalctl -u <unit> -n 200 --no-pager -o short-iso`. It has
// no state; the host target registers it with its other ops.
type JournalTailOp struct{}

// Name is the catalogue name.
func (JournalTailOp) Name() string { return "journal_tail" }

// Target is the target kind the op reaches.
func (JournalTailOp) Target() string { return journalTarget }

// Tier is T0: a read.
func (JournalTailOp) Tier() string { return protocol.TierT0 }

// Layer is the owners layer.
func (JournalTailOp) Layer() string { return journalLayer }

// Describe is the plan and preview line.
func (JournalTailOp) Describe() string {
	return "read the last 200 journal lines of one pipeline systemd unit (capped, redacted, newest last)"
}

// ParamsSchema is the op's JSON Schema.
func (JournalTailOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"unit"},
		"properties": map[string]any{
			"unit": map[string]any{"type": "string", "pattern": unitRE.String()},
			"user": map[string]any{"type": "boolean"},
		},
	}
}

// Validate checks the params without I/O.
func (JournalTailOp) Validate(params json.RawMessage, env ops.Env) error {
	_, err := decodeJournal(params)
	return err
}

// Execute runs journalctl through the injected command runner.
func (JournalTailOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, err := decodeJournal(params)
	if err != nil {
		return ops.Result{}, err
	}
	if env.Exec == nil {
		return ops.Result{}, errors.New("host: no command runner configured")
	}
	var at time.Time
	if env.Now != nil {
		at = env.Now()
	}
	tail := JournalLines
	var (
		argv []string
		out  []byte
		code int
		cut  bool
	)
	for {
		if argv, err = JournalArgv(p, tail); err != nil {
			return ops.Result{}, err
		}
		out, code, err = env.Exec(ctx, "journalctl", argv[1:]...)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return ops.Result{}, fmt.Errorf("host: journalctl: %w", cerr)
			}
			return ops.Result{}, fmt.Errorf("%w: running journalctl (is this a systemd host?): %w", ops.ErrExec, err)
		}
		// A killed child exits non-zero without an error; the deadline is
		// the cause to report.
		if cerr := ctx.Err(); cerr != nil {
			return ops.Result{}, fmt.Errorf("host: journalctl: %w", cerr)
		}
		if len(out) > maxJournalCapture {
			out = out[:maxJournalCapture]
		}
		// The command runner keeps the HEAD of the output, which for a
		// journal is the oldest lines; halving -n is how the newest survive.
		cut = len(out) >= maxJournalCapture
		if code != 0 || !cut || tail == 1 {
			break
		}
		tail /= 2
	}
	receipt := events.SetupReceipt{Kind: "exec", Argv: argv, Target: p.Unit, At: at, ExitCode: code}
	if code != 0 {
		return ops.Result{
			Summary:   journalFailure(code, out),
			Data:      map[string]any{"unit": p.Unit, "argv": argv, "exit_code": code, "output": tailBytes(out, 4<<10)},
			Truncated: cut,
			Receipt:   receipt,
		}, fmt.Errorf("%w: journalctl exited %d", ops.ErrExec, code)
	}
	shaped := shapeJournal(out, cut)
	data := journalData{
		Unit: p.Unit, User: p.User,
		Lines: shaped.lines, Count: len(shaped.lines), Tail: tail,
		DroppedOldest: shaped.dropped, OmittedLong: shaped.omitted, Masked: shaped.masked,
	}
	data.Truncated = tail < JournalLines || shaped.dropped > 0 || shaped.omitted > 0
	head := p.Unit + " journal"
	if p.User {
		head += " (user)"
	}
	return ops.Result{
		Summary:   journalSummary(head, shaped),
		Data:      data,
		Truncated: data.Truncated,
		Receipt:   receipt,
	}, nil
}

// JournalArgv is the one argv journal_tail runs:
//
//	journalctl [--user] -u <unit> -n <tail> --no-pager -o short-iso
//
// It re-checks the unit, so no caller can pass one CheckUnit refuses, and
// tail must be 1 to JournalLines.
func JournalArgv(p JournalParams, tail int) ([]string, error) {
	if err := CheckUnit(p.Unit); err != nil {
		return nil, err
	}
	if tail < 1 || tail > JournalLines {
		return nil, fmt.Errorf("%w: tail %d is outside 1..%d", ops.ErrParams, tail, JournalLines)
	}
	argv := []string{"journalctl"}
	if p.User {
		argv = append(argv, "--user")
	}
	return append(argv, "-u", p.Unit, "-n", strconv.Itoa(tail), "--no-pager", "-o", "short-iso"), nil
}

// decodeJournal decodes params, rejecting unknown fields and trailing JSON,
// and checks the unit against the allow pattern.
func decodeJournal(params json.RawMessage) (JournalParams, error) {
	var p JournalParams
	if len(params) == 0 {
		params = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(strings.NewReader(string(params)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("%w: %v", ops.ErrParams, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return p, fmt.Errorf("%w: trailing JSON", ops.ErrParams)
	}
	if err := CheckUnit(p.Unit); err != nil {
		return p, err
	}
	return p, nil
}

// CheckUnit reports whether unit is a name journal_tail may read: the strict
// shape and one of the pipeline prefixes, followed by a separator or the
// suffix.
func CheckUnit(unit string) error {
	if !unitRE.MatchString(unit) {
		return fmt.Errorf("%w: %w: %s is not a lowercase NAME.service unit name", ops.ErrParams, ErrUnit, strconv.Quote(clipJournal(unit, 128)))
	}
	stem := strings.TrimSuffix(unit, ".service")
	for _, prefix := range unitPrefixes {
		if !strings.HasPrefix(stem, prefix) {
			continue
		}
		rest := stem[len(prefix):]
		if rest == "" || strings.ContainsRune("-@._", rune(rest[0])) {
			return nil
		}
	}
	return fmt.Errorf("%w: %w: %s is not a pipeline service (allowed prefixes: %s)", ops.ErrParams, ErrUnit, strconv.Quote(unit), strings.Join(unitPrefixes, ", "))
}

// shapedJournal is captured journal output made safe to return.
type shapedJournal struct {
	lines   []string // oldest first, newest last
	dropped int      // older lines dropped to fit the caps
	omitted int      // over-long or partial lines cut or omitted
	masked  int      // values redact.Text masked
	empty   bool     // journalctl said "-- No entries --"
}

// journalANSI matches terminal escape sequences a service may log.
var journalANSI = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)`)

// shapeJournal turns captured output into at most JournalLines lines, newest
// last (journalctl prints in time order): invalid UTF-8 and escape sequences
// out, redacted by pattern over the whole text (a PEM block spans lines),
// control characters replaced, long lines cut at a blank, the oldest lines
// dropped until the JSON-encoded lines fit maxJournalDataBytes.
func shapeJournal(raw []byte, cut bool) shapedJournal {
	var out shapedJournal
	text := strings.ToValidUTF8(string(raw), "?")
	if cut {
		// The capture stopped mid-line; that line could end in the first
		// bytes of a secret, so it goes.
		text = text[:strings.LastIndexByte(text, '\n')+1]
		out.omitted++
	}
	text = journalANSI.ReplaceAllString(text, "")
	text, st := redact.Text(text)
	out.masked = st.Total

	var lines []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		switch strings.TrimSpace(line) {
		case "":
			continue
		case "-- No entries --":
			out.empty = true
			continue
		}
		line, long := capJournalLine(journalControlFree(line))
		if long {
			out.omitted++
		}
		lines = append(lines, line)
	}
	if len(lines) > JournalLines {
		out.dropped += len(lines) - JournalLines
		lines = lines[len(lines)-JournalLines:]
	}
	size, keep := 2, len(lines)
	for i := len(lines) - 1; i >= 0; i-- {
		b, _ := json.Marshal(lines[i])
		if size+len(b)+1 > maxJournalDataBytes {
			break
		}
		size += len(b) + 1
		keep = i
	}
	out.dropped += keep
	out.lines = append([]string{}, lines[keep:]...)
	return out
}

// journalControlFree replaces control characters other than tab.
func journalControlFree(s string) string {
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

// capJournalLine cuts a line longer than maxJournalLineBytes at its last
// blank before the cap; with no blank in the cap's second half the line is
// replaced by a marker, since a cut through a token could leave the head of a
// secret the runner's literal pass cannot match.
func capJournalLine(s string) (string, bool) {
	if len(s) <= maxJournalLineBytes {
		return s, false
	}
	i := strings.LastIndexAny(s[:maxJournalLineBytes], " \t")
	if i < maxJournalLineBytes/2 {
		return fmt.Sprintf("[a %d-byte line omitted: no break before the %d-byte line cap]", len(s), maxJournalLineBytes), true
	}
	return s[:i] + fmt.Sprintf(" …[%d bytes cut]", len(s)-i), true
}

// journalSummary is the head line plus the newest lines that fit
// maxJournalSummaryBytes, newest last; Data holds every kept line.
func journalSummary(head string, s shapedJournal) string {
	var b strings.Builder
	b.WriteString(head)
	if len(s.lines) == 0 {
		b.WriteString(": no entries")
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
	shown, size := 0, 0
	for i := len(s.lines) - 1; i >= 0; i-- {
		if size+len(s.lines[i])+1 > maxJournalSummaryBytes {
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

// journalFailure is the first non-empty output line of a failed journalctl
// and its exit code.
func journalFailure(code int, out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return fmt.Sprintf("journalctl failed (exit %d): %s", code, clipJournal(journalControlFree(line), 256))
		}
	}
	return fmt.Sprintf("journalctl failed (exit %d)", code)
}

// tailBytes is the last max bytes of out as valid text.
func tailBytes(out []byte, max int) string {
	s := strings.ToValidUTF8(string(out), "?")
	if len(s) <= max {
		return s
	}
	s = s[len(s)-max:]
	for len(s) > 0 && s[0]&0xC0 == 0x80 {
		s = s[1:]
	}
	return "…" + s
}

// clipJournal cuts s to max bytes on a rune boundary.
func clipJournal(s string, max int) string {
	s = strings.ToValidUTF8(s, "?")
	if len(s) <= max {
		return s
	}
	for max > 0 && s[max]&0xC0 == 0x80 {
		max--
	}
	return s[:max] + "…"
}
