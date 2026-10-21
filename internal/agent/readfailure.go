package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"syscall"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// KAN-408 (eval scenario 13): a Schema Registry read failed with a hard
// connection error and the agent dismissed it because the pipeline still moved
// data. On a CDC pipeline Kafka consumers keep going on cached schema ids for a
// while after the registry stops, so "no data impact" proves nothing about the
// component that refused the connection. A read that cannot reach its
// component is evidence about that component, not a benign unknown.
//
// This file makes the rule mechanical:
//   - every failed read is classified (unreachable / auth / not-found / other)
//     and its result carries a stable fact the model reads, for example
//     "error: unreachable: schema-registry (connection refused): ..."
//     (the A2/A3 tools may emit the same fact themselves; classify reads it
//     back and never doubles it);
//   - an unresolved unreachable gap refuses a healthy verdict in
//     update_hypothesis and in post_message, with the component, tool, step
//     and what to check;
//   - a later successful read of the same tool turns the failure into a blip
//     and clears the gap, so a transient blip does not block a healthy verdict;
//   - auth and not-found errors are findings too, but not outages: they never
//     block a healthy verdict.

// readFailureClass is what one failed read proves about its component.
type readFailureClass int

const (
	// readFailureOther is a read error that says nothing about reachability
	// (bad input, an out-of-scope resource, a bug). It keeps the old wording.
	readFailureOther readFailureClass = iota
	readFailureUnreachable
	readFailureAuth
	readFailureNotFound
)

// factName is the first token of the stable fact put on a failed read. The
// model reads it and the tests parse it; it is never localised or reworded.
func (c readFailureClass) factName() string {
	switch c {
	case readFailureUnreachable:
		return "unreachable"
	case readFailureAuth:
		return "auth"
	case readFailureNotFound:
		return "not-found"
	}
	return ""
}

// readFailure is one classified failed read.
type readFailure struct {
	class     readFailureClass
	tool      string
	component string // the pipeline layer whose health the failure speaks to
	reason    string // "connection refused", "timeout", "dns", "401", "404" ...
	stepID    string
	seq       int // newest wins when a component has more than one open gap
}

// fact renders the stable, model-visible fact, e.g.
// "unreachable: schema-registry (connection refused)". A class with no fact
// (other) renders "".
func (f readFailure) fact() string {
	if f.class == readFailureOther {
		return ""
	}
	return fmt.Sprintf("%s: %s (%s)", f.class.factName(), f.component, f.reason)
}

// readFailures remembers this run's failed reads by tool. It is per-runner
// state, never global; a restart loses it, which only makes the agent
// re-read, the same way the step ledger does.
type readFailures struct {
	mu   sync.Mutex
	open map[string]readFailure
	seq  int
}

func newReadFailures() *readFailures {
	return &readFailures{open: map[string]readFailure{}}
}

// record stores a failed read as an open gap for its tool. It is a no-op for
// a class that says nothing about the component, and on a nil tracker.
func (l *readFailures) record(f readFailure) {
	if l == nil || f.class == readFailureOther || f.tool == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	f.seq = l.seq
	l.open[f.tool] = f
}

// succeeded clears the open gap of tool after a successful read of it and
// reports the failure the read just resolved, if any. The caller stamps the
// result as a blip so the model reports a transient failure as one.
func (l *readFailures) succeeded(tool string) (readFailure, bool) {
	if l == nil {
		return readFailure{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	f, ok := l.open[tool]
	if !ok {
		return readFailure{}, false
	}
	delete(l.open, tool)
	return f, true
}

// unreachable returns the newest unresolved unreachable gap. Only this class
// blocks a healthy verdict: auth and not-found say the read ran and the
// answer is about credentials or a missing object, not about an outage.
func (l *readFailures) unreachable() (readFailure, bool) {
	if l == nil {
		return readFailure{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var newest readFailure
	found := false
	for _, f := range l.open {
		if f.class != readFailureUnreachable {
			continue
		}
		if !found || f.seq > newest.seq {
			newest, found = f, true
		}
	}
	return newest, found
}

// seedTriage records the failed reads of the first-look pass, which ran in
// its own runner and is shown to this run as alert.Triage. Their summaries
// carry the same stable fact (the pass runs its reads through runReadTool),
// so a registry that was unreachable in the first look blocks a healthy
// verdict here too, until this run reads the component successfully.
func (l *readFailures) seedTriage(t *TriageResult) {
	if l == nil || t == nil {
		return
	}
	for _, s := range t.Steps {
		if !s.IsError {
			continue
		}
		f, ok := parseFailureFact(s.Tool, s.Summary)
		if !ok {
			continue
		}
		f.stepID = s.StepID
		l.record(f)
	}
}

// failureFactRE matches a stable fact at the start of a tool error, with or
// without the "error: " prefix the runner adds.
var failureFactRE = regexp.MustCompile(`^(?:error: )?(unreachable|auth|not-found): ([A-Za-z0-9_.-]+) \(([^)]*)\)`)

// parseFailureFact reads a stable fact back from a tool error or summary. It
// lets a fact the A2/A3 tools emit themselves be reused unchanged, and lets
// the triage seeds feed this run's gaps.
func parseFailureFact(tool, text string) (readFailure, bool) {
	m := failureFactRE.FindStringSubmatch(strings.TrimSpace(text))
	if m == nil {
		return readFailure{}, false
	}
	f := readFailure{tool: tool, component: m[2], reason: m[3]}
	switch m[1] {
	case "unreachable":
		f.class = readFailureUnreachable
	case "auth":
		f.class = readFailureAuth
	case "not-found":
		f.class = readFailureNotFound
	default:
		return readFailure{}, false
	}
	if f.component == "" {
		f.component = componentForTool(tool)
	}
	return f, true
}

// statusCodeRE matches the HTTP status spelling of the client packages
// ("status 401: ...", "schema registry error 401: ...").
var statusCodeRE = regexp.MustCompile(`\b(?:status|error|code)\s*[:=]?\s*(401|403|404)\b`)

// classifyReadFailure maps one tool error to a class, its component and a
// short reason. Network failures are recognised through the typed error chain
// (errors.Is/As). HTTP status classes fall back to the error text because the
// client packages' status errors are unexported; A2/A3 will emit the stable
// fact from the client side, and parseFailureFact already prefers it.
func classifyReadFailure(tool string, err error) readFailure {
	f := readFailure{tool: tool, component: componentForTool(tool)}
	if err == nil {
		return f
	}
	if parsed, ok := parseFailureFact(tool, err.Error()); ok {
		return parsed
	}
	f.class, f.reason = classOfError(err)
	return f
}

// classOfError is classifyReadFailure without the tool and fact handling.
func classOfError(err error) (readFailureClass, string) {
	// A rejected input or scope never reached the component, so it says
	// nothing about its reachability however its text reads.
	if errors.Is(err, tools.ErrBadInput) || errors.Is(err, tools.ErrOutOfScope) || errors.Is(err, tools.ErrNoScope) {
		return readFailureOther, ""
	}
	// A missing object is an answer, even when the client spells it 404.
	if errors.Is(err, clients.ErrNotFound) {
		return readFailureNotFound, "not found"
	}
	// A deadline is the component not answering in time: the tool's own 10 s
	// deadline and a context deadline both land here.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return readFailureUnreachable, "timeout"
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return readFailureUnreachable, "connection refused"
	case errors.Is(err, syscall.ECONNRESET):
		return readFailureUnreachable, "connection reset"
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return readFailureUnreachable, "network unreachable"
	case errors.Is(err, syscall.ETIMEDOUT):
		return readFailureUnreachable, "timeout"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return readFailureUnreachable, "dns"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return readFailureUnreachable, "timeout"
	}

	// Fallback for errors whose type was lost on the way (a tool that re-wraps
	// with %s, a recorded error text). Keep each marker narrow: "not found" is
	// an answer, not an outage, so it is checked before the network words.
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection refused"), strings.Contains(msg, "actively refused"):
		// Windows says "connectex: No connection could be made because the
		// target machine actively refused it"; its Errno does not compare with
		// syscall.ECONNREFUSED, so the text is the portable signal there.
		return readFailureUnreachable, "connection refused"
	case strings.Contains(msg, "connection reset"), strings.Contains(msg, "forcibly closed"):
		return readFailureUnreachable, "connection reset"
	case strings.Contains(msg, "no such host"), strings.Contains(msg, "dns lookup"),
		strings.Contains(msg, "server misbehaving"), strings.Contains(msg, "could not be resolved"):
		return readFailureUnreachable, "dns"
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "context deadline exceeded"),
		strings.Contains(msg, "timed out"), strings.Contains(msg, "timeout"),
		strings.Contains(msg, "did not properly respond"):
		return readFailureUnreachable, "timeout"
	case strings.Contains(msg, "network is unreachable"), strings.Contains(msg, "no route to host"):
		return readFailureUnreachable, "network unreachable"
	}
	if m := statusCodeRE.FindStringSubmatch(msg); m != nil {
		switch m[1] {
		case "401", "403":
			return readFailureAuth, m[1]
		case "404":
			return readFailureNotFound, "404"
		}
	}
	switch {
	case strings.Contains(msg, "unauthorized"), strings.Contains(msg, "forbidden"):
		return readFailureAuth, "403"
	case strings.Contains(msg, "not found"):
		return readFailureNotFound, "not found"
	}
	return readFailureOther, ""
}

// componentForTool maps a read tool to the pipeline layer whose health its
// failure speaks to, using the canonical layer names (events.Layers). A tool
// the prefix table does not know is its own component, which is still exact.
func componentForTool(tool string) string {
	switch {
	case strings.HasPrefix(tool, "schema_registry_"):
		return "schema-registry"
	case strings.HasPrefix(tool, "connect_"), strings.HasPrefix(tool, "debezium_"):
		return "kafka-connect"
	case strings.HasPrefix(tool, "kafka_"), strings.HasPrefix(tool, "dlq_"):
		return "kafka"
	case strings.HasPrefix(tool, "pg_"):
		return "postgres"
	case strings.HasPrefix(tool, "warehouse_"):
		return "warehouse"
	}
	return tool
}

// failedReadSummary renders the model-visible summary of a failed read: the
// stable fact first, then the tool's own error text. An error that already
// carries the fact (A2/A3 emit it in the client) is not prefixed twice.
func failedReadSummary(f readFailure, detail string) string {
	if f.fact() == "" {
		return "error: " + detail
	}
	if _, ok := parseFailureFact(f.tool, detail); ok {
		// The tool (or the caller's test) already rendered the fact, with or
		// without the runner's prefix; never say it twice.
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(detail)), "error: ") {
			return detail
		}
		return "error: " + detail
	}
	return "error: " + f.fact() + ": " + detail
}

// blipSummary renders a successful read that resolved an earlier failure of
// the same tool: the model is told it is a blip, with the step that failed,
// so it does not keep reporting a fault the component no longer has.
func blipSummary(f readFailure, summary string) string {
	return "blip: " + f.fact() + " at " + f.stepID + "; this read now answers: " + summary
}

// healthyVerdictBlocked returns the unresolved unreachable gap that refuses a
// healthy verdict, if there is one.
func healthyVerdictBlocked(gaps *readFailures) (readFailure, bool) {
	return gaps.unreachable()
}

// blockedHealthyError is the refusal the model gets instead of a healthy
// verdict while a component is unreachable. It names the evidence and what
// to do, so the next call can succeed.
func blockedHealthyError(f readFailure) error {
	return fmt.Errorf("%w: a healthy verdict is blocked: %s is unreachable (%s at %s); name the read failure in the hypothesis and say what to check on %s, or read it again successfully first. You can say no data loss yet and the risk is rising",
		tools.ErrBadInput, f.component, f.fact(), f.stepID, f.component)
}

// healthyClaimRE matches wording that claims the pipeline is healthy or needs
// no fix. It mirrors the deterministic healthy vocabulary of the eval grader
// (evals/textmatch.go healthyTerms) so a phrase the grader counts cannot slip
// past the guard; the agent package cannot import evals, so the list lives
// here and is kept in sync by review.
var healthyClaimRE = regexp.MustCompile(`\b(?:healthy|recovered|resolved|back to normal|all clear|all good|fine|` +
	`nothing to fix|nothing needs fixing|nothing needs to be fixed|nothing to do|` +
	`no fix needed|no fix is needed|no fix required|no fix is required|` +
	`no action needed|no action is needed|no action required|no action is required|` +
	`no issues|no issue|no problems|no problem|nothing is wrong|` +
	`operating normally|running normally|caught up)\b`)

// claimCaveats mark a clause as conditional or partial, so it is not a healthy
// verdict ("healthy except the registry", "healthy if the read succeeds").
var claimCaveats = []string{" except ", " apart from ", " other than ", " until ", " would ", " if "}

// claimNegators before a healthy phrase cancel it ("not healthy", "no longer
// healthy", "I cannot say it is healthy"). They are looked for in the sub the
// phrase sits in, as the eval grader does, so "no data loss, the pipeline is
// healthy" is still a healthy claim in its second sub.
var claimNegators = []string{"not ", "no ", "never ", "without ", "nobody ", "cannot ", "can't ",
	"couldn't ", "don't ", "unable to ", "isn't ", "aren't ", "n't "}

// healthyClaim reports whether text claims the pipeline is healthy or has
// nothing to fix. It is deliberately conservative: a clause with a caveat or
// a negated phrase does not claim a healthy verdict, and a question states
// nothing. It is used only while an unreachable gap is open, so a false
// positive costs one refused call with a message that says what to write.
func healthyClaim(text string) bool {
	for _, clause := range claimClauses(strings.ToLower(text)) {
		if clause.question {
			continue
		}
		c := " " + clause.text + " "
		caveat := false
		for _, w := range claimCaveats {
			if strings.Contains(c, w) {
				caveat = true
				break
			}
		}
		if caveat {
			continue
		}
		for _, sub := range claimSubs(clause.text) {
			for _, loc := range healthyClaimRE.FindAllStringIndex(sub, -1) {
				before := sub[:loc[0]]
				negated := false
				for _, n := range claimNegators {
					if strings.Contains(before, n) {
						negated = true
						break
					}
				}
				if negated {
					continue
				}
				if strings.HasPrefix(strings.TrimSpace(sub[loc[1]:]), "yet") {
					continue
				}
				return true
			}
		}
	}
	return false
}

// claimSubs cuts a clause at the boundaries the eval grader treats as separate
// assertions (a comma or "and"), so a negation in another sub does not cancel
// this sub's healthy phrase.
func claimSubs(text string) []string {
	return claimSubSplit.Split(text, -1)
}

// claimSubSplit is the sub boundary of claimSubs.
var claimSubSplit = regexp.MustCompile(`,|\sand\s`)

// claimClause is one clause of a verdict text.
type claimClause struct {
	text     string
	question bool
}

// claimClauses cuts text at sentence and clause boundaries. A clause that ends
// in "?" is a question and states nothing.
func claimClauses(text string) []claimClause {
	var out []claimClause
	start := 0
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case ';', '!', '\n':
			if c := strings.TrimSpace(text[start:i]); c != "" {
				out = append(out, claimClause{text: c})
			}
			start = i + 1
		case '?':
			if c := strings.TrimSpace(text[start:i]); c != "" {
				out = append(out, claimClause{text: c, question: true})
			}
			start = i + 1
		case '.':
			// A sentence end, not a decimal point or an abbreviation inside a
			// number ("1.5").
			if i+1 == len(text) || text[i+1] == ' ' || text[i+1] == '\n' {
				if c := strings.TrimSpace(text[start:i]); c != "" {
					out = append(out, claimClause{text: c})
				}
				start = i + 1
			}
		}
	}
	if c := strings.TrimSpace(text[start:]); c != "" {
		out = append(out, claimClause{text: c})
	}
	return out
}

// checkHealthyVerdict refuses a text that claims a healthy verdict while a
// read failure leaves a component unreachable. It returns nil when no gap is
// open, when the text makes no healthy claim, or when the gaps belong to
// another tool. A nil gaps turns the check off (harnesses without a runner).
func checkHealthyVerdict(gaps *readFailures, text string) error {
	if gaps == nil || !healthyClaim(text) {
		return nil
	}
	if f, blocked := healthyVerdictBlocked(gaps); blocked {
		return blockedHealthyError(f)
	}
	return nil
}
