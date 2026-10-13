package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"piperoom/internal/events"
)

// Hand-off length caps. They mirror what internal/room applies before a
// summary is stored (handoffListMax and truncateDetail's 500 bytes). The
// duplication is deliberate: internal/room imports this package, so it cannot
// be shared from here. Keep in sync (room owner: A5).
const (
	// HandoffListMax is the most items a hand-off list may carry.
	HandoffListMax = 10
	// HandoffStringMax is the most bytes one hand-off string may carry.
	HandoffStringMax = 500
)

// Hand-off check rule names. They are stable so reports can be grouped.
const (
	RuleEmptyField     = "empty_field"
	RuleListTooLong    = "list_too_long"
	RuleStringTooLong  = "string_too_long"
	RuleUnknownRef     = "unknown_reference"
	RuleUnknownName    = "unknown_identifier"
	RuleUnknownNumber  = "unknown_number"
	RuleOpenProposal   = "open_proposal_missing"
	RulePendingLayer   = "pending_layer_missing"
	RuleFailedAction   = "failed_action_missing"
	RuleInterruptedRun = "interrupted_run_missing"
)

// HandoffViolation is one way a summary fails the checks.
type HandoffViolation struct {
	Rule   string // one of the Rule* constants
	Field  string // summary field, e.g. "open[2]"
	Detail string
}

func (v HandoffViolation) String() string {
	return fmt.Sprintf("%s %s: %s", v.Rule, v.Field, v.Detail)
}

// CheckHandoff checks a hand-off summary against the room log it was written
// from and returns every violation (nil when clean). It is a pure function.
//
// events.HandoffSummary has no citation field, so grounding works on the text:
//
//   - a cited seq (as "seq 12"), step id (s_...), proposal id (p_...), comment
//     id (c_...) or suggestion id (g_...) must exist in the log;
//   - an identifier-like name and a number of two or more digits must appear
//     in the log's text, case-insensitively. Names are backticked text, tokens
//     joined by . _ or :, slash paths with a digit, _ or ., and tokens with
//     two or more hyphens or a digit (except common English compounds).
//     Dates, times, percentages, durations, ports, #refs and URLs are prose
//     and are not checked;
//   - every open proposal must be named (id or title) in an open item, and
//     the layers still to approve must be named in those items. A proposal is
//     open until it is rejected, a step fails, or its last step runs (the
//     rule internal/room/snapshot.go applies);
//   - a proposal with a failed step must be named in open or risks;
//   - a run that did not finish must be said so in open or risks. In progress
//     counts as unfinished: the log ends inside a step or tool call, after a
//     tool result with the agent still "working", or on an "error" status;
//   - the length caps hold.
//
// Names and numbers are matched against the whole log, not only the part the
// model saw, so a derived number (a sum) can be a false positive.
func CheckHandoff(evs []events.Event, s events.HandoffSummary) []HandoffViolation {
	var out []HandoffViolation
	out = append(out, checkHandoffShape(s)...)
	idx := indexLog(evs)
	out = append(out, checkGrounding(idx, s)...)
	out = append(out, checkOpenProposals(idx, s)...)
	out = append(out, checkFailedActions(idx, s)...)
	out = append(out, checkInterrupted(idx, s)...)
	return out
}

func checkHandoffShape(s events.HandoffSummary) []HandoffViolation {
	var out []HandoffViolation
	str := func(field, v string, required bool) {
		if required && strings.TrimSpace(v) == "" {
			out = append(out, HandoffViolation{RuleEmptyField, field, "required text is empty"})
		}
		if len(v) > HandoffStringMax {
			out = append(out, HandoffViolation{RuleStringTooLong, field, fmt.Sprintf("%d bytes, cap %d", len(v), HandoffStringMax)})
		}
	}
	list := func(name string, items []string) {
		if len(items) > HandoffListMax {
			out = append(out, HandoffViolation{RuleListTooLong, name, fmt.Sprintf("%d items, cap %d", len(items), HandoffListMax)})
		}
		for i, it := range items {
			str(fmt.Sprintf("%s[%d]", name, i), it, true)
		}
	}
	str("what_broke", s.WhatBroke, true)
	str("why", s.Why, true)
	list("done", s.Done)
	list("ruled_out", s.RuledOut)
	list("open", s.Open)
	list("risks", s.Risks)
	return out
}

// proposalState is what the log says about one proposal.
type proposalState struct {
	id, title string
	required  []string
	steps     int
	approved  map[string]bool
	rejected  bool
	failed    bool
	executed  map[int]bool
}

// open follows internal/room/snapshot.go: a rejection, a failed step or the
// last step running decides a proposal.
func (p *proposalState) open() bool {
	return !p.rejected && !p.failed && !p.executed[p.steps-1]
}

func (p *proposalState) pendingLayers() []string {
	var out []string
	for _, l := range p.required {
		if !p.approved[l] {
			out = append(out, l)
		}
	}
	return out
}

// logIndex is the facts the checks need, gathered in one pass.
type logIndex struct {
	seqs      map[int64]bool
	ids       map[string]bool // step, proposal, comment, suggestion ids
	text      string          // every string and number in every payload, lower-cased
	textNC    string          // text with thousands commas removed, so 1,234 matches 1234
	proposals []*proposalState
	// interrupted is set when the run did not end cleanly.
	interrupted bool
}

func indexLog(evs []events.Event) logIndex {
	idx := logIndex{seqs: map[int64]bool{}, ids: map[string]bool{}}
	var text strings.Builder
	byID := map[string]*proposalState{}
	openCalls := map[string]bool{} // calls with no result yet
	lastStatus := ""
	var lastWork events.Type // last step start, tool call or tool result
	closed := false

	for _, e := range evs {
		idx.seqs[e.Seq] = true
		collectStrings(e.Payload, &text)
		text.WriteByte('\n')
		switch e.Type {
		case events.AgentStepStart:
			lastWork = e.Type
			if p, err := events.Decode[events.AgentStepStartPayload](e); err == nil {
				idx.ids[strings.ToLower(p.StepID)] = true
			}
		case events.AgentToolCall:
			lastWork = e.Type
			if p, err := events.Decode[events.AgentToolCallPayload](e); err == nil {
				idx.ids[strings.ToLower(p.StepID)] = true
				openCalls[p.ToolUseID+"|"+p.StepID+"|"+p.Tool] = true
			}
		case events.AgentToolResult:
			lastWork = e.Type
			if p, err := events.Decode[events.AgentToolResultPayload](e); err == nil {
				delete(openCalls, p.ToolUseID+"|"+p.StepID+"|"+p.Tool)
			}
		case events.CommentAdded:
			if p, err := events.Decode[events.CommentAddedPayload](e); err == nil {
				idx.ids[strings.ToLower(p.CommentID)] = true
			}
		case events.SteerSuggested:
			if p, err := events.Decode[events.SteerSuggestedPayload](e); err == nil {
				idx.ids[strings.ToLower(p.SuggestionID)] = true
			}
		case events.AgentStatus:
			if p, err := events.Decode[events.AgentStatusPayload](e); err == nil {
				lastStatus = p.State
				switch p.State {
				case "done", "idle", "waiting_approval":
					// The run ended cleanly after whatever it left dangling (a
					// restarted run), so earlier calls are not interrupted work.
					openCalls = map[string]bool{}
					lastWork = ""
				}
			}
		case events.RoomClosed:
			closed = true
		case events.ProposalCreated:
			p, err := events.Decode[events.ProposalCreatedPayload](e)
			if err != nil || byID[p.ProposalID] != nil {
				continue // a duplicate proposal.created does not reset state
			}
			idx.ids[strings.ToLower(p.ProposalID)] = true
			ps := &proposalState{
				id: p.ProposalID, title: p.Title, required: p.RequiredLayers,
				steps: len(p.Steps), approved: map[string]bool{}, executed: map[int]bool{},
			}
			byID[p.ProposalID] = ps
			idx.proposals = append(idx.proposals, ps)
		case events.ApprovalGiven:
			if p, err := events.Decode[events.ApprovalGivenPayload](e); err == nil && byID[p.ProposalID] != nil {
				byID[p.ProposalID].approved[p.Layer] = true
			}
		case events.ApprovalRejected:
			if p, err := events.Decode[events.ApprovalRejectedPayload](e); err == nil && byID[p.ProposalID] != nil {
				byID[p.ProposalID].rejected = true
			}
		case events.ActionExecuted:
			if p, err := events.Decode[events.ActionExecutedPayload](e); err == nil && byID[p.ProposalID] != nil {
				ps := byID[p.ProposalID]
				ps.executed[p.StepIndex] = true
				if !p.OK {
					ps.failed = true
				}
			}
		}
	}
	idx.text = strings.ToLower(text.String())
	idx.textNC = thousandsGroup.ReplaceAllStringFunc(idx.text, func(m string) string { return strings.ReplaceAll(m, ",", "") })
	// A closed room has nothing left to finish.
	if !closed {
		idx.interrupted = lastStatus == "error" ||
			len(openCalls) > 0 ||
			lastWork == events.AgentStepStart ||
			(lastStatus == "working" && lastWork == events.AgentToolResult)
	}
	return idx
}

// hasNumber reports whether n appears in the log as a whole number, with or
// without thousands separators.
func (idx logIndex) hasNumber(n string) bool {
	return containsNumber(idx.text, n) || containsNumber(idx.textNC, n)
}

// collectStrings appends every string and number in a JSON payload, so text
// is compared after JSON unescaping.
func collectStrings(raw json.RawMessage, b *strings.Builder) {
	if len(raw) == 0 {
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		b.Write(raw)
		return
	}
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			b.WriteString(x)
			b.WriteByte('\n')
		case json.Number:
			b.WriteString(x.String())
			b.WriteByte('\n')
		case []any:
			for _, y := range x {
				walk(y)
			}
		case map[string]any:
			for k, y := range x {
				b.WriteString(k)
				b.WriteByte('\n')
				walk(y)
			}
		}
	}
	walk(v)
}

var (
	seqRef   = regexp.MustCompile(`(?i)\bseq(?:uence)?[ =#:]*(\d+)`)
	idRef    = regexp.MustCompile(`\b[spcg]_[A-Za-z0-9]+\b`)
	backtick = regexp.MustCompile("`([^`\n]+)`")
	urlRef   = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://\S+`)
	// A name joined by . _ or : (pg.public.orders, debezium_orders); every
	// segment has 2+ characters, so "e.g" and "i.e" are not names.
	dotName = regexp.MustCompile(`[A-Za-z][A-Za-z0-9]+(?:[._:][A-Za-z0-9]{2,})+`)
	// A path with slashes (public/orders); checked only with a digit, _ or .
	slashName = regexp.MustCompile(`[A-Za-z0-9][A-Za-z0-9._]*(?:/[A-Za-z0-9][A-Za-z0-9._]*)+`)
	// A hyphenated token whose segments all have 2+ characters.
	// A trailing or inner number segment may be one digit (order-sync-1).
	hyphName = regexp.MustCompile(`[A-Za-z][A-Za-z0-9]+(?:-(?:[A-Za-z0-9]{2,}|\d))+`)

	isoDate        = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}(?:[T ]\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?`)
	clockTime      = regexp.MustCompile(`\b\d{1,2}:\d{2}(?::\d{2})?\b`)
	thousandsGroup = regexp.MustCompile(`\d{1,3}(?:,\d{3})+`)
	numToken       = regexp.MustCompile(`#?\d{1,3}(?:,\d{3})+(?:\.\d+)?|#?\d+(?:\.\d+)?`)
	// A number that is a rate, size-less unit or count of time, not a fact the
	// log must hold: 50%, 15 min, 30 seconds, 3x.
	unitAfter  = regexp.MustCompile(`^(?:\s?%|\s?(?:ms|msec|s|sec|secs|seconds?|m|min|mins|minutes?|h|hr|hrs|hours?|d|days?|weeks?)\b|x\b)`)
	portBefore = regexp.MustCompile(`(?i)\bports?\s*[:=]?\s*$`)
)

// englishCompounds are hyphenated phrases with 2+ hyphens that are prose, not
// pipeline names.
var englishCompounds = map[string]bool{
	"up-to-date": true, "out-of-sync": true, "out-of-date": true, "at-least-once": true,
	"at-most-once": true, "end-to-end": true, "state-of-the-art": true,
	"one-by-one": true, "step-by-step": true, "line-by-line": true, "side-by-side": true,
	"point-in-time": true, "top-to-bottom": true, "first-in-first-out": true, "on-call-to-on-call": true, "day-to-day": true,
	"one-to-one": true, "one-to-many": true, "many-to-many": true, "run-time-error": true,
	"read-after-write": true, "check-in-check-out": true,
	"hand-off-to-hand-off": true, "time-to-live": true, "per-partition-per-topic": true,
	"non-breaking-change": true, "not-yet-approved": true, "to-be-decided": true,
	"no-op-on-retry": true, "re-run-after-fix": true, "case-by-case": true,
	"by-the-book": true, "on-the-fly": true, "off-by-one": true, "run-to-completion": true,
	"fire-and-forget": true, "trial-and-error": true, "back-to-back": true,
	"exactly-once-delivery": true, "at-least-once-delivery": true, "wait-and-see": true,
	"stop-the-world": true,
}

type summaryText struct{ field, text string }

func orderedSummaryTexts(s events.HandoffSummary) []summaryText {
	out := []summaryText{{"what_broke", s.WhatBroke}, {"why", s.Why}}
	add := func(name string, items []string) {
		for i, it := range items {
			out = append(out, summaryText{fmt.Sprintf("%s[%d]", name, i), it})
		}
	}
	add("done", s.Done)
	add("ruled_out", s.RuledOut)
	add("open", s.Open)
	add("risks", s.Risks)
	return out
}

func checkGrounding(idx logIndex, s events.HandoffSummary) []HandoffViolation {
	var out []HandoffViolation
	seen := map[string]bool{} // report each (rule, token) once
	add := func(rule, field, token, detail string) {
		k := rule + "|" + token
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, HandoffViolation{rule, field, detail})
	}
	for _, st := range orderedSummaryTexts(s) {
		for _, m := range seqRef.FindAllStringSubmatch(st.text, -1) {
			n, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil || !idx.seqs[n] {
				add(RuleUnknownRef, st.field, "seq"+m[1], fmt.Sprintf("cites seq %s, which is not in the log", m[1]))
			}
		}
		for _, id := range idRef.FindAllString(st.text, -1) {
			if !idx.ids[strings.ToLower(id)] {
				add(RuleUnknownRef, st.field, id, fmt.Sprintf("cites %s, which is not in the log", id))
			}
		}
		for _, name := range candidateNames(st.text) {
			if !strings.Contains(idx.text, strings.ToLower(name)) {
				add(RuleUnknownName, st.field, strings.ToLower(name), fmt.Sprintf("%q does not appear in the log", name))
			}
		}
		for _, n := range candidateNumbers(st.text) {
			if !idx.hasNumber(n) {
				add(RuleUnknownNumber, st.field, n, fmt.Sprintf("number %s does not appear in the log", n))
			}
		}
	}
	return out
}

// candidateNames returns the identifier-like names in text that must be
// grounded in the log.
func candidateNames(text string) []string {
	var out []string
	for _, m := range backtick.FindAllStringSubmatch(text, -1) {
		t := strings.TrimSpace(m[1])
		// Backticked prose or commands are not names; names have no spaces.
		if t != "" && !strings.ContainsAny(t, " \t") {
			out = append(out, t)
		}
	}
	rest := backtick.ReplaceAllString(text, " ")
	rest = urlRef.ReplaceAllString(rest, " ")
	rest = idRef.ReplaceAllString(rest, " ") // ids are checked as references
	rest = isoDate.ReplaceAllString(rest, " ")
	for _, t := range dotName.FindAllString(rest, -1) {
		out = append(out, t)
	}
	for _, t := range slashName.FindAllString(rest, -1) {
		if looksLikePath(t) {
			out = append(out, strings.TrimRight(t, "."))
		}
	}
	for _, t := range hyphName.FindAllString(rest, -1) {
		if englishCompounds[strings.ToLower(t)] {
			continue
		}
		if strings.Count(t, "-") >= 2 || strings.ContainsAny(t, "0123456789") {
			out = append(out, t)
		}
	}
	return out
}

// candidateNumbers returns numbers of two or more digits, without thousands
// commas. Single digits are too common in prose to check. Dates, times,
// percentages, durations, ports, #refs and names are not facts to ground.
func candidateNumbers(text string) []string {
	var out []string
	stripped := backtick.ReplaceAllString(text, " ")
	for _, re := range []*regexp.Regexp{urlRef, idRef, seqRef, isoDate, clockTime, dotName, slashName, hyphName} {
		stripped = re.ReplaceAllString(stripped, " ")
	}
	for _, loc := range numToken.FindAllStringIndex(stripped, -1) {
		tok := stripped[loc[0]:loc[1]]
		if strings.HasPrefix(tok, "#") {
			continue
		}
		if unitAfter.MatchString(stripped[loc[1]:]) || portBefore.MatchString(stripped[:loc[0]]) {
			continue
		}
		n := strings.ReplaceAll(tok, ",", "")
		if len(strings.ReplaceAll(n, ".", "")) >= 2 {
			out = append(out, n)
		}
	}
	return out
}

// containsNumber reports whether n appears in text as a whole number (not
// inside a longer digit run).
func containsNumber(text, n string) bool {
	for from := 0; ; {
		i := strings.Index(text[from:], n)
		if i < 0 {
			return false
		}
		i += from
		end := i + len(n)
		before := i == 0 || !isDigit(text[i-1])
		after := end == len(text) || !isDigit(text[end])
		if before && after {
			return true
		}
		from = i + 1
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func mentionsProposal(low string, p *proposalState) bool {
	return strings.Contains(low, strings.ToLower(p.id)) ||
		(strings.TrimSpace(p.title) != "" && strings.Contains(low, strings.ToLower(strings.TrimSpace(p.title))))
}

func checkOpenProposals(idx logIndex, s events.HandoffSummary) []HandoffViolation {
	var out []HandoffViolation
	for _, p := range idx.proposals {
		if !p.open() {
			continue
		}
		var mentioned []string
		for _, it := range s.Open {
			if low := strings.ToLower(it); mentionsProposal(low, p) {
				mentioned = append(mentioned, low)
			}
		}
		if len(mentioned) == 0 {
			out = append(out, HandoffViolation{RuleOpenProposal, "open", fmt.Sprintf("open proposal %s (%q) is not listed", p.id, p.title)})
			continue
		}
		joined := strings.Join(mentioned, "\n")
		for _, l := range p.pendingLayers() {
			if !mentionsLayer(joined, l) {
				out = append(out, HandoffViolation{RulePendingLayer, "open", fmt.Sprintf("proposal %s still needs approval from layer %s, not named", p.id, l)})
			}
		}
	}
	return out
}

// checkFailedActions requires a proposal whose step failed to appear in open
// or risks: the executor stopped there, so the fix is half-applied or not
// applied and the next driver must know.
func checkFailedActions(idx logIndex, s events.HandoffSummary) []HandoffViolation {
	var out []HandoffViolation
	for _, p := range idx.proposals {
		if !p.failed {
			continue
		}
		found := false
		for _, it := range append(append([]string{}, s.Open...), s.Risks...) {
			if mentionsProposal(strings.ToLower(it), p) {
				found = true
				break
			}
		}
		if !found {
			out = append(out, HandoffViolation{RuleFailedAction, "open", fmt.Sprintf("proposal %s (%q) had a failed step but is in neither open nor risks", p.id, p.title)})
		}
	}
	return out
}

// looksLikePath reports whether a slash token is a name (public/orders_v2,
// pg.public/orders, orders2/x) and not prose like Kafka/Connect, HTTP/2 or
// 24/7: it needs an underscore, a dot, or a segment mixing letters and digits.
func looksLikePath(t string) bool {
	if strings.ContainsAny(t, "_.") {
		return true
	}
	for _, seg := range strings.Split(t, "/") {
		hasLetter := strings.IndexFunc(seg, func(r rune) bool { return r >= 'A' && r <= 'z' }) >= 0
		hasDigit := strings.ContainsAny(seg, "0123456789")
		if hasLetter && hasDigit {
			return true
		}
	}
	return false
}

// mentionsLayer matches a layer name as a whole word, so "kafka" does not
// match inside "kafka-connect".
func mentionsLayer(lowText, layer string) bool {
	re := regexp.MustCompile(`(^|[^a-z0-9-])` + regexp.QuoteMeta(strings.ToLower(layer)) + `($|[^a-z0-9-])`)
	return re.MatchString(lowText)
}

// A generic "failed"/"error" word is in nearly every CDC hand-off, so the
// item must refer to the agent run itself: the run/agent/investigation next
// to an interrupt, stop, error or unfinished word, within 60 characters.
var (
	runWord  = `(?:agent run|the run|investigation|run of the agent)`
	haltWord = `(?:interrupt\w*|stopp\w*|unfinish\w*|incomplete|crash\w*|abort\w*|errored|error|cut off|gave up|did not (?:finish|complete)|not (?:finished|complete\w*)|in progress|still running)`
	runHalt  = regexp.MustCompile(`(?i)\b` + runWord + `\b.{0,60}\b` + haltWord + `\b|\b` + haltWord + `\b.{0,60}\b` + runWord + `\b|\bagent (?:was |has |had )?(?:interrupted|stopped|crashed|errored|aborted|gave up)\b`)
)

func checkInterrupted(idx logIndex, s events.HandoffSummary) []HandoffViolation {
	if !idx.interrupted {
		return nil
	}
	for _, it := range append(append([]string{}, s.Open...), s.Risks...) {
		if runHalt.MatchString(it) {
			return nil
		}
	}
	return []HandoffViolation{{RuleInterruptedRun, "open", "the agent run did not finish, but open and risks do not say so"}}
}
