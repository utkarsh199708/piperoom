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
// summary is stored (handoffListMax and truncateDetail's 500 bytes); a summary
// over them is cut there, so the model should not produce one. Keep in sync.
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
//   - an identifier-like name (backticked text, or a token joined by . _ : /
//     or by two or more hyphens, or hyphenated with a digit) and a number of
//     two or more digits must appear in the log's text, case-insensitively;
//   - every open proposal must be named (id or title) in an open item, and
//     the layers still to approve must be named in those items;
//   - a run that did not finish must be mentioned in open or risks;
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
	executed  map[int]bool
}

func (p *proposalState) open() bool {
	return !p.rejected && len(p.executed) < p.steps
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
	ids       map[string]bool // step, tool-use, proposal, comment, suggestion ids
	text      string          // every string and number in every payload, lower-cased
	proposals []*proposalState
	// interrupted is set when the run did not end cleanly.
	interrupted bool
}

func indexLog(evs []events.Event) logIndex {
	idx := logIndex{seqs: map[int64]bool{}, ids: map[string]bool{}}
	var text strings.Builder
	byID := map[string]*proposalState{}
	openCalls := map[string]bool{} // tool_use_id with no result yet
	lastStatus := ""
	var lastType events.Type
	closed := false

	for _, e := range evs {
		idx.seqs[e.Seq] = true
		collectStrings(e.Payload, &text)
		text.WriteByte('\n')
		lastType = e.Type
		switch e.Type {
		case events.AgentStepStart:
			if p, err := events.Decode[events.AgentStepStartPayload](e); err == nil {
				idx.ids[strings.ToLower(p.StepID)] = true
			}
		case events.AgentToolCall:
			if p, err := events.Decode[events.AgentToolCallPayload](e); err == nil {
				idx.ids[strings.ToLower(p.StepID)] = true
				openCalls[p.ToolUseID+"|"+p.StepID+"|"+p.Tool] = true
			}
		case events.AgentToolResult:
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
			}
		case events.RoomClosed:
			closed = true
		case events.ProposalCreated:
			p, err := events.Decode[events.ProposalCreatedPayload](e)
			if err != nil {
				continue
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
				byID[p.ProposalID].executed[p.StepIndex] = true
			}
		}
	}
	idx.text = strings.ToLower(text.String())
	// A run is unfinished when the agent last reported an error, or the log
	// ends inside a tool call or step with no result. A closed room is not.
	if !closed {
		idx.interrupted = lastStatus == "error" ||
			(len(openCalls) > 0 && (lastType == events.AgentToolCall || lastType == events.AgentStepStart))
	}
	return idx
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
	// A name joined by . _ : / (pg.public.orders, debezium_orders, wh:x).
	dotName = regexp.MustCompile(`[A-Za-z][A-Za-z0-9]*(?:[._:/][A-Za-z0-9]+)+`)
	// A hyphenated name (pg-orders-connector, order-sync).
	hyphName = regexp.MustCompile(`[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)+`)
	number   = regexp.MustCompile(`\d[\d,]*(?:\.\d+)?`)
)

type summaryText struct{ field, text string }

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
			if !containsNumber(idx.text, n) {
				add(RuleUnknownNumber, st.field, n, fmt.Sprintf("number %s does not appear in the log", n))
			}
		}
	}
	return out
}

// orderedSummaryTexts is summaryTexts in a stable order (map iteration is not).
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
	rest = idRef.ReplaceAllString(rest, " ") // ids are checked as references
	for _, t := range dotName.FindAllString(rest, -1) {
		out = append(out, strings.TrimRight(t, "._:/"))
	}
	for _, t := range hyphName.FindAllString(rest, -1) {
		if strings.Count(t, "-") >= 2 || strings.ContainsAny(t, "0123456789") {
			out = append(out, t)
		}
	}
	return out
}

// candidateNumbers returns numbers of two or more digits, normalised (no
// thousands commas). Single digits are too common in prose to check.
func candidateNumbers(text string) []string {
	var out []string
	// Names are checked separately; strip them so pg-orders-2 is not a number.
	stripped := backtick.ReplaceAllString(text, " ")
	stripped = idRef.ReplaceAllString(stripped, " ")
	stripped = seqRef.ReplaceAllString(stripped, " ")
	stripped = dotName.ReplaceAllString(stripped, " ")
	stripped = hyphName.ReplaceAllString(stripped, " ")
	for _, m := range number.FindAllString(stripped, -1) {
		n := strings.TrimRight(strings.ReplaceAll(m, ",", ""), ".")
		if len(strings.ReplaceAll(n, ".", "")) >= 2 {
			out = append(out, n)
		}
	}
	return out
}

// containsNumber reports whether n appears in text as a whole number (not
// inside a longer digit run), ignoring thousands commas in the log.
func containsNumber(text, n string) bool {
	norm := strings.ReplaceAll(text, ",", "")
	for from := 0; ; {
		i := strings.Index(norm[from:], n)
		if i < 0 {
			return false
		}
		i += from
		end := i + len(n)
		before := i == 0 || !isDigit(norm[i-1])
		after := end == len(norm) || !isDigit(norm[end])
		if before && after {
			return true
		}
		from = i + 1
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func checkOpenProposals(idx logIndex, s events.HandoffSummary) []HandoffViolation {
	var out []HandoffViolation
	for _, p := range idx.proposals {
		if !p.open() {
			continue
		}
		var mentioned []string
		for _, it := range s.Open {
			low := strings.ToLower(it)
			if strings.Contains(low, strings.ToLower(p.id)) ||
				(strings.TrimSpace(p.title) != "" && strings.Contains(low, strings.ToLower(strings.TrimSpace(p.title)))) {
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

// mentionsLayer matches a layer name as a whole word, so "kafka" does not
// match inside "kafka-connect".
func mentionsLayer(lowText, layer string) bool {
	re := regexp.MustCompile(`(^|[^a-z0-9-])` + regexp.QuoteMeta(strings.ToLower(layer)) + `($|[^a-z0-9-])`)
	return re.MatchString(lowText)
}

var interruptedWords = regexp.MustCompile(`(?i)interrupt|unfinish|incomplete|did not (?:finish|complete)|not (?:finished|complete)|stopped|crash|gave up|error|failed|cut off|aborted`)

func checkInterrupted(idx logIndex, s events.HandoffSummary) []HandoffViolation {
	if !idx.interrupted {
		return nil
	}
	for _, it := range append(append([]string{}, s.Open...), s.Risks...) {
		if interruptedWords.MatchString(it) {
			return nil
		}
	}
	return []HandoffViolation{{RuleInterruptedRun, "open", "the agent run did not finish, but open and risks do not say so"}}
}
