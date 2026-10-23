package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/agent/llm"
	"piperoom/internal/injectscan"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
	"piperoom/internal/tools/similarincidents"
)

// Worked examples (tasks/L1-learn-from-rooms.md, L1b): when a room starts, the
// agent sees up to three past incidents of the same pipeline whose cause a
// person typed. Past-room text is untrusted data, so only the fields a person
// typed or confirmed reach the prompt (title, close date, labelled layer,
// cause, labelled fix), never the hand-off or tool-result text, and every one
// of them goes through wrapUntrusted. The lookup runs the tool directly, not
// as a step: an example is history, not evidence about this room, so nothing
// in it may become a citable step or a backed term (F3 grounding). A hit whose
// rendered text injectscan flags, alone or next to the hits rendered before it,
// is left out (renderPastIncidents).

const (
	// pastIncidentsMax is how many examples the block shows.
	pastIncidentsMax = 3
	// pastIncidentsAsk is how many hits are asked for (the tool's maximum),
	// so hits without a typed cause can be skipped and still leave three.
	pastIncidentsAsk = 5
	// pastIncidentsRunBytes and pastIncidentsTriageBytes cap the whole block
	// in the run's and in the triage pass's first message.
	pastIncidentsRunBytes    = 1200
	pastIncidentsTriageBytes = 400
	// pastIncidentsTimeout bounds the lookup of the run; the triage pass uses
	// the shorter one, since its first look must stay fast.
	pastIncidentsTimeout       = 10 * time.Second
	pastIncidentsTriageTimeout = 8 * time.Second
	// pastIncidentsQueryBytes keeps the query under the tool's input limit.
	pastIncidentsQueryBytes = 1000
	// pastIncidentsMinField is the shortest clip of a free-text field worth
	// showing; a line that cannot give its cause this much is left out.
	pastIncidentsMinField = 24
)

// pastIncidentsHeader opens the block. The system prompts say what it means.
const pastIncidentsHeader = "Past incidents on this pipeline (human-confirmed; history, not evidence about this room):\n"

// pastHit is the part of a similar_incidents hit the block reads. Cause and
// LabelFix are the last room.labelled's cause and fix_that_worked, both typed
// by a person. The tool's fix_that_worked is not read: it falls back to the
// executed proposal's title, which the agent wrote.
type pastHit struct {
	Title    string `json:"title"`
	ClosedAt string `json:"closed_at"`
	Layer    string `json:"layer"`
	Cause    string `json:"cause"`
	LabelFix string `json:"label_fix"`
}

// pastIncidents looks up the alert in similar_incidents and renders the block,
// at most maxBytes long. It returns "" when the tool is not registered (no
// room store, as in cmd/investigate), the alert names no pipeline, the lookup fails or
// no hit has a typed cause. A failure is logged at debug only: a room with no
// history is the normal case.
func (r *runner) pastIncidents(ctx context.Context, maxBytes int, timeout time.Duration) string {
	if r.deps.Tools == nil || strings.TrimSpace(r.alert.PipelineID) == "" {
		return ""
	}
	tool, ok := r.deps.Tools.Get(similarincidents.ToolName)
	if !ok {
		return ""
	}
	query := oneLineText(r.alert.Title)
	if query == "" {
		return ""
	}
	input, err := json.Marshal(map[string]any{
		"query":       clipText(query, pastIncidentsQueryBytes),
		"pipeline_id": r.alert.PipelineID,
		"limit":       pastIncidentsAsk,
	})
	if err != nil {
		return ""
	}
	tctx, cancel := context.WithTimeout(context.WithValue(r.toolScope(ctx), pastLookupKey{}, true), timeout)
	defer cancel()
	res, err := runToolSafely(tctx, tool, input)
	if err != nil {
		msg, _ := redact.Text(err.Error())
		r.log.Debug("agent: past incidents lookup failed", "room", r.alert.RoomID, "err", msg)
		return ""
	}
	hits, err := decodePastHits(res.Data)
	if err != nil {
		r.log.Debug("agent: past incidents result unreadable", "room", r.alert.RoomID, "err", err)
		return ""
	}
	block, drops := renderPastIncidents(hits, toolEntries(res.Summary, len(hits)), maxBytes)
	for _, d := range drops {
		r.log.Warn("agent: past incident left out of the opening block: instruction-like text",
			"tool", similarincidents.ToolName, "room", r.alert.RoomID, "hit", d.hit, "with_earlier_hits", d.cross,
			"tool_text", d.toolText, "kinds", d.kinds, "count", d.count)
	}
	return block
}

// pastLookupKey marks the context of the room-start lookup.
type pastLookupKey struct{}

// IsPastIncidentsLookup reports whether ctx is the agent's own
// similar_incidents lookup at room start rather than a call the model made.
// A replay harness can use it to keep that lookup out of its per-scenario
// miss and call counts.
func IsPastIncidentsLookup(ctx context.Context) bool {
	v, _ := ctx.Value(pastLookupKey{}).(bool)
	return v
}

// openingBlocks is the start of a first user turn: the alert message, then
// the worked-examples block when there is one, as its own text block.
func openingBlocks(alert Alert, past string) []llm.Part {
	blocks := []llm.Part{textPart(firstUserMessage(alert))}
	if past != "" {
		blocks = append(blocks, textPart(past))
	}
	return blocks
}

// runToolSafely runs tool and turns a panic into an error: the lookup is an
// optional extra and must not take the room down.
func runToolSafely(ctx context.Context, tool tools.Tool, input json.RawMessage) (res tools.Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("agent: similar_incidents panicked: %v", scrubPanic(p))
		}
	}()
	return tool.Run(ctx, input)
}

// decodePastHits reads the hits from a result's Data, whether it is the
// tool's own struct or JSON (a recorded result in evals).
func decodePastHits(data any) ([]pastHit, error) {
	var b []byte
	switch d := data.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		b = d
	case []byte:
		b = d
	default:
		var err error
		if b, err = json.Marshal(d); err != nil {
			return nil, fmt.Errorf("agent: encode similar_incidents data: %w", err)
		}
	}
	var v struct {
		Hits []pastHit `json:"hits"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("agent: decode similar_incidents data: %w", err)
	}
	return v.Hits, nil
}

// pastScanSep joins fields and lines in the scan text. It is not only
// whitespace, so the scanner reads each field as its own sentence, as the model
// does with "</untrusted> · fix: <untrusted>" between them: a field ending in a
// determiner ("cleared by the") would otherwise make the next field's leading
// verb read as a noun and hide a request. Each field still starts a line, for
// the line-start role and addressee rules.
const pastScanSep = "\n·\n"

// pastDrop is a hit left out because injectscan flagged its text: hit is its
// index in the tool's hits, cross that it was flagged only next to the hits
// already in the block, toolText that the flag came from the hit's entry in
// the tool's Summary rather than from the text the block shows. It carries
// finding kinds, never the text.
type pastDrop struct {
	hit      int
	cross    bool
	toolText bool
	kinds    []string
	count    int
}

// toolEntryStart opens a hit's entry in the similar_incidents Summary:
// "\n[1] 2026-09-30 ...".
var toolEntryStart = regexp.MustCompile(`\n\[[0-9]+\] `)

// toolEntries cuts the tool's Summary into one entry per hit, in hit order, so
// the block can scan each hit exactly as the tool path shows it. A Summary cut
// by tools.Truncate yields fewer entries (the last one maybe partial); hits
// past the end have none. Entries must be numbered [1]..[k] with k <= n: if a
// field the tool prints unfolded (closed by, layer) forges an entry start, the
// split cannot be trusted, and every hit gets the whole Summary instead, so
// the block stays at least as strict as the tool path.
func toolEntries(summary string, n int) []string {
	starts := toolEntryStart.FindAllStringIndex(summary, -1)
	entries := make([]string, len(starts))
	ok := len(starts) <= n
	for i, st := range starts {
		end := len(summary)
		if i+1 < len(starts) {
			end = starts[i+1][0]
		}
		entries[i] = summary[st[0]+1 : end]
		ok = ok && strings.HasPrefix(entries[i], "["+strconv.Itoa(i+1)+"] ")
	}
	if !ok {
		whole := make([]string, n)
		for i := range whole {
			whole[i] = summary
		}
		return whole
	}
	return entries
}

// renderPastIncidents renders up to three hits that have a typed cause as one
// block of at most maxBytes, or "" when none fits, leaving out each hit whose
// text injectscan flags. toolText holds each hit's entry in the tool's Summary
// (toolEntries), nil when there is none. drops says which hits were left out
// and why.
//
// Each line is scanned as a similar_incidents result is (wrapReadToolOutput),
// but on the field text the line renders (after cleaning and clipping), joined
// in render order: alone, then after the field text of the lines already kept,
// so an addressee in one field or hit and a request in another are seen
// together. The wrapper tags are not scanned: they are the agent's and would
// trip wrapper_tag. A flagged line is left out rather than marked: the block is
// a worked example the model is told to copy.
//
// The same two scans also run on the hit's entries in the tool's Summary, so
// the block is at least as strict as the similar_incidents result, which
// orders fields differently (its "fixed by:" follows the title, before the
// cause) and also shows fields the block leaves out. A line is left out when
// either text is flagged.
func renderPastIncidents(hits []pastHit, toolText []string, maxBytes int) (block string, drops []pastDrop) {
	return layoutPastIncidents(hits, toolText, maxBytes, injectscan.Scan)
}

// layoutPastIncidents lays out the block. Every field is masked, folded to one
// line and wrapped as untrusted text; a free-text field is clipped so its line
// fits its share of the cap. scan screens each line as renderPastIncidents
// says; nil keeps every line (layout tests only).
func layoutPastIncidents(hits []pastHit, toolText []string, maxBytes int, scan func(string) []injectscan.Finding) (string, []pastDrop) {
	type candidate struct {
		h   pastHit
		idx int
	}
	var use []candidate
	for i, h := range hits {
		h = pastHit{
			Title:    cleanPastField(h.Title),
			ClosedAt: cleanPastField(h.ClosedAt),
			Layer:    cleanPastField(h.Layer),
			Cause:    cleanPastField(h.Cause),
			LabelFix: cleanPastField(h.LabelFix),
		}
		if h.Cause == "" {
			continue
		}
		use = append(use, candidate{h, i})
	}
	room := maxBytes - len(pastIncidentsHeader)
	if len(use) == 0 || room <= 0 {
		return "", nil
	}
	var (
		lines    []string
		kept     []string // field text of the kept lines, for the joint scan
		keptTool []string // their tool Summary entries, likewise
		drops    []pastDrop
	)
	used := 0
	for i, c := range use {
		if len(lines) == pastIncidentsMax {
			break
		}
		// Each line gets an even share of what is left, so an early long
		// line cannot crowd out the rest.
		share := (room - used) / min(len(use)-i, pastIncidentsMax-len(lines))
		line, text, ok := pastIncidentLine(c.h, share)
		if !ok {
			continue
		}
		tool := ""
		if c.idx < len(toolText) {
			tool = toolText[c.idx]
		}
		if scan != nil {
			if d, bad := screenPastLine(scan, c.idx, kept, text, false); bad {
				drops = append(drops, d)
				continue
			}
			if d, bad := screenPastLine(scan, c.idx, keptTool, tool, true); bad {
				drops = append(drops, d)
				continue
			}
		}
		lines = append(lines, line)
		kept = append(kept, text)
		if tool != "" {
			keptTool = append(keptTool, tool)
		}
		used += len(line)
	}
	if len(lines) == 0 {
		return "", drops
	}
	return pastIncidentsHeader + strings.Join(lines, ""), drops
}

// screenPastLine scans text alone, then after the kept texts. text "" (no
// tool entry) passes. toolText marks the drop as coming from the tool's text.
func screenPastLine(scan func(string) []injectscan.Finding, hit int, kept []string, text string, toolText bool) (pastDrop, bool) {
	if text == "" {
		return pastDrop{}, false
	}
	if fs := scan(text); len(fs) > 0 {
		return pastDrop{hit: hit, toolText: toolText, kinds: injectscan.Kinds(fs), count: len(fs)}, true
	}
	if len(kept) > 0 {
		if fs := scan(strings.Join(append(kept[:len(kept):len(kept)], text), pastScanSep)); len(fs) > 0 {
			return pastDrop{hit: hit, cross: true, toolText: toolText, kinds: injectscan.Kinds(fs), count: len(fs)}, true
		}
	}
	return pastDrop{}, false
}

// pastIncidentLine renders one hit in at most n bytes (newline included). The
// close date and layer are short and kept whole; title, cause and fix share
// what is left, the cause first. text is the field text the line shows, in
// render order joined by pastScanSep, unwrapped, for the scan. ok is false when
// the cause cannot get pastIncidentsMinField bytes.
func pastIncidentLine(h pastHit, n int) (line, text string, ok bool) {
	date := clipText(h.ClosedAt, 10)
	layer := clipText(h.Layer, 24)
	if layer == "" {
		layer = "unlabelled"
	}
	fix := h.LabelFix
	fixText := func(s string) string {
		if fix == "" {
			return "none recorded"
		}
		return wrapUntrusted(s)
	}
	build := func(title, cause, fixClip string) string {
		return "- " + wrapUntrusted(date) + " · " + wrapUntrusted(title) + " · layer " + wrapUntrusted(layer) +
			" · cause: " + wrapUntrusted(cause) + " · fix: " + fixText(fixClip) + "\n"
	}
	fixed := len(build("", "", ""))
	avail := n - fixed
	if avail < pastIncidentsMinField {
		return "", "", false
	}
	// The cause gets half, the fix a third, the title the rest; what one
	// field does not use goes to the others.
	causeN := min(escapedLen(h.Cause), avail/2)
	fixN := 0
	if fix != "" {
		fixN = min(escapedLen(fix), (avail-causeN)*2/3)
	}
	titleN := min(escapedLen(h.Title), avail-causeN-fixN)
	left := avail - causeN - fixN - titleN
	extra := min(left, escapedLen(h.Cause)-causeN)
	causeN, left = causeN+extra, left-extra
	if fix != "" {
		extra = min(left, escapedLen(fix)-fixN)
		fixN = fixN + extra
	}
	if causeN < min(pastIncidentsMinField, escapedLen(h.Cause)) {
		return "", "", false
	}
	title, cause, fixClip := fitEscaped(h.Title, titleN), fitEscaped(h.Cause, causeN), fitEscaped(fix, fixN)
	line = build(title, cause, fixClip)
	if len(line) > n {
		return "", "", false
	}
	return line, strings.Join([]string{date, title, layer, cause, fixClip}, pastScanSep), true
}

// cleanPastField masks secrets and folds the text to one line, so a field
// cannot fake extra lines of the block.
func cleanPastField(s string) string {
	s, _ = redact.Text(s)
	return oneLineText(s)
}

// oneLineText folds every whitespace run, newlines included, to one space.
func oneLineText(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// escapedLen is the length of s once escaped for the wrapper.
func escapedLen(s string) int { return len(escapeToolText(s)) }

// fitEscaped returns the longest prefix of s, cut at a rune boundary and
// marked with "…" when cut, whose escaped form is at most n bytes. It clips
// before escaping so an entity such as &amp; is never cut in half.
func fitEscaped(s string, n int) string {
	if escapedLen(s) <= n {
		return s
	}
	const mark = "…"
	if n < len(mark) {
		return ""
	}
	// Binary search over the rune boundaries: the escaped length of a prefix
	// grows with the prefix.
	var cuts []int
	for i := range s {
		cuts = append(cuts, i)
	}
	lo, hi := 0, len(cuts)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if escapedLen(s[:cuts[mid]])+len(mark) <= n {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return strings.TrimSpace(s[:cuts[lo]]) + mark
}

// clipText cuts s to at most n bytes at a rune boundary.
func clipText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
