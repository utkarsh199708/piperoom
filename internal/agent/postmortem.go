package agent

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/events"
)

// WritePostmortem turns a closed room's log into a closing summary with one
// structured model request and checks it against the log. It is the CLI's
// writer; the server uses Endpoint.WritePostmortem, which also counts tokens.
func WritePostmortem(ctx context.Context, roomEvents []events.Event, opts Options) (PostmortemDraft, error) {
	opts = opts.withDefaults()
	return writePostmortem(ctx, roomEvents, opts, newModel(opts, nil))
}

// writePostmortem is the testable seam: the model is injected.
//
// The model reads how the room ended first (the close resolution and the fix
// notes people logged), then the log split at the newest hand-off: a hand-off
// is a snapshot of an earlier moment, so what it lists as open or risky is not
// current (QA G10; see renderPostmortemInput).
//
// Grounding is the product rule: a name, number or reference the log does not
// contain comes back in PostmortemDraft.Violations, so the reader sees it
// marked instead of shipped silently. A summary that is empty in a required
// field, or over the length caps, is an error and the draft is dropped.
func writePostmortem(ctx context.Context, roomEvents []events.Event, opts Options, m model) (PostmortemDraft, error) {
	if len(roomEvents) == 0 {
		return PostmortemDraft{}, fmt.Errorf("agent: cannot write postmortem from an empty room")
	}
	s, err := summarize(ctx, m, opts, postmortemSystemPrompt(), renderPostmortemInput(roomEvents), "postmortem")
	if err != nil {
		return PostmortemDraft{}, err
	}
	var violations []string
	for _, v := range CheckHandoff(roomEvents, s) {
		switch v.Rule {
		case RuleEmptyField, RuleListTooLong, RuleStringTooLong:
			return PostmortemDraft{}, fmt.Errorf("agent: postmortem: summary fails %s at %s", v.Rule, v.Field)
		case RuleUnknownName, RuleUnknownNumber, RuleUnknownRef:
			// Rule names only, once each (PostmortemDraft, the
			// postmortem.drafted payload): the detail quotes the log.
			if !slices.Contains(violations, v.Rule) {
				violations = append(violations, v.Rule)
			}
		}
	}
	return PostmortemDraft{Summary: s, Violations: violations}, nil
}

// postmortemOutcomeRules extends postmortemPrompt with how to read the two
// parts of the input that decide how the incident ended. Without them the
// model carried a stale hand-off's "still open" and "risks" into rooms that a
// person had fixed since, and wrote "no fix was made" for a fix made outside
// this tool (QA G10).
const postmortemOutcomeRules = `The input starts with the resolution and the fix notes. They say how the incident really ended and outrank everything written before the newest hand-off.
- A fix note is a person's own account of a fix made outside this tool, for example "restarted the connector by hand". Put each fix note's action in done, in the person's words, and say it is the person's account. Never write that no fix was made, proposed or verified when a fix note or the resolution says one was: say that a person made the fix, and that this tool did not verify it unless an event shows a verification.
- The earlier hand-off and the events before it are history: they were true when they were written. Do not copy the hand-off's open items or risks forward. Keep one only when an event after the hand-off still shows it unresolved. When the resolution or a fix note says the problem was fixed, the broken state is not open: open lists only the follow-ups that remain (prevention, monitoring, cleanup) and risks lists only what is still off after the fix.
- Leave out process noise: that a hand-off was written, failed or was replaced, that lines were truncated, that the agent paused. It is not part of the incident.`

// postmortemSystemPrompt is the post-mortem instruction: the closing-summary
// prompt plus postmortemOutcomeRules. The base prompt stays in prompt.go.
func postmortemSystemPrompt() string {
	return postmortemPrompt() + "\n\n" + postmortemOutcomeRules
}

// pmFixNotesMax caps the fix notes shown; the newest are kept.
const pmFixNotesMax = 20

// pmHandoffListMax caps the items of one hand-off list shown, like the caps a
// stored hand-off already has.
const pmHandoffListMax = HandoffListMax

// roomEventsHeader is the first line renderRoomEvents writes. The split
// render below puts its own headings in its place; a header that is not there
// (prompt.go changed) is simply left in.
const roomEventsHeader = "Room events (oldest first):\n"

// renderPostmortemInput builds the post-mortem model's input from the closed
// room's log, in this order:
//
//  1. the resolution the room was closed with (the last room.closed);
//  2. the fix notes (comment.added of kind fix_note): what people did outside
//     Piperoom, with who and when;
//  3. when the log holds a hand-off, the newest one, labelled as an earlier
//     snapshot; then the history up to it, labelled out of date; then the events
//     after it as the current state. A log with no hand-off is rendered as one
//     list, as before.
//
// Every string from the log is wrapped as untrusted data and capped.
func renderPostmortemInput(evs []events.Event) string {
	var b strings.Builder
	b.WriteString(resolutionBlock(evs))
	b.WriteString("\n")
	b.WriteString(fixNotesBlock(evs))

	cut := newestHandoff(evs)
	if cut < 0 {
		b.WriteString("\n")
		b.WriteString(renderRoomEvents(evs))
		return b.String()
	}
	b.WriteString("\n")
	b.WriteString(handoffBlock(evs[cut]))
	b.WriteString("\nRoom history up to and including that hand-off (earlier state, may be out of date; oldest first):\n")
	b.WriteString(strings.TrimPrefix(renderRoomEvents(evs[:cut+1]), roomEventsHeader))
	b.WriteString("\nCurrent state: the room events after that hand-off (oldest first):\n")
	after := evs[cut+1:]
	if len(after) == 0 {
		b.WriteString("- (no events after the hand-off)\n")
	} else {
		b.WriteString(strings.TrimPrefix(renderRoomEvents(after), roomEventsHeader))
	}
	return b.String()
}

// resolutionBlock renders how the room was closed.
func resolutionBlock(evs []events.Event) string {
	for i := len(evs) - 1; i >= 0; i-- {
		e := evs[i]
		if e.Type != events.RoomClosed {
			continue
		}
		p, err := events.Decode[events.RoomClosedPayload](e)
		if err != nil || strings.TrimSpace(p.Resolution) == "" {
			break
		}
		return "Resolution (how the room was closed, by " + wrapUntrusted(capFreeText(actorName(e.Actor))) + atText(e.At) + "): " +
			wrapUntrusted(capFreeText(p.Resolution)) + "\n"
	}
	return "Resolution: none was recorded.\n"
}

// fixNotesBlock renders the fix notes people logged, oldest first.
func fixNotesBlock(evs []events.Event) string {
	var notes []events.Event
	for _, e := range evs {
		if e.Type != events.CommentAdded {
			continue
		}
		if p, err := events.Decode[events.CommentAddedPayload](e); err == nil && p.Kind == events.CommentKindFixNote {
			notes = append(notes, e)
		}
	}
	if len(notes) == 0 {
		return "Fix notes (what people did outside this tool): none were logged.\n"
	}
	var b strings.Builder
	b.WriteString("Fix notes (what people did outside this tool, in their own words; oldest first):\n")
	if len(notes) > pmFixNotesMax {
		fmt.Fprintf(&b, "- (%d earlier fix notes omitted)\n", len(notes)-pmFixNotesMax)
		notes = notes[len(notes)-pmFixNotesMax:]
	}
	for _, e := range notes {
		p, _ := events.Decode[events.CommentAddedPayload](e)
		b.WriteString("- " + wrapUntrusted(capFreeText(actorName(e.Actor))) + atText(e.At) + ": " + wrapUntrusted(capFreeText(p.Text)) + "\n")
	}
	return b.String()
}

// newestHandoff returns the index of the newest handoff.created, or -1.
func newestHandoff(evs []events.Event) int {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Type == events.HandoffCreated {
			return i
		}
	}
	return -1
}

// handoffBlock renders a hand-off's summary as an earlier snapshot. It is
// still useful (what broke, why, what was ruled out), but its open items and
// risks are only as current as its time.
func handoffBlock(e events.Event) string {
	p, err := events.Decode[events.HandoffCreatedPayload](e)
	if err != nil {
		return "Earlier hand-off: present but unreadable.\n"
	}
	who := "the agent"
	if p.Manual {
		who = "a person"
	}
	var b strings.Builder
	b.WriteString("Earlier hand-off (written by " + who + atText(e.At) + "). It was true when it was written; items it lists as open or risky may have been settled since:\n")
	line := func(label, v string) {
		if strings.TrimSpace(v) != "" {
			b.WriteString("- " + label + ": " + wrapUntrusted(capFreeText(v)) + "\n")
		}
	}
	list := func(label string, items []string) {
		for i, it := range items {
			if i == pmHandoffListMax {
				break
			}
			line(label, it)
		}
	}
	line("what broke", p.Summary.WhatBroke)
	line("why", p.Summary.Why)
	list("done", p.Summary.Done)
	list("ruled out", p.Summary.RuledOut)
	list("open then", p.Summary.Open)
	list("risk then", p.Summary.Risks)
	return b.String()
}

// atText renders " at <UTC time>" for a known time, "" for the zero time.
func atText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return " at " + t.UTC().Format("2006-01-02 15:04 UTC")
}
