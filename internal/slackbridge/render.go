package slackbridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"piperoom/internal/events"
)

const (
	maxSteps       = 10
	maxLayers      = 10
	maxListItems   = 5
	fallbackBytes  = 300
	maxProposalMap = 200
)

// Important reports whether the bridge posts events of type t. Everything
// else (tool calls, results, chatter, steering, control, traces) stays in
// Piperoom; tool Data never goes to Slack.
func Important(t events.Type) bool {
	switch t {
	case events.RoomCreated, events.AgentHypothesis, events.ProposalCreated,
		events.ApprovalGiven, events.ApprovalRejected, events.ActionExecuted,
		events.HandoffCreated, events.HandoffAccepted, events.RoomClosed,
		events.ControlLeaseExpired:
		return true
	}
	return false
}

// renderer turns events into messages. It keeps small per-Run state: the last
// hypothesis (to skip repeats) and proposal titles (to name approvals).
type renderer struct {
	publicURL      string
	lastHypothesis string
	titles         map[string]string
}

func newRenderer(publicURL string) *renderer {
	return &renderer{publicURL: publicURL, titles: map[string]string{}}
}

func (r *renderer) roomLink(roomID string) string {
	if r.publicURL == "" {
		return ""
	}
	return fmt.Sprintf("<%s/rooms/%s|Open room>", r.publicURL, url.PathEscape(roomID))
}

func actorName(a events.Actor) string {
	if a.Name != "" {
		return a.Name
	}
	return a.ID
}

func md(text string) textObj { return textObj{Type: "mrkdwn", Text: text, Verbatim: true} }

func section(text string) block { m := md(text); return block{Type: "section", Text: &m} }

// linkElement is trusted markup only (our URL + fixed label), so it is the one
// text object without verbatim; Slack must parse it as a link.
func (r *renderer) linkElement(roomID string) []textObj {
	if l := r.roomLink(roomID); l != "" {
		return []textObj{{Type: "mrkdwn", Text: l, Verbatim: false}}
	}
	return nil
}

func fallback(prefix, untrusted string) string {
	return prefix + clip(untrusted, fallbackBytes-len(prefix))
}

// parent renders the thread's parent message from room.created.
func (r *renderer) parent(e events.Event) (message, error) {
	p, err := events.Decode[events.RoomCreatedPayload](e)
	if err != nil {
		return message{}, err
	}
	sev := clip(strings.ToUpper(p.Severity), 20)
	title := clip(p.Title, 200)
	// The raw alert in p.Alert is deliberately never rendered.
	ctxEls := []textObj{md("Pipeline: " + inlineCode(p.PipelineID, 200)), md("Room: " + inlineCode(e.RoomID, 100))}
	ctxEls = append(ctxEls, r.linkElement(e.RoomID)...)
	return message{
		Text:   fallback("Incident: ", "["+p.Severity+"] "+p.Title),
		Blocks: []block{section("*[" + sev + "] " + title + "*"), {Type: "context", Elements: ctxEls}},
	}, nil
}

// minimalParent is used when the first event we see is not room.created.
func (r *renderer) minimalParent(roomID string) message {
	els := []textObj{md("Piperoom room " + inlineCode(roomID, 100))}
	els = append(els, r.linkElement(roomID)...)
	return message{
		Text:   fallback("Piperoom room ", roomID),
		Blocks: []block{{Type: "context", Elements: els}},
	}
}

func (r *renderer) proposalName(id string) string {
	if t, ok := r.titles[id]; ok {
		return "“" + clip(t, 200) + "”"
	}
	return inlineCode(id, 100)
}

// reply renders a thread reply. ok is false when the event should be skipped.
func (r *renderer) reply(e events.Event) (m message, ok bool, err error) {
	who := clip(actorName(e.Actor), 100)
	switch e.Type {
	case events.AgentHypothesis:
		p, err := events.Decode[events.AgentHypothesisPayload](e)
		if err != nil {
			return m, false, err
		}
		if p.Text == r.lastHypothesis {
			return m, false, nil
		}
		r.lastHypothesis = p.Text
		head := "*Hypothesis* (confidence: " + clip(p.Confidence, 20)
		if p.Confirmed {
			head += ", confirmed"
		}
		head += ")"
		return message{Text: fallback("Hypothesis: ", p.Text), Blocks: []block{section(head + "\n" + clip(p.Text, 1500))}}, true, nil

	case events.ProposalCreated:
		p, err := events.Decode[events.ProposalCreatedPayload](e)
		if err != nil {
			return m, false, err
		}
		if len(r.titles) < maxProposalMap {
			r.titles[p.ProposalID] = p.Title
		}
		// The list length comes from the payload, so cap it or the section
		// can pass Slack's 3000-character text limit.
		layers := make([]string, 0, min(len(p.RequiredLayers), maxLayers+1))
		for i, l := range p.RequiredLayers {
			if i == maxLayers {
				layers = append(layers, fmt.Sprintf("+%d more", len(p.RequiredLayers)-maxLayers))
				break
			}
			layers = append(layers, inlineCode(l, 60))
		}
		if len(layers) == 0 {
			layers = append(layers, "(none)")
		}
		blocks := []block{
			section("*Proposed fix:* " + clip(p.Title, 200) + "\nProposal: " + inlineCode(p.ProposalID, 100) +
				"\nNeeds approval from: " + strings.Join(layers, ", ")),
			section("*Risk:* " + clip(p.Risk, 500) + "\n*Rollback:* " + clip(p.Rollback, 500)),
		}
		for i, s := range p.Steps {
			if i == maxSteps {
				blocks = append(blocks, section(fmt.Sprintf("+%d more steps — open the room", len(p.Steps)-maxSteps)))
				break
			}
			blocks = append(blocks, section(fmt.Sprintf("*Step %d* %s on %s\n%s\n%s", i+1,
				inlineCode(s.Action, 100), inlineCode(s.Layer, 60), clip(s.Description, 500), codeBlock(compact(s.Params), 600))))
		}
		foot := []textObj{{Type: "mrkdwn", Text: "Approve or reject in Piperoom", Verbatim: true}}
		foot = append(foot, r.linkElement(e.RoomID)...)
		blocks = append(blocks, block{Type: "context", Elements: foot})
		return message{Text: fallback("Proposed fix: ", p.Title), Blocks: blocks}, true, nil

	case events.ApprovalGiven:
		p, err := events.Decode[events.ApprovalGivenPayload](e)
		if err != nil {
			return m, false, err
		}
		t := fmt.Sprintf(":+1: *%s* approved %s for layer %s", who, r.proposalName(p.ProposalID), inlineCode(p.Layer, 60))
		return message{Text: fallback("Approval: ", actorName(e.Actor)+" / "+p.Layer), Blocks: []block{section(t)}}, true, nil

	case events.ApprovalRejected:
		p, err := events.Decode[events.ApprovalRejectedPayload](e)
		if err != nil {
			return m, false, err
		}
		t := fmt.Sprintf(":no_entry: *%s* rejected %s for layer %s\nReason: %s", who, r.proposalName(p.ProposalID),
			inlineCode(p.Layer, 60), clip(p.Reason, 500))
		return message{Text: fallback("Rejection: ", actorName(e.Actor)+" / "+p.Layer), Blocks: []block{section(t)}}, true, nil

	case events.ActionExecuted:
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			return m, false, err
		}
		res := "ran OK"
		if !p.OK {
			res = "FAILED"
		}
		// step_index is 0-based on the wire; people count from 1.
		t := fmt.Sprintf("*Step %d %s* (%s)\n%s", p.StepIndex+1, res, r.proposalName(p.ProposalID), codeBlock(p.Output, 500))
		return message{Text: fmt.Sprintf("Step %d %s", p.StepIndex+1, res), Blocks: []block{section(t)}}, true, nil

	case events.HandoffCreated:
		p, err := events.Decode[events.HandoffCreatedPayload](e)
		if err != nil {
			return m, false, err
		}
		blocks := []block{section(fmt.Sprintf(":handshake: *Hand-off* from %s to %s\n*What broke:* %s\n*Why:* %s",
			who, clip(actorName(p.To), 100), clip(p.Summary.WhatBroke, 500), clip(p.Summary.Why, 500)))}
		if s := bullets("Open", p.Summary.Open); s != "" {
			blocks = append(blocks, section(s))
		}
		if s := bullets("Risks", p.Summary.Risks); s != "" {
			blocks = append(blocks, section(s))
		}
		return message{Text: fallback("Hand-off to ", actorName(p.To)), Blocks: blocks}, true, nil

	case events.HandoffAccepted:
		t := who + " accepted the hand-off and is now the driver."
		return message{Text: fallback("Hand-off accepted by ", actorName(e.Actor)), Blocks: []block{section(t)}}, true, nil

	case events.ControlLeaseExpired:
		p, err := events.Decode[events.ControlLeaseExpiredPayload](e)
		if err != nil {
			return m, false, err
		}
		// The time comes from a decoded time.Time, so it is ours, not event text.
		t := ":warning: *No active driver.* The lease of " + clip(actorName(p.Driver), 100) + " ran out at " +
			p.LeaseUntil.UTC().Format(time.RFC3339) + " and nobody renewed it. Anyone can take control in Piperoom."
		blocks := []block{section(t)}
		if link := r.linkElement(e.RoomID); link != nil {
			blocks = append(blocks, block{Type: "context", Elements: link})
		}
		return message{Text: fallback("No active driver: ", actorName(p.Driver)), Blocks: blocks}, true, nil

	case events.RoomClosed:
		p, err := events.Decode[events.RoomClosedPayload](e)
		if err != nil {
			return m, false, err
		}
		t := ":white_check_mark: Room closed by " + who + ": " + clip(p.Resolution, 1000)
		return message{Text: fallback("Room closed: ", p.Resolution), Blocks: []block{section(t)}, ReplyBroadcast: true}, true, nil
	}
	return m, false, nil
}

func bullets(head string, items []string) string {
	if len(items) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("*" + head + ":*")
	for i, it := range items {
		if i == maxListItems {
			fmt.Fprintf(&b, "\n+%d more — open the room", len(items)-maxListItems)
			break
		}
		b.WriteString("\n• " + clip(it, 200))
	}
	return b.String()
}

func compact(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return string(raw)
	}
	return buf.String()
}
