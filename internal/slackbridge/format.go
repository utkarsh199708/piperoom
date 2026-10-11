package slackbridge

import (
	"fmt"
	"strings"

	"piperoom/internal/events"
)

const maxTextRunes = 2500

// escape neutralises Slack control characters and mentions. Room text can come
// from alerts, logs and record values, so it must never ping @channel or
// build links.
func escape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}

func clip(s string) string {
	rs := []rune(s)
	if len(rs) <= maxTextRunes {
		return s
	}
	return string(rs[:maxTextRunes]) + "… (truncated)"
}

func safe(s string) string { return escape(clip(s)) }

// Format renders an event as a Slack message. ok is false for events that are
// too noisy for chat (tool calls, raw tool data); the full detail stays in the
// room. Tool results are deliberately not mirrored: they may hold record values.
func Format(e events.Event, roomURL string) (text string, ok bool) {
	switch e.Type {
	case events.RoomCreated:
		p, err := events.Decode[events.RoomCreatedPayload](e)
		if err != nil {
			return "", false
		}
		t := fmt.Sprintf(":rotating_light: *[%s] %s*\nPipeline: `%s`", safe(strings.ToUpper(p.Severity)), safe(p.Title), safe(p.PipelineID))
		if roomURL != "" {
			t += "\nRoom: " + roomURL + "/rooms/" + e.RoomID
		}
		return t, true
	case events.RoomClosed:
		p, err := events.Decode[events.RoomClosedPayload](e)
		if err != nil {
			return "", false
		}
		return ":white_check_mark: Room closed: " + safe(p.Resolution), true
	case events.AgentStatus:
		p, err := events.Decode[events.AgentStatusPayload](e)
		if err != nil || (p.State != "waiting_approval" && p.State != "error" && p.State != "done") {
			return "", false
		}
		return fmt.Sprintf(":robot_face: Agent is *%s* %s", safe(p.State), safe(p.Detail)), true
	case events.AgentStepStart:
		p, err := events.Decode[events.AgentStepStartPayload](e)
		if err != nil {
			return "", false
		}
		return ":mag: Step: " + safe(p.Title), true
	case events.AgentMessage:
		p, err := events.Decode[events.AgentMessagePayload](e)
		if err != nil {
			return "", false
		}
		return ":robot_face: " + safe(p.Text), true
	case events.AgentHypothesis:
		p, err := events.Decode[events.AgentHypothesisPayload](e)
		if err != nil {
			return "", false
		}
		state := "hypothesis"
		if p.Confirmed {
			state = "confirmed cause"
		}
		return fmt.Sprintf(":bulb: *%s* (%s confidence): %s", state, safe(p.Confidence), safe(p.Text)), true
	case events.CommentAdded:
		p, err := events.Decode[events.CommentAddedPayload](e)
		if err != nil {
			return "", false
		}
		return fmt.Sprintf(":speech_balloon: *%s*: %s", safe(e.Actor.Name), safe(p.Text)), true
	case events.ControlGranted:
		p, err := events.Decode[events.ControlGrantedPayload](e)
		if err != nil {
			return "", false
		}
		return ":steering_wheel: Driver is now *" + safe(p.To.Name) + "*", true
	case events.ProposalCreated:
		p, err := events.Decode[events.ProposalCreatedPayload](e)
		if err != nil {
			return "", false
		}
		return fmt.Sprintf(":clipboard: *Proposed fix:* %s\nRisk: %s\nNeeds approval from: %s\nApprove in the room — not in Slack.",
			safe(p.Title), safe(p.Risk), safe(strings.Join(p.RequiredLayers, ", "))), true
	case events.ApprovalGiven:
		p, err := events.Decode[events.ApprovalGivenPayload](e)
		if err != nil {
			return "", false
		}
		return fmt.Sprintf(":+1: %s approved by `%s`", safe(e.Actor.Name), safe(p.Layer)), true
	case events.ApprovalRejected:
		p, err := events.Decode[events.ApprovalRejectedPayload](e)
		if err != nil {
			return "", false
		}
		return fmt.Sprintf(":no_entry: `%s` rejected: %s", safe(p.Layer), safe(p.Reason)), true
	case events.ActionExecuted:
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			return "", false
		}
		icon := ":white_check_mark:"
		if !p.OK {
			icon = ":x:"
		}
		return fmt.Sprintf("%s Step %d executed: %s", icon, p.StepIndex+1, safe(p.Output)), true
	case events.HandoffCreated:
		p, err := events.Decode[events.HandoffCreatedPayload](e)
		if err != nil {
			return "", false
		}
		return fmt.Sprintf(":handshake: Hand-off to *%s*\n*What broke:* %s\n*Why:* %s", safe(p.To.Name), safe(p.Summary.WhatBroke), safe(p.Summary.Why)), true
	case events.HandoffAccepted:
		return ":handshake: Hand-off accepted", true
	}
	return "", false
}
