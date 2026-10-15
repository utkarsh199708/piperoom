package events_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// ToolCall must stay a field-for-field mirror of tools.Call, or the agent's
// conversion silently drops a field.
func TestToolCallMirrorsToolsCall(t *testing.T) {
	a, b := reflect.TypeOf(events.ToolCall{}), reflect.TypeOf(tools.Call{})
	if a.NumField() != b.NumField() {
		t.Fatalf("events.ToolCall has %d fields, tools.Call %d", a.NumField(), b.NumField())
	}
	for i := 0; i < a.NumField(); i++ {
		fa, fb := a.Field(i), b.Field(i)
		if fa.Name != fb.Name || fa.Type != fb.Type || fa.Tag != fb.Tag {
			t.Errorf("field %d: events %s %s %q, tools %s %s %q", i, fa.Name, fa.Type, fa.Tag, fb.Name, fb.Type, fb.Tag)
		}
	}
}

// The F-wave fields are omitempty on payloads that already exist, so an event
// written before F0 marshals exactly as before (export hashes, params_hash).
func TestFWaveFieldsAreOmittedWhenEmpty(t *testing.T) {
	tests := []struct {
		name   string
		v      any
		absent string
	}{
		{"tool result provenance", events.AgentToolResultPayload{StepID: "s_1"}, "provenance"},
		{"hypothesis unbacked terms", events.AgentHypothesisPayload{Text: "x"}, "unbacked_terms"},
		{"handoff violations", events.HandoffSummary{WhatBroke: "x"}, "violations"},
		{"proposal step verify", events.ProposalStep{Action: "a", Params: json.RawMessage(`{}`)}, "verify"},
		{"sink check zero times", events.SinkCheck{Table: "t"}, "newest"},
		{"preview without dry run", events.PreviewResult{Target: "t"}, "dry_run"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.v)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(b), `"`+tt.absent+`"`) {
				t.Fatalf("%s present in %s", tt.absent, b)
			}
		})
	}
}

// A checked, clean hand-off keeps "violations": [] so clients can tell it from
// an unchecked one, and the field round-trips.
func TestHandoffViolationsCheckedAndClean(t *testing.T) {
	for _, tt := range []struct {
		name string
		v    []events.HandoffViolation
		want string
	}{
		{"not checked", nil, `{"what_broke":"x","why":"","done":null,"ruled_out":null,"open":null,"risks":null}`},
		{"checked and clean", []events.HandoffViolation{}, `{"what_broke":"x","why":"","done":null,"ruled_out":null,"open":null,"risks":null,"violations":[]}`},
		{"one violation", []events.HandoffViolation{{Rule: "unknown_number", Field: "why", Detail: "42"}},
			`{"what_broke":"x","why":"","done":null,"ruled_out":null,"open":null,"risks":null,"violations":[{"rule":"unknown_number","field":"why","detail":"42"}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := events.HandoffSummary{WhatBroke: "x", Violations: tt.v}
			b, err := json.Marshal(in)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != tt.want {
				t.Fatalf("got  %s\nwant %s", b, tt.want)
			}
			var out events.HandoffSummary
			if err := json.Unmarshal(b, &out); err != nil {
				t.Fatal(err)
			}
			if (out.Violations == nil) != (tt.v == nil) || len(out.Violations) != len(tt.v) {
				t.Fatalf("round trip: got %#v, want %#v", out.Violations, tt.v)
			}
			// As a pointer and inside a payload too.
			p, _ := json.Marshal(&in)
			if string(p) != tt.want {
				t.Fatalf("pointer: got %s", p)
			}
		})
	}
}

func TestFWaveWireNames(t *testing.T) {
	types := map[events.Type]string{
		events.ProposalPreviewed: "proposal.previewed",
		events.AgentVerification: "agent.verification",
		events.PostmortemDrafted: "postmortem.drafted",
		events.PostmortemFailed:  "postmortem.failed",
		events.HypothesisVerdict: "hypothesis.verdict",
		events.ApprovalNudged:    "approval.nudged",
		events.AgentTriage:       "agent.triage",
		events.RoomLabelled:      "room.labelled",
		events.ChecklistStepDone: "checklist.step_done",
	}
	for typ, want := range types {
		if string(typ) != want {
			t.Errorf("type %q, want %q", typ, want)
		}
	}
	if events.UsageRolePostmortem != "postmortem" || events.UsageRoleFirstLook != "first_look" {
		t.Errorf("usage roles = %q, %q", events.UsageRolePostmortem, events.UsageRoleFirstLook)
	}

	payloads := []struct {
		v    any
		keys []string
	}{
		{events.AgentTriagePayload{Status: "failed", Reason: "off"}, []string{`"status"`, `"reason"`, `"evidence_refs"`, `"model"`, `"took_ms"`}},
		{events.HypothesisVerdictPayload{Verdict: "confirmed", HypothesisSeq: 7}, []string{`"verdict"`, `"hypothesis_seq":7`}},
		{events.ApprovalNudgedPayload{ProposalID: "p_1", Layer: "kafka"}, []string{`"proposal_id"`, `"layer"`}},
		{events.PostmortemDraftedPayload{Source: "events"}, []string{`"source":"events"`}},
		{events.PostmortemFailedPayload{Reason: events.HandoffFailedSummary}, []string{`"reason":"summary_failed"`}},
		{events.RoomLabelledPayload{Cause: "c"}, []string{`"cause"`}},
		{events.ChecklistStepDonePayload{ToolUseID: "tu", Done: true}, []string{`"step_id"`, `"tool_use_id"`, `"hit"`, `"item"`, `"done":true`}},
		{events.ProposalPreviewedPayload{ProposalID: "p_1", Steps: []events.StepPreview{{StepIndex: 0, Preview: &events.PreviewResult{DryRun: &events.DryRunResult{Source: "s"}}}}}, []string{`"steps"`, `"step_index"`, `"preview"`, `"dry_run"`, `"reversible"`}},
		{events.AgentVerificationPayload{ProposalID: "p_1", DataFlow: &events.DataFlowCheck{Sinks: []events.SinkCheck{{Table: "t"}}}}, []string{`"checks"`, `"data_flow"`, `"sinks"`, `"recovered"`, `"verdict"`}},
	}
	for _, p := range payloads {
		b, err := json.Marshal(p.v)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range p.keys {
			if !strings.Contains(string(b), k) {
				t.Errorf("%T JSON %s lacks %s", p.v, b, k)
			}
		}
	}
}
