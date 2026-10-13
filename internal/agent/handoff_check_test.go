package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

func caseByName(t *testing.T, name string) handoffCase {
	t.Helper()
	for _, c := range handoffCases() {
		if c.name == name {
			return c
		}
	}
	t.Fatalf("no case %q", name)
	return handoffCase{}
}

func cloneSummary(s events.HandoffSummary) events.HandoffSummary {
	s.Done = append([]string{}, s.Done...)
	s.RuledOut = append([]string{}, s.RuledOut...)
	s.Open = append([]string{}, s.Open...)
	s.Risks = append([]string{}, s.Risks...)
	return s
}

func TestCheckHandoffViolations(t *testing.T) {
	tests := []struct {
		name   string
		base   string
		mutate func(*events.HandoffSummary)
		want   []string // rules that must be reported
		none   []string // rules that must not be
	}{
		{"clean", "schema-change", func(*events.HandoffSummary) {}, nil, []string{RuleUnknownName, RuleOpenProposal}},
		{"invented connector", "schema-change",
			func(s *events.HandoffSummary) { s.Why = "pg-payments-connector rejected the schema" },
			[]string{RuleUnknownName}, nil},
		{"invented topic in backticks", "schema-change",
			func(s *events.HandoffSummary) { s.Done = append(s.Done, "Checked `pg.public.refunds`") },
			[]string{RuleUnknownName}, nil},
		{"invented slot", "slot-inactive-wal-growth",
			func(s *events.HandoffSummary) { s.WhatBroke = "slot debezium_payments is inactive" },
			[]string{RuleUnknownName}, nil},
		{"invented number", "slot-inactive-wal-growth",
			func(s *events.HandoffSummary) { s.Open = []string{"Retained WAL was 91.2 GiB"} },
			[]string{RuleUnknownNumber}, nil},
		{"number in log matches", "topic-retention-data-loss",
			func(s *events.HandoffSummary) { s.Risks = []string{"7.9 and 2.9 appear in the log"} },
			nil, []string{RuleUnknownNumber}},
		{"unknown seq", "schema-change",
			func(s *events.HandoffSummary) { s.Done = []string{"Saw the failure (seq 999)"} },
			[]string{RuleUnknownRef}, nil},
		{"known seq", "schema-change",
			func(s *events.HandoffSummary) { s.Done = []string{"Saw the failure (seq 3)"} },
			nil, []string{RuleUnknownRef}},
		{"unknown step id", "schema-change",
			func(s *events.HandoffSummary) { s.Done = []string{"See step s_42"} },
			[]string{RuleUnknownRef}, nil},
		{"unknown proposal id", "schema-change",
			func(s *events.HandoffSummary) { s.Done = []string{"Approved p_404"} },
			[]string{RuleUnknownRef}, nil},
		{"open proposal dropped", "open-proposal-two-layers",
			func(s *events.HandoffSummary) { s.Open = nil },
			[]string{RuleOpenProposal}, nil},
		{"open proposal listed by title only", "open-proposal-two-layers",
			func(s *events.HandoffSummary) {
				s.Open = []string{"Register schema and restart task needs schema-registry and kafka-connect"}
			},
			nil, []string{RuleOpenProposal, RulePendingLayer}},
		{"pending layer missing", "open-proposal-two-layers",
			func(s *events.HandoffSummary) { s.Open = []string{"Proposal p_9 waits for schema-registry"} },
			[]string{RulePendingLayer}, []string{RuleOpenProposal}},
		{"kafka does not match kafka-connect", "open-proposal-two-layers",
			func(s *events.HandoffSummary) { s.Open = []string{"Proposal p_9 waits for schema-registry and kafka"} },
			[]string{RulePendingLayer}, nil},
		{"approved layer need not be named", "open-proposal-two-layers",
			func(s *events.HandoffSummary) { s.Open = []string{"p_9: schema-registry, kafka-connect"} },
			nil, []string{RulePendingLayer, RuleOpenProposal}},
		{"executed proposal needs no mention", "proposal-executed",
			func(s *events.HandoffSummary) { s.Open = nil }, nil, []string{RuleOpenProposal}},
		{"rejected proposal needs no mention", "proposal-rejected",
			func(s *events.HandoffSummary) { s.Open = []string{"Find the cause"} }, nil, []string{RuleOpenProposal}},
		{"interrupted run not mentioned", "interrupted-run",
			func(s *events.HandoffSummary) { s.Open = []string{"Find the cause"}; s.Risks = nil },
			[]string{RuleInterruptedRun}, nil},
		{"finished run needs no note", "schema-change",
			func(s *events.HandoffSummary) { s.Risks = nil }, nil, []string{RuleInterruptedRun}},
		{"too many items", "schema-change",
			func(s *events.HandoffSummary) {
				s.Done = make([]string, 11)
				for i := range s.Done {
					s.Done[i] = "step"
				}
			},
			[]string{RuleListTooLong}, nil},
		{"string too long", "schema-change",
			func(s *events.HandoffSummary) { s.Why = strings.Repeat("x", HandoffStringMax+1) },
			[]string{RuleStringTooLong}, nil},
		{"empty required field", "schema-change",
			func(s *events.HandoffSummary) { s.Why = " " },
			[]string{RuleEmptyField}, nil},
		{"empty list item", "schema-change",
			func(s *events.HandoffSummary) { s.Done = append(s.Done, "") },
			[]string{RuleEmptyField}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := caseByName(t, tt.base)
			s := cloneSummary(c.summary)
			tt.mutate(&s)
			vs := CheckHandoff(c.events, s)
			got := map[string]bool{}
			for _, v := range vs {
				got[v.Rule] = true
			}
			for _, r := range tt.want {
				if !got[r] {
					t.Errorf("missing %s; got:\n%s", r, joinViolations(vs))
				}
			}
			for _, r := range tt.none {
				if got[r] {
					t.Errorf("unexpected %s; got:\n%s", r, joinViolations(vs))
				}
			}
		})
	}
}

func TestCheckHandoffEmptyLog(t *testing.T) {
	vs := CheckHandoff(nil, events.HandoffSummary{WhatBroke: "pg-orders-connector failed", Why: "x"})
	if len(vs) == 0 {
		t.Fatal("a name with no log behind it must be flagged")
	}
}

func TestContainsNumber(t *testing.T) {
	tests := []struct {
		text, n string
		want    bool
	}{
		{"lag=2900000", "2900000", true},
		{"lag=29000001", "2900000", false},
		{"retained 1,234 rows", "1234", true},
		{"48.6 gib", "48.6", true},
		{"148.65", "48.6", false},
	}
	for _, tt := range tests {
		if got := containsNumber(tt.text, tt.n); got != tt.want {
			t.Errorf("containsNumber(%q,%q)=%v", tt.text, tt.n, got)
		}
	}
}
