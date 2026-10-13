package agent

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

// extraCases are logs only the checker tests need.
func extraCases() []handoffCase {
	base := events.HandoffSummary{WhatBroke: "Something broke.", Why: "Not known.", Done: []string{}, RuledOut: []string{}, Open: []string{}, Risks: []string{}}
	return []handoffCase{
		{"suggestion", (&logBuilder{}).room("x").suggest("g_1", "try a restart").status("done").evs, base},
		{"dangling-call", (&logBuilder{}).room("x").callOnly("connect_status").evs, base},
		{"step-only", (&logBuilder{}).room("x").tool("connect_status", "ok").stepOnly("next").evs, base},
		{"working-after-result", (&logBuilder{}).room("x").status("working").tool("connect_status", "ok").evs, base},
		{"closed-room", (&logBuilder{}).room("x").callOnly("connect_status").status("error").close("fixed by hand").evs, base},
		{"duplicate-proposal", (&logBuilder{}).room("x").
			propose("p_1", "Fix it", "kafka-connect").propose("p_1", "Fix it", "kafka-connect").evs, base},
		{"thousands", (&logBuilder{}).room("x").tool("kafka_consumer_lag", "lag 12,000 records").evs, base},
		{"failed-step0-two-step", (&logBuilder{}).room("x").
			propose("p_8", "Two step fix", "schema-registry", "kafka-connect").
			approve("p_8", "schema-registry").approve("p_8", "kafka-connect").
			failedAction("p_8", 0, "refused").evs, base},
		{"restarted-run", (&logBuilder{}).room("x").status("working").callOnly("connect_status").
			status("working").tool("connect_status", "ok").status("done").evs, base},
		{"order-sync-1", (&logBuilder{}).room("x").tool("kafka_list_groups", "group order-sync-1 Stable").evs, base},
	}
}

func caseByName(t *testing.T, name string) handoffCase {
	t.Helper()
	for _, c := range append(handoffCases(), extraCases()...) {
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
		{"unknown comment id", "consumer-poison-pill",
			func(s *events.HandoffSummary) { s.Open = []string{"See comment c_99"} },
			[]string{RuleUnknownRef}, nil},
		{"known comment id", "consumer-poison-pill",
			func(s *events.HandoffSummary) { s.Open = []string{"See comment c_1"} },
			nil, []string{RuleUnknownRef}},
		{"unknown suggestion id", "suggestion",
			func(s *events.HandoffSummary) { s.Open = []string{"See suggestion g_404"} },
			[]string{RuleUnknownRef}, nil},
		{"known suggestion id", "suggestion",
			func(s *events.HandoffSummary) { s.Open = []string{"See suggestion g_1"} },
			nil, []string{RuleUnknownRef}},
		{"failed proposal missing", "failed-one-step",
			func(s *events.HandoffSummary) { s.Open = []string{"Find the cause"} },
			[]string{RuleFailedAction}, []string{RuleOpenProposal}},
		{"failed proposal named in risks", "failed-one-step",
			func(s *events.HandoffSummary) { s.Open = []string{"Find the cause"}; s.Risks = []string{"p_4 failed"} },
			nil, []string{RuleFailedAction}},
		{"failed proposal named by title", "failed-one-step",
			func(s *events.HandoffSummary) { s.Open = []string{"Restart task 0 did not work"} },
			nil, []string{RuleFailedAction}},
		{"failed multi-step proposal missing", "failed-multi-step",
			func(s *events.HandoffSummary) { s.Open = nil; s.Risks = nil },
			[]string{RuleFailedAction}, nil},
		{"only kafka-connect named for kafka layer", "pending-kafka-layer",
			func(s *events.HandoffSummary) { s.Open = []string{"Proposal p_6 needs kafka-connect"} },
			[]string{RulePendingLayer}, nil},
		{"kafka layer named", "pending-kafka-layer",
			func(s *events.HandoffSummary) {}, nil, []string{RulePendingLayer}},
		{"generic failure word is not an interruption note", "interrupted-run",
			func(s *events.HandoffSummary) {
				s.Open = []string{"Task 0 failed and the connector stopped"}
				s.Risks = []string{"An error in the schema"}
			},
			[]string{RuleInterruptedRun}, nil},
		{"agent run note in risks", "interrupted-run",
			func(s *events.HandoffSummary) { s.Open = nil; s.Risks = []string{"The agent run stopped unfinished"} },
			nil, []string{RuleInterruptedRun}},
		{"dangling tool call", "dangling-call",
			func(s *events.HandoffSummary) { s.Open = []string{"Find the cause"} },
			[]string{RuleInterruptedRun}, nil},
		{"dangling tool call noted", "dangling-call",
			func(s *events.HandoffSummary) { s.Open = []string{"The investigation was interrupted mid tool call"} },
			nil, []string{RuleInterruptedRun}},
		{"log ends on a step start", "step-only",
			func(s *events.HandoffSummary) { s.Open = []string{"Find the cause"} },
			[]string{RuleInterruptedRun}, nil},
		{"working after a tool result", "working-after-result",
			func(s *events.HandoffSummary) { s.Open = []string{"Find the cause"} },
			[]string{RuleInterruptedRun}, nil},
		{"closed room needs no interruption note", "closed-room",
			func(s *events.HandoffSummary) { s.Open = nil; s.Risks = nil },
			nil, []string{RuleInterruptedRun}},
		{"duplicate proposal.created keeps state", "duplicate-proposal",
			func(s *events.HandoffSummary) { s.Open = nil },
			[]string{RuleOpenProposal}, nil},
		{"comma-separated number in the log", "thousands",
			func(s *events.HandoffSummary) { s.Risks = []string{"Lag is 12000 records and 3 partitions"} },
			nil, []string{RuleUnknownNumber}},
		{"failed step 0 of two, only in risks, is not an open proposal", "failed-step0-two-step",
			func(s *events.HandoffSummary) { s.Open = nil; s.Risks = []string{"p_8 failed at step 0"} },
			nil, []string{RuleOpenProposal, RuleFailedAction}},
		{"a run that restarted and finished is not interrupted", "restarted-run",
			func(s *events.HandoffSummary) {}, nil, []string{RuleInterruptedRun}},
		{"bare agent word is not a run note", "interrupted-run",
			func(s *events.HandoffSummary) {
				s.Open = []string{"The agent found the connector stopped"}
				s.Risks = nil
			},
			[]string{RuleInterruptedRun}, nil},
		{"agent stopped is a run note", "interrupted-run",
			func(s *events.HandoffSummary) {
				s.Open = []string{"The agent stopped before a hypothesis"}
				s.Risks = nil
			},
			nil, []string{RuleInterruptedRun}},
		{"trailing digit hyphen name is checked", "order-sync-1",
			func(s *events.HandoffSummary) { s.Why = "order-sync-2 is lagging" },
			[]string{RuleUnknownName}, nil},
		{"trailing digit hyphen name in the log", "order-sync-1",
			func(s *events.HandoffSummary) { s.Why = "order-sync-1 is lagging" },
			nil, []string{RuleUnknownName}},
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
		{"retained 1,234 rows", "1234", false}, // raw text only; hasNumber also tries the comma-free text
		{"48.6 gib", "48.6", true},
		{"148.65", "48.6", false},
	}
	for _, tt := range tests {
		if got := containsNumber(tt.text, tt.n); got != tt.want {
			t.Errorf("containsNumber(%q,%q)=%v", tt.text, tt.n, got)
		}
	}
}

func TestLogHasNumberIgnoresThousandsCommas(t *testing.T) {
	idx := indexLog((&logBuilder{}).room("x").tool("t", "lag 12,000 and decimal(12,2)").evs)
	for _, n := range []string{"12000", "12"} {
		if !idx.hasNumber(n) {
			t.Errorf("hasNumber(%q) = false", n)
		}
	}
	if idx.hasNumber("122") {
		t.Error("12,2 must not be folded into 122")
	}
}

// TestCheckHandoffProseIsNotFlagged: ordinary hand-off wording must not trip
// the name and number checks, on a log that has none of these tokens.
func TestCheckHandoffProseIsNotFlagged(t *testing.T) {
	log := (&logBuilder{}).room("orders stale").tool("connect_status", "pg-orders-connector FAILED; decimal(12,2); 1,000 rows").evs
	prose := []string{
		"Checked the usual suspects, e.g. the connector, i.e. the Debezium task",
		"Kafka/Connect and read/write paths were fine",
		"See https://example.com/docs/page-1/step-2 for the runbook",
		"The warehouse is up-to-date again, not out-of-sync, with at-least-once delivery",
		"The alert fired at 09:40 and cleared at 9:41:30",
		"Started 2026-10-11 and again 2026-10-11T09:40:00Z",
		"CPU was at 50% and then 99.5%",
		"Waited 15 min, then 30 seconds, then 2 hours, 45 ms and 3x",
		"Postgres on port 5432 answered",
		"Referenced as #12 in the thread",
		"decimal(12,2) is the new type",
		"Used a well-known trick, a one-off fix",
		"Roughly 1,000 rows",
		"Went through the tasks one-by-one over HTTP/2, 24/7",
		"Did it step-by-step",
	}
	for _, p := range prose {
		s := events.HandoffSummary{WhatBroke: "pg-orders-connector FAILED.", Why: p, Done: []string{p}, RuledOut: []string{}, Open: []string{}, Risks: []string{}}
		for _, v := range CheckHandoff(log, s) {
			if v.Rule == RuleUnknownName || v.Rule == RuleUnknownNumber {
				t.Errorf("%q flagged: %s", p, v)
			}
		}
	}
}

// TestCheckHandoffStillFlagsInventions: the relaxed rules keep the point.
func TestCheckHandoffStillFlagsInventions(t *testing.T) {
	log := (&logBuilder{}).room("orders stale").tool("connect_status", "pg-orders-connector FAILED").evs
	tests := []struct{ text, rule string }{
		{"pg-payments-connector failed", RuleUnknownName},
		{"topic pg.public.refunds is empty", RuleUnknownName},
		{"slot debezium_payments inactive", RuleUnknownName},
		{"the `refund_sink` is down", RuleUnknownName},
		{"lag is 48213 records", RuleUnknownNumber},
		{"public/refunds_v2 was scanned", RuleUnknownName},
		{"WAL is 91.2 GiB", RuleUnknownNumber},
	}
	for _, tt := range tests {
		s := events.HandoffSummary{WhatBroke: "pg-orders-connector FAILED.", Why: tt.text, Done: []string{}, RuledOut: []string{}, Open: []string{}, Risks: []string{}}
		found := false
		for _, v := range CheckHandoff(log, s) {
			if v.Rule == tt.rule {
				found = true
			}
		}
		if !found {
			t.Errorf("%q: %s not reported", tt.text, tt.rule)
		}
	}
}
