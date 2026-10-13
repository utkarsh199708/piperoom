//go:build claude

package agent

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

func TestJudgeHandoffParsesVerdict(t *testing.T) {
	c := caseByName(t, "schema-change")
	var gotParams anthropic.BetaMessageNewParams
	m := &capturingModel{msg: anthropic.BetaMessage{
		StopReason: anthropic.BetaStopReasonEndTurn,
		Content: []anthropic.BetaContentBlockUnion{{Type: "text",
			Text: `{"contradictions":[{"claim":"lag is 0","evidence":"lag=5"}]}`}},
	}, params: &gotParams}
	v, err := judgeHandoff(context.Background(), c.events, c.summary, m, judgeModel)
	if err != nil {
		t.Fatal(err)
	}
	if v.OK() || v.Contradictions[0].Claim != "lag is 0" {
		t.Fatalf("verdict = %+v", v)
	}
	if !gotParams.Temperature.Valid() || gotParams.Temperature.Value != 0 {
		t.Errorf("temperature = %+v, want 0", gotParams.Temperature)
	}
	body := gotParams.Messages[0].Content[0].OfText.Text
	if !strings.Contains(body, "<untrusted>") {
		t.Errorf("summary not wrapped as untrusted: %q", body)
	}
}

func TestJudgeHandoffRejectsBadVerdict(t *testing.T) {
	c := caseByName(t, "schema-change")
	for _, text := range []string{"nope", `{"contradictions":[],"extra":1}`} {
		m := &capturingModel{msg: anthropic.BetaMessage{
			StopReason: anthropic.BetaStopReasonEndTurn,
			Content:    []anthropic.BetaContentBlockUnion{{Type: "text", Text: text}},
		}, params: new(anthropic.BetaMessageNewParams)}
		if _, err := judgeHandoff(context.Background(), c.events, c.summary, m, judgeModel); err == nil {
			t.Errorf("%q: want error", text)
		}
	}
}

type capturingModel struct {
	msg    anthropic.BetaMessage
	params *anthropic.BetaMessageNewParams
}

func (m *capturingModel) Turn(_ context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	*m.params = p
	return m.msg, nil
}

// TestClaudeHandoffQuality runs the real hand-off writer on every regression
// log, then the deterministic checker and the judge. It needs
// ANTHROPIC_API_KEY:
//
//	go test -tags claude ./internal/agent -run TestClaudeHandoffQuality -v
func TestClaudeHandoffQuality(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}
	for _, c := range handoffCases() {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := WriteHandoff(ctx, c.events, Options{})
			if err != nil {
				t.Fatalf("WriteHandoff: %v", err)
			}
			// The room trims lists and strings before storing; do the same cut.
			s = clampHandoff(s)
			for _, v := range CheckHandoff(c.events, s) {
				t.Errorf("check: %s", v)
			}
			verdict, err := JudgeHandoff(ctx, c.events, s, Options{}, "")
			if err != nil {
				t.Fatalf("JudgeHandoff: %v", err)
			}
			for _, k := range verdict.Contradictions {
				t.Errorf("judge: claim %q contradicted by %q", k.Claim, k.Evidence)
			}
		})
	}
}

// clampHandoff applies the caps CheckHandoff enforces, so the live test
// reports content problems rather than length the room would cut anyway.
func clampHandoff(s events.HandoffSummary) events.HandoffSummary {
	cut := func(in []string) []string {
		if len(in) > HandoffListMax {
			in = in[:HandoffListMax]
		}
		return in
	}
	s.Done, s.RuledOut, s.Open, s.Risks = cut(s.Done), cut(s.RuledOut), cut(s.Open), cut(s.Risks)
	return s
}
