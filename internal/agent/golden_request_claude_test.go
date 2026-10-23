//go:build claude

package agent

import (
	"context"
	"testing"
)

// TestGoldenJudgeRequest captures the hand-off judge's request (V1a-0). The
// judge lives behind the live-judge build tag, so this golden runs only with
// it:
//
//	go test -tags claude ./internal/agent -run Golden            (compare)
//	go test -tags claude ./internal/agent -run Golden -update    (rewrite)
//
// It sends nothing outside the test server: the endpoint is pointed at it.
func TestGoldenJudgeRequest(t *testing.T) {
	c := caseByName(t, "schema-change")
	gs := newGoldenServer(t,
		sseAnswer(t, "msg_golden_judge", goldenMain, "end_turn", sseText(`{"contradictions":[]}`)),
	)
	ep := goldenEndpoint(t, gs, EndpointConfig{Provider: ProviderAnthropic})
	v, err := ep.JudgeHandoff(context.Background(), c.events, c.summary, Options{Model: goldenMain})
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	if !v.OK() {
		t.Fatalf("verdict = %+v", v)
	}
	checkGoldens(t, gs, "judge")
}
