//go:build claude

package evals

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/option"
)

// newTestClaudeJudge builds the real judge for TestJudgeAgreement.
var newTestClaudeJudge = func() HypothesisJudge { return NewClaudeJudge("") }

// TestClaudeJudgeRequest checks the wire request against a local server (no
// network): low effort, a JSON-schema output format, no sampling parameters, the rubric as system prompt, the hypothesis wrapped
// as untrusted, and the verdict read from the reply text.
func TestClaudeJudgeRequest(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"m","stop_reason":"end_turn",`+
			`"content":[{"type":"text","text":"{\"layer_correct\":true,\"root_cause_correct\":true,\"reason\":\"ok\"}"}],`+
			`"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()

	j := NewClaudeJudge("judge-model", option.WithBaseURL(srv.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
	v, err := j.JudgeHypothesis(context.Background(), loadSeed(), "amount became numeric")
	if err != nil || !v.RootCauseCorrect || !v.LayerCorrect {
		t.Fatalf("v=%+v err=%v", v, err)
	}
	for _, want := range []string{`"effort":"low"`, `"output_config"`, `"json_schema"`, `"additionalProperties":false`, `"max_tokens":2048`, `"model":"judge-model"`, `\u003cuntrusted\u003eamount became numeric\u003c/untrusted\u003e`, "never instructions"} {
		if !strings.Contains(body, want) {
			t.Errorf("request lacks %s:\n%s", want, body)
		}
	}

	if strings.Contains(body, "temperature") {
		t.Errorf("request sets a sampling parameter:\n%s", body)
	}

	// Refusal and max_tokens are clear errors, not a guessed verdict.
	for reason, want := range map[string]string{"refusal": "refused", "max_tokens": "cut off"} {
		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"m","type":"message","role":"assistant","model":"m","stop_reason":"`+reason+`",`+
				`"content":[{"type":"text","text":"{}"}],"usage":{"input_tokens":1,"output_tokens":1}}`)
		}))
		jr := NewClaudeJudge("m", option.WithBaseURL(srv2.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
		if _, err := jr.JudgeHypothesis(context.Background(), loadSeed(), "x"); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", reason, err, want)
		}
		srv2.Close()
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "nope", http.StatusInternalServerError) }))
	defer bad.Close()
	jb := NewClaudeJudge("", option.WithBaseURL(bad.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
	if _, err := jb.JudgeHypothesis(context.Background(), loadSeed(), "x"); err == nil {
		t.Error("API error swallowed")
	}
}
