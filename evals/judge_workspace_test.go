//go:build livejudge || claude

package evals

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/option"
)

// TestClaudeJudgeWorkspaceHeader checks that the judge sends the workspace
// header a user API key needs, only when ANTHROPIC_WORKSPACE_ID is set, and
// never sends a blank one. Found by running the first judge fill: a user key
// got "400 ... must include the anthropic-workspace-id header".
func TestClaudeJudgeWorkspaceHeader(t *testing.T) {
	for _, tc := range []struct {
		name, env, want string
	}{
		{"set", "ws_test_123", "ws_test_123"},
		{"padded", "  ws_test_123  ", "ws_test_123"},
		{"empty", "", ""},
		{"blank", "   ", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ANTHROPIC_WORKSPACE_ID", tc.env)
			var got string
			var seen bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("anthropic-workspace-id")
				_, seen = r.Header["Anthropic-Workspace-Id"]
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"m","type":"message","role":"assistant","model":"m","stop_reason":"end_turn",`+
					`"content":[{"type":"text","text":"{\"layer_correct\":true,\"root_cause_correct\":true,\"root_cause_layer_correct\":true,\"reason\":\"ok\"}"}],`+
					`"usage":{"input_tokens":1,"output_tokens":1}}`)
			}))
			defer srv.Close()
			j := NewClaudeJudge("m", option.WithBaseURL(srv.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
			if _, err := j.JudgeHypothesis(context.Background(), loadSeed(), "x"); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("anthropic-workspace-id = %q, want %q", got, tc.want)
			}
			if tc.want == "" && seen {
				t.Error("a blank workspace id must not send the header at all")
			}
		})
	}
}
