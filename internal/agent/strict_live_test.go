//go:build claude

package agent

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
)

// strictCapCandidates are the caps the live bisect probes: the shipped cap plus
// a few steps on either side, up to the API's hard limit. maxStrictTools says
// the hard limit is 20 per request ("Too many strict tools (21)") and that 20
// strict tools already exceeded the compiled-grammar size limit in a live run,
// so 20 is expected to reject with the grammar error and 21 with the count
// error.
var strictCapCandidates = []int{0, 4, 6, 8, 10, 12, 16, 20, 21}

// TestLiveStrictToolCap sends the real tool list (built by buildToolListWithCap,
// the same selection and schema shaping production ships) to the Anthropic API
// with max_tokens: 1, varying only the strict cap, and asserts the shipped
// maxStrictTools is accepted and not above what the API accepts. It is gated by
// the `claude` build tag and skips when ANTHROPIC_API_KEY is unset, so a normal
// run never needs a key and never fails here.
//
//	go test -tags claude ./internal/agent -run TestLiveStrictToolCap
func TestLiveStrictToolCap(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set; set it to run the live strict-tool-cap bisect")
	}

	// The exact production tool set (real read tools including the runbook tool,
	// and room tools with the real actions registry), so the compiled grammar
	// the API sees, including propose_fix's nested params, is the one that ships.
	reg, room := productionToolSet(t)

	// The production client: the SDK reads ANTHROPIC_API_KEY itself, and
	// newModel adds the anthropic-workspace-id header when ANTHROPIC_WORKSPACE_ID
	// is set. The key is never read or logged here.
	m := newModel(Options{}, discardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	maxAccepted := -1
	for _, cap := range strictCapCandidates {
		tl := buildToolListWithCap(reg, room, cap)
		_, strict := strictNames(t, tl)
		if got := len(strict); got != cap {
			t.Fatalf("cap %d produced %d strict tools, want %d: buildToolListWithCap is broken", cap, got, cap)
		}

		ok, reason := strictRequestAccepted(ctx, m, tl.params)
		if ok {
			maxAccepted = max(maxAccepted, cap)
		}
		t.Logf("cap=%2d strict=%2d accepted=%v reason=%q", cap, len(strict), ok, reason)

		// cap 0 (no strict tools) is the control: if it is rejected the envelope
		// or model is invalid, not the strict cap, so every later result is noise.
		if cap == 0 && !ok {
			t.Fatalf("cap=0 (no strict tools) was rejected (%s): the request envelope or model is invalid, not the strict cap", reason)
		}
		if cap == maxStrictTools && !ok {
			t.Errorf("shipped maxStrictTools=%d was rejected by the live API (%s)", cap, reason)
		}
	}

	if maxAccepted < maxStrictTools {
		t.Errorf("live API accepts at most %d strict tools, fewer than the shipped maxStrictTools=%d", maxAccepted, maxStrictTools)
	}
}

// strictRequestAccepted sends params once with a single output token and
// classifies the reply. Rejections for the strict cap carry the API's own
// wording; anything else is returned verbatim so the failure is visible.
func strictRequestAccepted(ctx context.Context, m model, params []anthropic.BetaToolUnionParam) (bool, string) {
	req := anthropic.BetaMessageNewParams{
		Model:     defaultModel,
		MaxTokens: 1, // one output token per call keeps cost ~0
		Tools:     params,
		Messages:  []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock("ping"))},
	}
	if _, err := m.Turn(ctx, req); err != nil {
		lower := strings.ToLower(err.Error())
		switch {
		case strings.Contains(lower, "compiled grammar is too large"):
			return false, "rejected: compiled grammar is too large"
		case strings.Contains(lower, "too many strict tools"):
			return false, "rejected: too many strict tools"
		default:
			return false, err.Error()
		}
	}
	return true, ""
}
