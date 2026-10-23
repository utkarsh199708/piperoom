package evals

import (
	"os"
	"strings"
	"testing"

	"piperoom/internal/agent/llm/openai"
)

// TestEvalsLiveHasEveryProviderKey: .github/workflows/evals-live.yml names one
// secret per provider (V12). The workflow cannot import openai.KeyEnv, so this
// fails when a flavor's key variable is missing from it, in the plan job's map
// or in the env blocks that expose it, and so a provider could never be run.
func TestEvalsLiveHasEveryProviderKey(t *testing.T) {
	b, err := os.ReadFile("../.github/workflows/evals-live.yml")
	if err != nil {
		t.Fatal(err)
	}
	wf := string(b)
	keys := []string{"GEMINI_API_KEY", "AWS_BEARER_TOKEN_BEDROCK", "ANTHROPIC_API_KEY"}
	for _, f := range openai.Flavors() {
		switch f {
		case openai.FlavorVLLM, openai.FlavorGeneric:
			continue // PIPEROOM_MODEL_API_KEY: optional, no secret of its own
		}
		if k := openai.KeyEnv(f); k != "" {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		if !strings.Contains(wf, `"`+k+`"`) {
			t.Errorf("evals-live.yml: the plan job's key map has no %s", k)
		}
		// Exposed to the job only for the leg that chose it.
		if want := "matrix.key_env == '" + k + "' && secrets." + k; k != "ANTHROPIC_API_KEY" && !strings.Contains(wf, want) {
			t.Errorf("evals-live.yml: %s is not exposed by %q", k, want)
		}
	}
	// No secret is read outside the one-leg conditions, except the judge's.
	for _, line := range strings.Split(wf, "\n") {
		if strings.Contains(line, "secrets.") && !strings.Contains(line, "matrix.key_env ==") &&
			!strings.Contains(line, "secrets.ANTHROPIC_API_KEY") {
			t.Errorf("evals-live.yml reads a secret outside its leg condition: %s", strings.TrimSpace(line))
		}
	}
}
