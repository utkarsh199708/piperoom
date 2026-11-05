package evals

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
)

// refusingModel answers every request with status and body, counting them.
func refusingModel(t *testing.T, status int, body string) (*fakeModel, *atomic.Int32) {
	t.Helper()
	fake := newFakeModel(t, nil)
	var n atomic.Int32
	fake.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	})
	return fake, &n
}

const creditBody = `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}`

// TestAPIRefusal gives today's answers for the API's own errors, classified
// by the Claude adapter as the agent's model turn sees them (V9: APIRefusal
// reads llm.APIError kinds, not the SDK's error).
func TestAPIRefusal(t *testing.T) {
	mk := func(t *testing.T, code int, body string) error {
		fake, _ := refusingModel(t, code, body)
		c := anthropic.NewClient(option.WithBaseURL(fake.URL), option.WithAPIKey("k"), option.WithMaxRetries(0))
		_, err := c.Messages.New(context.Background(), anthropic.MessageNewParams{
			Model: "m", MaxTokens: 1,
			Messages: []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock("x"))},
		})
		return fmt.Errorf("agent: model turn: %w", anthropicllm.ClassifyError(llm.ProviderAnthropic, err, nil))
	}
	for _, tc := range []struct {
		name   string
		code   int
		body   string
		refuse bool
	}{
		{"credit balance 400", 400, creditBody, true},
		{"other 400", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"messages: text content blocks must be non-empty"}}`, false},
		{"401", 401, `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`, true},
		{"403", 403, `{"type":"error","error":{"type":"permission_error","message":"forbidden"}}`, true},
		// The adapter calls this 403 model_not_found (for the triage
		// switch); an evaluation still stops on it, as before V9.
		{"403 naming the model", 403, `{"type":"error","error":{"type":"permission_error","message":"You do not have access to the model claude-x"}}`, true},
		{"404 model", 404, `{"type":"error","error":{"type":"not_found_error","message":"model: claude-x"}}`, false},
		{"too long 400", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 210000 tokens > 200000 maximum"}}`, false},
		{"429", 429, `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`, false},
		{"500", 500, `{"type":"error","error":{"type":"api_error","message":"oops"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, ok := APIRefusal(mk(t, tc.code, tc.body))
			if ok != tc.refuse || status != tc.code {
				t.Errorf("APIRefusal = %d, %v; want %d, %v", status, ok, tc.code, tc.refuse)
			}
		})
	}
	if _, ok := APIRefusal(errors.New("connection refused")); ok {
		t.Error("a plain error is not a refusal")
	}
}

// TestAPIRefusalKinds decides on the llm.APIError kind of any provider: auth
// and billing stop the evaluation, every other kind does not.
func TestAPIRefusalKinds(t *testing.T) {
	for _, tc := range []struct {
		kind   llm.ErrorKind
		status int
		refuse bool
	}{
		{llm.ErrAuth, 401, true},
		{llm.ErrAuth, 403, true},
		{llm.ErrBilling, 429, true}, // OpenAI insufficient_quota
		{llm.ErrBilling, 402, true},
		{llm.ErrModelNotFound, 404, false},
		{llm.ErrModelNotFound, 403, true}, // a 403 is the key's access
		{llm.ErrBadRequest, 400, false},
		{llm.ErrTooLong, 400, false},
		{llm.ErrRateLimited, 429, false},
		{llm.ErrOverloaded, 529, false},
		{llm.ErrTruncated, 200, false},
		{llm.ErrNetwork, 0, false},
	} {
		err := fmt.Errorf("agent: model turn: %w", &llm.APIError{Provider: llm.ProviderOpenAI, Status: tc.status, Kind: tc.kind})
		status, ok := APIRefusal(err)
		if ok != tc.refuse || status != tc.status {
			t.Errorf("%s %d: APIRefusal = %d, %v; want %d, %v", tc.kind, tc.status, status, ok, tc.status, tc.refuse)
		}
	}
	// The SDK's own error, unclassified, is no longer read.
	if _, ok := APIRefusal(&anthropic.Error{StatusCode: 401}); ok {
		t.Error("an unclassified SDK error is not a refusal")
	}
}

func TestRunErrorTextRedactsAndCuts(t *testing.T) {
	secret := "sk-ant-api03-" + strings.Repeat("Ab3x", 12) + "9Q"
	got := RunErrorText("boom " + secret + "\n" + strings.Repeat("y", 500))
	if strings.Contains(got, secret) || strings.Contains(got, "\n") {
		t.Errorf("not redacted or not one line: %q", got)
	}
	if n := len([]rune(got)); n > MaxRunErrorRunes+1 {
		t.Errorf("%d runes", n)
	}
}

// A refused API stops the evaluation after the first request: the runs are
// infra (not scored), the report says why and prints the error, and compared
// with a good baseline no scenario regresses.
func TestEvaluateStopsOnAPIRefusalAndKeepsInfraOutOfRegressions(t *testing.T) {
	good := newFakeModel(t, goodSeedTurns(seedCause))
	baseRep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, endpointConfig(t, good))
	if err != nil || !baseRep.Results[0].Pass {
		t.Fatalf("baseline: %v %+v", err, baseRep.Results)
	}
	baseRep.Runs = 3

	bad, requests := refusingModel(t, 400, creditBody)
	cfg := endpointConfig(t, bad)
	cfg.Runs = 3
	var seen []RunResult
	cfg.OnRun = func(r RunResult, _ Transcript) { seen = append(seen, r) }
	rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, cfg)
	if !errors.Is(err, ErrAPIRefused) {
		t.Fatalf("err = %v, want ErrAPIRefused", err)
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("%d model requests; the first refusal must stop new runs", n)
	}
	if len(rep.Results) != 3 || len(seen) != 3 {
		t.Fatalf("results %d, OnRun %d; want 3 (every run reported)", len(rep.Results), len(seen))
	}
	for _, r := range rep.Results {
		if !r.Infra() || r.Pass {
			t.Errorf("run %d not infra: %+v", r.Run, r)
		}
	}
	if !strings.Contains(rep.Results[0].Error, "credit balance") {
		t.Errorf("error text = %q", rep.Results[0].Error)
	}
	s := rep.Scenarios[0]
	if s.Runs != 0 || s.Infra != 3 || rep.Overall.Infra != 3 || rep.Overall.Runs != 0 {
		t.Errorf("summary = %+v overall %+v", s.Summary, rep.Overall)
	}
	if !strings.Contains(rep.InfraStop, "HTTP 400") {
		t.Errorf("infra stop = %q", rep.InfraStop)
	}

	var md bytes.Buffer
	if err := rep.WriteMarkdown(&md); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"infra (3, not scored)", "credit balance", "Evaluation stopped"} {
		if !strings.Contains(md.String(), want) {
			t.Errorf("markdown lacks %q:\n%s", want, md.String())
		}
	}

	c := Compare(baseRep, rep, DefaultScoreTolerance)
	if c.Regressions != 0 || c.Rows[0].Regressed || c.Overall.Regressed {
		t.Fatalf("an outage reads as a regression: %+v", c)
	}
	md.Reset()
	if err := c.WriteMarkdown(&md); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(md.String(), "REGRESSED") || !strings.Contains(md.String(), "infra") {
		t.Errorf("comparison:\n%s", md.String())
	}
	if pc := ComparePaired(baseRep, rep); pc.Pairs != 0 {
		t.Errorf("infra runs paired: %+v", pc)
	}
}

// A 429 is still retried by the SDK as before: no refusal, the run passes.
func TestEvaluateStillRetriesRateLimit(t *testing.T) {
	fake := newFakeModel(t, goodSeedTurns(seedCause))
	inner := fake.Config.Handler
	var first atomic.Bool
	fake.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first.CompareAndSwap(false, true) {
			w.Header().Set("retry-after-ms", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = fmt.Fprint(w, `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`)
			return
		}
		inner.ServeHTTP(w, r)
	})
	rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, endpointConfig(t, fake))
	if err != nil {
		t.Fatal(err)
	}
	if r := rep.Results[0]; !r.Pass || r.Infra() || rep.InfraStop != "" {
		t.Errorf("429 not retried: %+v", r)
	}
}

// A saved report with infra runs never reads as a regression either, and
// partly infra scenarios are scored over their other runs.
func TestCompareLeavesInfraOut(t *testing.T) {
	base := Report{Runs: 3, Scenarios: []ScenarioSummary{
		{Name: "a", Summary: Summary{Runs: 3, Passes: 3, PassRate: 1, MeanScore: 90}},
		{Name: "b", Summary: Summary{Runs: 3, Passes: 3, PassRate: 1, MeanScore: 90}},
	}}
	cand := Report{Runs: 3, Scenarios: []ScenarioSummary{
		{Name: "a", Summary: Summary{Infra: 3}},
		{Name: "b", Summary: Summary{Infra: 1, Runs: 2, Passes: 2, PassRate: 1, MeanScore: 90}},
	}}
	if c := Compare(base, cand, DefaultScoreTolerance); c.Regressions != 0 {
		t.Errorf("regressions = %d: %+v", c.Regressions, c.Rows)
	}
	rs := []RunResult{{Scenario: "a", Run: 1, Error: InfraPrefix + "x"}, {Scenario: "a", Run: 2, Pass: true, Score: 80}}
	if s := summarize(rs); s.Runs != 1 || s.Infra != 1 || s.Passes != 1 || s.PassRate != 1 || s.Errors != 0 {
		t.Errorf("summarize = %+v", s)
	}
}
