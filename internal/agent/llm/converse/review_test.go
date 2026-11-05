package converse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/llmtest"
	llmretry "piperoom/internal/agent/llm/retry"
)

// TestCredentialFailureIsAuth: credentials that cannot be retrieved are
// auth (unretryable), the request is never sent and never retried.
func TestCredentialFailureIsAuth(t *testing.T) {
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "never"}))
	cfg := testConfig(f.URL)
	cfg.AWS = &aws.Config{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{}, errors.New("no EC2 IMDS role found")
	})}
	calls := 0
	cfg.delay = func(int, error) time.Duration { calls++; return 0 }
	_, err := newModel(t, cfg).Turn(context.Background(), hi())
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrAuth || !llm.Unretryable(err) {
		t.Fatalf("err = %v, want unretryable auth", err)
	}
	if n := len(f.requests()); n != 0 || calls != 0 {
		t.Fatalf("requests %d, retries %d; want none", n, calls)
	}
}

// TestSharedRetryMaxAttemptsIgnored: AWS_MAX_ATTEMPTS in the shared config
// does not widen the design's bounds.
func TestSharedRetryMaxAttemptsIgnored(t *testing.T) {
	f := newFake(t, false, codeError(503, "ServiceUnavailableException", "busy"))
	cfg := testConfig(f.URL)
	cfg.AWS.RetryMaxAttempts = 10
	cfg.delay = func(int, error) time.Duration { return 0 }
	if _, err := newModel(t, cfg).Turn(context.Background(), hi()); err == nil {
		t.Fatal("no error")
	}
	if n := len(f.requests()); n != 3 {
		t.Fatalf("requests = %d, want 3", n)
	}
}

// TestCutBodyNotRetried: a 200 whose body ends mid-stream had started, so it
// is not retried (design 7.4) and is reported as network.
func TestCutBodyNotRetried(t *testing.T) {
	cut := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"output":{"message":{"role":"assistant","content":[{"text":"par`)
	}
	f := newFake(t, false, cut)
	cfg := testConfig(f.URL)
	cfg.delay = func(int, error) time.Duration { return 0 }
	_, err := newModel(t, cfg).Turn(context.Background(), hi())
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrNetwork {
		t.Fatalf("err = %v, want network", err)
	}
	if n := len(f.requests()); n != 1 {
		t.Fatalf("requests = %d, want 1 (not retried)", n)
	}
}

// TestForcedToolPerFamily: Nova gets toolChoice any; a family without forced
// tool choice gets the emit tool as the only tool and no toolChoice, both
// for a direct OutputSchema and for the agent's ToolChoiceRequired.
func TestForcedToolPerFamily(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"verdict": map[string]any{"type": "string"}}}
	for _, tc := range []struct {
		model  string
		forced bool
	}{
		{testModel, true},
		{"meta.llama3-3-70b-instruct-v1:0", false},
	} {
		t.Run(tc.model, func(t *testing.T) {
			f := newFake(t, false, answer(t, llmtest.Answer{Text: `{"verdict":"correct"}`, InputTokens: 600, OutputTokens: 5}))
			m := newModel(t, testConfig(f.URL))
			direct := llm.Request{Model: tc.model, Tools: []llm.ToolDef{lagTool}, OutputSchema: schema, Messages: []llm.Message{userText("grade")}}
			agentFallback := llm.Request{Model: tc.model, Tools: []llm.ToolDef{{Name: "emit_first_look", Schema: schema}}, ToolChoice: llm.ToolChoiceRequired, Messages: []llm.Message{userText("look")}}
			for _, req := range []llm.Request{direct, agentFallback} {
				if _, err := m.Turn(context.Background(), req); err != nil {
					t.Fatalf("Turn: %v", err)
				}
			}
			for i, r := range f.requests() {
				b := parseBody(t, r.body)
				if b.ToolConfig == nil {
					t.Fatalf("request %d: no toolConfig", i)
				}
				_, any := b.ToolConfig.ToolChoice["any"]
				if any != tc.forced || (!tc.forced && b.ToolConfig.ToolChoice != nil) {
					t.Fatalf("request %d: toolChoice = %v, forced %v", i, b.ToolConfig.ToolChoice, tc.forced)
				}
				if i == 0 {
					want := 2
					if !tc.forced {
						want = 1 // the emit tool alone
					}
					if len(b.ToolConfig.Tools) != want || !strings.Contains(string(b.ToolConfig.Tools[len(b.ToolConfig.Tools)-1]), outputToolName) {
						t.Fatalf("tools = %s", b.ToolConfig.Tools)
					}
				}
			}
		})
	}
}

// TestLimiter: the turn takes the endpoint's slot and releases it; a
// token-bucket wait past the deadline is rate_limited and not sent; a slot
// that does not free before the caller's deadline is network, not sent.
func TestLimiter(t *testing.T) {
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "ok", InputTokens: 40, OutputTokens: 1}))

	t.Run("slot", func(t *testing.T) {
		lim := llmretry.NewLimiter("https://bedrock-runtime.us-east-1.amazonaws.com", llmretry.Limits{MaxConcurrency: 1}, nil)
		cfg := testConfig(f.URL)
		cfg.Limiter = lim
		m := newModel(t, cfg)
		if _, err := m.Turn(context.Background(), hi()); err != nil {
			t.Fatalf("Turn: %v", err)
		}
		release, err := lim.Acquire(context.Background(), 1) // the first turn released its slot
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = m.Turn(ctx, hi())
		release(0)
		var ae *llm.APIError
		if !errors.As(err, &ae) || ae.Kind != llm.ErrNetwork || !errors.Is(err, llm.ErrNotSent) {
			t.Fatalf("err = %v, want network, not sent", err)
		}
	})

	t.Run("wait past deadline", func(t *testing.T) {
		big := newFake(t, false, answer(t, llmtest.Answer{Text: "ok", InputTokens: 4000, OutputTokens: 1}))
		cfg := testConfig(big.URL)
		cfg.Limiter = llmretry.NewLimiter("https://bedrock-runtime.us-east-1.amazonaws.com", llmretry.Limits{TPM: 60}, nil)
		m := newModel(t, cfg)
		// A turn larger than the bucket goes when it is full, and empties it.
		req := llm.Request{Model: testModel, Messages: []llm.Message{userText(strings.Repeat("lag ", 3000))}}
		if _, err := m.Turn(context.Background(), req); err != nil {
			t.Fatalf("first Turn: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := m.Turn(ctx, req)
		var ae *llm.APIError
		if !errors.As(err, &ae) || ae.Kind != llm.ErrRateLimited || !errors.Is(err, llm.ErrNotSent) || !errors.Is(err, llmretry.ErrWaitPastDeadline) {
			t.Fatalf("err = %v, want rate_limited, not sent", err)
		}
		if n := len(big.requests()); n != 1 {
			t.Fatalf("requests = %d, want 1: the second turn was sent", n)
		}
	})
}

func TestEstimateTokens(t *testing.T) {
	req := llm.Request{System: strings.Repeat("s", 30), Messages: []llm.Message{userText(strings.Repeat("u", 30))}}
	if got := estimateTokens(req); got != 20 {
		t.Fatalf("estimate = %d, want 20", got)
	}
}

// TestBaseEndpointChecked: only https, no user info, query or fragment, no
// metadata address or name; plain http only to loopback under the test hook.
func TestBaseEndpointChecked(t *testing.T) {
	for _, tc := range []struct {
		v        string
		loopback bool
		ok       bool
	}{
		{"https://vpce-1.bedrock-runtime.us-east-1.vpce.amazonaws.com", false, true},
		{"http://bedrock.internal.example", false, false},
		{"http://127.0.0.1:8080", false, false},
		{"http://127.0.0.1:8080", true, true},
		{"https://user:pw@bedrock.example", false, false},
		{"https://bedrock.example/?x=1", false, false},
		{"https://bedrock.example/#f", false, false},
		{"https://169.254.169.254", false, false},
		{"https://[fd00:ec2::254]", false, false},
		{"https://metadata.google.internal", false, false},
	} {
		_, err := checkBaseEndpoint(tc.v, tc.loopback)
		if (err == nil) != tc.ok {
			t.Errorf("%s (loopback %v): err = %v, want ok %v", tc.v, tc.loopback, err, tc.ok)
		}
		if err != nil && strings.Contains(err.Error(), "pw") {
			t.Errorf("the error echoes the value: %v", err)
		}
	}
	cfg := testConfig("https://169.254.169.254")
	if _, err := New(cfg); err == nil {
		t.Fatal("New accepted a metadata endpoint")
	}
}
