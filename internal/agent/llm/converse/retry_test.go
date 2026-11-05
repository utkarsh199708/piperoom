package converse

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/llmtest"
)

// delays records the waits the SDK retryer asks for, computed by the
// production schedule from the real error, and waits for none of them.
type delays struct {
	mu   sync.Mutex
	got  []time.Duration
	atts []int
}

func (d *delays) hook(attempt int, err error) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.atts = append(d.atts, attempt)
	d.got = append(d.got, scheduleDelay(attempt, err))
	return 0
}

func retryModel(t *testing.T, url string, d *delays) *Model {
	t.Helper()
	cfg := testConfig(url) // attempts 0: the design's bounds
	cfg.delay = d.hook
	return newModel(t, cfg)
}

func hi() llm.Request {
	return llm.Request{Model: testModel, Messages: []llm.Message{userText("hi")}}
}

// TestThrottlingRetriedWithinBounds: a ThrottlingException is retried at
// most twice (three requests), 1 s then 4 s with jitter; a third failure is
// rate_limited.
func TestThrottlingRetriedWithinBounds(t *testing.T) {
	throttle := codeError(http.StatusTooManyRequests, "ThrottlingException", "Too many requests, please wait before trying again.")

	t.Run("recovers", func(t *testing.T) {
		d := &delays{}
		f := newFake(t, false, throttle, throttle, answer(t, llmtest.Answer{Text: "ok", InputTokens: 40, OutputTokens: 2}))
		resp, err := retryModel(t, f.URL, d).Turn(context.Background(), hi())
		if err != nil || textOfParts(resp.Message.Parts) != "ok" {
			t.Fatalf("resp %+v, err %v", resp, err)
		}
		if n := len(f.requests()); n != 3 {
			t.Fatalf("requests = %d, want 3", n)
		}
		checkSchedule(t, d)
	})

	t.Run("gives up after two retries", func(t *testing.T) {
		d := &delays{}
		f := newFake(t, false, throttle)
		_, err := retryModel(t, f.URL, d).Turn(context.Background(), hi())
		var ae *llm.APIError
		if !errors.As(err, &ae) || ae.Kind != llm.ErrRateLimited || ae.Status != 429 {
			t.Fatalf("err = %v, want rate_limited", err)
		}
		if n := len(f.requests()); n != 3 {
			t.Fatalf("requests = %d, want 3 (two retries)", n)
		}
		checkSchedule(t, d)
	})
}

func textOfParts(ps []llm.Part) string {
	s := ""
	for _, p := range ps {
		if p.Kind == llm.PartText {
			s += p.Text
		}
	}
	return s
}

func checkSchedule(t *testing.T, d *delays) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.got) != 2 || d.atts[0] != 1 || d.atts[1] != 2 {
		t.Fatalf("retries = %v (attempts %v), want two", d.got, d.atts)
	}
	if d.got[0] < 800*time.Millisecond || d.got[0] > 1200*time.Millisecond {
		t.Fatalf("first wait = %s, want 1 s ±20%%", d.got[0])
	}
	if d.got[1] < 3200*time.Millisecond || d.got[1] > 4800*time.Millisecond {
		t.Fatalf("second wait = %s, want 4 s ±20%%", d.got[1])
	}
}

// TestRetryStatuses: 5xx in the design's list are retried; a quota, a daily
// token limit, a validation error and a 424 model error are not.
func TestRetryStatuses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		reply    http.HandlerFunc
		requests int
		kind     llm.ErrorKind
	}{
		{"503", codeError(503, "ServiceUnavailableException", "busy"), 3, llm.ErrOverloaded},
		{"500", codeError(500, "InternalServerException", "oops"), 3, llm.ErrOverloaded},
		{"408 model timeout", codeError(408, "ModelTimeoutException", "slow"), 3, llm.ErrOverloaded},
		{"quota", codeError(400, "ServiceQuotaExceededException", "quota exceeded"), 1, llm.ErrBilling},
		{"daily tokens", codeError(429, "ThrottlingException", "Too many tokens per day, please wait before trying again."), 1, llm.ErrBilling},
		{"validation", codeError(400, "ValidationException", "bad"), 1, llm.ErrBadRequest},
		{"model error", codeError(424, "ModelErrorException", "model failed"), 1, llm.ErrOverloaded},
		{"auth", codeError(403, "UnrecognizedClientException", "bad token"), 1, llm.ErrAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, false, tc.reply)
			_, err := retryModel(t, f.URL, &delays{}).Turn(context.Background(), hi())
			var ae *llm.APIError
			if !errors.As(err, &ae) || ae.Kind != tc.kind {
				t.Fatalf("err = %v, want kind %s", err, tc.kind)
			}
			if n := len(f.requests()); n != tc.requests {
				t.Fatalf("requests = %d, want %d", n, tc.requests)
			}
		})
	}
}

// TestRetryAfter: the answer's Retry-After is honoured, capped at 60 s.
func TestRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{
		{"2", 2 * time.Second},
		{"120", 60 * time.Second},
	} {
		d := &delays{}
		_, body := renderCodeError("ThrottlingException", "slow down")
		f := newFake(t, false, jsonReply(429, string(body), "Retry-After", tc.header), answer(t, llmtest.Answer{Text: "ok", InputTokens: 40, OutputTokens: 1}))
		if _, err := retryModel(t, f.URL, d).Turn(context.Background(), hi()); err != nil {
			t.Fatalf("Turn: %v", err)
		}
		if len(d.got) != 1 || d.got[0] != tc.want {
			t.Fatalf("Retry-After %s: waits %v, want [%s]", tc.header, d.got, tc.want)
		}
	}
}

// TestNoRetryPastTheDeadline: a wait longer than the caller's deadline ends
// with the deadline, not after the wait.
func TestNoRetryPastTheDeadline(t *testing.T) {
	f := newFake(t, false, codeError(503, "ServiceUnavailableException", "busy"))
	cfg := testConfig(f.URL)
	cfg.delay = func(int, error) time.Duration { return time.Minute }
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := newModel(t, cfg).Turn(ctx, hi())
	if err == nil {
		t.Fatal("no error")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Turn took %s past a 300 ms deadline", took)
	}
	if n := len(f.requests()); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
}

// TestConnectionErrorRetried: no answer at all (connection refused) is
// retried within the same bounds, then reported as network.
func TestConnectionErrorRetried(t *testing.T) {
	f := newFake(t, false, answer(t, llmtest.Answer{Text: "never"}))
	url := f.URL
	f.Close()
	d := &delays{}
	_, err := retryModel(t, url, d).Turn(context.Background(), hi())
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrNetwork {
		t.Fatalf("err = %v, want network", err)
	}
	if len(d.got) != 2 {
		t.Fatalf("retries = %d, want 2", len(d.got))
	}
}

func TestScheduleJitterBounds(t *testing.T) {
	plain := errors.New("no answer")
	for i := 0; i < 200; i++ {
		if d := scheduleDelay(1, plain); d < 800*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatalf("attempt 1 wait %s", d)
		}
		if d := scheduleDelay(2, plain); d < 3200*time.Millisecond || d > 4800*time.Millisecond {
			t.Fatalf("attempt 2 wait %s", d)
		}
	}
}
