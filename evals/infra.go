package evals

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"piperoom/internal/agent/llm"
	"piperoom/internal/redact"
)

// ErrAPIRefused is wrapped by the error Evaluate returns when the model API
// refused a request for a reason a retry cannot fix (an empty credit balance,
// a bad or revoked key, no access). Evaluate then starts no new run and marks
// the affected and remaining runs infra; evalrun exits 3.
var ErrAPIRefused = errors.New("evals: the model API refused the request")

// InfraPrefix starts RunResult.Error of a run that never measured the agent
// because the model API was unavailable to it. RunResult is score.go's, so the
// marker lives in the Error text (kept in the JSON report): Infra reads it.
const InfraPrefix = "infra: "

// MaxRunErrorRunes bounds the run error text printed in logs and reports.
const MaxRunErrorRunes = 200

// Infra reports whether the run is an infrastructure failure, not a result:
// it is not scored, not compared and not counted in a pass rate.
func (r RunResult) Infra() bool { return strings.HasPrefix(r.Error, InfraPrefix) }

// APIRefusal reports whether err is the model API refusing the request for a
// reason that no retry or later run can fix, and the HTTP status. It decides
// on the adapter's classification (*llm.APIError), never on a provider's
// body: an auth error (401, a key without access) and a billing error (an
// empty credit balance, an exhausted quota) are refusals. A 403 is one
// whatever its kind: the adapter calls a 403 that names the model
// model_not_found so the room's triage switch can turn a triage model off,
// but for an evaluation it is still the key's access. Other 400s are about one
// request; rate limits, overload and network failures were retried by the
// adapter and are never refusals here.
func APIRefusal(err error) (status int, ok bool) {
	var ae *llm.APIError
	if !errors.As(err, &ae) {
		return 0, false
	}
	switch {
	case ae.Kind == llm.ErrAuth, ae.Kind == llm.ErrBilling:
		return ae.Status, true
	case ae.Status == http.StatusForbidden:
		return ae.Status, true
	}
	return ae.Status, false
}

// RunErrorText is a run error made safe and short for a log line or a report:
// redacted (it can echo model or server text), on one line, at most
// MaxRunErrorRunes runes.
func RunErrorText(s string) string {
	s = strings.TrimPrefix(s, InfraPrefix)
	s, _ = redact.Text(s)
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > MaxRunErrorRunes {
		s = string(r[:MaxRunErrorRunes]) + "…"
	}
	return s
}

// infraResult is the result of a run the API refusal kept from being scored.
func infraResult(scenario string, run int, why string) RunResult {
	return RunResult{
		Scenario: scenario, Run: run, ToolCallsToRootCause: -1,
		Error:    InfraPrefix + RunErrorText(why),
		Failures: []string{"infra: not scored"},
	}
}

// apiStop is the first refusal an evaluation saw, for the report and the error.
type apiStop struct {
	status int
	text   string
}

func (s apiStop) String() string {
	return fmt.Sprintf("HTTP %d: %s", s.status, RunErrorText(s.text))
}

// refusalLatch keeps the first API refusal of an evaluation; its workers read
// it before starting a run.
type refusalLatch struct {
	mu   sync.Mutex
	stop *apiStop
}

func (l *refusalLatch) set(status int, err error) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stop == nil {
		l.stop = &apiStop{status: status, text: err.Error()}
	}
}

func (l *refusalLatch) get() *apiStop {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stop
}
