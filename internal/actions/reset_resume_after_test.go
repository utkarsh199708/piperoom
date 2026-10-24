package actions

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
)

// resumeAfterConnect is a stateful Kafka Connect REST stand-in for the
// stop -> reset(resume_after) path (KAN-514). It follows the KIP-875 rules the
// real client codes against: DELETE /offsets is refused unless the connector
// is STOPPED, PUT /stop moves it to STOPPED, PUT /resume moves it to RUNNING.
type resumeAfterConnect struct {
	mu         sync.Mutex
	state      string
	offsets    string // GET /offsets body
	resumeCode int    // 0 means 202
	resumeHang bool   // PUT /resume waits until the request ends
	writes     []string
}

func (c *resumeAfterConnect) handler(w http.ResponseWriter, r *http.Request) {
	const base = "/connectors/" + lifecycleConnector
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/connectors":
		w.Write([]byte(`["` + lifecycleConnector + `"]`))
	case r.Method == http.MethodGet && r.URL.Path == base+"/status":
		w.Write([]byte(`{"name":"` + lifecycleConnector + `","type":"source","connector":{"state":"` + c.state + `"},"tasks":[{"id":0,"state":"` + c.state + `"}]}`))
	case r.Method == http.MethodGet && r.URL.Path == base+"/config":
		w.Write([]byte(`{"connector.class":"` + pgSourceClass + `","snapshot.mode":"initial","topic.prefix":"orders"}`))
	case r.Method == http.MethodGet && r.URL.Path == base+"/offsets":
		w.Write([]byte(c.offsets))
	case r.Method == http.MethodPut && r.URL.Path == base+"/stop":
		c.writes = append(c.writes, "PUT stop")
		c.state = "STOPPED"
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodDelete && r.URL.Path == base+"/offsets":
		c.writes = append(c.writes, "DELETE offsets")
		if c.state != "STOPPED" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error_code":400,"message":"Connectors must be in the STOPPED state before their offsets can be modified."}`))
			return
		}
		c.offsets = `{"offsets":[{"partition":{"server":"orders"},"offset":null}]}`
		w.Write([]byte(`{"message":"The offsets for this connector have been reset successfully"}`))
	case r.Method == http.MethodPut && r.URL.Path == base+"/resume":
		c.writes = append(c.writes, "PUT resume")
		if c.resumeHang {
			c.mu.Unlock()
			<-r.Context().Done()
			c.mu.Lock()
			return
		}
		if c.resumeCode != 0 {
			w.WriteHeader(c.resumeCode)
			return
		}
		c.state = "RUNNING"
		w.WriteHeader(http.StatusAccepted)
	default:
		http.NotFound(w, r)
	}
}

func (c *resumeAfterConnect) writeLog() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.writes...)
}

func (c *resumeAfterConnect) currentState() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

const storedOrdersOffset = `{"offsets":[{"partition":{"server":"orders"},"offset":{"lsn":12345,"txId":7,"ts_usec":1700000000000000}}]}`

// newResumeAfter serves c over httptest and returns the three actions of the
// stale-offsets fix wired to the real connect client.
func newResumeAfter(t *testing.T, c *resumeAfterConnect, timeout time.Duration) (stop, reset, resume Action) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	t.Cleanup(srv.Close)
	cc := connect.New(srv.URL, srv.Client())
	ld := LifecycleDeps{Connect: cc, PollInterval: 5 * time.Millisecond, Timeout: timeout}
	return &stopConnector{d: ld}, &resetSourceOffsets{d: ld}, &resumeConnector{d: Deps{Connect: cc}}
}

const resetResumeIn = `{"connector":"pg-orders-connector","stop_first":true,"resume_after":true}`

// The KAN-514 proposal: stop, then reset with resume_after, validates as one
// proposal on a RUNNING connector and runs end to end.
func TestResetResumeAfterOneProposal(t *testing.T) {
	c := &resumeAfterConnect{state: "RUNNING", offsets: storedOrdersOffset}
	stop, reset, resume := newResumeAfter(t, c, 0)
	steps := []struct {
		a      Action
		params string
	}{
		{stop, `{"connector":"pg-orders-connector"}`},
		{reset, resetResumeIn},
	}
	for i, s := range steps {
		if err := s.a.Validate(scoped(), json.RawMessage(s.params)); err != nil {
			t.Fatalf("step %d (%s) Validate: %v", i, s.a.Name(), err)
		}
	}
	// The separate resume step this replaces is still refused at proposal
	// time: the connector is RUNNING when the proposal is checked.
	if err := resume.Validate(scoped(), json.RawMessage(`{"connector":"pg-orders-connector"}`)); err == nil || !strings.Contains(err.Error(), "RUNNING") {
		t.Fatalf("resume Validate on RUNNING = %v, want a refusal naming RUNNING", err)
	}
	if got := c.writeLog(); len(got) != 0 {
		t.Fatalf("Validate wrote: %v", got)
	}

	if _, err := stop.Execute(scoped(), json.RawMessage(steps[0].params)); err != nil {
		t.Fatalf("stop Execute: %v", err)
	}
	if err := reset.Validate(scoped(), json.RawMessage(resetResumeIn)); err != nil {
		t.Fatalf("reset re-Validate before Execute: %v", err)
	}
	out, err := reset.Execute(scoped(), json.RawMessage(resetResumeIn))
	if err != nil {
		t.Fatalf("reset Execute: %v", err)
	}
	for _, want := range []string{"cleared the stored offsets of pg-orders-connector", "resumed pg-orders-connector (RUNNING)"} {
		if !strings.Contains(out, want) {
			t.Fatalf("out %q, want %q", out, want)
		}
	}
	if got, want := strings.Join(c.writeLog(), ", "), "PUT stop, DELETE offsets, PUT resume"; got != want {
		t.Fatalf("writes %q, want %q", got, want)
	}
	if s := c.currentState(); s != "RUNNING" {
		t.Fatalf("final state %s, want RUNNING", s)
	}
}

// The executor still checks live state: without resume_after nothing resumes,
// and a reset whose stop never ran is refused before any write, resume_after
// or not. A lone resume of a RUNNING connector stays refused.
func TestResetResumeAfterLiveStateRefusals(t *testing.T) {
	cases := []struct {
		name       string
		state      string
		action     string // "reset" or "resume"
		params     string
		wantErr    error  // nil: no error expected
		wantText   string // substring of the error, when no sentinel
		wantWrites string
		wantState  string
	}{
		{"reset without resume_after leaves it STOPPED", "STOPPED", "reset", `{"connector":"pg-orders-connector","stop_first":true}`, nil, "", "DELETE offsets", "STOPPED"},
		{"resume_after on RUNNING, stop never ran", "RUNNING", "reset", resetResumeIn, ErrStopStepRequired, "", "", "RUNNING"},
		{"resume on RUNNING with no stop before it", "RUNNING", "resume", `{"connector":"pg-orders-connector"}`, nil, "resume only applies to PAUSED or STOPPED", "", "RUNNING"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &resumeAfterConnect{state: tc.state, offsets: storedOrdersOffset}
			_, reset, resume := newResumeAfter(t, c, 0)
			var err error
			if tc.action == "resume" {
				// The executor runs Validate right before Execute.
				err = resume.Validate(scoped(), json.RawMessage(tc.params))
			} else {
				_, err = reset.Execute(scoped(), json.RawMessage(tc.params))
			}
			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err %v, want %v", err, tc.wantErr)
				}
			case tc.wantText != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantText) {
					t.Fatalf("err %v, want %q", err, tc.wantText)
				}
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			}
			if got := strings.Join(c.writeLog(), ", "); got != tc.wantWrites {
				t.Fatalf("writes %q, want %q", got, tc.wantWrites)
			}
			if s := c.currentState(); s != tc.wantState {
				t.Fatalf("state %s, want %s", s, tc.wantState)
			}
		})
	}
}

// A failed resume after a successful reset is an error that says the reset
// happened, so a person resumes by hand instead of re-running the reset.
func TestResetResumeAfterResumeFails(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		hang    bool
		timeout time.Duration
		wantIs  error
	}{
		{"connect answers 404", http.StatusNotFound, false, 0, clients.ErrNotFound},
		{"connect answers 500", http.StatusInternalServerError, false, 0, nil},
		{"resume times out", 0, true, 300 * time.Millisecond, context.DeadlineExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &resumeAfterConnect{state: "STOPPED", offsets: storedOrdersOffset, resumeCode: tc.code, resumeHang: tc.hang}
			_, reset, _ := newResumeAfter(t, c, tc.timeout)
			out, err := reset.Execute(scoped(), json.RawMessage(resetResumeIn))
			if err == nil {
				t.Fatalf("Execute = %q, nil; want an error", out)
			}
			if !errors.Is(err, ErrResumeAfterReset) {
				t.Fatalf("err %v, want ErrResumeAfterReset", err)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("err %v, want it to wrap %v", err, tc.wantIs)
			}
			for _, want := range []string{"cleared the stored offsets of pg-orders-connector", "the reset succeeded but the resume failed", "PUT /connectors/pg-orders-connector/resume"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err %q, want %q", err, want)
				}
			}
			if got, want := strings.Join(c.writeLog(), ", "), "DELETE offsets, PUT resume"; got != want {
				t.Fatalf("writes %q, want %q", got, want)
			}
		})
	}
}

// A context that has already ended stops the step before any write.
func TestResetResumeAfterContextEnded(t *testing.T) {
	c := &resumeAfterConnect{state: "STOPPED", offsets: storedOrdersOffset}
	_, reset, _ := newResumeAfter(t, c, 0)
	ctx, cancel := context.WithTimeout(scoped(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if _, err := reset.Execute(ctx, json.RawMessage(resetResumeIn)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v, want context.DeadlineExceeded", err)
	}
	if got := c.writeLog(); len(got) != 0 {
		t.Fatalf("writes %v after the context ended", got)
	}
}

// resume_after is a strict boolean: anything else is refused by Validate and
// Execute before any write, and a misspelling is not silently dropped.
func TestResetResumeAfterBadParam(t *testing.T) {
	cases := []struct{ name, params string }{
		{"string", `{"connector":"pg-orders-connector","stop_first":true,"resume_after":"yes"}`},
		{"number", `{"connector":"pg-orders-connector","stop_first":true,"resume_after":1}`},
		{"object", `{"connector":"pg-orders-connector","stop_first":true,"resume_after":{}}`},
		{"misspelled", `{"connector":"pg-orders-connector","stop_first":true,"resume_afer":true}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &resumeAfterConnect{state: "STOPPED", offsets: storedOrdersOffset}
			_, reset, _ := newResumeAfter(t, c, 0)
			if err := reset.Validate(scoped(), json.RawMessage(tc.params)); err == nil || !strings.Contains(err.Error(), "bad params") {
				t.Fatalf("Validate err %v, want bad params", err)
			}
			if _, err := reset.Execute(scoped(), json.RawMessage(tc.params)); err == nil || !strings.Contains(err.Error(), "bad params") {
				t.Fatalf("Execute err %v, want bad params", err)
			}
			if got := c.writeLog(); len(got) != 0 {
				t.Fatalf("writes %v", got)
			}
		})
	}
}

// A client that can reset but not resume is refused before the reset, so an
// approved "reset then resume" never leaves the connector STOPPED half done.
func TestResetResumeAfterNoResumerWritesNothing(t *testing.T) {
	f := newLifecycleFake()
	f.allowWrites = true
	a := &resetSourceOffsets{d: LifecycleDeps{Connect: f, PollInterval: time.Millisecond}}
	_, err := a.Execute(scoped(), json.RawMessage(resetResumeIn))
	if !errors.Is(err, ErrNoLifecycleWriter) || !strings.Contains(err.Error(), "nothing was reset") {
		t.Fatalf("err %v, want ErrNoLifecycleWriter saying nothing was reset", err)
	}
	if len(f.reset) != 0 || f.refusedWrites() != 0 {
		t.Fatalf("reset %v, refused writes %d", f.reset, f.refusedWrites())
	}
}

// Validate refuses it too, at proposal create: by Execute an earlier stop step
// would already have run.
func TestResetResumeAfterNoResumerRefusedByValidate(t *testing.T) {
	f := newLifecycleFake()
	a := &resetSourceOffsets{d: LifecycleDeps{Connect: f, PollInterval: time.Millisecond}}
	if err := a.Validate(scoped(), json.RawMessage(resetResumeIn)); !errors.Is(err, ErrNoLifecycleWriter) {
		t.Fatalf("Validate err %v, want ErrNoLifecycleWriter", err)
	}
	if f.refusedWrites() != 0 {
		t.Fatalf("refused writes %d", f.refusedWrites())
	}
}

// Preview, the schema and the description tell the approver and the agent
// about resume_after.
func TestResetResumeAfterPreviewSchemaDescribe(t *testing.T) {
	c := &resumeAfterConnect{state: "RUNNING", offsets: storedOrdersOffset}
	_, reset, _ := newResumeAfter(t, c, 0)
	res, err := reset.(Preview).Preview(scoped(), json.RawMessage(resetResumeIn))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"then resume: PUT /connectors/pg-orders-connector/resume", "STOPPED to RUNNING", "to undo, connect_stop_connector pg-orders-connector first"} {
		if !strings.Contains(res.Expected, want) {
			t.Fatalf("Expected %q, want %q", res.Expected, want)
		}
	}
	if !res.Reversible || !strings.HasPrefix(res.Rollback, ResetSourceOffsetsName+" {") {
		t.Fatalf("rollback %q (reversible %v): must stay \"<action> {params}\"", res.Rollback, res.Reversible)
	}
	if !strings.Contains(strings.Join(res.Downstream, " "), "starts again right after the reset") {
		t.Fatalf("Downstream %v, want the restart named", res.Downstream)
	}
	plain, err := reset.(Preview).Preview(scoped(), json.RawMessage(`{"connector":"pg-orders-connector","stop_first":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain.Expected, "then resume") {
		t.Fatalf("Expected without resume_after promises a resume: %q", plain.Expected)
	}
	if got := c.writeLog(); len(got) != 0 {
		t.Fatalf("Preview wrote: %v", got)
	}

	props, _ := reset.ParamsSchema()["properties"].(map[string]any)
	ra, _ := props["resume_after"].(map[string]any)
	if ra["type"] != "boolean" {
		t.Fatalf("resume_after schema %v, want a boolean", ra)
	}
	if d := reset.Describe(); !strings.Contains(d, "resume_after: true") || !strings.Contains(d, "do not add a connect_resume_connector step") {
		t.Fatalf("Describe does not steer to resume_after: %q", d)
	}
}
