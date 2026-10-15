package room_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// outAction returns a fixed output or error, like a real action would.
type outAction struct {
	name string
	out  string
	err  error
}

func (a *outAction) Name() string                                    { return a.name }
func (a *outAction) Layer() string                                   { return "postgres" }
func (a *outAction) Describe() string                                { return a.name }
func (a *outAction) ParamsSchema() map[string]any                    { return map[string]any{"type": "object"} }
func (a *outAction) Validate(context.Context, json.RawMessage) error { return nil }
func (a *outAction) Execute(context.Context, json.RawMessage) (string, error) {
	return a.out, a.err
}
func (a *outAction) CheckScope(_ owners.Pipeline, _ json.RawMessage) error { return nil }

// recStore records the step outputs written to the store (memstore drops
// them), so a test can see what would reach the database.
type recStore struct {
	*memstore.Store
	mu  sync.Mutex
	out []string
}

func (r *recStore) FinishProposalStep(ctx context.Context, pid string, i int, status, output string) error {
	r.mu.Lock()
	r.out = append(r.out, output)
	r.mu.Unlock()
	return r.Store.FinishProposalStep(ctx, pid, i, status, output)
}

// syncBuf is a bytes.Buffer safe for the executor goroutine and the test.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// runMasked executes one approved step whose action returns out/err and
// returns everything observable: the action.executed output, the stored step
// output, and the captured log.
func runMasked(t *testing.T, out string, aerr error) (event, stored, logs string) {
	t.Helper()
	return runMaskedLimits(t, out, aerr, config.Limits{})
}

func runMaskedLimits(t *testing.T, out string, aerr error, lim config.Limits) (event, stored, logs string) {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	ms := memstore.NewWithClock(c.Now)
	st := &recStore{Store: ms}
	reg := actions.NewRegistry()
	if err := reg.Register(&outAction{name: "out_act", out: out, err: aerr}); err != nil {
		t.Fatal(err)
	}
	var buf syncBuf
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Owners: testOwners(),
		Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now, Limits: lim,
		Logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Risk: "low", Rollback: "revert the change", Title: "fix",
		Steps:          []events.ProposalStep{{Action: "out_act", Layer: "postgres", Params: json.RawMessage("{}")}},
		RequiredLayers: []string{"postgres"},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := ms.GetProposal(context.Background(), pid)
		return err == nil && (p.Status == "executed" || p.Status == "failed")
	})
	var raw strings.Builder
	for _, e := range allEvents(t, ms, roomID) {
		if e.Type == events.ActionExecuted {
			p, err := events.Decode[events.ActionExecutedPayload](e)
			if err != nil {
				t.Fatal(err)
			}
			event += p.Output
			raw.Write(e.Payload) // the broadcast form
		}
	}
	if !strings.Contains(raw.String(), "ok") {
		t.Fatalf("no action.executed payload seen: %q", raw.String())
	}
	st.mu.Lock()
	stored = strings.Join(st.out, "")
	st.mu.Unlock()
	// The payload as broadcast must equal the masked event text.
	event += "\n" + raw.String()
	return event, stored, buf.String()
}

func TestExecutorOutputIsMasked(t *testing.T) {
	const pw = "Zx9!hunter2-Qw7"
	aws := "AKIA" + "IOSFODNN7EXAMPLE"
	awsSecret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	bearer := "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r"
	basic := "Basic " + "dXNlcjpaeDkhaHVudGVyMi1RdzcK"
	cases := []struct {
		name   string
		out    string
		err    error
		secret []string // none of these may appear anywhere
	}{
		{"conn string", "connected to postgres://app:" + pw + "@db:5432/orders", nil, []string{pw}},
		{"jdbc", "jdbc:postgresql://db/orders?user=app&password=" + pw, nil, []string{pw}},
		{"bearer", "Authorization: " + bearer, nil, []string{"dBjftJeZ4CVPmB92K27uhbUJU1p1r"}},
		{"basic", "Authorization: " + basic, nil, []string{"dXNlcjpaeDkhaHVudGVyMi1RdzcK"}},
		{"aws", "key " + aws + " secret_access_key=" + awsSecret, nil, []string{aws, awsSecret}},
		{"json password", `{"password":"` + pw + `","user":"app"}`, nil, []string{pw}},
		{"json secret", `{"client_secret": "` + pw + `"}`, nil, []string{pw}},
		{"json token", `{"token":"` + pw + `x"}`, nil, []string{pw}},
		{"url encoded password in url", "postgres://app:Zx9%21hunter2-Qw7@db/orders", nil, []string{"Zx9%21hunter2-Qw7"}},
		{"base64 value", "password=Wng5IWh1bnRlcjItUXc3", nil, []string{"Wng5IWh1bnRlcjItUXc3"}},
		{"base64 basic", "Authorization: Basic YXBwOlp4OSFodW50ZXIyLVF3Nw==", nil, []string{"YXBwOlp4OSFodW50ZXIyLVF3Nw=="}},
		{"wrapped error", "", fmt.Errorf("connect: %w", fmt.Errorf("dial postgres://app:%s@db/orders: refused", pw)), []string{pw}},
		{"joined error", "", errors.Join(errors.New("a"), fmt.Errorf("password=%s", pw)), []string{pw}},
		{"error on failure path", "", errors.New(`{"password":"` + pw + `"}`), []string{pw}},
		{"nul inside", "password=" + "Zx9!hun\x00ter2-Qw7", nil, []string{"Zx9!hun"}},
		{"multi line", "step 1 ok\nstep 2\npassword=" + pw + "\nstep 3 done", nil, []string{pw}},
		{"private key", "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASC\nabcdefghijklmnop\n-----END PRIVATE KEY-----", nil, []string{"MIIEvQIBADANBgkqhkiG9w0BAQEFAASC", "abcdefghijklmnop"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, stored, logs := runMasked(t, tc.out, tc.err)
			for _, s := range tc.secret {
				for where, text := range map[string]string{"event": ev, "stored step": stored, "log": logs} {
					if strings.Contains(text, s) {
						t.Errorf("%s holds the secret %q:\n%s", where, s, text)
					}
				}
			}
		})
	}
}

// TestExecutorOutputKeepsEvidence guards against over-masking: LSNs, names
// and a plain result must survive.
func TestExecutorOutputKeepsEvidence(t *testing.T) {
	const out = "restarted task orders-cdc-0 at lsn 0/16B3748, topic orders.public.orders"
	ev, stored, _ := runMasked(t, out, nil)
	if !strings.Contains(ev, out) || stored != out {
		t.Fatalf("evidence was altered:\nevent: %s\nstored: %s", ev, stored)
	}
}

// TestExecutorOutputVeryLongIsMaskedAndBounded: a huge output with a secret at
// the start, in the middle and at the cap boundary never leaks, and finishes.
func TestExecutorOutputVeryLongIsMaskedAndBounded(t *testing.T) {
	const pw = "Zx9!hunter2-Qw7"
	filler := strings.Repeat("row ok\n", 20000) // ~140 KB
	out := "password=" + pw + "\n" + filler + "secret=" + pw + "\n" + filler + "token=" + pw
	done := make(chan struct{})
	var ev, stored, logs string
	go func() {
		defer close(done)
		ev, stored, logs = runMasked(t, out, nil)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("masking a very long output did not finish")
	}
	for where, text := range map[string]string{"event": ev, "stored": stored, "log": logs} {
		if strings.Contains(text, pw) {
			t.Errorf("%s holds the secret", where)
		}
	}
}

// The cases below were leaks in internal/redact, found while writing these
// tests and closed by #217. They fail now if the executor's output leaks again.
func TestExecutorOutputFormerRedactorGaps(t *testing.T) {
	cases := []struct{ name, out, secret string }{
		{"url-encoded key", "password%3DZx9%21hunter2-Qw7", "Zx9%21hunter2-Qw7"},
		{"secret split across a line break", "password=Zx9!hun\nter2-Qw7", "ter2-Qw7"},
		{"quoted value continued with backslash-newline", "password=\"Zx9!hunter2-\\\nQw7\"", "Qw7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, _, _ := runMasked(t, tc.out, nil)
			if strings.Contains(ev, tc.secret) {
				t.Errorf("secret %q survives masking in the event", tc.secret)
			}
		})
	}
}

// TestExecutorMasksBeforeTheEventCap slides a secret across the event size cap.
// Masking after the cap would leave the secret's first half (before the cut)
// in the event; masking first keeps only the mask.
func TestExecutorMasksBeforeTheEventCap(t *testing.T) {
	const pw = "Zx9!hunter2-Qw7"
	const capBytes = 1024
	for off := capBytes - 120; off <= capBytes-20; off += 7 {
		out := strings.Repeat("x", off) + " password=" + pw + " tail"
		ev, _, _ := runMaskedLimits(t, out, nil, config.Limits{EventMaxDataBytes: capBytes})
		if strings.Contains(ev, "Zx9!") || strings.Contains(ev, "hunter2") {
			t.Fatalf("offset %d: secret (or its first half) reached the event:\n%.1500s", off, ev)
		}
	}
}
