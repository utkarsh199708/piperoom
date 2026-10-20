package room_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

// auditRecord is one emitted setup.* audit event.
type auditRecord struct {
	roomID  string
	typ     events.Type
	payload any
}

// auditRecorder captures the registry's audit sink for tests.
type auditRecorder struct {
	mu  sync.Mutex
	evs []auditRecord
}

func (a *auditRecorder) emit(_ context.Context, roomID string, typ events.Type, payload any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.evs = append(a.evs, auditRecord{roomID: roomID, typ: typ, payload: payload})
	return nil
}

func (a *auditRecorder) last() (auditRecord, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.evs) == 0 {
		return auditRecord{}, false
	}
	return a.evs[len(a.evs)-1], true
}

func (a *auditRecorder) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.evs)
}

func newRegistry(t *testing.T) (*room.RunnerRegistry, *auditRecorder, *clock) {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	rec := &auditRecorder{}
	return room.NewRunnerRegistry(c.Now, rec.emit, nil), rec, c
}

var testCreator = events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}

// TestPairingCodeShapeAndSingleUse: a code is 8 characters from the alphabet,
// pairs once, and is consumed by Attach.
func TestPairingCodeShapeAndSingleUse(t *testing.T) {
	reg, _, _ := newRegistry(t)
	ctx := context.Background()

	code, err := reg.IssuePairingCode("", "r1", testCreator)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if len(code) != 8 {
		t.Fatalf("code length = %d, want 8", len(code))
	}
	for _, ch := range code {
		if !strings.ContainsRune("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", ch) {
			t.Fatalf("code %q has a character outside the alphabet", code)
		}
	}

	res, err := reg.PairCode(ctx, code, "10.0.0.1")
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	if res.Outcome != events.PairingOK || res.RoomID != "r1" || res.Creator.ID != "arjun" {
		t.Fatalf("pair result = %+v", res)
	}

	if err := reg.Attach(ctx, code, room.Runner{
		Tenant: "", RoomID: "r1", Creator: testCreator, Label: "laptop",
		OS: "linux", Arch: "amd64", ProtocolVersion: 1,
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// Single use: the code is gone after Attach.
	res, err = reg.PairCode(ctx, code, "10.0.0.2")
	if err != nil {
		t.Fatalf("second pair: %v", err)
	}
	if res.Outcome != events.PairingBadCode {
		t.Fatalf("second pair outcome = %q, want bad_code", res.Outcome)
	}
}

// TestPairingCodeMaxAttempts: the sixth attempt on one code is rate_limited.
func TestPairingCodeMaxAttempts(t *testing.T) {
	reg, _, _ := newRegistry(t)
	ctx := context.Background()
	code, err := reg.IssuePairingCode("", "r1", testCreator)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	for i := 0; i < 5; i++ {
		res, err := reg.PairCode(ctx, code, "10.0.0.1")
		if err != nil {
			t.Fatalf("pair %d: %v", i, err)
		}
		if res.Outcome != events.PairingOK {
			t.Fatalf("pair %d outcome = %q, want ok", i, res.Outcome)
		}
	}
	res, err := reg.PairCode(ctx, code, "10.0.0.1")
	if err != nil {
		t.Fatalf("sixth pair: %v", err)
	}
	if res.Outcome != events.PairingRateLimited {
		t.Fatalf("sixth pair outcome = %q, want rate_limited", res.Outcome)
	}
}

// TestPairingCodeExpires: after protocol.CodeTTL the code answers expired.
func TestPairingCodeExpires(t *testing.T) {
	reg, _, c := newRegistry(t)
	ctx := context.Background()
	code, err := reg.IssuePairingCode("", "r1", testCreator)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	c.Advance(10*time.Minute + time.Second)
	res, err := reg.PairCode(ctx, code, "10.0.0.1")
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	if res.Outcome != events.PairingExpired {
		t.Fatalf("expired outcome = %q, want expired", res.Outcome)
	}
}

// TestPairingSourceLimit: a source address is limited to 20 attempts per 10
// minutes; the 21st is rate_limited.
func TestPairingSourceLimit(t *testing.T) {
	reg, _, _ := newRegistry(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		res, err := reg.PairCode(ctx, "ZZZZZZZZ", "10.0.0.1")
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		if res.Outcome != events.PairingBadCode {
			t.Fatalf("attempt %d outcome = %q, want bad_code", i, res.Outcome)
		}
	}
	res, err := reg.PairCode(ctx, "ZZZZZZZZ", "10.0.0.1")
	if err != nil {
		t.Fatalf("21st: %v", err)
	}
	if res.Outcome != events.PairingRateLimited {
		t.Fatalf("21st outcome = %q, want rate_limited", res.Outcome)
	}
}

// TestPairingGlobalLimitClosesPairing: the 101st attempt in 10 minutes closes
// pairing for 10 minutes and is audited with the source; pairing resumes after
// the close window.
func TestPairingGlobalLimitClosesPairing(t *testing.T) {
	reg, rec, c := newRegistry(t)
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		res, err := reg.PairCode(ctx, "ZZZZZZZZ", fmt.Sprintf("10.0.%d.%d", i/250, i%250))
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		if res.Outcome != events.PairingBadCode {
			t.Fatalf("attempt %d outcome = %q, want bad_code", i, res.Outcome)
		}
	}

	// The 101st attempt closes pairing and is audited with the source.
	res, err := reg.PairCode(ctx, "ZZZZZZZZ", "10.9.9.9")
	if err != nil {
		t.Fatalf("101st: %v", err)
	}
	if res.Outcome != events.PairingRateLimited {
		t.Fatalf("101st outcome = %q, want rate_limited", res.Outcome)
	}
	last, ok := rec.last()
	if !ok || last.typ != events.SetupPairing {
		t.Fatalf("101st attempt not audited: %+v", last)
	}
	p, _ := last.payload.(events.SetupPairingPayload)
	if p.Outcome != events.PairingRateLimited || p.Source != "10.9.9.9" {
		t.Fatalf("101st audit = %+v, want rate_limited with the source", p)
	}

	// While closed, even a valid code is refused.
	code, err := reg.IssuePairingCode("", "r1", testCreator)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	res, err = reg.PairCode(ctx, code, "10.9.9.9")
	if err != nil {
		t.Fatalf("pair while closed: %v", err)
	}
	if res.Outcome != events.PairingRateLimited {
		t.Fatalf("pair while closed = %q, want rate_limited", res.Outcome)
	}

	// After the close window pairing resumes.
	c.Advance(10 * time.Minute)
	code2, err := reg.IssuePairingCode("", "r1", testCreator)
	if err != nil {
		t.Fatalf("issue 2: %v", err)
	}
	res, err = reg.PairCode(ctx, code2, "10.9.9.9")
	if err != nil {
		t.Fatalf("pair after close: %v", err)
	}
	if res.Outcome != events.PairingOK {
		t.Fatalf("pair after close = %q, want ok", res.Outcome)
	}
}

// TestRunnerLostEmitsEvent: Lost detaches the runner and emits
// setup.runner_lost with its label.
func TestRunnerLostEmitsEvent(t *testing.T) {
	reg, rec, _ := newRegistry(t)
	ctx := context.Background()
	code, err := reg.IssuePairingCode("", "r1", testCreator)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := reg.PairCode(ctx, code, "10.0.0.1"); err != nil {
		t.Fatalf("pair: %v", err)
	}
	if err := reg.Attach(ctx, code, room.Runner{
		Tenant: "", RoomID: "r1", Creator: testCreator, Label: "laptop",
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	reg.Lost(ctx, "", "r1", "arjun")

	last, ok := rec.last()
	if !ok || last.typ != events.SetupRunnerLost {
		t.Fatalf("lost not audited: %+v", last)
	}
	p, _ := last.payload.(events.SetupRunnerLostPayload)
	if p.RunnerLabel != "laptop" {
		t.Fatalf("lost label = %q, want laptop", p.RunnerLabel)
	}
	if _, ok := reg.Runner("", "r1", "arjun"); ok {
		t.Fatal("runner still attached after Lost")
	}
}
