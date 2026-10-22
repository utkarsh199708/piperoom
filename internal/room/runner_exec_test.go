package room_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/store/memstore"
)

// captureWriter is a fake runner socket: it records every frame and can answer
// nothing (the test drives results through the executor).
type captureWriter struct {
	mu     sync.Mutex
	frames []protocol.Frame
	fail   bool
}

func (w *captureWriter) WriteRunnerFrame(_ context.Context, f protocol.Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fail {
		return errors.New("fake writer: gone")
	}
	w.frames = append(w.frames, f)
	return nil
}

func (w *captureWriter) opFrames() []protocol.OpRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []protocol.OpRequest
	for _, f := range w.frames {
		if f.Type == protocol.FrameOp && f.Op != nil {
			out = append(out, *f.Op)
		}
	}
	return out
}

func (w *captureWriter) cancelFrames() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, f := range w.frames {
		if f.Type == protocol.FrameCancel {
			n++
		}
	}
	return n
}

// resultsRecorder captures the executor's orphan outcomes.
type resultsRecorder struct {
	mu      sync.Mutex
	results []protocol.OpResult
	unknown []protocol.OpRequest
}

func (r *resultsRecorder) result(_ context.Context, _ string, _ protocol.OpRequest, res protocol.OpResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, res)
}

func (r *resultsRecorder) unknownOp(_ context.Context, _ string, req protocol.OpRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unknown = append(r.unknown, req)
}

func (r *resultsRecorder) counts() (results, unknown int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.results), len(r.unknown)
}

// execFixture is a registry, one attached and bound runner, and an executor.
type execFixture struct {
	reg *room.RunnerRegistry
	w   *captureWriter
	rec *resultsRecorder
	ex  *room.RunnerExecutor
}

func newExecFixture(t *testing.T) *execFixture {
	t.Helper()
	reg := room.NewRunnerRegistry(nil, nil, slog.New(slog.NewTextHandler(&strings.Builder{}, nil)))
	if err := reg.Attach(context.Background(), "CODE1234", room.Runner{
		Tenant: "", RoomID: "r1", Creator: testCreator, Label: "laptop", Token: "tok-a",
		TokenExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	w := &captureWriter{}
	if !reg.BindWriter("", "r1", "arjun", "tok-a", w) {
		t.Fatal("bind writer refused")
	}
	rec := &resultsRecorder{}
	ex := room.NewRunnerExecutor(reg, room.RunnerResults{Result: rec.result, Unknown: rec.unknownOp}, nil)
	return &execFixture{reg: reg, w: w, rec: rec, ex: ex}
}

// stepReq builds a T1 op request for one proposal step.
func stepReq(id, proposal string, step int) protocol.OpRequest {
	idx := step
	return protocol.OpRequest{
		ID: id, Op: "compose_up", Params: []byte(`{}`), Tier: protocol.TierT1,
		Class: protocol.ClassLocal, Deadline: time.Now().Add(10 * time.Second),
		ProposalID: proposal, StepIndex: &idx, ParamsHash: "hash",
	}
}

// waitOp waits until the fake writer saw n op frames.
func waitOp(t *testing.T, w *captureWriter, n int) []protocol.OpRequest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ops := w.opFrames(); len(ops) >= n {
			return ops
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("runner never saw %d op frame(s)", n)
	return nil
}

// TestExecuteNoRunner: without an attached runner Execute answers ErrNoRunner.
func TestExecuteNoRunner(t *testing.T) {
	ex := room.NewRunnerExecutor(room.NewRunnerRegistry(nil, nil, nil), room.RunnerResults{}, nil)
	_, err := ex.Execute(context.Background(), "r1", stepReq("op1", "p1", 0))
	if !errors.Is(err, protocol.ErrNoRunner) {
		t.Fatalf("Execute without a runner = %v, want ErrNoRunner", err)
	}
}

// TestExecuteOnAnotherReplicaIsNoRunner pins the replica decision for U1-5a:
// the socket lives on one replica, and Execute from a replica that does not
// hold it refuses with ErrNoRunner instead of routing (docs/design/
// replicas.md §3). The refusal is clear and never runs the op twice.
func TestExecuteOnAnotherReplicaIsNoRunner(t *testing.T) {
	st := memstore.New()
	users := room.DefaultUsers()
	a := room.New(room.Deps{Store: st, Users: users, Run: fakeRun})
	b := room.New(room.Deps{Store: st, Users: users, Run: fakeRun})
	ctx := context.Background()
	arjun, ok := a.LookupUser("arjun")
	if !ok {
		t.Fatal("arjun missing")
	}
	snap, err := a.CreateRoom(ctx, arjun, "Setup", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	if err := a.AttachRunner(ctx, "CODE1234", room.Runner{
		Tenant: "", RoomID: snap.ID, Creator: testCreator, Label: "laptop",
		Token: "tok-a", TokenExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if !a.BindRunnerWriter("", snap.ID, "arjun", "tok-a", &captureWriter{}) {
		t.Fatal("bind writer refused")
	}
	if _, err := b.RunnerExecutor().Execute(ctx, snap.ID, stepReq("op1", "p1", 0)); !errors.Is(err, protocol.ErrNoRunner) {
		t.Fatalf("Execute on the other replica = %v, want ErrNoRunner", err)
	}
}

// TestExecuteResultDeliveredOnce: the waiting Execute gets the result once; a
// duplicate result for the same op is ignored and never reaches the orphan
// recorder.
func TestExecuteResultDeliveredOnce(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type out struct {
		res protocol.OpResult
		err error
	}
	ch := make(chan out, 1)
	go func() {
		res, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
		ch <- out{res, err}
	}()
	waitOp(t, f.w, 1)

	f.ex.DeliverResult(ctx, "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "ok"})
	got := <-ch
	if got.err != nil || !got.res.OK || got.res.Summary != "ok" {
		t.Fatalf("Execute = %+v, %v", got.res, got.err)
	}

	// The duplicate is ignored: no second delivery, no orphan recording.
	f.ex.DeliverResult(ctx, "r1", protocol.OpResult{ID: "op1", OK: false, Summary: "dup"})
	if results, unknown := f.rec.counts(); results != 0 || unknown != 0 {
		t.Fatalf("duplicate result recorded: results=%d unknown=%d", results, unknown)
	}
	if n := len(f.w.opFrames()); n != 1 {
		t.Fatalf("op frames = %d, want 1", n)
	}
}

// TestExecuteResultAfterDeadlineRecordedOnce: a result that arrives after its
// Execute call ended is recorded exactly once, and a duplicate is ignored.
func TestExecuteResultAfterDeadlineRecordedOnce(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
		done <- err
	}()
	waitOp(t, f.w, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute = %v, want context.Canceled", err)
	}

	f.ex.DeliverResult(context.Background(), "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "ok"})
	f.ex.DeliverResult(context.Background(), "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "ok again"})
	if results, unknown := f.rec.counts(); results != 1 || unknown != 0 {
		t.Fatalf("orphan results = %d unknown = %d, want 1 and 0", results, unknown)
	}
}

// TestExecuteUnknownResultIgnored: a result for an op this executor never sent
// changes nothing.
func TestExecuteUnknownResultIgnored(t *testing.T) {
	f := newExecFixture(t)
	f.ex.DeliverResult(context.Background(), "r1", protocol.OpResult{ID: "never-sent", OK: true, Summary: "x"})
	if results, unknown := f.rec.counts(); results != 0 || unknown != 0 {
		t.Fatalf("unknown result recorded: results=%d unknown=%d", results, unknown)
	}
}

// TestExecuteCancelSendsCancel: a deadline while the runner is still attached
// ends Execute with ctx's error and asks the runner to stop the op.
func TestExecuteCancelSendsCancel(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute = %v, want deadline exceeded", err)
	}
	if n := f.w.cancelFrames(); n != 1 {
		t.Fatalf("cancel frames = %d, want 1", n)
	}
}

// TestExecuteLostRunner: a runner that is not attached when the request ends
// answers ErrRunnerLost together with ctx's error; the step is unknown.
func TestExecuteLostRunner(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
		done <- err
	}()
	waitOp(t, f.w, 1)
	f.reg.Lost(context.Background(), "", "r1", "arjun", "tok-a")
	cancel()
	err := <-done
	if !errors.Is(err, room.ErrRunnerLost) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute = %v, want ErrRunnerLost and context.Canceled", err)
	}
}

// TestResumeDoneNeverRerun: a done op reported in Resume is recorded once and
// never re-sent, even when a second Resume names it again.
func TestResumeDoneNeverRerun(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	req := stepReq("op1", "p1", 0)
	go func() {
		_, err := f.ex.Execute(ctx, "r1", req)
		done <- err
	}()
	waitOp(t, f.w, 1)
	cancel()
	<-done

	res := protocol.Resume{InFlight: []protocol.OpStatus{{
		ID: "op1", ProposalID: "p1", StepIndex: req.StepIndex, State: protocol.StateDone,
		Result: &protocol.OpResult{ID: "op1", OK: true, Summary: "done"},
	}}}
	f.ex.ReconcileResume(context.Background(), "r1", res)
	f.ex.ReconcileResume(context.Background(), "r1", res)

	if results, unknown := f.rec.counts(); results != 1 || unknown != 0 {
		t.Fatalf("resume done recorded %d results, %d unknown; want 1 and 0", results, unknown)
	}
	if n := len(f.w.opFrames()); n != 1 {
		t.Fatalf("done op re-sent: op frames = %d, want 1", n)
	}
}

// TestResumeRunningIsUnknownNotRerun: a running op whose Execute is gone is
// recorded as unknown once and never re-sent.
func TestResumeRunningIsUnknownNotRerun(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	req := stepReq("op1", "p1", 0)
	go func() {
		_, err := f.ex.Execute(ctx, "r1", req)
		done <- err
	}()
	waitOp(t, f.w, 1)
	cancel()
	<-done

	status := protocol.OpStatus{ID: "op1", ProposalID: "p1", StepIndex: req.StepIndex, State: protocol.StateRunning}
	f.ex.ReconcileResume(context.Background(), "r1", protocol.Resume{InFlight: []protocol.OpStatus{status}})
	f.ex.ReconcileResume(context.Background(), "r1", protocol.Resume{InFlight: []protocol.OpStatus{status}})

	if results, unknown := f.rec.counts(); results != 0 || unknown != 1 {
		t.Fatalf("resume running recorded %d results, %d unknown; want 0 and 1", results, unknown)
	}
	if n := len(f.w.opFrames()); n != 1 {
		t.Fatalf("running op re-sent: op frames = %d, want 1", n)
	}
}

// TestResumeRunningKeepsWaiter: while an Execute still waits, Resume running is
// not an outcome; the result delivered afterwards completes the call.
func TestResumeRunningKeepsWaiter(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type out struct {
		res protocol.OpResult
		err error
	}
	ch := make(chan out, 1)
	req := stepReq("op1", "p1", 0)
	go func() {
		res, err := f.ex.Execute(ctx, "r1", req)
		ch <- out{res, err}
	}()
	waitOp(t, f.w, 1)

	f.ex.ReconcileResume(ctx, "r1", protocol.Resume{InFlight: []protocol.OpStatus{{
		ID: "op1", ProposalID: "p1", StepIndex: req.StepIndex, State: protocol.StateRunning,
	}}})
	if results, unknown := f.rec.counts(); results != 0 || unknown != 0 {
		t.Fatalf("running op with a waiter recorded: results=%d unknown=%d", results, unknown)
	}
	f.ex.DeliverResult(ctx, "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "late"})
	got := <-ch
	if got.err != nil || !got.res.OK {
		t.Fatalf("Execute = %+v, %v", got.res, got.err)
	}
}

// TestResumeResendsUnseenOnce: an op the runner did not report (it never saw
// it) is re-sent exactly once by Resume.
func TestResumeResendsUnseenOnce(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
		done <- err
	}()
	waitOp(t, f.w, 1)

	f.ex.ReconcileResume(ctx, "r1", protocol.Resume{})
	f.ex.ReconcileResume(ctx, "r1", protocol.Resume{})
	if n := len(f.w.opFrames()); n != 2 {
		t.Fatalf("op frames = %d, want 2 (initial + one re-send)", n)
	}

	// The first result completes the waiting Execute.
	f.ex.DeliverResult(ctx, "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "ok"})
	if err := <-done; err != nil {
		t.Fatalf("Execute = %v", err)
	}
}

// TestResumeUnknownIgnored: an op and a state the executor does not know are
// ignored safely, with no resend and no outcome.
func TestResumeUnknownIgnored(t *testing.T) {
	f := newExecFixture(t)
	f.ex.ReconcileResume(context.Background(), "r1", protocol.Resume{InFlight: []protocol.OpStatus{
		{ID: "stranger", State: protocol.StateRunning},
		{ID: "op-x", State: "exploded"},
	}})
	if results, unknown := f.rec.counts(); results != 0 || unknown != 0 {
		t.Fatalf("unknown resume recorded: results=%d unknown=%d", results, unknown)
	}
	if n := len(f.w.opFrames()); n != 0 {
		t.Fatalf("unknown resume wrote %d op frames", n)
	}
}

// TestResumeDuplicateStepRefusalKeepsWaiter: a duplicate_step refusal for a
// re-sent op means the runner already had it, so the waiter stays for the real
// result.
func TestResumeDuplicateStepRefusalKeepsWaiter(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type out struct {
		res protocol.OpResult
		err error
	}
	ch := make(chan out, 1)
	go func() {
		res, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
		ch <- out{res, err}
	}()
	waitOp(t, f.w, 1)
	f.ex.ReconcileResume(ctx, "r1", protocol.Resume{}) // marks op1 re-sent
	f.ex.DeliverRefusal("r1", protocol.Refusal{ID: "op1", Op: "compose_up", Reason: protocol.RefuseDuplicateStep, ProposalID: "p1"})
	select {
	case got := <-ch:
		t.Fatalf("waiter ended on a duplicate_step refusal: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	f.ex.DeliverResult(ctx, "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "real"})
	got := <-ch
	if got.err != nil || !got.res.OK || got.res.Summary != "real" {
		t.Fatalf("Execute = %+v, %v", got.res, got.err)
	}
}

// TestExecuteRefusalDelivered: the runner's refusal reaches the waiting
// Execute as a *protocol.RefusedError.
func TestExecuteRefusalDelivered(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan error, 1)
	go func() {
		_, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
		ch <- err
	}()
	waitOp(t, f.w, 1)
	f.ex.DeliverRefusal("r1", protocol.Refusal{ID: "op1", Op: "compose_up", Reason: protocol.RefuseGuard})
	err := <-ch
	if !errors.Is(err, room.ErrRunnerLost) && err == nil {
		t.Fatalf("Execute = %v, want a refusal error", err)
	}
	var refused *protocol.RefusedError
	if !errors.As(err, &refused) || refused.Refusal.Reason != protocol.RefuseGuard {
		t.Fatalf("Execute = %v, want a refusal with reason guard", err)
	}
}

// TestDisconnectResultRace: a result delivered while the request is being
// canceled completes exactly once: either the waiter gets it or the orphan
// recorder records it, never both.
func TestDisconnectResultRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		f := newExecFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		delivered := make(chan bool, 1)
		go func() {
			_, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
			delivered <- err == nil
		}()
		waitOp(t, f.w, 1)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			f.ex.DeliverResult(context.Background(), "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "ok"})
		}()
		go func() {
			defer wg.Done()
			cancel()
		}()
		wg.Wait()
		got := <-delivered
		results, _ := f.rec.counts()
		if got && results != 0 {
			t.Fatalf("iteration %d: delivered to the waiter and recorded %d orphan result(s)", i, results)
		}
		if !got && results > 1 {
			t.Fatalf("iteration %d: %d orphan results, want at most 1", i, results)
		}
		cancel()
	}
}

// TestRunnerTokenReconnectRotation: a lost runner reattaches with its token,
// gets a new one, and the old token never authenticates again; the reconnect
// emits setup.runner_attached and keeps the record.
func TestRunnerTokenReconnectRotation(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	rec := &auditRecorder{}
	reg := room.NewRunnerRegistry(c.Now, rec.emit, nil)
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
		Token: "tok-a", TokenExpiresAt: c.Now().Add(protocol.TokenIdle),
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	reg.Lost(ctx, "", "r1", "arjun", "tok-a")

	rn, outcome := reg.Reconnect(ctx, "tok-a", "tok-b", c.Now().Add(protocol.TokenIdle), "10.0.0.2")
	if outcome != events.PairingOK {
		t.Fatalf("reconnect outcome = %q, want ok", outcome)
	}
	if rn.RoomID != "r1" || rn.Label != "laptop" || !rn.Connected {
		t.Fatalf("reconnected runner = %+v", rn)
	}
	if _, ok := reg.Runner("", "r1", "arjun"); !ok {
		t.Fatal("runner not attached after token reconnect")
	}

	// The old token is rotated away: it is a bad code now.
	if _, outcome := reg.Reconnect(ctx, "tok-a", "tok-c", c.Now().Add(protocol.TokenIdle), "10.0.0.2"); outcome != events.PairingBadCode {
		t.Fatalf("old token outcome = %q, want bad_code", outcome)
	}
	// The new token works while live.
	if _, outcome := reg.Reconnect(ctx, "tok-b", "tok-c", c.Now().Add(protocol.TokenIdle), "10.0.0.2"); outcome != events.PairingOK {
		t.Fatalf("new token outcome = %q, want ok", outcome)
	}

	// The reconnect is audited as attached, never with a token.
	last, ok := rec.last()
	if !ok || last.typ != events.SetupRunnerAttached {
		t.Fatalf("reconnect not audited as runner_attached: %+v", last)
	}
}

// TestRunnerTokenExpiry: an idle token expires after protocol.TokenIdle and a
// token this server never issued is bad_code; neither changes the registry.
func TestRunnerTokenExpiry(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	rec := &auditRecorder{}
	reg := room.NewRunnerRegistry(c.Now, rec.emit, nil)
	ctx := context.Background()
	if err := reg.Attach(ctx, "CODE1234", room.Runner{
		Tenant: "", RoomID: "r1", Creator: testCreator, Label: "laptop",
		Token: "tok-a", TokenExpiresAt: c.Now().Add(protocol.TokenIdle),
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	reg.Lost(ctx, "", "r1", "arjun", "tok-a")

	if _, outcome := reg.Reconnect(ctx, "not-a-token", "tok-b", c.Now().Add(protocol.TokenIdle), "10.0.0.3"); outcome != events.PairingBadCode {
		t.Fatalf("unknown token outcome = %q, want bad_code", outcome)
	}
	c.Advance(protocol.TokenIdle + time.Second)
	if _, outcome := reg.Reconnect(ctx, "tok-a", "tok-b", c.Now().Add(protocol.TokenIdle), "10.0.0.3"); outcome != events.PairingExpired {
		t.Fatalf("expired token outcome = %q, want expired", outcome)
	}
	if _, ok := reg.Runner("", "r1", "arjun"); ok {
		t.Fatal("expired detached token left the runner attached")
	}

	// No token ever reaches an audit payload.
	for _, e := range rec.evs {
		got := events.SetupPairingPayload{}
		if p, ok := e.payload.(events.SetupPairingPayload); ok {
			got = p
		}
		if strings.Contains(got.Source, "tok") || strings.Contains(got.RunnerLabel, "tok-a") {
			t.Fatalf("token leaked into an audit payload: %+v", got)
		}
	}
}

// TestCrossRoomOutcomeIgnored pins the room-isolation invariant: an outcome
// (result, refusal, or resume state) naming a pending op id is accepted only
// from the room the op was sent to. Room isolation never rests on op-id
// secrecy, so another room's authenticated runner cannot forge or refuse
// room r1's step.
func TestCrossRoomOutcomeIgnored(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan error, 1)
	go func() {
		_, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
		ch <- err
	}()
	waitOp(t, f.w, 1)

	// A forged result, refusal, and done/running resume from room r2.
	f.ex.DeliverResult(ctx, "r2", protocol.OpResult{ID: "op1", OK: true, Summary: "forged"})
	f.ex.DeliverRefusal("r2", protocol.Refusal{ID: "op1", Op: "compose_up", Reason: protocol.RefuseGuard})
	done := protocol.OpResult{ID: "op1", OK: true, Summary: "forged"}
	f.ex.ReconcileResume(ctx, "r2", protocol.Resume{InFlight: []protocol.OpStatus{
		{ID: "op1", State: protocol.StateDone, Result: &done},
	}})
	select {
	case got := <-ch:
		t.Fatalf("waiter ended on another room's outcome: %v", got)
	case <-time.After(20 * time.Millisecond):
	}

	// The real room's result still lands.
	f.ex.DeliverResult(ctx, "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "real"})
	if err := <-ch; err != nil {
		t.Fatalf("Execute = %v", err)
	}
}

// TestCrossRoomLateOutcomeIgnored: the same invariant for an op whose Execute
// already ended (the late-result memory): only the op's own room can settle
// it, as a result or as resume-running.
func TestCrossRoomLateOutcomeIgnored(t *testing.T) {
	f := newExecFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, err := f.ex.Execute(ctx, "r1", stepReq("op1", "p1", 0))
	if err == nil {
		t.Fatal("Execute ended without an error despite cancellation")
	}

	f.ex.DeliverResult(context.Background(), "r2", protocol.OpResult{ID: "op1", OK: true, Summary: "forged"})
	f.ex.ReconcileResume(context.Background(), "r2", protocol.Resume{InFlight: []protocol.OpStatus{
		{ID: "op1", State: protocol.StateRunning},
	}})
	if res, unk := f.rec.counts(); res != 0 || unk != 0 {
		t.Fatalf("another room settled the op: results=%d unknown=%d", res, unk)
	}

	// The op's own room still records the late result exactly once.
	f.ex.DeliverResult(context.Background(), "r1", protocol.OpResult{ID: "op1", OK: true, Summary: "real"})
	if res, unk := f.rec.counts(); res != 1 || unk != 0 {
		t.Fatalf("own room's late result: results=%d unknown=%d, want 1, 0", res, unk)
	}
}
