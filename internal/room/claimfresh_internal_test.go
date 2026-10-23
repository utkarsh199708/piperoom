package room

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// claimStore is a memstore that says it is shared (store.Coordinator) and
// keeps one proposal claim the way the Postgres store does: a touch refreshes
// it while this replica holds it, and sweep takes it once it is older than
// stale. Coordinator methods the tests do not use are left nil.
type claimStore struct {
	*memstore.Store
	store.Coordinator

	mu        sync.Mutex
	claimedAt time.Time
	taken     bool
	touches   int
}

func (c *claimStore) ExpireRoomLeases(context.Context, string) error { return nil }

func (c *claimStore) TouchProposalClaim(context.Context, string, string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taken {
		return false, nil
	}
	c.claimedAt = time.Now()
	c.touches++
	return true, nil
}

// sweep is the stale sweep of another replica: every period it takes the
// claim once it was not refreshed for stale, until ctx ends.
func (c *claimStore) sweep(ctx context.Context, stale, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.mu.Lock()
			if time.Since(c.claimedAt) > stale {
				c.taken = true
			}
			c.mu.Unlock()
		}
	}
}

func (c *claimStore) state() (taken bool, touches int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.taken, c.touches
}

// funcAction runs fn as its Execute.
type funcAction struct {
	fn func(ctx context.Context) (string, error)
}

func (a *funcAction) Name() string                                      { return "fn" }
func (a *funcAction) Layer() string                                     { return "setup" }
func (a *funcAction) Describe() string                                  { return "fn" }
func (a *funcAction) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (a *funcAction) Validate(context.Context, json.RawMessage) error   { return nil }
func (a *funcAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *funcAction) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	return a.fn(ctx)
}

var claimTimings = ReplicaTimings{LeaseRenew: 30 * time.Millisecond, ClaimStale: 150 * time.Millisecond}

// claimRig is a replica of a shared store whose claim on proposal "p1" was
// just taken, with another replica's stale sweep running against it.
func claimRig(t *testing.T) (*Service, *claimStore) {
	t.Helper()
	cs := &claimStore{Store: memstore.New(), claimedAt: time.Now()}
	svc := New(Deps{Store: cs, Users: DefaultUsers(), ReplicaID: "replica-a", ReplicaTimings: claimTimings})
	t.Cleanup(svc.Close)
	if svc.coord == nil {
		t.Fatal("claim store not seen as a coordinator")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go cs.sweep(ctx, claimTimings.ClaimStale, 10*time.Millisecond)
	return svc, cs
}

// sleepAction holds the step for d (or until its context ends).
func sleepAction(d time.Duration) *funcAction {
	return &funcAction{fn: func(ctx context.Context) (string, error) {
		select {
		case <-time.After(d):
			return "ok", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}}
}

// A step bound longer than stepTimeout (a setup T3 step) keeps its claim
// fresh, so a step that outlives ClaimStale is not swept while it runs; a
// step at the plain bound is not refreshed and the same sweep takes it.
func TestLongStepClaimSurvivesTheSweep(t *testing.T) {
	long := 4 * claimTimings.ClaimStale

	svc, cs := claimRig(t)
	out, err := svc.executeStep(context.Background(), "p1", sleepAction(long), nil, stepTimeout+time.Minute)
	if err != nil || out != "ok" {
		t.Fatalf("executeStep = %q, %v; want ok", out, err)
	}
	if taken, touches := cs.state(); taken || touches == 0 {
		t.Fatalf("taken=%v touches=%d: the claim of a running step was swept", taken, touches)
	}

	// Control: the sweep does take a claim nobody refreshes.
	svc, cs = claimRig(t)
	if _, err := svc.executeStep(context.Background(), "p1", sleepAction(long), nil, stepTimeout); err != nil {
		t.Fatal(err)
	}
	if taken, touches := cs.state(); !taken || touches != 0 {
		t.Fatalf("control: taken=%v touches=%d, want a swept claim and no refresh", taken, touches)
	}
}

// The refresher ends with the step, also when the action panics.
func TestClaimRefreshStopsWithTheStep(t *testing.T) {
	quiet := func(t *testing.T, cs *claimStore) {
		t.Helper()
		_, before := cs.state()
		time.Sleep(5 * claimTimings.LeaseRenew)
		if _, after := cs.state(); after != before {
			t.Fatalf("the claim was refreshed %d time(s) after the step ended", after-before)
		}
	}

	svc, cs := claimRig(t)
	if _, err := svc.executeStep(context.Background(), "p1", sleepAction(3*claimTimings.LeaseRenew), nil, stepTimeout+time.Minute); err != nil {
		t.Fatal(err)
	}
	quiet(t, cs)

	svc, cs = claimRig(t)
	boom := &funcAction{fn: func(context.Context) (string, error) {
		time.Sleep(3 * claimTimings.LeaseRenew)
		panic("boom")
	}}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the action's panic was swallowed")
			}
		}()
		_, _ = svc.executeStep(context.Background(), "p1", boom, nil, stepTimeout+time.Minute)
	}()
	quiet(t, cs)
}

// A claim the stale sweep took cancels the running step at the next refresh,
// long before its bound: the sweep has already recorded the run as
// interrupted, so a pending T3 confirmation must not go on.
func TestLostClaimCancelsTheStep(t *testing.T) {
	svc, cs := claimRig(t)
	cs.mu.Lock()
	cs.taken = true
	cs.mu.Unlock()
	start := time.Now()
	_, err := svc.executeStep(context.Background(), "p1", sleepAction(time.Hour), nil, stepTimeout+time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("executeStep = %v, want context.Canceled", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the step ran on for %s after its claim was lost", took)
	}
}
