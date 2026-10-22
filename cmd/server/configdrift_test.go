package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/watch"
)

// driftWatcher is a roomClosedWatcher that also takes config_drift calls.
type driftWatcher struct {
	mu       sync.Mutex
	approved []string
	err      error
}

func (*driftWatcher) OnRoomClosed(string, string, string) {}

func (d *driftWatcher) ApproveConnectorChange(pipelineID, connector string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.approved = append(d.approved, pipelineID+"/"+connector)
}

func (d *driftWatcher) AcceptConfig(context.Context, string, string) (string, string, error) {
	return "a", "b", d.err
}

func TestStepConnector(t *testing.T) {
	for params, want := range map[string]string{
		`{"connector":"pg-orders","task":0}`: "pg-orders",
		`{"slot":"s"}`:                       "",
		`{"connector":7}`:                    "",
		`not json`:                           "",
		``:                                   "",
	} {
		if got := stepConnector(json.RawMessage(params)); got != want {
			t.Errorf("stepConnector(%s) = %q, want %q", params, got, want)
		}
	}
}

// The action hook passes only steps that name a connector, and only to the
// installed watcher.
func TestOnActionExecutedApprovesConnector(t *testing.T) {
	hook := &roomClosedHook{}
	step := func(p string) events.ProposalStep {
		return events.ProposalStep{Action: "a", Params: json.RawMessage(p)}
	}
	hook.onActionExecuted(context.Background(), "orders", step(`{"connector":"pg-orders"}`)) // no watcher: no-op
	w := &driftWatcher{}
	hook.setWatcher(w)
	hook.onActionExecuted(context.Background(), "orders", step(`{"connector":"pg-orders"}`))
	hook.onActionExecuted(context.Background(), "orders", step(`{"slot":"x"}`))
	if len(w.approved) != 1 || w.approved[0] != "orders/pg-orders" {
		t.Fatalf("approved = %v", w.approved)
	}
	hook.clearWatcher(w)
	hook.onActionExecuted(context.Background(), "orders", step(`{"connector":"pg-orders"}`))
	if len(w.approved) != 1 {
		t.Fatalf("a cleared watcher was called: %v", w.approved)
	}
}

// accept-config maps the watcher's answers to the route's errors; no watcher
// on this replica is watch_off.
func TestHookAcceptConfigErrors(t *testing.T) {
	hook := &roomClosedHook{}
	if _, _, err := hook.AcceptConfig(context.Background(), "orders", "pg-orders"); !errors.Is(err, httpapi.ErrWatchOff) {
		t.Fatalf("no watcher: %v", err)
	}
	for _, c := range []struct {
		err  error
		want error
	}{
		{watch.ErrNotWatched, httpapi.ErrWatchOff},
		{watch.ErrConnectUnavailable, httpapi.ErrConnectUnavailable},
	} {
		hook.setWatcher(&driftWatcher{err: c.err})
		if _, _, err := hook.AcceptConfig(context.Background(), "orders", "pg-orders"); !errors.Is(err, c.want) {
			t.Fatalf("%v: got %v, want %v", c.err, err, c.want)
		}
	}
	other := errors.New("boom")
	hook.setWatcher(&driftWatcher{err: other})
	if _, _, err := hook.AcceptConfig(context.Background(), "orders", "pg-orders"); !errors.Is(err, other) ||
		errors.Is(err, httpapi.ErrWatchOff) || errors.Is(err, httpapi.ErrConnectUnavailable) {
		t.Fatalf("other error: %v", err)
	}
	hook.setWatcher(&driftWatcher{})
	if o, n, err := hook.AcceptConfig(context.Background(), "orders", "pg-orders"); err != nil || o != "a" || n != "b" {
		t.Fatalf("ok: %q %q %v", o, n, err)
	}
}

// driftConnect answers Config with cfg or err.
type driftConnect struct {
	cfg map[string]string
	err error
}

func (driftConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{Name: name, State: "RUNNING"}, nil
}

func (d driftConnect) Config(context.Context, string) (map[string]string, error) { return d.cfg, d.err }

// The hook over a real watcher: the new hash is the redacted config's id,
// and a Connect failure or an unknown connector maps to the route's errors.
func TestHookAcceptConfigRealWatcher(t *testing.T) {
	cfg := map[string]string{"slot.name": "debezium_orders", "database.password": "pw"}
	build := func(c watch.ConnectClient) *roomClosedHook {
		w, err := watch.New(watch.Deps{
			Owners: loadTestOwners(t), Connect: c, Rooms: watchRooms{},
			Config: config.Watch{Mode: config.WatchLog, Interval: time.Minute},
		})
		if err != nil {
			t.Fatal(err)
		}
		hook := &roomClosedHook{}
		hook.setWatcher(w)
		return hook
	}
	hook := build(driftConnect{cfg: cfg})
	oldHash, newHash, err := hook.AcceptConfig(context.Background(), "orders", "pg-orders")
	if err != nil || oldHash != "" || newHash != actions.ConfigHash(actions.RedactConfig(cfg)) {
		t.Fatalf("AcceptConfig = %q, %q, %v", oldHash, newHash, err)
	}
	if o, _, _ := hook.AcceptConfig(context.Background(), "orders", "pg-orders"); o != newHash {
		t.Fatalf("second accept: old hash %q, want %q", o, newHash)
	}
	if _, _, err := hook.AcceptConfig(context.Background(), "orders", "pg-nope"); !errors.Is(err, httpapi.ErrWatchOff) {
		t.Fatalf("unknown connector: %v", err)
	}
	hook = build(driftConnect{err: errors.New("connection refused")})
	if _, _, err := hook.AcceptConfig(context.Background(), "orders", "pg-orders"); !errors.Is(err, httpapi.ErrConnectUnavailable) {
		t.Fatalf("connect down: %v", err)
	}
}
