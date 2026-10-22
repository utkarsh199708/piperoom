package watch

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/store/memstore"
)

// flakyVersions is a ConfigVersionWriter over memstore that fails while err
// is set and counts the calls.
type flakyVersions struct {
	*memstore.Store
	mu    sync.Mutex
	err   error
	calls int
}

func (f *flakyVersions) RecordConfigVersion(ctx context.Context, v clients.ConnectorConfigVersion) (bool, error) {
	f.mu.Lock()
	f.calls++
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return false, err
	}
	return f.Store.RecordConfigVersion(ctx, v)
}

func (f *flakyVersions) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

// versionHarness is cfgDriftHarness with the config versions on; its orders
// connector also carries a JDBC URL with a password in it.
func versionHarness(t *testing.T, st *flakyVersions) *harness {
	t.Helper()
	h := cfgDriftHarness(t, opts{})
	h.editCfg("orders", func(m map[string]string) {
		m["connection.url"] = "jdbc:postgresql://db:5432/orders?user=app&password=" + driftSecretOld
	})
	h.w.versions = st
	return h
}

func versionsOf(t *testing.T, st *flakyVersions) []clients.ConnectorConfigVersion {
	t.Helper()
	vs, err := st.ConfigVersions(context.Background(), "orders", "pg-orders", clients.MaxConfigVersionList)
	if err != nil {
		t.Fatal(err)
	}
	return vs
}

// noStoredSecret fails when a secret or the password inside the JDBC URL is
// in any stored version (TC2).
func noStoredSecret(t *testing.T, st *flakyVersions) {
	t.Helper()
	for _, v := range versionsOf(t, st) {
		full, err := st.ConfigVersion(context.Background(), "orders", "pg-orders", v.Hash)
		if err != nil {
			t.Fatal(err)
		}
		for k, val := range full.Config {
			if strings.Contains(val, driftSecretOld) || strings.Contains(val, driftSecretNew) {
				t.Fatalf("stored %s holds a secret: %q", k, val)
			}
		}
		if full.Config["database.password"] != actions.RedactedValue {
			t.Fatalf("stored password = %q", full.Config["database.password"])
		}
	}
}

// X5c TC1/TC2 (watcher side): the first read is a version with no changed
// keys; an unchanged config writes nothing; one changed key is a new version
// naming only that key; a change only to a secret is a version with the same
// hash and the placeholder; no secret value is stored.
func TestConfigVersionsRecorded(t *testing.T) {
	st := &flakyVersions{Store: memstore.New()}
	h := versionHarness(t, st)
	h.steps(3, time.Minute)
	vs := versionsOf(t, st)
	if len(vs) != 1 || len(vs[0].ChangedKeys) != 0 || vs[0].By != "" {
		t.Fatalf("after the first reads: %+v", vs)
	}
	if st.calls != 1 {
		t.Fatalf("%d writes for an unchanged config, want 1", st.calls)
	}
	first := vs[0].Hash
	if want := actions.DigestConfig(h.conn.cfg["pg-orders"]).Hash; first != want {
		t.Fatalf("hash %s, want the digest's %s", first, want)
	}

	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(2, time.Minute)
	vs = versionsOf(t, st)
	if len(vs) != 2 || !slices.Equal(vs[0].ChangedKeys, []string{"decimal.handling.mode"}) || vs[0].Hash == first {
		t.Fatalf("after one key changed: %+v", vs)
	}

	h.editCfg("orders", func(m map[string]string) { m["database.password"] = driftSecretNew })
	h.step(time.Minute)
	vs = versionsOf(t, st)
	if len(vs) != 3 || vs[0].Hash != vs[1].Hash || !slices.Equal(vs[0].ChangedKeys, []string{events.SecretKeyChanged}) {
		t.Fatalf("after a secret-only change: %+v", vs)
	}
	noStoredSecret(t, st)
	noValues(t, h)
}

// By: the version an approved action produced names its proposal; a later
// hand edit does not, and an action that changed nothing leaves no By behind.
func TestConfigVersionsByProposal(t *testing.T) {
	st := &flakyVersions{Store: memstore.New()}
	h := versionHarness(t, st)
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.w.ApproveConnectorChangeBy("orders", "pg-orders", "p_01")
	h.steps(2, time.Minute)
	vs := versionsOf(t, st)
	if len(vs) != 2 || vs[0].By != "proposal:p_01" {
		t.Fatalf("after the approved action: %+v", vs)
	}
	// An approved action that changed nothing, then a hand edit.
	h.w.ApproveConnectorChangeBy("orders", "pg-orders", "p_02")
	h.step(time.Minute)
	h.editCfg("orders", func(m map[string]string) { m["tasks.max"] = "4" })
	h.step(time.Minute)
	vs = versionsOf(t, st)
	if len(vs) != 3 || vs[0].By != "" || !slices.Equal(vs[0].ChangedKeys, []string{"tasks.max"}) {
		t.Fatalf("after a hand edit: %+v", vs)
	}
}

// A read that started before the action's hook is not written: the next
// read writes the version with the proposal's By.
func TestConfigVersionsWaitForTheActionsRead(t *testing.T) {
	w := &Watcher{versions: &flakyVersions{Store: memstore.New()}}
	ps := &pipeState{sigs: map[string]*sigState{}}
	b := ps.driftBase("c")
	b.seen, b.seenOK = actions.DigestConfig(map[string]string{"k": "1"}), true
	b.rebaseAt, b.rebaseBy = t0, "proposal:p_09"
	obs := &observation{at: t0.Add(-time.Second), conns: map[string]*connObs{"c": {cfgTried: true, cfg: map[string]string{"k": "2"}}}}
	w.planVersion(t0, "orders", "c", ps, obs)
	if len(w.versionJobs) != 0 {
		t.Fatalf("a read from before the action planned %+v", w.versionJobs)
	}
	obs.at = t0
	w.planVersion(t0, "orders", "c", ps, obs)
	if len(w.versionJobs) != 1 || w.versionJobs[0].by != "proposal:p_09" || !slices.Equal(w.versionJobs[0].changed, []string{"k"}) {
		t.Fatalf("jobs = %+v", w.versionJobs)
	}
	// A failed or missing read plans nothing.
	w.versionJobs = nil
	for _, co := range []*connObs{nil, {cfgTried: false}, {cfgTried: true, cfgErr: errors.New("connect down")}} {
		w.planVersion(t0, "orders", "c", ps, &observation{at: t0, conns: map[string]*connObs{"c": co}})
	}
	if len(w.versionJobs) != 0 {
		t.Fatalf("a failed read planned %+v", w.versionJobs)
	}
}

// E5: a failed write is logged once and changes nothing; once the store is
// back the change is written against the last version that was, so no key
// is lost. A watcher restart (new process, empty memory) writes no
// duplicate of the newest version.
func TestConfigVersionsStoreErrorAndRestart(t *testing.T) {
	st := &flakyVersions{Store: memstore.New()}
	h := versionHarness(t, st)
	h.step(time.Minute)
	st.setErr(errors.New("store down"))
	h.editCfg("orders", func(m map[string]string) { m["decimal.handling.mode"] = driftValueNew })
	h.steps(3, time.Minute)
	warned := 0
	for _, l := range strings.Split(h.logs.String(), "\n") {
		if strings.Contains(l, "level=WARN") && strings.Contains(l, "store down") {
			warned++
		}
	}
	if warned != 1 {
		t.Fatalf("the store error was warned %d times, want 1:\n%s", warned, h.logs.String())
	}
	h.editCfg("orders", func(m map[string]string) { m["tasks.max"] = "4" })
	st.setErr(nil)
	h.step(time.Minute)
	vs := versionsOf(t, st)
	if len(vs) != 2 || !slices.Equal(vs[0].ChangedKeys, []string{"decimal.handling.mode", "tasks.max"}) {
		t.Fatalf("after the store came back: %+v", vs)
	}

	w2, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	w2.versions = st
	h.w = w2
	h.steps(2, time.Minute)
	if vs := versionsOf(t, st); len(vs) != 2 {
		t.Fatalf("a restart wrote a duplicate: %+v", vs)
	}
	// Off without a writer: nothing is planned.
	h.w.versions = nil
	h.editCfg("orders", func(m map[string]string) { m["tasks.max"] = "8" })
	h.step(time.Minute)
	if vs := versionsOf(t, st); len(vs) != 2 {
		t.Fatalf("a watcher without a writer wrote: %+v", vs)
	}
}

// blockingVersions never answers before its context ends.
type blockingVersions struct {
	mu    sync.Mutex
	calls int
}

func (b *blockingVersions) RecordConfigVersion(ctx context.Context, _ clients.ConnectorConfigVersion) (bool, error) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	<-ctx.Done()
	return false, ctx.Err()
}

// E4: a store that does not answer costs one check timeout per tick, not
// one per connector; the other connectors are planned again next tick.
func TestConfigVersionsSlowStoreStopsTheTick(t *testing.T) {
	h := cfgDriftHarness(t, opts{pipes: []pipeSpec{{id: "orders"}, {id: "billing"}}, checkTimeout: 20 * time.Millisecond})
	bv := &blockingVersions{}
	h.w.versions = bv
	h.step(time.Minute)
	bv.mu.Lock()
	defer bv.mu.Unlock()
	if bv.calls != 1 {
		t.Fatalf("%d writes in a tick with a hung store, want 1", bv.calls)
	}
}
