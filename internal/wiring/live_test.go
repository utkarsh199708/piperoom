package wiring

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/integrations"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// liveConn is a fake ConnectClient that notices calls after its close.
type liveConn struct {
	name     string
	closes   atomic.Int32
	late     atomic.Int32  // calls that started after close
	block    chan struct{} // when non-nil, Status waits on it
	entered  chan struct{} // when non-nil, Status signals entry
	inFlight atomic.Int32
}

func (c *liveConn) close() { c.closes.Add(1) }

func (c *liveConn) enter() {
	if c.closes.Load() > 0 {
		c.late.Add(1)
	}
}

func (c *liveConn) ListConnectors(context.Context) ([]string, error) {
	c.enter()
	return []string{c.name}, nil
}

func (c *liveConn) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	c.enter()
	c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	if c.entered != nil {
		c.entered <- struct{}{}
	}
	if c.block != nil {
		<-c.block
	}
	return clients.ConnectorStatus{Name: c.name + "/" + name}, nil
}

func (c *liveConn) Config(context.Context, string) (map[string]string, error) {
	c.enter()
	return map[string]string{"from": c.name}, nil
}

func (c *liveConn) RestartTask(context.Context, string, int) error { c.enter(); return nil }
func (c *liveConn) RestartConnector(context.Context, string) error { c.enter(); return nil }

type liveKafkaFake struct{}

func (liveKafkaFake) Topics(context.Context) ([]string, error) { return []string{"t"}, nil }
func (liveKafkaFake) TopicOffsets(context.Context, string) ([]clients.PartitionOffsets, error) {
	return nil, nil
}
func (liveKafkaFake) ConsumerGroups(context.Context) ([]string, error) { return nil, nil }
func (liveKafkaFake) GroupLag(context.Context, string) (clients.GroupLag, error) {
	return clients.GroupLag{}, nil
}
func (liveKafkaFake) FindByKey(context.Context, string, string, time.Time, int) ([]clients.RecordRef, error) {
	return nil, nil
}

type liveKafkaDLQ struct{ liveKafkaFake }

func (liveKafkaDLQ) DLQOffsets(_ context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	return map[string][]clients.PartitionOffsets{topics[0]: {{End: 7}}}, nil
}
func (liveKafkaDLQ) PeekDLQ(context.Context, string, int) ([]clients.DLQRecord, error) {
	return []clients.DLQRecord{{Offset: 3}}, nil
}

type liveSrcFake struct{}

func (liveSrcFake) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return []clients.ReplicationSlot{{Name: "s"}}, nil
}
func (liveSrcFake) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return nil, nil
}
func (liveSrcFake) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return nil, nil
}
func (liveSrcFake) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return map[string]any{"id": 1}, true, nil
}

type liveWhFake struct{}

func (liveWhFake) Freshness(context.Context, string, string) (time.Time, error) {
	return time.Unix(10, 0), nil
}
func (liveWhFake) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return nil, false, nil
}

type liveRegFake struct{}

func (liveRegFake) Subjects(context.Context) ([]string, error)      { return []string{"x"}, nil }
func (liveRegFake) Versions(context.Context, string) ([]int, error) { return []int{1}, nil }
func (liveRegFake) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{Version: 1}, nil
}
func (liveRegFake) Compatibility(context.Context, string) (string, bool, error) {
	return "BACKWARD", true, nil
}
func (liveRegFake) SetCompatibility(context.Context, string, string) error { return nil }
func (liveRegFake) Register(context.Context, string, string) (int, error)  { return 5, nil }

// waitGoroutines polls until the goroutine count is back to at most n.
func waitGoroutines(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > n {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutine leak: %d running, want <= %d\n%s", runtime.NumGoroutine(), n, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLiveUnconfigured(t *testing.T) {
	l, w := NewLive(LiveOptions{})
	defer w.Close()
	ctx := context.Background()

	cases := []struct {
		name string
		err  error
		msg  string
	}{
		{"source", func() error { _, err := w.Source.ReplicationSlots(ctx); return err }(), "The source database is not connected"},
		{"source row", func() error { _, _, err := w.Source.Row(ctx, "a.b", "id", "1"); return err }(), "The source database is not connected"},
		{"warehouse", func() error { _, err := w.Warehouse.Freshness(ctx, "a.b", "ts"); return err }(), "The warehouse is not connected"},
		{"connect", func() error { _, err := w.Connect.Status(ctx, "c"); return err }(), "Kafka Connect is not connected"},
		{"connect write", w.Connect.RestartConnector(ctx, "c"), "Kafka Connect is not connected"},
		{"kafka", func() error { _, err := w.Kafka.Topics(ctx); return err }(), "Kafka is not connected"},
		{"kafka dlq", func() error {
			_, err := w.Kafka.(clients.DLQReader).PeekDLQ(ctx, "t", 1)
			return err
		}(), "Kafka is not connected"},
		{"registry", func() error { _, _, err := w.Registry.Compatibility(ctx, "s"); return err }(), "The Schema Registry is not connected"},
		{"model", l.Model().Run(ctx, agent.Alert{}, agent.Deps{}, agent.Options{}), "The model endpoint is not connected"},
	}
	for _, tc := range cases {
		if !errors.Is(tc.err, ErrNotConfigured) {
			t.Errorf("%s: err = %v, want ErrNotConfigured", tc.name, tc.err)
			continue
		}
		if tc.err.Error() != tc.msg {
			t.Errorf("%s: message = %q, want %q", tc.name, tc.err.Error(), tc.msg)
		}
	}
	if got := l.Configured(); len(got) != 0 {
		t.Errorf("Configured() = %v, want none", got)
	}
	if l.WarehouseKind() != "" {
		t.Errorf("WarehouseKind() = %q, want empty", l.WarehouseKind())
	}
	l.Model().Forget("room") // must not panic with no endpoint
}

func TestLiveForwardsAndConfigured(t *testing.T) {
	l, w := NewLive(LiveOptions{})
	defer w.Close()
	ctx := context.Background()
	mustNil(t, l.SwapSource(liveSrcFake{}, nil))
	mustNil(t, l.SwapWarehouse(liveWhFake{}, "", nil))
	mustNil(t, l.SwapConnect(&liveConn{name: "a"}, nil))
	mustNil(t, l.SwapKafka(liveKafkaDLQ{}, nil))
	mustNil(t, l.SwapRegistry(liveRegFake{}, nil))

	want := []spec.Category{spec.CategorySourceDB, spec.CategoryKafka, spec.CategoryConnect, spec.CategoryRegistry, spec.CategoryWarehouse}
	if got := l.Configured(); !slices.Equal(got, want) {
		t.Errorf("Configured() = %v, want %v", got, want)
	}
	if l.WarehouseKind() != "postgres" {
		t.Errorf("WarehouseKind() = %q, want postgres", l.WarehouseKind())
	}
	if row, ok, err := w.Source.Row(ctx, "a.b", "id", "1"); err != nil || !ok || row["id"] != 1 {
		t.Errorf("Source.Row = %v %v %v", row, ok, err)
	}
	if ts, err := w.Warehouse.Freshness(ctx, "a.b", "ts"); err != nil || !ts.Equal(time.Unix(10, 0)) {
		t.Errorf("Warehouse.Freshness = %v %v", ts, err)
	}
	if st, err := w.Connect.Status(ctx, "c"); err != nil || st.Name != "a/c" {
		t.Errorf("Connect.Status = %v %v", st, err)
	}
	if lvl, sub, err := w.Registry.Compatibility(ctx, "s"); err != nil || lvl != "BACKWARD" || !sub {
		t.Errorf("Registry.Compatibility = %v %v %v", lvl, sub, err)
	}
	if id, err := w.Registry.Register(ctx, "s", "{}"); err != nil || id != 5 {
		t.Errorf("Registry.Register = %v %v", id, err)
	}
	d := w.Kafka.(clients.DLQReader)
	if offs, err := d.DLQOffsets(ctx, "dlq"); err != nil || offs["dlq"][0].End != 7 {
		t.Errorf("DLQOffsets = %v %v", offs, err)
	}

	// A Kafka client without DLQ support: the wrapper answers with an error,
	// not ErrNotConfigured.
	mustNil(t, l.SwapKafka(liveKafkaFake{}, nil))
	if _, err := d.PeekDLQ(ctx, "dlq", 1); !errors.Is(err, ErrDLQUnsupported) || !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("PeekDLQ on a plain client = %v, want ErrDLQUnsupported wrapping errors.ErrUnsupported", err)
	}
	if _, err := d.DLQOffsets(ctx, "dlq"); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("DLQOffsets on a plain client = %v, want errors.ErrUnsupported", err)
	}
}

func TestLiveSwapClosesOldOnce(t *testing.T) {
	l, w := NewLive(LiveOptions{})
	ctx := context.Background()
	a, b := &liveConn{name: "a"}, &liveConn{name: "b"}
	mustNil(t, l.SwapConnect(a, a.close))
	mustNil(t, l.SwapConnect(b, b.close))
	if st, _ := w.Connect.Status(ctx, "x"); st.Name != "b/x" {
		t.Errorf("after swap, Status from %q, want b", st.Name)
	}
	waitFor(t, func() bool { return a.closes.Load() == 1 })
	mustNil(t, l.SwapConnect(nil, nil)) // clear
	waitFor(t, func() bool { return b.closes.Load() == 1 })
	if _, err := w.Connect.Status(ctx, "x"); !errors.Is(err, ErrNotConfigured) {
		t.Errorf("after clear: %v, want ErrNotConfigured", err)
	}
	w.Close()
	w.Close()
	if a.closes.Load() != 1 || b.closes.Load() != 1 {
		t.Errorf("closes a=%d b=%d, want 1 each", a.closes.Load(), b.closes.Load())
	}
}

func TestLiveDrainWaitsForInFlight(t *testing.T) {
	l, w := NewLive(LiveOptions{DrainGrace: time.Minute})
	a := &liveConn{name: "a", block: make(chan struct{}), entered: make(chan struct{}, 1)}
	mustNil(t, l.SwapConnect(a, a.close))

	done := make(chan clients.ConnectorStatus)
	go func() {
		st, _ := w.Connect.Status(context.Background(), "x")
		done <- st
	}()
	<-a.entered
	b := &liveConn{name: "b"}
	mustNil(t, l.SwapConnect(b, b.close))

	// New calls go to b at once while a's call is still running.
	if st, _ := w.Connect.Status(context.Background(), "y"); st.Name != "b/y" {
		t.Errorf("new call went to %q, want b", st.Name)
	}
	time.Sleep(20 * time.Millisecond)
	if a.closes.Load() != 0 {
		t.Fatal("old client closed while a call was in flight")
	}
	close(a.block)
	if st := <-done; st.Name != "a/x" {
		t.Errorf("in-flight call finished on %q, want a", st.Name)
	}
	waitFor(t, func() bool { return a.closes.Load() == 1 })
	w.Close()
	if a.late.Load() != 0 {
		t.Errorf("%d calls reached a after its close", a.late.Load())
	}
}

func TestLiveGraceForcesClose(t *testing.T) {
	l, w := NewLive(LiveOptions{DrainGrace: 30 * time.Millisecond})
	a := &liveConn{name: "a", block: make(chan struct{}), entered: make(chan struct{}, 1)}
	mustNil(t, l.SwapConnect(a, a.close))
	done := make(chan struct{})
	go func() {
		_, _ = w.Connect.Status(context.Background(), "x")
		close(done)
	}()
	<-a.entered
	mustNil(t, l.SwapConnect(&liveConn{name: "b"}, nil))
	waitFor(t, func() bool { return a.closes.Load() == 1 })
	if a.inFlight.Load() != 1 {
		t.Fatal("expected the call to still be in flight when the grace ran out")
	}
	close(a.block)
	<-done
	w.Close()
	if a.closes.Load() != 1 {
		t.Errorf("closes = %d, want exactly 1", a.closes.Load())
	}
}

// TestLiveSwapDuringConcurrentCalls is the race test: run it with -race.
func TestLiveSwapDuringConcurrentCalls(t *testing.T) {
	before := runtime.NumGoroutine()
	l, w := NewLive(LiveOptions{DrainGrace: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())

	var (
		mu    sync.Mutex
		conns []*liveConn
	)
	newConn := func() *liveConn {
		c := &liveConn{name: "c"}
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
		return c
	}
	first := newConn()
	mustNil(t, l.SwapConnect(first, first.close))

	var calls sync.WaitGroup
	var unexpected atomic.Int32
	for range 16 {
		calls.Add(1)
		go func() {
			defer calls.Done()
			for ctx.Err() == nil {
				if _, err := w.Connect.Status(ctx, "x"); err != nil && !errors.Is(err, ErrNotConfigured) {
					unexpected.Add(1)
				}
				_, _ = w.Connect.ListConnectors(ctx)
			}
		}()
	}
	for i := range 300 {
		if i%50 == 49 {
			mustNil(t, l.SwapConnect(nil, nil))
			continue
		}
		c := newConn()
		mustNil(t, l.SwapConnect(c, c.close))
	}
	cancel()
	calls.Wait()
	w.Close()

	if unexpected.Load() != 0 {
		t.Errorf("%d unexpected errors", unexpected.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for i, c := range conns {
		if n := c.closes.Load(); n != 1 {
			t.Errorf("conn %d closed %d times, want 1", i, n)
		}
		if n := c.late.Load(); n != 0 {
			t.Errorf("conn %d got %d calls after close", i, n)
		}
	}
	waitGoroutines(t, before)
}

func TestLiveCloseThenSwap(t *testing.T) {
	before := runtime.NumGoroutine()
	l, w := NewLive(LiveOptions{})
	a := &liveConn{name: "a"}
	mustNil(t, l.SwapConnect(a, a.close))
	adopted := 0
	mustNil(t, l.Adopt(&Wired{Source: liveSrcFake{}, close: func() { adopted++ }}))
	w.Close()
	if a.closes.Load() != 1 || adopted != 1 {
		t.Errorf("Close: connect closes=%d, adopted closes=%d, want 1 and 1", a.closes.Load(), adopted)
	}
	b := &liveConn{name: "b"}
	if err := l.SwapConnect(b, b.close); !errors.Is(err, ErrLiveClosed) {
		t.Errorf("SwapConnect after Close = %v, want ErrLiveClosed", err)
	}
	if b.closes.Load() != 1 {
		t.Errorf("client refused after Close was closed %d times, want 1", b.closes.Load())
	}
	if _, err := w.Connect.Status(context.Background(), "x"); !errors.Is(err, ErrNotConfigured) {
		t.Errorf("call after Close = %v, want ErrNotConfigured", err)
	}
	w.Close()
	if adopted != 1 {
		t.Errorf("adopted closer ran %d times, want 1", adopted)
	}
	waitGoroutines(t, before)
}

func TestLiveAdopt(t *testing.T) {
	l, w := NewLive(LiveOptions{Env: []spec.Category{spec.CategorySourceDB, spec.CategoryKafka, spec.CategoryWarehouse}})
	defer w.Close()
	// Open builds all five clients, with dev defaults for unset variables;
	// only the env categories may be adopted.
	mustNil(t, l.Adopt(&Wired{
		Source: liveSrcFake{}, Warehouse: liveWhFake{}, WarehouseKind: "snowflake", Kafka: liveKafkaFake{},
		Connect: &liveConn{name: "dev-default"}, Registry: liveRegFake{},
	}))
	want := []spec.Category{spec.CategorySourceDB, spec.CategoryKafka, spec.CategoryWarehouse}
	if got := l.Configured(); !slices.Equal(got, want) {
		t.Errorf("Configured() = %v, want %v", got, want)
	}
	if l.WarehouseKind() != "snowflake" {
		t.Errorf("WarehouseKind() = %q, want snowflake", l.WarehouseKind())
	}
}

func TestLiveApplyAndClear(t *testing.T) {
	ctx := context.Background()
	rec := spec.Integration{ID: "int_1", WorkspaceID: spec.DefaultWorkspace, Category: spec.CategoryConnect, Kind: "connect", Scope: spec.ScopeAll}
	built := &liveConn{name: "built"}
	good := spec.Kind{ID: "connect", Category: spec.CategoryConnect, Build: func(context.Context, spec.Values) (any, func(), error) {
		return built, built.close, nil
	}}

	l, w := NewLive(LiveOptions{Env: []spec.Category{spec.CategoryKafka}})
	defer w.Close()
	mustNil(t, l.Apply(ctx, rec, good, spec.Values{}))
	if st, _ := w.Connect.Status(ctx, "x"); st.Name != "built/x" {
		t.Fatalf("after Apply, Status from %q", st.Name)
	}

	// A failed build keeps the old client.
	boom := errors.New("boom")
	failing := good
	failing.Build = func(context.Context, spec.Values) (any, func(), error) { return nil, nil, boom }
	if err := l.Apply(ctx, rec, failing, spec.Values{}); !errors.Is(err, boom) {
		t.Errorf("failed build: %v, want boom", err)
	}
	// A client of the wrong type is closed and keeps the old client.
	wrongClosed := 0
	wrong := good
	wrong.Build = func(context.Context, spec.Values) (any, func(), error) {
		return liveWhFake{}, func() { wrongClosed++ }, nil
	}
	if err := l.Apply(ctx, rec, wrong, spec.Values{}); err == nil || wrongClosed != 1 {
		t.Errorf("wrong type: err=%v closed=%d, want error and 1", err, wrongClosed)
	}
	if st, _ := w.Connect.Status(ctx, "x"); st.Name != "built/x" {
		t.Errorf("old client lost after failed Apply: %q", st.Name)
	}

	// Refusals.
	other := rec
	other.WorkspaceID = "ws_other"
	if err := l.Apply(ctx, other, good, spec.Values{}); err == nil {
		t.Error("Apply for another workspace: want error")
	}
	scoped := rec
	scoped.Scope = "pipe_1"
	if err := l.Apply(ctx, scoped, good, spec.Values{}); err == nil {
		t.Error("Apply with a pipeline scope: want error")
	}
	mismatch := good
	mismatch.Category = spec.CategoryRegistry
	if err := l.Apply(ctx, rec, mismatch, spec.Values{}); err == nil {
		t.Error("Apply with a kind of another category: want error")
	}
	envRec := rec
	envRec.Category = spec.CategoryKafka
	if err := l.Apply(ctx, envRec, spec.Kind{ID: "kafka", Category: spec.CategoryKafka}, spec.Values{}); !errors.Is(err, spec.ErrManagedByEnv) {
		t.Errorf("Apply on env category = %v, want ErrManagedByEnv", err)
	}
	if err := l.Clear(ctx, envRec); !errors.Is(err, spec.ErrManagedByEnv) {
		t.Errorf("Clear on env category = %v, want ErrManagedByEnv", err)
	}
	noBuild := good
	noBuild.Build = nil
	if err := l.Apply(ctx, rec, noBuild, spec.Values{}); err == nil {
		t.Error("Apply with no Build: want error")
	}

	// Alerts have no client: Apply is a no-op that still closes what Build made.
	alertClosed := 0
	alertRec := spec.Integration{ID: "int_2", Category: spec.CategoryAlerts, Kind: "grafana"}
	alertKind := spec.Kind{ID: "grafana", Category: spec.CategoryAlerts, Build: func(context.Context, spec.Values) (any, func(), error) {
		return nil, func() { alertClosed++ }, nil
	}}
	if err := l.Apply(ctx, alertRec, alertKind, spec.Values{}); err != nil || alertClosed != 1 {
		t.Errorf("alerts Apply: err=%v closed=%d", err, alertClosed)
	}

	// Warehouse kind follows the kind id.
	whRec := spec.Integration{ID: "int_3", Category: spec.CategoryWarehouse, Kind: "snowflake"}
	whKind := spec.Kind{ID: "snowflake", Category: spec.CategoryWarehouse, Build: func(context.Context, spec.Values) (any, func(), error) {
		return liveWhFake{}, nil, nil
	}}
	mustNil(t, l.Apply(ctx, whRec, whKind, spec.Values{}))
	if l.WarehouseKind() != "snowflake" {
		t.Errorf("WarehouseKind() = %q, want snowflake", l.WarehouseKind())
	}

	mustNil(t, l.Clear(ctx, rec))
	if _, err := w.Connect.Status(ctx, "x"); !errors.Is(err, ErrNotConfigured) {
		t.Errorf("after Clear: %v, want ErrNotConfigured", err)
	}
	waitFor(t, func() bool { return built.closes.Load() == 1 })
}

func TestLiveRegisterTools(t *testing.T) {
	ownersFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(ownersFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write owners: %v", err)
	}
	o, err := owners.Load(ownersFile)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	_, w := NewLive(LiveOptions{Owners: o})
	defer w.Close()
	if w.Builder == nil {
		t.Fatal("NewLive: Builder is nil")
	}
	if err := RegisterTools(tools.NewRegistry(), actions.NewRegistry(), w); err != nil {
		t.Fatalf("RegisterTools on live clients: %v", err)
	}
}

func TestLiveAdoptNoEnv(t *testing.T) {
	adopted := 0
	l, w := NewLive(LiveOptions{})
	mustNil(t, l.Adopt(&Wired{Source: liveSrcFake{}, Connect: &liveConn{name: "dev"}, Kafka: liveKafkaFake{}, close: func() { adopted++ }}))
	if got := l.Configured(); len(got) != 0 {
		t.Errorf("Configured() with no env categories = %v, want none", got)
	}
	if _, err := w.Kafka.Topics(context.Background()); !errors.Is(err, ErrNotConfigured) {
		t.Errorf("Topics = %v, want ErrNotConfigured", err)
	}
	w.Close()
	if adopted != 1 {
		t.Errorf("adopted closer ran %d times, want 1", adopted)
	}
}

func TestLiveApplyRejectsTypedNil(t *testing.T) {
	ctx := context.Background()
	l, w := NewLive(LiveOptions{})
	defer w.Close()
	closed := 0
	rec := spec.Integration{ID: "int_1", Category: spec.CategoryConnect, Kind: "connect"}
	kind := spec.Kind{ID: "connect", Category: spec.CategoryConnect, Build: func(context.Context, spec.Values) (any, func(), error) {
		return (*liveConn)(nil), func() { closed++ }, nil
	}}
	if err := l.Apply(ctx, rec, kind, spec.Values{}); err == nil || closed != 1 {
		t.Errorf("typed nil: err=%v closed=%d, want error and 1", err, closed)
	}
	if _, err := w.Connect.Status(ctx, "x"); !errors.Is(err, ErrNotConfigured) {
		t.Errorf("after typed-nil Apply: %v, want ErrNotConfigured", err)
	}
	for _, v := range []any{nil, (*liveConn)(nil), clients.ConnectClient(nil), map[string]int(nil)} {
		if !isNil(v) {
			t.Errorf("isNil(%#v) = false", v)
		}
	}
	for _, v := range []any{liveSrcFake{}, &liveConn{}, 0} {
		if isNil(v) {
			t.Errorf("isNil(%#v) = true", v)
		}
	}
}

func TestLiveConcurrentClose(t *testing.T) {
	l, w := NewLive(LiveOptions{DrainGrace: time.Minute})
	var adopted atomic.Int32
	slow := make(chan struct{})
	mustNil(t, l.Adopt(&Wired{close: func() { <-slow; adopted.Add(1) }}))
	first := make(chan struct{})
	go func() { w.Close(); close(first) }()
	waitFor(t, func() bool { l.mu.Lock(); defer l.mu.Unlock(); return l.closed })
	second := make(chan struct{})
	go func() { l.Close(); close(second) }()
	select {
	case <-second:
		t.Fatal("second Close returned before the first released the adopted clients")
	case <-time.After(50 * time.Millisecond):
	}
	close(slow)
	<-first
	<-second
	if adopted.Load() != 1 {
		t.Errorf("adopted closer ran %d times, want 1", adopted.Load())
	}
}

func TestLiveModelGrace(t *testing.T) {
	l, w := NewLive(LiveOptions{DrainGrace: 10 * time.Millisecond, ModelDrainGrace: time.Hour})
	if l.model.grace != time.Hour || l.connect.grace != 10*time.Millisecond {
		t.Errorf("graces: model %v connect %v", l.model.grace, l.connect.grace)
	}
	l2, w2 := NewLive(LiveOptions{DrainGrace: time.Hour, ModelDrainGrace: time.Second})
	if l2.model.grace != time.Hour {
		t.Errorf("model grace below DrainGrace: %v, want %v", l2.model.grace, time.Hour)
	}
	w2.Close()

	// A held model reference outlives DrainGrace; at Close, the short grace
	// applies so shutdown is not held up for the model grace.
	var closed atomic.Int32
	mustNil(t, l.SwapModel(&agent.Endpoint{}, func() { closed.Add(1) }))
	sl, err := l.model.acquire()
	mustNil(t, err)
	mustNil(t, l.SwapModel(nil, nil))
	time.Sleep(50 * time.Millisecond)
	if closed.Load() != 0 {
		t.Fatal("model endpoint closed after DrainGrace, want it to wait for the model grace")
	}
	start := time.Now()
	w.Close()
	if closed.Load() != 1 {
		t.Errorf("model closes = %d, want 1", closed.Load())
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("Close took %v, want about DrainGrace", d)
	}
	sl.release()
}

func TestEnvCategories(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want []spec.Category
	}{
		{"nothing set (config defaults only)", nil, nil},
		{"kafka and connect", map[string]string{"KAFKA_BROKERS": "b:9092", "CONNECT_URL": "http://c"}, []spec.Category{spec.CategoryKafka, spec.CategoryConnect}},
		{"blank variable", map[string]string{"KAFKA_BROKERS": "  "}, nil},
		{"registry", map[string]string{"SCHEMA_REGISTRY_URL": "http://r"}, []spec.Category{spec.CategoryRegistry}},
		{"api key only", map[string]string{"ANTHROPIC_API_KEY": "k"}, []spec.Category{spec.CategoryModel}},
		{"source", map[string]string{"SOURCE_DB_URL": "postgres://x"}, []spec.Category{spec.CategorySourceDB}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EnvCategories(func(k string) (string, bool) { v, ok := tc.env[k]; return v, ok })
			if !slices.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnvVarsMatchIntegrations keeps envVars and internal/integrations' env
// view in step, so the Integrations page and Live.Apply agree on which
// categories are env-managed.
func TestEnvVarsMatchIntegrations(t *testing.T) {
	asked := map[string]bool{}
	integrations.EnvFromLookup(func(k string) (string, bool) { asked[k] = true; return "", false })
	theirs := map[string]spec.Category{}
	for name := range asked {
		env := integrations.EnvFromLookup(func(k string) (string, bool) {
			if k == name {
				return "x", true
			}
			return "", false
		})
		for cat := range env.Categories {
			theirs[name] = cat
		}
	}
	if len(theirs) < 20 {
		t.Fatalf("integrations env view mapped only %d variables; the probe is broken", len(theirs))
	}
	ours := map[string]spec.Category{}
	for cat, names := range envVars {
		for _, n := range names {
			ours[n] = cat
		}
	}
	for n, c := range theirs {
		if ours[n] != c {
			t.Errorf("%s: integrations says %q, wiring says %q", n, c, ours[n])
		}
	}
	for n, c := range ours {
		if theirs[n] != c {
			t.Errorf("%s: wiring says %q, integrations says %q", n, c, theirs[n])
		}
	}
	// Same answer for a whole environment.
	full := func(k string) (string, bool) { return "v", asked[k] }
	var want []spec.Category
	got := integrations.EnvFromLookup(full).Categories
	for _, c := range spec.Categories {
		if _, ok := got[c]; ok {
			want = append(want, c)
		}
	}
	if have := EnvCategories(full); !slices.Equal(have, want) {
		t.Errorf("EnvCategories = %v, integrations = %v", have, want)
	}
}

func mustNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Two saves of one row whose builds finish out of order: the older version
// must not replace the newer one.
func TestLiveApplyDropsOlderVersion(t *testing.T) {
	ctx := context.Background()
	kindFor := func(c *liveConn) spec.Kind {
		return spec.Kind{ID: "connect", Category: spec.CategoryConnect, Build: func(context.Context, spec.Values) (any, func(), error) {
			return c, c.close, nil
		}}
	}
	rec := func(id string, v int64) spec.Integration {
		return spec.Integration{ID: id, WorkspaceID: spec.DefaultWorkspace, Category: spec.CategoryConnect, Kind: "connect", Scope: spec.ScopeAll, Version: v}
	}
	l, w := NewLive(LiveOptions{})
	defer w.Close()

	v3, v2 := &liveConn{name: "v3"}, &liveConn{name: "v2"}
	mustNil(t, l.Apply(ctx, rec("int_1", 3), kindFor(v3), spec.Values{}))
	mustNil(t, l.Apply(ctx, rec("int_1", 2), kindFor(v2), spec.Values{}))
	if st, _ := w.Connect.Status(ctx, "x"); st.Name != "v3/x" {
		t.Fatalf("live client = %q, want v3", st.Name)
	}
	if v2.closes.Load() != 1 {
		t.Fatalf("dropped client closes = %d, want 1", v2.closes.Load())
	}
	// A stale apply after a newer Clear does not bring the client back.
	mustNil(t, l.Clear(ctx, rec("int_1", 4)))
	late := &liveConn{name: "late"}
	mustNil(t, l.Apply(ctx, rec("int_1", 3), kindFor(late), spec.Values{}))
	if _, err := w.Connect.Status(ctx, "x"); err == nil {
		t.Fatal("stale apply re-configured a cleared category")
	}
	// A new row (after delete and create) starts again at version 1.
	fresh := &liveConn{name: "fresh"}
	mustNil(t, l.Apply(ctx, rec("int_2", 1), kindFor(fresh), spec.Values{}))
	if st, _ := w.Connect.Status(ctx, "x"); st.Name != "fresh/x" {
		t.Fatalf("live client = %q, want fresh", st.Name)
	}
}
