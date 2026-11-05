package actions

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- fakes (X6c) ----

// tuneConnect is a production Connect with one connector; writes only with
// allowWrites.
type tuneConnect struct {
	mu          sync.Mutex
	typ         string
	state       string
	taskStates  []string
	cfg         map[string]string
	allowWrites bool
	patches     []map[string]string
	calls       []string
	validate    *connect.ValidateResult
}

func (f *tuneConnect) call(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *tuneConnect) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *tuneConnect) ListConnectors(context.Context) ([]string, error) {
	f.call("list")
	return []string{"orders-sink", "orders-src"}, nil
}

func (f *tuneConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	f.call("status")
	st := clients.ConnectorStatus{Name: name, Type: f.typ, State: "RUNNING"}
	if f.state != "" {
		st.State = f.state
	}
	states := f.taskStates
	if states == nil {
		states = []string{"RUNNING"}
	}
	for i, s := range states {
		st.Tasks = append(st.Tasks, clients.TaskStatus{ID: i, State: s})
	}
	return st, nil
}

func (f *tuneConnect) Config(context.Context, string) (map[string]string, error) {
	f.call("config")
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.cfg), nil
}

func (f *tuneConnect) RestartTask(context.Context, string, int) error { return errWriteRefused }
func (f *tuneConnect) RestartConnector(context.Context, string) error { return errWriteRefused }
func (f *tuneConnect) PauseConnector(context.Context, string) error   { return errWriteRefused }

func (f *tuneConnect) UpdateConnectorConfigKeys(_ context.Context, _ string, changes map[string]string) error {
	f.call("patch")
	if !f.allowWrites {
		return errWriteRefused
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patches = append(f.patches, maps.Clone(changes))
	maps.Copy(f.cfg, changes)
	return nil
}

func (f *tuneConnect) ValidateConfig(context.Context, string, map[string]string) (connect.ValidateResult, error) {
	f.call("validate")
	if f.validate != nil {
		return *f.validate, nil
	}
	return connect.ValidateResult{}, nil
}

var (
	_ clients.ConnectClient    = (*tuneConnect)(nil)
	_ clients.ConnectFixWriter = (*tuneConnect)(nil)
	_ configValidator          = (*tuneConnect)(nil)
)

// tuneKafka holds per partition the timestamp of every record, oldest
// first, and answers offsets by time from them.
type tuneKafka struct {
	mu    sync.Mutex
	parts map[string][][]time.Time // topic -> partition -> record timestamps
	calls int
	err   error
}

func (k *tuneKafka) hit() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.calls++
}

func (k *tuneKafka) callCount() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.calls
}

func (k *tuneKafka) Topics(context.Context) ([]string, error) { k.hit(); return nil, nil }
func (k *tuneKafka) ConsumerGroups(context.Context) ([]string, error) {
	k.hit()
	return nil, nil
}
func (k *tuneKafka) GroupLag(context.Context, string) (clients.GroupLag, error) {
	k.hit()
	return clients.GroupLag{}, nil
}
func (k *tuneKafka) FindByKey(context.Context, string, string, time.Time, int) ([]clients.RecordRef, error) {
	k.hit()
	return nil, nil
}

func (k *tuneKafka) TopicOffsets(_ context.Context, topic string) ([]clients.PartitionOffsets, error) {
	k.hit()
	ps, ok := k.parts[topic]
	if !ok {
		return nil, clients.ErrNotFound
	}
	out := make([]clients.PartitionOffsets, len(ps))
	for i, p := range ps {
		out[i] = clients.PartitionOffsets{Partition: int32(i), End: int64(len(p))}
	}
	return out, nil
}

func (k *tuneKafka) PartitionRates(_ context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	k.hit()
	if k.err != nil {
		return nil, k.err
	}
	var out []clients.PartitionRate
	for _, topic := range topics {
		for i, p := range k.parts[topic] {
			at := sort.Search(len(p), func(j int) bool { return !p[j].Before(since) })
			r := clients.PartitionRate{Topic: topic, Partition: int32(i), End: int64(len(p)), OffsetAtStart: -1}
			if at < len(p) {
				r.OffsetAtStart = int64(at)
				r.Records = r.End - r.OffsetAtStart
			}
			out = append(out, r)
		}
	}
	return out, nil
}

var (
	_ clients.KafkaReader = (*tuneKafka)(nil)
	_ TuningKafkaReader   = (*tuneKafka)(nil)
)

// writeRate appends perMin records a minute, evenly spaced, to partition p
// of topic over [from, to).
func (k *tuneKafka) writeRate(topic string, p int, from, to time.Time, perMin int) {
	if k.parts == nil {
		k.parts = map[string][][]time.Time{}
	}
	for len(k.parts[topic]) <= p {
		k.parts[topic] = append(k.parts[topic], nil)
	}
	step := time.Minute / time.Duration(perMin)
	for t := from; t.Before(to); t = t.Add(step) {
		k.parts[topic][p] = append(k.parts[topic][p], t)
	}
}

// tuneSamples is a group_lag series.
type tuneSamples struct {
	pts   []clients.SamplePoint
	calls int
}

func (s *tuneSamples) Samples(_ context.Context, pid, metric string, since time.Time) ([]clients.SamplePoint, error) {
	s.calls++
	if pid != "orders" || metric != clients.WatchMetricGroupLag {
		return nil, errors.New("unexpected series")
	}
	var out []clients.SamplePoint
	for _, p := range s.pts {
		if !p.At.Before(since) {
			out = append(out, p)
		}
	}
	return out, nil
}

func tunePipeline() owners.Pipeline {
	return owners.Pipeline{
		ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"},
		Connectors: []string{"orders-src", "orders-sink"}, ConsumerGroups: []string{"connect-orders-sink"},
	}
}

func tuneCtx() context.Context { return tools.WithPipeline(context.Background(), tunePipeline()) }

const ordersTopic = "pg.public.orders"

func sinkConnect() *tuneConnect {
	return &tuneConnect{typ: "sink", cfg: map[string]string{"connector.class": "io.example.JdbcSinkConnector", "topics": ordersTopic, "tasks.max": "1"}}
}

func sourceConnect() *tuneConnect {
	return &tuneConnect{typ: "source", cfg: map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "max.batch.size": "2048", "tasks.max": "1"}}
}

func threePartitions() *tuneKafka {
	k := &tuneKafka{}
	for p := range 3 {
		k.writeRate(ordersTopic, p, time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC), time.Date(2026, 10, 22, 8, 1, 0, 0, time.UTC), 1)
	}
	return k
}

func newTune(d TuningDeps) *tuneThroughput {
	return &tuneThroughput{d: d, stage: Deps{Staging: d.Staging}}
}

func tuneParams(connector string, changes map[string]any) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"connector": connector, "changes": changes})
	return b
}

// ---- registration and class ----

func TestTuneRegistersAsPerformance(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterTuning(reg, TuningDeps{}); err != nil {
		t.Fatalf("RegisterTuning: %v", err)
	}
	a, ok := reg.Get(tuneName)
	if !ok || ClassOf(a) != ClassPerformance || a.Layer() != "kafka-connect" {
		t.Fatalf("registered %v class %q", ok, ClassOf(a))
	}
	for _, k := range TunableKeys() {
		if !strings.Contains(a.Describe(), k.Key) {
			t.Fatalf("Describe does not list %s", k.Key)
		}
	}
	props := a.ParamsSchema()["properties"].(map[string]any)["changes"].(map[string]any)["properties"].(map[string]any)
	if len(props) != len(TunableKeys()) {
		t.Fatalf("changes schema lists %d keys, the allow-list %d", len(props), len(TunableKeys()))
	}
}

// ---- TC1: the allow-list, with no I/O ----

func TestTuneRefusesKeysOffTheAllowList(t *testing.T) {
	cases := []struct {
		name    string
		changes map[string]any
		want    error
		text    string
	}{
		{"decimal.handling.mode", map[string]any{"decimal.handling.mode": "precise"}, ErrNotTunable, "not in the performance allow-list"},
		{"secret-looking key", map[string]any{"database.password": "4"}, ErrNotTunable, "looks like a credential"},
		{"api key", map[string]any{"sasl.api.key": "4"}, ErrNotTunable, "looks like a credential"},
		{"not a key name", map[string]any{"tasks max": "4"}, ErrNotTunable, "not a config key name"},
		{"not an integer", map[string]any{"tasks.max": "four"}, ErrTuningBounds, "integer from 1 to 128"},
		{"static bound", map[string]any{"max.batch.size": "999999"}, ErrTuningBounds, "from 1 to 20480"},
		{"float", map[string]any{"tasks.max": 2.5}, ErrTuningBounds, "integer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, k := sourceConnect(), threePartitions()
			a := newTune(TuningDeps{Connect: c, Kafka: k})
			raw := tuneParams("orders-src", tc.changes)
			for _, err := range []error{a.Validate(tuneCtx(), raw), execErr(a, raw)} {
				if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.text) {
					t.Fatalf("err = %v, want %v containing %q", err, tc.want, tc.text)
				}
				if strings.Contains(err.Error(), "999999") || strings.Contains(err.Error(), "precise") {
					t.Fatalf("error echoes a value: %v", err)
				}
			}
			if c.callCount() != 0 || k.callCount() != 0 {
				t.Fatalf("a refused key read the target: connect %d kafka %d calls", c.callCount(), k.callCount())
			}
		})
	}
}

func execErr(a *tuneThroughput, raw json.RawMessage) error {
	_, err := a.Execute(tuneCtx(), raw)
	return err
}

// A key of the other connector type is refused once the type is read.
func TestTuneRefusesKeyOfOtherConnectorType(t *testing.T) {
	a := newTune(TuningDeps{Connect: sourceConnect(), Kafka: threePartitions()})
	err := a.Validate(tuneCtx(), tuneParams("orders-src", map[string]any{"consumer.override.max.poll.records": "500"}))
	if !errors.Is(err, ErrNotTunable) || !strings.Contains(err.Error(), "applies to sink connectors only") {
		t.Fatalf("err = %v", err)
	}
}

// ---- TC2: target bounds ----

func TestTuneTasksMaxBoundedByPartitions(t *testing.T) {
	a := newTune(TuningDeps{Connect: sinkConnect(), Kafka: threePartitions()})
	err := a.Validate(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "64"}))
	if !errors.Is(err, ErrTuningBounds) || !strings.Contains(err.Error(), "at most 3") || !strings.Contains(err.Error(), "61 tasks would sit idle") {
		t.Fatalf("err = %v, want the partition bound", err)
	}
	if err := a.Validate(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": 3})); err != nil {
		t.Fatalf("tasks.max 3 on 3 partitions: %v", err)
	}
	// A source's tasks.max has no partition bound (it writes, it does not read).
	src := newTune(TuningDeps{Connect: sourceConnect()})
	if err := src.Validate(tuneCtx(), tuneParams("orders-src", map[string]any{"tasks.max": "8"})); err != nil {
		t.Fatalf("source tasks.max: %v", err)
	}
}

func TestTuneQueueAboveBatch(t *testing.T) {
	cases := []struct {
		name    string
		cfg     map[string]string
		changes map[string]any
		ok      bool
	}{
		{"queue at the configured batch", map[string]string{"max.batch.size": "2048"}, map[string]any{"max.queue.size": "2048"}, false},
		{"batch above the default queue", map[string]string{}, map[string]any{"max.batch.size": "10000"}, false},
		{"both raised together", map[string]string{}, map[string]any{"max.batch.size": "10000", "max.queue.size": "40000"}, true},
		{"queue a reference", map[string]string{"max.queue.size": "${file:/x:q}"}, map[string]any{"max.batch.size": "100"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := sourceConnect()
			c.cfg = tc.cfg
			err := newTune(TuningDeps{Connect: c}).Validate(tuneCtx(), tuneParams("orders-src", tc.changes))
			if tc.ok != (err == nil) || (err != nil && !errors.Is(err, ErrTuningBounds)) {
				t.Fatalf("err = %v, want ok %v", err, tc.ok)
			}
			if err != nil && strings.Contains(err.Error(), "${file") {
				t.Fatalf("error echoes a config value: %v", err)
			}
		})
	}
}

func TestTuneRefusesFailingConnector(t *testing.T) {
	c := sinkConnect()
	c.taskStates = []string{"RUNNING", "FAILED"}
	err := newTune(TuningDeps{Connect: c, Kafka: threePartitions()}).Validate(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "2"}))
	if err == nil || !strings.Contains(err.Error(), "task 1 is FAILED") || !strings.Contains(err.Error(), "not a fix for a failing connector") {
		t.Fatalf("err = %v", err)
	}
}

// ---- E3: scope ----

func TestTuneScope(t *testing.T) {
	c, k := sinkConnect(), threePartitions()
	a := newTune(TuningDeps{Connect: c, Kafka: k})
	raw := tuneParams("billing-sink", map[string]any{"tasks.max": "2"})
	if err := a.Validate(tuneCtx(), raw); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Validate: %v", err)
	}
	if _, err := a.Verify(tuneCtx(), raw, time.Now().Add(-time.Hour)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Verify: %v", err)
	}
	if _, err := a.Verify(context.Background(), tuneParams("orders-sink", map[string]any{"tasks.max": "2"}), time.Now().Add(-time.Hour)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Verify without a pipeline: %v", err)
	}
	if err := a.CheckScope(tunePipeline(), raw); err == nil {
		t.Fatal("CheckScope passed another pipeline's connector")
	}
	if c.callCount() != 0 || k.callCount() != 0 {
		t.Fatalf("out-of-scope calls reached the clients: %d/%d", c.callCount(), k.callCount())
	}
	// A sink that also reads another pipeline's topic: refused before any
	// partition read.
	c.cfg["topics"] = ordersTopic + ",billing.public.invoices"
	err := a.Validate(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "2"}))
	if !errors.Is(err, tools.ErrOutOfScope) || k.callCount() != 0 {
		t.Fatalf("err = %v, kafka calls %d", err, k.callCount())
	}
}

// ---- Execute ----

func TestTuneExecute(t *testing.T) {
	c := sinkConnect()
	c.allowWrites = true
	a := newTune(TuningDeps{Connect: c, Kafka: threePartitions()})
	out, err := a.Execute(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "3", "consumer.override.max.poll.records": "1000"}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(c.patches) != 1 || c.patches[0]["tasks.max"] != "3" || c.patches[0]["consumer.override.max.poll.records"] != "1000" {
		t.Fatalf("patches = %v", c.patches)
	}
	// The rollback cannot be offered: max.poll.records was unset.
	if !strings.Contains(out, "tasks.max: 1 → 3") || strings.Contains(out, "rollback:") {
		t.Fatalf("result = %q", out)
	}
	// Run again: nothing to write.
	out, err = a.Execute(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "3"}))
	if err != nil || !strings.Contains(out, "no change made") || len(c.patches) != 1 {
		t.Fatalf("second run %q %v, patches %d", out, err, len(c.patches))
	}
	out, err = a.Execute(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "2"}))
	if err != nil || !strings.Contains(out, `rollback: connect_tune_throughput {"changes":{"tasks.max":"3"},"connector":"orders-sink"}`) {
		t.Fatalf("result %q %v, want the rollback values", out, err)
	}
}

func TestTuneExecuteRefusesWithoutWriter(t *testing.T) {
	a := newTune(TuningDeps{Connect: readerOnly{sinkConnect()}, Kafka: threePartitions()})
	if _, err := a.Execute(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "2"})); err == nil || !strings.Contains(err.Error(), "write mode") {
		t.Fatalf("err = %v", err)
	}
	// The allow-list is checked again in Execute, against the state now: a
	// sink whose topic lost partitions is refused before the write.
	c := sinkConnect()
	c.allowWrites = true
	k := threePartitions()
	k.parts[ordersTopic] = k.parts[ordersTopic][:1]
	if _, err := newTune(TuningDeps{Connect: c, Kafka: k}).Execute(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "2"})); !errors.Is(err, ErrTuningBounds) || len(c.patches) != 0 {
		t.Fatalf("err = %v, patches %d", err, len(c.patches))
	}
}

// readerOnly hides every write method of a Connect client.
type readerOnly struct{ clients.ConnectReader }

// ---- Preview: the tuning list is the rollback ----

func TestTunePreview(t *testing.T) {
	c := sourceConnect()
	c.cfg["poll.interval.ms"] = "500"
	a := newTune(TuningDeps{Connect: c})
	res, err := a.Preview(tuneCtx(), tuneParams("orders-src", map[string]any{"max.batch.size": "4096", "max.queue.size": "16384", "poll.interval.ms": "100"}))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	want := []TuningChange{
		{Key: "max.batch.size", Current: "2048", Proposed: "4096"},
		{Key: "max.queue.size", Current: "", Proposed: "16384"},
		{Key: "poll.interval.ms", Current: "500", Proposed: "100"},
	}
	if len(res.Tuning) != len(want) {
		t.Fatalf("tuning = %+v", res.Tuning)
	}
	for i := range want {
		if res.Tuning[i] != want[i] {
			t.Fatalf("tuning[%d] = %+v, want %+v", i, res.Tuning[i], want[i])
		}
	}
	if res.Reversible || res.DryRun == nil || !res.DryRun.OK || !strings.Contains(res.Current, "max.queue.size = unset") {
		t.Fatalf("preview = %+v", res)
	}
	// Every key set: the rollback is offered with the values now.
	res, err = a.Preview(tuneCtx(), tuneParams("orders-src", map[string]any{"max.batch.size": "4096", "poll.interval.ms": "100"}))
	if err != nil || !res.Reversible || res.Rollback != `connect_tune_throughput {"changes":{"max.batch.size":"2048","poll.interval.ms":"500"},"connector":"orders-src"}` {
		t.Fatalf("preview %+v %v", res, err)
	}
	// Preview never writes.
	for _, call := range c.calls {
		if call == "patch" {
			t.Fatal("Preview wrote")
		}
	}
}

// ---- TC3: the verify table ----

var tuneT0 = time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)

// sourceKafka writes before records a minute for the 15 min before t0 and
// after a minute from t0 on, over 2 partitions.
func sourceKafka(before, after int) *tuneKafka {
	k := &tuneKafka{}
	for p := range 2 {
		k.writeRate(ordersTopic, p, tuneT0.Add(-20*time.Minute), tuneT0, before/2)
		k.writeRate(ordersTopic, p, tuneT0, tuneT0.Add(20*time.Minute), after/2)
	}
	return k
}

func TestTuneVerifyTable(t *testing.T) {
	cases := []struct {
		name          string
		before, after int
		outcome       string
	}{
		{"+15 % improved", 1000, 1150, events.PerformanceImproved},
		{"+2 % no change", 1000, 1020, events.PerformanceNoChange},
		{"-20 % worse", 1000, 800, events.PerformanceWorse},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := sourceConnect()
			a := newTune(TuningDeps{Connect: c, Kafka: sourceKafka(tc.before, tc.after), now: func() time.Time { return tuneT0.Add(18 * time.Minute) }})
			raw := tuneParams("orders-src", map[string]any{"max.batch.size": "4096"})
			res, err := a.Verify(tuneCtx(), raw, tuneT0)
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if res.Unit != events.PerformanceUnitRecordsPerMinute || res.Before != int64(tc.before) || res.After != int64(tc.after) || res.LagSlopeBefore != nil {
				t.Fatalf("result = %+v", res)
			}
			if got := events.PerformanceOutcomeFor(res.Before, res.After, res.LagSlopeBefore, res.LagSlopeAfter); got != tc.outcome {
				t.Fatalf("outcome = %s, want %s", got, tc.outcome)
			}
			if !strings.Contains(res.Detail, "source orders-src") || len(res.Detail) > 200 {
				t.Fatalf("detail = %q", res.Detail)
			}
			// worse: the rollback values the card shows are the preview's
			// current values, read before the step.
			if tc.outcome == events.PerformanceWorse {
				pv, err := a.Preview(tuneCtx(), raw)
				if err != nil || len(pv.Tuning) != 1 || pv.Tuning[0].Current != "2048" || !pv.Reversible {
					t.Fatalf("preview %+v %v, want the rollback value 2048", pv, err)
				}
			}
		})
	}
}

func TestTuneVerifyWaitsForTheAfterWindow(t *testing.T) {
	a := newTune(TuningDeps{Connect: sourceConnect(), Kafka: sourceKafka(1000, 1000), now: func() time.Time { return tuneT0.Add(10 * time.Minute) }})
	_, err := a.Verify(tuneCtx(), tuneParams("orders-src", map[string]any{"tasks.max": "1"}), tuneT0)
	if err == nil || errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "after window closes at 10:17:00") {
		t.Fatalf("err = %v, want a plain not-yet error", err)
	}
}

func TestTuneVerifyErrors(t *testing.T) {
	now := func() time.Time { return tuneT0.Add(18 * time.Minute) }
	raw := tuneParams("orders-src", map[string]any{"tasks.max": "1"})
	// No Kafka client: unverifiable.
	a := newTune(TuningDeps{Connect: sourceConnect(), now: now})
	if _, err := a.Verify(tuneCtx(), raw, tuneT0); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("no Kafka client: %v", err)
	}
	// A backend error is wrapped and is one failed call, not unverifiable.
	k := sourceKafka(1000, 1000)
	k.err = errors.New("broker down")
	a = newTune(TuningDeps{Connect: sourceConnect(), Kafka: k, now: now})
	if _, err := a.Verify(tuneCtx(), raw, tuneT0); err == nil || errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "broker down") {
		t.Fatalf("broker error: %v", err)
	}
	// A cluster without offsets by time answers ErrNotSupported.
	k.err = clients.ErrNotSupported
	if _, err := a.Verify(tuneCtx(), raw, tuneT0); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("unsupported: %v", err)
	}
	if _, err := a.Verify(tuneCtx(), raw, time.Time{}); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("zero executedAt: %v", err)
	}
}

// sinkSetup is a sink consuming 900/min while 1000/min are written before the
// step, and consumedAfter/min after it; group_lag samples every 15 min.
func sinkSetup(consumedAfter int) (*tuneConnect, *tuneKafka, *tuneSamples) {
	k := &tuneKafka{}
	for p := range 2 {
		k.writeRate(ordersTopic, p, tuneT0.Add(-40*time.Minute), tuneT0.Add(40*time.Minute), 500)
	}
	// Lag grows by produced - consumed per minute.
	lag := func(at time.Time) float64 {
		m := at.Sub(tuneT0).Minutes()
		if m <= 0 {
			return 50000 + 100*m // +100/min before
		}
		return 50000 + float64(1000-consumedAfter)*m
	}
	s := &tuneSamples{}
	for _, off := range []time.Duration{-31, -16, -1, 3, 18, 33} {
		at := tuneT0.Add(off * time.Minute)
		s.pts = append(s.pts, clients.SamplePoint{At: at, Value: lag(at)})
	}
	return sinkConnect(), k, s
}

func TestTuneVerifySink(t *testing.T) {
	c, k, s := sinkSetup(1200)
	a := newTune(TuningDeps{Connect: c, Kafka: k, Samples: s, now: func() time.Time { return tuneT0.Add(19 * time.Minute) }})
	res, err := a.Verify(tuneCtx(), tuneParams("orders-sink", map[string]any{"tasks.max": "2"}), tuneT0)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if res.Before != 900 || res.After != 1200 || res.LagSlopeBefore == nil || *res.LagSlopeBefore != 100 || res.LagSlopeAfter == nil || *res.LagSlopeAfter != -200 {
		t.Fatalf("result = %+v (slopes %v %v)", res, deref(res.LagSlopeBefore), deref(res.LagSlopeAfter))
	}
	if got := events.PerformanceOutcomeFor(res.Before, res.After, res.LagSlopeBefore, res.LagSlopeAfter); got != events.PerformanceImproved {
		t.Fatalf("outcome %s", got)
	}
	if !strings.Contains(res.Detail, "before 09:44-09:59, after 10:03-10:18 UTC") {
		t.Fatalf("detail %q", res.Detail)
	}
}

func deref(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestTuneVerifySinkUnverifiable(t *testing.T) {
	raw := tuneParams("orders-sink", map[string]any{"tasks.max": "2"})
	at := func(m time.Duration) func() time.Time { return func() time.Time { return tuneT0.Add(m * time.Minute) } }

	// The watcher is off: no samples.
	c, k, _ := sinkSetup(1000)
	if _, err := newTune(TuningDeps{Connect: c, Kafka: k, now: at(19)}).Verify(tuneCtx(), raw, tuneT0); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("no samples: %v", err)
	}
	// The watcher was young: no sample before the step.
	c, k, s := sinkSetup(1000)
	s.pts = s.pts[3:]
	if _, err := newTune(TuningDeps{Connect: c, Kafka: k, Samples: s, now: at(19)}).Verify(tuneCtx(), raw, tuneT0); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("no before samples: %v", err)
	}
	// The after window's second sample is not written yet: a plain error
	// until the timeout, then unverifiable.
	c, k, s = sinkSetup(1000)
	s.pts = s.pts[:4]
	if _, err := newTune(TuningDeps{Connect: c, Kafka: k, Samples: s, now: at(19)}).Verify(tuneCtx(), raw, tuneT0); err == nil || errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("after sample pending: %v", err)
	}
	if _, err := newTune(TuningDeps{Connect: c, Kafka: k, Samples: s, now: at(31)}).Verify(tuneCtx(), raw, tuneT0); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("after sample missing at the timeout: %v", err)
	}
	// group_lag also sums another group.
	c, k, s = sinkSetup(1000)
	p := tunePipeline()
	p.ConsumerGroups = append(p.ConsumerGroups, "audit")
	ctx := tools.WithPipeline(context.Background(), p)
	if _, err := newTune(TuningDeps{Connect: c, Kafka: k, Samples: s, now: at(19)}).Verify(ctx, raw, tuneT0); !errors.Is(err, clients.ErrNotSupported) || s.calls != 0 {
		t.Fatalf("other groups: %v (samples read %d)", err, s.calls)
	}
}

// ---- Stage ----

func TestTuneStage(t *testing.T) {
	st := &stageConnect{t: t, cfg: map[string]string{"connector.class": "x", "max.batch.size": "2048"},
		status: func(int) clients.ConnectorStatus {
			s := running(1)
			s.Type = "source"
			return s
		}}
	prod := sourceConnect()
	a := newTune(TuningDeps{Connect: prod, Staging: &Staging{Connect: st}})
	a.stage = fastStage(a.stage)
	ctx := stageCtx()
	res, err := a.Stage(ctx, tuneParams("orders-src", map[string]any{"max.batch.size": "4096"}))
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if !res.Applied || !res.Healthy || res.Leftover != "" || len(st.patches) != 1 || st.patches[0]["max.batch.size"] != "4096" {
		t.Fatalf("result %+v patches %v", res, st.patches)
	}
	if len(st.puts) != 1 || st.puts[0]["max.batch.size"] != "2048" {
		t.Fatalf("staging not restored: %v", st.puts)
	}
	if prod.callCount() != 0 {
		t.Fatalf("Stage called production: %v", prod.calls)
	}
	// Staging's own guard: a queue at the batch size is refused there with
	// nothing written.
	res, err = a.Stage(ctx, tuneParams("orders-src", map[string]any{"max.queue.size": "2048"}))
	if err != nil || res.Applied || res.DryRun == nil || res.DryRun.OK || len(st.patches) != 1 {
		t.Fatalf("guard %+v %v", res, err)
	}
	// No staging: nothing to stage.
	res, err = newTune(TuningDeps{Connect: prod}).Stage(ctx, tuneParams("orders-src", map[string]any{"max.batch.size": "4096"}))
	if err != nil || res.Detail != events.StageNoStaging {
		t.Fatalf("no staging %+v %v", res, err)
	}
}
