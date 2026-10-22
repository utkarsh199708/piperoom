package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const tpTopicName = "pg.public.orders"

// tpFake is a Kafka client with RateReader whose answers depend on since,
// so the window read and the two sample-bound reads can differ.
type tpFake struct {
	*fakeKafka
	ratesFn   func(since time.Time, topics []string) ([]clients.PartitionRate, error)
	rateCalls int
	lagCalls  int
}

func (f *tpFake) PartitionRates(_ context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	f.rateCalls++
	return f.ratesFn(since, topics)
}

// tpSamples is a SampleReader with fixed points.
type tpSamples struct {
	pts       []clients.SamplePoint
	err       error
	calls     int
	gotPID    string
	gotMetric string
}

func (s *tpSamples) Samples(_ context.Context, pid, metric string, _ time.Time) ([]clients.SamplePoint, error) {
	s.calls++
	s.gotPID, s.gotMetric = pid, metric
	return s.pts, s.err
}

func tpPipeline() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{
		ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders", "public.items"},
		ConsumerGroups: []string{"order-sync"},
	})
}

// newTP builds a fake whose window read answers counts on tpTopicName and
// whose group reads the given per-partition lag.
func newTP(counts []int64, lags []int64) *tpFake {
	f := &tpFake{}
	f.fakeKafka = &fakeKafka{lagFn: func(_ context.Context, g string) (clients.GroupLag, error) {
		f.lagCalls++
		gl := clients.GroupLag{Group: g, State: "Stable", Members: 2}
		for i, l := range lags {
			gl.Partitions = append(gl.Partitions, clients.PartitionLag{Topic: tpTopicName, Partition: int32(i), Committed: 10, End: 10 + l, Lag: l})
			gl.TotalLag += l
		}
		return gl, nil
	}}
	f.ratesFn = func(_ time.Time, _ []string) ([]clients.PartitionRate, error) {
		var out []clients.PartitionRate
		for i, c := range counts {
			out = append(out, clients.PartitionRate{Topic: tpTopicName, Partition: int32(i), End: 1000 + c, OffsetAtStart: 1000, Records: c})
		}
		return out, nil
	}
	return f
}

func runTP(t *testing.T, ctx context.Context, d Deps, input string) (tools.Result, error) {
	t.Helper()
	return (&throughput{d: d}).Run(ctx, json.RawMessage(input))
}

func dataJSON(t *testing.T, r tools.Result) string {
	t.Helper()
	b, err := json.Marshal(r.Data)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TC1: balanced partitions give no skew finding; one partition at 4x the
// median is named. A partition holding most of a group's lag is named too.
func TestThroughputSkew(t *testing.T) {
	tests := []struct {
		name   string
		counts []int64
		lags   []int64
		want   []string
		reject []string
	}{
		{"balanced", []int64{900, 900, 900, 900}, []int64{50, 50, 50, 50},
			[]string{"orders last 15m: no skew found", "→ even"}, []string{"skewed", "holds"}},
		{"hot partition at 4x", []int64{900, 900, 3600, 900}, nil,
			[]string{"pg.public.orders p2 at 4.0/s is 4.0x the median partition", "→ skewed"}, []string{"no skew found"}},
		{"at 3x is not skew (> 3x)", []int64{900, 900, 2700, 900}, nil, []string{"no skew found"}, []string{"skewed"}},
		{"one partition holds 81% of the lag", []int64{900, 900, 900, 900}, []int64{100, 100, 2430, 370},
			[]string{"pg.public.orders p2 holds 81% of order-sync's lag", "order-sync (Stable, 2 members): lag 3000"}, []string{"no skew found"}},
		{"small lag is not skew", []int64{900, 900, 900, 900}, []int64{0, 0, 40, 0}, []string{"no skew found"}, []string{"holds"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTP(tt.counts, tt.lags)
			r, err := runTP(t, tpPipeline(), Deps{Kafka: f}, `{"table":"public.orders"}`)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tt.want {
				if !strings.Contains(r.Summary, w) {
					t.Errorf("summary lacks %q:\n%s", w, r.Summary)
				}
			}
			for _, w := range tt.reject {
				if strings.Contains(r.Summary, w) {
					t.Errorf("summary has %q:\n%s", w, r.Summary)
				}
			}
		})
	}
}

// samplesAround returns two group_lag samples 14 minutes apart ending a
// minute ago, and the fake rates that make produced records between them
// produce*840 and the window's records produce*900.
func tpConsume(t *testing.T, producePerSec, consumePerSec int64) (*tpFake, *tpSamples) {
	t.Helper()
	now := time.Now()
	s0, s1 := now.Add(-15*time.Minute), now.Add(-time.Minute)
	const lag0 = 1000
	span := int64(s1.Sub(s0).Seconds())
	samples := &tpSamples{pts: []clients.SamplePoint{
		{At: now.Add(-29 * time.Minute), Value: 7},
		{At: s0, Value: lag0},
		{At: s1, Value: float64(lag0 + (producePerSec-consumePerSec)*span)},
	}}
	f := newTP(nil, []int64{10, 10})
	f.ratesFn = func(since time.Time, topics []string) ([]clients.PartitionRate, error) {
		if len(topics) != 1 || topics[0] != tpTopicName {
			t.Errorf("rates topics = %v, want only the group's topic", topics)
		}
		// Records written since: the produce rate times the time since.
		n := producePerSec * int64(now.Sub(since).Seconds())
		return []clients.PartitionRate{
			{Topic: tpTopicName, Partition: 0, Records: n / 2},
			{Topic: tpTopicName, Partition: 1, Records: n - n/2},
		}, nil
	}
	return f, samples
}

// TC2: consumption below production over the samples is "falling behind at
// N/s"; equal is "keeping up".
func TestThroughputConsumeVerdict(t *testing.T) {
	tests := []struct {
		name             string
		produce, consume int64
		want             string
		verdict          string
	}{
		{"falling behind", 410, 120, "falling behind at 290/s: consumer rate 120/s vs produce 410/s", verdictFallingBehind},
		{"keeping up", 410, 410, "keeping up: consumer rate 410/s vs produce 410/s", verdictKeepingUp},
		{"catching up", 100, 300, "catching up at 200/s: consumer rate 300/s vs produce 100/s", verdictCatchingUp},
		{"idle", 0, 0, "idle: nothing written or consumed", verdictIdle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, s := tpConsume(t, tt.produce, tt.consume)
			r, err := runTP(t, tpPipeline(), Deps{Kafka: f, Samples: s}, `{"table":"public.orders"}`)
			if err != nil {
				t.Fatal(err)
			}
			if first := strings.SplitN(r.Summary, "\n", 2)[0]; !strings.Contains(first, tt.want) {
				t.Fatalf("headline = %q, want %q\n%s", first, tt.want, r.Summary)
			}
			data := r.Data.(map[string]any)
			cons, ok := data["consumption"].(*tpConsumption)
			if !ok || cons.Verdict != tt.verdict {
				t.Fatalf("consumption = %+v, want verdict %s", data["consumption"], tt.verdict)
			}
			if s.gotPID != "orders" || s.gotMetric != clients.WatchMetricGroupLag {
				t.Fatalf("samples read for %s/%s", s.gotPID, s.gotMetric)
			}
			if f.rateCalls != 3 {
				t.Fatalf("rate reads = %d, want 3 (window, both samples)", f.rateCalls)
			}
		})
	}
}

func TestConsumeVerdictTolerance(t *testing.T) {
	for _, tt := range []struct {
		produced, consumed int64
		want               string
	}{
		{1000, 1000, verdictKeepingUp},
		{1000, 901, verdictKeepingUp},
		{1000, 899, verdictFallingBehind},
		{1000, 1101, verdictCatchingUp},
		{0, 50, verdictCatchingUp},
		{0, 0, verdictIdle},
	} {
		if got := consumeVerdict(tt.produced, tt.consumed); got != tt.want {
			t.Errorf("consumeVerdict(%d, %d) = %s, want %s", tt.produced, tt.consumed, got, tt.want)
		}
	}
}

// Without samples the tool says why there is no consumed rate, and still
// answers produce rates and lag.
func TestThroughputConsumeNotMeasurable(t *testing.T) {
	now := time.Now()
	stale := &tpSamples{pts: []clients.SamplePoint{{At: now.Add(-3 * time.Hour), Value: 1}, {At: now.Add(-2 * time.Hour), Value: 2}}}
	tests := []struct {
		name    string
		samples clients.SampleReader
		lag     func(*tpFake)
		want    string
	}{
		{"no sample reader", nil, nil, "this process has no group_lag watch samples"},
		{"one sample", &tpSamples{pts: []clients.SamplePoint{{At: now.Add(-time.Minute), Value: 1}}}, nil, "1 group_lag sample(s) in the window, 2 needed"},
		{"stale samples", stale, nil, "the watcher is off or stuck"},
		{"samples fail", &tpSamples{err: errors.New("db down")}, nil, "group_lag samples not read: db down"},
		{"group reads another pipeline's topic", &tpSamples{pts: []clients.SamplePoint{{At: now.Add(-15 * time.Minute)}, {At: now.Add(-time.Minute)}}},
			func(f *tpFake) {
				f.lagFn = func(context.Context, string) (clients.GroupLag, error) {
					return clients.GroupLag{Group: "order-sync", Partitions: []clients.PartitionLag{
						{Topic: tpTopicName, Lag: 1}, {Topic: "billing.secret_table", Lag: 5},
					}}, nil
				}
			}, "group order-sync also reads topics outside this pipeline"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTP([]int64{60, 60}, []int64{5, 5})
			if tt.lag != nil {
				tt.lag(f)
			}
			r, err := runTP(t, tpPipeline(), Deps{Kafka: f, Samples: tt.samples}, `{"table":"public.orders"}`)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(r.Summary, tt.want) || !strings.Contains(r.Summary, "pg.public.orders: 120 records") {
				t.Fatalf("summary:\n%s\nwant %q", r.Summary, tt.want)
			}
			if _, ok := r.Data.(map[string]any)["consumption"]; ok {
				t.Fatal("consumption reported without the reads it needs")
			}
			// The other pipeline's topic is never echoed (E6).
			if strings.Contains(r.Summary+dataJSON(t, r), "billing") {
				t.Fatalf("another pipeline's topic leaked:\n%s\n%s", r.Summary, dataJSON(t, r))
			}
		})
	}
}

// TC3: a cluster that cannot count by time answers lag only, and says so.
func TestThroughputRatesNotSupported(t *testing.T) {
	for _, tt := range []struct {
		name string
		k    func() clients.KafkaAdmin
	}{
		{"PartitionRates answers ErrNotSupported", func() clients.KafkaAdmin {
			f := newTP(nil, []int64{30, 270})
			f.ratesFn = func(time.Time, []string) ([]clients.PartitionRate, error) {
				return nil, fmt.Errorf("kafka: offsets after: %w", clients.ErrNotSupported)
			}
			return f
		}},
		{"client without RateReader", func() clients.KafkaAdmin { return newTP(nil, []int64{30, 270}).fakeKafka }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, err := runTP(t, tpPipeline(), Deps{Kafka: tt.k()}, `{"table":"public.orders"}`)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range []string{"rates not available from this cluster", "order-sync (Stable, 2 members): lag 300", "p1 holds 90% of order-sync's lag"} {
				if !strings.Contains(r.Summary, w) {
					t.Fatalf("summary lacks %q:\n%s", w, r.Summary)
				}
			}
			if d := r.Data.(map[string]any); d["rates_available"] != false || d["topics"] != nil {
				t.Fatalf("data = %v", d)
			}
		})
	}
	// Lag-only still has the lag trend from the samples.
	now := time.Now()
	f := newTP(nil, []int64{1})
	f.ratesFn = func(time.Time, []string) ([]clients.PartitionRate, error) { return nil, clients.ErrNotSupported }
	s := &tpSamples{pts: []clients.SamplePoint{{At: now.Add(-16 * time.Minute), Value: 100}, {At: now.Add(-time.Minute), Value: 1600}}}
	r, err := runTP(t, tpPipeline(), Deps{Kafka: f, Samples: s}, `{}`)
	if err != nil || !strings.Contains(r.Summary, "lag growing 100/min") || f.rateCalls != 1 {
		t.Fatalf("err=%v rateCalls=%d summary:\n%s", err, f.rateCalls, r.Summary)
	}
}

// TC4 + E2/E3: bad input and out-of-scope names are refused before any
// client call; no pipeline is refused.
func TestThroughputScopeAndInput(t *testing.T) {
	tests := []struct {
		name  string
		ctx   context.Context
		input string
		want  error
	}{
		{"other pipeline's table", tpPipeline(), `{"table":"billing.invoices"}`, tools.ErrOutOfScope},
		{"window too long", tpPipeline(), `{"window":"7h"}`, tools.ErrBadInput},
		{"window too short", tpPipeline(), `{"window":"30s"}`, tools.ErrBadInput},
		{"window not a duration", tpPipeline(), `{"window":"soon"}`, tools.ErrBadInput},
		{"bad json", tpPipeline(), `{"table":`, tools.ErrBadInput},
		{"unknown field", tpPipeline(), `{"topic":"x"}`, nil},
		{"no room pipeline", context.Background(), `{}`, tools.ErrNoScope},
		{"unscoped", tools.WithUnscoped(context.Background()), `{}`, tools.ErrBadInput},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTP([]int64{1}, []int64{1})
			s := &tpSamples{}
			_, err := runTP(t, tt.ctx, Deps{Kafka: f, Samples: s}, tt.input)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if f.rateCalls+f.lagCalls+s.calls != 0 {
				t.Fatalf("client called before refusing: rates %d lag %d samples %d", f.rateCalls, f.lagCalls, s.calls)
			}
		})
	}
}

// TC4: 500 partitions keep the Summary under the cap and Data bounded.
func TestThroughputManyPartitions(t *testing.T) {
	counts := make([]int64, 500)
	lags := make([]int64, 500)
	for i := range counts {
		counts[i], lags[i] = int64(1000+i), int64(i)
	}
	f := newTP(counts, lags)
	r, err := runTP(t, tpPipeline(), Deps{Kafka: f}, `{"table":"public.orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary %d bytes", len(r.Summary))
	}
	d := r.Data.(map[string]any)
	if parts := d["partitions"].([]tpPartition); len(parts) != maxThroughputRows || parts[0].Partition != 499 {
		t.Fatalf("partitions = %d rows, first p%d", len(parts), parts[0].Partition)
	}
	if d["partitions_omitted"] != 400 {
		t.Fatalf("omitted = %v", d["partitions_omitted"])
	}
	if !strings.Contains(r.Summary, "over 500 partitions") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
}

// E5: a failed rate read is an error; a failed group read is shown, not
// dropped; a deadline is an error.
func TestThroughputErrors(t *testing.T) {
	boom := errors.New("broker unreachable")
	f := newTP(nil, nil)
	f.ratesFn = func(time.Time, []string) ([]clients.PartitionRate, error) { return nil, boom }
	if _, err := runTP(t, tpPipeline(), Deps{Kafka: f}, `{}`); !errors.Is(err, boom) {
		t.Fatalf("rates error = %v", err)
	}

	f = newTP([]int64{5, 5}, nil)
	f.lagFn = func(context.Context, string) (clients.GroupLag, error) { return clients.GroupLag{}, boom }
	r, err := runTP(t, tpPipeline(), Deps{Kafka: f}, `{}`)
	if err != nil || !strings.Contains(r.Summary, "order-sync: lag not read: broker unreachable") {
		t.Fatalf("err=%v summary:\n%s", err, r.Summary)
	}

	ctx, cancel := context.WithCancel(tpPipeline())
	cancel()
	f = newTP([]int64{5}, nil)
	f.lagFn = func(ctx context.Context, _ string) (clients.GroupLag, error) { return clients.GroupLag{}, ctx.Err() }
	if _, err := runTP(t, ctx, Deps{Kafka: f}, `{}`); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled err = %v", err)
	}
}

// The tool reads every topic of the pipeline by default and only the
// pipeline's, and it is registered with the other F12 rate reads.
func TestThroughputTopicsAndRegistration(t *testing.T) {
	f := newTP([]int64{1}, nil)
	var got []string
	inner := f.ratesFn
	f.ratesFn = func(since time.Time, topics []string) ([]clients.PartitionRate, error) {
		got = topics
		return inner(since, topics)
	}
	if _, err := runTP(t, tpPipeline(), Deps{Kafka: f}, `{}`); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "pg.public.orders,pg.public.items" {
		t.Fatalf("topics = %v", got)
	}
	tool := newTool(t, f, "kafka_throughput")
	if tool.Kind() != tools.KindRead {
		t.Fatal("not a read tool")
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: &fakeKafka{}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("kafka_throughput"); ok {
		t.Fatal("registered for a client without RateReader")
	}
}
