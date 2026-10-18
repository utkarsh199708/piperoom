package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const (
	rsGroup  = "orders-sink"
	rsTopic  = "pg.public.orders"
	rsTopic2 = "pg.public.customers"
)

// rsNow is the fixed clock of these tests.
var rsNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

func rsPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:             "pg-orders",
		TopicPrefix:    "pg",
		Connectors:     []string{"pg-orders-connector"},
		SourceTables:   []string{"public.orders", "public.customers"},
		ConsumerGroups: []string{rsGroup},
	}
}

func rsCtx() context.Context { return tools.WithPipeline(context.Background(), rsPipeline()) }

// fakePlanner is a read-only clients.GroupResetPlanner. After a reset by
// fakeWriter its plan reports the targets as committed.
type fakePlanner struct {
	mu       sync.Mutex
	moves    []clients.OffsetMove
	inactive error // CheckGroupInactive's answer
	planErr  error
	// planErrAfter fails PlanGroupReset only once the writer reset (re-read).
	planErrAfter error
	block        bool
	reset        bool
	calls        []string
}

func (f *fakePlanner) rec(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *fakePlanner) CheckGroupInactive(ctx context.Context, group string) error {
	f.rec("CheckGroupInactive " + group)
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.inactive
}

func (f *fakePlanner) PlanGroupReset(ctx context.Context, group string, to clients.OffsetTarget) ([]clients.OffsetMove, error) {
	f.rec("PlanGroupReset " + group)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reset {
		if f.planErrAfter != nil {
			return nil, f.planErrAfter
		}
		out := make([]clients.OffsetMove, len(f.moves))
		for i, m := range f.moves {
			m.Committed = m.Target
			out[i] = m
		}
		return out, nil
	}
	if f.planErr != nil {
		return nil, f.planErr
	}
	return append([]clients.OffsetMove(nil), f.moves...), nil
}

// fakeKafkaWriter records resets; Produce is never used here and refuses.
type fakeKafkaWriter struct {
	p      *fakePlanner
	moved  int
	err    error
	resets []string
	ctxs   []context.Context
}

func (f *fakeKafkaWriter) ResetGroupOffsets(ctx context.Context, group string, to clients.OffsetTarget) (int, error) {
	f.resets = append(f.resets, group+" "+targetText(to))
	f.ctxs = append(f.ctxs, ctx)
	if f.err != nil {
		return f.moved, f.err
	}
	f.p.mu.Lock()
	f.p.reset = true
	f.p.mu.Unlock()
	return f.moved, nil
}

func (f *fakeKafkaWriter) Produce(context.Context, string, []clients.Record) (int, error) {
	return 0, errors.New("fake: produce not used")
}

func twoTopicMoves() []clients.OffsetMove {
	return []clients.OffsetMove{
		{Topic: rsTopic2, Partition: 0, Committed: 40, Target: 0, End: 50},
		{Topic: rsTopic, Partition: 0, Committed: 120, Target: 0, End: 150},
		{Topic: rsTopic, Partition: 1, Committed: 98, Target: 10, End: 100},
	}
}

func oneTopicMoves() []clients.OffsetMove {
	return []clients.OffsetMove{
		{Topic: rsTopic, Partition: 0, Committed: 120, Target: 0, End: 150},
		{Topic: rsTopic, Partition: 1, Committed: 98, Target: 10, End: 100},
	}
}

func newReset(p *fakePlanner, w *fakeKafkaWriter) *resetConsumerOffsets {
	d := KafkaDeps{now: func() time.Time { return rsNow }}
	if p != nil {
		d.Planner = p
	}
	if w != nil {
		d.Writer = w
	}
	return &resetConsumerOffsets{d: d}
}

func rsParams(t *testing.T, m map[string]any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func earliestParams(t *testing.T, topics ...string) json.RawMessage {
	if len(topics) == 0 {
		topics = []string{rsTopic}
	}
	return rsParams(t, map[string]any{"group": rsGroup, "topics": topics, "to": "earliest"})
}

func TestResetParse(t *testing.T) {
	past := rsNow.Add(-time.Hour)
	tests := []struct {
		name    string
		params  string
		want    clients.OffsetTarget
		wantErr string
	}{
		{name: "earliest", params: `{"group":"g","topics":["t"],"to":"earliest"}`, want: clients.OffsetTarget{Earliest: true}},
		{name: "timestamp", params: `{"group":"g","topics":["t"],"to":"timestamp","timestamp":"` + past.Format(time.RFC3339) + `"}`, want: clients.OffsetTarget{Timestamp: past}},
		{name: "timestamp with offset zone", params: `{"group":"g","topics":["t"],"to":"timestamp","timestamp":"2026-10-18T13:00:00+02:00"}`, want: clients.OffsetTarget{Timestamp: rsNow.Add(-time.Hour)}},
		{name: "future timestamp", params: `{"group":"g","topics":["t"],"to":"timestamp","timestamp":"2026-10-18T12:00:01Z"}`, wantErr: "in the future"},
		{name: "timestamp missing", params: `{"group":"g","topics":["t"],"to":"timestamp"}`, wantErr: "needs timestamp"},
		{name: "timestamp not RFC 3339", params: `{"group":"g","topics":["t"],"to":"timestamp","timestamp":"yesterday"}`, wantErr: "not RFC 3339"},
		{name: "timestamp before 1970", params: `{"group":"g","topics":["t"],"to":"timestamp","timestamp":"1969-12-31T00:00:00Z"}`, wantErr: "before 1970"},
		{name: "earliest with timestamp", params: `{"group":"g","topics":["t"],"to":"earliest","timestamp":"2026-10-18T11:00:00Z"}`, wantErr: "only allowed with to=timestamp"},
		{name: "latest not offered", params: `{"group":"g","topics":["t"],"to":"latest"}`, wantErr: "not one of earliest, timestamp"},
		{name: "to missing", params: `{"group":"g","topics":["t"]}`, wantErr: "not one of earliest, timestamp"},
		{name: "explicit offsets not offered", params: `{"group":"g","topics":["t"],"to":"earliest","offsets":{"0":5}}`, wantErr: "unknown field"},
		{name: "group missing", params: `{"topics":["t"],"to":"earliest"}`, wantErr: "group is required"},
		{name: "topics missing", params: `{"group":"g","to":"earliest"}`, wantErr: "topics is required"},
		{name: "empty topic", params: `{"group":"g","topics":[""],"to":"earliest"}`, wantErr: "empty name"},
		{name: "duplicate topic", params: `{"group":"g","topics":["t","t"],"to":"earliest"}`, wantErr: "listed twice"},
		{name: "not json", params: `{`, wantErr: "bad params"},
	}
	a := newReset(nil, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, got, err := a.parse(json.RawMessage(tc.params))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v; want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Earliest != tc.want.Earliest || !got.Timestamp.Equal(tc.want.Timestamp) {
				t.Fatalf("target = %+v; want %+v", got, tc.want)
			}
		})
	}
}

func TestResetCheckScope(t *testing.T) {
	a := newReset(nil, nil)
	noPrefix := rsPipeline()
	noPrefix.TopicPrefix = ""
	tests := []struct {
		name     string
		pipeline owners.Pipeline
		params   string
		ok       bool
	}{
		{name: "in scope", pipeline: rsPipeline(), params: `{"group":"orders-sink","topics":["pg.public.orders"],"to":"earliest"}`, ok: true},
		{name: "two topics in scope", pipeline: rsPipeline(), params: `{"group":"orders-sink","topics":["pg.public.orders","pg.public.customers"],"to":"earliest"}`, ok: true},
		{name: "group of another pipeline", pipeline: rsPipeline(), params: `{"group":"billing-sink","topics":["pg.public.orders"],"to":"earliest"}`},
		{name: "topic of another pipeline", pipeline: rsPipeline(), params: `{"group":"orders-sink","topics":["pg.public.orders","billing.public.orders"],"to":"earliest"}`},
		{name: "suffix lookalike topic", pipeline: rsPipeline(), params: `{"group":"orders-sink","topics":["xpg.public.orders"],"to":"earliest"}`},
		{name: "pipeline without topic_prefix", pipeline: noPrefix, params: `{"group":"orders-sink","topics":["pg.public.orders"],"to":"earliest"}`},
		{name: "no topics", pipeline: rsPipeline(), params: `{"group":"orders-sink","to":"earliest"}`},
		{name: "no group", pipeline: rsPipeline(), params: `{"topics":["pg.public.orders"],"to":"earliest"}`},
		{name: "bad json", pipeline: rsPipeline(), params: `[`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := a.CheckScope(tc.pipeline, json.RawMessage(tc.params))
			if tc.ok != (err == nil) {
				t.Fatalf("CheckScope = %v; want ok=%v", err, tc.ok)
			}
		})
	}
	err := a.CheckScope(rsPipeline(), json.RawMessage(`{"group":"billing-sink","topics":["pg.public.orders"]}`))
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out-of-scope group: %v; want tools.ErrOutOfScope", err)
	}
}

func TestResetValidate(t *testing.T) {
	brokerDown := errors.New("dial tcp 127.0.0.1:29092: connect: connection refused")
	tests := []struct {
		name    string
		p       *fakePlanner
		ctx     context.Context
		params  json.RawMessage
		wantErr string
		wantIs  error
	}{
		{name: "earliest ok, no scope in ctx", p: &fakePlanner{moves: oneTopicMoves()}, ctx: context.Background(), params: earliestParams(t)},
		{name: "ok with the room's pipeline", p: &fakePlanner{moves: oneTopicMoves()}, ctx: rsCtx(), params: earliestParams(t)},
		{name: "shared group: two topics listed", p: &fakePlanner{moves: twoTopicMoves()}, ctx: rsCtx(), params: earliestParams(t, rsTopic, rsTopic2)},
		{name: "no planner", ctx: context.Background(), params: earliestParams(t), wantErr: "no Kafka client"},
		{name: "group active", p: &fakePlanner{inactive: clients.ErrGroupActive}, ctx: context.Background(), params: earliestParams(t), wantErr: "stop its consumers first", wantIs: clients.ErrGroupActive},
		{name: "broker down", p: &fakePlanner{inactive: brokerDown}, ctx: context.Background(), params: earliestParams(t), wantErr: "connection refused", wantIs: brokerDown},
		{name: "no committed offsets", p: &fakePlanner{planErr: clients.ErrNotFound}, ctx: context.Background(), params: earliestParams(t), wantErr: "no committed offsets to reset", wantIs: clients.ErrNotFound},
		{name: "group commits on an unlisted topic", p: &fakePlanner{moves: twoTopicMoves()}, ctx: context.Background(), params: earliestParams(t), wantErr: "also has committed offsets on pg.public.customers"},
		{name: "listed topic not committed", p: &fakePlanner{moves: oneTopicMoves()}, ctx: context.Background(), params: earliestParams(t, rsTopic, rsTopic2), wantErr: `no committed offsets on topic "pg.public.customers"`},
		{name: "group outside the room's pipeline", p: &fakePlanner{moves: oneTopicMoves()}, ctx: rsCtx(), params: rsParams(t, map[string]any{"group": "billing-sink", "topics": []string{rsTopic}, "to": "earliest"}), wantIs: tools.ErrOutOfScope},
		{name: "future timestamp", p: &fakePlanner{moves: oneTopicMoves()}, ctx: context.Background(), params: rsParams(t, map[string]any{"group": rsGroup, "topics": []string{rsTopic}, "to": "timestamp", "timestamp": "2027-01-01T00:00:00Z"}), wantErr: "in the future"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := newReset(tc.p, nil).Validate(tc.ctx, tc.params)
			if tc.wantErr == "" && tc.wantIs == nil {
				if err != nil {
					t.Fatalf("Validate = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate = %v; want %q", err, tc.wantErr)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("Validate = %v; want errors.Is %v", err, tc.wantIs)
			}
		})
	}
}

func TestResetValidateTimeout(t *testing.T) {
	a := newReset(&fakePlanner{block: true}, nil)
	a.d.validateTimeout = 20 * time.Millisecond
	start := time.Now()
	err := a.Validate(context.Background(), earliestParams(t))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Validate = %v; want deadline exceeded", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("Validate did not stop at its deadline")
	}
}

func TestResetPreviewPerPartition(t *testing.T) {
	p := &fakePlanner{moves: oneTopicMoves()}
	w := &fakeKafkaWriter{p: p}
	res, err := newReset(p, w).Preview(rsCtx(), earliestParams(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(w.resets) != 0 {
		t.Fatalf("Preview called the writer: %v", w.resets)
	}
	for _, want := range []string{"p0 120 (lag 30)", "p1 98 (lag 2)"} {
		if !strings.Contains(res.Current, want) {
			t.Errorf("Current %q lacks %q", res.Current, want)
		}
	}
	for _, want := range []string{
		"2 of 2 partitions move to earliest; the group re-reads 208 records",
		"p0 120 → 0, lag 30 → lag 150 (+120)",
		"p1 98 → 10, lag 2 → lag 90 (+88)",
	} {
		if !strings.Contains(res.Expected, want) {
			t.Errorf("Expected %q lacks %q", res.Expected, want)
		}
	}
	if !strings.Contains(res.Rollback, "pg.public.orders: p0=120, p1=98") {
		t.Errorf("Rollback %q lacks the committed offsets", res.Rollback)
	}
	if res.Reversible {
		t.Error("Reversible: no action restores explicit offsets yet")
	}
	if !strings.Contains(res.Target, rsGroup) || !strings.Contains(res.Target, rsTopic) {
		t.Errorf("Target = %q", res.Target)
	}
}

func TestResetPreviewTimestampAndSkip(t *testing.T) {
	p := &fakePlanner{moves: []clients.OffsetMove{
		{Topic: rsTopic, Partition: 0, Committed: 5, Target: 3, End: 5},   // rewinds 2
		{Topic: rsTopic, Partition: 1, Committed: 2, Target: 9, End: 9},   // no record after ts: end, skips 7
		{Topic: rsTopic, Partition: 2, Committed: -1, Target: 4, End: 10}, // no commit before
	}}
	params := rsParams(t, map[string]any{"group": rsGroup, "topics": []string{rsTopic}, "to": "timestamp", "timestamp": "2026-10-18T11:00:00Z"})
	res, err := newReset(p, nil).Preview(rsCtx(), params)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"to 2026-10-18T11:00:00Z",
		"p1 2 → 9, lag 7 → lag 0 (-7) (no record at or after the time: end offset)",
		"WARNING: 1 partitions move forward and skip 7 unread records",
		"p2 none → 4, lag ? → lag 6",
	} {
		if !strings.Contains(res.Expected, want) {
			t.Errorf("Expected %q lacks %q", res.Expected, want)
		}
	}
	if !strings.Contains(res.Rollback, "p0=5, p1=2, p2=none") {
		t.Errorf("Rollback = %q", res.Rollback)
	}
}

func TestResetPreviewCapsListing(t *testing.T) {
	var moves []clients.OffsetMove
	for i := int32(0); i < 60; i++ {
		moves = append(moves, clients.OffsetMove{Topic: rsTopic, Partition: i, Committed: 10, Target: 0, End: 10})
	}
	res, err := newReset(&fakePlanner{moves: moves}, nil).Preview(rsCtx(), earliestParams(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Current, "and 10 more partitions") || strings.Contains(res.Current, "p59") {
		t.Errorf("Current not capped: %q", res.Current)
	}
	if !strings.Contains(res.Rollback, "p59=10") {
		t.Errorf("Rollback must list every partition: %q", res.Rollback)
	}
}

func TestResetPreviewRefusals(t *testing.T) {
	other := rsParams(t, map[string]any{"group": "billing-sink", "topics": []string{rsTopic}, "to": "earliest"})
	otherTopic := rsParams(t, map[string]any{"group": rsGroup, "topics": []string{"billing.public.orders"}, "to": "earliest"})
	tests := []struct {
		name     string
		p        *fakePlanner
		ctx      context.Context
		params   json.RawMessage
		wantIs   error
		wantText string
		noIO     bool
	}{
		{name: "no scope", p: &fakePlanner{moves: oneTopicMoves()}, ctx: context.Background(), params: earliestParams(t), wantIs: tools.ErrNoScope, noIO: true},
		{name: "pipeline id only", p: &fakePlanner{moves: oneTopicMoves()}, ctx: tools.WithPipelineID(context.Background(), "pg-orders"), params: earliestParams(t), wantIs: tools.ErrNoScope, noIO: true},
		{name: "group out of scope", p: &fakePlanner{moves: oneTopicMoves()}, ctx: rsCtx(), params: other, wantIs: tools.ErrOutOfScope, noIO: true},
		{name: "topic out of scope", p: &fakePlanner{moves: oneTopicMoves()}, ctx: rsCtx(), params: otherTopic, wantIs: tools.ErrOutOfScope, noIO: true},
		{name: "group active", p: &fakePlanner{inactive: clients.ErrGroupActive}, ctx: rsCtx(), params: earliestParams(t), wantIs: clients.ErrGroupActive, wantText: "stop its consumers first"},
		{name: "shared group: unlisted topic", p: &fakePlanner{moves: twoTopicMoves()}, ctx: rsCtx(), params: earliestParams(t), wantText: "also has committed offsets"},
		{name: "no planner", ctx: rsCtx(), params: earliestParams(t), wantText: "no Kafka client"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newReset(tc.p, nil).Preview(tc.ctx, tc.params)
			if err == nil {
				t.Fatal("Preview succeeded; want refused")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("Preview = %v; want errors.Is %v", err, tc.wantIs)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("Preview = %v; want %q", err, tc.wantText)
			}
			if tc.noIO && len(tc.p.calls) != 0 {
				t.Fatalf("refused after I/O: %v", tc.p.calls)
			}
		})
	}
}

func TestResetExecute(t *testing.T) {
	p := &fakePlanner{moves: oneTopicMoves()}
	w := &fakeKafkaWriter{p: p, moved: 2}
	out, err := newReset(p, w).Execute(rsCtx(), earliestParams(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(w.resets) != 1 || w.resets[0] != rsGroup+" earliest" {
		t.Fatalf("resets = %v", w.resets)
	}
	if _, ok := tools.PipelineFrom(w.ctxs[0]); !ok {
		t.Fatal("the writer did not get the room's pipeline in ctx")
	}
	for _, want := range []string{
		"reset group orders-sink to earliest: 2 of 2 partitions moved",
		"committed before → after: pg.public.orders: p0 120 → 0, p1 98 → 10",
		"rollback offsets",
		"p0=120, p1=98",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output %q lacks %q", out, want)
		}
	}
}

func TestResetExecuteRefusals(t *testing.T) {
	brokerDown := errors.New("dial tcp: connection refused")
	tests := []struct {
		name      string
		p         *fakePlanner
		writer    bool
		writerErr error
		ctx       context.Context
		params    json.RawMessage
		wantIs    error
		wantText  string
		noIO      bool
	}{
		{name: "write mode off", p: &fakePlanner{moves: oneTopicMoves()}, ctx: rsCtx(), params: earliestParams(t), wantText: "PIPEROOM_WRITE_MODE is off", noIO: true},
		{name: "no scope", p: &fakePlanner{moves: oneTopicMoves()}, writer: true, ctx: context.Background(), params: earliestParams(t), wantIs: tools.ErrNoScope, noIO: true},
		{name: "group out of scope", p: &fakePlanner{moves: oneTopicMoves()}, writer: true, ctx: rsCtx(), params: rsParams(t, map[string]any{"group": "billing-sink", "topics": []string{rsTopic}, "to": "earliest"}), wantIs: tools.ErrOutOfScope, noIO: true},
		{name: "group active", p: &fakePlanner{inactive: fmtActive()}, writer: true, ctx: rsCtx(), params: earliestParams(t), wantIs: clients.ErrGroupActive, wantText: "stop its consumers first"},
		{name: "shared group: unlisted topic", p: &fakePlanner{moves: twoTopicMoves()}, writer: true, ctx: rsCtx(), params: earliestParams(t), wantText: "also has committed offsets"},
		{name: "broker down at commit", p: &fakePlanner{moves: oneTopicMoves()}, writer: true, writerErr: brokerDown, ctx: rsCtx(), params: earliestParams(t), wantIs: brokerDown, wantText: "p0=120, p1=98"},
		{name: "consumer joined before commit", p: &fakePlanner{moves: oneTopicMoves()}, writer: true, writerErr: fmtActive(), ctx: rsCtx(), params: earliestParams(t), wantIs: clients.ErrGroupActive, wantText: "a consumer joined"},
		{name: "commit timeout", p: &fakePlanner{moves: oneTopicMoves()}, writer: true, writerErr: context.DeadlineExceeded, ctx: rsCtx(), params: earliestParams(t), wantIs: context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var w *fakeKafkaWriter
			if tc.writer {
				w = &fakeKafkaWriter{p: tc.p, err: tc.writerErr}
			}
			_, err := newReset(tc.p, w).Execute(tc.ctx, tc.params)
			if err == nil {
				t.Fatal("Execute succeeded; want refused")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("Execute = %v; want errors.Is %v", err, tc.wantIs)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("Execute = %v; want %q", err, tc.wantText)
			}
			if tc.noIO && len(tc.p.calls) != 0 {
				t.Fatalf("refused after I/O: %v", tc.p.calls)
			}
			if w != nil && tc.writerErr == nil && len(w.resets) != 0 {
				t.Fatalf("writer called on a refused step: %v", w.resets)
			}
		})
	}
}

func fmtActive() error {
	return errors.Join(errors.New("kafka: group orders-sink is Stable with 2 members"), clients.ErrGroupActive)
}

// TestResetExecuteRereadFails: once the commit succeeded, a failed re-read is
// a result, not an error (the agent must not retry a reset that happened),
// and it still carries the offsets from before.
func TestResetExecuteRereadFails(t *testing.T) {
	p := &fakePlanner{moves: oneTopicMoves(), planErrAfter: errors.New("broker went away")}
	w := &fakeKafkaWriter{p: p, moved: 2}
	out, err := newReset(p, w).Execute(rsCtx(), earliestParams(t))
	if err != nil {
		t.Fatalf("Execute = %v; want a result", err)
	}
	for _, want := range []string{"2 of 2 partitions moved", "re-read failed: broker went away", "offsets before: pg.public.orders: p0 120, p1 98", "p0=120, p1=98"} {
		if !strings.Contains(out, want) {
			t.Errorf("output %q lacks %q", out, want)
		}
	}
}

// TestRegisterKafkaWriteModeOff: with only the read-only planner (write mode
// off) the action is registered, validates and previews, and refuses Execute.
func TestRegisterKafkaWriteModeOff(t *testing.T) {
	reg := NewRegistry()
	p := &fakePlanner{moves: oneTopicMoves()}
	if err := RegisterKafka(reg, KafkaDeps{Planner: p}); err != nil {
		t.Fatal(err)
	}
	a, ok := reg.Get(resetName)
	if !ok {
		t.Fatal("kafka_reset_consumer_offsets not registered")
	}
	if a.Layer() != "consumer" {
		t.Fatalf("layer = %q", a.Layer())
	}
	if _, ok := a.(Scoped); !ok {
		t.Fatal("not Scoped")
	}
	if err := a.Validate(context.Background(), earliestParams(t)); err != nil {
		t.Fatalf("Validate with write mode off: %v", err)
	}
	if _, err := a.(Preview).Preview(rsCtx(), earliestParams(t)); err != nil {
		t.Fatalf("Preview with write mode off: %v", err)
	}
	if _, err := a.Execute(rsCtx(), earliestParams(t)); err == nil || !strings.Contains(err.Error(), "PIPEROOM_WRITE_MODE is off") {
		t.Fatalf("Execute with write mode off = %v", err)
	}
	schema := a.ParamsSchema()
	props, _ := schema["properties"].(map[string]any)
	for _, k := range []string{"group", "topics", "to", "timestamp"} {
		if _, ok := props[k]; !ok {
			t.Errorf("ParamsSchema lacks %q", k)
		}
	}
	if err := RegisterKafka(reg, KafkaDeps{}); err == nil {
		t.Fatal("second RegisterKafka: want duplicate error")
	}
}
