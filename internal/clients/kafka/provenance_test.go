package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/tools"
)

func recordCtx() (context.Context, *tools.Recorder) {
	rec := tools.NewRecorder()
	return tools.WithRecorder(context.Background(), rec), rec
}

func texts(calls []tools.Call) []string {
	var out []string
	for _, c := range calls {
		out = append(out, c.Text)
	}
	return out
}

func TestDLQOffsetsRecordsCalls(t *testing.T) {
	f := &fakeLister{
		starts: listed("a-dlq", map[int32]int64{0: 0}),
		ends:   listed("a-dlq", map[int32]int64{0: 3}),
	}
	c := &Client{brokers: []string{"k1:9092", "k2:9092"}, offs: f}
	ctx, rec := recordCtx()
	if _, err := c.DLQOffsets(ctx, "a-dlq"); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if got := texts(calls); len(got) != 2 || got[0] != "kadm.ListStartOffsets(topics)" || got[1] != "kadm.ListEndOffsets(topics)" {
		t.Fatalf("texts = %v", got)
	}
	for _, call := range calls {
		if call.Kind != tools.CallKafka || call.Target != "kafka k1:9092,k2:9092" || call.TookMS < 0 {
			t.Fatalf("call = %+v", call)
		}
		if len(call.ParamsRedacted) != 1 || call.ParamsRedacted[0] != "topics=a-dlq" {
			t.Fatalf("params = %v", call.ParamsRedacted)
		}
	}
}

func TestDLQOffsetsErrorStillRecords(t *testing.T) {
	f := &fakeLister{err: context.DeadlineExceeded}
	c := &Client{brokers: []string{"k:9092"}, offs: f}
	ctx, rec := recordCtx()
	if _, err := c.DLQOffsets(ctx, "d"); err == nil {
		t.Fatal("want error")
	}
	if n := len(rec.Calls()); n != 1 {
		t.Fatalf("got %d calls, want 1 (the failed start request)", n)
	}
}

func TestPeekDLQRecordsHeadersOnlyReceipt(t *testing.T) {
	now := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	r := rec(0, 0, now, hdr("__connect.errors.exception.message", "SECRET-VALUE"))
	r.Value = []byte("SECRET-VALUE")
	r.Key = []byte("SECRET-KEY")
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("d", r)}}
	c := peekClient(merge(listed("d", map[int32]int64{0: 0})), merge(listed("d", map[int32]int64{0: 1})), cons, nil)
	c.brokers = []string{"k:9092"}
	ctx, recd := recordCtx()
	if _, err := c.PeekDLQ(ctx, "d", 5); err != nil {
		t.Fatal(err)
	}
	got := texts(recd.Calls())
	want := []string{"kadm.ListStartOffsets(topics)", "kadm.ListEndOffsets(topics)", "kadm.ListCommittedOffsets(topic)", "kgo.PollFetches(topic, limit)"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("texts = %v, want %v", got, want)
	}
	if b, _ := json.Marshal(recd.Calls()); strings.Contains(string(b), "SECRET") {
		t.Fatalf("record bytes leaked into provenance: %s", b)
	}
}

// unreachable returns a client whose brokers refuse connections, so a request
// fails when the deadline expires: the error path of every admin method.
func unreachable(t *testing.T) *Client {
	t.Helper()
	c, err := New([]string{"127.0.0.1:1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestKafkaGroupLagRecordsKadmLag(t *testing.T) {
	c := unreachable(t)
	ctx, recd := recordCtx()
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := c.GroupLag(ctx, "order-sync"); err == nil {
		t.Fatal("want error")
	}
	calls := recd.Calls()
	if len(calls) != 1 || calls[0].Text != "kadm.Lag(group)" || len(calls[0].ParamsRedacted) != 1 || calls[0].ParamsRedacted[0] != "group=order-sync" {
		t.Fatalf("calls = %+v", calls)
	}
	if calls[0].Target != "kafka 127.0.0.1:1" {
		t.Fatalf("target = %q", calls[0].Target)
	}
}

func TestTopicsAndGroupsRecord(t *testing.T) {
	c := unreachable(t)
	ctx, recd := recordCtx()
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, _ = c.Topics(ctx)
	_, _ = c.ConsumerGroups(ctx)
	got := texts(recd.Calls())
	if len(got) != 2 || got[0] != "kadm.ListTopics()" || got[1] != "kadm.ListGroups()" {
		t.Fatalf("texts = %v", got)
	}
}

func TestFindByKeyNeverRecordsKey(t *testing.T) {
	c := unreachable(t)
	ctx, recd := recordCtx()
	ctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, _ = c.FindByKey(ctx, "pg.public.orders", "customer-4711", time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC), 5000)
	calls := recd.Calls()
	if len(calls) == 0 {
		t.Fatal("no call recorded")
	}
	b, _ := json.Marshal(calls)
	if strings.Contains(string(b), "customer-4711") {
		t.Fatalf("record key leaked into provenance: %s", b)
	}
	if calls[0].Text != "kadm.ListOffsetsAfterMilli(topic, since)" || calls[0].ParamsRedacted[1] != "since=2026-10-14T03:00:00Z" {
		t.Fatalf("first call = %+v", calls[0])
	}
}
