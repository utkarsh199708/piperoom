package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func TestSnapshotSignalValue(t *testing.T) {
	got, err := SnapshotSignalValue([]string{"public.orders", "public.refunds", "public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"execute-snapshot","data":{"data-collections":["public.orders","public.refunds"],"type":"INCREMENTAL"}}`
	if string(got) != want {
		t.Fatalf("value %s, want %s", got, want)
	}
	stop, err := StopSnapshotSignalValue([]string{"public.orders"})
	if err != nil || !strings.HasPrefix(string(stop), `{"type":"stop-snapshot"`) {
		t.Fatalf("stop value %s, %v", stop, err)
	}
	many := make([]string, maxSignalTables+1)
	for i := range many {
		many[i] = "public.t" + strings.Repeat("x", i)
	}
	for name, tables := range map[string][]string{
		"empty": nil, "bare": {"orders"}, "regex": {"public.ord.*"}, "three parts": {"db.public.orders"},
		"quote": {`public.o"rders`}, "too many": many,
	} {
		if _, err := SnapshotSignalValue(tables); err == nil {
			t.Errorf("%s: accepted %v", name, tables)
		}
	}
}

var sigPipeline = owners.Pipeline{
	ID: "orders", TopicPrefix: "pg",
	SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders-connector"},
}

func TestSendSnapshotSignalScopeBeforeIO(t *testing.T) {
	// No client: any I/O would panic, so every case must refuse first.
	w := &Writer{}
	room := tools.WithPipeline(context.Background(), sigPipeline)
	for _, tc := range []struct {
		name                string
		ctx                 context.Context
		connector, key, tbl string
		want                error
	}{
		{"no scope", context.Background(), "pg-orders-connector", "pg", "public.orders", tools.ErrNoScope},
		{"pipeline id only", tools.WithPipelineID(context.Background(), "orders"), "pg-orders-connector", "pg", "public.orders", tools.ErrNoScope},
		{"other connector", room, "billing", "pg", "public.orders", tools.ErrOutOfScope},
		{"other table", room, "pg-orders-connector", "pg", "public.users", tools.ErrOutOfScope},
		{"other key", room, "pg-orders-connector", "billing", "public.orders", tools.ErrOutOfScope},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := w.SendSnapshotSignal(tc.ctx, "pg-signal", tc.connector, tc.key, []string{tc.tbl})
			if !errors.Is(err, tc.want) {
				t.Fatalf("SendSnapshotSignal = %v, want %v", err, tc.want)
			}
		})
	}
	for name, args := range map[string][3]string{"no topic": {"", "c", "pg"}, "no connector": {"t", "", "pg"}, "no key": {"t", "c", ""}} {
		if _, err := w.SendSnapshotSignal(room, args[0], args[1], args[2], []string{"public.orders"}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestSendSnapshotSignalKfake produces the signal to kfake under the room's
// scope and reads it back as Debezium would: partition 0, key topic.prefix.
func TestSendSnapshotSignalKfake(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "pg-signal"), kfake.SeedTopics(3, "wide-signal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	w := writerOn(t, c.ListenAddrs())
	ctx, cancel := context.WithTimeout(tools.WithPipeline(context.Background(), sigPipeline), 10*time.Second)
	defer cancel()

	sig, err := w.SendSnapshotSignal(ctx, "pg-signal", "pg-orders-connector", "pg", []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if sig.Topic != "pg-signal" || sig.Partition != 0 || sig.ID != "0" {
		t.Fatalf("signal %+v, want offset 0 of partition 0", sig)
	}
	cons, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics("pg-signal"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	fs := cons.PollFetches(ctx)
	if errs := fs.Errors(); len(errs) > 0 {
		t.Fatalf("fetch: %v", errs)
	}
	recs := fs.Records()
	if len(recs) != 1 || string(recs[0].Key) != "pg" || !strings.Contains(string(recs[0].Value), `"data-collections":["public.orders"]`) {
		t.Fatalf("records %+v", recs)
	}

	if _, err := w.SendSnapshotSignal(ctx, "wide-signal", "pg-orders-connector", "pg", []string{"public.orders"}); err == nil || !strings.Contains(err.Error(), "3 partitions") {
		t.Fatalf("multi-partition topic: %v", err)
	}
	if _, err := w.SendSnapshotSignal(ctx, "no-such-signal", "pg-orders-connector", "pg", []string{"public.orders"}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing topic: %v", err)
	}
}
