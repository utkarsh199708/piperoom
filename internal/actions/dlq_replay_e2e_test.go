package actions

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// TestDLQReplayKFake runs dlq_replay over the real Kafka clients against
// kfake, franz-go's in-process broker (no Docker, no network).
// dlq_replay_integration_test.go runs the same against the dev stack.
func TestDLQReplayKFake(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	runDLQReplayEndToEnd(t, c.ListenAddrs())
}

// dlqName is a unique it_ name (testenv.Name is integration-tagged).
func dlqName(prefix string) string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return "it_" + prefix + "_" + hex.EncodeToString(b)
}

// e2ePipeline loads an owners.yaml whose pipeline declares dlq as its DLQ.
func e2ePipeline(t *testing.T, prefix string, dlqs ...string) owners.Pipeline {
	t.Helper()
	var decl strings.Builder
	for _, d := range dlqs {
		fmt.Fprintf(&decl, "        - {topic: %s, written_by: %s-sink}\n", d, prefix)
	}
	y := fmt.Sprintf(`
pipelines:
  - id: %[1]s
    title: replay test
    topic_prefix: %[1]s
    source_tables: [public.t]
    connectors: [%[1]s-sink]
    consumer_groups: [%[1]s-group]
    sink_tables: [analytics.t]
    sink_ts_column: updated_at
    watch:
      dlq_topics:
%[2]slayers:
  postgres: a
  kafka-connect: a
  schema-registry: a
  kafka: a
  consumer: a
  warehouse: a
`, prefix, decl.String())
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	p, _ := o.Pipeline(prefix)
	return p
}

// runDLQReplayEndToEnd: an it_ DLQ with five records (Connect error headers
// plus an app header) and an it_ CDC topic; Validate and Preview with only
// the read-only ReplayReader (write mode off), a refused Execute without the
// writer, a refused multi-partition DLQ, then two Execute steps with the
// writer under the room's pipeline: 3 records, then the rest from the
// reported next_from_offset. End offsets and the copied records are read back.
func runDLQReplayEndToEnd(t *testing.T, brokers []string) {
	t.Helper()
	raw, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	adm := kadm.NewClient(raw)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	prefix := dlqName("replay")
	toTopic := prefix + ".public.t"
	dlq := prefix + "-dlq"
	dlq2 := prefix + "-dlq2"
	for topic, parts := range map[string]int32{toTopic: 1, dlq: 1, dlq2: 2} {
		if resps, err := adm.CreateTopics(ctx, parts, 1, nil, topic); err != nil || resps.Error() != nil {
			t.Fatalf("CreateTopics %s: %v %v", topic, err, resps.Error())
		}
	}
	t.Cleanup(func() { _, _ = adm.DeleteTopics(context.Background(), toTopic, dlq, dlq2) })
	for i := 0; i < 5; i++ {
		r := &kgo.Record{Topic: dlq, Partition: 0, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf("v%d", i)),
			Headers: []kgo.RecordHeader{
				{Key: "trace-id", Value: []byte(fmt.Sprintf("t%d", i))},
				{Key: "__connect.errors.topic", Value: []byte(toTopic)},
				{Key: "__connect.errors.exception.message", Value: []byte("boom")},
			}}
		if err := raw.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	for p := int32(0); p < 2; p++ {
		if err := raw.ProduceSync(ctx, &kgo.Record{Topic: dlq2, Partition: p, Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatalf("produce dlq2: %v", err)
		}
	}

	rc, err := kafka.New(brokers, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Close)
	reader := kafka.NewReplayReader(rc)
	pl := e2ePipeline(t, prefix, dlq, dlq2)
	room := tools.WithPipeline(ctx, pl)
	params := func(m map[string]any) json.RawMessage {
		b, _ := json.Marshal(m)
		return b
	}
	step1 := params(map[string]any{"dlq_topic": dlq, "to_topic": toTopic, "count": 3})

	// Write mode off: no writer is built, the action still validates and previews.
	readOnly := &dlqReplay{d: KafkaDeps{Records: reader}}
	if err := readOnly.CheckScope(pl, step1); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	// The reader's metadata may not list the new topics for a moment.
	var verr error
	for i := 0; i < 50; i++ {
		if verr = readOnly.Validate(room, step1); verr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if verr != nil {
		t.Fatalf("Validate (write mode off): %v", verr)
	}
	pv, err := readOnly.Preview(room, step1)
	if err != nil {
		t.Fatalf("Preview (write mode off): %v", err)
	}
	if !strings.Contains(pv.Expected, `"k0"`) || !strings.Contains(pv.Target, "offsets 0–2") {
		t.Fatalf("Preview = %+v", pv)
	}
	if _, err := readOnly.Execute(room, step1); err == nil || !strings.Contains(err.Error(), "PIPEROOM_WRITE_MODE is off") {
		t.Fatalf("Execute without writer = %v, want refused", err)
	}
	multi := params(map[string]any{"dlq_topic": dlq2, "to_topic": toTopic, "count": 1})
	if err := readOnly.Validate(room, multi); err == nil || !strings.Contains(err.Error(), "has 2 partitions") {
		t.Fatalf("Validate multi-partition DLQ = %v, want refused", err)
	}

	w, err := kafka.NewWriter(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	a := &dlqReplay{d: KafkaDeps{Records: reader, Writer: w}}
	out, err := a.Execute(room, step1)
	if err != nil {
		t.Fatalf("Execute step 1: %v", err)
	}
	if !strings.Contains(out, "offsets 0 to 2") || !strings.Contains(out, "next_from_offset 3") {
		t.Fatalf("Execute step 1 = %q", out)
	}
	out, err = a.Execute(room, params(map[string]any{"dlq_topic": dlq, "to_topic": toTopic, "count": 1000, "from_offset": 3}))
	if err != nil {
		t.Fatalf("Execute step 2: %v", err)
	}
	if !strings.Contains(out, "offsets 3 to 4") || !strings.Contains(out, "acked 2 of 2") || !strings.Contains(out, "next_from_offset 5") {
		t.Fatalf("Execute step 2 = %q", out)
	}

	ends, err := adm.ListEndOffsets(ctx, toTopic, dlq)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := ends.Lookup(toTopic, 0); e.Offset != 5 {
		t.Fatalf("target end offset = %d, want 5", e.Offset)
	}
	if e, _ := ends.Lookup(dlq, 0); e.Offset != 5 {
		t.Fatalf("DLQ end offset = %d, want 5 (unchanged)", e.Offset)
	}
	got, err := reader.ReadRecords(ctx, toTopic, 0, 0, 10)
	if err != nil || len(got) != 5 {
		t.Fatalf("read back %d records: %v", len(got), err)
	}
	for i, r := range got {
		if string(r.Key) != fmt.Sprintf("k%d", i) || string(r.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("copy %d = %q/%q, want k%d/v%d in order", i, r.Key, r.Value, i, i)
		}
		want := []clients.RecordHeader{{Key: "trace-id", Value: []byte(fmt.Sprintf("t%d", i))}}
		if len(r.Headers) != 1 || r.Headers[0].Key != want[0].Key || string(r.Headers[0].Value) != string(want[0].Value) {
			t.Fatalf("copy %d headers = %+v, want only trace-id", i, r.Headers)
		}
	}
}
