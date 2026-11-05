package actions

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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

// TestResetConsumerOffsetsKFake runs the action over the real Kafka clients
// against kfake, franz-go's in-process broker (no Docker, no network).
// kafka_reset_integration_test.go runs the same against the dev stack.
func TestResetConsumerOffsetsKFake(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	runResetEndToEnd(t, c.ListenAddrs())
}

// rsName is a unique it_ name (testenv.Name is integration-tagged).
func rsName(prefix string) string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return "it_" + prefix + "_" + hex.EncodeToString(b)
}

// rsWaitFor polls cond every 200 ms until it holds or timeout passes.
func rsWaitFor(t *testing.T, timeout time.Duration, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// runResetEndToEnd: an it_ topic of an it_ pipeline, an it_ group with
// committed offsets and no member; Validate and Preview with only the
// read-only planner (write mode off), a refused Execute without the writer,
// a refused active group, then Execute with the writer under the room's
// pipeline, and the committed offsets read back.
func runResetEndToEnd(t *testing.T, brokers []string) {
	t.Helper()
	raw, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	adm := kadm.NewClient(raw)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := rsName("reset")
	topic := prefix + ".t"
	group := rsName("reset_group")
	if resps, err := adm.CreateTopics(ctx, 2, 1, nil, topic); err != nil || resps.Error() != nil {
		t.Fatalf("CreateTopics: %v %v", err, resps.Error())
	}
	t.Cleanup(func() {
		_, _ = adm.DeleteGroups(context.Background(), group)
		_, _ = adm.DeleteTopics(context.Background(), topic)
	})
	for p, n := range map[int32]int{0: 5, 1: 3} {
		for i := 0; i < n; i++ {
			if err := raw.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: p, Value: []byte("v")}).FirstErr(); err != nil {
				t.Fatalf("produce: %v", err)
			}
		}
	}
	os := kadm.Offsets{}
	os.AddOffset(topic, 0, 5, -1)
	os.AddOffset(topic, 1, 3, -1)
	if resp, err := adm.CommitOffsets(ctx, group, os); err != nil || resp.Error() != nil {
		t.Fatalf("commit: %v %v", err, resp.Error())
	}

	rc, err := kafka.New(brokers, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Close)
	w, err := kafka.NewWriter(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)

	pl := owners.Pipeline{ID: prefix, TopicPrefix: prefix, SourceTables: []string{"t"}, ConsumerGroups: []string{group}}
	room := tools.WithPipeline(ctx, pl)
	params, _ := json.Marshal(map[string]any{"group": group, "topics": []string{topic}, "to": "earliest"})

	readOnly := &resetConsumerOffsets{d: KafkaDeps{Planner: kafka.NewPlanner(rc)}}
	if err := readOnly.CheckScope(pl, params); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	// The reader's metadata may not list the new topic for a moment.
	var verr error
	rsWaitFor(t, 15*time.Second, func() (bool, error) {
		verr = readOnly.Validate(ctx, params)
		return verr == nil, nil
	})
	res, err := readOnly.Preview(room, params)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Rollback, "p0=5, p1=3") || !strings.Contains(res.Expected, "2 of 2 partitions move to earliest") {
		t.Fatalf("preview = %+v", res)
	}
	if _, err := readOnly.Execute(room, params); err == nil {
		t.Fatal("Execute without a writer: want refused")
	}

	// An active member: Validate refuses.
	member, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatal(err)
	}
	rsWaitFor(t, 20*time.Second, func() (bool, error) {
		pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer pcancel()
		member.PollFetches(pctx)
		err := readOnly.Validate(ctx, params)
		return err != nil && strings.Contains(err.Error(), "stop its consumers first"), nil
	})
	member.Close() // leaves the group
	rsWaitFor(t, 30*time.Second, func() (bool, error) {
		return readOnly.Validate(ctx, params) == nil, nil
	})
	// The member may have committed on leave; reset from wherever it is.
	before, err := w.PlanGroupReset(ctx, group, clients.OffsetTarget{Earliest: true})
	if err != nil {
		t.Fatal(err)
	}

	a := &resetConsumerOffsets{d: KafkaDeps{Planner: w, Writer: w}}
	out, err := a.Execute(room, params)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(out, "partitions moved") || !strings.Contains(out, "rollback offsets") ||
		!strings.Contains(out, "p0 "+offText(before[0].Committed)+" → 0") {
		t.Fatalf("output = %q", out)
	}
	resp, err := adm.FetchOffsets(ctx, group)
	if err != nil || resp.Error() != nil {
		t.Fatalf("fetch: %v %v", err, resp.Error())
	}
	for p, r := range resp[topic] {
		if r.At != 0 {
			t.Fatalf("partition %d committed %d after reset; want 0", p, r.At)
		}
	}
}
