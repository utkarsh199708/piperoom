//go:build integration

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/testenv"
)

// fixkTopic creates a one-partition topic named it_fixk_* and deletes it at
// the end of the test.
func fixkTopic(t *testing.T, adm *kadm.Client) string {
	t.Helper()
	topic := testenv.Name("fixk")
	resps, err := adm.CreateTopics(context.Background(), 1, 1, nil, topic)
	if err != nil || resps.Error() != nil {
		t.Fatalf("CreateTopics: %v %v", err, resps.Error())
	}
	t.Cleanup(func() {
		if _, err := adm.DeleteTopics(context.Background(), topic); err != nil {
			t.Logf("DeleteTopics: %v", err)
		}
	})
	return topic
}

// produceTxn writes keyed records in one committed transaction, so the
// topic's last offset is a commit marker.
func produceTxn(t *testing.T, brokers []string, topic string, keys ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.TransactionalID(testenv.Name("fixk-txn")))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		r := &kgo.Record{Topic: topic, Key: []byte(k), Value: []byte("v-" + k)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
}

// TestTransactionalTopicOffsetsAndFind: the record at End-1 of a
// transactional topic is a commit marker. TopicOffsets must return promptly
// with a timestamp, FindByKey must find every occurrence (newest last) and
// report an absent key as not found, not as a timeout.
func TestTransactionalTopicOffsetsAndFind(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	topic := fixkTopic(t, c.adm)

	produceTxn(t, brokers, topic, "k1", "k2", "k1")
	produceTxn(t, brokers, topic, "k3", "k1")
	waitForTopic(t, c, topic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	offs, err := c.TopicOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("TopicOffsets: %v", err)
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("TopicOffsets took %s: waited on the commit marker", d)
	}
	// 5 records + 2 commit markers.
	if len(offs) != 1 || offs[0].End != 7 || offs[0].LastTimestamp.IsZero() {
		t.Fatalf("offsets = %+v, want end 7 with a timestamp", offs)
	}

	start = time.Now()
	refs, err := c.FindByKey(ctx, topic, "k1", time.Now().Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("FindByKey k1: %v", err)
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("FindByKey took %s", d)
	}
	// k1 at offsets 0, 2 (txn 1) and 5 (txn 2); markers at 3 and 6.
	if len(refs) != 3 || refs[0].Offset != 0 || refs[1].Offset != 2 || refs[2].Offset != 5 {
		t.Fatalf("k1 refs = %+v, want offsets 0, 2, 5 (newest last)", refs)
	}

	start = time.Now()
	refs, err = c.FindByKey(ctx, topic, "absent", time.Now().Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("absent key: want not found, got %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("absent key refs = %+v", refs)
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("absent-key scan took %s", d)
	}
}

func TestFindByKeyMissingTopic(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	_, err := c.FindByKey(context.Background(), testenv.Name("fixk-missing"), "k", time.Now().Add(-time.Hour), 10)
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing topic: err = %v, want ErrNotFound", err)
	}
}

func TestGroupLagUnknownGroup(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	_, err := c.GroupLag(context.Background(), testenv.Name("fixk-nogroup"))
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown group: err = %v, want ErrNotFound", err)
	}
}

// TestGroupLagReportAnomalies commits offsets past the end and on a topic
// that is then deleted; both must be reported, not dropped or read as lag 0.
func TestGroupLagReportAnomalies(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	ctx := context.Background()

	live := fixkTopic(t, c.adm)
	doomed := fixkTopic(t, c.adm)
	for _, topic := range []string{live, doomed} {
		if err := c.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	group := testenv.Name("fixk-group")
	var offs kadm.Offsets
	offs.Add(kadm.Offset{Topic: live, Partition: 0, At: 50, LeaderEpoch: -1})
	offs.Add(kadm.Offset{Topic: doomed, Partition: 0, At: 1, LeaderEpoch: -1})
	resp, err := c.adm.CommitOffsets(ctx, group, offs)
	if err != nil || resp.Error() != nil {
		t.Fatalf("CommitOffsets: %v %v", err, resp.Error())
	}
	t.Cleanup(func() { _, _ = c.adm.DeleteGroups(context.Background(), group) })

	if _, err := c.adm.DeleteTopics(ctx, doomed); err != nil {
		t.Fatal(err)
	}

	var rep LagReport
	testenv.WaitFor(t, 20*time.Second, func() (bool, error) {
		rep, err = c.GroupLagReport(ctx, group)
		if err != nil {
			return false, nil
		}
		var beyond, missing bool
		for _, a := range rep.Anomalies {
			beyond = beyond || (a.Topic == live && a.Kind == AnomalyCommitBeyondEnd)
			missing = missing || (a.Topic == doomed && a.Kind == AnomalyTopicMissing)
		}
		return beyond && missing, nil
	})
	t.Logf("anomalies: %+v", rep.Anomalies)
}
