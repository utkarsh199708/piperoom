package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

// fakeDupKafka is a KafkaAdmin with DupScan.
type fakeDupKafka struct {
	*fakeKafka
	calls int
	dupFn func(ctx context.Context, topic string, since time.Time, maxScan int) (clients.DupReport, error)
}

func (f *fakeDupKafka) DupScan(ctx context.Context, topic string, since time.Time, maxScan int) (clients.DupReport, error) {
	f.calls++
	return f.dupFn(ctx, topic, since, maxScan)
}

var _ clients.DupScanner = (*fakeDupKafka)(nil)

func dupTool(t *testing.T, k clients.KafkaAdmin) tools.Tool {
	t.Helper()
	return scopeTool(t, Deps{Kafka: k, Owners: scopeOwners()}, "kafka_dup_keys")
}

func reportOf(rep clients.DupReport) *fakeDupKafka {
	return &fakeDupKafka{fakeKafka: &fakeKafka{}, dupFn: func(_ context.Context, topic string, since time.Time, maxScan int) (clients.DupReport, error) {
		rep.Topic, rep.Since, rep.MaxScan = topic, since, maxScan
		return rep, nil
	}}
}

func TestDupKeysSummary(t *testing.T) {
	in := `{"topic":"pg.public.orders","since":"` + since + `"}`
	// TC1's line, with the ticket's numbers.
	r, err := dupTool(t, reportOf(clients.DupReport{Scanned: 41_200, Duplicates: 3, Keys: 2,
		Partitions: []clients.DupPartition{{Partition: 0, Done: true}}})).Run(roomCtx(), json.RawMessage(in))
	if err != nil {
		t.Fatal(err)
	}
	if want := "pg.public.orders: 3 duplicate events across 2 keys, 0 order regressions in 41 200 records"; r.Summary != want {
		t.Fatalf("summary = %q, want %q", r.Summary, want)
	}
	// TC4: capped; TC7: partial; plus findings and undecodable reasons.
	rep := clients.DupReport{Scanned: 50_000, Capped: true, OrderRegressions: 1, RegressionKeys: 1, Undecodable: 4,
		UndecodableReasons: map[string]int{clients.DupUndecodableNoLSN: 3, clients.DupUndecodableRegistry: 1},
		Partitions:         []clients.DupPartition{{Partition: 0, Done: true}, {Partition: 1}},
		Findings:           []clients.DupFinding{{Kind: clients.DupKindRegression, Key: "sha256:00aa", Partition: 1, Offset: 9, LSN: 5, PrevLSN: 7, FirstOffset: 3}},
		FindingsOmitted:    25,
	}
	r, err = dupTool(t, reportOf(rep)).Run(roomCtx(), json.RawMessage(`{"topic":"pg.public.orders","since":"`+since+`","max_scan":1000000}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"scanned 50 000 (capped)", "partition(s) [1]", "1 order regression in 50 000 records (regressions on 1 key)",
		"4 records had no usable key or source.lsn (no_lsn 3, registry 1)", "regression: key sha256:00aa, partition 1 offset 9 has lsn 5 after lsn 7 at offset 3",
		"25 more finding(s)"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	if got := r.Data.(clients.DupReport).MaxScan; got != clients.DupScanMaxScan {
		t.Errorf("max_scan passed = %d, want the cap %d", got, clients.DupScanMaxScan)
	}
	rep = clients.DupReport{Scanned: 7, Partial: true, Partitions: []clients.DupPartition{{Partition: 0}}}
	r, err = dupTool(t, reportOf(rep)).Run(roomCtx(), json.RawMessage(in))
	if err != nil || !strings.Contains(r.Summary, "partial: the time limit cut the scan before the end of partition(s) [0]") {
		t.Fatalf("partial summary = %q, err %v", r.Summary, err)
	}
	if len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary is %d bytes", len(r.Summary))
	}
}

// TestDupKeysRefusals: TC5 (another pipeline's topic, no client call), bad
// input before any I/O (E2), a client error wrapped (E5), and no tool for a
// client without DupScan.
func TestDupKeysRefusals(t *testing.T) {
	f := reportOf(clients.DupReport{})
	tool := dupTool(t, f)
	_, err := tool.Run(roomCtx(), json.RawMessage(`{"topic":"bill.public.invoices","since":"`+since+`"}`))
	if !errors.Is(err, tools.ErrOutOfScope) || f.calls != 0 {
		t.Fatalf("out of scope: err = %v after %d calls, want ErrOutOfScope and no call", err, f.calls)
	}
	for _, in := range []string{`{}`, `{"topic":"pg.public.orders"}`, `{"topic":"pg.public.orders","since":"yesterday"}`,
		`{"topic":"pg.public.orders","since":"` + since + `","max_scan":0}`, `not json`} {
		if _, err := tool.Run(roomCtx(), json.RawMessage(in)); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s: err = %v, want ErrBadInput", in, err)
		}
	}
	if f.calls != 0 {
		t.Fatalf("bad input reached the client %d times", f.calls)
	}
	f.dupFn = func(context.Context, string, time.Time, int) (clients.DupReport, error) {
		return clients.DupReport{}, fmt.Errorf("kafka: dup scan: %w", clients.ErrNotFound)
	}
	if _, err := tool.Run(roomCtx(), json.RawMessage(`{"topic":"pg.public.orders","since":"`+since+`"}`)); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("client error: %v, want it wrapped", err)
	}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: &fakeKafka{}, Owners: scopeOwners()}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("kafka_dup_keys"); ok {
		t.Fatal("kafka_dup_keys registered for a client without DupScan")
	}
}

// TestDupKeysKFake runs the tool over the real client and kfake (TC1, TC6):
// the counts are found, and neither Summary nor Data holds a key or value.
func TestDupKeysKFake(t *testing.T) {
	const topic = "pg.public.orders"
	cl, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	raw, err := kgo.NewClient(kgo.SeedBrokers(cl.ListenAddrs()...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	val := func(id, lsn int) []byte {
		return fmt.Appendf(nil, `{"before":null,"after":{"id":%d,"email":"jane.doe.%d@corp.example"},"source":{"lsn":%d},"op":"c"}`, id, id, lsn)
	}
	var recs []*kgo.Record
	for i := range 97 {
		recs = append(recs, &kgo.Record{Key: fmt.Appendf(nil, "customer-%d", i%10), Value: val(i%10, 100+i)})
	}
	recs = append(recs, &kgo.Record{Key: recs[1].Key, Value: recs[1].Value}, &kgo.Record{Key: recs[1].Key, Value: recs[1].Value},
		&kgo.Record{Key: recs[2].Key, Value: recs[2].Value})
	if err := raw.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	kc, err := kafka.New(cl.ListenAddrs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kc.Close)
	r, err := dupTool(t, kc).Run(roomCtx(), json.RawMessage(`{"topic":"`+topic+`","since":"2000-01-01T00:00:00Z"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(r.Summary, "pg.public.orders: 3 duplicate events across 2 keys, 0 order regressions in 100 records") {
		t.Fatalf("summary = %q", r.Summary)
	}
	data, _ := json.Marshal(r.Data)
	for _, leak := range []string{"customer-", "jane.doe", "corp.example", `"after"`, `"op"`} {
		if strings.Contains(r.Summary, leak) || strings.Contains(string(data), leak) {
			t.Errorf("%q leaked:\nsummary %s\ndata %s", leak, r.Summary, data)
		}
	}
	if !strings.Contains(string(data), kafka.KeyFingerprint([]byte("customer-1"))) {
		t.Errorf("data has no fingerprint of the duplicated key: %s", data)
	}
}

// TestDupKeysManyOpenPartitions (review of X1c): a partial scan of a topic
// with hundreds of partitions names the first 20 and counts the rest.
func TestDupKeysManyOpenPartitions(t *testing.T) {
	parts := make([]clients.DupPartition, 500)
	for i := range parts {
		parts[i] = clients.DupPartition{Partition: int32(i)}
	}
	s := dupSummary(clients.DupReport{Topic: "pg.public.orders", Scanned: 9, Partial: true, Partitions: parts})
	if !strings.Contains(s, "19] and 480 more") || strings.Contains(s, " 20 ") || len(s) > 400 {
		t.Fatalf("summary = %q", s)
	}
}
