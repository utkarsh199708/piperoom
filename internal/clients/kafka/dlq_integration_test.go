//go:build integration

package kafka

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/testenv"
)

// TestDLQReaderAgainstDevStack produces Connect-style error records to a temp
// topic and checks DLQOffsets/PeekDLQ: absent before creation (and not
// auto-created), counts after, newest first, redacted, no key or value.
func TestDLQReaderAgainstDevStack(t *testing.T) {
	cfg := testenv.Require(t)
	brokers := strings.Split(cfg.KafkaBrokers, ",")
	c := newClient(t)
	ctx := context.Background()

	topic := testenv.Name("it-orders-dlq")
	offs, err := c.DLQOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := offs[topic]; ok {
		t.Fatalf("topic %s should not exist yet", topic)
	}
	if _, err := c.PeekDLQ(ctx, topic, 3); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("peek of missing topic: %v, want ErrNotFound", err)
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()
	adm := kadm.NewClient(producer)
	if td, err := adm.ListTopics(ctx, topic); err != nil || td.Has(topic) {
		t.Fatalf("topic was auto-created by a read: %v, %v", td, err)
	}

	resps, err := adm.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil || resps.Error() != nil {
		t.Fatalf("CreateTopics: %v %v", err, resps.Error())
	}
	t.Cleanup(func() {
		if _, err := adm.DeleteTopics(context.Background(), topic); err != nil {
			t.Logf("DeleteTopics: %v", err)
		}
	})

	const keyJunk, valJunk = "JUNKKEY-it-91", "JUNKVAL-it-37"
	base := time.Now().Add(-time.Minute)
	var recs []*kgo.Record
	for i := 0; i < 5; i++ {
		msg := "Unexpected character at field " + string(rune('a'+i))
		if i == 4 {
			msg = `Unexpected character at [Source: (byte[])"{"email":"` + valJunk + `"}"; line: 1, column: 2]`
		}
		recs = append(recs, &kgo.Record{
			Topic: topic, Key: []byte(keyJunk), Value: []byte(`{"v":"` + valJunk + `"}`),
			Timestamp: base.Add(time.Duration(i) * time.Second),
			Headers: []kgo.RecordHeader{
				{Key: "__connect.errors.exception.class.name", Value: []byte("org.apache.kafka.connect.errors.DataException")},
				{Key: "__connect.errors.exception.message", Value: []byte(msg)},
				{Key: "app-secret", Value: []byte("nope")},
			},
		})
	}
	if err := producer.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	offs, err = c.DLQOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if p := offs[topic]; len(p) != 1 || p[0].Start != 0 || p[0].End != 5 {
		t.Fatalf("offsets = %+v, want Start=0 End=5", offs[topic])
	}

	got, err := c.PeekDLQ(ctx, topic, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Offset != 4 || got[1].Offset != 3 || got[2].Offset != 2 {
		t.Fatalf("peek = %+v, want offsets 4,3,2", got)
	}
	if got[0].Headers["__connect.errors.exception.class.name"] == "" {
		t.Fatalf("headers missing: %+v", got[0])
	}
	if _, ok := got[0].Headers["app-secret"]; ok {
		t.Fatal("application header leaked")
	}
	if m := got[0].Headers["__connect.errors.exception.message"]; strings.Contains(m, valJunk) || strings.Contains(m, "Source") {
		t.Fatalf("message not redacted: %q", m)
	}
	b, _ := json.Marshal(got)
	for _, m := range []string{keyJunk, valJunk} {
		for _, f := range []string{m, base64.StdEncoding.EncodeToString([]byte(m)), hex.EncodeToString([]byte(m))} {
			if strings.Contains(string(b), f) {
				t.Fatalf("%q leaked into %s", f, b)
			}
		}
	}
}
