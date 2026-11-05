package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/watch"
	"piperoom/internal/wiring"
)

// lagKafka is a Kafka admin whose GroupLag works; the embedded nil interface
// is never called otherwise.
type lagKafka struct{ clients.KafkaAdmin }

func (lagKafka) GroupLag(context.Context, string) (clients.GroupLag, error) {
	return clients.GroupLag{}, nil
}

type oneStack struct{ s *wiring.Stack }

func (o oneStack) For(string) *wiring.Stack { return o.s }

func stackWith(k clients.KafkaAdmin) oneStack {
	return oneStack{&wiring.Stack{Name: "pipeline orders", Clients: &wiring.Wired{Kafka: k}}}
}

func TestWatchClientsSetsLagWhenKafkaHasGroupLag(t *testing.T) {
	pc := watchClients(stackWith(lagKafka{}))("orders")
	if pc.Lag == nil {
		t.Fatal("Lag is nil although the Kafka client implements GroupLag")
	}
}

func TestWatchClientsLeavesLagNilWithoutKafka(t *testing.T) {
	if pc := watchClients(stackWith(nil))("orders"); pc.Lag != nil {
		t.Fatal("Lag set for a set without a Kafka client")
	}
	if pc := watchClients(oneStack{&wiring.Stack{Name: wiring.NoStack}})("orders"); pc.Lag != nil {
		t.Fatal("Lag set for the empty client set")
	}
}

func TestCheckWatchLagNamesPipelineWithoutReader(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	o := loadTestOwners(t)

	bad := checkWatchLag(log, config.WatchLog, o, watchClients(stackWith(nil)))
	if len(bad) != 1 || bad[0] != "orders" {
		t.Fatalf("bad = %v, want [orders]", bad)
	}
	if out := buf.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "pipeline=orders") {
		t.Fatalf("log = %q, want an ERROR naming the pipeline", out)
	}

	buf.Reset()
	if bad := checkWatchLag(log, config.WatchLog, o, watchClients(stackWith(lagKafka{}))); len(bad) != 0 || buf.Len() != 0 {
		t.Fatalf("with a reader: bad = %v, log = %q, want none", bad, buf.String())
	}
	if bad := checkWatchLag(log, config.WatchOff, o, watchClients(stackWith(nil))); len(bad) != 0 {
		t.Fatalf("watch off: bad = %v, want none", bad)
	}
}

var _ watch.LagReader = lagKafka{}
