package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/config"
)

func TestBelowLogStart(t *testing.T) {
	tests := []struct {
		name      string
		committed map[int32]int64
		start     map[int32]int64
		want      bool
		wantParts []int32
	}{
		{"all above", map[int32]int64{0: 100, 1: 50}, map[int32]int64{0: 0, 1: 0}, false, nil},
		{"one below", map[int32]int64{0: 100, 1: 5}, map[int32]int64{0: 0, 1: 10}, true, []int32{1}},
		{"equal is not below", map[int32]int64{0: 10}, map[int32]int64{0: 10}, false, nil},
		{"no committed offset skipped", map[int32]int64{0: -1, 1: 3}, map[int32]int64{0: 0, 1: 5}, true, []int32{1}},
		{"sorted parts", map[int32]int64{2: 0, 0: 0, 1: 0}, map[int32]int64{0: 5, 1: 5, 2: 5}, true, []int32{0, 1, 2}},
		{"partition absent from start", map[int32]int64{0: 3, 1: 3}, map[int32]int64{0: 5}, true, []int32{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			below, parts := belowLogStart(tt.committed, tt.start)
			if below != tt.want || !slices.Equal(parts, tt.wantParts) {
				t.Fatalf("belowLogStart(%v, %v) = %v %v, want %v %v", tt.committed, tt.start, below, parts, tt.want, tt.wantParts)
			}
		})
	}
}

// retentionWorld plays the parts of the dev stack that retention-loss touches:
// docker compose (devconsumer stop/start), the consumer group, Postgres (the
// insert) and Kafka (the topic the break reconfigures). The committed-below-
// log-start read is stubbed, since the fake broker does not age out segments.
type retentionWorld struct {
	t           *testing.T
	addrs       []string
	mu          sync.Mutex
	consumerUp  bool
	composeArgs []string
	sql         []string
}

func newRetentionWorld(t *testing.T) *retentionWorld {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, ordersTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	return &retentionWorld{t: t, addrs: c.ListenAddrs(), consumerUp: true}
}

func (w *retentionWorld) compose(_ context.Context, _ map[string]string, args ...string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch strings.Join(args, " ") {
	case "stop devconsumer":
		w.consumerUp = false
	case "start devconsumer":
		w.consumerUp = true
	default:
		return fmt.Errorf("fake compose: unexpected %q", strings.Join(args, " "))
	}
	w.composeArgs = append(w.composeArgs, strings.Join(args, " "))
	return nil
}

func (w *retentionWorld) group(_ context.Context, _ string) (groupView, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.consumerUp {
		return groupView{state: "Stable", members: 1}, nil
	}
	return groupView{state: "Empty"}, nil
}

func (w *retentionWorld) psql(_ context.Context, db string, stmts ...string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if db != "orders" {
		return "", fmt.Errorf("fake postgres: unexpected db %q", db)
	}
	w.sql = append(w.sql, stmts...)
	return "", nil
}

func (w *retentionWorld) stack(below bool) (*stack, *bytes.Buffer) {
	w.t.Helper()
	var logs bytes.Buffer
	return &stack{
		cfg:             config.Config{KafkaBrokers: strings.Join(w.addrs, ",")},
		log:             slog.New(slog.NewTextHandler(&logs, nil)),
		composeFn:       w.compose,
		groupFn:         w.group,
		sqlFn:           w.psql,
		belowLogStartFn: func(context.Context, string, string) (bool, []int32, error) { return below, []int32{0, 1, 2}, nil },
		poll:            10 * time.Millisecond,
	}, &logs
}

func (w *retentionWorld) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	w.t.Cleanup(cancel)
	return ctx
}

// topicConfig reads a topic config value through kadm against the fake broker.
func (w *retentionWorld) topicConfig(key string) string {
	w.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(w.addrs...))
	if err != nil {
		w.t.Fatalf("kgo client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfgs, err := kadm.NewClient(cl).DescribeTopicConfigs(ctx, ordersTopic)
	if err != nil {
		w.t.Fatalf("describe topic configs: %v", err)
	}
	for _, rc := range cfgs {
		if rc.Err != nil {
			w.t.Fatalf("describe %s: %v", rc.Name, rc.Err)
		}
		for _, c := range rc.Configs {
			if c.Key == key {
				return c.MaybeValue()
			}
		}
	}
	return ""
}

func TestRetentionLossBreakAndFix(t *testing.T) {
	w := newRetentionWorld(t)
	s, logs := w.stack(true)
	ctx := w.ctx()

	if err := breakRetentionLoss(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.mu.Lock()
	stopped := !w.consumerUp
	inserted := slices.Contains(w.sql, sqlInsertRetentionLoss)
	w.mu.Unlock()
	if !stopped {
		t.Error("break did not stop the consumer")
	}
	if !inserted {
		t.Errorf("break did not insert the %d orders (%v)", retentionOrders, w.sql)
	}
	if got := w.topicConfig("retention.ms"); got != "1000" {
		t.Errorf("retention.ms = %q, want 1000", got)
	}
	if got := w.topicConfig("segment.ms"); got != "1000" {
		t.Errorf("segment.ms = %q, want 1000", got)
	}
	if !strings.Contains(logs.String(), "offset_below_log_start") {
		t.Errorf("break did not log the watcher signal:\n%s", logs)
	}

	if err := fixRetentionLoss(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.mu.Lock()
	started := w.consumerUp
	w.mu.Unlock()
	if !started {
		t.Error("fix did not restart the consumer")
	}
	if got := w.topicConfig("retention.ms"); got == "1000" {
		t.Errorf("fix left retention.ms = %q", got)
	}
	if got := w.topicConfig("segment.ms"); got == "1000" {
		t.Errorf("fix left segment.ms = %q", got)
	}
	// Idempotent: running the fix again succeeds without changing the stack.
	if err := fixRetentionLoss(ctx, s); err != nil {
		t.Fatalf("second fix: %v\n%s", err, logs)
	}
}

// The committed offsets never fall below the log start: the break reports the
// timeout rather than claiming a broken stack.
func TestRetentionLossBreakOffsetsStayInRange(t *testing.T) {
	w := newRetentionWorld(t)
	s, _ := w.stack(false)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := breakRetentionLoss(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "did not fall below the log start") {
		t.Fatalf("break = %v, want a timeout", err)
	}
}
