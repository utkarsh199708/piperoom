//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/stacklock"
	"piperoom/internal/testenv"
)

// TestIntegrationRetentionLossRealBroker proves the part of retention-loss the
// unit tests stub: on a real broker, after the break the group's committed
// offset really falls below the topic's ListStartOffsets — the path
// committedBelowLogStart takes when belowLogStartFn is unset (the unit test
// retentionWorld always sets it). It runs the break against a unique topic so
// it never touches the shared pg.public.orders pipeline, then restores the
// topic and proves the committed offset recovers back above the log start.
// It holds the shared stack lock (docs/collab.md §5) and, via devStack, skips
// rather than fails when the dev stack is unreachable.
func TestIntegrationRetentionLossRealBroker(t *testing.T) {
	s := devStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	acquireStackLock(t)

	topic := testenv.Name("retention")
	group := testenv.Name("group")
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer ccancel()
		if err := s.deleteTopics(cctx, topic); err != nil {
			t.Errorf("cleanup: delete topic %s: %v", topic, err)
		}
		cl, err := s.kafkaClient()
		if err != nil {
			t.Errorf("cleanup: kafka client: %v", err)
			return
		}
		defer cl.Close()
		if _, err := kadm.NewClient(cl).DeleteGroups(cctx, group); err != nil {
			t.Errorf("cleanup: delete group %s: %v", group, err)
		}
	})

	if err := s.ensureTopic(ctx, topic, 1); err != nil {
		t.Fatal(err)
	}

	// Seed one record and pin the group's committed offset at it (offset 0):
	// the position the short retention must delete.
	if err := s.produce(ctx, []*kgo.Record{{Topic: topic, Value: []byte("seed")}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.resetGroupToStart(ctx, group, topic); err != nil {
		t.Fatalf("commit start offset: %v", err)
	}

	// Break: cut the retention, roll the seed's segment past it, and wait for
	// the committed offset to fall below the log start (the real broker path).
	parts, err := s.breakRetention(ctx, group, topic, func(ctx context.Context) error {
		// Let the seed segment age past the 1 s retention/segment window, then
		// write the burst; its first record rolls the aged segment and the
		// broker's 1 s retention check deletes it, moving the log start past
		// the committed offset.
		if err := sleepCtx(ctx, 2*time.Second); err != nil {
			return err
		}
		recs := make([]*kgo.Record, 0, retentionOrders)
		for i := 0; i < retentionOrders; i++ {
			recs = append(recs, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("burst-%d", i))})
		}
		return s.produce(ctx, recs)
	})
	if err != nil {
		t.Fatalf("break: %v", err)
	}
	if len(parts) == 0 {
		t.Fatal("break reported no partitions below the log start")
	}

	// The real check, right after the break: the committed offset is below the
	// log start on the broker.
	if below, ps, err := s.committedBelowLogStart(ctx, group, topic); err != nil {
		t.Fatalf("committedBelowLogStart: %v", err)
	} else if !below {
		t.Fatalf("after break, committed offset is not below the log start (partitions %v)", ps)
	}

	// Fix: restore the broker defaults, then reset the group to the current log
	// start (what the consumer's auto.offset.reset=earliest does on rejoin).
	if err := s.fixRetention(ctx, topic); err != nil {
		t.Fatalf("fix: %v", err)
	}
	if err := s.resetGroupToStart(ctx, group, topic); err != nil {
		t.Fatalf("reset after fix: %v", err)
	}
	if below, _, err := s.committedBelowLogStart(ctx, group, topic); err != nil {
		t.Fatalf("committedBelowLogStart after fix: %v", err)
	} else if below {
		t.Fatal("after fix and reset, the committed offset is still below the log start")
	}
}

// acquireStackLock holds the shared stack lock for the duration of the test.
// When the owner already holds it (the CI job locks as "ci") it is re-entrant
// and the test leaves the existing lock in place; otherwise it acquires and
// releases it.
func acquireStackLock(t *testing.T) {
	t.Helper()
	owner := os.Getenv(ownerEnv)
	if owner == "" {
		owner = "devctl-retention-test"
	}
	lk := stacklock.New(stacklock.Path())
	rec, err := lk.Status()
	if err != nil {
		t.Fatalf("read stack lock: %v", err)
	}
	heldByUs := rec != nil && rec.Owner == owner
	if _, err := lk.Acquire(owner); err != nil {
		t.Fatalf("acquire stack lock: %v", err)
	}
	if !heldByUs {
		t.Cleanup(func() {
			if err := lk.Release(owner); err != nil {
				t.Errorf("release stack lock: %v", err)
			}
		})
	}
}
