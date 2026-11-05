package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// TestKFakePlannerOnReaderClient: the Planner plans over a reader Client
// (no Writer built, write mode off) the same moves the Writer would commit,
// and commits nothing.
func TestKFakePlannerOnReaderClient(t *testing.T) {
	const topic, group = "kf-plan", "kf-plan-group"
	addrs, raw, w := kfWriterCluster(t, 2, topic)
	old := time.Now().Add(-time.Hour)
	produceAt(t, raw, topic, 0, old, old, old)
	produceAt(t, raw, topic, 1, old)
	commitAt(t, raw, group, topic, map[int32]int64{0: 3, 1: 1})

	rc, err := New(addrs, nil)
	if err != nil {
		t.Fatalf("reader client: %v", err)
	}
	t.Cleanup(rc.Close)
	p := NewPlanner(rc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.CheckGroupInactive(ctx, group); err != nil {
		t.Fatalf("CheckGroupInactive: %v", err)
	}
	// Plain ctx: the planner only reads, so it needs no scope.
	got, err := p.PlanGroupReset(ctx, group, clients.OffsetTarget{Earliest: true})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	want, err := w.PlanGroupReset(unscopedCtx(t), group, clients.OffsetTarget{Earliest: true})
	if err != nil {
		t.Fatalf("writer plan: %v", err)
	}
	if len(got) != 2 || len(want) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("planner %+v, writer %+v; want equal", got, want)
	}
	if got[0] != (clients.OffsetMove{Topic: topic, Partition: 0, Committed: 3, Target: 0, End: 3}) {
		t.Fatalf("p0 = %+v", got[0])
	}
	if c := committedNow(t, raw, group, topic); c[0] != 3 || c[1] != 1 {
		t.Fatalf("planner changed offsets: %v", c)
	}
}

func TestPlannerNilClient(t *testing.T) {
	var nilP *Planner
	for _, p := range []*Planner{NewPlanner(nil), nilP} {
		if err := p.CheckGroupInactive(context.Background(), "g"); !errors.Is(err, errNoPlannerClient) {
			t.Fatalf("CheckGroupInactive = %v", err)
		}
		if _, err := p.PlanGroupReset(context.Background(), "g", clients.OffsetTarget{Earliest: true}); !errors.Is(err, errNoPlannerClient) {
			t.Fatalf("PlanGroupReset = %v", err)
		}
	}
}
