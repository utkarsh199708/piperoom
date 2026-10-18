package room

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// sweepWindowStore is a memstore that says it is shared (store.Coordinator)
// and records the unclaimed window of each stale-sweep query. Coordinator
// methods the test does not reach are left nil and panic if called.
type sweepWindowStore struct {
	*memstore.Store
	store.Coordinator
	mu        sync.Mutex
	unclaimed []time.Duration
}

func (s *sweepWindowStore) StaleProposals(_ context.Context, _, unclaimed time.Duration, _ int) ([]store.StaleProposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unclaimed = append(s.unclaimed, unclaimed)
	return nil, nil
}

func (s *sweepWindowStore) ExpireRoomLeases(context.Context, string) error { return nil }

// With write mode off the stale sweep never lists a fully approved, unclaimed
// proposal: it is blocked, not abandoned by a dead replica. With write mode
// on the window is the configured one, as before.
func TestSweepUnclaimedWindowFollowsWriteMode(t *testing.T) {
	for _, tc := range []struct {
		off  bool
		want time.Duration
	}{{false, DefaultUnclaimedStale}, {true, writeOffUnclaimedStale}} {
		st := &sweepWindowStore{Store: memstore.New()}
		s := New(Deps{Store: st, WriteModeOff: tc.off})
		s.sweepStaleProposals()
		s.Close()
		if len(st.unclaimed) != 1 || st.unclaimed[0] != tc.want {
			t.Errorf("off=%v: unclaimed windows %v, want [%v]", tc.off, st.unclaimed, tc.want)
		}
	}
	// The window must stay far beyond any real wait, and fit the store's
	// millisecond parameter.
	if writeOffUnclaimedStale < 50*365*24*time.Hour || writeOffUnclaimedStale.Milliseconds() <= 0 {
		t.Fatalf("writeOffUnclaimedStale = %v", writeOffUnclaimedStale)
	}
}

// A proposal that reaches the executor some other way than the queue is
// refused before its claim: nothing is written.
func TestExecuteProposalRefusesWithWriteModeOff(t *testing.T) {
	st := memstore.New()
	s := New(Deps{Store: st, WriteModeOff: true})
	defer s.Close()
	err := s.executeProposal(store.ProposalRow{ProposalID: "p_x", RoomID: "r_x", Steps: []byte(`[{"action":"a","layer":"postgres"}]`)}, false)
	if !errors.Is(err, errWriteModeOff) {
		t.Fatalf("executeProposal = %v, want errWriteModeOff", err)
	}
	if got := st.ProposalStepStatus("p_x", 0); got != "" {
		t.Fatalf("step 0 status %q, want nothing recorded", got)
	}
	if s.WriteOn() {
		t.Fatal("WriteOn = true with WriteModeOff")
	}
}
