package slackbridge

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/events"
)

func pagedEvent(seq int64, owners []string, reason string) events.Event {
	return ev(seq, events.OwnerPaged, "bo", events.OwnerPagedPayload{Layer: "consumer", Owners: owners, Reason: reason})
}

// owner.paged is posted in the thread and pages owners that have a Slack ID
// in Users; owners without one, and event text, never become mentions (Q2 F5).
func TestPagingOwnerPaged(t *testing.T) {
	created := ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})
	fs := &fakeSlack{}
	attack := "<!channel> <@U0EVIL0001> look & <https://x|y>"
	st := &fakeStore{evs: []events.Event{created, pagedEvent(2, []string{"bo", "zed", "ana"}, attack)}}
	b, _, _ := newBridge(t, st, fs, func(o *Options) { o.Paging = pagingOpts() })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = b.Run(ctx, "r1")
	n, ids := mentionCount(fs.reqs)
	if n != 1 || ids[owner2] == 0 || ids[owner3] == 0 || len(ids) != 2 {
		t.Fatalf("page messages = %d, mentions %v, want one message for bo and ana", n, ids)
	}
	if fs.count() != 3 { // parent, notice, page
		t.Errorf("requests = %d", fs.count())
	}
	assertOnlyTrustedMarkup(t, fs.reqs, "owner.paged")
}

// Without a paging allow-list the page is still shown in the thread.
func TestOwnerPagedPostedWithoutPaging(t *testing.T) {
	created := ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: []events.Event{created, pagedEvent(2, []string{}, "lag")}}, fs, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = b.Run(ctx, "r1")
	if fs.count() != 2 {
		t.Fatalf("requests = %d, want parent + notice", fs.count())
	}
}
