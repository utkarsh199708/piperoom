package room

import (
	"testing"
	"time"

	"piperoom/internal/store/memstore"
)

// QA R4: an alert's layer label nobody owns is skipped: the fallback goes on
// to the first layer that has an owner, so the room is still paged.
func TestAutoPageFallbackSkipsLabelNobodyOwns(t *testing.T) {
	c := &testClock{t: pageT0}
	ms := memstore.NewWithClock(c.Now)
	r := newPageRig(t, ms, c, 5*time.Minute, nil)
	r.svc.users = mustUsers([]User{{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}}})
	r.load()
	id := openAMRoom(t, r.svc, "a", `{"commonLabels":{"layer":"warehouse"},"alerts":[]}`)
	c.Advance(6 * time.Minute)
	r.tick()
	r.tick()
	if got := autoPages(t, ms, id); len(got) != 1 {
		t.Fatalf("auto pages = %+v, want one to someone (kabir owns consumer)", got)
	}
}
