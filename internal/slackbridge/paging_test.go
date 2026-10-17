package slackbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

const (
	owner1 = "U0ARJUN01"
	owner2 = "U0BO0001A"
	owner3 = "U0ANA0001" // lapsed driver "ana"
	oncall = "U0ONCALL1" // NoDriver
)

func pagingOpts() PagingOptions {
	return PagingOptions{
		Layers:   map[string][]string{"kafka": {owner1}},
		Users:    map[string]string{"bo": owner2, "ana": owner3},
		NoDriver: []string{oncall},
	}
}

// strings collects every string value in a decoded JSON document.
func jsonStrings(v any, out *[]string) {
	switch x := v.(type) {
	case string:
		*out = append(*out, x)
	case []any:
		for _, e := range x {
			jsonStrings(e, out)
		}
	case map[string]any:
		for _, e := range x {
			jsonStrings(e, out)
		}
	}
}

var trustedMarkup = regexp.MustCompile(`<@(` + owner1 + `|` + owner2 + `|` + owner3 + `|` + oncall + `)>|<https://pr\.example/rooms/r1\|Open room>`)

// assertOnlyTrustedMarkup fails if any request holds a < or > that is not an
// allow-listed mention or our own room link, i.e. if event text could render
// as a mention, broadcast, link or channel reference.
func assertOnlyTrustedMarkup(t *testing.T, reqs []map[string]any, attack string) {
	t.Helper()
	for i, r := range reqs {
		var ss []string
		jsonStrings(r, &ss)
		for _, s := range ss {
			rest := trustedMarkup.ReplaceAllString(s, "")
			if strings.ContainsAny(rest, "<>") {
				t.Errorf("attack %q: request %d has live markup: %q", attack, i, s)
			}
		}
	}
}

func mentionCount(reqs []map[string]any) (n int, ids map[string]int) {
	ids = map[string]int{}
	re := regexp.MustCompile(`<@([^>]*)>`)
	for _, r := range reqs {
		raw, _ := json.Marshal(r)
		var decoded map[string]any
		_ = json.Unmarshal(raw, &decoded)
		var ss []string
		jsonStrings(decoded, &ss)
		seen := false
		for _, s := range ss {
			for _, m := range re.FindAllStringSubmatch(s, -1) {
				ids[m[1]]++
				seen = true
			}
		}
		if seen {
			n++
		}
	}
	return n, ids
}

var injections = []string{
	"<@U0EVIL001>",
	"<@U0EVIL001|admin>",
	"<!here>",
	"<!channel>",
	"<!everyone>",
	"<!subteam^S0EVIL001>",
	"<!subteam^S0EVIL001|@oncall>",
	"<#C0EVIL001|general>",
	"&lt;@U0EVIL001&gt;",
	"&lt;!channel&gt;",
	"<https://evil.example|click here>",
	"<mailto:a@evil.example|mail>",
	"`<@U0EVIL001>`",
	"```<!here>```",
	"<\x00@U0EVIL001>",
	"<​!channel>",
	"‮<@U0EVIL001>",
	"<@U0EVIL001\xff>",
	"ok\n<!channel>\n<@U0EVIL001>",
}

// Every text field an event carries gets each injection attempt. Only the two
// allow-listed IDs may ever render as mentions, once each.
func TestPagingInjectionInEveryTextField(t *testing.T) {
	for _, a := range injections {
		t.Run(fmt.Sprintf("%q", a), func(t *testing.T) {
			params, _ := json.Marshal(map[string]string{a: a})
			steps := []events.ProposalStep{{Action: a, Layer: a, Description: a, Params: params}}
			evs := []events.Event{
				ev(1, events.RoomCreated, a, events.RoomCreatedPayload{Title: a, Severity: a, PipelineID: a, Alert: json.RawMessage(`{"x":"` + "<!channel>" + `"}`)}),
				ev(2, events.AgentHypothesis, a, events.AgentHypothesisPayload{Text: a, Confidence: a}),
				ev(3, events.ProposalCreated, a, events.ProposalCreatedPayload{ProposalID: a, Title: a, RequiredLayers: []string{"kafka", a}, Risk: a, Rollback: a, Steps: steps}),
				ev(4, events.ApprovalGiven, a, events.ApprovalGivenPayload{ProposalID: a, Layer: a}),
				ev(5, events.ApprovalRejected, a, events.ApprovalRejectedPayload{ProposalID: a, Layer: a, Reason: a}),
				ev(6, events.ActionExecuted, a, events.ActionExecutedPayload{ProposalID: a, Output: a}),
				ev(7, events.HandoffCreated, a, events.HandoffCreatedPayload{To: events.Actor{ID: "bo", Name: a},
					Summary: events.HandoffSummary{WhatBroke: a, Why: a, Open: []string{a}, Risks: []string{a}}}),
				ev(8, events.HandoffAccepted, a, struct{}{}),
				ev(9, events.ControlLeaseExpired, "system", events.ControlLeaseExpiredPayload{
					Driver: events.Actor{ID: "ana", Name: a}, LeaseUntil: time.Unix(1700000000, 0)}),
				ev(10, events.ApprovalNudged, a, events.ApprovalNudgedPayload{ProposalID: a, Layer: "kafka"}),
				ev(11, events.ApprovalNudged, a, events.ApprovalNudgedPayload{ProposalID: a, Layer: a}), // unpaged layer
				ev(12, events.RoomClosed, a, events.RoomClosedPayload{Resolution: a}),
			}
			fs := &fakeSlack{}
			// A tiny gap lets owner1 be paged by both the proposal and the nudge.
			b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) {
				o.Paging = pagingOpts()
				o.Paging.Limits.UserRoomInterval = time.Nanosecond
			})
			if err := b.Run(context.Background(), "r1"); err != nil {
				t.Fatal(err)
			}
			if fs.count() != 16 { // 12 mirrored + proposal, hand-off, lease and nudge pages
				t.Fatalf("requests = %d, want 16", fs.count())
			}
			assertOnlyTrustedMarkup(t, fs.reqs, a)
			n, ids := mentionCount(fs.reqs)
			if n != 4 || len(ids) != 4 || ids[owner1] == 0 || ids[owner2] == 0 || ids[owner3] == 0 || ids[oncall] == 0 {
				t.Errorf("mentions: messages=%d ids=%v", n, ids)
			}
			raw, _ := json.Marshal(fs.reqs)
			if strings.Contains(string(raw), "U0EVIL001>") && strings.Contains(string(raw), "<@U0EVIL001") {
				t.Error("injected ID rendered as a mention")
			}
		})
	}
}

func TestPagingNotAllowListed(t *testing.T) {
	cases := map[string]events.Event{
		"unknown layer": ev(2, events.ProposalCreated, "agent", events.ProposalCreatedPayload{
			ProposalID: "p1", Title: "t", RequiredLayers: []string{"unknown", "<@U0EVIL001>"}}),
		"hand-off to a Slack-looking ID": ev(2, events.HandoffCreated, "ana", events.HandoffCreatedPayload{
			To: events.Actor{ID: "U0EVIL001", Name: "Eve"}}),
		"hand-off to an allow-listed Slack ID instead of a user key": ev(2, events.HandoffCreated, "ana", events.HandoffCreatedPayload{
			To: events.Actor{ID: owner2, Name: "Bo"}}),
		"hand-off with no target": ev(2, events.HandoffCreated, "ana", events.HandoffCreatedPayload{}),
	}
	for name, e := range cases {
		t.Run(name, func(t *testing.T) {
			evs := []events.Event{ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t", Severity: "sev2"}), e}
			fs := &fakeSlack{}
			b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) { o.Paging = pagingOpts() })
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_ = b.Run(ctx, "r1")
			if n, _ := mentionCount(fs.reqs); n != 0 || fs.count() != 2 {
				t.Errorf("paged: requests=%d mention messages=%d", fs.count(), n)
			}
		})
	}
}

func leaseEvent(seq int64, driverID string, until time.Time) events.Event {
	return ev(seq, events.ControlLeaseExpired, "system", events.ControlLeaseExpiredPayload{
		Driver: events.Actor{ID: driverID, Name: "Ana"}, LeaseUntil: until})
}

// Lease expiry pages the lapsed driver (if allow-listed) and the on-call IDs.
// A driver ID that is not a Users key, even a Slack-looking one, pages nobody.
func TestPagingLeaseExpired(t *testing.T) {
	until := time.Unix(1700000000, 0)
	created := ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})
	cases := []struct {
		name   string
		opts   PagingOptions
		driver string
		want   []string // mentioned IDs, sorted by name below
	}{
		{"listed driver and on-call", pagingOpts(), "ana", []string{oncall, owner3}},
		{"unlisted driver: on-call only", pagingOpts(), "U0EVIL001", []string{oncall}},
		{"driver id is an allow-listed Slack ID, not a key", pagingOpts(), owner3, []string{oncall}},
		{"no on-call configured, unlisted driver", PagingOptions{Users: map[string]string{"ana": owner3}}, "zed", nil},
		{"no on-call configured, listed driver", PagingOptions{Users: map[string]string{"ana": owner3}}, "ana", []string{owner3}},
		{"on-call only", PagingOptions{NoDriver: []string{oncall}}, "ana", []string{oncall}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs := &fakeSlack{}
			b, _, _ := newBridge(t, &fakeStore{evs: []events.Event{created, leaseEvent(2, c.driver, until)}}, fs, func(o *Options) { o.Paging = c.opts })
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_ = b.Run(ctx, "r1")
			_, ids := mentionCount(fs.reqs)
			if len(ids) != len(c.want) {
				t.Fatalf("mentioned %v, want %v", ids, c.want)
			}
			for _, w := range c.want {
				if ids[w] == 0 {
					t.Errorf("%s not paged: %v", w, ids)
				}
			}
			if fs.count() != 3-boolToInt(len(c.want) == 0) { // parent, lease notice, page
				t.Errorf("requests = %d", fs.count())
			}
			assertOnlyTrustedMarkup(t, fs.reqs, c.name)
		})
	}
	t.Run("same lease twice pages once, a new lease later pages again", func(t *testing.T) {
		st := &fakeStore{evs: []events.Event{created, leaseEvent(2, "ana", until), leaseEvent(3, "ana", until)}}
		fs := &fakeSlack{}
		b, _, _ := newBridge(t, st, fs, func(o *Options) { o.Paging = pagingOpts() })
		now := time.Unix(1700000000, 0)
		b.pager.now = func() time.Time { return now }
		run := func() {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_ = b.Run(ctx, "r1")
		}
		run()
		if n, _ := mentionCount(fs.reqs); n != 1 {
			t.Fatalf("pages = %d, want 1", n)
		}
		now = now.Add(11 * time.Minute)
		st.evs = append(st.evs, leaseEvent(4, "ana", until.Add(15*time.Minute)))
		run()
		if n, _ := mentionCount(fs.reqs); n != 2 {
			t.Errorf("pages = %d, want 2", n)
		}
	})
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestPagingEmptyAllowListIsOff(t *testing.T) {
	fs := &fakeSlack{}
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"}),
		ev(2, events.ProposalCreated, "agent", events.ProposalCreatedPayload{ProposalID: "p1", Title: "t", RequiredLayers: []string{"kafka"}}),
		ev(3, events.HandoffCreated, "ana", events.HandoffCreatedPayload{To: events.Actor{ID: "bo"}}),
		ev(4, events.RoomClosed, "ana", events.RoomClosedPayload{Resolution: "ok"}),
	}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) {
		o.Paging = PagingOptions{Layers: map[string][]string{}, Users: map[string]string{}}
	})
	if b.pager != nil {
		t.Fatal("pager built for an empty allow-list")
	}
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if n, _ := mentionCount(fs.reqs); n != 0 || fs.count() != 4 {
		t.Errorf("requests=%d mention messages=%d", fs.count(), n)
	}
}

func TestPagingDuplicatesAndRateLimit(t *testing.T) {
	prop := func(seq int64, id string) events.Event {
		return ev(seq, events.ProposalCreated, "agent", events.ProposalCreatedPayload{ProposalID: id, Title: "t", RequiredLayers: []string{"kafka"}})
	}
	// Same proposal twice: one page. A second proposal inside the 10 minute
	// gap: none. After the gap (second Run, clock moved): paged again.
	created := ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})
	st := &fakeStore{evs: []events.Event{created, prop(2, "p1"), prop(3, "p1"), prop(4, "p2")}}
	fs := &fakeSlack{}
	th := NewMemThreads()
	b, _, _ := newBridge(t, st, fs, func(o *Options) { o.Paging = pagingOpts(); o.Threads = th })
	now := time.Unix(1700000000, 0)
	b.pager.now = func() time.Time { return now }
	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_ = b.Run(ctx, "r1")
	}
	run()
	if n, _ := mentionCount(fs.reqs); n != 1 {
		t.Fatalf("mention messages=%d, want 1 (p1 only)", n)
	}
	now = now.Add(11 * time.Minute)
	st.evs = append(st.evs, prop(5, "p3"))
	run()
	if n, ids := mentionCount(fs.reqs); n != 2 || ids[owner1] != 4 { // 4 = block + fallback text, twice
		t.Errorf("mention messages=%d ids=%v, want 2 pages (p1, then p3)", n, ids)
	}
}

// A page that Slack did not accept is dropped, not retried, and must not use
// up the user's quota; the room keeps mirroring.
func TestPagingNonFatalSlackFailureDropsPageNotQuota(t *testing.T) {
	cases := map[string]struct {
		reply    func(n int, w http.ResponseWriter)
		wantReqs int
		wantCode string
	}{
		"503 five times": {func(n int, w http.ResponseWriter) {
			if n >= 3 && n <= 7 { // the page and its 4 retries
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"channel":"CREAL","ts":"1700000000.000101"}`))
		}, 8, "http_503"},
		"429 with long Retry-After": {func(n int, w http.ResponseWriter) {
			if n == 3 {
				w.Header().Set("Retry-After", "600")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"channel":"CREAL","ts":"1700000000.000101"}`))
		}, 4, "rate_limited"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			evs := []events.Event{
				ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"}),
				ev(2, events.ProposalCreated, "agent", events.ProposalCreatedPayload{ProposalID: "p1", Title: "t", RequiredLayers: []string{"kafka"}}),
				ev(3, events.RoomClosed, "ana", events.RoomClosedPayload{Resolution: "ok"}),
			}
			fs := &fakeSlack{reply: c.reply}
			var logs bytes.Buffer
			b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) {
				o.Paging = pagingOpts()
				o.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			})
			if err := b.Run(context.Background(), "r1"); err != nil {
				t.Fatalf("Run = %v, want nil: the room must keep mirroring", err)
			}
			if fs.count() != c.wantReqs {
				t.Errorf("requests = %d, want %d", fs.count(), c.wantReqs)
			}
			if !strings.Contains(logs.String(), "slack post failed") || !strings.Contains(logs.String(), "code="+c.wantCode) {
				t.Errorf("drop not logged with code %s: %s", c.wantCode, logs.String())
			}
			if strings.Contains(logs.String(), testToken) {
				t.Error("token in log")
			}
			// Nothing was booked, so the same user can still be paged.
			k := pageKey{"r1", owner1, "proposal:p1"}
			if len(b.pager.seen) != 0 || !b.pager.check(k, nil) {
				t.Error("failed page was recorded")
			}
		})
	}
}

func TestPagerLimits(t *testing.T) {
	now := time.Unix(1700000000, 0)
	mk := func(l PagingLimits) *pager {
		p, err := newPager(PagingOptions{Layers: map[string][]string{"k": {owner1}}, Limits: l})
		if err != nil {
			t.Fatal(err)
		}
		p.now = func() time.Time { return now }
		return p
	}
	// page = check then record, as the bridge does after a successful post.
	page := func(p *pager, room, user, reason string) bool {
		k := pageKey{room, user, reason}
		if !p.check(k, nil) {
			return false
		}
		p.record([]pageKey{k})
		return true
	}
	t.Run("check alone records nothing", func(t *testing.T) {
		p := mk(PagingLimits{})
		k := pageKey{"r1", owner1, "a"}
		if !p.check(k, nil) || !p.check(k, nil) {
			t.Error("check must not book the page")
		}
	})
	t.Run("same reason twice", func(t *testing.T) {
		p := mk(PagingLimits{UserRoomInterval: time.Nanosecond})
		if !page(p, "r1", owner1, "x") {
			t.Fatal("first denied")
		}
		now = now.Add(time.Hour)
		if page(p, "r1", owner1, "x") {
			t.Error("duplicate reason allowed")
		}
	})
	t.Run("per user per room gap", func(t *testing.T) {
		p := mk(PagingLimits{})
		if !page(p, "r1", owner1, "a") || page(p, "r1", owner1, "b") {
			t.Error("gap not enforced")
		}
		if !page(p, "r2", owner1, "c") {
			t.Error("another room should not be blocked by the gap")
		}
		now = now.Add(11 * time.Minute)
		if !page(p, "r1", owner1, "d") {
			t.Error("gap should have passed")
		}
	})
	t.Run("per user hourly cap across rooms", func(t *testing.T) {
		p := mk(PagingLimits{UserPerHour: 2})
		ok := []bool{page(p, "r1", owner1, "a"), page(p, "r2", owner1, "a"), page(p, "r3", owner1, "a")}
		if !ok[0] || !ok[1] || ok[2] {
			t.Errorf("got %v", ok)
		}
		now = now.Add(61 * time.Minute)
		if !page(p, "r3", owner1, "b") {
			t.Error("cap should have reset after an hour")
		}
	})
	t.Run("per room hourly cap", func(t *testing.T) {
		p := mk(PagingLimits{RoomPerHour: 2})
		ok := []bool{page(p, "r1", "UAAAAAAAA1", "a"), page(p, "r1", "UAAAAAAAA2", "a"), page(p, "r1", "UAAAAAAAA3", "a"), page(p, "r2", "UAAAAAAAA3", "a")}
		if !ok[0] || !ok[1] || ok[2] || !ok[3] {
			t.Errorf("got %v", ok)
		}
	})
	t.Run("cap counts users planned in the same message", func(t *testing.T) {
		p := mk(PagingLimits{RoomPerHour: 2})
		planned := []pageKey{{"r1", "UAAAAAAAA1", "a"}, {"r1", "UAAAAAAAA2", "a"}}
		if p.check(pageKey{"r1", "UAAAAAAAA3", "a"}, planned) {
			t.Error("planned pages not counted")
		}
	})
}

func TestPagingOptionsValidation(t *testing.T) {
	bad := []PagingOptions{
		{Layers: map[string][]string{"k": {"<@U0ARJUN01>"}}},
		{Layers: map[string][]string{"k": {"U0ARJUN01>|x"}}},
		{Layers: map[string][]string{"k": {"U0ARJUN01 U0ARJUN02"}}},
		{Layers: map[string][]string{"k": {"u0arjun01"}}},
		{Layers: map[string][]string{"k": {""}}},
		{Layers: map[string][]string{"k": {"S0ARJUN01"}}}, // user group, not a user
		{Layers: map[string][]string{"": {owner1}}},
		{Users: map[string]string{"bo": "!here"}},
		{Users: map[string]string{"": owner1}},
		{NoDriver: []string{"<!here>"}},
		{NoDriver: []string{""}},
	}
	for i, o := range bad {
		if _, err := newPager(o); err == nil {
			t.Errorf("case %d accepted: %+v", i, o)
		}
	}
	if _, err := newPager(pagingOpts()); err != nil {
		t.Errorf("valid options rejected: %v", err)
	}
	// New surfaces the error and never echoes the token.
	_, err := New(&fakeStore{}, Options{Token: testToken, ChannelID: "CCONF", Paging: bad[0]})
	if err == nil || strings.Contains(err.Error(), testToken) {
		t.Errorf("New err = %v", err)
	}
}

func TestPagingFatalSlackErrorStopsRoom(t *testing.T) {
	evs := []events.Event{
		ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"}),
		ev(2, events.ProposalCreated, "agent", events.ProposalCreatedPayload{ProposalID: "p1", Title: "t", RequiredLayers: []string{"kafka"}}),
	}
	fs := &fakeSlack{reply: func(n int, w http.ResponseWriter) {
		if n == 3 { // the page
			_, _ = w.Write([]byte(`{"ok":false,"error":"not_in_channel"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"channel":"CREAL","ts":"1700000000.000101"}`))
	}}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) { o.Paging = pagingOpts() })
	err := b.Run(context.Background(), "r1")
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "not_in_channel" {
		t.Errorf("err = %v", err)
	}
}

func nudgeEvent(seq int64, proposal, layer string) events.Event {
	return ev(seq, events.ApprovalNudged, "bo", events.ApprovalNudgedPayload{ProposalID: proposal, Layer: layer})
}

// A nudge is a thread reply for everyone and a page for the nudged layer's
// owners only. One event pages once (a redelivery does not repeat it) and a
// re-nudge 10 minutes later pages again.
func TestPagingNudge(t *testing.T) {
	created := ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})
	opts := pagingOpts()
	opts.Layers["kafka"] = []string{owner1, owner1} // a duplicate ID is mentioned once
	opts.Layers["postgres"] = []string{owner2}
	first := nudgeEvent(2, "p1", "kafka")
	st := &fakeStore{evs: []events.Event{created, first, first}} // same seq delivered twice
	fs := &fakeSlack{}
	// A tiny gap isolates the seq de-dup from the per-user-per-room gap.
	opts.Limits.UserRoomInterval = time.Nanosecond
	b, _, _ := newBridge(t, st, fs, func(o *Options) { o.Paging = opts })
	now := time.Unix(1700000000, 0)
	// Every read moves the clock on, so the 1ns gap is always over and only the
	// seq key can stop a repeated page.
	b.pager.now = func() time.Time { now = now.Add(time.Millisecond); return now }
	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = b.Run(ctx, "r1")
	}
	run()
	if n, ids := mentionCount(fs.reqs); n != 1 || len(ids) != 1 || ids[owner1] != 2 { // block text + fallback text
		t.Fatalf("after first nudge: pages=%d ids=%v, want one page for %s only", n, ids, owner1)
	}
	// A replay of the same event (a stream redelivery after the thread state
	// was lost) must not page again.
	if err := b.threads.Put(context.Background(), "r1", Thread{Channel: "CCONF", TS: "1700000000.000100", LastSeq: 1}); err != nil {
		t.Fatal(err)
	}
	run()
	if n, _ := mentionCount(fs.reqs); n != 1 {
		t.Fatalf("redelivered nudge paged again: pages=%d reqs=%d", n, fs.count())
	}
	now = now.Add(10 * time.Minute)
	st.evs = append(st.evs, nudgeEvent(3, "p1", "kafka"))
	run()
	if n, ids := mentionCount(fs.reqs); n != 2 || len(ids) != 1 {
		t.Errorf("re-nudge: pages=%d ids=%v, want 2 pages in total", n, ids)
	}
	assertOnlyTrustedMarkup(t, fs.reqs, "nudge")
}

func TestPagingNudgeUnknownLayerAndInjection(t *testing.T) {
	created := ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})
	for _, layer := range []string{"unknown", "<@U0EVIL001>", "<!channel>"} {
		fs := &fakeSlack{}
		b, _, _ := newBridge(t, &fakeStore{evs: []events.Event{created, nudgeEvent(2, "<!here>", layer)}}, fs,
			func(o *Options) { o.Paging = pagingOpts() })
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_ = b.Run(ctx, "r1")
		cancel()
		if n, _ := mentionCount(fs.reqs); n != 0 || fs.count() != 2 {
			t.Errorf("%q: requests=%d pages=%d", layer, fs.count(), n)
		}
		assertOnlyTrustedMarkup(t, fs.reqs, layer)
	}
}

func TestNudgeWithoutPagingConfigIsReplyOnly(t *testing.T) {
	created := ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: []events.Event{created, nudgeEvent(2, "p1", "kafka")}}, fs, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = b.Run(ctx, "r1")
	if n, _ := mentionCount(fs.reqs); n != 0 || fs.count() != 2 {
		t.Errorf("requests=%d pages=%d", fs.count(), n)
	}
}

// Nudges skip the per-room gap (the server limits them per proposal and layer)
// and have their own hourly cap, so a nudge 5 minutes after the proposal page
// still pages, and nudges never use up the budget for proposal pages.
func TestPagingNudgeIntervals(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{})
	if !pageOnce(p, "r1", "proposal:p1") {
		t.Fatal("proposal page denied")
	}
	now = now.Add(5 * time.Minute)
	if !pageOnce(p, "r1", "nudge:2") {
		t.Error("nudge 5m after the proposal page paged nobody")
	}
	// A re-nudge whose page was stamped late is still allowed at 10m of event time.
	now = now.Add(9*time.Minute + 58*time.Second)
	if !pageOnce(p, "r1", "nudge:3") {
		t.Error("re-nudge after lag paged nobody")
	}
}

// Nudge pages cap at UserPerHour per user and leave the other buckets alone.
func TestPagingNudgesDoNotStarveOtherPages(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{})
	for i := 0; i < 6; i++ {
		// One room each, so only the per-owner cap can deny the seventh.
		if !pageOnce(p, fmt.Sprintf("r%d", i+1), fmt.Sprintf("nudge:%d", i)) {
			t.Fatalf("nudge %d denied", i)
		}
		now = now.Add(5 * time.Minute)
	}
	if pageOnce(p, "r7", "nudge:99") {
		t.Error("a seventh nudge page in the hour was allowed")
	}
	if !pageOnce(p, "r2", "proposal:p9") {
		t.Error("nudges used up the owner's budget for a proposal page in another room")
	}
}

// One room's nudges must not use up an owner's nudge budget for other rooms
// (#509), and a burst of nudges in one room in the same second is capped too.
func TestPagingNudgesCappedPerRoom(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{})
	paged := 0
	for i := 0; i < 6; i++ {
		if pageOnce(p, "r1", fmt.Sprintf("nudge:%d", i)) {
			paged++
		}
		now = now.Add(5 * time.Minute)
	}
	if paged != 2 {
		t.Errorf("room r1 nudge pages in the hour = %d, want 2", paged)
	}
	if !pageOnce(p, "r2", "nudge:1") {
		t.Error("a nudge in r2 paged nobody after r1's nudges")
	}
	// The per-room cap frees up once the hour passes.
	now = now.Add(time.Hour)
	if !pageOnce(p, "r1", "nudge:100") {
		t.Error("r1 nudge denied after its hour passed")
	}
}

// Several nudges in one message count against the same per-room cap.
func TestPagingNudgeBurstInOneMessage(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{})
	var planned []pageKey
	for i := 0; i < 6; i++ {
		k := pageKey{room: "r1", user: owner1, reason: fmt.Sprintf("nudge:p%d", i)}
		if p.check(k, planned) {
			planned = append(planned, k)
		}
	}
	if len(planned) != 2 {
		t.Errorf("burst planned %d nudge pages, want 2", len(planned))
	}
}

// Many rooms cannot grow the per-room nudge map without bound.
func TestPagingNudgeRoomMapBounded(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{UserPerHour: 1 << 30})
	for i := 0; i < maxTrackedRooms+10; i++ {
		pageOnce(p, fmt.Sprintf("room%d", i), "nudge:1")
	}
	p.mu.Lock()
	n := len(p.nudgeRoom)
	p.mu.Unlock()
	if n > maxTrackedRooms {
		t.Errorf("nudgeRoom has %d entries, cap %d", n, maxTrackedRooms)
	}
	now = now.Add(2 * time.Hour)
	pageOnce(p, "late", "nudge:1")
	p.mu.Lock()
	n = len(p.nudgeRoom)
	p.mu.Unlock()
	if n != 1 {
		t.Errorf("nudgeRoom has %d entries after an idle hour, want 1", n)
	}
}

// #541: two nudges in each of r1-r3 must leave a page for a new room r4, while
// repeat nudges and a flood across new rooms stay capped.
func TestPagingNudgesKeepPageForNewRoom(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{})
	paged := 0
	for _, room := range []string{"r1", "r2", "r3"} {
		for i := 0; i < 2; i++ {
			if pageOnce(p, room, fmt.Sprintf("nudge:%s-%d", room, i)) {
				paged++
			}
			now = now.Add(time.Minute)
		}
	}
	if paged != 5 {
		t.Errorf("r1-r3 paged %d, want 5 (the last slot is kept for a new room)", paged)
	}
	if !pageOnce(p, "r4", "nudge:r4") {
		t.Error("nudges in r1-r3 starve r4")
	}
	if pageOnce(p, "r5", "nudge:r5") {
		t.Error("a seventh page in the hour was allowed")
	}
}

// #541: a nudge that is not paged says so in the thread reply.
func TestNudgeReplyAdmitsUnpagedNudge(t *testing.T) {
	evs := []events.Event{ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})}
	for i := int64(0); i < 3; i++ {
		evs = append(evs, nudgeEvent(2+i, fmt.Sprintf("p%d", i), "kafka"))
	}
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: evs}, fs, func(o *Options) { o.Paging = pagingOpts() })
	now := time.Unix(1700000000, 0)
	b.pager.now = func() time.Time { return now }
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = b.Run(ctx, "r1")
	const note = "owner not paged: hourly nudge limit reached"
	got := 0
	for _, r := range fs.reqs {
		if strings.Contains(fmt.Sprint(r), note) {
			got++
		}
	}
	if got != 1 {
		t.Errorf("replies with the not-paged note = %d, want 1 (third nudge in the room)", got)
	}
}
