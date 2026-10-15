package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/events"
)

// postEvent posts body and returns the status and the decoded event (zero on
// an error answer).
func postEvent(t *testing.T, url, user, body string) (int, events.Event) {
	t.Helper()
	resp := doJSON(t, http.MethodPost, url, user, body)
	defer resp.Body.Close()
	var ev events.Event
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&ev); err != nil {
			t.Fatalf("decode event: %v", err)
		}
	}
	return resp.StatusCode, ev
}

func clientIDRoom(t *testing.T) (base string, roomID string, count func() int) {
	t.Helper()
	srv, svc := newServer(t)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(context.Background(), arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	count = func() int {
		evs, _, err := svc.Events(context.Background(), snap.ID, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range evs {
			switch e.Type {
			case events.CommentAdded, events.SteerSent, events.SteerSuggested:
				n++
			}
		}
		return n
	}
	return srv.URL + "/api/rooms/" + snap.ID, snap.ID, count
}

func payloadClientID(t *testing.T, e events.Event) string {
	t.Helper()
	var p struct {
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		t.Fatal(err)
	}
	return p.ClientID
}

func TestClientIDInvalidIs400(t *testing.T) {
	base, _, count := clientIDRoom(t)
	for _, id := range []string{`""`, `"a b"`, `"` + strings.Repeat("a", 65) + `"`, `"é"`, `"a.b"`, `"a/b"`, `"a\u0000"`, `5`, `true`, `{}`} {
		for _, route := range []string{"/comments", "/steer"} {
			wantHTTPError(t, doJSON(t, http.MethodPost, base+route, "priya", `{"text":"hi","client_id":`+id+`}`), http.StatusBadRequest, "validation")
		}
	}
	if n := count(); n != 0 {
		t.Fatalf("events appended: %d, want 0", n)
	}
	// 64 characters of the whole alphabet is a valid key; null is no key.
	ok := strings.Repeat("aZ0_-", 12) + "abcd"
	if code, _ := postEvent(t, base+"/comments", "priya", `{"text":"hi","client_id":"`+ok+`"}`); code != http.StatusOK {
		t.Fatalf("64-char key: %d", code)
	}
	if code, ev := postEvent(t, base+"/comments", "priya", `{"text":"hi","client_id":null}`); code != http.StatusOK || payloadClientID(t, ev) != "" {
		t.Fatalf("null key: %d %s", code, ev.Payload)
	}
}

func TestClientIDRepeatAndReuse(t *testing.T) {
	base, _, count := clientIDRoom(t)
	code, first := postEvent(t, base+"/comments", "priya", `{"text":"hi @agent","client_id":"k1"}`)
	if code != http.StatusOK || first.Type != events.CommentAdded || payloadClientID(t, first) != "k1" {
		t.Fatalf("first: %d %+v", code, first)
	}
	// The repeat answers the stored event; "" and absent step_id are the same.
	code, again := postEvent(t, base+"/comments", "priya", `{"text":"hi @agent","step_id":"","client_id":"k1"}`)
	if code != http.StatusOK || again.Seq != first.Seq || again.Type != first.Type || payloadClientID(t, again) != "k1" {
		t.Fatalf("repeat: %d seq %d, want 200 seq %d", code, again.Seq, first.Seq)
	}
	// Another body, another step, another route: 409 client_id_reused.
	for _, c := range []struct{ route, body string }{
		{"/comments", `{"text":"hi @agent ","client_id":"k1"}`},
		{"/comments", `{"text":"hi @agent","step_id":"s_1","client_id":"k1"}`},
		{"/steer", `{"text":"hi @agent","client_id":"k1"}`},
	} {
		wantHTTPError(t, doJSON(t, http.MethodPost, base+c.route, "priya", c.body), http.StatusConflict, "client_id_reused")
	}
	// A used key answers before the route's checks: an empty text that would
	// be 400 is client_id_reused.
	wantHTTPError(t, doJSON(t, http.MethodPost, base+"/comments", "priya", `{"text":"","client_id":"k1"}`), http.StatusConflict, "client_id_reused")
	// Another user's same value is another key.
	if code, ev := postEvent(t, base+"/comments", "arjun", `{"text":"hi @agent","client_id":"k1"}`); code != http.StatusOK || ev.Seq == first.Seq {
		t.Fatalf("other user: %d seq %d", code, ev.Seq)
	}
	// Keys are case-sensitive.
	if code, ev := postEvent(t, base+"/comments", "priya", `{"text":"hi @agent","client_id":"K1"}`); code != http.StatusOK || ev.Seq == first.Seq {
		t.Fatalf("K1: %d seq %d", code, ev.Seq)
	}
	if n := count(); n != 3 {
		t.Fatalf("events: %d, want 3", n)
	}
}

func TestClientIDSteerRepeatAfterClose(t *testing.T) {
	base, _, count := clientIDRoom(t)
	// arjun drives: steer.sent; priya: steer.suggested.
	code, sent := postEvent(t, base+"/steer", "arjun", `{"text":"look","client_id":"s1"}`)
	if code != http.StatusOK || sent.Type != events.SteerSent || payloadClientID(t, sent) != "s1" {
		t.Fatalf("sent: %d %+v", code, sent)
	}
	code, sugg := postEvent(t, base+"/steer", "priya", `{"text":"look","client_id":"s1"}`)
	if code != http.StatusOK || sugg.Type != events.SteerSuggested || payloadClientID(t, sugg) != "s1" {
		t.Fatalf("suggested: %d %+v", code, sugg)
	}
	if resp := doJSON(t, http.MethodPost, base+"/close", "arjun", `{"resolution":"done"}`); resp.StatusCode != http.StatusOK {
		t.Fatalf("close: %d", resp.StatusCode)
	}
	// After close the retries still answer 200 with the stored events.
	if code, ev := postEvent(t, base+"/steer", "arjun", `{"text":"look","client_id":"s1"}`); code != http.StatusOK || ev.Seq != sent.Seq || ev.Type != events.SteerSent {
		t.Fatalf("sent retry: %d %+v", code, ev)
	}
	if code, ev := postEvent(t, base+"/steer", "priya", `{"text":"look","client_id":"s1"}`); code != http.StatusOK || ev.Seq != sugg.Seq {
		t.Fatalf("suggested retry: %d %+v", code, ev)
	}
	// A new key on the closed room gets the route's answer.
	wantHTTPError(t, doJSON(t, http.MethodPost, base+"/steer", "priya", `{"text":"look","client_id":"s2"}`), http.StatusConflict, "room_closed")
	if n := count(); n != 2 {
		t.Fatalf("events: %d, want 2", n)
	}
}

func TestClientIDConcurrentDuplicates(t *testing.T) {
	base, _, count := clientIDRoom(t)
	for _, c := range []struct{ user, route, key string }{
		{"priya", "/comments", "c"}, {"priya", "/steer", "g"}, {"arjun", "/steer", "d"},
	} {
		const n = 16
		var wg sync.WaitGroup
		seqs := make([]int64, n)
		codes := make([]int, n)
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp := doJSON(t, http.MethodPost, base+c.route, c.user, `{"text":"same","client_id":"`+c.key+`"}`)
				defer resp.Body.Close()
				codes[i] = resp.StatusCode
				var ev events.Event
				_ = json.NewDecoder(resp.Body).Decode(&ev)
				seqs[i] = ev.Seq
			}()
		}
		wg.Wait()
		for i := range n {
			if codes[i] != http.StatusOK || seqs[i] != seqs[0] {
				t.Fatalf("%s %s: request %d answered %d seq %d, want 200 seq %d", c.user, c.route, i, codes[i], seqs[i], seqs[0])
			}
		}
	}
	if got := count(); got != 3 {
		t.Fatalf("events: %d, want 3", got)
	}
}

func TestNoClientIDStillAppendsEveryTime(t *testing.T) {
	base, _, count := clientIDRoom(t)
	for range 2 {
		for _, route := range []string{"/comments", "/steer"} {
			code, ev := postEvent(t, base+route, "priya", `{"text":"same"}`)
			if code != http.StatusOK || payloadClientID(t, ev) != "" || strings.Contains(string(ev.Payload), "client_id") {
				t.Fatalf("%s: %d %s", route, code, ev.Payload)
			}
		}
	}
	if n := count(); n != 4 {
		t.Fatalf("events: %d, want 4", n)
	}
}
