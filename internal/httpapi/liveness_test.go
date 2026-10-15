package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/room"
)

// #417: in oidc mode the room's person lookup holds sessions to the rule every
// request is held to (auth.Liveness). A dev-login row, a row past the current
// PIPEROOM_SESSION_MAX and an expired row are not listed in GET /api/people,
// cannot be given control or a hand-off, and do not shadow the person's real,
// older oidc session.
func TestOIDCPeopleOnlyUsableSessions(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	priya := e.mustLogin(t, idpPriya)
	e.mustLogin(t, idpOncall)
	oncallID := derivedID(e, "idp-oncall")
	now := time.Now().UTC()

	seed := func(user, name, method string, created, expires time.Time) {
		t.Helper()
		_, hash, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		if err := e.sessions.Create(context.Background(), auth.Session{
			ID: room.NewID("ses_"), TokenHash: hash, UserID: user, Name: name, Method: method,
			Groups: []string{"dataeng-oncall"}, CreatedAt: created, LastSeenAt: now, ExpiresAt: expires,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// A dev-login row in oidc mode (left from before the switch, or written by
	// a dev server sharing the DB), for a new id and newer than oncall's real one.
	seed("u_devghost", "Dev Ghost", auth.MethodDev, now, now.Add(time.Hour))
	seed(oncallID, "Dev Ghost", auth.MethodDev, now.Add(time.Second), now.Add(time.Hour))
	// Made under a longer max: past today's 12 h, though before its own expiry.
	seed("u_overmax", "Over Max", auth.MethodOIDC, now.Add(-13*time.Hour), now.Add(time.Hour))
	seed("u_expired", "Expired", auth.MethodOIDC, now.Add(-time.Hour), now.Add(-time.Minute))
	bad := []string{"u_devghost", "u_overmax", "u_expired"}

	resp := e.do(t, http.MethodGet, "/api/people", "", ck(priya))
	wantStatus(t, resp, http.StatusOK)
	var people []room.PersonEntry
	if err := json.NewDecoder(resp.Body).Decode(&people); err != nil {
		t.Fatal(err)
	}
	byID := map[string]room.PersonEntry{}
	for _, p := range people {
		byID[p.ID] = p
	}
	for _, id := range bad {
		if _, ok := byID[id]; ok {
			t.Errorf("GET /api/people lists %s: %+v", id, people)
		}
	}
	if p, ok := byID[oncallID]; !ok || p.Name != "On Call" {
		t.Fatalf("the usable on-call member = %+v (listed %v), want On Call, not the newer dev row", p, ok)
	}

	snap := e.newRoom(t, "priya")
	for _, id := range bad {
		if _, err := e.svc.LookupPerson(context.Background(), id); err == nil {
			t.Errorf("LookupPerson(%s) accepted", id)
		}
		for _, path := range []string{"/control/grant", "/handoffs"} {
			r := e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+path, fmt.Sprintf(`{"to":%q,"stay_as_watcher":true}`, id), ck(priya))
			if r.StatusCode != http.StatusBadRequest {
				b, _ := io.ReadAll(r.Body)
				t.Errorf("%s to %s = %d %s, want 400", path, id, r.StatusCode, b)
			}
		}
	}
	got, err := e.svc.GetRoom(context.Background(), snap.ID)
	if err != nil || got.Driver == nil || got.Driver.ID != "priya" {
		t.Fatalf("driver = %+v, %v; want priya still", got.Driver, err)
	}

	u, err := e.svc.LookupPerson(context.Background(), oncallID)
	if err != nil || u.Name != "On Call" {
		t.Fatalf("LookupPerson(oncall) = %+v, %v; want the oidc session's On Call", u, err)
	}
	wantStatus(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/control/grant", fmt.Sprintf(`{"to":%q}`, oncallID), ck(priya)), http.StatusOK)
	if got, _ := e.svc.GetRoom(context.Background(), snap.ID); got.Driver == nil || got.Driver.ID != oncallID || got.Driver.Name != "On Call" {
		t.Fatalf("driver after grant = %+v", got.Driver)
	}
}
