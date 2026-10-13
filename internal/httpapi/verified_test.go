package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/testenv"
)

// Display-name impersonation (security audit 2026-10 A-ID-2, design section
// 2.1): every person carries verified, an IdP name that renders like a
// users.yaml person is shown with the email (or id) appended, and decisions
// carry the email. Look-alike letters are escapes so a reader sees them.

// comment posts a comment and returns its event.
func comment(t *testing.T, e *oidcEnv, roomID string, c *http.Cookie) events.Event {
	t.Helper()
	return event(t, e.do(t, http.MethodPost, "/api/rooms/"+roomID+"/comments", `{"text":"hello"}`, ck(c)))
}

func TestVerified_LookalikeIdPNamesAreSuffixed(t *testing.T) {
	sh := newLifeShared(t)
	e, _ := sh.mustStart(t, lifeConfig{mode: auth.ModeOIDC, users: lifeUsersYAML, owners: lifeOwnersYAML})
	snap := e.newRoom(t, "kabir")

	cases := []struct {
		name, resembles string
	}{
		{"Priya", "priya"},
		{"PRIYA", "priya"},
		{"pRiYa", "priya"},
		{"Pri\u0443a", "priya"},                     // Cyrillic small u (looks like y)
		{"Priy\u0430", "priya"},                     // Cyrillic small a
		{"Pr\u03b9ya", "priya"},                     // Greek small iota
		{"\uff30\uff52\uff49\uff59\uff41", "priya"}, // fullwidth
		{"\u041c\u0435\u0435r\u0430", "meera"},      // Cyrillic capital M, small e, a
		{"K\u03b1b\u0456r", "kabir"},                // Greek alpha, Cyrillic i
		{"arjun@example.com", "arjun"},              // a users.yaml email as a name
		{"\u0391gent", events.AgentActor.Name},      // Greek capital alpha: the agent
		{"Priya \u2713", "priya"},                   // a check mark to look verified
		{"Pri\u200bya", "priya"},                    // zero-width space (dropped at login)
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			email := fmt.Sprintf("member%d@example.com", i)
			u := testenv.IDPUser{Sub: fmt.Sprintf("idp-lookalike-%d", i), Email: email, EmailVerified: true, Name: c.name, Groups: []string{"dataeng-oncall"}}
			cookie := e.mustLogin(t, u)
			me := e.me(t, cookie)
			name, _ := me["name"].(string)
			if !strings.HasSuffix(name, " ("+email+")") || me["verified"] != false {
				t.Fatalf("/api/me = %v, want the name with (%s) appended, unverified", me, email)
			}
			ev := comment(t, e, snap.ID, cookie)
			if ev.Actor.Name != name || ev.Actor.Verified || !strings.HasPrefix(ev.Actor.ID, room.DerivedIDPrefix) {
				t.Fatalf("comment actor = %+v, want %q, unverified, derived id", ev.Actor, name)
			}
			if !strings.Contains(e.logs.String(), "resembles="+c.resembles) {
				t.Fatalf("the login was not logged with resembles=%s:\n%s", c.resembles, e.logs.String())
			}
		})
	}

	t.Run("no verified email: the id is appended", func(t *testing.T) {
		u := testenv.IDPUser{Sub: "idp-lookalike-noemail", Email: "priya@example.com", EmailVerified: false, Name: "Priya", Groups: []string{"dataeng-oncall"}}
		me := e.me(t, e.mustLogin(t, u))
		id, _ := me["id"].(string)
		if me["name"] != "Priya ("+id+")" || me["verified"] != false {
			t.Fatalf("/api/me = %v, want Priya (%s), unverified", me, id)
		}
	})

	t.Run("an unverified email is never the name", func(t *testing.T) {
		u := testenv.IDPUser{Sub: "idp-noname", Email: "boss@example.com", EmailVerified: false, Groups: []string{"dataeng-oncall"}}
		me := e.me(t, e.mustLogin(t, u))
		if me["name"] != me["id"] {
			t.Fatalf("/api/me = %v, want the id as the name", me)
		}
	})

	t.Run("a different name is left alone", func(t *testing.T) {
		u := testenv.IDPUser{Sub: "idp-other", Email: "on.call@example.com", EmailVerified: true, Name: "Priyanka Rao", Groups: []string{"dataeng-oncall"}}
		cookie := e.mustLogin(t, u)
		if me := e.me(t, cookie); me["name"] != "Priyanka Rao" || me["verified"] != false {
			t.Fatalf("/api/me = %v, want Priyanka Rao, unverified", me)
		}
	})

	t.Run("the users.yaml person is unaffected", func(t *testing.T) {
		// priya's IdP name differs from users.yaml and does not matter.
		u := testenv.IDPUser{Sub: "idp-priya", Email: "priya@example.com", EmailVerified: true, Name: "PRIYA (IdP)", Groups: []string{}}
		cookie := e.mustLogin(t, u)
		me := e.me(t, cookie)
		if me["id"] != "priya" || me["name"] != "Priya" || me["verified"] != true {
			t.Fatalf("/api/me = %v, want priya, Priya, verified", me)
		}
		ev := comment(t, e, snap.ID, cookie)
		if ev.Actor != (events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya", Verified: true}) {
			t.Fatalf("comment actor = %+v", ev.Actor)
		}
	})

	t.Run("/api/people", func(t *testing.T) {
		resp := e.do(t, http.MethodGet, "/api/people", "", ck(e.mustLogin(t, testenv.IDPUser{Sub: "sub-kabir-idp", Email: "kabir@example.com", EmailVerified: true, Groups: []string{}})))
		wantStatus(t, resp, http.StatusOK)
		var people []room.PersonEntry
		if err := json.NewDecoder(resp.Body).Decode(&people); err != nil {
			t.Fatal(err)
		}
		for _, p := range people {
			_, inFile := e.svc.LookupUser(p.ID)
			if p.Verified != inFile {
				t.Errorf("%+v: verified = %v, want %v", p, p.Verified, inFile)
			}
			if !inFile && auth.LooksLike(p.Name, "Priya") {
				t.Errorf("%+v passes for Priya in the people list", p)
			}
		}
	})
}

// A session made before the rule (or before a users.yaml entry with that name
// was added) is checked on every request, not only at login.
func TestVerified_NameRecheckedPerRequest(t *testing.T) {
	sh := newLifeShared(t)
	e, _ := sh.mustStart(t, lifeConfig{mode: auth.ModeOIDC, users: lifeUsersYAML, owners: lifeOwnersYAML})
	token, hash, err := auth.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	id := room.NewID("ses_")
	if err := sh.sessions.Create(context.Background(), auth.Session{
		ID: id, TokenHash: hash, UserID: "u_0123456789ab", Name: "M\u0435\u0435ra", Email: "old@example.com",
		Iss: sh.idp.Issuer, Sub: "idp-old", Groups: []string{"dataeng-oncall"}, Method: auth.MethodOIDC,
		CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	cookie := "Cookie: " + auth.OIDCSessionCookie + "=" + token
	resp := e.do(t, http.MethodGet, "/api/me", "", cookie)
	wantStatus(t, resp, http.StatusOK)
	var me map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil {
		t.Fatal(err)
	}
	if me["name"] != "M\u0435\u0435ra (old@example.com)" || me["verified"] != false {
		t.Fatalf("/api/me = %v, want the stored look-alike name with the email appended", me)
	}
	// LookupPerson (control and hand-off) shows the same.
	p, err := e.svc.LookupPerson(context.Background(), "u_0123456789ab")
	if err != nil || p.Name != "M\u0435\u0435ra (old@example.com)" || p.Verified {
		t.Fatalf("LookupPerson = %+v, %v", p, err)
	}
}

// Dev mode: users.yaml people are verified, an ad-hoc identity is not. ops-bot
// is a user: principal in owners.yaml with no users.yaml entry (a session such
// a principal holds can only come from outside the dev login, so it is seeded).
// The flag reaches events, the snapshot (driver, members, suggestion author),
// /api/me and WebSocket presence; the dev approval records the users.yaml email.
func TestVerified_DevModeAdHocNamesAreUnverified(t *testing.T) {
	sh := newLifeShared(t)
	e, _ := sh.mustStart(t, lifeConfig{mode: auth.ModeDev, users: lifeUsersYAML, owners: lifeOwnersYAML})
	seed := func(user, name string) string {
		token, hash, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		if err := sh.sessions.Create(context.Background(), auth.Session{
			ID: room.NewID("ses_"), TokenHash: hash, UserID: user, Name: name, Method: auth.MethodDev,
			CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		return token
	}
	bot := &http.Cookie{Name: auth.DevSessionCookie, Value: seed("ops-bot", "ops-bot")}
	impostor := &http.Cookie{Name: auth.DevSessionCookie, Value: seed("ops-bot", "Pr\u0456ya")}
	priya := signIn(t, e, auth.ModeDev, "priya")

	if me := e.me(t, bot); me["id"] != "ops-bot" || me["verified"] != false || me["name"] != "ops-bot" {
		t.Fatalf("ops-bot /api/me = %v, want unverified ops-bot", me)
	}
	if me := e.me(t, impostor); me["name"] != "Pr\u0456ya (ops-bot)" || me["verified"] != false {
		t.Fatalf("ad-hoc look-alike /api/me = %v, want the id appended", me)
	}
	if me := e.me(t, priya); me["verified"] != true {
		t.Fatalf("priya /api/me = %v, want verified", me)
	}

	// priya opens the room and drives; ops-bot's steer is a suggestion.
	resp := e.do(t, http.MethodPost, "/api/rooms", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`, ck(priya))
	wantStatus(t, resp, http.StatusCreated)
	var snap room.RoomSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	dialRoom(t, e, snap.ID, priya)
	conn := dialRoom(t, e, snap.ID, bot)
	presence := readUntil(t, conn, "presence with priya and ops-bot", func(m map[string]any) bool {
		ms, _ := m["members"].([]any)
		return m["type"] == "presence" && len(ms) == 2
	})
	for _, raw := range presence["members"].([]any) {
		m := raw.(map[string]any)
		if want := m["id"] == "priya"; m["verified"] != want {
			t.Fatalf("presence member %v, want verified %v", m, want)
		}
	}
	ev := event(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/steer", `{"text":"look at the slot"}`, ck(bot)))
	if ev.Type != events.SteerSuggested || ev.Actor.Verified || ev.Actor.ID != "ops-bot" {
		t.Fatalf("ops-bot steer = %s by %+v, want an unverified suggestion", ev.Type, ev.Actor)
	}
	// X-User (curl, no cookie) is a users.yaml user: verified.
	if ev := event(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/comments", `{"text":"curl"}`, "X-User: meera")); !ev.Actor.Verified {
		t.Fatalf("X-User comment actor = %+v, want verified", ev.Actor)
	}
	got, err := e.svc.GetRoom(context.Background(), snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Driver == nil || !got.Driver.Verified {
		t.Fatalf("driver = %+v, want verified priya", got.Driver)
	}
	if len(got.PendingSuggestions) != 1 || got.PendingSuggestions[0].From.Verified {
		t.Fatalf("suggestions = %+v, want one unverified from ops-bot", got.PendingSuggestions)
	}
	for _, m := range got.Members {
		_, inFile := e.svc.LookupUser(m.ID)
		if m.Verified != inFile {
			t.Errorf("member %+v: verified = %v, want %v", m, m.Verified, inFile)
		}
	}

	// A dev approval carries the users.yaml email next to the id.
	pid := e.proposal(t, snap.ID, "postgres")
	approved := event(t, e.approve(t, priya, pid, "postgres"))
	given, err := events.Decode[events.ApprovalGivenPayload](approved)
	if err != nil {
		t.Fatal(err)
	}
	if !approved.Actor.Verified || approved.Actor.ID != "priya" || given.Identity == nil || given.Identity.Email != "priya@example.com" || given.Identity.Method != auth.MethodDev {
		t.Fatalf("approval by %+v identity %+v, want verified priya with priya@example.com", approved.Actor, given.Identity)
	}
}
