package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/auth"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/testenv"
)

// Security audit 2026-10 (reviews/security-audit-2026-10.md). Each test is one
// attack against the login, role and approval rules of docs/design/auth.md. A
// test that passes is an attack that is defended; it stays as a regression
// test. The audit ids (A-...) are the report's.

// ---- session fixation ----

// A-SF-1 (dev). An attacker who can plant a cookie in the victim's browser
// (another app on a localhost port shares cookies with this one) plants a
// session token. When the victim signs in, the server issues a fresh token: the
// planted one never becomes the victim's session, and the planted session is
// revoked.
func TestAttack_SessionFixationDevLogin(t *testing.T) {
	e := newAuthEnv(t)
	planted := e.devLogin(t, "arjun") // the attacker's own valid session

	resp := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"priya"}`, cookieHdr(planted))
	wantStatus(t, resp, http.StatusOK)
	var fresh *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == auth.DevSessionCookie {
			fresh = c
		}
	}
	if fresh == nil || fresh.Value == "" || fresh.Value == planted.Value {
		t.Fatalf("dev-login kept the planted token: %+v", fresh)
	}
	// The planted token is dead; the fresh one is priya.
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", cookieHdr(planted)), http.StatusUnauthorized)
	me := e.do(t, http.MethodGet, "/api/me", "", cookieHdr(fresh))
	wantStatus(t, me, http.StatusOK)
	var u struct{ ID string }
	_ = json.NewDecoder(me.Body).Decode(&u)
	if u.ID != "priya" {
		t.Fatalf("me = %q", u.ID)
	}

	// A token the attacker made up (well formed, never issued) is not a session:
	// the server never adopts a client-chosen value.
	made, _, _ := auth.NewToken()
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", "Cookie: "+auth.DevSessionCookie+"="+made), http.StatusUnauthorized)
}

// A-SF-2 (oidc). The callback issues a new session token whatever cookie the
// browser carried, revokes the old session, and the cookies are __Host-
// (Secure, Path=/, no Domain), so a sibling subdomain cannot plant them.
func TestAttack_SessionFixationOIDCCallback(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	planted := e.mustLogin(t, idpOncall) // the attacker's session

	e.idp.SetUser(idpPriya)
	_, lc, authURL := e.startLogin(t, "/")
	if lc == nil || !lc.Secure || !lc.HttpOnly || lc.Path != "/" || lc.Domain != "" || !strings.HasPrefix(lc.Name, "__Host-") {
		t.Fatalf("login cookie attributes: %+v", lc)
	}
	code, state := e.idpAuthorize(t, authURL)
	q := url.Values{"code": {code}, "state": {state}}
	cb := e.do(t, http.MethodGet, "/auth/callback?"+q.Encode(), "", ck(lc), ck(planted))
	if cb.StatusCode != http.StatusFound {
		t.Fatalf("callback = %d", cb.StatusCode)
	}
	fresh := cookieNamed(cb, auth.OIDCSessionCookie)
	if fresh == nil || fresh.Value == planted.Value {
		t.Fatalf("callback kept the planted token")
	}
	if !fresh.Secure || !fresh.HttpOnly || fresh.Path != "/" || fresh.Domain != "" || fresh.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie attributes: %+v", fresh)
	}
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", ck(planted)), http.StatusUnauthorized)
	if me := e.me(t, fresh); me["id"] != "priya" {
		t.Fatalf("me = %v", me)
	}
}

// ---- login CSRF ----

// A-CSRF-1. The attacker runs the login with their own IdP account, stops
// before the callback, and makes the victim's browser open the callback URL
// with the attacker's code and state. The victim's browser has no login
// cookie (login_expired) or its own (state mismatch, and the victim's row is
// spent): either way no session for the attacker's account is set.
func TestAttack_LoginCSRFAttackerCodeInVictimBrowser(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	e.idp.SetUser(idpOncall)
	_, _, attackerURL := e.startLogin(t, "/")
	code, state := e.idpAuthorize(t, attackerURL)

	// The victim never started a login.
	resp := e.callback(t, code, state, nil)
	wantLoginError(t, resp, "login_expired")
	if cookieNamed(resp, auth.OIDCSessionCookie) != nil {
		t.Fatal("a session cookie was set")
	}

	// The victim started their own login in this browser.
	e.idp.SetUser(idpPriya)
	_, victimLC, _ := e.startLogin(t, "/")
	resp = e.callback(t, code, state, victimLC)
	wantLoginError(t, resp, "unauthenticated")
	if c := cookieNamed(resp, auth.OIDCSessionCookie); c != nil && c.Value != "" {
		t.Fatal("a session cookie was set for the attacker's account")
	}
	// The victim's row is single use: replaying with the right state now fails too.
	wantLoginError(t, e.callback(t, code, state, victimLC), "login_expired")
}

// ---- CSRF on state-changing routes ----

// A-CSRF-2. A page on another site, or on another localhost port (same-site,
// not same-origin), posts with the victim's cookie. Every such request is
// refused before the handler: no approval, no logout, no dev login. A
// browser too old to send Sec-Fetch-Site or Origin can only send a simple
// content type, which requireJSON refuses.
func TestAttack_CSRFStateChangingRoutes(t *testing.T) {
	e := newAuthEnv(t)
	victim := e.devLogin(t, "priya")
	snap, err := e.svc.CreateRoom(context.Background(), mustUserFrom(t, e.svc, "priya"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	cross := [][]string{
		{"Sec-Fetch-Site: cross-site"},
		{"Sec-Fetch-Site: same-site", "Origin: http://localhost:3000"},
		{"Origin: http://evil.test"},
		{"Origin: http://localhost:3000"},
		{"Origin: null"},
	}
	targets := []struct{ path, body string }{
		{"/api/rooms/" + snap.ID + "/comments", `{"text":"csrf"}`},
		{"/api/rooms/" + snap.ID + "/steer", `{"text":"csrf"}`},
		{"/api/rooms/" + snap.ID + "/close", `{"resolution":"csrf"}`},
		{"/api/proposals/p_x/approve", `{"layer":"postgres","params_hash":"x"}`},
		{"/auth/logout", ``},
		{"/auth/dev-login", `{"user":"arjun"}`},
	}
	for _, tg := range targets {
		for _, h := range cross {
			hdr := append([]string{cookieHdr(victim)}, h...)
			resp := e.do(t, http.MethodPost, tg.path, tg.body, hdr...)
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("POST %s with %v = %d, want 403", tg.path, h, resp.StatusCode)
			}
		}
		// No fetch metadata at all, as a form post: a simple content type.
		for _, ct := range []string{"text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x"} {
			resp := e.do(t, http.MethodPost, tg.path, tg.body, cookieHdr(victim), "Content-Type: "+ct)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("POST %s as %s = %d, want 400", tg.path, ct, resp.StatusCode)
			}
		}
	}
	// Logout is not a GET (an <img> cannot sign the victim out).
	resp := e.do(t, http.MethodGet, "/auth/logout", "", cookieHdr(victim))
	if resp.StatusCode < 400 {
		t.Errorf("GET /auth/logout = %d, want an error", resp.StatusCode)
	}
	// Nothing happened: the session is alive, the room is open with no comment.
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", cookieHdr(victim)), http.StatusOK)
	got, _ := e.svc.GetRoom(context.Background(), snap.ID)
	if got.Status != "open" {
		t.Fatalf("room status = %q", got.Status)
	}
	evs, _, _ := e.svc.Events(context.Background(), snap.ID, 0, 100)
	for _, ev := range evs {
		if ev.Type == events.CommentAdded || ev.Type == events.SteerSent || ev.Type == events.SteerSuggested {
			t.Fatalf("a cross-site request wrote %s", ev.Type)
		}
	}
}

// A-CSRF-3. Cross-site WebSocket hijacking: a page on another localhost port
// (same-site, so a Lax cookie is sent) opens the room socket. The handshake is
// refused on Origin.
func TestAttack_CrossSiteWebSocketHijack(t *testing.T) {
	e := newAuthEnv(t)
	victim := e.devLogin(t, "priya")
	snap, err := e.svc.CreateRoom(context.Background(), mustUserFrom(t, e.svc, "priya"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, origin := range []string{"http://localhost:3000", "http://evil.test", "null"} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, resp, err := websocket.Dial(ctx, e.wsURL(snap.ID), &websocket.DialOptions{HTTPHeader: http.Header{
			"Cookie": {victim.Name + "=" + victim.Value},
			"Origin": {origin},
		}})
		cancel()
		if err == nil {
			conn.Close(websocket.StatusNormalClosure, "")
			t.Fatalf("Origin %s: the WebSocket opened", origin)
		}
		if resp == nil || resp.StatusCode != http.StatusForbidden {
			t.Fatalf("Origin %s: resp %v, want 403", origin, resp)
		}
	}
}

// ---- DNS rebinding ----

// A-DNS-1. A page whose name was rebound to 127.0.0.1 sends its own name as
// Host and Sec-Fetch-Site: same-origin, which passes the CSRF check. Every
// spelling of a non-allowed Host is 421, the unsigned webhook included, and no
// room is opened.
func TestAttack_DNSRebindingHostSpellings(t *testing.T) {
	e := newAuthEnv(t)
	hosts := []string{
		"evil.test", "evil.test:8090", "localhost.evil.test", "localhost.evil.test:8090",
		"127.0.0.1.nip.io:8090", "localhost:8090.evil.test", "[::1].evil.test", "[::1]:x",
		"127.0.0.1:", "localhost:", "0.0.0.0:8090", "[::ffff:127.0.0.1]:8090", "127.1:8090",
		"2130706433:8090", "localhost.:8090", "127.0.0.1:8090:80", "127.0.0.1:123456",
		"localhost\t",
	}
	for _, h := range hosts {
		for _, rt := range []struct{ method, path, body string }{
			{http.MethodPost, "/auth/dev-login", `{"user":"priya"}`},
			{http.MethodPost, "/api/alerts/webhook", `{"title":"x","pipeline_id":"orders"}`},
			{http.MethodPost, "/api/rooms", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`},
			{http.MethodGet, "/api/rooms", ``},
		} {
			resp := e.do(t, rt.method, rt.path, rt.body, "Host: "+h, "Sec-Fetch-Site: same-origin", "X-User: priya")
			if resp.StatusCode != http.StatusMisdirectedRequest && resp.StatusCode != http.StatusBadRequest {
				t.Errorf("Host %q %s %s = %d, want 421 (or 400 from net/http)", h, rt.method, rt.path, resp.StatusCode)
			}
		}
	}
	// Path spellings that try to look like a signed alert route to skip the
	// Host check while reaching the unsigned webhook.
	for _, p := range []string{"/api/alerts/webhook/", "/api/alerts/%77ebhook", "/api/alerts//webhook", "/api/alerts/./webhook", "/api/alerts/webhook%2F"} {
		resp := e.do(t, http.MethodPost, p, `{"title":"x","pipeline_id":"orders"}`, "Host: evil.test", "Sec-Fetch-Site: same-origin", "X-User: priya")
		if resp.StatusCode < 400 {
			t.Errorf("POST %s from a rebound host = %d", p, resp.StatusCode)
		}
	}
	rooms, err := e.svc.ListRooms(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rooms) != 0 {
		t.Fatalf("a rebound page opened %d rooms", len(rooms))
	}
}

// ---- viewer write paths ----

// A-VW-1. A viewer looks for a write the method rule misses: method override
// headers, HEAD and OPTIONS, odd path spellings, an X-User header in oidc
// mode, a group claim that looks like a user principal, and messages on the
// WebSocket. None writes anything.
func TestAttack_ViewerWritePaths(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	pid := e.proposal(t, snap.ID, "postgres")
	hash := e.hash(t, pid)
	// A viewer whose groups claim also carries "user:priya" and the
	// approver group spelled with a trailing space.
	v := e.mustLogin(t, testenv.IDPUser{Sub: "idp-sneaky", Name: "Sneaky", Groups: []string{"viewers", "user:priya", "dataeng-oncall "}})
	vh := ck(v)
	if me := e.me(t, v); me["role"] != "viewer" {
		t.Fatalf("me = %v", me)
	}

	approve := fmt.Sprintf(`{"layer":"postgres","params_hash":%q}`, hash)
	try := []struct {
		method, path, body string
		hdr                []string
	}{
		{http.MethodPost, "/api/proposals/" + pid + "/approve", approve, []string{"X-HTTP-Method-Override: GET"}},
		{http.MethodPost, "/api/proposals/" + pid + "/approve", approve, []string{"X-User: priya"}},
		{http.MethodPost, "/api/proposals/" + pid + "/approve", approve, []string{"X-Method-Override: GET", "X-HTTP-Method: GET"}},
		{http.MethodHead, "/api/rooms/" + snap.ID + "/steer", "", nil},
		{http.MethodOptions, "/api/rooms/" + snap.ID + "/steer", "", nil},
		{http.MethodPost, "/api//proposals/" + pid + "/approve", approve, nil},
		{http.MethodPost, "/API/proposals/" + pid + "/approve", approve, nil},
		{http.MethodPost, "/api/proposals/" + pid + "/approve/", approve, nil},
		{http.MethodPost, "/api/rooms/" + snap.ID + "/control/request", `{}`, nil},
		{http.MethodPost, "/api/rooms/" + snap.ID + "/steer", `{"text":"drop table"}`, []string{"Sec-Fetch-Site: same-origin"}},
		{"PROPFIND", "/api/rooms/" + snap.ID, "", nil},
	}
	for _, tr := range try {
		resp := e.do(t, tr.method, tr.path, tr.body, append([]string{vh}, tr.hdr...)...)
		if resp.StatusCode < 400 {
			b, _ := io.ReadAll(resp.Body)
			t.Errorf("viewer %s %s %v = %d; body %s", tr.method, tr.path, tr.hdr, resp.StatusCode, b)
		}
	}

	// The WebSocket accepts presence only; anything else is dropped.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/api/rooms/"+snap.ID+"/ws?after=0",
		&websocket.DialOptions{HTTPHeader: http.Header{"Cookie": {v.Name + "=" + v.Value}}})
	if err != nil {
		t.Fatalf("viewer WebSocket: %v", err)
	}
	for _, m := range []string{
		`{"type":"steer","text":"drop table"}`,
		fmt.Sprintf(`{"type":"approve","proposal_id":%q,"layer":"postgres","params_hash":%q}`, pid, hash),
		`{"type":"comment","text":"x"}`,
	} {
		if err := conn.Write(ctx, websocket.MessageText, []byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	conn.Close(websocket.StatusNormalClosure, "")

	evs, _, err := e.svc.Events(context.Background(), snap.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		switch ev.Type {
		case events.ApprovalGiven, events.SteerSent, events.SteerSuggested, events.CommentAdded, events.ControlRequested:
			t.Fatalf("a viewer wrote %s", ev.Type)
		}
	}
	if e.acts["postgres"].count() != 0 {
		t.Fatal("the action ran")
	}
}

// ---- approval forgery ----

// A-AP-1. A member tries to approve what they do not own: a layer another
// person owns, a layer the proposal does not need, a proposal of another room
// with this proposal's hash, a group claim that differs only by case, space
// or a look-alike character, and the same layer twice.
func TestAttack_ApprovalForgeryOverHTTP(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	other := e.newRoom(t, "priya")
	pid := e.proposal(t, snap.ID, "postgres", "kafka-connect")
	hash := e.hash(t, pid)
	otherPID := e.proposal(t, other.ID, "postgres", "kafka-connect")
	otherHash := e.hash(t, otherPID)
	if hash == otherHash {
		t.Fatal("two proposals share a hash: the room and proposal ids are not bound")
	}

	oncall := e.mustLogin(t, idpOncall)
	post := func(c *http.Cookie, pid, layer, h string) *http.Response {
		return e.do(t, http.MethodPost, "/api/proposals/"+pid+"/approve", fmt.Sprintf(`{"layer":%q,"params_hash":%q}`, layer, h), ck(c))
	}
	// Someone else's layer.
	wantStatus(t, post(oncall, pid, "postgres", hash), http.StatusForbidden)
	// A layer this proposal does not need (warehouse is meera's; use meera).
	meera := e.mustLogin(t, testenv.IDPUser{Sub: "idp-meera", Email: "meera@example.com", EmailVerified: true})
	wantStatus(t, post(meera, pid, "warehouse", hash), http.StatusBadRequest)
	// The hash of another proposal (the other room's, which oncall may approve).
	wantStatus(t, post(oncall, pid, "kafka-connect", otherHash), http.StatusConflict)
	// Look-alike group claims.
	for _, g := range []string{"DataEng-OnCall", "dataeng-oncall ", " dataeng-oncall", "dataeng\u2011oncall", "dataeng-oncall\u200b", "group:dataeng-oncall"} {
		c := e.mustLogin(t, testenv.IDPUser{Sub: "idp-lookalike-" + url.QueryEscape(g), Groups: []string{g, "viewers"}})
		resp := post(c, pid, "kafka-connect", hash)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("group %q approved: %d", g, resp.StatusCode)
		}
	}
	// The real owner approves once; a second approval of the layer is 409.
	wantStatus(t, post(oncall, pid, "kafka-connect", hash), http.StatusOK)
	wantStatus(t, post(oncall, pid, "kafka-connect", hash), http.StatusConflict)
	if n := e.acts["kafka-connect"].count(); n != 0 {
		t.Fatalf("ran %d times with one of two layers approved", n)
	}
}

// ---- identity spoofing ----

// A-ID-1. IdP accounts that try to become a users.yaml user without being
// mapped to them: a sub equal to the users.yaml id, the right email but not
// verified, a non-ASCII look-alike email, and a name claim equal to the
// user's name. None gets the user's id; the name claim is only a label (see
// A-ID-2 in the report).
func TestAttack_IdentitySpoofOIDC(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	// Make every attempt pass the login gate as a viewer, so the id is visible.
	cases := []testenv.IDPUser{
		{Sub: "priya", Name: "Priya", Groups: []string{"viewers"}},
		{Sub: "idp-x1", Email: "priya@example.com", EmailVerified: false, Groups: []string{"viewers"}},
		{Sub: "idp-x2", Email: "pr\u0456ya@example.com", EmailVerified: true, Groups: []string{"viewers"}},
		{Sub: "idp-x3", Email: "priya@example.com\u200b", EmailVerified: true, Groups: []string{"viewers"}},
		{Sub: "idp-x4", Name: "Priya", Email: "someone@example.com", EmailVerified: true, Groups: []string{"viewers"}},
	}
	for _, u := range cases {
		c := e.mustLogin(t, u)
		me := e.me(t, c)
		if me["id"] == "priya" || me["role"] != "viewer" {
			t.Errorf("IdP user %+v became %v", u, me)
		}
		if id, _ := me["id"].(string); !strings.HasPrefix(id, room.DerivedIDPrefix) {
			t.Errorf("IdP user %+v got id %q, want a derived u_ id", u, id)
		}
	}
}

// A-ID-3 (fixed). arjun's users.yaml entry pins sub: sub-arjun. A session made
// for another IdP account before the pin existed (sessions are in Postgres and
// survive the restart that loads the new pin) must stop working, on HTTP and on
// the WebSocket presence tick, not run until the 12 h cap.
func TestAttack_SessionOutlivesSubPin(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	now := time.Now().UTC()
	seed := func(sub string) (cookie, id string) {
		token, hash, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		id = room.NewID("ses_")
		if err := e.sessions.Create(context.Background(), auth.Session{
			ID: id, TokenHash: hash, UserID: "arjun", Name: "Arjun", Method: auth.MethodOIDC,
			Iss: e.idp.Issuer, Sub: sub, CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		return "Cookie: " + auth.OIDCSessionCookie + "=" + token, id
	}
	recycled, recycledID := seed("recycled-account")
	resp := e.do(t, http.MethodGet, "/api/me", "", recycled)
	wantStatus(t, resp, http.StatusUnauthorized)
	if c, _ := errCode(t, resp); c != "session_expired" {
		t.Fatalf("code = %q", c)
	}
	// The pinned account itself is fine.
	pinned, pinnedID := seed("sub-arjun")
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", pinned), http.StatusOK)
	// The WebSocket presence tick (sessionAlive) applies the same pin: the
	// recycled account's connection is closed, the pinned one's stays open.
	if e.api.SessionAliveForTest(context.Background(), recycledID) {
		t.Error("presence tick: the recycled account's session is still alive")
	}
	if !e.api.SessionAliveForTest(context.Background(), pinnedID) {
		t.Error("presence tick: the pinned account's session was closed")
	}
	// A users.yaml user with no pin is not affected by the rule (priya binds by email).
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", ck(e.mustLogin(t, idpPriya))), http.StatusOK)
}

// A-ID-4 (fixed). A name claim with a right-to-left override or zero-width
// characters renders as another string in the room. Format characters are
// dropped from the name before it is stored.
func TestAttack_NameClaimBidiOverride(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	c := e.mustLogin(t, testenv.IDPUser{Sub: "idp-bidi", Name: "\u202eayirP\u202c\u200b", Groups: []string{"dataeng-oncall"}})
	me := e.me(t, c)
	name, _ := me["name"].(string)
	for _, r := range name {
		if r == '\u202e' || r == '\u202c' || r == '\u200b' {
			t.Fatalf("name %q keeps a format character", name)
		}
	}
	if name != "ayirP" {
		t.Fatalf("name = %q, want the visible letters only", name)
	}
}

// A-ID-5 (fixed). An admin unbinds priya's identity because the mailbox was
// recycled (design section 2.1, "Unbind"). The unbind now also revokes the
// sessions that account holds, so it stops acting as priya at once.
func TestAttack_UnbindLeavesSessionsAlive(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	old := e.mustLogin(t, idpPriya)
	if me := e.me(t, old); me["id"] != "priya" {
		t.Fatalf("me = %v", me)
	}
	// A bystander's session must survive the unbind.
	kabir := e.mustLogin(t, testenv.IDPUser{Sub: "sub-kabir"})
	ctx := context.Background()
	sessionID := func(cookie *http.Cookie) string {
		t.Helper()
		hash, ok := auth.HashToken(cookie.Value)
		if !ok {
			t.Fatal("session cookie has invalid token shape")
		}
		sess, err := e.sessions.ByTokenHash(ctx, hash)
		if err != nil {
			t.Fatalf("load session for %s: %v", cookie.Name, err)
		}
		return sess.ID
	}
	oldSessionID, kabirSessionID := sessionID(old), sessionID(kabir)
	if !e.api.SessionAliveForTest(ctx, oldSessionID) {
		t.Fatal("old session is not alive before unbind")
	}
	if !e.api.SessionAliveForTest(ctx, kabirSessionID) {
		t.Fatal("bystander session is not alive before unbind")
	}
	r, err := e.idents.Unbind(context.Background(), "priya", time.Now())
	if err != nil || r.Unbound != 1 || r.Revoked != 1 {
		t.Fatalf("unbind = %+v, %v; want 1 binding and 1 session", r, err)
	}
	if e.api.SessionAliveForTest(ctx, oldSessionID) {
		t.Error("WebSocket presence check kept the unbound user's session alive")
	}
	if !e.api.SessionAliveForTest(ctx, kabirSessionID) {
		t.Error("WebSocket presence check closed a bystander's session")
	}
	wantStatus(t, e.do(t, http.MethodGet, "/api/me", "", ck(old)), http.StatusUnauthorized)
	if me := e.me(t, kabir); me["id"] != "kabir" {
		t.Fatalf("bystander session = %v", me)
	}
}

// ---- export ----

// A-EX-1. Who may export, and what the file holds: a plain member who drives
// the room cannot export, a viewer cannot, an auditor can; the file holds no
// session token, token hash or IdP group.
//
// Emails (A-ID-2, accepted by LEAD-ARCH): the file, and the events a viewer
// reads, hold exactly two kinds of email, each a person's own: (a) an
// approver's, in identity.email of their own decision, and (b) a member's,
// in their own actor name when the server suffixed a look-alike name. Never
// a third party's (the auditor's, the driver's, anyone who only logged in).
func TestAttack_ExportAccessAndContent(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	priya := e.mustLogin(t, idpPriya)
	pid := e.proposal(t, snap.ID, "postgres")
	wantStatus(t, e.approve(t, priya, pid, "postgres"), http.StatusOK)
	waitFor(t, func() bool { return e.acts["postgres"].count() == 1 }, "the step to run")
	// A group member whose IdP name is "Priya" comments: shown with their own email.
	lookalike := e.mustLogin(t, testenv.IDPUser{Sub: "idp-lookalike", Email: "lookalike@example.com", EmailVerified: true, Name: "Priya", Groups: []string{"dataeng-oncall"}})
	wantStatus(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/comments", `{"text":"hi"}`, ck(lookalike)), http.StatusOK)

	// ksenia is in users.yaml with no layer: a plain member. Make her the driver.
	ksenia := e.mustLogin(t, testenv.IDPUser{Sub: "idp-ksenia", Email: "ksenia@example.com", EmailVerified: true})
	wantStatus(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/control/grant", `{"to":"ksenia"}`, ck(priya)), http.StatusOK)
	got, _ := e.svc.GetRoom(context.Background(), snap.ID)
	if got.Driver == nil || got.Driver.ID != "ksenia" {
		t.Fatalf("driver = %+v", got.Driver)
	}
	viewer := e.mustLogin(t, idpViewer)
	for name, c := range map[string]*http.Cookie{"driver (plain member)": ksenia, "viewer": viewer} {
		resp := e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/export", "", ck(c))
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s export = %d, want 403", name, resp.StatusCode)
		}
		// An unknown room is no different for them from a forbidden one
		// unless they could export it: no 200 either way.
		if r := e.do(t, http.MethodGet, "/api/rooms/r_nope/export", "", ck(c)); r.StatusCode == http.StatusOK {
			t.Errorf("%s export of an unknown room = 200", name)
		}
	}

	auditor := e.mustLogin(t, idpAuditor)
	resp := e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/export", "", ck(auditor))
	wantStatus(t, resp, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	doc := string(body)
	sessCookie := priya.Value
	for _, leak := range []string{sessCookie, "audit@example.com", "ksenia@example.com", "token_hash", "security", "dataeng-oncall", e.idp.ClientSecret} {
		if leak != "" && strings.Contains(doc, leak) {
			t.Errorf("the export contains %q", leak)
		}
	}
	// (a) the approver's own email, with their decision: once in the
	// approval.given event and once in approvals[].identity.
	if n := strings.Count(doc, `"email":"priya@example.com"`); n != 2 {
		t.Errorf("the approver's email appears %d times, want 2 (the event and approvals[].identity)", n)
	}
	// (b) the suffixed member's own email, in their own actor name only.
	if n, m := strings.Count(doc, "lookalike@example.com"), strings.Count(doc, `"name":"Priya (lookalike@example.com)"`); n != 1 || m != 1 {
		t.Errorf("the suffixed member's email appears %d times (%d as their name), want once, as their name", n, m)
	}
	// Nothing else: the same holds for what a viewer reads.
	want := map[string]bool{"priya@example.com": true, "lookalike@example.com": true}
	if got := emailsIn(doc); !maps.Equal(got, want) {
		t.Errorf("emails in the export = %v, want only %v", got, want)
	}
	vresp := e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/events?after=0", "", ck(viewer))
	wantStatus(t, vresp, http.StatusOK)
	vbody, _ := io.ReadAll(vresp.Body)
	if got := emailsIn(string(vbody)); !maps.Equal(got, want) {
		t.Errorf("emails a viewer reads = %v, want only %v", got, want)
	}
	if !strings.Contains(doc, `"params_hash"`) {
		t.Error("the export has no params_hash")
	}
}

var emailRE = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]+`)

// emailsIn is the set of email addresses in s.
func emailsIn(s string) map[string]bool {
	out := map[string]bool{}
	for _, m := range emailRE.FindAllString(s, -1) {
		out[m] = true
	}
	return out
}

// A-EX-2 (design, documented). The export is limited to approvers and
// auditors, but a viewer reads the same events through GET .../events: the
// export rule protects the signed file, not the content. This test pins the
// current behaviour; see the report.
func TestAttack_ViewerReadsWhatExportWouldShow(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	e.proposal(t, snap.ID, "postgres")
	v := e.mustLogin(t, idpViewer)
	resp := e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/events?after=0", "", ck(v))
	wantStatus(t, resp, http.StatusOK)
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "proposal.created") {
		t.Fatalf("viewer events = %s", b)
	}
}

// ---- rate-limit lockout ----

// loginFrom sends GET /auth/login straight to the handler with a chosen peer
// address (httptest's own peer is always 127.0.0.1).
func loginFrom(e *oidcEnv, peer string, hdr ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/auth/login", nil)
	r.Host = "localhost"
	r.RemoteAddr = peer
	for _, h := range hdr {
		k, v, _ := strings.Cut(h, ": ")
		r.Header.Add(k, v)
	}
	w := httptest.NewRecorder()
	e.api.ServeHTTP(w, r)
	return w
}

func limited(w *httptest.ResponseRecorder) bool {
	return w.Code == http.StatusFound && strings.Contains(w.Header().Get("Location"), "login_error=rate_limited")
}

func burn(t *testing.T, e *oidcEnv, peer string, hdr ...string) {
	t.Helper()
	for i := 0; i < 320; i++ {
		loginFrom(e, peer, hdr...)
	}
}

// A-RL-1. Without trusted proxies, an attacker who names the victim's address
// in X-Forwarded-For spends only their own budget.
func TestAttack_LoginLimitSpoofedXFFNoTrustedProxy(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	burn(t, e, "203.0.113.9:4000", "X-Forwarded-For: 198.51.100.7")
	if !limited(loginFrom(e, "203.0.113.9:4001")) {
		t.Fatal("the attacker is not limited")
	}
	if w := loginFrom(e, "198.51.100.7:5000"); limited(w) || w.Code != http.StatusFound {
		t.Fatalf("the victim is locked out: %d %s", w.Code, w.Header().Get("Location"))
	}
}

// A-RL-2. Behind a trusted proxy that appends the peer address, an attacker
// who prepends the victim's address, or a fake proxy hop, still spends only
// their own budget; an IPv6 attacker in another /64 cannot reach the victim's.
func TestAttack_LoginLimitSpoofedXFFBehindTrustedProxy(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{trusted: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/24")}})
	proxy := "10.0.0.1:443"
	burn(t, e, proxy, "X-Forwarded-For: 198.51.100.7, 203.0.113.9")
	burn(t, e, proxy, "X-Forwarded-For: 198.51.100.7, 10.0.0.2, 203.0.113.9")
	burn(t, e, proxy, "X-Forwarded-For: 198.51.100.7", "X-Forwarded-For: 203.0.113.9")
	if !limited(loginFrom(e, proxy, "X-Forwarded-For: 203.0.113.9")) {
		t.Fatal("the attacker is not limited")
	}
	if w := loginFrom(e, proxy, "X-Forwarded-For: 198.51.100.7"); limited(w) {
		t.Fatal("the victim is locked out")
	}

	burn(t, e, proxy, "X-Forwarded-For: 2001:db8:1:2::66")
	if w := loginFrom(e, proxy, "X-Forwarded-For: 2001:db8:1:3::7"); limited(w) {
		t.Fatal("an IPv6 attacker locked out another /64")
	}
}

// A-RL-3. Flooding the table with new keys evicts the oldest bucket instead of
// refusing new people: a fresh client is never limited by other clients.
func TestAttack_LoginLimitKeyFlood(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	for i := 0; i < 2000; i++ {
		loginFrom(e, fmt.Sprintf("198.18.%d.%d:1", i/250, i%250+1))
	}
	if w := loginFrom(e, "192.0.2.77:1"); limited(w) || w.Code != http.StatusFound {
		t.Fatalf("a new client is limited after a key flood: %d %s", w.Code, w.Header().Get("Location"))
	}
}

// A-RL-4 (design, documented). Two limits of the design, pinned so a change is
// noticed: (a) behind a proxy with PIPEROOM_TRUSTED_PROXIES unset, every user
// shares the proxy's key, so one client can spend everyone's budget; (b) a
// peer inside a trusted range picks its own key with X-Forwarded-For, so it
// can spend a named victim's budget. Both only stop new logins (sessions keep
// working) for at most a minute after the flood ends. See the report.
func TestAttack_LoginLimitDesignLimits(t *testing.T) {
	t.Run("shared proxy key without trusted proxies", func(t *testing.T) {
		e := newOIDCEnv(t, oidcOpts{})
		burn(t, e, "10.0.0.1:443", "X-Forwarded-For: 203.0.113.9")
		if !limited(loginFrom(e, "10.0.0.1:443", "X-Forwarded-For: 198.51.100.7")) {
			t.Fatal("expected the shared key to be limited (design limit changed?)")
		}
	})
	t.Run("a trusted peer chooses its key", func(t *testing.T) {
		e := newOIDCEnv(t, oidcOpts{trusted: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}})
		burn(t, e, "10.9.9.9:1", "X-Forwarded-For: 198.51.100.7") // an internal host, not the proxy
		if !limited(loginFrom(e, "10.0.0.1:443", "X-Forwarded-For: 198.51.100.7")) {
			t.Fatal("expected the victim's key to be spent (design limit changed?)")
		}
	})
}

// A-RL-5. The login-state table under a flood from many addresses: the
// victim's own row is replaced only by the victim's next login, never by
// other people's, so the victim still completes the login.
func TestAttack_LoginStateFloodDoesNotEvictVictim(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	e.idp.SetUser(idpPriya)
	_, lc, authURL := e.startLogin(t, "/")
	for i := 0; i < 500; i++ {
		loginFrom(e, fmt.Sprintf("198.18.1.%d:1", i%250+1), "Cookie: "+auth.OIDCLoginCookie+"=attacker-"+fmt.Sprint(i))
	}
	code, state := e.idpAuthorize(t, authURL)
	cb := e.callback(t, code, state, lc)
	if cb.StatusCode != http.StatusFound || cookieNamed(cb, auth.OIDCSessionCookie) == nil {
		t.Fatalf("victim login after a flood: %d %s", cb.StatusCode, cb.Header.Get("Location"))
	}
}
