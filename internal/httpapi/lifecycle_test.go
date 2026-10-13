package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/testenv"
)

// The identity and approval lifecycle as an enterprise control (task T3),
// end to end through the real HTTP handlers, room service and in-memory
// stores: a users.yaml layer over a group-owned layer (security audit
// A-AP-3), identity unbind (A-ID-5), a role change or a revoked session in
// the middle of a room, the old driver after a hand-off, decided proposals and
// the params_hash binding, and a group member's approval.
//
// Each server here is one process. A restart is a new room service and HTTP
// server, built from new users.yaml and owners.yaml text, over the same event,
// session and identities stores: what Postgres keeps across a real restart.

const lifeUsersYAML = `
users:
  - id: priya
    name: Priya
    email: priya@example.com
    layers: [postgres]
  - id: arjun
    name: Arjun
    email: arjun@example.com
    layers: [kafka]
  - id: meera
    name: Meera
    email: meera@example.com
    layers: [warehouse]
  - id: kabir
    name: Kabir
    email: kabir@example.com
    layers: [consumer]
`

// kafka-connect is owned by a group and by ops-bot, a user: principal with no
// users.yaml entry (an ad-hoc dev identity); consumer by kabir and a group.
const lifeOwnersYAML = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
layers:
  postgres: priya
  kafka: arjun
  warehouse: meera
  consumer: [user:kabir, group:consumers]
  kafka-connect: [group:dataeng-oncall, user:ops-bot]
auditors: [group:security]
`

var lifeModes = []string{auth.ModeDev, auth.ModeOIDC}

// lifeStore is the shared event store. edit, when set, rewrites every proposal
// row it hands out: a scripted edit of the proposals table between an approval
// and the run.
type lifeStore struct {
	*memstore.Store
	edit atomic.Pointer[func(store.ProposalRow) store.ProposalRow]
}

func (s *lifeStore) GetProposal(ctx context.Context, id string) (store.ProposalRow, error) {
	p, err := s.Store.GetProposal(ctx, id)
	if f := s.edit.Load(); err == nil && f != nil {
		p = (*f)(p)
	}
	return p, err
}

// tableAction is a postgres action with one parameter, so two proposals can
// differ in their params alone.
type tableAction struct{ *testAction }

func (a tableAction) Name() string { return "set_table" }
func (a tableAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false,
		"properties": map[string]any{"table": map[string]any{"type": "string"}}}
}

// lifeShared is what survives a restart.
type lifeShared struct {
	st       *lifeStore
	sessions *auth.MemStore
	idents   *auth.MemIdentities
	states   *auth.MemLoginStates
	idp      *testenv.FakeIdP
	reg      *actions.Registry
	acts     map[string]*testAction
	table    tableAction
}

func newLifeShared(t *testing.T) *lifeShared {
	t.Helper()
	reg := actions.NewRegistry()
	acts := map[string]*testAction{}
	for _, l := range events.Layers {
		a := &testAction{layer: l}
		acts[l] = a
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	table := tableAction{&testAction{layer: "postgres"}}
	if err := reg.Register(table); err != nil {
		t.Fatal(err)
	}
	sessions := auth.NewMemStore()
	return &lifeShared{
		st: &lifeStore{Store: memstore.New()}, sessions: sessions,
		idents: auth.NewMemIdentities().WithSessions(sessions), states: auth.NewMemLoginStates(),
		idp: testenv.NewFakeIdP(t), reg: reg, acts: acts, table: table,
	}
}

type lifeConfig struct {
	mode   string // auth.ModeDev or auth.ModeOIDC
	users  string // users.yaml text
	owners string // owners.yaml text
	// unchecked skips the users/owners start check, as for a config that
	// reached the service some other way; the per-request rules must hold.
	unchecked bool
}

// start does what cmd/server does before it serves (parse users.yaml, load
// owners.yaml, the users/owners start check, the room service and Load, the
// HTTP server for the mode). An error is a refused start. The returned stop
// ends this "process"; the test cleanup calls it too.
func (sh *lifeShared) start(t *testing.T, c lifeConfig) (*oidcEnv, func(), error) {
	t.Helper()
	users, err := room.ParseUsers([]byte(c.users))
	if err != nil {
		return nil, nil, err
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(c.owners), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		return nil, nil, err
	}
	if !c.unchecked {
		if err := room.ValidateUsers(users, own); err != nil {
			return nil, nil, err
		}
	}
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	svc := room.New(room.Deps{
		Store: sh.st, Users: users, Owners: own, Actions: sh.reg, Logger: log,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
		Sessions: sh.sessions, SessionIdle: 8 * time.Hour, ViewerGroups: []string{"viewers"},
	})
	if err := svc.Load(context.Background()); err != nil {
		svc.Close()
		t.Fatalf("load: %v", err)
	}
	settings := auth.Default()
	settings.Mode = c.mode
	settings.ViewerGroups = []string{"viewers"}
	opts := []httpapi.Option{httpapi.WithAuth(settings), httpapi.WithSessions(sh.sessions),
		httpapi.WithLoginStates(sh.states), httpapi.WithIdentities(sh.idents)}
	var rp *auth.OIDC
	if c.mode == auth.ModeOIDC {
		rp, err = auth.NewOIDC(auth.OIDCConfig{
			Issuer: sh.idp.Issuer, ClientID: sh.idp.ClientID, ClientSecret: sh.idp.ClientSecret,
			RedirectURL: "https://piperoom.test/auth/callback", Scopes: []string{"openid", "email", "profile"},
			GroupsClaim: "groups", HTTPClient: &http.Client{Timeout: 2 * time.Second},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := rp.Discover(context.Background()); err != nil {
			t.Fatalf("discovery: %v", err)
		}
		opts = append(opts, httpapi.WithOIDC(rp), httpapi.WithPublicOrigin(httpapi.PublicOrigin{Scheme: "https", Host: "piperoom.test"}))
	}
	api := httpapi.New(svc, sh.st, log, opts...)
	srv := httptest.NewServer(api)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			srv.Close()
			api.Close()
			svc.Close()
		})
	}
	t.Cleanup(stop)
	return &oidcEnv{
		idp: sh.idp, rp: rp, srv: srv, api: api, svc: svc, own: own, sessions: sh.sessions, states: sh.states,
		idents: sh.idents, logs: logs, acts: sh.acts,
		client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, stop, nil
}

func (sh *lifeShared) mustStart(t *testing.T, c lifeConfig) (*oidcEnv, func()) {
	t.Helper()
	e, stop, err := sh.start(t, c)
	if err != nil {
		t.Fatalf("start (%s): %v", c.mode, err)
	}
	return e, stop
}

// signIn signs a users.yaml user in and returns the session cookie: the dev
// login in dev mode; in oidc mode an IdP account whose verified email is the
// user's, so the first login binds it.
func signIn(t *testing.T, e *oidcEnv, mode, id string) *http.Cookie {
	t.Helper()
	if mode == auth.ModeDev {
		resp := e.do(t, http.MethodPost, "/auth/dev-login", `{"user":"`+id+`"}`)
		wantStatus(t, resp, http.StatusOK)
		c := cookieNamed(resp, auth.DevSessionCookie)
		if c == nil {
			t.Fatal("dev login set no cookie")
		}
		return c
	}
	return e.mustLogin(t, testenv.IDPUser{Sub: "idp-" + id, Email: id + "@example.com", EmailVerified: true, Groups: []string{}})
}

// sessionID is the public id of the session a cookie holds.
func sessionID(t *testing.T, e *oidcEnv, c *http.Cookie) string {
	t.Helper()
	hash, ok := auth.HashToken(c.Value)
	if !ok {
		t.Fatal("bad cookie value")
	}
	sess, err := e.sessions.ByTokenHash(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	return sess.ID
}

// event decodes a 200 response that carries one events.Event.
func event(t *testing.T, resp *http.Response) events.Event {
	t.Helper()
	wantStatus(t, resp, http.StatusOK)
	var ev events.Event
	if err := json.NewDecoder(resp.Body).Decode(&ev); err != nil {
		t.Fatal(err)
	}
	return ev
}

// wantError checks the status and the JSON error code.
func wantError(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	wantStatus(t, resp, status)
	if got, msg := errCode(t, resp); got != code {
		t.Fatalf("code = %q (%s), want %q", got, msg, code)
	}
}

func dialRoom(t *testing.T, e *oidcEnv, roomID string, c *http.Cookie) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/api/rooms/"+roomID+"/ws?after=0",
		&websocket.DialOptions{HTTPHeader: http.Header{"Cookie": {c.Name + "=" + c.Value}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

// readUntil reads frames until match accepts one, or fails after 5 s.
func readUntil(t *testing.T, conn *websocket.Conn, what string, match func(map[string]any) bool) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		var m map[string]any
		if json.Unmarshal(data, &m) == nil && match(m) {
			return m
		}
	}
}

func approvalsOf(t *testing.T, e *oidcEnv, roomID string) []events.ApprovalGivenPayload {
	t.Helper()
	return decodedEvents[events.ApprovalGivenPayload](t, e.svc, roomID, events.ApprovalGiven)
}

// ---- (a) A-AP-3: a users.yaml layer over a group-owned layer ----

// users.yaml gives mallory the consumer layer; owners.yaml lists consumer as
// [user:kabir, group:consumers] and does not name her. The start check refuses
// the config in both modes. If such a config reaches the service without the
// check, her approval is refused per request and /api/me does not offer the
// layer, while the layer's real owners still approve.
func TestLifecycle_UsersLayerOverGroupLayer(t *testing.T) {
	bad := lifeUsersYAML + `
  - id: mallory
    name: Mallory
    email: mallory@example.com
    layers: [consumer]
`
	for _, mode := range lifeModes {
		t.Run(mode, func(t *testing.T) {
			sh := newLifeShared(t)
			_, _, err := sh.start(t, lifeConfig{mode: mode, users: bad, owners: lifeOwnersYAML})
			if err == nil || !strings.Contains(err.Error(), `grants "mallory" layer "consumer"`) {
				t.Fatalf("start = %v, want the users/owners check to refuse mallory's consumer layer", err)
			}

			e, _ := sh.mustStart(t, lifeConfig{mode: mode, users: bad, owners: lifeOwnersYAML, unchecked: true})
			snap := e.newRoom(t, "kabir")
			mallory := signIn(t, e, mode, "mallory")
			if layers, _ := e.me(t, mallory)["layers"].([]any); len(layers) != 0 {
				t.Errorf("/api/me.layers = %v for mallory, want none", layers)
			}
			pid := e.proposal(t, snap.ID, "consumer")
			wantError(t, e.approve(t, mallory, pid, "consumer"), http.StatusForbidden, "not_layer_owner")
			body := fmt.Sprintf(`{"layer":"consumer","reason":"no","params_hash":%q}`, e.hash(t, pid))
			wantError(t, e.do(t, http.MethodPost, "/api/proposals/"+pid+"/reject", body, ck(mallory)), http.StatusForbidden, "not_layer_owner")
			if got := approvalsOf(t, e, snap.ID); len(got) != 0 {
				t.Fatalf("approvals = %+v, want none", got)
			}

			// The owners owners.yaml names still approve: kabir as user:kabir and,
			// with an IdP group claim, a consumers member.
			wantStatus(t, e.approve(t, signIn(t, e, mode, "kabir"), pid, "consumer"), http.StatusOK)
			if mode == auth.ModeOIDC {
				member := e.mustLogin(t, testenv.IDPUser{Sub: "idp-consumers-1", Groups: []string{"consumers"}})
				pid2 := e.proposal(t, snap.ID, "consumer")
				wantStatus(t, e.approve(t, member, pid2, "consumer"), http.StatusOK)
			}
		})
	}
}

// ---- (b) A-ID-5: unbind ends the old account's sessions and sockets ----

// The admin unbinds priya (devctl identities unbind runs the store Unbind).
// The account's live session is revoked in the same step: its next HTTP request
// is 401 session_expired, a write is refused, and its WebSocket closes on the
// next presence tick. A bystander's session and socket carry on, and the new
// account binds on its first verified-email login.
func TestLifecycle_UnbindEndsSessionsAndSockets(t *testing.T) {
	sh := newLifeShared(t)
	e, _ := sh.mustStart(t, lifeConfig{mode: auth.ModeOIDC, users: lifeUsersYAML, owners: lifeOwnersYAML})
	snap := e.newRoom(t, "kabir")
	priya := signIn(t, e, auth.ModeOIDC, "priya")
	kabir := signIn(t, e, auth.ModeOIDC, "kabir")
	pConn := dialRoom(t, e, snap.ID, priya)
	kConn := dialRoom(t, e, snap.ID, kabir)
	pID, kID := sessionID(t, e, priya), sessionID(t, e, kabir)
	pid := e.proposal(t, snap.ID, "postgres")
	hash := e.hash(t, pid)

	res, err := e.idents.Unbind(context.Background(), "priya", time.Now())
	if err != nil || res.Unbound != 1 || res.Revoked != 1 {
		t.Fatalf("unbind = %+v, %v; want 1 binding and 1 session", res, err)
	}

	wantError(t, e.do(t, http.MethodGet, "/api/me", "", ck(priya)), http.StatusUnauthorized, "session_expired")
	body := fmt.Sprintf(`{"layer":"postgres","params_hash":%q}`, hash)
	wantError(t, e.do(t, http.MethodPost, "/api/proposals/"+pid+"/approve", body, ck(priya)), http.StatusUnauthorized, "session_expired")
	if got := approvalsOf(t, e, snap.ID); len(got) != 0 {
		t.Fatalf("approvals = %+v, want none", got)
	}

	closed := e.api.PresenceTickForTest(context.Background(), pID, kID)
	if len(closed) != 1 || closed[0] != pID {
		t.Fatalf("presence tick closed %v, want only priya's session %s", closed, pID)
	}
	frames, closeErr := readUntilClosed(t, pConn)
	if websocket.CloseStatus(closeErr) != websocket.StatusPolicyViolation || !strings.Contains(closeErr.Error(), "session_expired") {
		t.Fatalf("priya's socket ended with %v, want a policy-violation close with session_expired", closeErr)
	}
	if len(frames) != 1 {
		t.Fatalf("error frames = %v, want one session_expired frame", frames)
	}

	// kabir's socket is still live: his own comment comes back on it.
	wantStatus(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/comments", `{"text":"still here"}`, ck(kabir)), http.StatusOK)
	readUntil(t, kConn, "kabir's comment", func(m map[string]any) bool {
		ev, _ := m["event"].(map[string]any)
		p, _ := ev["payload"].(map[string]any)
		return m["type"] == "event" && ev["type"] == string(events.CommentAdded) && p["text"] == "still here"
	})

	// The new account binds on its first verified-email login.
	fresh := e.mustLogin(t, testenv.IDPUser{Sub: "idp-priya-new", Email: "priya@example.com", EmailVerified: true, Groups: []string{}})
	if me := e.me(t, fresh); me["id"] != "priya" {
		t.Fatalf("new account = %v, want priya", me)
	}
}

// ---- (c) a role change or a revoked session mid-room ----

// A proposal waits for priya's postgres approval; priya has the room open and
// her session is live. Then the admin changes the files and restarts, or her
// session is revoked. Her click on the approve button she still sees is
// refused, and nothing is recorded.
func TestLifecycle_RoleChangeMidRoom(t *testing.T) {
	// postgres moves to meera; priya stays a member with no layer.
	movedUsers := strings.Replace(lifeUsersYAML, "layers: [postgres]", "layers: []", 1)
	movedUsers = strings.Replace(movedUsers, "layers: [warehouse]", "layers: [warehouse, postgres]", 1)
	movedOwners := strings.Replace(lifeOwnersYAML, "postgres: priya", "postgres: meera", 1)
	// priya leaves both files.
	goneUsers := strings.Replace(movedUsers, "  - id: priya\n    name: Priya\n    email: priya@example.com\n    layers: []\n", "", 1)

	for _, mode := range lifeModes {
		// setup starts the first process with priya's room and proposal open.
		setup := func(t *testing.T) (*lifeShared, *oidcEnv, func(), string, string, *http.Cookie) {
			sh := newLifeShared(t)
			e, stop := sh.mustStart(t, lifeConfig{mode: mode, users: lifeUsersYAML, owners: lifeOwnersYAML})
			snap := e.newRoom(t, "kabir")
			pid := e.proposal(t, snap.ID, "postgres")
			priya := signIn(t, e, mode, "priya")
			if layers, _ := e.me(t, priya)["layers"].([]any); len(layers) != 1 || layers[0] != "postgres" {
				t.Fatalf("before: /api/me.layers = %v, want [postgres]", layers)
			}
			return sh, e, stop, snap.ID, pid, priya
		}
		click := func(t *testing.T, e *oidcEnv, pid, hash string, c *http.Cookie) *http.Response {
			return e.do(t, http.MethodPost, "/api/proposals/"+pid+"/approve", fmt.Sprintf(`{"layer":"postgres","params_hash":%q}`, hash), ck(c))
		}

		t.Run(mode+"/layer moved to another owner, restart", func(t *testing.T) {
			sh, e, stop, roomID, pid, priya := setup(t)
			hash := e.hash(t, pid)
			stop()
			e2, _ := sh.mustStart(t, lifeConfig{mode: mode, users: movedUsers, owners: movedOwners})
			wantError(t, click(t, e2, pid, hash, priya), http.StatusForbidden, "not_layer_owner")
			if layers, _ := e2.me(t, priya)["layers"].([]any); len(layers) != 0 {
				t.Errorf("after: /api/me.layers = %v, want none", layers)
			}
			if got := approvalsOf(t, e2, roomID); len(got) != 0 {
				t.Fatalf("approvals = %+v, want none", got)
			}
			// The new owner can.
			wantStatus(t, click(t, e2, pid, hash, signIn(t, e2, mode, "meera")), http.StatusOK)
			waitFor(t, func() bool { return e2.acts["postgres"].count() == 1 }, "the step to run")
		})

		t.Run(mode+"/owner removed from both files, restart", func(t *testing.T) {
			sh, e, stop, roomID, pid, priya := setup(t)
			hash := e.hash(t, pid)
			stop()
			e2, _ := sh.mustStart(t, lifeConfig{mode: mode, users: goneUsers, owners: movedOwners})
			wantError(t, click(t, e2, pid, hash, priya), http.StatusUnauthorized, "session_expired")
			if got := approvalsOf(t, e2, roomID); len(got) != 0 {
				t.Fatalf("approvals = %+v, want none", got)
			}
		})

		t.Run(mode+"/session revoked", func(t *testing.T) {
			_, e, _, roomID, pid, priya := setup(t)
			if err := e.sessions.Revoke(context.Background(), sessionID(t, e, priya), time.Now()); err != nil {
				t.Fatal(err)
			}
			wantError(t, click(t, e, pid, e.hash(t, pid), priya), http.StatusUnauthorized, "session_expired")
			if got := approvalsOf(t, e, roomID); len(got) != 0 {
				t.Fatalf("approvals = %+v, want none", got)
			}
		})

		t.Run(mode+"/signed out in another tab", func(t *testing.T) {
			_, e, _, roomID, pid, priya := setup(t)
			wantStatus(t, e.do(t, http.MethodPost, "/auth/logout", "", ck(priya)), http.StatusNoContent)
			wantError(t, click(t, e, pid, e.hash(t, pid), priya), http.StatusUnauthorized, "session_expired")
			if got := approvalsOf(t, e, roomID); len(got) != 0 {
				t.Fatalf("approvals = %+v, want none", got)
			}
		})
	}

	// A group grant ends with the group: owners.yaml takes dataeng-oncall off
	// kafka-connect (it keeps the group on kafka, so the person is still a
	// member and gets in).
	t.Run("oidc/group taken off the layer, restart", func(t *testing.T) {
		sh := newLifeShared(t)
		e, stop := sh.mustStart(t, lifeConfig{mode: auth.ModeOIDC, users: lifeUsersYAML, owners: lifeOwnersYAML})
		snap := e.newRoom(t, "kabir")
		pid := e.proposal(t, snap.ID, "kafka-connect")
		hash := e.hash(t, pid)
		oncall := e.mustLogin(t, idpOncall)
		stop()
		regrouped := strings.Replace(lifeOwnersYAML, "kafka-connect: [group:dataeng-oncall, user:ops-bot]", "kafka-connect: [group:platform]", 1)
		regrouped = strings.Replace(regrouped, "kafka: arjun", "kafka: [user:arjun, group:dataeng-oncall]", 1)
		e2, _ := sh.mustStart(t, lifeConfig{mode: auth.ModeOIDC, users: lifeUsersYAML, owners: regrouped})
		resp := e2.do(t, http.MethodPost, "/api/proposals/"+pid+"/approve", fmt.Sprintf(`{"layer":"kafka-connect","params_hash":%q}`, hash), ck(oncall))
		wantError(t, resp, http.StatusForbidden, "not_layer_owner")
		if got := approvalsOf(t, e2, snap.ID); len(got) != 0 {
			t.Fatalf("approvals = %+v, want none", got)
		}
	})
}

// ---- (d) the old driver after a hand-off ----

// priya drives, hands off and the recipient accepts. Her next steer is a
// suggestion (steer.suggested), never a steer the agent acts on; she can no
// longer forward suggestions, hand off or take the wheel back while the new
// lease is live. In oidc mode the recipient is a group-only member, and the
// events mark them unverified.
func TestLifecycle_OldDriverAfterHandoff(t *testing.T) {
	for _, mode := range lifeModes {
		t.Run(mode, func(t *testing.T) {
			sh := newLifeShared(t)
			e, _ := sh.mustStart(t, lifeConfig{mode: mode, users: lifeUsersYAML, owners: lifeOwnersYAML})
			priya := signIn(t, e, mode, "priya")
			resp := e.do(t, http.MethodPost, "/api/rooms", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`, ck(priya))
			wantStatus(t, resp, http.StatusCreated)
			var snap room.RoomSnapshot
			if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
				t.Fatal(err)
			}
			if snap.Driver == nil || snap.Driver.ID != "priya" || !snap.Driver.Verified {
				t.Fatalf("driver = %+v, want priya, verified", snap.Driver)
			}

			// The recipient: arjun in dev mode; in oidc mode a member who is in
			// owners.yaml only through the dataeng-oncall group.
			next, nextID, nextVerified := (*http.Cookie)(nil), "arjun", true
			if mode == auth.ModeDev {
				next = signIn(t, e, mode, "arjun")
			} else {
				next = e.mustLogin(t, idpOncall)
				nextID, _ = e.me(t, next)["id"].(string)
				nextVerified = false
			}

			created := event(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/handoffs", `{"to":"`+nextID+`","stay_as_watcher":true}`, ck(priya)))
			hp, err := events.Decode[events.HandoffCreatedPayload](created)
			if err != nil {
				t.Fatal(err)
			}
			if hp.To.ID != nextID || hp.To.Verified != nextVerified || !created.Actor.Verified {
				t.Fatalf("handoff.created actor %+v to %+v, want verified priya to %s (verified %v)", created.Actor, hp.To, nextID, nextVerified)
			}
			event(t, e.do(t, http.MethodPost, "/api/handoffs/"+hp.HandoffID+"/accept", "", ck(next)))
			got, err := e.svc.GetRoom(context.Background(), snap.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Driver == nil || got.Driver.ID != nextID || got.Driver.Verified != nextVerified {
				t.Fatalf("driver after accept = %+v, want %s (verified %v)", got.Driver, nextID, nextVerified)
			}

			old := event(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/steer", `{"text":"restart the connector"}`, ck(priya)))
			if old.Type != events.SteerSuggested {
				t.Fatalf("the old driver's steer is %s, want %s", old.Type, events.SteerSuggested)
			}
			sp, err := events.Decode[events.SteerSuggestedPayload](old)
			if err != nil {
				t.Fatal(err)
			}
			wantError(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/suggestions/"+sp.SuggestionID+"/send", "", ck(priya)), http.StatusForbidden, "not_driver")
			wantError(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/handoffs", `{"to":"meera"}`, ck(priya)), http.StatusForbidden, "not_driver")
			wantError(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/control/grant", `{"to":"priya"}`, ck(priya)), http.StatusConflict, "lease_held")
			wantError(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/close", `{"resolution":"done"}`, ck(priya)), http.StatusForbidden, "not_driver")

			// Every steer.sent after the accept is the new driver's.
			if ev := event(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/steer", `{"text":"check the slot"}`, ck(next))); ev.Type != events.SteerSent {
				t.Fatalf("the new driver's steer is %s, want %s", ev.Type, events.SteerSent)
			}
			evs, _, err := e.svc.Events(context.Background(), snap.ID, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			for _, ev := range evs {
				if ev.Type == events.SteerSent && ev.Actor.ID == "priya" {
					t.Fatalf("priya has a steer.sent at seq %d", ev.Seq)
				}
			}
		})
	}
}

// ---- (e) decided proposals and the params_hash binding ----

func (e *oidcEnv) tableProposal(t *testing.T, roomID, table string) string {
	t.Helper()
	pid, err := e.svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "point the sink at " + table, Risk: "low", Rollback: "point it back",
		Steps: []events.ProposalStep{{Action: "set_table", Layer: "postgres", Params: json.RawMessage(`{"table":"` + table + `"}`), Description: "set table"}},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// A rejected, executed or closed-room proposal cannot be approved (there is
// no time-based proposal expiry; a proposal ends rejected, executed or failed,
// or with its room). The approval is bound to the exact params: another
// proposal's hash is 409, a missing one 400, the recorded hash is the hash of
// these params, and a row whose params change after an approval never runs.
func TestLifecycle_DecidedProposalsAndParamsHash(t *testing.T) {
	sh := newLifeShared(t)
	e, _ := sh.mustStart(t, lifeConfig{mode: auth.ModeDev, users: lifeUsersYAML, owners: lifeOwnersYAML})
	priya := signIn(t, e, auth.ModeDev, "priya")
	kabir := signIn(t, e, auth.ModeDev, "kabir")
	approve := func(c *http.Cookie, pid, layer, hash string) *http.Response {
		return e.do(t, http.MethodPost, "/api/proposals/"+pid+"/approve", fmt.Sprintf(`{"layer":%q,"params_hash":%q}`, layer, hash), ck(c))
	}

	t.Run("rejected", func(t *testing.T) {
		snap := e.newRoom(t, "kabir")
		pid := e.proposal(t, snap.ID, "postgres", "consumer")
		hash := e.hash(t, pid)
		body := fmt.Sprintf(`{"layer":"consumer","reason":"not now","params_hash":%q}`, hash)
		wantStatus(t, e.do(t, http.MethodPost, "/api/proposals/"+pid+"/reject", body, ck(kabir)), http.StatusOK)
		wantError(t, approve(priya, pid, "postgres", hash), http.StatusConflict, "already_decided")
		if got := approvalsOf(t, e, snap.ID); len(got) != 0 {
			t.Fatalf("approvals = %+v, want none", got)
		}
	})

	t.Run("executed", func(t *testing.T) {
		snap := e.newRoom(t, "kabir")
		pid := e.proposal(t, snap.ID, "consumer")
		hash := e.hash(t, pid)
		before := e.acts["consumer"].count()
		wantStatus(t, approve(kabir, pid, "consumer", hash), http.StatusOK)
		waitFor(t, func() bool { return e.acts["consumer"].count() == before+1 }, "the step to run")
		waitFor(t, func() bool {
			p, err := sh.st.GetProposal(context.Background(), pid)
			return err == nil && p.Status == "executed"
		}, "the proposal to be executed")
		wantError(t, approve(kabir, pid, "consumer", hash), http.StatusConflict, "already_decided")
	})

	t.Run("room closed", func(t *testing.T) {
		snap := e.newRoom(t, "kabir")
		pid := e.proposal(t, snap.ID, "postgres")
		hash := e.hash(t, pid)
		wantStatus(t, e.do(t, http.MethodPost, "/api/rooms/"+snap.ID+"/close", `{"resolution":"fixed by hand"}`, ck(kabir)), http.StatusOK)
		wantError(t, approve(priya, pid, "postgres", hash), http.StatusConflict, "room_closed")
	})

	t.Run("hash binds the params", func(t *testing.T) {
		snap := e.newRoom(t, "kabir")
		a := e.tableProposal(t, snap.ID, "orders_a")
		b := e.tableProposal(t, snap.ID, "orders_b")
		ha, hb := e.hash(t, a), e.hash(t, b)
		if ha == hb {
			t.Fatal("two proposals that differ in params share a hash")
		}
		wantError(t, approve(priya, a, "postgres", hb), http.StatusConflict, "params_mismatch")
		wantError(t, e.do(t, http.MethodPost, "/api/proposals/"+a+"/approve", `{"layer":"postgres"}`, ck(priya)), http.StatusBadRequest, "validation")
		ev := event(t, approve(priya, a, "postgres", ha))
		given, err := events.Decode[events.ApprovalGivenPayload](ev)
		if err != nil {
			t.Fatal(err)
		}
		want, err := room.ComputeParamsHash(snap.ID, a, "point the sink at orders_a", "low", "point it back", []string{"postgres"},
			[]events.ProposalStep{{Action: "set_table", Layer: "postgres", Params: json.RawMessage(`{"table":"orders_a"}`), Description: "set table"}})
		if err != nil {
			t.Fatal(err)
		}
		if given.ParamsHash != want || given.ParamsHash != ha {
			t.Fatalf("approval params_hash = %s, want the hash of these params %s (shown %s)", given.ParamsHash, want, ha)
		}
	})

	t.Run("params edited between two approvals", func(t *testing.T) {
		snap := e.newRoom(t, "kabir")
		pid := e.proposal(t, snap.ID, "postgres", "consumer")
		hash := e.hash(t, pid)
		wantStatus(t, approve(priya, pid, "postgres", hash), http.StatusOK)
		pgBefore, conBefore := e.acts["postgres"].count(), e.acts["consumer"].count()
		edit := func(p store.ProposalRow) store.ProposalRow {
			if p.ProposalID == pid {
				p.Steps = json.RawMessage(strings.Replace(string(p.Steps), `"do postgres"`, `"drop the table"`, 1))
			}
			return p
		}
		sh.st.edit.Store(&edit)
		defer sh.st.edit.Store(nil)
		wantStatus(t, approve(kabir, pid, "consumer", hash), http.StatusOK)
		waitFor(t, func() bool {
			p, err := sh.st.Store.GetProposal(context.Background(), pid)
			return err == nil && p.Status == "failed"
		}, "the proposal to fail")
		if e.acts["postgres"].count() != pgBefore || e.acts["consumer"].count() != conBefore {
			t.Fatal("a step ran on an edited row")
		}
		res := decodedEvents[events.ActionExecutedPayload](t, e.svc, snap.ID, events.ActionExecuted)
		if len(res) != 1 || res[0].OK || res[0].Output != "params hash mismatch" {
			t.Fatalf("action.executed = %+v, want one failed step: params hash mismatch", res)
		}
	})
}

// ---- (f) a group member's approval ----

// A person who is in owners.yaml only through an IdP group claim approves a
// group-owned layer. The approval records the group principal as the grant,
// the derived actor id (unverified), the IdP iss and sub, the session, the
// verified email and the params_hash; the export carries the same. The right
// comes from the claim alone: the same account without the group is refused.
func TestLifecycle_GroupApprovalRecordsTheGroup(t *testing.T) {
	sh := newLifeShared(t)
	e, _ := sh.mustStart(t, lifeConfig{mode: auth.ModeOIDC, users: lifeUsersYAML, owners: lifeOwnersYAML})
	snap := e.newRoom(t, "kabir")
	oncall := e.mustLogin(t, idpOncall)
	me := e.me(t, oncall)
	id, _ := me["id"].(string)
	if !strings.HasPrefix(id, room.DerivedIDPrefix) || me["verified"] != false {
		t.Fatalf("/api/me = %v, want a derived id, unverified", me)
	}
	pid := e.proposal(t, snap.ID, "kafka-connect")
	hash := e.hash(t, pid)
	ev := event(t, e.approve(t, oncall, pid, "kafka-connect"))
	if ev.Actor.ID != id || ev.Actor.Verified || ev.Actor.Name != idpOncall.Name {
		t.Fatalf("actor = %+v, want %s, unverified, named %q", ev.Actor, id, idpOncall.Name)
	}
	given, err := events.Decode[events.ApprovalGivenPayload](ev)
	if err != nil {
		t.Fatal(err)
	}
	ident := given.Identity
	if ident == nil || ident.Grant != "group:dataeng-oncall" || ident.Method != auth.MethodOIDC || ident.Iss != sh.idp.Issuer ||
		ident.Sub != idpOncall.Sub || ident.SessionID != sessionID(t, e, oncall) || ident.Email != idpOncall.Email || given.ParamsHash != hash {
		t.Fatalf("approval = %+v identity %+v, want grant group:dataeng-oncall with iss, sub, session, email and the hash", given, ident)
	}
	waitFor(t, func() bool { return e.acts["kafka-connect"].count() == 1 }, "the step to run")

	// The export (an auditor's) records the same approver and grant.
	resp := e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/export", "", ck(e.mustLogin(t, idpAuditor)))
	wantStatus(t, resp, http.StatusOK)
	var doc struct {
		Approvals []struct {
			Approver events.Actor     `json:"approver"`
			Identity *events.Identity `json:"identity"`
		} `json:"approvals"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Approvals) != 1 || doc.Approvals[0].Approver.ID != id || doc.Approvals[0].Approver.Verified ||
		doc.Approvals[0].Identity == nil || doc.Approvals[0].Identity.Grant != "group:dataeng-oncall" {
		t.Fatalf("export approvals = %+v", doc.Approvals)
	}

	// Moved out of the group in the IdP, the same person signs in as a
	// viewer only, and cannot approve.
	moved := idpOncall
	moved.Groups = []string{"viewers"}
	again := e.mustLogin(t, moved)
	pid2 := e.proposal(t, snap.ID, "kafka-connect")
	wantError(t, e.approve(t, again, pid2, "kafka-connect"), http.StatusForbidden, "viewer")
	if n := len(approvalsOf(t, e, snap.ID)); n != 1 {
		t.Fatalf("approvals = %d, want only the first", n)
	}
}
