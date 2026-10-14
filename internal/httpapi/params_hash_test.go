package httpapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gowebpki/jcs"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/testenv"
)

// Auth PR 4 over HTTP (docs/design/auth.md sections 3.1 to 3.4): the params_hash
// echo on approve and reject, the identity on approval events, and who may
// export.

// decodedEvents returns the room's events of one type, decoded.
func decodedEvents[T any](t *testing.T, svc *room.Service, roomID string, typ events.Type) []T {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []T
	for _, ev := range evs {
		if ev.Type != typ {
			continue
		}
		p, err := events.Decode[T](ev)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func TestOIDCApproveNeedsTheParamsHash(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	priya := e.mustLogin(t, idpPriya)
	pid := e.proposal(t, snap.ID, "postgres")
	hash := e.hash(t, pid)
	if len(hash) != 64 {
		t.Fatalf("snapshot params_hash = %q, want 64 hex characters", hash)
	}
	do := func(path, body string) *http.Response {
		return e.do(t, http.MethodPost, "/api/proposals/"+pid+path, body, ck(priya))
	}

	// Missing: 400 validation. Different: 409 params_mismatch. Nothing is recorded either way.
	for name, body := range map[string]string{
		"approve, no hash":    `{"layer":"postgres"}`,
		"approve, empty hash": `{"layer":"postgres","params_hash":""}`,
	} {
		resp := do("/approve", body)
		wantStatus(t, resp, http.StatusBadRequest)
		if c, _ := errCode(t, resp); c != "validation" {
			t.Fatalf("%s: code %q, want validation", name, c)
		}
	}
	resp := do("/reject", `{"layer":"postgres","reason":"no"}`)
	wantStatus(t, resp, http.StatusBadRequest)
	other := strings.Repeat("0", 64)
	for _, path := range []string{"/approve", "/reject"} {
		resp := do(path, fmt.Sprintf(`{"layer":"postgres","reason":"no","params_hash":%q}`, other))
		wantStatus(t, resp, http.StatusConflict)
		if c, _ := errCode(t, resp); c != "params_mismatch" {
			t.Fatalf("%s: code %q, want params_mismatch", path, c)
		}
	}
	if n := len(decodedEvents[events.ApprovalGivenPayload](t, e.svc, snap.ID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals recorded by refused calls", n)
	}

	// The right hash goes through.
	wantStatus(t, do("/approve", fmt.Sprintf(`{"layer":"postgres","params_hash":%q}`, hash)), http.StatusOK)
	waitFor(t, func() bool { return e.acts["postgres"].count() == 1 }, "the approved step to run")
}

// An oidc approval carries the params_hash and the full identity; action.executed
// carries the hash; the export row re-hashes to it.
func TestOIDCApprovalIdentityAndExportRehash(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	snap := e.newRoom(t, "priya")
	priya := e.mustLogin(t, idpPriya)
	auditor := e.mustLogin(t, idpAuditor)
	pid := e.proposal(t, snap.ID, "postgres")
	hash := e.hash(t, pid)
	wantStatus(t, e.approve(t, priya, pid, "postgres"), http.StatusOK)
	waitFor(t, func() bool { return e.acts["postgres"].count() == 1 }, "the approved step to run")
	waitFor(t, func() bool {
		return len(decodedEvents[events.ActionExecutedPayload](t, e.svc, snap.ID, events.ActionExecuted)) == 1
	}, "action.executed")

	hh, _ := auth.HashToken(priya.Value)
	sess, err := e.sessions.ByTokenHash(context.Background(), hh)
	if err != nil {
		t.Fatal(err)
	}

	created := decodedEvents[events.ProposalCreatedPayload](t, e.svc, snap.ID, events.ProposalCreated)
	if len(created) != 1 || created[0].ParamsHash != hash {
		t.Fatalf("proposal.created params_hash = %v, want %s", created, hash)
	}
	given := decodedEvents[events.ApprovalGivenPayload](t, e.svc, snap.ID, events.ApprovalGiven)
	if len(given) != 1 || given[0].ParamsHash != hash || given[0].Identity == nil {
		t.Fatalf("approval.given = %+v", given)
	}
	id := given[0].Identity
	if id.Method != "oidc" || id.Iss != e.idp.Issuer || id.Sub != "idp-priya" || id.SessionID != sess.ID ||
		!strings.HasPrefix(id.SessionID, "ses_") || id.Grant != "user:priya" {
		t.Fatalf("identity = %+v (session %s)", id, sess.ID)
	}
	if !id.AuthTime.IsZero() && id.AuthTime.Location() != time.UTC {
		t.Fatalf("auth_time %v is not UTC", id.AuthTime)
	}
	if strings.Contains(string(mustJSON(t, id)), priya.Value) {
		t.Fatal("the session token is in the identity")
	}
	done := decodedEvents[events.ActionExecutedPayload](t, e.svc, snap.ID, events.ActionExecuted)
	if len(done) != 1 || !done[0].OK || done[0].ParamsHash != hash {
		t.Fatalf("action.executed = %+v, want ok with params_hash %s", done, hash)
	}

	// The export: the same chain, and the proposal re-hashes to params_hash.
	resp := e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/export", "", ck(auditor))
	wantStatus(t, resp, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	props := doc["proposals"].([]any)
	if len(props) != 1 {
		t.Fatalf("proposals = %v", props)
	}
	p := props[0].(map[string]any)
	if p["params_hash"] != hash {
		t.Fatalf("export proposal params_hash = %v, want %s", p["params_hash"], hash)
	}
	rebuilt := map[string]any{
		"v": 2, "room_id": doc["room"].(map[string]any)["id"], "proposal_id": p["proposal_id"],
		"title": p["title"], "risk": p["risk"], "rollback": p["rollback"],
		"required_layers": p["required_layers"], "steps": p["steps"],
	}
	raw, err := json.Marshal(rebuilt)
	if err != nil {
		t.Fatal(err)
	}
	canon, err := jcs.Transform(raw)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canon)
	if got := hex.EncodeToString(sum[:]); got != hash {
		t.Fatalf("the export row re-hashes to %s, want params_hash %s", got, hash)
	}
	ap := doc["approvals"].([]any)[0].(map[string]any)
	if ap["params_hash"] != hash || ap["identity"].(map[string]any)["method"] != "oidc" || ap["identity"].(map[string]any)["grant"] != "user:priya" {
		t.Fatalf("export approval = %v", ap)
	}
	if ar := doc["action_results"].([]any)[0].(map[string]any); ar["params_hash"] != hash {
		t.Fatalf("export action result = %v", ar)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Design 3.4: only an approver or backup for a layer, or an auditors: principal,
// may export. The driver, a plain member and a viewer may not.
func TestOIDCExportAccess(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	// ksenia is in users.yaml with no layer and is not in owners.yaml: a plain
	// member. She opens the room, so she is its driver.
	snap := e.newRoom(t, "ksenia")
	if snap.Driver == nil || snap.Driver.ID != "ksenia" {
		t.Fatalf("driver = %+v, want ksenia", snap.Driver)
	}
	ksenia := e.mustLogin(t, testenv.IDPUser{Sub: "idp-ksenia", Email: "ksenia@example.com", EmailVerified: true})
	if me := e.me(t, ksenia); me["id"] != "ksenia" || me["role"] != "member" {
		t.Fatalf("me = %v", me)
	}
	other := e.mustLogin(t, testenv.IDPUser{Sub: "idp-other-member", Groups: []string{"consumers"}}) // a group approver, not the driver

	cases := []struct {
		name string
		who  *http.Cookie
		want int
	}{
		{"a driver who is only a member", ksenia, http.StatusForbidden},
		{"a viewer", e.mustLogin(t, idpViewer), http.StatusForbidden},
		{"an auditor-only principal", e.mustLogin(t, idpAuditor), http.StatusOK},
		{"an approver by users.yaml layers and owners.yaml", e.mustLogin(t, idpPriya), http.StatusOK},
		{"an approver through a group", e.mustLogin(t, idpOncall), http.StatusOK},
		{"an approver through another group", other, http.StatusOK},
		{"a backup approver", e.mustLogin(t, testenv.IDPUser{Sub: "idp-meera", Email: "meera@example.com", EmailVerified: true}), http.StatusOK},
	}
	for _, c := range cases {
		resp := e.do(t, http.MethodGet, "/api/rooms/"+snap.ID+"/export", "", ck(c.who))
		if resp.StatusCode != c.want {
			b, _ := io.ReadAll(resp.Body)
			t.Errorf("%s: export = %d, want %d; %s", c.name, resp.StatusCode, c.want, b)
			continue
		}
		if c.want == http.StatusForbidden {
			if code, _ := errCode(t, resp); code != "not_allowed" {
				t.Errorf("%s: code %q, want not_allowed", c.name, code)
			}
		}
	}
	// Even the driver loses nothing but the export: she can still read the room.
	wantStatus(t, e.do(t, http.MethodGet, "/api/rooms/"+snap.ID, "", ck(ksenia)), http.StatusOK)
}

// The login_error value is always one of the fixed set, never IdP text.
func TestOIDCLoginErrorIsAFixedCode(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	_, lc, au := e.startLogin(t, "/")
	resp := e.do(t, http.MethodGet, "/auth/callback?error="+url.QueryEscape("<script>alert(1)</script>")+"&error_description=evil&state="+url.QueryEscape(au.Query().Get("state")), "", ck(lc))
	wantLoginError(t, resp, "unauthenticated")
	if strings.Contains(resp.Header.Get("Location"), "script") {
		t.Fatal("IdP text reached the redirect")
	}
}

// ---- dev mode ----

// devApprovalEnv is a dev-mode server (users.yaml layers) with a postgres and a
// kafka action.
func devApprovalEnv(t *testing.T, solo string) (*httptest.Server, *room.Service, room.RoomSnapshot, func(layer string) string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	src := "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\nlayers:\n  postgres: priya\n"
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := actions.NewRegistry()
	for _, l := range []string{"postgres", "kafka"} {
		if err := reg.Register(&testAction{layer: l}); err != nil {
			t.Fatal(err)
		}
	}
	log := slog.New(slog.NewTextHandler(&syncBuf{}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: own, Actions: reg, Logger: log, SoloUser: solo,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	api := httpapi.New(svc, st, log, httpapi.WithSessions(auth.NewMemStore()))
	srv := httptest.NewServer(api)
	t.Cleanup(func() { srv.Close(); api.Close() })
	priya, _ := svc.LookupUser("priya")
	snap, err := svc.CreateRoom(context.Background(), priya, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	newProposal := func(layer string) string {
		pid, err := svc.CreateProposal(context.Background(), snap.ID, events.ProposalCreatedPayload{
			Title: "fix", Risk: "low risk", Rollback: "undo it",
			Steps: []events.ProposalStep{{Action: "fix_" + layer, Layer: layer, Params: json.RawMessage(`{}`), Description: "d"}},
		}, events.AgentActor)
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}
	return srv, svc, snap, newProposal
}

func hashOfOpen(t *testing.T, svc *room.Service, pid string) string {
	t.Helper()
	rooms, err := svc.ListRooms(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rooms {
		for _, p := range r.OpenProposals {
			if p.ProposalID == pid {
				return p.ParamsHash
			}
		}
	}
	t.Fatalf("proposal %s is not open", pid)
	return ""
}

// Dev mode: a dev-login session records method "dev" with its public session id;
// an X-User request with no session records method "dev" and no session id. The
// identity is never empty.
func TestDevApprovalIdentity(t *testing.T) {
	srv, svc, snap, newProposal := devApprovalEnv(t, "")

	// X-User, no cookie.
	pid := newProposal("postgres")
	body := fmt.Sprintf(`{"layer":"postgres","params_hash":%q}`, hashOfOpen(t, svc, pid))
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/proposals/"+pid+"/approve", "priya", body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("X-User approve = %d", resp.StatusCode)
	}

	// A dev-login session (cookie only).
	resp = doJSON(t, http.MethodPost, srv.URL+"/auth/dev-login", "", `{"user":"priya"}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(resp.Cookies()) == 0 {
		t.Fatalf("dev-login = %d", resp.StatusCode)
	}
	cookie := resp.Cookies()[0]
	pid2 := newProposal("postgres")
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/proposals/"+pid2+"/approve",
		strings.NewReader(fmt.Sprintf(`{"layer":"postgres","params_hash":%q}`, hashOfOpen(t, svc, pid2))))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("cookie approve = %d", resp2.StatusCode)
	}

	given := decodedEvents[events.ApprovalGivenPayload](t, svc, snap.ID, events.ApprovalGiven)
	if len(given) != 2 {
		t.Fatalf("approvals = %+v", given)
	}
	for i, g := range given {
		if g.ParamsHash == "" || g.Identity == nil || g.Identity.Method != "dev" || g.Identity.Grant != "user:priya" ||
			g.Identity.Iss != "" || g.Identity.Sub != "" {
			t.Fatalf("approval %d identity = %+v (%q)", i, g.Identity, g.ParamsHash)
		}
	}
	if given[0].Identity.SessionID != "" {
		t.Fatalf("an X-User request has no session, got session_id %q", given[0].Identity.SessionID)
	}
	if !strings.HasPrefix(given[1].Identity.SessionID, "ses_") {
		t.Fatalf("a dev-login session should record its public id, got %q", given[1].Identity.SessionID)
	}
}

// A solo approval records grant "solo".
func TestSoloApprovalIdentityAndMeFlag(t *testing.T) {
	srv, svc, snap, newProposal := devApprovalEnv(t, "priya")
	pid := newProposal("kafka")
	body := fmt.Sprintf(`{"layer":"kafka","params_hash":%q,"confirm":"orders/kafka"}`, hashOfOpen(t, svc, pid))
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/proposals/"+pid+"/approve", "priya", body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("solo approve = %d", resp.StatusCode)
	}
	given := decodedEvents[events.ApprovalGivenPayload](t, svc, snap.ID, events.ApprovalGiven)
	if len(given) != 1 || given[0].Identity == nil || given[0].Identity.Grant != "solo" || given[0].Identity.Method != "dev" {
		t.Fatalf("approval = %+v", given)
	}

	// GET /api/me: solo is true for the solo user only, and absent otherwise.
	for user, want := range map[string]bool{"priya": true, "arjun": false} {
		resp := doJSON(t, http.MethodGet, srv.URL+"/api/me", user, "")
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var me map[string]any
		if err := json.Unmarshal(raw, &me); err != nil {
			t.Fatal(err)
		}
		v, present := me["solo"]
		if want && v != true {
			t.Errorf("%s: /api/me = %s, want solo:true", user, raw)
		}
		if !want && present {
			t.Errorf("%s: /api/me = %s, solo must be left out", user, raw)
		}
	}
}
