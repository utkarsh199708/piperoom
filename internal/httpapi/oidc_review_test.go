package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/netip"
	"strings"
	"testing"

	"piperoom/internal/httpapi"
	"piperoom/internal/testenv"
)

// A stale identities row must not beat a users.yaml sub: pin. kabir pins
// sub-kabir; an old row binds attacker-sub to kabir (from before the pin).
func TestOIDCOldIdentityRowDoesNotBeatASubPin(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	if err := e.idents.Bind(context.Background(), e.idp.Issuer, "attacker-sub", "kabir", "kabir@example.com"); err != nil {
		t.Fatal(err)
	}
	out := e.login(t, testenv.IDPUser{Sub: "attacker-sub", Email: "kabir@example.com", EmailVerified: true, Name: "Attacker"}, "/")
	wantLoginError(t, out.callback, "not_provisioned")
	if out.session != nil {
		t.Fatal("the stale row logged the attacker in as kabir")
	}
	if !strings.Contains(e.logs.String(), "now pinned to another sub") {
		t.Fatalf("the refusal reason is not in the log: %s", e.logs.String())
	}
	// The pinned sub still works.
	s := e.mustLogin(t, testenv.IDPUser{Sub: "sub-kabir", Name: "Kabir"})
	if got := e.me(t, s)["id"]; got != "kabir" {
		t.Fatalf("pinned login = %v", got)
	}
}

// Emails match as ASCII only: a Kelvin sign or a long s must not pass for k or s.
func TestOIDCEmailMatchIsASCIIOnly(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	for name, email := range map[string]string{
		"Kelvin sign": "Ksenia@example.com", // folds to k
		"long s":      "kſenia@example.com", // folds to s
	} {
		out := e.login(t, testenv.IDPUser{Sub: "idp-" + name, Email: email, EmailVerified: true, Name: "Lookalike"}, "/")
		wantLoginError(t, out.callback, "not_provisioned")
		if out.session != nil {
			t.Fatalf("%s: a lookalike email got a session", name)
		}
		if bound, err := e.idents.UserFor(context.Background(), e.idp.Issuer, "idp-"+name); err == nil {
			t.Fatalf("%s: bound to %q", name, bound)
		}
	}
	// Plain ASCII case differences still match.
	s := e.mustLogin(t, testenv.IDPUser{Sub: "idp-ksenia", Email: "KSenia@Example.COM", EmailVerified: true})
	if got := e.me(t, s)["id"]; got != "ksenia" {
		t.Fatalf("ASCII upper-case email: %v, want ksenia", got)
	}
}

// An ID token whose azp is not our client id is refused (OIDC Core 3.1.3.7).
func TestOIDCAZPMustBeOurClient(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	e.idp.AZP = "some-other-client"
	out := e.login(t, idpPriya, "/")
	wantLoginError(t, out.callback, "unauthenticated")
	if out.session != nil {
		t.Fatalf("session %v", out.session)
	}
	e.idp.AZP = e.idp.ClientID
	out = e.login(t, idpPriya, "/")
	if out.callback.StatusCode != http.StatusFound || out.session == nil {
		t.Fatalf("azp == client id: status %d", out.callback.StatusCode)
	}
}

// An IPv6 client is limited by its /64, not by address.
func TestOIDCLoginRateLimitIPv6Prefix(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{trusted: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}})
	for i := 1; i <= 300; i++ {
		resp := e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 2001:db8:0:1::1")
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("request %d = %d", i, resp.StatusCode)
		}
	}
	// Another address in the same /64: no fresh budget.
	resp := e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 2001:db8:0:1:ffff:ffff:ffff:ffff")
	wantLoginError(t, resp, "rate_limited")
	// The next /64 is somebody else.
	wantStatus(t, e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 2001:db8:0:2::1"), http.StatusFound)
	// An entry with a port is not an address: the chain falls back to the peer.
	wantStatus(t, e.do(t, http.MethodGet, "/auth/login", "", "X-Forwarded-For: 203.0.113.9:4444"), http.StatusFound)
}

// GET /api/people (docs/api.md): members only.
func TestOIDCPeople(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	priya := e.mustLogin(t, idpPriya)
	e.mustLogin(t, idpOncall)
	e.mustLogin(t, idpViewer)
	e.mustLogin(t, idpAuditor)
	// A member with an unverified email: listed, without the email.
	e.mustLogin(t, testenv.IDPUser{Sub: "idp-unverified", Email: "who@example.com", EmailVerified: false, Name: "Unverified", Groups: []string{"consumers"}})
	// A member who signs out is dropped.
	gone := e.mustLogin(t, testenv.IDPUser{Sub: "idp-gone", Email: "gone@example.com", EmailVerified: true, Name: "Gone", Groups: []string{"consumers"}})
	wantStatus(t, e.do(t, http.MethodPost, "/auth/logout", "", ck(gone)), http.StatusNoContent)

	resp := e.do(t, http.MethodGet, "/api/people", "", ck(priya))
	wantStatus(t, resp, http.StatusOK)
	var people []struct {
		ID    string  `json:"id"`
		Name  string  `json:"name"`
		Email *string `json:"email"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&people); err != nil {
		t.Fatal(err)
	}
	byID := map[string]int{}
	for i, p := range people {
		byID[p.ID] = i
	}
	// users.yaml users first, in file order, with no email.
	for i, id := range []string{"priya", "arjun", "meera", "kabir", "ksenia"} {
		if byID[id] != i {
			t.Fatalf("users.yaml user %s is at %d, want %d: %+v", id, byID[id], i, people)
		}
		if people[i].Email != nil {
			t.Errorf("users.yaml user %s has an email", id)
		}
	}
	oncallID := derivedID(e, "idp-oncall")
	if i, ok := byID[oncallID]; !ok || people[i].Name != "On Call" || people[i].Email == nil || *people[i].Email != "oncall@example.com" {
		t.Fatalf("the on-call member is missing or has no email: %+v", people)
	}
	if i, ok := byID[derivedID(e, "idp-unverified")]; !ok || people[i].Email != nil {
		t.Fatalf("the unverified-email member: %+v", people)
	}
	for name, sub := range map[string]string{"viewer": "idp-viewer", "auditor": "idp-auditor", "signed-out": "idp-gone"} {
		if _, ok := byID[derivedID(e, sub)]; ok {
			t.Errorf("the %s is listed", name)
		}
	}
	// The list is exactly what LookupPerson accepts: every entry can be picked.
	for _, p := range people {
		if _, err := e.svc.LookupPerson(context.Background(), p.ID); err != nil {
			t.Errorf("%s is listed but LookupPerson refuses: %v", p.ID, err)
		}
	}
	// A person with a verified email also shows no email key when it is empty.
	raw := e.do(t, http.MethodGet, "/api/people", "", ck(priya))
	var generic []map[string]any
	_ = json.NewDecoder(raw.Body).Decode(&generic)
	if _, has := generic[0]["email"]; has {
		t.Errorf("an empty email is sent as a key: %v", generic[0])
	}
}

func TestOIDCPeopleIsNotForViewers(t *testing.T) {
	e := newOIDCEnv(t, oidcOpts{})
	for name, u := range map[string]testenv.IDPUser{"viewer": idpViewer, "auditor-only": idpAuditor} {
		c := e.mustLogin(t, u)
		resp := e.do(t, http.MethodGet, "/api/people", "", ck(c))
		wantStatus(t, resp, http.StatusForbidden)
		if code, _ := errCode(t, resp); code != "viewer" {
			t.Errorf("%s: code %q, want viewer", name, code)
		}
	}
}

// In dev mode the list is the users.yaml users.
func TestDevPeople(t *testing.T) {
	srv, _ := newServer(t)
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/people", "priya", "")
	wantStatus(t, resp, http.StatusOK)
	var people []struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&people)
	if len(people) != 4 || people[0].ID != "arjun" {
		t.Fatalf("people = %+v", people)
	}
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/people", "", "")
	wantStatus(t, resp, http.StatusUnauthorized)
}

func derivedID(e *oidcEnv, sub string) string {
	return httpapi.DerivedUserID(e.idp.Issuer, sub)
}
