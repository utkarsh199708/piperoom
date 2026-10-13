package auth_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/testenv"
)

func TestSafeReturn(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", "/"},
		{"/", "/"},
		{"/rooms/r_x", "/rooms/r_x"},
		{"/rooms/r_x?tab=trace&a=b", "/rooms/r_x?tab=trace&a=b"},
		{"/rooms/r_x#frag", "/rooms/r_x"}, // fragment dropped
		{"/a%20b", "/a%20b"},
		// Open-redirect attempts. Each becomes "/".
		{"//evil.com", "/"},
		{"//evil.com/path", "/"},
		{"https://evil.com", "/"},
		{"http://evil.com/x", "/"},
		{"javascript:alert(1)", "/"},
		{"mailto:a@b.c", "/"},
		{`/\evil.com`, "/"},
		{`/\/evil.com`, "/"},
		{"/%5Cevil.com", "/"},           // decodes to /\evil.com
		{"/%2F%2Fevil.com", "/"},        // decodes to //evil.com
		{"/%2f/evil.com", "/"},          // decodes to //evil.com
		{"evil.com", "/"},               // no leading slash
		{"?x=1", "/"},                   // no path
		{"/x\nSet-Cookie: a=b", "/"},    // raw control character
		{"/x%0d%0aSet-Cookie:a=b", "/"}, // encoded control character in the path
		{"/x\x00", "/"},
		{"/x\x7f", "/"},
		{"/\t/evil.com", "/"},
		{"//", "/"},
		{"/" + strings.Repeat("a", 2100), "/"}, // over 2 KB
		{"%2F%2Fevil.com", "/"},                // no leading slash before decoding
	}
	for _, c := range cases {
		if got := auth.SafeReturn(c.in); got != c.want {
			t.Errorf("SafeReturn(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	// A value right at the limit still works.
	ok := "/" + strings.Repeat("a", 2047)
	if got := auth.SafeReturn(ok); got != ok {
		t.Errorf("a 2048-byte path was refused")
	}
	// The result never has a scheme or host and always starts with a single slash.
	for _, c := range cases {
		got := auth.SafeReturn(c.in)
		u, err := url.Parse(got)
		if err != nil || u.Host != "" || u.Scheme != "" || !strings.HasPrefix(got, "/") || strings.HasPrefix(got, "//") {
			t.Errorf("SafeReturn(%q) = %q is not a plain site path", c.in, got)
		}
	}
}

func TestLoginStatesSingleUseAndExpiry(t *testing.T) {
	m := auth.NewMemLoginStates()
	ctx := context.Background()
	now := time.Now()
	ls := auth.LoginState{ID: "a", State: "s", Nonce: "n", Verifier: "v", ReturnPath: "/x", ExpiresAt: now.Add(auth.LoginStateTTL)}
	if err := m.Create(ctx, ls, "", now); err != nil {
		t.Fatal(err)
	}
	got, err := m.Take(ctx, "a", now)
	if err != nil || got != ls {
		t.Fatalf("Take = %+v, %v", got, err)
	}
	if _, err := m.Take(ctx, "a", now); !errors.Is(err, auth.ErrLoginStateNotFound) {
		t.Fatalf("second Take = %v, want ErrLoginStateNotFound (single use)", err)
	}
	// Expired rows are not returned, and are pruned by the next Create.
	old := auth.LoginState{ID: "old", ExpiresAt: now.Add(-time.Second)}
	_ = m.Create(ctx, old, "", now.Add(-time.Minute))
	if _, err := m.Take(ctx, "old", now); !errors.Is(err, auth.ErrLoginStateNotFound) {
		t.Fatalf("expired Take = %v", err)
	}
	_ = m.Create(ctx, auth.LoginState{ID: "e1", ExpiresAt: now.Add(-time.Second)}, "", now.Add(-time.Minute))
	_ = m.Create(ctx, auth.LoginState{ID: "n1", ExpiresAt: now.Add(time.Minute)}, "", now)
	if m.Len() != 1 {
		t.Fatalf("rows after prune = %d, want 1", m.Len())
	}
	// A new Create replaces the old row of the same cookie.
	_ = m.Create(ctx, auth.LoginState{ID: "n2", ExpiresAt: now.Add(time.Minute)}, "n1", now)
	if _, err := m.Take(ctx, "n1", now); !errors.Is(err, auth.ErrLoginStateNotFound) {
		t.Fatalf("replaced row still there: %v", err)
	}
}

func TestIdentitiesBindRules(t *testing.T) {
	m := auth.NewMemIdentities()
	ctx := context.Background()
	if _, err := m.UserFor(ctx, "iss", "s1"); !errors.Is(err, auth.ErrIdentityNotFound) {
		t.Fatalf("empty store: %v", err)
	}
	if err := m.Bind(ctx, "iss", "s1", "priya", "p@x.com"); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(ctx, "iss", "s1", "priya", "p@x.com"); err != nil {
		t.Fatalf("binding the same pair twice: %v", err)
	}
	if err := m.Bind(ctx, "iss", "s2", "priya", "p@x.com"); !errors.Is(err, auth.ErrIdentityConflict) {
		t.Fatalf("second sub for the same user: %v, want ErrIdentityConflict", err)
	}
	if err := m.Bind(ctx, "iss", "s1", "arjun", "a@x.com"); !errors.Is(err, auth.ErrIdentityConflict) {
		t.Fatalf("a bound pair for another user: %v, want ErrIdentityConflict", err)
	}
	if u, err := m.UserFor(ctx, "iss", "s1"); err != nil || u != "priya" {
		t.Fatalf("UserFor = %q, %v", u, err)
	}
	if n, _ := m.Unbind(ctx, "priya"); n != 1 {
		t.Fatalf("Unbind = %d", n)
	}
	if n, _ := m.Unbind(ctx, "priya"); n != 0 {
		t.Fatalf("second Unbind = %d, want 0", n)
	}
	if err := m.Bind(ctx, "iss", "s2", "priya", "p@x.com"); err != nil {
		t.Fatalf("bind after unbind: %v", err)
	}
}

func TestOIDCConfigNeverPrintsTheSecret(t *testing.T) {
	c := auth.OIDCConfig{Issuer: "https://idp", ClientID: "cid", ClientSecret: "s3cr3t-value", RedirectURL: "https://app/auth/callback"}
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), fmt.Sprintf("%s", c)} {
		if strings.Contains(s, "s3cr3t-value") {
			t.Fatalf("config printed the secret: %s", s)
		}
	}
}

func newRP(t *testing.T, f *testenv.FakeIdP, groupsClaim string) *auth.OIDC {
	t.Helper()
	o, err := auth.NewOIDC(auth.OIDCConfig{
		Issuer: f.Issuer, ClientID: f.ClientID, ClientSecret: f.ClientSecret,
		RedirectURL: "https://piperoom.test/auth/callback",
		Scopes:      []string{"openid", "email", "profile"},
		GroupsClaim: groupsClaim,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// authorize drives the redirect URL the relying party built through the fake
// IdP and returns the code.
func authorize(t *testing.T, authURL string) (code, state string) {
	t.Helper()
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Get(authURL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || resp.StatusCode != http.StatusFound {
		t.Fatalf("authorize: status %d, location %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	return loc.Query().Get("code"), loc.Query().Get("state")
}

func TestOIDCNotReadyUntilDiscovery(t *testing.T) {
	// An issuer that answers 500 to discovery.
	var calls atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer bad.Close()
	o, err := auth.NewOIDC(auth.OIDCConfig{
		Issuer: bad.URL, ClientID: "c", RedirectURL: "https://x/auth/callback", Scopes: []string{"openid"},
		RetryMin: 5 * time.Millisecond, RetryMax: 10 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if o.Ready() {
		t.Fatal("ready before discovery")
	}
	if _, err := o.AuthCodeURL("s", "n", auth.NewVerifier()); !errors.Is(err, auth.ErrIDPUnavailable) {
		t.Fatalf("AuthCodeURL before discovery = %v, want ErrIDPUnavailable", err)
	}
	if _, err := o.Exchange(context.Background(), "c", "v", "n"); !errors.Is(err, auth.ErrIDPUnavailable) {
		t.Fatalf("Exchange before discovery = %v, want ErrIDPUnavailable", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() < 3 {
		t.Fatalf("discovery was retried %d times, want at least 3", calls.Load())
	}
	if o.Ready() {
		t.Fatal("ready although discovery keeps failing")
	}
}

func TestOIDCDiscoveryRecovers(t *testing.T) {
	f := testenv.NewFakeIdP(t)
	// The relying party talks to the fake IdP through a client that fails until
	// switched on, like an IdP that is down at start-up.
	var ready atomic.Bool
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !ready.Load() {
			return nil, errors.New("network down")
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	o, err := auth.NewOIDC(auth.OIDCConfig{
		Issuer: f.Issuer, ClientID: f.ClientID, ClientSecret: f.ClientSecret,
		RedirectURL: "https://piperoom.test/auth/callback", Scopes: []string{"openid"},
		HTTPClient: &http.Client{Transport: rt, Timeout: time.Second},
		RetryMin:   5 * time.Millisecond, RetryMax: 10 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)
	time.Sleep(40 * time.Millisecond)
	if o.Ready() {
		t.Fatal("ready while the network is down")
	}
	ready.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for !o.Ready() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !o.Ready() {
		t.Fatal("discovery never recovered")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOIDCExchangeChecks(t *testing.T) {
	f := testenv.NewFakeIdP(t)
	f.SetUser(testenv.IDPUser{
		Sub: "sub-1", Email: "Priya@Example.com", EmailVerified: true, Name: "  Priya\x07 N ",
		Groups: []string{"g1", "g2"}, AMR: []string{"pwd", "mfa"}, ACR: "urn:mfa",
	})
	o := newRP(t, f, "groups")
	if err := o.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	flow := func(fault testenv.IDPFault, nonce string) (auth.Claims, error) {
		f.SetFault(fault)
		verifier := auth.NewVerifier()
		u, err := o.AuthCodeURL("state-1", "nonce-1", verifier)
		if err != nil {
			t.Fatal(err)
		}
		code, _ := authorize(t, u)
		return o.Exchange(context.Background(), code, verifier, nonce)
	}

	t.Run("good", func(t *testing.T) {
		c, err := flow(testenv.FaultNone, "nonce-1")
		if err != nil {
			t.Fatal(err)
		}
		if c.Subject != "sub-1" || c.Issuer != f.Issuer || c.Email != "Priya@Example.com" || !c.EmailVerified {
			t.Errorf("claims = %+v", c)
		}
		if c.Name != "Priya N" { // control character dropped, trimmed
			t.Errorf("name = %q", c.Name)
		}
		if len(c.Groups) != 2 || c.GroupsMissing || len(c.AMR) != 2 || c.ACR != "urn:mfa" {
			t.Errorf("groups/amr/acr = %+v", c)
		}
	})
	t.Run("PKCE S256 is sent", func(t *testing.T) {
		if _, err := flow(testenv.FaultNone, "nonce-1"); err != nil {
			t.Fatal(err)
		}
		if got := f.LastChallengeMethod(); got != "S256" {
			t.Fatalf("code_challenge_method = %q, want S256", got)
		}
	})
	t.Run("wrong verifier is refused by the IdP", func(t *testing.T) {
		f.SetFault(testenv.FaultNone)
		u, _ := o.AuthCodeURL("s", "nonce-1", auth.NewVerifier())
		code, _ := authorize(t, u)
		_, err := o.Exchange(context.Background(), code, auth.NewVerifier(), "nonce-1")
		if !errors.Is(err, auth.ErrLoginRefused) {
			t.Fatalf("err = %v, want ErrLoginRefused", err)
		}
	})
	for _, c := range []struct {
		name  string
		fault testenv.IDPFault
		nonce string
	}{
		{"nonce differs (IdP side)", testenv.FaultWrongNonce, "nonce-1"},
		{"nonce differs (our side)", testenv.FaultNone, "other-nonce"},
		{"empty nonce", testenv.FaultNone, ""},
		{"bad signature", testenv.FaultBadSignature, "nonce-1"},
		{"wrong audience", testenv.FaultWrongAudience, "nonce-1"},
		{"expired", testenv.FaultExpired, "nonce-1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := flow(c.fault, c.nonce)
			if !errors.Is(err, auth.ErrLoginRefused) {
				t.Fatalf("err = %v, want ErrLoginRefused", err)
			}
		})
	}
	t.Run("missing groups claim", func(t *testing.T) {
		c, err := flow(testenv.FaultMissingGroups, "nonce-1")
		if err != nil {
			t.Fatal(err)
		}
		if !c.GroupsMissing || len(c.Groups) != 0 {
			t.Fatalf("claims = %+v, want groups_missing", c)
		}
	})
	t.Run("a replayed code is refused", func(t *testing.T) {
		f.SetFault(testenv.FaultNone)
		verifier := auth.NewVerifier()
		u, _ := o.AuthCodeURL("s", "nonce-1", verifier)
		code, _ := authorize(t, u)
		if _, err := o.Exchange(context.Background(), code, verifier, "nonce-1"); err != nil {
			t.Fatal(err)
		}
		if _, err := o.Exchange(context.Background(), code, verifier, "nonce-1"); !errors.Is(err, auth.ErrLoginRefused) {
			t.Fatalf("replay err = %v, want ErrLoginRefused", err)
		}
	})
}

func TestOIDCGroupsClaimOff(t *testing.T) {
	f := testenv.NewFakeIdP(t)
	f.SetUser(testenv.IDPUser{Sub: "s", Groups: []string{"g1"}})
	o := newRP(t, f, "") // no group rights
	if err := o.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier()
	u, _ := o.AuthCodeURL("s", "n", verifier)
	code, _ := authorize(t, u)
	c, err := o.Exchange(context.Background(), code, verifier, "n")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Groups) != 0 || c.GroupsMissing {
		t.Fatalf("claims = %+v: with no groups claim configured, groups stay empty and not 'missing'", c)
	}
}

func TestOIDCPublicClientSendsNoSecret(t *testing.T) {
	f := testenv.NewFakeIdP(t)
	f.ClientSecret = "" // the IdP expects no secret from a public client
	o, err := auth.NewOIDC(auth.OIDCConfig{
		Issuer: f.Issuer, ClientID: f.ClientID, RedirectURL: "https://piperoom.test/auth/callback",
		Scopes: []string{"openid"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier()
	u, _ := o.AuthCodeURL("s", "n", verifier)
	code, _ := authorize(t, u)
	if _, err := o.Exchange(context.Background(), code, verifier, "n"); err != nil {
		t.Fatalf("public client exchange: %v", err)
	}
}

func TestNewOIDCValidation(t *testing.T) {
	good := auth.OIDCConfig{Issuer: "https://i", ClientID: "c", RedirectURL: "https://x/auth/callback", Scopes: []string{"openid"}}
	if _, err := auth.NewOIDC(good, nil); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*auth.OIDCConfig){
		"no issuer":    func(c *auth.OIDCConfig) { c.Issuer = "" },
		"no client id": func(c *auth.OIDCConfig) { c.ClientID = "" },
		"no redirect":  func(c *auth.OIDCConfig) { c.RedirectURL = "" },
		"no openid":    func(c *auth.OIDCConfig) { c.Scopes = []string{"email"} },
	} {
		c := good
		mut(&c)
		if _, err := auth.NewOIDC(c, nil); err == nil {
			t.Errorf("%s: NewOIDC accepted the config", name)
		}
	}
}

func TestParseTrustedProxies(t *testing.T) {
	got, err := auth.ParseTrustedProxies("10.0.0.0/8, 192.168.1.5 ,fd00::/8,")
	if err != nil || len(got) != 3 {
		t.Fatalf("got %v, %v", got, err)
	}
	if got[1].String() != "192.168.1.5/32" {
		t.Errorf("bare address = %s, want a /32", got[1])
	}
	if _, err := auth.ParseTrustedProxies("not-a-cidr"); err == nil {
		t.Error("a bad entry was accepted")
	}
	if got, err := auth.ParseTrustedProxies(""); err != nil || got != nil {
		t.Errorf("empty list = %v, %v", got, err)
	}
}
