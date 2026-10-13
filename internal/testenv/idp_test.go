package testenv

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const testRedirect = "http://localhost:9999/auth/callback"
const testVerifier = "a-long-random-verifier-0123456789-0123456789-abcdefghij"

type idpClaims struct {
	Sub           string   `json:"sub"`
	Email         string   `json:"email"`
	EmailVerified bool     `json:"email_verified"`
	Name          string   `json:"name"`
	Nonce         string   `json:"nonce"`
	Groups        []string `json:"groups"`
	AMR           []string `json:"amr"`
	ACR           string   `json:"acr"`
}

// login runs discovery -> authorize -> token with go-oidc and oauth2, the way a
// real relying party does, and verifies the ID token.
func login(t *testing.T, f *FakeIdP, nonce string) (*oidc.IDToken, idpClaims, error) {
	t.Helper()
	ctx := context.Background()
	provider, err := oidc.NewProvider(ctx, f.Issuer)
	if err != nil {
		t.Fatalf("oidc.NewProvider (discovery): %v", err)
	}
	cfg := oauth2.Config{
		ClientID:     f.ClientID,
		ClientSecret: f.ClientSecret,
		Endpoint:     provider.Endpoint(),
		RedirectURL:  testRedirect,
		Scopes:       []string{oidc.ScopeOpenID, "email", "profile"},
	}
	// Build the authorize URL with oauth2 (checks our discovery endpoints), then
	// follow it without a browser.
	verifier := oauth2.GenerateVerifier()
	authURL := cfg.AuthCodeURL("state-1", oidc.Nonce(nonce), oauth2.S256ChallengeOption(verifier))
	if !strings.HasPrefix(authURL, f.Issuer+"/authorize?") {
		t.Fatalf("authorize endpoint = %s", authURL)
	}
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Get(authURL)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	_ = resp.Body.Close()
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || resp.StatusCode != http.StatusFound {
		t.Fatalf("authorize: status %d location %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if loc.Query().Get("state") != "state-1" {
		t.Fatalf("state = %q", loc.Query().Get("state"))
	}
	if got := f.LastChallengeMethod(); got != "S256" {
		t.Fatalf("LastChallengeMethod = %q, want S256", got)
	}
	tok, err := cfg.Exchange(ctx, loc.Query().Get("code"), oauth2.VerifierOption(verifier))
	if err != nil {
		return nil, idpClaims{}, err
	}
	raw, _ := tok.Extra("id_token").(string)
	if raw == "" {
		t.Fatal("token response has no id_token")
	}
	idt, err := provider.Verifier(&oidc.Config{ClientID: f.ClientID}).Verify(ctx, raw)
	if err != nil {
		return nil, idpClaims{}, err
	}
	var c idpClaims
	if err := idt.Claims(&c); err != nil {
		t.Fatalf("claims: %v", err)
	}
	return idt, c, nil
}

func TestFakeIdPRoundTrip(t *testing.T) {
	f := NewFakeIdP(t)
	f.SetUser(IDPUser{
		Sub: "u-42", Email: "priya@example.com", EmailVerified: true, Name: "Priya",
		Groups: []string{"dataeng-oncall", "security"}, AMR: []string{"pwd", "mfa"}, ACR: "urn:mfa",
	})
	idt, c, err := login(t, f, "n-123")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if idt.Issuer != f.Issuer || idt.Subject != "u-42" {
		t.Errorf("iss/sub = %q/%q", idt.Issuer, idt.Subject)
	}
	if !reflect.DeepEqual(idt.Audience, []string{f.ClientID}) {
		t.Errorf("aud = %v", idt.Audience)
	}
	want := idpClaims{
		Sub: "u-42", Email: "priya@example.com", EmailVerified: true, Name: "Priya", Nonce: "n-123",
		Groups: []string{"dataeng-oncall", "security"}, AMR: []string{"pwd", "mfa"}, ACR: "urn:mfa",
	}
	if !reflect.DeepEqual(c, want) {
		t.Errorf("claims = %+v\nwant     %+v", c, want)
	}
	if !idt.Expiry.After(time.Now()) {
		t.Errorf("expiry %v is not in the future", idt.Expiry)
	}
}

func TestFakeIdPDefaultUserAndOptionalClaims(t *testing.T) {
	f := NewFakeIdP(t)
	_, c, err := login(t, f, "")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if c.Sub != "fake-sub-1" || c.Email != "fake@example.com" || !c.EmailVerified || c.Nonce != "" || c.Groups != nil || c.AMR != nil || c.ACR != "" {
		t.Errorf("default claims = %+v", c)
	}

	f.SetUser(IDPUser{Sub: "s2", Email: "a@b.com", EmailVerified: false})
	_, c, err = login(t, f, "n")
	if err != nil || c.EmailVerified || c.Email != "a@b.com" {
		t.Errorf("unverified email: %+v, %v", c, err)
	}

	f.OmitNonce = true
	if _, c, err = login(t, f, "n"); err != nil || c.Nonce != "" {
		t.Errorf("OmitNonce: nonce %q, %v", c.Nonce, err)
	}
}

func TestFakeIdPGroupsClaimName(t *testing.T) {
	f := NewFakeIdP(t)
	f.GroupsClaim = "roles"
	f.SetUser(IDPUser{Sub: "s", Groups: []string{"g1"}})
	idt, c, err := login(t, f, "n")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := idt.Claims(&raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["groups"]; ok || c.Groups != nil {
		t.Errorf("groups claim present under the default name: %v", raw)
	}
	if got, _ := raw["roles"].([]any); len(got) != 1 || got[0] != "g1" {
		t.Errorf("roles claim = %v", raw["roles"])
	}
}

func TestFakeIdPFaults(t *testing.T) {
	cases := []struct {
		name    string
		fault   IDPFault
		wantErr string // substring of the go-oidc error; "" means verifies fine
		check   func(t *testing.T, c idpClaims)
	}{
		{name: "none", fault: FaultNone},
		{name: "bad signature", fault: FaultBadSignature, wantErr: "failed to verify signature"},
		{name: "wrong audience", fault: FaultWrongAudience, wantErr: "expected audience"},
		{name: "expired", fault: FaultExpired, wantErr: "token is expired"},
		{name: "missing groups", fault: FaultMissingGroups, check: func(t *testing.T, c idpClaims) {
			if c.Groups != nil {
				t.Errorf("groups = %v, want none", c.Groups)
			}
		}},
		{name: "wrong nonce", fault: FaultWrongNonce, check: func(t *testing.T, c idpClaims) {
			// A verifier does not know the nonce; the relying party compares it.
			if c.Nonce == "n-sent" || c.Nonce == "" {
				t.Errorf("nonce = %q, want a value different from the one sent", c.Nonce)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFakeIdP(t)
			f.SetUser(IDPUser{Sub: "s", Groups: []string{"g"}})
			f.SetFault(tc.fault)
			_, c, err := login(t, f, "n-sent")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("login: %v", err)
			}
			if tc.check != nil {
				tc.check(t, c)
			} else if c.Nonce != "n-sent" || !reflect.DeepEqual(c.Groups, []string{"g"}) {
				t.Errorf("claims = %+v", c)
			}
		})
	}
}

func TestFakeIdPWrongNonceWithNoNonceSent(t *testing.T) {
	f := NewFakeIdP(t)
	tok := f.IssueIDToken(IDPUser{Sub: "s"}, "", FaultWrongNonce)
	provider, err := oidc.NewProvider(context.Background(), f.Issuer)
	if err != nil {
		t.Fatal(err)
	}
	idt, err := provider.Verifier(&oidc.Config{ClientID: f.ClientID}).Verify(context.Background(), tok)
	if err != nil {
		t.Fatal(err)
	}
	if idt.Nonce == "" {
		t.Error("FaultWrongNonce with no nonce sent should still emit a (wrong) nonce")
	}
}

func TestFakeIdPCodeRules(t *testing.T) {
	f := NewFakeIdP(t)

	t.Run("code is single use", func(t *testing.T) {
		code, state := f.Authorize(t, testRedirect, "st", "n", PKCEChallenge(testVerifier))
		if state != "st" || code == "" {
			t.Fatalf("code %q state %q", code, state)
		}
		if _, err := f.Exchange(t, code, testRedirect, testVerifier); err != nil {
			t.Fatalf("first exchange: %v", err)
		}
		if _, err := f.Exchange(t, code, testRedirect, testVerifier); err == nil || !strings.Contains(err.Error(), "invalid_grant") {
			t.Fatalf("replay: %v, want invalid_grant", err)
		}
	})

	t.Run("redirect_uri must match", func(t *testing.T) {
		code, _ := f.Authorize(t, testRedirect, "st", "n", PKCEChallenge(testVerifier))
		if _, err := f.Exchange(t, code, "http://evil.test/cb", testVerifier); err == nil {
			t.Fatal("exchange with another redirect_uri succeeded")
		}
	})

	t.Run("PKCE", func(t *testing.T) {
		verifier := "a-long-random-verifier-0123456789-0123456789-abcdefghij"
		code, _ := f.Authorize(t, testRedirect, "st", "n", PKCEChallenge(verifier))
		if _, err := f.Exchange(t, code, testRedirect, "wrong"); err == nil || !strings.Contains(err.Error(), "invalid_grant") {
			t.Fatalf("wrong verifier: %v", err)
		}
		code, _ = f.Authorize(t, testRedirect, "st", "n", PKCEChallenge(verifier))
		if _, err := f.Exchange(t, code, testRedirect, verifier); err != nil {
			t.Fatalf("right verifier: %v", err)
		}
	})

	t.Run("RequirePKCE: a code issued without a challenge is refused", func(t *testing.T) {
		if !f.RequirePKCE {
			t.Fatal("RequirePKCE should default to true")
		}
		code, _ := f.Authorize(t, testRedirect, "st", "n", "")
		if got := f.LastChallengeMethod(); got != "" {
			t.Errorf("LastChallengeMethod = %q, want empty when none was sent", got)
		}
		if _, err := f.Exchange(t, code, testRedirect, ""); err == nil || !strings.Contains(err.Error(), "invalid_grant") {
			t.Fatalf("no code_challenge: %v, want invalid_grant", err)
		}
		f2 := NewFakeIdP(t)
		f2.RequirePKCE = false
		code, _ = f2.Authorize(t, testRedirect, "st", "n", "")
		if _, err := f2.Exchange(t, code, testRedirect, ""); err != nil {
			t.Fatalf("RequirePKCE=false: %v", err)
		}
	})

	t.Run("records a non-S256 challenge method", func(t *testing.T) {
		raw := f.Issuer + "/authorize?response_type=code&client_id=" + f.ClientID +
			"&redirect_uri=" + url.QueryEscape(testRedirect) + "&code_challenge=x&code_challenge_method=plain&state=s"
		noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, err := noRedirect.Get(raw)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if got := f.LastChallengeMethod(); got != "plain" {
			t.Errorf("LastChallengeMethod = %q, want plain", got)
		}
		if loc := resp.Header.Get("Location"); !strings.Contains(loc, "error=invalid_request") {
			t.Errorf("plain method should be refused: %s", loc)
		}
	})

	t.Run("basic auth credentials are form-decoded (RFC 6749 2.3.1)", func(t *testing.T) {
		f3 := NewFakeIdP(t)
		f3.ClientID = "app id/1"
		f3.ClientSecret = "s3cr+t &=é%"
		code, _ := f3.Authorize(t, testRedirect, "st", "n", PKCEChallenge(testVerifier))
		form := url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {testRedirect}, "code_verifier": {testVerifier}}
		do := func(user, pass string) int {
			req, _ := http.NewRequest(http.MethodPost, f3.Issuer+"/token", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.SetBasicAuth(user, pass)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			return resp.StatusCode
		}
		// Raw (undecoded) values are what a non-compliant client sends: refused.
		if got := do(f3.ClientID, f3.ClientSecret); got != http.StatusUnauthorized {
			t.Fatalf("raw credentials: status %d, want 401", got)
		}
		// Encoded as the RFC says: accepted (the code was not burned above).
		if got := do(url.QueryEscape(f3.ClientID), url.QueryEscape(f3.ClientSecret)); got != http.StatusOK {
			t.Fatalf("form-encoded credentials: status %d, want 200", got)
		}
	})

	t.Run("client authentication", func(t *testing.T) {
		code, _ := f.Authorize(t, testRedirect, "st", "n", PKCEChallenge(testVerifier))
		form := url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {testRedirect}, "code_verifier": {testVerifier}}
		req, _ := http.NewRequest(http.MethodPost, f.Issuer+"/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth(f.ClientID, "wrong")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("wrong secret: status %d, want 401", resp.StatusCode)
		}
		// HTTP Basic with the right secret works (and the code was not burned above).
		req, _ = http.NewRequest(http.MethodPost, f.Issuer+"/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth(f.ClientID, f.ClientSecret)
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("basic auth: status %d, want 200", resp.StatusCode)
		}
	})

	t.Run("authorize rejects unknown client and unregistered redirect", func(t *testing.T) {
		f2 := NewFakeIdP(t)
		f2.RedirectURIs = []string{testRedirect}
		for _, raw := range []string{
			f2.Issuer + "/authorize?response_type=code&client_id=nope&redirect_uri=" + url.QueryEscape(testRedirect),
			f2.Issuer + "/authorize?response_type=code&client_id=" + f2.ClientID + "&redirect_uri=" + url.QueryEscape("http://evil.test/cb"),
		} {
			resp, err := http.Get(raw)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s: status %d, want 400", raw, resp.StatusCode)
			}
		}
	})
}
