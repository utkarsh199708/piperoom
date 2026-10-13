package testenv

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"
)

// FakeIdP is an in-process OpenID Connect provider for auth tests. It serves
//
//	GET  /.well-known/openid-configuration   discovery
//	GET  /jwks                               the RS256 public key
//	GET  /authorize                          authorization code flow (302 back to redirect_uri)
//	POST /token                              code -> ID token (RS256), single-use codes, optional PKCE S256
//
// There is no login page: /authorize signs in whichever user was set with
// SetUser and redirects at once. Use Authorize to drive it without a browser.
//
// Claims are configured with SetUser. Faults for the failure paths of a relying
// party are set with SetFault (or call IssueIDToken with a fault directly):
// a bad signature, a wrong aud, an expired token, a missing groups claim and a
// wrong nonce.
//
// Nothing here talks to the network beyond the loopback httptest server.
type FakeIdP struct {
	// Issuer is the base URL of the server, e.g. http://127.0.0.1:41233. It is
	// the `iss` claim and what discovery reports. Set once the server starts.
	Issuer string
	// ClientID and ClientSecret are what /token expects (Basic or form post).
	ClientID     string
	ClientSecret string
	// RedirectURIs, when non-empty, limits the redirect_uri that /authorize accepts.
	RedirectURIs []string
	// GroupsClaim is the claim name that carries the user's groups. Default "groups".
	GroupsClaim string
	// Lifetime is how long an ID token is valid. Default 1 hour.
	Lifetime time.Duration
	// OmitNonce stops the IdP echoing the nonce (a non-compliant IdP).
	OmitNonce bool
	// Now is the clock. Default time.Now.
	Now func() time.Time

	srv *httptest.Server

	mu    sync.Mutex
	user  IDPUser
	fault IDPFault
	codes map[string]idpCode
}

// IDPUser is the identity the fake IdP signs in. The zero values of Email,
// Name, Groups, AMR and ACR leave that claim out of the token.
type IDPUser struct {
	Sub           string
	Email         string
	EmailVerified bool
	Name          string
	Groups        []string // nil: no groups claim at all
	AMR           []string
	ACR           string
}

// IDPFault selects a deliberately broken ID token.
type IDPFault string

// The faults. FaultNone issues a correct token.
const (
	FaultNone          IDPFault = ""
	FaultBadSignature  IDPFault = "bad_signature"  // signed by another key under the same kid
	FaultWrongAudience IDPFault = "wrong_audience" // aud is not ClientID
	FaultExpired       IDPFault = "expired"        // exp (and iat) in the past
	FaultMissingGroups IDPFault = "missing_groups" // no groups claim, whatever the user has
	FaultWrongNonce    IDPFault = "wrong_nonce"    // nonce differs from the one sent to /authorize
)

// IDPTokenResponse is the JSON body of a successful /token call.
type IDPTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	IDToken     string `json:"id_token"`
}

type idpCode struct {
	user        IDPUser
	nonce       string
	redirectURI string
	challenge   string // PKCE S256 challenge, "" when none
}

const idpKeyID = "fake-idp-key-1"

var (
	idpKeysOnce sync.Once
	idpKey      *rsa.PrivateKey // the key published in the JWKS
	idpRogue    *rsa.PrivateKey // signs FaultBadSignature tokens under the same kid
)

func idpKeys() (*rsa.PrivateKey, *rsa.PrivateKey) {
	idpKeysOnce.Do(func() {
		var err error
		if idpKey, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
		if idpRogue, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
	})
	return idpKey, idpRogue
}

// NewFakeIdP starts a fake IdP and closes it when the test ends. The default
// user is sub "fake-sub-1", email fake@example.com (verified), name "Fake
// User", no groups. ClientID is "piperoom-test" and ClientSecret "test-secret".
func NewFakeIdP(t testing.TB) *FakeIdP {
	t.Helper()
	idpKeys()
	f := &FakeIdP{
		ClientID:     "piperoom-test",
		ClientSecret: "test-secret",
		GroupsClaim:  "groups",
		Lifetime:     time.Hour,
		Now:          time.Now,
		codes:        map[string]idpCode{},
		user: IDPUser{
			Sub: "fake-sub-1", Email: "fake@example.com", EmailVerified: true, Name: "Fake User",
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", f.handleDiscovery)
	mux.HandleFunc("GET /jwks", f.handleJWKS)
	mux.HandleFunc("GET /authorize", f.handleAuthorize)
	mux.HandleFunc("POST /token", f.handleToken)
	f.srv = httptest.NewServer(mux)
	f.Issuer = f.srv.URL
	t.Cleanup(f.srv.Close)
	return f
}

// Server returns the underlying httptest server.
func (f *FakeIdP) Server() *httptest.Server { return f.srv }

// SetUser sets the identity that the next /authorize signs in.
func (f *FakeIdP) SetUser(u IDPUser) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.user = u
}

// SetFault makes every ID token issued by /token from now on carry the fault.
func (f *FakeIdP) SetFault(fault IDPFault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = fault
}

// AuthorizeURL builds the /authorize URL for a relying party. challenge is a
// PKCE S256 code_challenge, or "" for none.
func (f *FakeIdP) AuthorizeURL(redirectURI, state, nonce, challenge string) string {
	q := url.Values{
		"response_type": {"code"},
		"client_id":     {f.ClientID},
		"redirect_uri":  {redirectURI},
		"scope":         {"openid email profile"},
		"state":         {state},
	}
	if nonce != "" {
		q.Set("nonce", nonce)
	}
	if challenge != "" {
		q.Set("code_challenge", challenge)
		q.Set("code_challenge_method", "S256")
	}
	return f.Issuer + "/authorize?" + q.Encode()
}

// PKCEChallenge returns the S256 code_challenge for a verifier.
func PKCEChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// Authorize calls /authorize without a browser and returns the code and state
// from the redirect. It does not follow the redirect to redirectURI.
func (f *FakeIdP) Authorize(t testing.TB, redirectURI, state, nonce, challenge string) (code, gotState string) {
	t.Helper()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get(f.AuthorizeURL(redirectURI, state, nonce, challenge))
	if err != nil {
		t.Fatalf("fake idp: authorize: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("fake idp: authorize: status %d, want 302", resp.StatusCode)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatalf("fake idp: authorize: bad Location: %v", err)
	}
	return loc.Query().Get("code"), loc.Query().Get("state")
}

// Exchange posts the code to /token with the client secret (form post). It
// returns an error carrying the OAuth error code when /token refuses.
func (f *FakeIdP) Exchange(t testing.TB, code, redirectURI, verifier string) (IDPTokenResponse, error) {
	t.Helper()
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirectURI},
		"client_id":     {f.ClientID},
		"client_secret": {f.ClientSecret},
	}
	if verifier != "" {
		form.Set("code_verifier", verifier)
	}
	resp, err := http.PostForm(f.Issuer+"/token", form)
	if err != nil {
		t.Fatalf("fake idp: token: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return IDPTokenResponse{}, fmt.Errorf("token endpoint: status %d: %s", resp.StatusCode, e.Error)
	}
	var tr IDPTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		t.Fatalf("fake idp: token: decode: %v", err)
	}
	return tr, nil
}

// IssueIDToken signs an ID token for u with the given fault. It is what
// /token uses; call it directly to build a token without the HTTP flow.
func (f *FakeIdP) IssueIDToken(u IDPUser, nonce string, fault IDPFault) string {
	now := f.now()
	iat, exp := now, now.Add(f.lifetime())
	aud := f.ClientID
	switch fault {
	case FaultExpired:
		iat, exp = now.Add(-2*time.Hour), now.Add(-time.Hour)
	case FaultWrongAudience:
		aud = "some-other-client"
	case FaultWrongNonce:
		nonce += "-wrong"
		if nonce == "-wrong" {
			nonce = "wrong-nonce"
		}
	}
	claims := map[string]any{
		"iss": f.Issuer,
		"sub": u.Sub,
		"aud": aud,
		"iat": iat.Unix(),
		"exp": exp.Unix(),
	}
	if nonce != "" && !f.OmitNonce {
		claims["nonce"] = nonce
	}
	if u.Email != "" {
		claims["email"] = u.Email
		claims["email_verified"] = u.EmailVerified
	}
	if u.Name != "" {
		claims["name"] = u.Name
	}
	if u.Groups != nil && fault != FaultMissingGroups {
		claims[f.groupsClaim()] = u.Groups
	}
	if len(u.AMR) > 0 {
		claims["amr"] = u.AMR
	}
	if u.ACR != "" {
		claims["acr"] = u.ACR
	}
	key, rogue := idpKeys()
	if fault == FaultBadSignature {
		key = rogue
	}
	return signRS256(key, claims)
}

func (f *FakeIdP) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *FakeIdP) lifetime() time.Duration {
	if f.Lifetime > 0 {
		return f.Lifetime
	}
	return time.Hour
}

func (f *FakeIdP) groupsClaim() string {
	if f.GroupsClaim != "" {
		return f.GroupsClaim
	}
	return "groups"
}

func signRS256(key *rsa.PrivateKey, claims map[string]any) string {
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": idpKeyID})
	payload, _ := json.Marshal(claims)
	enc := base64.RawURLEncoding
	signing := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		panic(err) // cannot happen with a valid key
	}
	return signing + "." + enc.EncodeToString(sig)
}

func (f *FakeIdP) handleDiscovery(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer":                                f.Issuer,
		"authorization_endpoint":                f.Issuer + "/authorize",
		"token_endpoint":                        f.Issuer + "/token",
		"jwks_uri":                              f.Issuer + "/jwks",
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"scopes_supported":                      []string{"openid", "email", "profile"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_basic", "client_secret_post"},
		"code_challenge_methods_supported":      []string{"S256"},
		"claims_supported":                      []string{"sub", "iss", "aud", "exp", "iat", "nonce", "email", "email_verified", "name", f.groupsClaim(), "amr", "acr"},
	})
}

func (f *FakeIdP) handleJWKS(w http.ResponseWriter, _ *http.Request) {
	key, _ := idpKeys()
	enc := base64.RawURLEncoding
	writeJSON(w, http.StatusOK, map[string]any{"keys": []map[string]string{{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": idpKeyID,
		"n":   enc.EncodeToString(key.N.Bytes()),
		"e":   enc.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}})
}

func (f *FakeIdP) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// Errors about client_id or redirect_uri are shown, never redirected (RFC 6749 4.1.2.1).
	if q.Get("client_id") != f.ClientID {
		http.Error(w, "unknown client_id", http.StatusBadRequest)
		return
	}
	redirect := q.Get("redirect_uri")
	if redirect == "" || (len(f.RedirectURIs) > 0 && !slices.Contains(f.RedirectURIs, redirect)) {
		http.Error(w, "bad redirect_uri", http.StatusBadRequest)
		return
	}
	target, err := url.Parse(redirect)
	if err != nil {
		http.Error(w, "bad redirect_uri", http.StatusBadRequest)
		return
	}
	out := target.Query()
	if q.Get("state") != "" {
		out.Set("state", q.Get("state"))
	}
	if q.Get("response_type") != "code" {
		out.Set("error", "unsupported_response_type")
		target.RawQuery = out.Encode()
		http.Redirect(w, r, target.String(), http.StatusFound)
		return
	}
	rec := idpCode{nonce: q.Get("nonce"), redirectURI: redirect}
	if ch := q.Get("code_challenge"); ch != "" {
		if q.Get("code_challenge_method") != "S256" {
			out.Set("error", "invalid_request")
			target.RawQuery = out.Encode()
			http.Redirect(w, r, target.String(), http.StatusFound)
			return
		}
		rec.challenge = ch
	}
	code := randomHex(16)
	f.mu.Lock()
	rec.user = f.user
	f.codes[code] = rec
	f.mu.Unlock()
	out.Set("code", code)
	target.RawQuery = out.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
}

func (f *FakeIdP) handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	id, secret, basic := r.BasicAuth()
	if !basic {
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	if id != f.ClientID || subtle.ConstantTimeCompare([]byte(secret), []byte(f.ClientSecret)) != 1 {
		oauthError(w, http.StatusUnauthorized, "invalid_client")
		return
	}
	if r.PostForm.Get("grant_type") != "authorization_code" {
		oauthError(w, http.StatusBadRequest, "unsupported_grant_type")
		return
	}
	f.mu.Lock()
	rec, ok := f.codes[r.PostForm.Get("code")]
	delete(f.codes, r.PostForm.Get("code")) // single use: a replay finds nothing
	fault := f.fault
	f.mu.Unlock()
	if !ok || rec.redirectURI != r.PostForm.Get("redirect_uri") {
		oauthError(w, http.StatusBadRequest, "invalid_grant")
		return
	}
	if rec.challenge != "" && subtle.ConstantTimeCompare([]byte(PKCEChallenge(r.PostForm.Get("code_verifier"))), []byte(rec.challenge)) != 1 {
		oauthError(w, http.StatusBadRequest, "invalid_grant")
		return
	}
	writeJSON(w, http.StatusOK, IDPTokenResponse{
		AccessToken: "at-" + randomHex(8),
		TokenType:   "Bearer",
		ExpiresIn:   int(f.lifetime().Seconds()),
		IDToken:     f.IssueIDToken(rec.user, rec.nonce, fault),
	})
}

func oauthError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
