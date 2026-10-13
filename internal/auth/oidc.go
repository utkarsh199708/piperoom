package auth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// OIDC login against an identity provider (docs/design/auth.md §1.1 and §1.2):
// authorization code flow with PKCE (S256) and a nonce, run by this server.
// Provider discovery is retried in the background and is not fatal at start:
// until it succeeds new logins answer 503 idp_unavailable, while sessions that
// already exist keep working because they never touch the IdP.

// Errors returned by OIDC.
var (
	// ErrIDPUnavailable: discovery has not succeeded yet, or the IdP could not
	// be reached while exchanging the code. The HTTP layer answers 503.
	ErrIDPUnavailable = errors.New("auth: identity provider unavailable")
	// ErrLoginRefused: the IdP or the token failed a check (code, signature,
	// issuer, audience, expiry, nonce). The HTTP layer answers 401. The wrapped
	// error says which check, for the log; it never holds a token.
	ErrLoginRefused = errors.New("auth: login refused")
)

// Limits on what an ID token may put into a session, so one hostile or
// misconfigured IdP cannot fill a row.
const (
	maxGroups     = 1000
	maxGroupBytes = 512
	maxClaimBytes = 1024
)

// OIDCConfig configures the relying party.
type OIDCConfig struct {
	Issuer       string
	ClientID     string
	ClientSecret string // never logged; empty only for a public client
	RedirectURL  string // PIPEROOM_PUBLIC_URL + /auth/callback
	Scopes       []string
	// GroupsClaim is the ID token claim that carries the groups. "" means no
	// group rights: the claim is not read and groups_missing stays false.
	GroupsClaim string

	// HTTPClient is used for discovery, the key set and the token call. The
	// default has a 10 s timeout.
	HTTPClient *http.Client
	// Now is the clock for token expiry. Default time.Now.
	Now func() time.Time
	// RetryMin and RetryMax bound the discovery back-off. Defaults 1 s and 30 s.
	RetryMin, RetryMax time.Duration
}

// String redacts the secret, so a stray %v or %+v cannot log it.
func (c OIDCConfig) String() string {
	secret := "unset"
	if c.ClientSecret != "" {
		secret = "REDACTED"
	}
	return fmt.Sprintf("OIDCConfig{Issuer:%s ClientID:%s ClientSecret:%s RedirectURL:%s}", c.Issuer, c.ClientID, secret, c.RedirectURL)
}

// GoString makes %#v redact too.
func (c OIDCConfig) GoString() string { return c.String() }

// OIDC is the relying party. It is safe for concurrent use.
type OIDC struct {
	cfg  OIDCConfig
	log  *slog.Logger
	http *http.Client

	mu       sync.RWMutex
	oauth    *oauth2.Config
	verifier *oidc.IDTokenVerifier
}

// NewOIDC checks cfg and returns a relying party that is not ready until
// Discover or Start has succeeded. It does no network I/O.
func NewOIDC(cfg OIDCConfig, log *slog.Logger) (*OIDC, error) {
	if log == nil {
		log = slog.Default()
	}
	if strings.TrimSpace(cfg.Issuer) == "" || cfg.ClientID == "" || cfg.RedirectURL == "" {
		return nil, errors.New("auth: oidc needs an issuer, a client id and a redirect URL")
	}
	hasOpenID := false
	for _, s := range cfg.Scopes {
		if s == oidc.ScopeOpenID {
			hasOpenID = true
		}
	}
	if !hasOpenID {
		return nil, errors.New("auth: PIPEROOM_OIDC_SCOPES must include openid")
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = time.Second
	}
	if cfg.RetryMax < cfg.RetryMin {
		cfg.RetryMax = 30 * time.Second
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &OIDC{cfg: cfg, log: log, http: client}, nil
}

// Ready reports whether discovery has succeeded. It never goes back to false:
// the key set refreshes itself, and a later IdP outage shows up as
// ErrIDPUnavailable from Exchange.
func (o *OIDC) Ready() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.oauth != nil
}

// Discover runs one discovery attempt. On success the relying party is ready.
func (o *OIDC) Discover(ctx context.Context) error {
	ctx = oidc.ClientContext(ctx, o.http)
	provider, err := oidc.NewProvider(ctx, o.cfg.Issuer)
	if err != nil {
		return fmt.Errorf("auth: oidc discovery for %s: %w", o.cfg.Issuer, err)
	}
	ep := provider.Endpoint()
	if o.cfg.ClientSecret == "" {
		// A public client has no secret to put in a Basic header.
		ep.AuthStyle = oauth2.AuthStyleInParams
	}
	conf := &oauth2.Config{
		ClientID:     o.cfg.ClientID,
		ClientSecret: o.cfg.ClientSecret,
		Endpoint:     ep,
		RedirectURL:  o.cfg.RedirectURL,
		Scopes:       o.cfg.Scopes,
	}
	v := provider.Verifier(&oidc.Config{ClientID: o.cfg.ClientID, Now: o.cfg.Now})
	o.mu.Lock()
	o.oauth, o.verifier = conf, v
	o.mu.Unlock()
	return nil
}

// Start runs discovery in the background until it succeeds or ctx ends,
// backing off between attempts. It returns at once.
func (o *OIDC) Start(ctx context.Context) {
	go func() {
		delay := o.cfg.RetryMin
		for attempt := 1; ; attempt++ {
			err := o.Discover(ctx)
			if err == nil {
				o.log.Info("auth: oidc discovery succeeded", "issuer", o.cfg.Issuer, "attempt", attempt)
				return
			}
			o.log.Warn("auth: oidc discovery failed, will retry; new logins answer 503 until it succeeds",
				"issuer", o.cfg.Issuer, "attempt", attempt, "retry_in", delay.String(), "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			delay *= 2
			if delay > o.cfg.RetryMax {
				delay = o.cfg.RetryMax
			}
		}
	}()
}

func (o *OIDC) parts() (*oauth2.Config, *oidc.IDTokenVerifier) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.oauth, o.verifier
}

// AuthCodeURL builds the IdP redirect with the PKCE S256 challenge for
// verifier and the nonce. ErrIDPUnavailable until discovery has succeeded.
func (o *OIDC) AuthCodeURL(state, nonce, verifier string) (string, error) {
	conf, _ := o.parts()
	if conf == nil {
		return "", ErrIDPUnavailable
	}
	return conf.AuthCodeURL(state, oauth2.S256ChallengeOption(verifier), oidc.Nonce(nonce)), nil
}

// NewVerifier returns a fresh PKCE code verifier.
func NewVerifier() string { return oauth2.GenerateVerifier() }

// Claims is what the session needs from a verified ID token.
type Claims struct {
	Issuer        string
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
	Groups        []string
	// GroupsMissing is true when a groups claim is configured but the token has
	// none (an IdP overage, or a missing mapper): design §2.2.
	GroupsMissing bool
	AMR           []string
	ACR           string
	AuthTime      time.Time // zero when the token has no auth_time
}

// Exchange trades the code for tokens (with the PKCE verifier), verifies the ID
// token (signature, iss, aud, exp), checks the nonce in constant time (Verify
// does not) and returns the claims. Every IdP token is discarded here.
func (o *OIDC) Exchange(ctx context.Context, code, verifier, nonce string) (Claims, error) {
	conf, verify := o.parts()
	if conf == nil {
		return Claims{}, ErrIDPUnavailable
	}
	ctx = oidc.ClientContext(ctx, o.http)
	tok, err := conf.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		// A RetrieveError carries the response body, which may hold anything the
		// IdP wrote. Only the status and a vetted OAuth error code are kept; the
		// error value itself is never wrapped or logged.
		var re *oauth2.RetrieveError
		if errors.As(err, &re) {
			status := 0
			if re.Response != nil {
				status = re.Response.StatusCode
			}
			if status >= 500 || status == 0 {
				return Claims{}, fmt.Errorf("%w: token endpoint said %d %s", ErrIDPUnavailable, status, safeOAuthCode(re.ErrorCode))
			}
			// The IdP answered and said no: a bad, used or expired code.
			return Claims{}, fmt.Errorf("%w: token endpoint said %d %s", ErrLoginRefused, status, safeOAuthCode(re.ErrorCode))
		}
		return Claims{}, fmt.Errorf("%w: token call: %w", ErrIDPUnavailable, scrubURLErr(err))
	}
	raw, _ := tok.Extra("id_token").(string)
	if raw == "" {
		return Claims{}, fmt.Errorf("%w: the token response has no id_token", ErrLoginRefused)
	}
	idt, err := verify.Verify(ctx, raw)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: id token: %v", ErrLoginRefused, err)
	}
	if subtle.ConstantTimeCompare([]byte(idt.Nonce), []byte(nonce)) != 1 || nonce == "" {
		return Claims{}, fmt.Errorf("%w: id token nonce does not match", ErrLoginRefused)
	}
	var all map[string]json.RawMessage
	if err := idt.Claims(&all); err != nil {
		return Claims{}, fmt.Errorf("%w: id token claims: %v", ErrLoginRefused, err)
	}
	// OIDC Core 3.1.3.7: if the token names an authorized party, it must be us.
	// go-oidc checks aud only.
	if raw, present := all["azp"]; present && string(raw) != "null" {
		var azp string
		if json.Unmarshal(raw, &azp) != nil || azp != o.cfg.ClientID {
			return Claims{}, fmt.Errorf("%w: id token azp is not this client", ErrLoginRefused)
		}
	}
	c := o.claimsFrom(idt, all)
	if c.Subject == "" || len(c.Subject) > maxClaimBytes || len(c.Issuer) > maxClaimBytes {
		return Claims{}, fmt.Errorf("%w: id token sub or iss is empty or too long", ErrLoginRefused)
	}
	return c, nil
}

func (o *OIDC) claimsFrom(idt *oidc.IDToken, all map[string]json.RawMessage) Claims {
	c := Claims{Issuer: idt.Issuer, Subject: idt.Subject}
	c.Email = clean(stringClaim(all["email"]), maxClaimBytes)
	c.EmailVerified = boolClaim(all["email_verified"])
	// The name is shown to other people as the author of comments, steers and
	// approvals, so bidirectional and other invisible format characters are
	// dropped: U+202E could make one name render as another (security audit
	// 2026-10, A-ID-4). Only the name: the email is matched against
	// users.yaml, and silently removing characters from it could turn one
	// address into another.
	c.Name = clean(stripFormat(stringClaim(all["name"])), 200)
	c.ACR = clean(stringClaim(all["acr"]), maxClaimBytes)
	for _, v := range stringsClaim(all["amr"]) {
		if v = clean(v, 64); v != "" && len(c.AMR) < 16 {
			c.AMR = append(c.AMR, v)
		}
	}
	if raw, ok := all["auth_time"]; ok {
		var secs float64
		if json.Unmarshal(raw, &secs) == nil && secs > 0 {
			c.AuthTime = time.Unix(int64(secs), 0).UTC()
		}
	}
	if o.cfg.GroupsClaim != "" {
		gs, ok := groupsClaim(all[o.cfg.GroupsClaim])
		c.GroupsMissing = !ok
		for _, g := range gs {
			if len(g) > maxGroupBytes || len(c.Groups) >= maxGroups || !validText(g) {
				continue
			}
			c.Groups = append(c.Groups, g)
		}
	}
	return c
}

// groupsClaim reads a groups claim: a JSON array of strings, or one string.
// ok is false when the claim is absent, null or of another type.
func groupsClaim(raw json.RawMessage) (groups []string, ok bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, false
	}
	var list []any
	if json.Unmarshal(raw, &list) == nil {
		for _, v := range list {
			if s, isStr := v.(string); isStr && s != "" {
				groups = append(groups, s)
			}
		}
		return groups, true
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		if one == "" {
			return nil, true
		}
		return []string{one}, true
	}
	return nil, false
}

func stringClaim(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

func stringsClaim(raw json.RawMessage) []string {
	var out []string
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return out
}

// boolClaim reads email_verified, which some IdPs send as the string "true".
func boolClaim(raw json.RawMessage) bool {
	var b bool
	if json.Unmarshal(raw, &b) == nil {
		return b
	}
	var s string
	return json.Unmarshal(raw, &s) == nil && strings.EqualFold(s, "true")
}

// clean drops control characters and invalid UTF-8 and caps the length in
// bytes, so a claim can be stored and shown safely.
func clean(s string, max int) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if len(s) > max {
		s = strings.ToValidUTF8(s[:max], "")
	}
	return s
}

// stripFormat removes Unicode format characters (category Cf: the bidi
// embeddings, overrides and isolates, zero-width joiners and spaces, the soft
// hyphen) from a display string.
func stripFormat(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, s)
}

func validText(s string) bool {
	if s == "" || strings.ToValidUTF8(s, "") != s {
		return false
	}
	return !strings.ContainsRune(s, 0)
}

// safeOAuthCode keeps an OAuth error code (invalid_grant, ...) for the log and
// drops anything else the IdP wrote.
func safeOAuthCode(c string) string {
	if len(c) > 40 {
		return ""
	}
	for _, r := range c {
		if !(r >= 'a' && r <= 'z' || r == '_') {
			return ""
		}
	}
	return c
}

// scrubURLErr turns *url.Error into its inner error, which holds no URL, so
// the log line does not repeat the token endpoint's query.
func scrubURLErr(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// SafeReturn checks the ?return= value of /auth/login (design §1.1) and
// returns the path and query to redirect to after login, rebuilt from the
// parsed URL. Anything that is not a plain same-site path becomes "/".
//
// It parses the value and checks the DECODED path, so /%2F%2Fevil is caught as
// well as //evil. Browsers turn a backslash into a slash, so a path starting
// with /\ is refused too. A value over 2 KB, or with a control character in
// the raw value or the decoded path, is refused.
func SafeReturn(raw string) string {
	const maxReturn = 2048
	if raw == "" || len(raw) > maxReturn || hasControl(raw) {
		return "/"
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "" || u.Host != "" || u.Opaque != "" || u.User != nil {
		return "/"
	}
	p := u.Path
	if !strings.HasPrefix(p, "/") || strings.HasPrefix(p, "//") || strings.HasPrefix(p, `/\`) || hasControl(p) {
		return "/"
	}
	out := url.URL{Path: p, RawQuery: u.RawQuery}
	return out.String()
}

func hasControl(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}

// GroupsClaim returns the configured groups claim name ("" = none).
func (o *OIDC) GroupsClaim() string { return o.cfg.GroupsClaim }
