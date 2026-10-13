package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/room"
)

// OIDC login routes (docs/design/auth.md §1.1, §1.2 and §2.1). The flow itself
// (PKCE, nonce, token checks) is in package auth; this file owns the cookies,
// the login_states row, the identity mapping and the session.

// WithOIDC sets the relying party for oidc mode. Without it (or until its
// discovery has succeeded) /auth/login and /auth/callback answer 503
// idp_unavailable.
func WithOIDC(o *auth.OIDC) Option {
	return func(s *Server) { s.oidc = o }
}

// WithLoginStates sets the login_states store. The default is in memory.
func WithLoginStates(st auth.LoginStateStore) Option {
	return func(s *Server) { s.loginStates = st }
}

// WithIdentities sets the identities store. The default is in memory.
func WithIdentities(st auth.IdentityStore) Option {
	return func(s *Server) { s.identities = st }
}

// loginRateLimit is the per client IP cap on GET /auth/login (design §1.1):
// large on purpose, because the route needs no login and a small cap would be
// an anonymous lockout.
const loginRateLimit = 300

// handleLogin is GET /auth/login?return=/path: it stores the state, nonce and
// PKCE verifier in a login_states row, sets the pre-login cookie and sends the
// browser to the IdP.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	if !s.loginLimit.allow(s.clientIP(r), now) {
		w.Header().Set("Retry-After", "60")
		writeError(w, http.StatusTooManyRequests, "rate_limited", "too many login attempts from this address; try again in a minute")
		return
	}
	if s.oidc == nil || !s.oidc.Ready() {
		writeIdPUnavailable(w)
		return
	}
	ret := auth.SafeReturn(r.URL.Query().Get("return"))

	state, err1 := auth.NewStateNonce()
	nonce, err2 := auth.NewStateNonce()
	id, err3 := auth.NewLoginStateID()
	if err := errors.Join(err1, err2, err3); err != nil {
		s.log.Error("auth: login random values", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	verifier := auth.NewVerifier()
	target, err := s.oidc.AuthCodeURL(state, nonce, verifier)
	if err != nil {
		writeIdPUnavailable(w)
		return
	}

	// One live row per login cookie: a new call replaces the old row (§1.1).
	replaces := ""
	if c, err := r.Cookie(s.authSettings.LoginCookieName()); err == nil && validCookieID(c.Value) {
		replaces = c.Value
	}
	expires := now.Add(auth.LoginStateTTL)
	if err := s.loginStates.Create(r.Context(), auth.LoginState{
		ID: id, State: state, Nonce: nonce, Verifier: verifier, ReturnPath: ret, ExpiresAt: expires,
	}, replaces, now); err != nil {
		s.log.Error("auth: store login state", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	http.SetCookie(w, s.loginCookie(id, expires, int(auth.LoginStateTTL.Seconds())))
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, target, http.StatusFound)
}

// handleCallback is GET /auth/callback. The order is design §1.2 and matters:
// the row is found through the login cookie, never by the state value.
func (s *Server) handleCallback(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.oidc == nil || !s.oidc.Ready() {
		writeIdPUnavailable(w)
		return
	}
	// 1. The login cookie. None: the login expired, or this browser never
	// started one (the login-CSRF case).
	c, err := r.Cookie(s.authSettings.LoginCookieName())
	if err != nil || c.Value == "" {
		writeError(w, http.StatusBadRequest, "login_expired", "the login expired or was not started in this browser; sign in again")
		return
	}
	// The cookie is spent whatever happens next.
	http.SetCookie(w, s.clearLoginCookie())

	// 2. DELETE ... RETURNING: single use, so a replayed callback finds nothing.
	row, err := s.loginStates.Take(r.Context(), c.Value, s.now())
	if errors.Is(err, auth.ErrLoginStateNotFound) {
		writeError(w, http.StatusBadRequest, "login_expired", "the login expired or was already used; sign in again")
		return
	}
	if err != nil {
		s.log.Error("auth: take login state", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}

	// 3. The state, in constant time.
	q := r.URL.Query()
	if subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(row.State)) != 1 {
		s.log.Warn("auth: callback state mismatch")
		writeError(w, http.StatusUnauthorized, "unauthenticated", "login state did not match; sign in again")
		return
	}
	if e := q.Get("error"); e != "" {
		// The IdP refused (access_denied, ...). Only a short code is logged.
		s.log.Info("auth: the identity provider refused the login", "error", truncateCode(e))
		writeError(w, http.StatusUnauthorized, "unauthenticated", "the identity provider did not sign you in")
		return
	}
	code := q.Get("code")
	if code == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "the callback has no code")
		return
	}

	// 4. and 5. Exchange with the PKCE verifier, verify the ID token, compare
	// the nonce.
	claims, err := s.oidc.Exchange(r.Context(), code, row.Verifier, row.Nonce)
	if err != nil {
		if errors.Is(err, auth.ErrIDPUnavailable) {
			s.log.Warn("auth: identity provider unavailable during callback", "err", err)
			writeIdPUnavailable(w)
			return
		}
		s.log.Warn("auth: login refused", "err", err)
		writeError(w, http.StatusUnauthorized, "unauthenticated", "the identity provider's answer was not accepted; sign in again")
		return
	}

	// 6. Map the identity, run the login gate, create the session.
	userID, name, aerr := s.mapIdentity(r.Context(), claims)
	if aerr != nil {
		writeError(w, aerr.status, aerr.code, aerr.msg)
		return
	}
	if _, ok := s.svc.RoleFor(userID, claims.Groups); !ok {
		s.log.Info("auth: login refused by the gate (not provisioned)", "user", userID, "iss", claims.Issuer)
		writeError(w, http.StatusForbidden, "not_provisioned", "this account is not set up for Piperoom; ask an admin to add you")
		return
	}
	if claims.GroupsMissing {
		s.log.Warn("auth: no groups claim in id token", "user", userID, "iss", claims.Issuer, "claim", s.oidc.GroupsClaim())
	}

	token, hash, err := auth.NewToken()
	if err != nil {
		s.log.Error("auth: new token", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	now := s.now().UTC()
	sess := auth.Session{
		ID:            room.NewID("ses_"),
		TokenHash:     hash,
		UserID:        userID,
		Name:          name,
		Iss:           claims.Issuer,
		Sub:           claims.Subject,
		Groups:        claims.Groups,
		GroupsMissing: claims.GroupsMissing,
		AuthTime:      claims.AuthTime,
		AMR:           claims.AMR,
		ACR:           claims.ACR,
		Method:        auth.MethodOIDC,
		CreatedAt:     now,
		LastSeenAt:    now,
		ExpiresAt:     now.Add(s.authSettings.Max),
	}
	if err := s.sessions.Create(r.Context(), sess); err != nil {
		s.log.Error("auth: create session", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	// Signing in again in the same browser: the old session is no longer wanted.
	if err := s.revokeCookieSession(r); err != nil {
		s.log.Warn("auth: revoke the previous session", "err", err)
	}
	s.log.Info("auth: oidc login", "user", userID, "session", sess.ID, "groups", len(claims.Groups))

	http.SetCookie(w, s.sessionCookie(token, sess.ExpiresAt, int(s.authSettings.Max.Seconds())))
	// 7. Back to where the person was going; re-checked even though login
	// checked it, because the row is only as safe as its writer.
	http.Redirect(w, r, auth.SafeReturn(row.ReturnPath), http.StatusFound)
}

// mapIdentity turns verified claims into a Piperoom user id (design §2.1):
//
//  1. a users.yaml sub: wins;
//  2. a bound (iss, sub) in identities;
//  3. a verified email that matches a users.yaml user binds (iss, sub) the
//     first time. A user that is already bound to another account, or whose
//     users.yaml entry pins another sub, is refused: a recycled email;
//  4. otherwise u_ + the first 12 hex of SHA-256(iss|sub).
//
// The returned name is the users.yaml name when there is one, else the claim.
func (s *Server) mapIdentity(ctx context.Context, c auth.Claims) (id, name string, aerr *authError) {
	if u, ok := s.svc.UserBySub(c.Subject); ok {
		return u.ID, u.Name, nil
	}
	bound, err := s.identities.UserFor(ctx, c.Issuer, c.Subject)
	switch {
	case err == nil:
		return bound, s.displayName(bound, c), nil
	case !errors.Is(err, auth.ErrIdentityNotFound):
		s.log.Error("auth: look up identity", "err", err)
		return "", "", &authError{http.StatusInternalServerError, "internal_error", "internal error"}
	}
	if c.EmailVerified && c.Email != "" {
		if u, ok := s.svc.UserByEmail(c.Email); ok {
			refuse := &authError{http.StatusForbidden, "not_provisioned", "this email is already linked to a different account; ask an admin to unbind it"}
			if u.Sub != "" {
				s.log.Warn("auth: login refused: the email belongs to a user pinned to another sub", "user", u.ID, "iss", c.Issuer)
				return "", "", refuse
			}
			switch err := s.identities.Bind(ctx, c.Issuer, c.Subject, u.ID, c.Email); {
			case err == nil:
				s.log.Info("auth: bound an identity to a users.yaml user", "user", u.ID, "iss", c.Issuer)
				return u.ID, u.Name, nil
			case errors.Is(err, auth.ErrIdentityConflict):
				s.log.Warn("auth: login refused: the user is bound to another identity (recycled email?)", "user", u.ID, "iss", c.Issuer)
				return "", "", refuse
			default:
				s.log.Error("auth: bind identity", "err", err)
				return "", "", &authError{http.StatusInternalServerError, "internal_error", "internal error"}
			}
		}
	}
	derived := DerivedUserID(c.Issuer, c.Subject)
	return derived, s.displayName(derived, c), nil
}

// displayName is the users.yaml name when the id is in users.yaml, else the
// name claim, else the email, else the id.
func (s *Server) displayName(id string, c auth.Claims) string {
	if u, ok := s.svc.LookupUser(id); ok && u.Name != "" {
		return u.Name
	}
	switch {
	case c.Name != "":
		return c.Name
	case c.Email != "":
		return c.Email
	}
	return id
}

// DerivedUserID is the id of a person with no users.yaml entry: "u_" and the
// first 12 hex characters of SHA-256(iss|sub). An owners.yaml principal may
// name it as user:u_....
func DerivedUserID(iss, sub string) string {
	sum := sha256.Sum256([]byte(iss + "|" + sub))
	return "u_" + hex.EncodeToString(sum[:])[:12]
}

func writeIdPUnavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "5")
	writeError(w, http.StatusServiceUnavailable, "idp_unavailable", "the identity provider is not reachable right now; try again shortly")
}

func truncateCode(s string) string {
	if len(s) > 40 {
		s = s[:40]
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return '?'
		}
		return r
	}, s)
}

// validCookieID accepts the shape NewLoginStateID produces, so a hostile cookie
// value never reaches the store.
func validCookieID(v string) bool {
	if v == "" || len(v) > 64 {
		return false
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func (s *Server) loginCookie(value string, expires time.Time, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     s.authSettings.LoginCookieName(),
		Value:    value,
		Path:     "/",
		Expires:  expires,
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   s.authSettings.IsOIDC(),
		// Lax, not Strict: the IdP sends the browser back with a cross-site
		// redirect, and a Strict cookie would not be sent on it.
		SameSite: http.SameSiteLaxMode,
	}
}

func (s *Server) clearLoginCookie() *http.Cookie {
	return s.loginCookie("", time.Unix(0, 0), -1)
}

// ---- client address and the /auth/login limit ----

// clientIP is the address the login limit and the request log key on. In oidc
// mode, when the peer is in PIPEROOM_TRUSTED_PROXIES, it is the rightmost
// X-Forwarded-For entry that is not itself a trusted proxy; otherwise it is the
// peer address (design §1.1). Dev mode always uses the peer address.
func (s *Server) clientIP(r *http.Request) string {
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		host = h
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return host
	}
	peer = peer.Unmap()
	if !s.authSettings.IsOIDC() || !inPrefixes(s.authSettings.TrustedProxies, peer) {
		return peer.String()
	}
	var hops []string
	for _, line := range r.Header.Values("X-Forwarded-For") {
		for _, h := range strings.Split(line, ",") {
			hops = append(hops, strings.TrimSpace(h))
		}
	}
	for i := len(hops) - 1; i >= 0; i-- {
		a, err := netip.ParseAddr(hops[i])
		if err != nil {
			// Garbage from the right means the chain cannot be trusted past it.
			return peer.String()
		}
		a = a.Unmap()
		if !inPrefixes(s.authSettings.TrustedProxies, a) {
			return a.String()
		}
	}
	return peer.String()
}

func inPrefixes(ps []netip.Prefix, a netip.Addr) bool {
	for _, p := range ps {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// rateLimiter is a fixed one-minute window counter per key, in memory and per
// process (design §1.1).
type rateLimiter struct {
	limit  int
	window time.Duration

	mu      sync.Mutex
	buckets map[string]*bucket
	pruneAt time.Time
}

type bucket struct {
	start time.Time
	n     int
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window, buckets: map[string]*bucket{}}
}

// allow counts one call for key at now and reports whether it is within the limit.
func (l *rateLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.After(l.pruneAt) {
		for k, b := range l.buckets {
			if now.Sub(b.start) >= l.window {
				delete(l.buckets, k)
			}
		}
		l.pruneAt = now.Add(l.window)
	}
	b := l.buckets[key]
	if b == nil || now.Sub(b.start) >= l.window {
		b = &bucket{start: now}
		l.buckets[key] = b
	}
	b.n++
	return b.n <= l.limit
}
