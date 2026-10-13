package httpapi

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"strings"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/room"
	"piperoom/internal/ws"
)

// Login (docs/design/auth.md, auth PR 2): server-side sessions in a cookie, the
// dev login, logout, the Host allow-list and the dev proxy guard. OIDC login is
// auth PR 3; here oidc mode only refuses X-User and has no way to create a
// session yet.

// WithAuth sets the login settings (mode, session lengths, allowed hosts). The
// default is dev mode with 8 h idle / 12 h absolute sessions.
func WithAuth(a auth.Settings) Option {
	return func(s *Server) { s.authSettings = a }
}

// WithSessions sets the session store. The default is an in-memory store, so a
// server built without it (tests, --demo) loses its sessions on restart.
func WithSessions(st auth.Store) Option {
	return func(s *Server) { s.sessions = st }
}

// proxyIdentityMsg is the 403 proxy_identity text (docs/design/auth.md §5.2).
const proxyIdentityMsg = "a reverse proxy needs PIPEROOM_AUTH_MODE=oidc or tag pre-auth"

// normalizeAuth fills the settings a caller left unset.
func (s *Server) normalizeAuth() {
	d := auth.Default()
	if s.authSettings.Mode == "" {
		s.authSettings.Mode = d.Mode
	}
	if s.authSettings.Idle <= 0 {
		s.authSettings.Idle = d.Idle
	}
	if s.authSettings.Max <= 0 {
		s.authSettings.Max = d.Max
	}
	if s.sessions == nil {
		s.sessions = auth.NewMemStore()
	}
	if s.now == nil {
		s.now = time.Now
	}
}

// ---- Host allow-list (design §1.5) ----

// hostGuard refuses any Host that is not on the allow-list with 421 bad_host.
// CrossOriginProtection compares Origin with Host, so a page on a rebound DNS
// name passes it; this check is what stops that. It runs before every route,
// probes and signed alert routes included.
func (s *Server) hostGuard(next http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, h := range s.authSettings.AllowedHosts {
		allowed[strings.ToLower(h)] = true
	}
	if h := s.publicHostInList(); h != "" {
		allowed[h] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := strings.ToLower(r.Host)
		if !isLoopbackHostHeader(h) && !allowed[h] {
			writeError(w, http.StatusMisdirectedRequest, "bad_host", "this Host is not allowed; see PIPEROOM_PUBLIC_URL and PIPEROOM_ALLOWED_HOSTS")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// publicHostInList is the PIPEROOM_PUBLIC_URL host when it joins the allow-list:
// always in oidc mode, in dev mode only with the non-loopback switch.
func (s *Server) publicHostInList() string {
	if s.public.Host == "" {
		return ""
	}
	if s.authSettings.IsOIDC() || s.authSettings.DevNonLoopback {
		return strings.ToLower(s.public.Host)
	}
	return ""
}

// isLoopbackHostHeader reports whether a lower-case Host header names localhost,
// 127.0.0.1 or [::1], with or without a port (any port: an attacker page can
// never make a browser send Host: localhost, so the port adds nothing).
func isLoopbackHostHeader(h string) bool {
	name, port := h, ""
	if strings.HasPrefix(h, "[") {
		end := strings.IndexByte(h, ']')
		if end < 0 {
			return false
		}
		name, port = h[:end+1], h[end+1:]
		if port != "" {
			if port[0] != ':' {
				return false
			}
			port = port[1:]
			if port == "" {
				return false
			}
		}
	} else if i := strings.LastIndexByte(h, ':'); i >= 0 {
		name, port = h[:i], h[i+1:]
		if port == "" {
			return false
		}
	}
	for i := 0; i < len(port); i++ {
		if port[i] < '0' || port[i] > '9' {
			return false
		}
	}
	if len(port) > 5 {
		return false
	}
	return name == "localhost" || name == "127.0.0.1" || name == "[::1]"
}

// ---- JSON-only POSTs (design §1.4) ----

// requireJSON refuses a state-changing request whose Content-Type is not
// application/json. A cross-site HTML form cannot send that type without a
// preflight, which is part of the CSRF defence. The signed alert routes do not
// use it: they take the sender's own content type.
func requireJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
		default:
			mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || mt != "application/json" {
				writeError(w, http.StatusBadRequest, "bad_request", "Content-Type must be application/json")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ---- identity ----

type ctxKey int

const (
	userKey ctxKey = iota + 1
	sessionIDKey
)

type authError struct {
	status int
	code   string
	msg    string
}

type principal struct {
	user      room.User
	sessionID string // "" for an X-User request with no cookie
}

// authenticate resolves the caller and stores the user (and session id) in the
// context. It re-runs the login gate on every request: in dev mode the user
// must still be in users.yaml (design §1.3).
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, aerr := s.resolve(r)
		if aerr != nil {
			if aerr.code == "session_expired" {
				// Only this code carries the mode, so the web app knows
				// whether to show the dev picker or go to /auth/login.
				writeJSON(w, aerr.status, map[string]any{
					"error": map[string]string{"code": aerr.code, "message": aerr.msg, "auth_mode": s.authSettings.Mode},
				})
				return
			}
			writeError(w, aerr.status, aerr.code, aerr.msg)
			return
		}
		ctx := context.WithValue(r.Context(), userKey, p.user)
		ctx = context.WithValue(ctx, sessionIDKey, p.sessionID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// resolve implements design §5.2.
//
// dev: the cookie session first. With no cookie, X-User works as before (curl).
// A cookie plus an X-User naming another user is a proxy forcing an identity:
// 403 proxy_identity. oidc: X-User is ignored; no session is 401 session_expired.
func (s *Server) resolve(r *http.Request) (principal, *authError) {
	oidc := s.authSettings.IsOIDC()
	hasXUser := len(r.Header.Values("X-User")) > 0
	xuser := strings.TrimSpace(r.Header.Get("X-User"))

	if c, err := r.Cookie(s.authSettings.CookieName()); err == nil && c.Value != "" {
		sess, aerr := s.sessionFromToken(r.Context(), c.Value)
		if aerr != nil {
			return principal{}, aerr
		}
		user, ok := s.svc.LookupUser(sess.UserID)
		if !ok {
			// Removed from users.yaml: out at once, not at the 12 h cap.
			return principal{}, &authError{http.StatusUnauthorized, "session_expired", "this user is no longer allowed to sign in"}
		}
		if !oidc && hasXUser && xuser != sess.UserID {
			return principal{}, &authError{http.StatusForbidden, "proxy_identity", proxyIdentityMsg}
		}
		s.touch(r.Context(), sess)
		return principal{user: user, sessionID: sess.ID}, nil
	}

	if oidc {
		return principal{}, &authError{http.StatusUnauthorized, "session_expired", "sign in required"}
	}
	if xuser == "" {
		return principal{}, &authError{http.StatusUnauthorized, "unauthenticated", "missing X-User header"}
	}
	user, ok := s.svc.LookupUser(xuser)
	if !ok {
		return principal{}, &authError{http.StatusUnauthorized, "unauthenticated", "unknown user " + xuser}
	}
	return principal{user: user}, nil
}

// sessionFromToken loads the session a cookie value names and checks it. Any
// reason it cannot be used is 401 session_expired; a store failure is a 500.
func (s *Server) sessionFromToken(ctx context.Context, token string) (auth.Session, *authError) {
	expired := &authError{http.StatusUnauthorized, "session_expired", "session expired or signed out"}
	hash, ok := auth.HashToken(token)
	if !ok {
		return auth.Session{}, expired
	}
	sess, err := s.sessions.ByTokenHash(ctx, hash)
	if errors.Is(err, auth.ErrNotFound) {
		return auth.Session{}, expired
	}
	if err != nil {
		s.log.Error("auth: load session", "err", err)
		return auth.Session{}, &authError{http.StatusInternalServerError, "internal_error", "internal error"}
	}
	if sess.Check(s.now(), s.authSettings.Idle) != nil {
		return auth.Session{}, expired
	}
	return sess, nil
}

// touch writes last_seen_at at most once a minute. A failure only means the
// idle clock is a little behind, so it is logged and ignored.
func (s *Server) touch(ctx context.Context, sess auth.Session) {
	now := s.now()
	if now.Sub(sess.LastSeenAt) < auth.TouchEvery {
		return
	}
	if err := s.sessions.Touch(ctx, sess.ID, now); err != nil {
		s.log.Warn("auth: touch session", "err", err)
	}
}

// sessionAlive is the gateway's presence-tick check.
func (s *Server) sessionAlive(ctx context.Context, id ws.Identity) bool {
	sess, err := s.sessions.ByID(ctx, id.SessionID)
	if err != nil {
		// A store outage must not log everyone out: only a definite "gone"
		// closes the connection.
		return !errors.Is(err, auth.ErrNotFound)
	}
	if sess.Check(s.now(), s.authSettings.Idle) != nil {
		return false
	}
	_, ok := s.svc.LookupUser(sess.UserID)
	return ok
}

// wsIdentify hands the gateway the identity authenticate already resolved.
func wsIdentify(r *http.Request) (ws.Identity, bool) {
	u, ok := r.Context().Value(userKey).(room.User)
	if !ok {
		return ws.Identity{}, false
	}
	sid, _ := r.Context().Value(sessionIDKey).(string)
	return ws.Identity{User: u, SessionID: sid}, true
}

// ---- cookies ----

func (s *Server) sessionCookie(value string, expires time.Time, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     s.authSettings.CookieName(),
		Value:    value,
		Path:     "/",
		Expires:  expires,
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   s.authSettings.IsOIDC(), // __Host- needs it; plain-http dev cannot have it
		SameSite: http.SameSiteLaxMode,
	}
}

func (s *Server) clearCookie() *http.Cookie {
	return s.sessionCookie("", time.Unix(0, 0), -1)
}

// ---- routes ----

// handleDevLogin is POST /auth/dev-login {user} (dev mode only): it issues a
// real session for a users.yaml user and sets the cookie.
func (s *Server) handleDevLogin(w http.ResponseWriter, r *http.Request) {
	// A proxy that forces X-User must not be able to pick another identity
	// (design §5.2 rule a). Any value counts, even an empty one.
	if len(r.Header.Values("X-User")) > 0 {
		writeError(w, http.StatusForbidden, "proxy_identity", proxyIdentityMsg)
		return
	}
	var body struct {
		User string `json:"user"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		s.writeErr(w, err)
		return
	}
	if body.User == "" {
		s.writeErr(w, validation("user is required"))
		return
	}
	user, ok := s.svc.LookupUser(body.User)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "unknown user "+body.User)
		return
	}

	token, hash, err := auth.NewToken()
	if err != nil {
		s.log.Error("auth: new token", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	now := s.now().UTC()
	sess := auth.Session{
		ID:         room.NewID("ses_"),
		TokenHash:  hash,
		UserID:     user.ID,
		Name:       user.Name,
		Method:     auth.MethodDev,
		CreatedAt:  now,
		LastSeenAt: now,
		ExpiresAt:  now.Add(s.authSettings.Max),
	}
	if err := s.sessions.Create(r.Context(), sess); err != nil {
		s.log.Error("auth: create session", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	// Switching user in the same browser: the old session is no longer wanted.
	if err := s.revokeCookieSession(r); err != nil {
		s.log.Warn("auth: revoke the previous session", "err", err)
	}

	if s.authSettings.DevNonLoopback {
		s.log.Warn("auth: dev auth on a non-loopback address: anyone who can reach this port can act as any user", "user", user.ID, "session", sess.ID)
	} else {
		s.log.Info("auth: dev login", "user", user.ID, "session", sess.ID)
	}
	http.SetCookie(w, s.sessionCookie(token, sess.ExpiresAt, int(s.authSettings.Max.Seconds())))
	writeJSON(w, http.StatusOK, user)
}

// handleLogout is POST /auth/logout: it revokes the session, clears the cookie
// and closes that session's WebSockets. It is idempotent: with no valid cookie
// it still clears the cookie and answers 204.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if err := s.revokeCookieSession(r); err != nil {
		s.log.Error("auth: logout", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	http.SetCookie(w, s.clearCookie())
	w.WriteHeader(http.StatusNoContent)
}

// revokeCookieSession revokes the session named by the request's cookie, if
// any, and closes its WebSockets.
func (s *Server) revokeCookieSession(r *http.Request) error {
	c, err := r.Cookie(s.authSettings.CookieName())
	if err != nil || c.Value == "" {
		return nil
	}
	hash, ok := auth.HashToken(c.Value)
	if !ok {
		return nil
	}
	sess, err := s.sessions.ByTokenHash(r.Context(), hash)
	if errors.Is(err, auth.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := s.sessions.Revoke(r.Context(), sess.ID, s.now()); err != nil {
		return err
	}
	s.gw.CloseSession(sess.ID)
	return nil
}

// handleOIDCPending answers the two OIDC routes until auth PR 3 lands.
func (s *Server) handleOIDCPending(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "not_implemented", "OIDC login is not available in this build yet")
}
