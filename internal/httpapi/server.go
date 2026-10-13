// Package httpapi exposes the room service over HTTP exactly per docs/api.md.
// Handlers are thin; all rules live in package room.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"piperoom/internal/alertin"
	"piperoom/internal/auth"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/ws"
)

// maxBody is the request body cap (docs/api.md: 1 MB).
const maxBody = 1 << 20

// requestTimeout is the default per-request budget (docs/api.md).
const requestTimeout = 60 * time.Second

// handoffTimeout matches the hand-off allowance in docs/api.md: the summary
// generation may take up to 3 minutes and this route is exempt from the 60 s
// request timeout.
const handoffTimeout = 3 * time.Minute

// Server wires the chi router to the room service.
type Server struct {
	svc     *room.Service
	gw      *ws.Gateway
	log     *slog.Logger
	handler http.Handler

	alerts       alertin.Config // signed alert sources; the zero value has every source disabled
	ready        Pinger
	probeTimeout time.Duration
	readyWarn    readyWarner
	refuseWarn   refuseWarner

	// Optional web UI serving (WithWebDir, WithPublicOrigin).
	webDir  string
	webRoot *os.Root
	public  PublicOrigin

	// Login (WithAuth, WithSessions): auth.go.
	authSettings auth.Settings
	sessions     auth.Store
	now          func() time.Time

	// OIDC login (WithOIDC, WithLoginStates, WithIdentities): auth_oidc.go.
	oidc        *auth.OIDC
	loginStates auth.LoginStateStore
	identities  auth.IdentityStore
	loginLimit  *rateLimiter
}

// New builds the HTTP handler (router). store is passed to the WebSocket
// gateway for replay + live events. The returned Server must be closed with
// Close so WebSocket connections and the room service shut down cleanly.
// opts add optional behaviour such as the /readyz store check (WithReady).
func New(svc *room.Service, st store.Store, log *slog.Logger, opts ...Option) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		svc:          svc,
		gw:           ws.NewGateway(svc, st, log),
		log:          log,
		probeTimeout: DefaultProbeTimeout,
		readyWarn:    readyWarner{every: readyWarnEvery, now: time.Now},
		refuseWarn:   refuseWarner{every: refuseWarnEvery, now: time.Now},
	}
	for _, o := range opts {
		o(s)
	}
	s.normalizeAuth()
	s.gw.Identify = wsIdentify
	s.gw.SessionAlive = s.sessionAlive

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	if s.webDir != "" {
		r.Use(s.securityHeaders)
	}
	r.Use(requestLog(log, s.clientIP))
	// Host allow-list first (DNS rebinding, design §1.5), then the cross-origin
	// check, on every route including probes and signed alerts.
	r.Use(s.hostGuard)
	r.Use(s.crossOriginProtection().Handler)
	// Router-level misses use the same JSON envelope as every other error.
	notFound := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "not found")
	}))
	if s.webDir != "" {
		// Everything no API or probe route matched is the web UI (static.go);
		// unknown /api paths still get the JSON 404 there.
		root, err := os.OpenRoot(s.webDir)
		if err != nil {
			// cmd/server checks the directory before it gets here.
			log.Error("httpapi: open web dir", "dir", s.webDir, "err", err)
		} else {
			s.webRoot = root
			notFound = newStaticHandler(root)
		}
	}
	r.NotFound(notFound.ServeHTTP)
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})

	// Liveness/readiness probes: outside /api, so no X-User and no 60 s
	// request budget; /readyz bounds its store check with probeTimeout.
	r.Get(pathHealthz, s.handleHealthz)
	r.Get(pathReadyz, s.handleReadyz)

	// Login routes (auth.go, auth_oidc.go). Dev login exists only in dev mode;
	// the two OIDC routes only in oidc mode.
	r.Route("/auth", func(a chi.Router) {
		a.Use(requireJSON)
		a.Use(middleware.Timeout(requestTimeout))
		a.Post("/logout", s.handleLogout)
		if s.authSettings.IsOIDC() {
			a.Get("/login", s.handleLogin)
			a.Get("/callback", s.handleCallback)
		} else {
			a.Post("/dev-login", s.handleDevLogin)
		}
	})

	r.Route("/api", func(api chi.Router) {
		// Signed alert routes: the sender's signature or token is the auth, so
		// no session. They take the sender's own content type (no requireJSON).
		// A source without a secret answers 401.
		api.With(middleware.Timeout(requestTimeout)).Post("/alerts/{source}", s.handleSignedAlert)

		api.Group(func(protected chi.Router) {
			protected.Use(s.authenticate)
			protected.Use(viewerGuard)
			protected.Use(requireJSON)

			// The WebSocket authenticates by the session cookie (or, in dev
			// mode, X-User for curl and wscat) like every other route, and
			// runs outside the request timeout: the connection outlives the
			// request. Replay has its own deadline in the gateway.
			protected.Get("/rooms/{id}/ws", s.gw.ServeHTTP)

			// The hand-off route is exempt from the default budget.
			protected.Post("/rooms/{id}/handoffs", withTimeout(handoffTimeout, s.handleCreateHandoff))
			// The audit export reads the whole log, so it has its own budget.
			protected.Get("/rooms/{id}/export", withTimeout(exportTimeout, s.handleExportRoom))

			protected.Group(func(short chi.Router) {
				short.Use(middleware.Timeout(requestTimeout))

				short.Get("/me", s.handleMe)
				short.Get("/people", s.handlePeople)
				short.Get("/rooms", s.handleListRooms)
				short.Post("/rooms", s.handleCreateRoom)
				if !s.authSettings.IsOIDC() {
					// Unsigned: dev mode only (design §4). In oidc mode this
					// path is not mounted and falls to /alerts/{source}, which
					// answers 404 for the unknown source "webhook".
					short.Post("/alerts/webhook", s.handleAlertWebhook)
				}
				short.Get("/rooms/{id}", s.handleGetRoom)
				short.Get("/rooms/{id}/events", s.handleRoomEvents)
				short.Post("/rooms/{id}/comments", s.handleAddComment)
				short.Post("/rooms/{id}/steer", s.handleSteer)
				short.Post("/rooms/{id}/suggestions/{sid}/send", s.handleSendSuggestion)
				short.Post("/rooms/{id}/control/request", s.handleRequestControl)
				short.Post("/rooms/{id}/control/grant", s.handleGrantControl)
				short.Post("/rooms/{id}/close", s.handleCloseRoom)
				short.Post("/rooms/{id}/trace", s.handleTrace)
				short.Post("/rooms/{id}/shadow/blind", s.handleShadowBlind)
				short.Get("/rooms/{id}/shadow", s.handleShadow)
				short.Post("/rooms/{id}/shadow/grade", s.handleShadowGrade)
				short.Get("/shadow/metrics", s.handleShadowMetrics)
				short.Post("/proposals/{pid}/approve", s.handleApproveProposal)
				short.Post("/proposals/{pid}/reject", s.handleRejectProposal)
				short.Post("/handoffs/{hid}/accept", s.handleAcceptHandoff)
				short.Get("/pipelines/{pid}/topology", s.handleTopology)
			})
		})
	})
	s.gw.OriginPatterns = s.wsOriginPatterns()
	s.handler = r
	return s
}

// withTimeout applies the chi timeout middleware to a single handler.
func withTimeout(d time.Duration, h http.HandlerFunc) http.HandlerFunc {
	return middleware.Timeout(d)(h).ServeHTTP
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// Close shuts the WebSocket gateway down (closing every live connection) and
// stops the room service.
func (s *Server) Close() {
	if s.webRoot != nil {
		_ = s.webRoot.Close()
	}
	s.gw.Close()
	s.svc.Close()
}

// wsOriginPatterns are the extra WebSocket origin hosts (the library always
// accepts the request's own host). They follow the Host allow-list (design
// §1.6): the validated public host and PIPEROOM_ALLOWED_HOSTS entries, which
// are the names a proxy shows the browser while rewriting Host. When the
// server does not serve the UI itself (dev), the Vite dev server's origins are
// added too.
func (s *Server) wsOriginPatterns() []string {
	patterns := []string{}
	if s.public.Host != "" {
		patterns = append(patterns, s.public.Host)
	}
	patterns = append(patterns, s.authSettings.AllowedHosts...)
	if s.webDir == "" && !s.authSettings.IsOIDC() {
		patterns = append(patterns, "localhost:5173", "127.0.0.1:5173")
	}
	return patterns
}

// crossOriginProtection builds Go's CSRF check (design §1.4): a non-safe
// request from another origin, by Sec-Fetch-Site or by Origin against Host, is
// refused with 403. Requests with neither header (curl, alert senders) pass.
// Behind a proxy that rewrites Host, the browser's Origin is the public origin,
// which is trusted here.
func (s *Server) crossOriginProtection() *http.CrossOriginProtection {
	cop := http.NewCrossOriginProtection()
	if h := s.publicHostInList(); h != "" {
		if err := cop.AddTrustedOrigin(s.public.Scheme + "://" + h); err != nil {
			s.log.Error("httpapi: trusted origin", "host", h, "err", err)
		}
	}
	return cop
}

func userFrom(ctx context.Context) room.User {
	u, _ := ctx.Value(userKey).(room.User)
	return u
}

func requestLog(log *slog.Logger, client func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			// Probes hit every few seconds; keep them out of the Info log.
			level := slog.LevelInfo
			if isProbePath(r.URL.Path) {
				level = slog.LevelDebug
			}
			log.Log(r.Context(), level, "http request", "method", r.Method, "path", r.URL.Path, "status", ww.Status(), "client", client(r), "dur", time.Since(start).Round(time.Millisecond).String())
		})
	}
}

// ---- helpers ----

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		return validation("invalid JSON body")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeEvent(w http.ResponseWriter, status int, v any) {
	writeJSON(w, status, v)
}

// writeErr maps a room error to its HTTP status and code; anything else is a
// 500 internal_error and is logged with its cause.
func (s *Server) writeErr(w http.ResponseWriter, err error) {
	var re *room.Error
	if errors.As(err, &re) {
		writeError(w, re.Status, re.Code, re.Msg)
		return
	}
	// The room service refuses unstorable text before it writes. If one gets
	// through (a path with no check yet), it is still the client's text and a
	// 400, never a 500. The message does not quote the text.
	if errors.Is(err, store.ErrUnstorable) {
		writeError(w, http.StatusBadRequest, "validation", "text contains characters that cannot be stored")
		return
	}
	s.log.Error("http: request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": msg},
	})
}

func validation(msg string) *room.Error {
	return &room.Error{Status: http.StatusBadRequest, Code: "validation", Msg: msg}
}

func parseAfter(r *http.Request) (int64, error) {
	q := r.URL.Query().Get("after")
	if q == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(q, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("after must be a non-negative integer")
	}
	return n, nil
}

func parseLimit(r *http.Request) (int, error) {
	q := r.URL.Query().Get("limit")
	if q == "" {
		return 500, nil
	}
	n, err := strconv.Atoi(q)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("limit must be a non-negative integer")
	}
	return n, nil
}
