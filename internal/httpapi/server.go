// Package httpapi exposes the room service over HTTP exactly per docs/api.md.
// Handlers are thin; all rules live in package room.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"piperoom/internal/alertin"
	"piperoom/internal/auth"
	"piperoom/internal/clients"
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
	changes      http.Handler   // POST /api/changes/webhook (WithChangesWebhook); nil = not mounted
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

	// Model spend routes (WithSpend): usage.go.
	spend Spend

	// Scripted demo presenter controls (WithDemoControls, --demo only):
	// demo.go. Nil outside --demo, and then the routes are not mounted.
	demo DemoControls

	// GET /metrics: metrics.go.
	metrics *metrics

	// Admin settings and connection tests (WithAdmin): admin.go.
	admin      AdminSettings
	adminLimit *rateLimiter

	// Watch sample history (WithSamples): trends.go.
	samples clients.SampleReader

	// Setup runner endpoint (WithSetup): runners.go.
	setupEnabled bool
	// runnerCtx is the lifecycle of the runner WebSocket connections: Close
	// cancels it, so a runner that keeps pinging cannot outlive the server.
	runnerCtx    context.Context
	runnerCancel context.CancelFunc
}

// New builds the HTTP handler (router). store is passed to the WebSocket
// gateway for replay + live events. The returned Server must be closed with
// Close so WebSocket connections and the room service shut down cleanly.
// opts add optional behaviour such as the /readyz store check (WithReady).
func New(svc *room.Service, st store.Store, log *slog.Logger, opts ...Option) *Server {
	if log == nil {
		log = slog.Default()
	}
	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	s := &Server{
		svc:          svc,
		gw:           ws.NewGateway(svc, st, log),
		log:          log,
		probeTimeout: DefaultProbeTimeout,
		readyWarn:    readyWarner{every: readyWarnEvery, now: time.Now},
		refuseWarn:   refuseWarner{every: refuseWarnEvery, now: time.Now},
		runnerCtx:    runnerCtx,
		runnerCancel: runnerCancel,
	}
	for _, o := range opts {
		o(s)
	}
	s.normalizeAuth()
	s.gw.Identify = wsIdentify
	s.gw.SessionAlive = s.sessionAlive
	s.metrics = newMetrics(time.Now(), gauges{conns: s.gw.Connections, agents: svc.AgentSlotStats, openRooms: svc.OpenRooms, approvals: svc.ApprovalsByGrant,
		soleMembers: svc.SoleMemberGrants})

	r := chi.NewRouter()
	// Outside Recoverer, so a handler that panics is counted with the 500
	// Recoverer writes: that is exactly when an operator looks.
	r.Use(s.metrics.middleware)
	r.Use(middleware.Recoverer)
	if s.webDir != "" {
		r.Use(s.securityHeaders)
	}
	r.Use(requestLog(log, s.clientIP))
	// Host allow-list first (DNS rebinding, design §1.5), then the cross-origin
	// check, except signed alert routes and GET /healthz and /readyz (hostGuard).
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
	// Room links from Slack and alerts (deeplink.go).
	r.Get("/rooms/{id}", s.handleRoomDeepLink(notFound.ServeHTTP))
	r.Head("/rooms/{id}", s.handleRoomDeepLink(notFound.ServeHTTP))

	r.Get(pathHealthz, s.handleHealthz)
	r.Get(pathReadyz, s.handleReadyz)
	r.Get(pathMetrics, s.metrics.handle)

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
			// The login picker: before login, so outside authenticate.
			a.Get("/users", s.handleAuthUsers)
		}
	})

	r.Route("/api", func(api chi.Router) {
		// Signed alert routes: the sender's signature or token is the auth, so
		// no session. They take the sender's own content type (no requireJSON).
		// A source without a secret answers 401.
		api.With(middleware.Timeout(requestTimeout)).Post("/alerts/{source}", s.handleSignedAlert)
		// The signed change webhook (X4): the same scheme, its own secret.
		if s.changes != nil {
			api.With(middleware.Timeout(requestTimeout)).Post("/changes/webhook", s.changes.ServeHTTP)
		}

		// The setup runner's WebSocket (PIPEROOM_SETUP_ENABLED=on): the
		// credential is the pairing code in its first frame, not a session, so
		// it sits outside the protected group like the signed alerts.
		if s.setupEnabled {
			api.Get("/runners/ws", s.handleRunnerWS)
		}

		api.Group(func(protected chi.Router) {
			protected.Use(s.authenticate)
			protected.Use(viewerGuard)
			protected.Use(requireJSON)

			// Every /rooms/{id} route answers 404 for a room outside the
			// caller's tenants before its handler runs (tenancy.go);
			// approve, reject and accept check inside the service.
			visible := s.roomVisible

			// The WebSocket authenticates by the session cookie (or, in dev
			// mode, X-User for curl and wscat) like every other route, and
			// runs outside the request timeout: the connection outlives the
			// request. Replay has its own deadline in the gateway.
			protected.Get("/rooms/{id}/ws", visible(s.gw.ServeHTTP))
			// The rooms list socket (F6): filtered per person in the gateway.
			protected.Get("/rooms/ws", s.gw.ServeRoomsHTTP)

			// The hand-off route is exempt from the default budget.
			protected.Post("/rooms/{id}/handoffs", withTimeout(handoffTimeout, visible(s.handleCreateHandoff)))
			// The audit export reads the whole log, so it has its own budget.
			protected.Get("/rooms/{id}/export", withTimeout(exportTimeout, visible(s.handleExportRoom)))
			// The post-mortem draft reads the whole log too.
			protected.Get("/rooms/{id}/postmortem.md", withTimeout(exportTimeout, visible(s.handleRoomPostmortem)))
			// Deletion may write the same export to the archive first. Not
			// wrapped in visible: the admin check answers first (403 for
			// anyone else, whatever the id), then DeleteRoom answers 404 for
			// a room, live or in its audit row, outside the admin's tenants.
			protected.Delete("/rooms/{id}", withTimeout(deleteTimeout, s.handleDeleteRoom))

			protected.Group(func(short chi.Router) {
				short.Use(middleware.Timeout(requestTimeout))

				short.Get("/me", s.handleMe)
				short.Get("/people", s.handlePeople)
				short.Get("/rooms", s.handleListRooms)
				short.Post("/rooms", s.handleCreateRoom)
				// Setup rooms (setup.go; U wave). Mounted always: with setup
				// off the service answers 404 setup_disabled, as docs/api.md
				// says.
				short.Post("/setup", s.handleSetupCreate)
				short.Post("/rooms/{id}/setup/targets", visible(s.handleSetupAddTarget))
				short.Post("/rooms/{id}/setup/targets/{t}/class", visible(s.handleSetupRaiseClass))
				short.Post("/rooms/{id}/setup/pipeline", visible(s.handleSetupExtendPipeline))
				short.Post("/rooms/{id}/setup/maintenance-window", visible(s.handleSetupMaintenanceWindow))
				short.Get("/actions", s.handleActions)
				if !s.authSettings.IsOIDC() {
					// Unsigned: dev mode only (design §4). In oidc mode this
					// path is not mounted and falls to /alerts/{source}, which
					// answers 404 for the unknown source "webhook".
					short.Post("/alerts/webhook", s.handleAlertWebhook)
				}
				short.Get("/rooms/{id}", visible(s.handleGetRoom))
				short.Get("/rooms/{id}/events", visible(s.handleRoomEvents))
				short.Post("/rooms/{id}/comments", visible(s.handleAddComment))
				short.Post("/rooms/{id}/steer", visible(s.handleSteer))
				short.Post("/rooms/{id}/suggestions/{sid}/send", visible(s.handleSendSuggestion))
				short.Post("/rooms/{id}/suggestions/{sid}/decline", visible(s.handleDeclineSuggestion))
				short.Post("/rooms/{id}/hypothesis", visible(s.handleHypothesisVerdict))
				short.Post("/rooms/{id}/nudge", visible(s.handleNudge))
				short.Post("/rooms/{id}/proposals", visible(s.handleCreateProposal))
				short.Post("/rooms/{id}/control/request", visible(s.handleRequestControl))
				short.Post("/rooms/{id}/control/grant", visible(s.handleGrantControl))
				short.Post("/rooms/{id}/close", visible(s.handleCloseRoom))
				short.Post("/rooms/{id}/extend", visible(s.handleExtendRoom))
				short.Post("/rooms/{id}/trace", visible(s.handleTrace))
				short.Post("/rooms/{id}/label", visible(s.handleLabel))
				short.Post("/rooms/{id}/checklist", visible(s.handleChecklist))
				short.Post("/rooms/{id}/shadow/blind", visible(s.handleShadowBlind))
				short.Get("/rooms/{id}/shadow", visible(s.handleShadow))
				short.Post("/rooms/{id}/shadow/grade", visible(s.handleShadowGrade))
				short.Get("/shadow/metrics", s.handleShadowMetrics)
				short.Get("/rooms/{id}/usage", visible(s.handleRoomUsage))
				short.Get("/usage", s.handlePipelineUsage)
				short.Post("/proposals/{pid}/approve", s.handleApproveProposal)
				short.Post("/proposals/{pid}/approve-all", s.handleApproveAllProposal)
				short.Post("/proposals/{pid}/cancel", s.handleCancelProposal)
				short.Post("/proposals/{pid}/reject", s.handleRejectProposal)
				short.Post("/handoffs/{hid}/accept", s.handleAcceptHandoff)
				short.Get("/pipelines", s.handlePipelines)
				short.Get("/pipelines/{pid}/topology", s.pipelineVisible(s.handleTopology))
				short.Get("/pipelines/{pid}/agent-stats", s.handleAgentStats)
				short.Get("/pipelines/{pid}/trends", s.handleTrends)
				// The scripted demo's presenter controls; no-op outside --demo.
				s.mountDemo(short)
			})
		})

		// Admin settings (WithAdmin; admin.go): their own group, so the
		// admin check answers before the content type and body checks.
		s.mountAdmin(api)
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
// stops the room service. It also cancels every runner WebSocket connection
// context, so a runner that keeps pinging cannot leave its handler or socket
// alive through shutdown.
func (s *Server) Close() {
	if s.webRoot != nil {
		_ = s.webRoot.Close()
	}
	s.runnerCancel()
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

// decodeBody decodes the JSON request body into dst, refusing bodies over
// maxBody (docs/api.md: 1 MB).
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	return decodeBodyLimit(w, r, dst, maxBody)
}

// decodeBodyLimit is decodeBody with an explicit cap, for routes that accept
// less than maxBody. A body over the cap is 413 too_large, not invalid JSON:
// the JSON may be well formed, and "invalid JSON" would send the caller
// debugging its payload instead of its size (KAN-388, docs/api.md 413).
func decodeBodyLimit(w http.ResponseWriter, r *http.Request, dst any, limit int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return bodyTooLarge(limit)
		}
		return validation("invalid JSON body")
	}
	return nil
}

// readBody reads the whole request body up to limit, for handlers that need
// the raw bytes (the alert webhook). Over the cap is the same 413 as
// decodeBodyLimit; any other read error is a 400. Neither message echoes the
// body.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	raw, err := io.ReadAll(r.Body)
	if err == nil {
		return raw, nil
	}
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return nil, bodyTooLarge(limit)
	}
	return nil, validation("unreadable request body")
}

// bodyTooLarge is docs/api.md's 413 too_large for a body over its route's cap.
// The message gives the limit, never any part of the body.
func bodyTooLarge(limit int64) *room.Error {
	return &room.Error{
		Status: http.StatusRequestEntityTooLarge,
		Code:   "too_large",
		Msg:    "request body too large (limit " + byteLimit(limit) + ")",
	}
}

// byteLimit renders a body cap the way the 413 message reads it ("1 MiB").
func byteLimit(n int64) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return strconv.FormatInt(n>>20, 10) + " MiB"
	case n >= 1<<10 && n%(1<<10) == 0:
		return strconv.FormatInt(n>>10, 10) + " KiB"
	default:
		return strconv.FormatInt(n, 10) + " bytes"
	}
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
	// The room log is past its cap and its reserve (docs/api.md, Room limits).
	if errors.Is(err, store.ErrLogFull) {
		writeError(w, http.StatusConflict, "log_full", "this room's log is full; it can still be closed")
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
