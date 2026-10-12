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

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	if s.webDir != "" {
		r.Use(s.securityHeaders)
	} else {
		// The dev UI is a separate origin and needs CORS. When the server
		// serves the UI itself, everything is same-origin and CORS stays off.
		r.Use(cors)
	}
	r.Use(requestLog(log))
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

	r.Route("/api", func(api chi.Router) {
		// WebSocket authenticates via ?user= (browsers cannot set headers) and
		// runs outside the request timeout: the connection outlives the
		// request. Replay has its own deadline in the gateway.
		api.Get("/rooms/{id}/ws", s.gw.ServeHTTP)

		// Signed alert routes: the sender's signature or token is the auth, so
		// no X-User. A source without a secret answers 401.
		api.With(middleware.Timeout(requestTimeout)).Post("/alerts/{source}", s.handleSignedAlert)

		api.Group(func(protected chi.Router) {
			protected.Use(authenticate(svc))

			// The hand-off route is exempt from the default budget.
			protected.Post("/rooms/{id}/handoffs", withTimeout(handoffTimeout, s.handleCreateHandoff))

			protected.Group(func(short chi.Router) {
				short.Use(middleware.Timeout(requestTimeout))

				short.Get("/me", s.handleMe)
				short.Get("/rooms", s.handleListRooms)
				short.Post("/rooms", s.handleCreateRoom)
				short.Post("/alerts/webhook", s.handleAlertWebhook)
				short.Get("/rooms/{id}", s.handleGetRoom)
				short.Get("/rooms/{id}/events", s.handleRoomEvents)
				short.Post("/rooms/{id}/comments", s.handleAddComment)
				short.Post("/rooms/{id}/steer", s.handleSteer)
				short.Post("/rooms/{id}/suggestions/{sid}/send", s.handleSendSuggestion)
				short.Post("/rooms/{id}/control/request", s.handleRequestControl)
				short.Post("/rooms/{id}/control/grant", s.handleGrantControl)
				short.Post("/rooms/{id}/close", s.handleCloseRoom)
				short.Post("/rooms/{id}/trace", s.handleTrace)
				short.Post("/proposals/{pid}/approve", s.handleApproveProposal)
				short.Post("/proposals/{pid}/reject", s.handleRejectProposal)
				short.Post("/handoffs/{hid}/accept", s.handleAcceptHandoff)
				short.Get("/pipelines/{pid}/topology", s.handleTopology)
			})
		})
	})
	if s.webDir != "" {
		// Same-origin only (the library always accepts the request's own
		// host), plus the validated public host. The dev origins are for the
		// Vite dev server, which this mode does not use.
		s.gw.OriginPatterns = []string{}
		if s.public.Host != "" {
			s.gw.OriginPatterns = append(s.gw.OriginPatterns, s.public.Host)
		}
	}
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

// ---- auth ----

type ctxKey int

const userKey ctxKey = 1

// authenticate resolves X-User and stores the user in the context.
func authenticate(svc *room.Service) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-User")
			if id == "" {
				writeError(w, http.StatusUnauthorized, "unauthenticated", "missing X-User header")
				return
			}
			u, ok := svc.LookupUser(id)
			if !ok {
				writeError(w, http.StatusUnauthorized, "unauthenticated", "unknown user "+id)
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, u)))
		})
	}
}

func userFrom(ctx context.Context) room.User {
	u, _ := ctx.Value(userKey).(room.User)
	return u
}

// cors allows the A6 dev UI.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:5173")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-User")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requestLog(log *slog.Logger) func(http.Handler) http.Handler {
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
			log.Log(r.Context(), level, "http request", "method", r.Method, "path", r.URL.Path, "status", ww.Status(), "dur", time.Since(start).Round(time.Millisecond).String())
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
