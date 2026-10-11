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
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/ws"
)

// maxBody is the request body cap (docs/api.md: 1 MB).
const maxBody = 1 << 20

// Server wires the chi router to the room service.
type Server struct {
	svc *room.Service
	gw  *ws.Gateway
	log *slog.Logger
}

// New builds the HTTP handler (router). store is passed to the WebSocket
// gateway for replay + live events.
func New(svc *room.Service, st store.Store, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{svc: svc, gw: ws.NewGateway(svc, st, log), log: log}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(cors)
	r.Use(requestLog(log))
	r.Use(middleware.Timeout(60 * time.Second))

	r.Route("/api", func(api chi.Router) {
		// WebSocket authenticates via ?user= (browsers cannot set headers).
		api.Get("/rooms/{id}/ws", s.gw.ServeHTTP)

		api.Group(func(protected chi.Router) {
			protected.Use(authenticate(svc))

			protected.Get("/me", s.handleMe)
			protected.Get("/rooms", s.handleListRooms)
			protected.Post("/rooms", s.handleCreateRoom)
			protected.Post("/alerts/webhook", s.handleAlertWebhook)
			protected.Get("/rooms/{id}", s.handleGetRoom)
			protected.Get("/rooms/{id}/events", s.handleRoomEvents)
			protected.Post("/rooms/{id}/comments", s.handleAddComment)
			protected.Post("/rooms/{id}/steer", s.handleSteer)
			protected.Post("/rooms/{id}/suggestions/{sid}/send", s.handleSendSuggestion)
			protected.Post("/rooms/{id}/control/request", s.handleRequestControl)
			protected.Post("/rooms/{id}/control/grant", s.handleGrantControl)
			protected.Post("/rooms/{id}/close", s.handleCloseRoom)
			protected.Post("/rooms/{id}/handoffs", s.handleCreateHandoff)
			protected.Post("/rooms/{id}/trace", s.handleTrace)
			protected.Post("/proposals/{pid}/approve", s.handleApproveProposal)
			protected.Post("/proposals/{pid}/reject", s.handleRejectProposal)
			protected.Post("/handoffs/{hid}/accept", s.handleAcceptHandoff)
			protected.Get("/pipelines/{pid}/topology", s.handleTopology)
		})
	})
	return r
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
			log.Info("http request", "method", r.Method, "path", r.URL.Path, "status", ww.Status(), "dur", time.Since(start).Round(time.Millisecond).String())
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
// 500 internal_error.
func writeErr(w http.ResponseWriter, err error) {
	var re *room.Error
	if errors.As(err, &re) {
		writeError(w, re.Status, re.Code, re.Msg)
		return
	}
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
