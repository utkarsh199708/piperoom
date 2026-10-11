package httpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
)

type ctxKey int

const (
	ctxUser ctxKey = iota
	ctxReqInfo
)

// reqInfo is filled in by inner middleware so the outer request logger can
// report who made the call without reading headers itself.
type reqInfo struct {
	user string // authenticated user id; empty when the call was not authenticated
}

// userFrom returns the authenticated user the auth middleware stored.
func userFrom(ctx context.Context) room.User {
	u, _ := ctx.Value(ctxUser).(room.User)
	return u
}

func routeContext(r *http.Request) *chi.Context { return chi.RouteContext(r.Context()) }

// statusRecorder remembers the status and size for the request log. Unwrap
// lets the WebSocket library reach the real http.Hijacker underneath.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(p []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(p)
	s.bytes += int64(n)
	return n, err
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// requestLogger logs one line per request: method, route, status, size,
// duration and the authenticated user id. It never logs bodies, query strings
// or the raw X-User header (an unauthenticated caller controls that text).
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			info := &reqInfo{}
			r = r.WithContext(context.WithValue(r.Context(), ctxReqInfo, info))
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)

			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			level := slog.LevelInfo
			switch {
			case status >= 500:
				level = slog.LevelError
			case status >= 400:
				level = slog.LevelWarn
			}
			// Probes hit the server every few seconds; logging each one would
			// drown real traffic. Readiness failures reach Config.OnNotReady.
			if isProbePath(r.URL.Path) {
				level = slog.LevelDebug
			}
			log.LogAttrs(r.Context(), level, "http request",
				slog.String("method", r.Method),
				slog.String("route", routePattern(r)),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int64("bytes", rec.bytes),
				slog.Duration("duration", time.Since(start)),
				slog.String("user", info.user),
				slog.String("remote", r.RemoteAddr),
			)
		})
	}
}

// recoverer turns a handler panic into a 500 instead of a dropped connection.
// http.ErrAbortHandler is the net/http way to abort on purpose, so it passes.
func recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				v := recover()
				if v == nil {
					return
				}
				if v == http.ErrAbortHandler {
					panic(v)
				}
				log.Error("httpapi: handler panic", "method", r.Method, "route", routePattern(r), "panic", fmt.Sprint(v))
				writeError(w, http.StatusInternalServerError, codeInternal, genericInternalMessage)
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// corsMiddleware allows browser calls from the listed origins only. Matching
// is exact (scheme, host and port); there is no wildcard on purpose. It runs
// before authentication so that a preflight, which carries no X-User, and the
// 401 answers both reach the page.
func corsMiddleware(allowed []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			h.Add("Vary", "Origin")
			origin := r.Header.Get("Origin")
			ok := origin != "" && slices.Contains(allowed, origin)
			if ok {
				h.Set("Access-Control-Allow-Origin", origin)
			}
			preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
			if preflight {
				if ok {
					h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
					h.Set("Access-Control-Allow-Headers", "X-User, Content-Type")
					h.Set("Access-Control-Max-Age", "600")
				}
				// A preflight from an origin that is not allowed gets no CORS
				// headers, which makes the browser block the real request.
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// timeoutMiddleware gives every request in the group a deadline. The service
// honours the context; a handler that ran out of time answers 504 (fail).
func timeoutMiddleware(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// bodyLimit caps the request body; handlers see *http.MaxBytesError on overflow.
func bodyLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// auth resolves X-User through the service. Unknown or missing users get 401;
// the header value is never logged or echoed.
func (a *api) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-User")
		if id == "" {
			writeError(w, http.StatusUnauthorized, codeUnauthenticated, "Send the X-User header with a known user id.")
			return
		}
		u, ok := a.svc.Authenticate(id)
		if !ok {
			writeError(w, http.StatusUnauthorized, codeUnauthenticated, "Unknown user.")
			return
		}
		ctx := context.WithValue(r.Context(), ctxUser, u)
		if info, ok := ctx.Value(ctxReqInfo).(*reqInfo); ok {
			info.user = u.ID
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
