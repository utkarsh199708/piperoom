package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// RouteTable returns "METHOD /pattern" for every route of s, for the tenancy
// route test (tests only).
func RouteTable(s *Server) []string {
	var out []string
	r, ok := s.handler.(chi.Routes)
	if !ok {
		return nil
	}
	_ = chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		out = append(out, method+" "+route)
		return nil
	})
	return out
}
