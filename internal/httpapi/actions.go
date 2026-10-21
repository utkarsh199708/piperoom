package httpapi

import (
	"net/http"

	"piperoom/internal/actions"
)

// actionResponse is one entry of GET /api/actions (docs/api.md, P3c): what the
// web form (P3d) and the agent's proposal registry are generated from. It
// never includes client state, connection settings or credentials.
type actionResponse struct {
	Name         string         `json:"name"`
	Layer        string         `json:"layer"`
	Describe     string         `json:"describe"`
	ParamsSchema map[string]any `json:"params_schema"`
	Preview      bool           `json:"preview"`
}

// handleActions serves GET /api/actions: the full write-action registry in
// registration order, listed whether or not write mode is on (docs/api.md).
func (s *Server) handleActions(w http.ResponseWriter, _ *http.Request) {
	acts := s.svc.AllActions()
	out := make([]actionResponse, 0, len(acts))
	for _, a := range acts {
		_, preview := a.(actions.Preview)
		out = append(out, actionResponse{
			Name:         a.Name(),
			Layer:        a.Layer(),
			Describe:     a.Describe(),
			ParamsSchema: a.ParamsSchema(),
			Preview:      preview,
		})
	}
	writeJSON(w, http.StatusOK, out)
}
