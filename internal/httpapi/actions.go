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
	// LayerByParams: an actions.Layered action, whose step's layer comes
	// from its params and the room's pipeline (actions.StepLayer); Layer is
	// then "". Deferred: an actions.Deferred action (a fix PR). Both are
	// omitted when false (Q7).
	LayerByParams bool `json:"layer_by_params,omitempty"`
	Deferred      bool `json:"deferred,omitempty"`
}

// handleActions serves GET /api/actions: the full write-action registry in
// registration order, listed whether or not write mode is on (docs/api.md).
// It has no room, so Layer is a.Layer(): "postgres" for an
// actions.SourceLayered action even where a MySQL room's step needs "mysql".
// The room service derives a driver step's layer (actions.StepLayer) and takes
// this catalogue value, or "", as standing for it; no per-room actions route
// exists to serve the derived layer.
func (s *Server) handleActions(w http.ResponseWriter, _ *http.Request) {
	acts := s.svc.AllActions()
	out := make([]actionResponse, 0, len(acts))
	for _, a := range acts {
		_, preview := a.(actions.Preview)
		_, layered := a.(actions.Layered)
		_, deferred := a.(actions.Deferred)
		layer := a.Layer()
		if layered {
			// The layer is the step's, never the action's (docs/api.md).
			layer = ""
		}
		out = append(out, actionResponse{
			Name:          a.Name(),
			Layer:         layer,
			Describe:      a.Describe(),
			ParamsSchema:  a.ParamsSchema(),
			Preview:       preview,
			LayerByParams: layered,
			Deferred:      deferred,
		})
	}
	writeJSON(w, http.StatusOK, out)
}
