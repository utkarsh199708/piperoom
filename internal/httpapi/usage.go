package httpapi

import (
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
	"piperoom/internal/store"
)

// maxPipelineIDLen bounds the pipeline query parameter of GET /api/usage.
const maxPipelineIDLen = 200

// UsageVisibility is what the usage routes check before any role check
// (tenancy, docs/design/tenancy.md): a room or pipeline the caller cannot see
// answers 404. *room.Service implements it with an adapter until #112 merges;
// it then switches to #112's roomVisible / CanSeePipeline.
type UsageVisibility interface {
	UsageRoomVisible(user room.User, snap room.RoomSnapshot) bool
	UsagePipelineVisible(user room.User, pipelineID string) bool
}

// Spend configures the usage routes (docs/api.md, docs/design/cost.md 4).
type Spend struct {
	// Store holds the spend tables; nil = both routes answer 404 (they stay
	// mounted, so route walks see them).
	Store store.SpendStore
	// Visibility decides what a caller can see; nil = the room service.
	Visibility UsageVisibility
	// The configured caps, shown as "limit"; 0 = off.
	RoomTokens           int64
	PipelineDailyTokens  int64
	PipelineDailyCostUSD float64
	// Now picks the default day; nil = time.Now.
	Now func() time.Time
}

// WithSpend serves GET /api/rooms/{id}/usage and GET /api/usage.
func WithSpend(sp Spend) Option {
	return func(s *Server) {
		if sp.Now == nil {
			sp.Now = time.Now
		}
		s.spend = sp
	}
}

// usageRow is the Usage object of docs/api.md.
type usageRow struct {
	Model                    string   `json:"model,omitempty"`
	Calls                    int64    `json:"calls"`
	UnpricedCalls            int64    `json:"unpriced_calls"`
	InputTokens              int64    `json:"input_tokens"`
	CacheCreationInputTokens int64    `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64    `json:"cache_read_input_tokens"`
	OutputTokens             int64    `json:"output_tokens"`
	Tokens                   int64    `json:"tokens"`
	CostUSD                  *float64 `json:"cost_usd"`
}

// toUsageRow renders one breakdown row. The cost is null when no request in
// the row had a price: an unknown cost is never shown as 0.
func toUsageRow(u store.SpendUsage) usageRow {
	r := usageRow{
		Model:                    u.Model,
		Calls:                    u.Calls,
		UnpricedCalls:            u.UnpricedCalls,
		InputTokens:              u.InputTokens,
		CacheCreationInputTokens: u.CacheCreationInputTokens,
		CacheReadInputTokens:     u.CacheReadInputTokens,
		OutputTokens:             u.OutputTokens,
		Tokens:                   u.Tokens(),
	}
	if u.Calls > u.UnpricedCalls {
		r.CostUSD = usd(u.CostMicros)
	}
	return r
}

func usd(micros int64) *float64 {
	v := float64(micros) / 1e6
	return &v
}

// charged is what the caps check: finished requests plus estimates in flight.
type charged struct {
	Tokens  int64    `json:"tokens"`
	CostUSD *float64 `json:"cost_usd"`
}

type usageBody struct {
	Charged charged    `json:"charged"`
	Totals  usageRow   `json:"totals"`
	ByModel []usageRow `json:"by_model"`
}

func newUsageBody(rep store.SpendReport) usageBody {
	b := usageBody{ByModel: make([]usageRow, 0, len(rep.ByModel))}
	totals := rep.Totals()
	b.Totals = toUsageRow(totals)
	b.Charged = charged{Tokens: rep.ChargedTokens}
	// A charged cost exists only once some request had a price: estimates are
	// 0 for an unpriced model, so 0 here would read as a known cost.
	if totals.Calls > totals.UnpricedCalls || rep.ChargedCostMicros != 0 {
		b.Charged.CostUSD = usd(rep.ChargedCostMicros)
	}
	for _, u := range rep.ByModel {
		b.ByModel = append(b.ByModel, toUsageRow(u))
	}
	return b
}

type roomUsageResponse struct {
	RoomID     string `json:"room_id"`
	PipelineID string `json:"pipeline_id"`
	Limit      struct {
		Tokens int64 `json:"tokens"`
	} `json:"limit"`
	usageBody
}

type pipelineUsageResponse struct {
	PipelineID string `json:"pipeline_id"`
	Day        string `json:"day"`
	Limit      struct {
		Tokens  int64   `json:"tokens"`
		CostUSD float64 `json:"cost_usd"`
	} `json:"limit"`
	usageBody
}

// errNotFound is the body of a missing room (room.mapStoreErr), so a room the
// caller cannot see is told apart from nothing.
var errNotFound = &room.Error{Status: http.StatusNotFound, Code: "not_found", Msg: "not found"}

// visibility returns the configured visibility check.
func (s *Server) visibility() UsageVisibility {
	if s.spend.Visibility != nil {
		return s.spend.Visibility
	}
	return s.svc
}

// handleRoomUsage serves GET /api/rooms/{id}/usage: anyone who can see and
// read the room, since its agent.usage events carry the same numbers. A room
// the caller cannot see is a 404 like a missing one. A shadow room's spend
// stays hidden until it closes, like the rest of its run.
func (s *Server) handleRoomUsage(w http.ResponseWriter, r *http.Request) {
	if s.spend.Store == nil {
		s.writeErr(w, errNotFound)
		return
	}
	snap, err := s.svc.GetRoom(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	if !s.visibility().UsageRoomVisible(userFrom(r.Context()), snap) {
		s.writeErr(w, errNotFound)
		return
	}
	if snap.Shadow && snap.Status != "closed" {
		writeError(w, http.StatusConflict, "room_open", "a shadow room's usage is shown after it closes")
		return
	}
	rep, err := s.spend.Store.RoomSpend(r.Context(), snap.ID)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	out := roomUsageResponse{RoomID: snap.ID, PipelineID: snap.PipelineID, usageBody: newUsageBody(rep)}
	out.Limit.Tokens = s.spend.RoomTokens
	writeJSON(w, http.StatusOK, out)
}

// handlePipelineUsage serves GET /api/usage?pipeline=&day=. Visibility comes
// first: an unknown pipeline, or one outside the caller's tenants, is 404
// before any role check, so the 403 never confirms that a pipeline exists.
// Then the pipeline's members by the export rule, the solo user, and auditors.
func (s *Server) handlePipelineUsage(w http.ResponseWriter, r *http.Request) {
	if s.spend.Store == nil {
		s.writeErr(w, errNotFound)
		return
	}
	q := r.URL.Query()
	pipelineID := q.Get("pipeline")
	if pipelineID == "" {
		s.writeErr(w, validation("pipeline is required"))
		return
	}
	if len(pipelineID) > maxPipelineIDLen || !utf8.ValidString(pipelineID) || strings.ContainsRune(pipelineID, 0) {
		s.writeErr(w, validation("pipeline is not a valid pipeline id"))
		return
	}
	day := q.Get("day")
	if day == "" {
		day = s.spend.Now().UTC().Format(time.DateOnly)
	} else if store.CheckDay(day) != nil {
		s.writeErr(w, validation("day must be a YYYY-MM-DD date (UTC) from 2000-01-01 to 2999-12-31"))
		return
	}
	user := userFrom(r.Context())
	if !s.visibility().UsagePipelineVisible(user, pipelineID) {
		writeError(w, http.StatusNotFound, "not_found", "pipeline not found")
		return
	}
	if !s.svc.CanReadPipelineUsage(user, pipelineID) {
		writeError(w, http.StatusForbidden, "not_allowed", "reading a pipeline's usage needs an approver or backup of one of its layers, or the auditor role")
		return
	}
	rep, err := s.spend.Store.PipelineSpend(r.Context(), pipelineID, day)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	out := pipelineUsageResponse{PipelineID: pipelineID, Day: day, usageBody: newUsageBody(rep)}
	out.Limit.Tokens = s.spend.PipelineDailyTokens
	out.Limit.CostUSD = s.spend.PipelineDailyCostUSD
	writeJSON(w, http.StatusOK, out)
}
