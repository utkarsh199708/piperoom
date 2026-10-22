package httpapi

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
)

// The fleet view (X5a; docs/api.md "Fleet view"): GET /api/fleet and
// GET /api/fleet/changes. Both are built from Piperoom's own tables only (the
// watcher's status rows, pipeline_changes, rooms and their events): no route
// here reads a customer system, so the fleet stays cheap with 50+ pipelines.
// Both are aggregates: a pipeline outside the caller's tenants is left out and
// its rows are never counted.

const (
	// fleetTTL is how long the store facts behind GET /api/fleet are reused.
	fleetTTL = 10 * time.Second
	// fleetReadTimeout is the store read deadline of docs/api.md.
	fleetReadTimeout = 10 * time.Second
	// fleetStaleIntervals: a status row older than this many watch intervals
	// is not "checked recently" and the pipeline's health is unknown.
	fleetStaleIntervals = 3
	// fleetMaxForecasts bounds FleetPipeline.Forecasts.
	fleetMaxForecasts = 8
	// fleetClosedRooms and fleetVerdictRooms bound the last-verdict lookup:
	// the newest closed rooms of all pipelines are read once, and at most
	// fleetVerdictRooms of one pipeline are opened for their verdict events.
	fleetClosedRooms  = 1000
	fleetVerdictRooms = 5
	fleetVerdictEvent = 50
	// Paging of GET /api/fleet/changes.
	fleetChangesDefaultLimit = 100
	fleetChangesDefaultDays  = 7
)

// Fleet wires the fleet routes (WithFleet). Every field is optional: a nil
// reader shows what it would have read as absent (no status = health
// unknown, no change reader = empty pages), never an error.
type Fleet struct {
	// Owners gives each pipeline its tenant and team, and the pipeline list
	// the shared facts cover.
	Owners *owners.Owners
	// Status is the watcher's status rows (the room store's watch_status).
	Status clients.WatchStatusReader
	// Changes pages pipeline_changes across pipelines.
	Changes clients.FleetChangeReader
	// Counts counts each pipeline's changes of the last 24 hours.
	Counts ChangeCounter
	// Rooms lists the room rows (open rooms per pipeline); Closed reads the
	// closed rooms and their verdict events (last verdict).
	Rooms  RoomLister
	Closed store.ClosedRoomReader
	// WatchOff is PIPEROOM_WATCH=off: every pipeline's health is unknown.
	WatchOff bool
	// WatchInterval is PIPEROOM_WATCH_INTERVAL; zero means 60 s.
	WatchInterval time.Duration
	// ChangesRetentionDays is PIPEROOM_CHANGES_RETENTION_DAYS: the earliest
	// since of GET /api/fleet/changes. Zero means no floor.
	ChangesRetentionDays int
}

// ChangeCounter counts changes per pipeline (store.FleetStore).
type ChangeCounter interface {
	CountChangesSince(ctx context.Context, pipelineIDs []string, since time.Time) (map[string]int, error)
}

// RoomLister lists room rows (store.Store).
type RoomLister interface {
	ListRooms(ctx context.Context) ([]store.Room, error)
}

// WithFleet serves the fleet routes from f. Without it both routes still
// answer: every pipeline the caller can see with health unknown, and empty
// change pages.
func WithFleet(f Fleet) Option {
	return func(s *Server) { s.fleet.cfg = f }
}

// FleetPipeline is one row of GET /api/fleet.
type FleetPipeline struct {
	PipelineID    string          `json:"pipeline_id"`
	Title         string          `json:"title"`
	Kind          string          `json:"kind"`
	Tenant        string          `json:"tenant"`
	Team          string          `json:"team"`
	Health        string          `json:"health"`
	WorstNode     string          `json:"worst_node"`
	OpenRooms     int             `json:"open_rooms"`
	SignalsFiring []string        `json:"signals_firing"`
	Changes24h    int             `json:"changes_24h"`
	Forecasts     []FleetForecast `json:"forecasts"`
	LastVerdict   *FleetVerdict   `json:"last_verdict"`
	CheckedAt     *time.Time      `json:"checked_at,omitempty"`
}

// FleetForecast is one firing forecast of a pipeline.
type FleetForecast struct {
	Metric string    `json:"metric"`
	ETA    time.Time `json:"eta"`
}

// FleetVerdict is the human judgement of a pipeline's last judged room.
type FleetVerdict struct {
	RoomID       string    `json:"room_id"`
	Verdict      string    `json:"verdict"`
	LayerCorrect *bool     `json:"layer_correct"`
	At           time.Time `json:"at"`
}

// FleetChangesPage is the body of GET /api/fleet/changes.
type FleetChangesPage struct {
	Changes []clients.PipelineChange `json:"changes"`
	Next    string                   `json:"next"`
}

// Health words, worst first (the sort order of GET /api/fleet).
var fleetHealthOrder = []string{"fail", "warn", "unknown", "ok"}

// fleetView is the fleet routes' state: the wiring and the shared facts cache.
type fleetView struct {
	cfg Fleet
	ttl time.Duration // 0 = fleetTTL; < 0 = no cache (tests)

	mu       sync.Mutex // one computation at a time; guards the fields below
	facts    *fleetFacts
	at       time.Time
	verdicts map[string]verdictMemo // closed room id -> its verdict at a last_seq
}

// fleetFacts is what the store says about a set of pipelines at one time.
// Facts are not per caller: each request filters them by what the caller can
// see, so a pipeline outside the caller's tenants never reaches a row.
type fleetFacts struct {
	ids       map[string]bool
	status    map[string]clients.WatchStatus
	changes   map[string]int
	open      map[string]int
	verdict   map[string]*FleetVerdict
	changesAt time.Time
}

type verdictMemo struct {
	seq int64
	v   *FleetVerdict // nil: the room has no verdict and no grade
}

// handleFleet serves GET /api/fleet?tenant=&team=&kind=.
func (s *Server) handleFleet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	kind := q.Get("kind")
	if kind != "" && kind != events.PipelineKindCDC && kind != events.PipelineKindBatch {
		s.writeErr(w, validation("unknown kind"))
		return
	}
	tenant, team := q.Get("tenant"), q.Get("team")
	u := userFrom(r.Context())
	var infos []room.PipelineInfo
	for _, p := range s.svc.PipelinesFor(u) {
		pt, tm := s.fleet.tenantTeam(p.ID)
		if (tenant != "" && pt != tenant) || (team != "" && tm != team) || (kind != "" && p.Kind != kind) {
			continue
		}
		infos = append(infos, p)
	}
	out := []FleetPipeline{}
	if len(infos) == 0 {
		writeJSON(w, http.StatusOK, out)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fleetReadTimeout)
	defer cancel()
	now := s.now().UTC()
	ids := make([]string, len(infos))
	for i, p := range infos {
		ids[i] = p.ID
	}
	facts, err := s.fleet.get(ctx, ids, now)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	for _, p := range infos {
		out = append(out, s.fleet.row(p, facts, now))
	}
	sort.SliceStable(out, func(i, j int) bool {
		hi, hj := slices.Index(fleetHealthOrder, out[i].Health), slices.Index(fleetHealthOrder, out[j].Health)
		if hi != hj {
			return hi < hj
		}
		return out[i].PipelineID < out[j].PipelineID
	})
	writeJSON(w, http.StatusOK, out)
}

// tenantTeam returns a pipeline's owners.yaml tenant and team: (X5-0), ""
// when unset. The owners loader has checked that a team of a teams: list
// serves the pipeline's own tenant.
func (f *fleetView) tenantTeam(pipelineID string) (tenant, team string) {
	if f.cfg.Owners == nil {
		return "", ""
	}
	p, ok := f.cfg.Owners.Pipeline(pipelineID)
	if !ok {
		return "", ""
	}
	return p.Tenant, p.Team
}

// get returns facts covering ids, from the cache when they are younger than
// fleetTTL. With Owners set the facts cover every owners.yaml pipeline, so
// every caller shares one computation per fleetTTL.
func (f *fleetView) get(ctx context.Context, ids []string, now time.Time) (*fleetFacts, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ttl := f.ttl
	if ttl == 0 {
		ttl = fleetTTL
	}
	if c := f.facts; c != nil && now.Sub(f.at) < ttl && !now.Before(f.at) && coversAll(c.ids, ids) {
		return c, nil
	}
	all := ids
	if f.cfg.Owners != nil {
		all = make([]string, 0, len(f.cfg.Owners.Pipelines))
		for _, p := range f.cfg.Owners.Pipelines {
			all = append(all, p.ID)
		}
		for _, id := range ids {
			if !slices.Contains(all, id) {
				all = append(all, id)
			}
		}
	}
	facts, err := f.compute(ctx, all, now)
	if err != nil {
		return nil, err
	}
	f.facts, f.at = facts, now
	return facts, nil
}

func coversAll(have map[string]bool, ids []string) bool {
	for _, id := range ids {
		if !have[id] {
			return false
		}
	}
	return true
}

// compute reads the facts of ids from the store. Caller holds f.mu.
func (f *fleetView) compute(ctx context.Context, ids []string, now time.Time) (*fleetFacts, error) {
	c := &fleetFacts{ids: map[string]bool{}, status: map[string]clients.WatchStatus{}, changes: map[string]int{},
		open: map[string]int{}, verdict: map[string]*FleetVerdict{}, changesAt: now}
	for _, id := range ids {
		c.ids[id] = true
	}
	if f.cfg.Status != nil {
		rows, err := f.cfg.Status.WatchStatuses(ctx, ids)
		if err != nil {
			return nil, fmt.Errorf("httpapi: fleet: watch status: %w", err)
		}
		for _, st := range rows {
			c.status[st.PipelineID] = st
		}
	}
	if f.cfg.Counts != nil {
		n, err := f.cfg.Counts.CountChangesSince(ctx, ids, now.Add(-24*time.Hour))
		if err != nil {
			return nil, fmt.Errorf("httpapi: fleet: count changes: %w", err)
		}
		c.changes = n
	}
	if f.cfg.Rooms != nil {
		rooms, err := f.cfg.Rooms.ListRooms(ctx)
		if err != nil {
			return nil, fmt.Errorf("httpapi: fleet: list rooms: %w", err)
		}
		for _, r := range rooms {
			// Shadow rooms count; setup rooms have no owners.yaml pipeline
			// yet, so they are never one of ids.
			if r.Status != "closed" && c.ids[r.PipelineID] {
				c.open[r.PipelineID]++
			}
		}
	}
	if f.cfg.Closed != nil {
		if err := f.lastVerdicts(ctx, c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// fleetVerdictTypes are the events a verdict is folded from.
var fleetVerdictTypes = []events.Type{events.HypothesisVerdict, events.ShadowGraded}

// lastVerdicts fills c.verdict: per pipeline, the newest closed room (of the
// fleetClosedRooms newest overall, at most fleetVerdictRooms per pipeline)
// that has a hypothesis.verdict or a shadow.graded. A room is read again only
// when its last_seq moved. Caller holds f.mu.
func (f *fleetView) lastVerdicts(ctx context.Context, c *fleetFacts) error {
	rooms, err := f.cfg.Closed.ListClosedRooms(ctx, "", fleetClosedRooms)
	if err != nil {
		return fmt.Errorf("httpapi: fleet: list closed rooms: %w", err)
	}
	memo := map[string]verdictMemo{}
	tried := map[string]int{}
	for _, r := range rooms { // newest close first
		pid := r.PipelineID
		if !c.ids[pid] || c.verdict[pid] != nil || tried[pid] >= fleetVerdictRooms {
			continue
		}
		tried[pid]++
		m, ok := f.verdicts[r.ID]
		if !ok || m.seq != r.LastSeq {
			evs, err := f.cfg.Closed.EventsOfTypes(ctx, r.ID, fleetVerdictTypes, fleetVerdictEvent)
			if err != nil {
				return fmt.Errorf("httpapi: fleet: verdict events of %s: %w", r.ID, err)
			}
			m = verdictMemo{seq: r.LastSeq, v: foldVerdict(r.ID, evs)}
		}
		memo[r.ID] = m
		if m.v != nil {
			c.verdict[pid] = m.v
		}
	}
	f.verdicts = memo
	return nil
}

// foldVerdict is a room's last hypothesis.verdict and last shadow.graded;
// nil when it has neither. At is the time of the later of the two. A payload
// that does not decode is skipped, as the snapshot fold does.
func foldVerdict(roomID string, evs []events.Event) *FleetVerdict {
	var v *FleetVerdict
	for _, e := range evs {
		switch e.Type {
		case events.HypothesisVerdict:
			p, err := events.Decode[events.HypothesisVerdictPayload](e)
			if err != nil || (p.Verdict != "confirmed" && p.Verdict != "disputed") {
				continue
			}
			if v == nil {
				v = &FleetVerdict{RoomID: roomID}
			}
			v.Verdict, v.At = p.Verdict, e.At.UTC()
		case events.ShadowGraded:
			p, err := events.Decode[events.ShadowGradedPayload](e)
			if err != nil {
				continue
			}
			if v == nil {
				v = &FleetVerdict{RoomID: roomID}
			}
			lc := p.LayerCorrect
			v.LayerCorrect, v.At = &lc, e.At.UTC()
		}
	}
	return v
}

// row builds one FleetPipeline from the shared facts.
func (f *fleetView) row(p room.PipelineInfo, c *fleetFacts, now time.Time) FleetPipeline {
	tenant, team := f.tenantTeam(p.ID)
	out := FleetPipeline{
		PipelineID: p.ID, Title: p.Title, Kind: p.Kind, Tenant: tenant, Team: team,
		Health: "unknown", OpenRooms: c.open[p.ID], Changes24h: c.changes[p.ID],
		SignalsFiring: []string{}, Forecasts: []FleetForecast{}, LastVerdict: c.verdict[p.ID],
	}
	st, has := c.status[p.ID]
	if has && !st.CheckedAt.IsZero() {
		at := st.CheckedAt.UTC()
		out.CheckedAt = &at
	}
	interval := f.cfg.WatchInterval
	if interval <= 0 {
		interval = 60 * time.Second
	}
	fresh := has && !f.cfg.WatchOff && !st.CheckedAt.IsZero() && now.Sub(st.CheckedAt) <= fleetStaleIntervals*interval
	if fresh {
		out.Health = "ok"
		sigs := slices.Clone(st.Signals)
		// Worst first, then oldest: the order of signals_firing and the
		// signal worst_node names.
		slices.SortStableFunc(sigs, func(a, b clients.WatchSignal) int {
			if d := sevNumber(a.Severity) - sevNumber(b.Severity); d != 0 {
				return d
			}
			return a.Since.Compare(b.Since)
		})
		for i, g := range sigs {
			if i == 0 {
				out.WorstNode = g.Node
			}
			if g.State == clients.WatchFiring && (g.Severity == "sev1" || g.Severity == "sev2") {
				out.Health = "fail"
			} else if out.Health == "ok" {
				out.Health = "warn"
			}
			if !slices.Contains(out.SignalsFiring, g.Signal) && len(out.SignalsFiring) < clients.MaxWatchSignals {
				out.SignalsFiring = append(out.SignalsFiring, g.Signal)
			}
			if g.Signal == events.SignalForecast && g.ETA != nil {
				out.Forecasts = append(out.Forecasts, FleetForecast{Metric: g.Metric, ETA: g.ETA.UTC()})
			}
		}
		slices.SortStableFunc(out.Forecasts, func(a, b FleetForecast) int { return a.ETA.Compare(b.ETA) })
		if len(out.Forecasts) > fleetMaxForecasts {
			out.Forecasts = out.Forecasts[:fleetMaxForecasts]
		}
	}
	// An open room raises ok and unknown to warn, never to fail.
	if out.OpenRooms > 0 && (out.Health == "ok" || out.Health == "unknown") {
		out.Health = "warn"
	}
	return out
}

// sevNumber turns "sev2" into 2; anything else ranks after sev4.
func sevNumber(s string) int {
	if n, err := strconv.Atoi(strings.TrimPrefix(s, "sev")); err == nil && strings.HasPrefix(s, "sev") && n >= 1 {
		return n
	}
	return 9
}

// handleFleetChanges serves GET
// /api/fleet/changes?since=&kind=&pipeline=&limit=&cursor=.
func (s *Server) handleFleetChanges(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	now := s.now().UTC()
	since := now.Add(-fleetChangesDefaultDays * 24 * time.Hour)
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			s.writeErr(w, validation("since must be an RFC 3339 time"))
			return
		}
		since = t.UTC()
	}
	if d := s.fleet.cfg.ChangesRetentionDays; d > 0 {
		if floor := now.Add(-time.Duration(d) * 24 * time.Hour); since.Before(floor) {
			since = floor
		}
	}
	var kinds []string
	if v := q.Get("kind"); v != "" {
		for _, k := range strings.Split(v, ",") {
			if !slices.Contains(clients.ChangeKinds(), k) {
				s.writeErr(w, validation("unknown kind"))
				return
			}
			if !slices.Contains(kinds, k) {
				kinds = append(kinds, k)
			}
		}
	}
	limit := fleetChangesDefaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > clients.MaxFleetChangesPage {
			s.writeErr(w, validation("limit must be a whole number from 1 to 200"))
			return
		}
		limit = n
	}
	var cur clients.ChangeCursor
	if v := q.Get("cursor"); v != "" {
		c, ok := decodeChangeCursor(v)
		if !ok {
			s.writeErr(w, validation("bad cursor"))
			return
		}
		cur = c
	}
	page := FleetChangesPage{Changes: []clients.PipelineChange{}}
	var ids []string
	pipeline := q.Get("pipeline")
	for _, p := range s.svc.PipelinesFor(userFrom(r.Context())) {
		if pipeline == "" || p.ID == pipeline {
			ids = append(ids, p.ID)
		}
	}
	// A pipeline outside the caller's tenants, or unknown, is an empty page
	// like any aggregate: the answer does not say which.
	if len(ids) == 0 || s.fleet.cfg.Changes == nil {
		writeJSON(w, http.StatusOK, page)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fleetReadTimeout)
	defer cancel()
	// One row more than the page says whether another page follows; at the
	// reader's cap a full page sets next (the following page may be empty).
	ask := min(limit+1, clients.MaxFleetChangesPage)
	rows, err := s.fleet.cfg.Changes.ListFleetChanges(ctx, ids, since, kinds, cur, ask)
	if err != nil {
		s.writeErr(w, fmt.Errorf("httpapi: fleet changes: %w", err))
		return
	}
	more := len(rows) > limit || (ask == limit && len(rows) == limit)
	if len(rows) > limit {
		rows = rows[:limit]
	}
	if rows != nil {
		page.Changes = rows
	}
	if more && len(rows) > 0 {
		last := rows[len(rows)-1]
		page.Next = encodeChangeCursor(clients.ChangeCursor{At: last.At, ID: last.ID})
	}
	writeJSON(w, http.StatusOK, page)
}

// encodeChangeCursor is the opaque form of a page position: the row's time in
// Unix nanoseconds and its id, base64url.
func encodeChangeCursor(c clients.ChangeCursor) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(c.At.UnixNano(), 10) + "." + strconv.FormatInt(c.ID, 10)))
}

// decodeChangeCursor reverses encodeChangeCursor; ok is false for anything
// it did not make (a position always has a time and a positive id).
func decodeChangeCursor(s string) (clients.ChangeCursor, bool) {
	if len(s) > 64 {
		return clients.ChangeCursor{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return clients.ChangeCursor{}, false
	}
	at, id, ok := strings.Cut(string(raw), ".")
	if !ok {
		return clients.ChangeCursor{}, false
	}
	ns, err1 := strconv.ParseInt(at, 10, 64)
	n, err2 := strconv.ParseInt(id, 10, 64)
	if err1 != nil || err2 != nil || ns <= 0 || n <= 0 {
		return clients.ChangeCursor{}, false
	}
	return clients.ChangeCursor{At: time.Unix(0, ns).UTC(), ID: n}, true
}
