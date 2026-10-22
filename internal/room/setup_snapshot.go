package room

import (
	"encoding/json"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Setup rooms (docs/design/setup-agent.md; docs/api.md, "U-wave snapshot
// fields"): the "setup" member of a setup room's view, folded from the same
// event log as the rest of the room. Every field is derived from an event the
// server or the runner wrote; nothing here reads the model's text.

// SetupSnapshot is RoomSnapshot.Setup on a setup room (room.created.mode
// "setup"). It is nil on every incident room, and an absent field reads as its
// empty value (docs/api.md).
type SetupSnapshot struct {
	// Mode is always true; the presence of the object is what says the room is
	// a setup room.
	Mode bool `json:"mode"`
	// Runner is the paired runner (setup.runner_attached); nil before pairing.
	// A lost runner keeps its record with Connected false.
	Runner *SetupRunnerState `json:"runner"`
	// PlanID, Phase and StepIndex are the current setup.plan and the step the
	// agent is on: the first step of the plan no action.executed has run yet.
	PlanID    string `json:"plan_id"`
	Phase     string `json:"phase"`
	StepIndex int    `json:"step_index"`
	// Bounds are the stops of the setup run (design §3.1).
	Bounds SetupBoundsState `json:"bounds"`
	// Targets are the request's targets plus every setup.target_registered,
	// with their class after setup.target_classed (a class only rises).
	Targets []SetupTargetState `json:"targets"`
	// Provisional is the pipeline the room is scoped to (owners.Provisional)
	// after every setup.pipeline_extended.
	Provisional SetupProvisionalState `json:"provisional"`
	// MaintenanceWindow is the latest setup.maintenance_window while it is
	// still in progress; absent when there is none or it has ended.
	MaintenanceWindow *SetupWindowState `json:"maintenance_window,omitempty"`
	// Verified are the rungs of the latest setup.verified for PlanID; [] before
	// one exists.
	Verified []events.SetupRung `json:"verified"`
	// Blocked is the latest setup.blocked while the agent is idle on it
	// (a later working agent.status clears it).
	Blocked *SetupBlockedState `json:"blocked,omitempty"`
	// WatchedBy is pipeline.registered.watched_by: "server", "local", or "".
	WatchedBy string `json:"watched_by"`

	// tenant is the room's tenant (SetupRequest.tenant). Unexported: the
	// snapshot's documented shape has no tenant, but tenant visibility of a
	// setup room, whose pipeline owners.yaml does not define yet, needs it.
	tenant string
	// req is the request room.created carries. The provisional pipeline is
	// rebuilt from it on demand (SetupProvisional); the JSON lists below are
	// what the view shows.
	req events.SetupRequest
	// phases is the plan's phase list and executed the steps that ran, so
	// Phase and StepIndex follow the plan.
	phases   []events.SetupPlanPhase
	executed map[int]bool
	// list marks the trimmed form GET /api/rooms rows carry: mode, phase and
	// runner.connected only (MarshalJSON, trimSetupForList).
	list bool
}

// setupListRow is the "setup" member of a GET /api/rooms row (docs/api.md: the
// list carries setup.mode, setup.runner.connected and setup.phase only).
type setupListRow struct {
	Mode   bool             `json:"mode"`
	Runner *setupListRunner `json:"runner"`
	Phase  string           `json:"phase"`
}

// setupListRunner is a list row's runner: whether it is connected, nothing
// else.
type setupListRunner struct {
	Connected bool `json:"connected"`
}

// MarshalJSON writes the trimmed list row for a snapshot trimSetupForList
// marked, and every documented field otherwise.
func (s SetupSnapshot) MarshalJSON() ([]byte, error) {
	if s.list {
		row := setupListRow{Mode: s.Mode, Phase: s.Phase}
		if s.Runner != nil {
			row.Runner = &setupListRunner{Connected: s.Runner.Connected}
		}
		return json.Marshal(row)
	}
	// A defined type has no methods of its own, so this does not recurse.
	type full SetupSnapshot
	return json.Marshal(full(s))
}

// SetupRunnerState is the paired runner as the snapshot shows it
// (setup.runner_attached and setup.runner_lost).
type SetupRunnerState struct {
	Label     string            `json:"label"`
	Connected bool              `json:"connected"`
	Tools     map[string]string `json:"tools"`
}

// SetupBoundsState is the setup run's stops (agent.SetupBounds; design §3.1).
type SetupBoundsState struct {
	AttemptsPerStep int   `json:"attempts_per_step"`
	StepsPerPlan    int   `json:"steps_per_plan"`
	WallClockS      int   `json:"wall_clock_s"`
	TokenBudget     int64 `json:"token_budget"`
}

// SetupTargetState is one target with its class: exactly the three fields the
// snapshot documents (a registered target's kind is not shown again, the same
// as SetupRequest).
type SetupTargetState struct {
	Kind    string `json:"kind"`
	Address string `json:"address"`
	Class   string `json:"class"`
}

// SetupProvisionalState is the provisional pipeline's scope. Consumer groups
// and sink tables are kept for the room (the scope rules read them) but are
// not part of the documented shape.
type SetupProvisionalState struct {
	TopicPrefix    string   `json:"topic_prefix"`
	Tables         []string `json:"tables"`
	Connectors     []string `json:"connectors"`
	ConsumerGroups []string `json:"-"`
	SinkTables     []string `json:"-"`
}

// SetupWindowState is one recorded maintenance window.
type SetupWindowState struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// SetupBlockedState is the stop that made the agent ask a question
// (setup.blocked).
type SetupBlockedState struct {
	Reason   string `json:"reason"`
	Question string `json:"question"`
}

// The fixed bounds of a setup run (design §3.1). The token budget comes from
// PIPEROOM_SETUP_TOKEN_BUDGET; defaultSetupTokenBudget is config.ParseSetup's
// default, replaced with the configured value on the room view (RoomFor).
const (
	setupAttemptsPerStep    = 3
	setupStepsPerPlan       = 12
	setupWallClockSeconds   = 45 * 60
	defaultSetupTokenBudget = 2_000_000
	setupSeverity           = "sev4"
)

// Setup name caps: one entry of the provisional pipeline's lists, and how many
// entries one list may hold. Both match what owners.yaml refuses to load.
const (
	maxSetupNameChars    = 256
	maxSetupPipelineList = 200
)

// DefaultSetupTokenBudget is defaultSetupTokenBudget as a typed value, for a
// caller that has no config.
func DefaultSetupTokenBudget() int64 { return defaultSetupTokenBudget }

// defaultSetupBounds is the bounds every setup room starts with.
func defaultSetupBounds() SetupBoundsState {
	return SetupBoundsState{
		AttemptsPerStep: setupAttemptsPerStep,
		StepsPerPlan:    setupStepsPerPlan,
		WallClockS:      setupWallClockSeconds,
		TokenBudget:     defaultSetupTokenBudget,
	}
}

// IsSetupRoom reports whether the room POST /api/setup opened.
func (s RoomSnapshot) IsSetupRoom() bool {
	return s.Setup != nil && s.Setup.Mode
}

// SetupTenant returns the setup room's tenant ("" without a tenants: block).
func (s RoomSnapshot) SetupTenant() string {
	if s.Setup == nil {
		return ""
	}
	return s.Setup.tenant
}

// SetupProvisional returns the provisional pipeline the room is scoped to
// (owners.Provisional over the request, with every setup.pipeline_extended
// applied), or false for a room that is not a setup room. It is the pipeline
// the setup tools and actions check their scope against until
// pipeline_register writes the owners entry.
func (s RoomSnapshot) SetupProvisional() (owners.Pipeline, bool) {
	if s.Setup == nil || !s.Setup.Mode {
		return owners.Pipeline{}, false
	}
	req := s.Setup.req
	req.PipelineID = s.PipelineID
	req.Tenant = s.Setup.tenant
	req.Tables = append([]string{}, s.Setup.Provisional.Tables...)
	req.Connectors = append([]string{}, s.Setup.Provisional.Connectors...)
	p, err := owners.Provisional(req)
	if err != nil {
		return owners.Pipeline{}, false
	}
	p.ConsumerGroups = append([]string{}, s.Setup.Provisional.ConsumerGroups...)
	p.SinkTables = append([]string{}, s.Setup.Provisional.SinkTables...)
	return p, true
}

// clone deep-copies the setup state; nil stays nil.
func (s *SetupSnapshot) clone() *SetupSnapshot {
	if s == nil {
		return nil
	}
	c := *s
	if s.Runner != nil {
		r := *s.Runner
		if s.Runner.Tools != nil {
			r.Tools = make(map[string]string, len(s.Runner.Tools))
			for k, v := range s.Runner.Tools {
				r.Tools[k] = v
			}
		}
		c.Runner = &r
	}
	c.Targets = append([]SetupTargetState{}, s.Targets...)
	c.Provisional.Tables = append([]string{}, s.Provisional.Tables...)
	c.Provisional.Connectors = append([]string{}, s.Provisional.Connectors...)
	c.Provisional.ConsumerGroups = append([]string{}, s.Provisional.ConsumerGroups...)
	c.Provisional.SinkTables = append([]string{}, s.Provisional.SinkTables...)
	if s.MaintenanceWindow != nil {
		w := *s.MaintenanceWindow
		c.MaintenanceWindow = &w
	}
	c.Verified = append([]events.SetupRung{}, s.Verified...)
	if s.Blocked != nil {
		b := *s.Blocked
		c.Blocked = &b
	}
	c.req = s.req
	c.req.Tables = append([]string{}, s.req.Tables...)
	c.req.Connectors = append([]string{}, s.req.Connectors...)
	c.req.Targets = append([]events.SetupTarget{}, s.req.Targets...)
	c.req.Repos = append([]string{}, s.req.Repos...)
	c.phases = make([]events.SetupPlanPhase, len(s.phases))
	for i, ph := range s.phases {
		c.phases[i] = ph
		c.phases[i].Steps = append([]events.SetupPlanStep{}, ph.Steps...)
	}
	if s.executed != nil {
		c.executed = make(map[int]bool, len(s.executed))
		for k, v := range s.executed {
			c.executed[k] = v
		}
	}
	return &c
}

// newSetupSnapshot builds the initial "setup" object from room.created.
func newSetupSnapshot(req events.SetupRequest) *SetupSnapshot {
	st := &SetupSnapshot{
		Mode:     true,
		Bounds:   defaultSetupBounds(),
		Targets:  []SetupTargetState{},
		Verified: []events.SetupRung{},
		tenant:   req.Tenant,
		req:      req,
		executed: map[int]bool{},
	}
	st.Provisional = SetupProvisionalState{
		TopicPrefix: req.TopicPrefix,
		Tables:      append([]string{}, req.Tables...),
		Connectors:  append([]string{}, req.Connectors...),
	}
	for _, t := range req.Targets {
		// The request's class is a floor; the classed events that follow
		// room.created carry the decision, so this only seeds the list.
		class := t.Class
		if !validSetupClass(class) {
			class = protocolClassShared
		}
		st.Targets = append(st.Targets, SetupTargetState{Kind: t.Kind, Address: t.Address, Class: class})
	}
	return st
}

// advance moves Phase and StepIndex to the first step of the current plan that
// has not run. With every step run (or no plan) it keeps the last position.
func (s *SetupSnapshot) advance() {
	for _, phase := range s.phases {
		for _, step := range phase.Steps {
			if !s.executed[step.Index] {
				s.Phase = phase.Name
				s.StepIndex = step.Index
				return
			}
		}
	}
	if n := len(s.phases); n > 0 {
		last := s.phases[n-1]
		s.Phase = last.Name
		if m := len(last.Steps); m > 0 {
			s.StepIndex = last.Steps[m-1].Index
		}
	}
}

// applySetup folds one setup-related event into the snapshot. It runs after
// Apply's own switch, so setup rooms fold exactly like every other room plus
// this member. Events of other types are ignored.
func applySetup(snap RoomSnapshot, e events.Event) RoomSnapshot {
	switch e.Type {
	case events.RoomCreated:
		p, ok := decode[events.RoomCreatedPayload](e)
		if !ok || p.Mode != events.RoomModeSetup {
			return snap
		}
		var req events.SetupRequest
		if len(p.Alert) > 0 {
			if err := json.Unmarshal(p.Alert, &req); err != nil {
				// A setup room whose request does not decode is still a room:
				// the setup object says so and carries what decoded (nothing).
				req = events.SetupRequest{}
			}
		}
		snap.Setup = newSetupSnapshot(req)
		return snap

	case events.SetupRunnerAttached:
		p, ok := decode[events.SetupRunnerAttachedPayload](e)
		if !ok || snap.Setup == nil {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.Runner = &SetupRunnerState{Label: p.RunnerLabel, Connected: true, Tools: p.Tools}
		if snap.Setup.Runner.Tools == nil {
			snap.Setup.Runner.Tools = map[string]string{}
		}

	case events.SetupRunnerLost:
		if snap.Setup == nil || snap.Setup.Runner == nil {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.Runner.Connected = false

	case events.SetupPlan:
		p, ok := decode[events.SetupPlanPayload](e)
		if !ok || snap.Setup == nil {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.PlanID = p.PlanID
		snap.Setup.phases = p.Phases
		snap.Setup.executed = map[int]bool{}
		snap.Setup.advance()

	case events.ActionExecuted:
		p, ok := decode[events.ActionExecutedPayload](e)
		if !ok || snap.Setup == nil || len(snap.Setup.phases) == 0 {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		if snap.Setup.executed == nil {
			snap.Setup.executed = map[int]bool{}
		}
		snap.Setup.executed[p.StepIndex] = true
		snap.Setup.advance()

	case events.SetupTargetRegistered:
		p, ok := decode[events.SetupTargetRegisteredPayload](e)
		if !ok || snap.Setup == nil {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		for i := range snap.Setup.Targets {
			if snap.Setup.Targets[i].Address == p.Address {
				snap.Setup.Targets[i].Kind = p.Kind
				snap.Setup.Targets[i].Class = classAtLeast(snap.Setup.Targets[i].Class, p.Class)
				return snap
			}
		}
		snap.Setup.Targets = append(snap.Setup.Targets, SetupTargetState{Kind: p.Kind, Address: p.Address, Class: p.Class})

	case events.SetupTargetClassed:
		p, ok := decode[events.SetupTargetClassedPayload](e)
		if !ok || snap.Setup == nil || !validSetupClass(p.Class) {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		for i := range snap.Setup.Targets {
			if snap.Setup.Targets[i].Address == p.Target {
				// A class only rises.
				snap.Setup.Targets[i].Class = classAtLeast(snap.Setup.Targets[i].Class, p.Class)
				return snap
			}
		}

	case events.SetupPipelineExtended:
		p, ok := decode[events.SetupPipelineExtendedPayload](e)
		if !ok || snap.Setup == nil {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.Provisional.Connectors = appendUnique(snap.Setup.Provisional.Connectors, p.AddConnectors)
		snap.Setup.Provisional.Tables = appendUnique(snap.Setup.Provisional.Tables, p.AddTables)
		snap.Setup.Provisional.ConsumerGroups = appendUnique(snap.Setup.Provisional.ConsumerGroups, p.AddConsumerGroups)
		snap.Setup.Provisional.SinkTables = appendUnique(snap.Setup.Provisional.SinkTables, p.AddSinkTables)

	case events.SetupMaintenanceWindow:
		p, ok := decode[events.SetupMaintenanceWindowPayload](e)
		if !ok || snap.Setup == nil {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.MaintenanceWindow = &SetupWindowState{Start: p.Start.UTC(), End: p.End.UTC()}

	case events.SetupVerified:
		p, ok := decode[events.SetupVerifiedPayload](e)
		if !ok || snap.Setup == nil {
			return snap
		}
		if snap.Setup.PlanID != "" && p.PlanID != snap.Setup.PlanID {
			return snap // a run of an older plan says nothing about this one
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.Verified = append([]events.SetupRung{}, p.Rungs...)
		if snap.Setup.Verified == nil {
			snap.Setup.Verified = []events.SetupRung{}
		}

	case events.SetupBlocked:
		p, ok := decode[events.SetupBlockedPayload](e)
		if !ok || snap.Setup == nil {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.Blocked = &SetupBlockedState{Reason: p.Reason, Question: p.Question}

	case events.AgentStatus:
		// The agent went back to work: the blocked question no longer asks.
		p, ok := decode[events.AgentStatusPayload](e)
		if !ok || snap.Setup == nil || snap.Setup.Blocked == nil || p.State == "idle" {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.Blocked = nil

	case events.PipelineRegistered:
		p, ok := decode[events.PipelineRegisteredPayload](e)
		if !ok || snap.Setup == nil {
			return snap
		}
		snap.Setup = snap.Setup.clone()
		snap.Setup.WatchedBy = p.WatchedBy
	}
	return snap
}

// appendUnique appends every name of add that list does not hold yet.
func appendUnique(list, add []string) []string {
	for _, name := range add {
		if name == "" {
			continue
		}
		found := false
		for _, have := range list {
			if have == name {
				found = true
				break
			}
		}
		if !found {
			list = append(list, name)
		}
	}
	return list
}

// trimSetupForList keeps only the members GET /api/rooms rows carry: mode,
// runner.connected and phase (docs/api.md). It mutates a snapshot ListRooms
// just cloned.
func trimSetupForList(snap *RoomSnapshot) {
	if snap.Setup == nil {
		return
	}
	full := snap.Setup
	trimmed := &SetupSnapshot{Mode: full.Mode, Phase: full.Phase, list: true}
	if full.Runner != nil {
		trimmed.Runner = &SetupRunnerState{Connected: full.Runner.Connected}
	}
	snap.Setup = trimmed
}

// fillSetupRuntime sets the fields that depend on the serving process rather
// than on the log: the configured token budget and whether the maintenance
// window has ended. Callers pass a clone (RoomFor does).
func (s *Service) fillSetupRuntime(snap *RoomSnapshot) {
	if snap.Setup == nil {
		return
	}
	if s.setupCfg.TokenBudget > 0 {
		snap.Setup.Bounds.TokenBudget = s.setupCfg.TokenBudget
	}
	if w := snap.Setup.MaintenanceWindow; w != nil && !s.now().Before(w.End) {
		snap.Setup.MaintenanceWindow = nil
	}
}

// setupWindow bounds of a maintenance window (design §3.5, docs/api.md).
const (
	MaxSetupWindowDuration = 4 * time.Hour
	MaxSetupWindowAhead    = 24 * time.Hour
)
