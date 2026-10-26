package room

import (
	"context"
	"time"
)

// registeredAtLoadTimeout bounds the one room load registeredAt makes for a
// pipeline whose registration time this replica has not folded yet.
const registeredAtLoadTimeout = 5 * time.Second

// PickerUser is one entry of GET /auth/users: the dev login picker. Only the
// id, name and layers of a users.yaml entry; never email, sub or admin.
type PickerUser struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Layers []string `json:"layers"`
}

// LoginPicker returns the users the dev login screen may pick from: every
// users.yaml user in file order, or only the solo user when
// PIPEROOM_SOLO_USER is set (docs/api.md, Login picker). Never nil.
func (s *Service) LoginPicker() []PickerUser {
	out := []PickerUser{}
	for _, u := range s.users.All() {
		if s.soloUser != "" && u.ID != s.soloUser {
			continue
		}
		layers := append([]string{}, u.Layers...)
		out = append(out, PickerUser{ID: u.ID, Name: u.Name, Layers: layers})
	}
	return out
}

// PipelineInfo is one entry of GET /api/pipelines.
type PipelineInfo struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Shadow bool   `json:"shadow"`
	// Kind is the pipeline's owners.yaml kind, "cdc" or "batch" ("cdc" when
	// the file names none). (P4a-4)
	Kind string `json:"kind"`
	// Setup is the pipeline's setup: block as GET /api/pipelines shows it
	// (docs/api.md "Registration"): where the pipeline was set up from, its
	// room, when it registered and who watches it. nil for a pipeline a
	// setup did not register.
	Setup *PipelineSetupInfo `json:"setup,omitempty"`
}

// PipelineSetupInfo is the setup row of PipelineInfo (docs/api.md).
type PipelineSetupInfo struct {
	RoomID       string `json:"room_id"`
	Target       string `json:"target"`
	RegisteredAt string `json:"registered_at"`
	WatchedBy    string `json:"watched_by"`
	// UnreachableLayers lists the layers the server cannot read for this
	// pipeline (owners.SetupSpec.UnreachableLayers, which the watcher skips
	// with a reason); omitted when there are none.
	UnreachableLayers []string `json:"unreachable_layers,omitempty"`
}

// PipelinesFor returns the owners.yaml pipelines u may see, in file order
// (tenancy filter as for rooms; the solo user sees all). Never nil: with no
// owners config there are none, and the client must not invent one.
func (s *Service) PipelinesFor(u User) []PipelineInfo {
	out := []PipelineInfo{}
	o := s.ownersNow()
	if o == nil {
		return out
	}
	for _, p := range o.Pipelines {
		if !s.CanSeePipeline(u, p.ID) {
			continue
		}
		info := PipelineInfo{ID: p.ID, Title: p.Title, Shadow: p.Shadow(), Kind: p.KindOrDefault()}
		if spec, ok := p.Setup(); ok {
			setup := &PipelineSetupInfo{RoomID: spec.RoomID, Target: spec.Target, WatchedBy: spec.WatchedBy}
			if len(spec.UnreachableLayers) > 0 {
				setup.UnreachableLayers = append([]string{}, spec.UnreachableLayers...)
			}
			if at, ok := s.registeredAt(p.ID, spec.RoomID); ok {
				setup.RegisteredAt = at.UTC().Format(time.RFC3339)
			}
			info.Setup = setup
		}
		out = append(out, info)
	}
	return out
}

// registeredAt is the time pipeline.registered was appended for pipelineID,
// from the fold of the rooms this replica has loaded (noteRegistered). Rooms
// load on demand, so after a restart the time is unknown until the setup room
// is read by someone; when it is unknown, the setup room the owners entry
// names (setup.room_id) is loaded once here, which folds its log and records
// the time. A room that is gone, or a store that does not answer in time,
// leaves it unknown (registered_at empty) for this call.
func (s *Service) registeredAt(pipelineID, roomID string) (time.Time, bool) {
	s.regMu.Lock()
	at, ok := s.regAt[pipelineID]
	s.regMu.Unlock()
	if ok || roomID == "" {
		return at, ok
	}
	ctx, cancel := context.WithTimeout(s.ctx, registeredAtLoadTimeout)
	defer cancel()
	if _, err := s.readRoom(ctx, roomID); err != nil {
		return time.Time{}, false
	}
	s.regMu.Lock()
	at, ok = s.regAt[pipelineID]
	s.regMu.Unlock()
	return at, ok
}
