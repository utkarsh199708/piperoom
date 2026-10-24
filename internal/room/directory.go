package room

import "time"

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
			s.regMu.Lock()
			if at, ok := s.regAt[p.ID]; ok {
				setup.RegisteredAt = at.UTC().Format(time.RFC3339)
			}
			s.regMu.Unlock()
			info.Setup = setup
		}
		out = append(out, info)
	}
	return out
}
