package room

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
}

// PipelinesFor returns the owners.yaml pipelines u may see, in file order
// (tenancy filter as for rooms; the solo user sees all). Never nil: with no
// owners config there are none, and the client must not invent one.
func (s *Service) PipelinesFor(u User) []PipelineInfo {
	out := []PipelineInfo{}
	if s.owners == nil {
		return out
	}
	for _, p := range s.owners.Pipelines {
		if !s.CanSeePipeline(u, p.ID) {
			continue
		}
		out = append(out, PipelineInfo{ID: p.ID, Title: p.Title, Shadow: p.Shadow()})
	}
	return out
}
