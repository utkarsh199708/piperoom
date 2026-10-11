package room

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// User is a person who can watch, steer, approve and drive a room.
type User struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Layers []string `json:"layers"`
}

// Users is the user directory parsed from dev/users.yaml.
type Users struct {
	byID  map[string]User
	order []User
}

type usersFile struct {
	Users []User `yaml:"users"`
}

// ParseUsers parses the users.yaml format.
func ParseUsers(data []byte) (*Users, error) {
	var f usersFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("room: parse users: %w", err)
	}
	u := &Users{byID: map[string]User{}}
	for _, user := range f.Users {
		if user.ID == "" {
			return nil, fmt.Errorf("room: user with empty id")
		}
		u.byID[user.ID] = user
		u.order = append(u.order, user)
	}
	return u, nil
}

// LoadUsers reads and parses the users file at path.
func LoadUsers(path string) (*Users, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("room: read users file: %w", err)
	}
	return ParseUsers(data)
}

// ValidateUsers checks the users directory against the loaded owners config
// (LEAD-ARCH round 4, item 7). Every layer a user claims must be one of
// events.Layers, and every layer in the owners config must name a user who
// exists in the users file and actually lists that layer. Any disagreement is
// returned as an error so the server refuses to start: an approver that can
// never approve, or a layer with no owner, would otherwise stall proposals
// forever with no visible cause. own may be nil (no owners config loaded),
// in which case only the users file itself is checked.
func ValidateUsers(users *Users, own *owners.Owners) error {
	for _, user := range users.order {
		for _, layer := range user.Layers {
			if !isKnownLayer(layer) {
				return fmt.Errorf("users file: user %q claims unknown layer %q (known: %v)", user.ID, layer, events.Layers)
			}
		}
	}
	if own == nil {
		return nil
	}
	layers := make([]string, 0, len(own.Layers))
	for layer := range own.Layers {
		layers = append(layers, layer)
	}
	sort.Strings(layers) // deterministic error for the same config
	for _, layer := range layers {
		ownerID := own.Layers[layer]
		user, ok := users.Get(ownerID)
		if !ok {
			return fmt.Errorf("owners file assigns layer %q to %q, who is not in the users file", layer, ownerID)
		}
		if !user.OwnsLayer(layer) {
			return fmt.Errorf("users file and owners file disagree: %q owns layer %q in the owners file but lists layers %v", ownerID, layer, user.Layers)
		}
	}
	return nil
}

// DefaultUsers returns the demo directory from docs/api.md, matching
// dev/users.yaml (both moved schema-registry to priya's source team). Used by
// --demo and tests when the users file has not landed yet.
func DefaultUsers() *Users {
	return mustUsers([]User{
		{ID: "arjun", Name: "Arjun", Layers: []string{"kafka-connect", "kafka"}},
		{ID: "priya", Name: "Priya", Layers: []string{"postgres", "schema-registry"}},
		{ID: "meera", Name: "Meera", Layers: []string{"warehouse"}},
		{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}},
	})
}

func mustUsers(users []User) *Users {
	u := &Users{byID: map[string]User{}}
	for _, user := range users {
		u.byID[user.ID] = user
		u.order = append(u.order, user)
	}
	return u
}

// Get returns a user by id.
func (u *Users) Get(id string) (User, bool) {
	user, ok := u.byID[id]
	return user, ok
}

// All returns every user in file order.
func (u *Users) All() []User {
	return append([]User(nil), u.order...)
}

// OwnsLayer reports whether the user may approve the given layer.
func (u User) OwnsLayer(layer string) bool {
	for _, l := range u.Layers {
		if l == layer {
			return true
		}
	}
	return false
}
