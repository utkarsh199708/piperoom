package room

import (
	"fmt"
	"os"
	"slices"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Users is the set of people who can use the room service (dev/users.yaml).
type Users struct {
	byID  map[string]User
	order []User
}

type usersFile struct {
	Users []User `yaml:"users"`
}

// LoadUsers reads and validates the users file at path.
func LoadUsers(path string) (*Users, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("room: read users %s: %w", path, err)
	}
	var f usersFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("room: parse users %s: %w", path, err)
	}
	u, err := NewUsers(f.Users)
	if err != nil {
		return nil, fmt.Errorf("room: users %s: %w", path, err)
	}
	return u, nil
}

// NewUsers validates and indexes users.
func NewUsers(list []User) (*Users, error) {
	u := &Users{byID: map[string]User{}}
	for _, x := range list {
		if x.ID == "" {
			return nil, fmt.Errorf("user with empty id")
		}
		if _, dup := u.byID[x.ID]; dup {
			return nil, fmt.Errorf("duplicate user id %q", x.ID)
		}
		if x.Name == "" {
			x.Name = x.ID
		}
		for _, l := range x.Layers {
			if !slices.Contains(events.Layers, l) {
				return nil, fmt.Errorf("user %q: unknown layer %q (known: %v)", x.ID, l, events.Layers)
			}
		}
		if x.Layers == nil {
			x.Layers = []string{}
		}
		u.byID[x.ID] = x
		u.order = append(u.order, x)
	}
	return u, nil
}

// DemoUsers returns the built-in users for --demo (same people as dev/users.yaml).
func DemoUsers() *Users {
	u, _ := NewUsers([]User{
		{ID: "arjun", Name: "Arjun", Layers: []string{"kafka-connect", "kafka"}},
		{ID: "priya", Name: "Priya", Layers: []string{"postgres", "schema-registry"}},
		{ID: "meera", Name: "Meera", Layers: []string{"warehouse"}},
		{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}},
	})
	return u
}

// Get returns the user with this id.
func (u *Users) Get(id string) (User, bool) {
	x, ok := u.byID[id]
	return x, ok
}

// All returns the users in file order.
func (u *Users) All() []User { return slices.Clone(u.order) }

// CheckOwners verifies that users.yaml and owners.yaml agree about who owns
// which layer (docs/api.md §Auth): every layer in o.Layers must be owned by
// the user listed there, and no user may own a layer o assigns to someone else.
func (u *Users) CheckOwners(o *owners.Owners) error {
	for layer, owner := range o.Layers {
		usr, ok := u.byID[owner]
		if !ok {
			return fmt.Errorf("owners.yaml: layer %q owner %q is not in the users file", layer, owner)
		}
		if !slices.Contains(usr.Layers, layer) {
			return fmt.Errorf("owners.yaml says %q owns layer %q but the users file does not list it", owner, layer)
		}
	}
	for _, usr := range u.order {
		for _, layer := range usr.Layers {
			if owner, ok := o.Layers[layer]; ok && owner != usr.ID {
				return fmt.Errorf("users file lists layer %q for %q but owners.yaml gives it to %q", layer, usr.ID, owner)
			}
		}
	}
	return nil
}
