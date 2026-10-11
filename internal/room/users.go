package room

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
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

// DefaultUsers returns the demo directory from docs/api.md. Used by --demo
// when dev/users.yaml has not landed yet.
func DefaultUsers() *Users {
	return mustUsers([]User{
		{ID: "arjun", Name: "Arjun", Layers: []string{"kafka-connect", "kafka", "schema-registry"}},
		{ID: "priya", Name: "Priya", Layers: []string{"postgres"}},
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
