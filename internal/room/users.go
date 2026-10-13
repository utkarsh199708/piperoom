package room

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// User is a person who can watch, steer, approve and drive a room.
type User struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Layers []string `json:"layers"`
	// Email and Sub tie this user to an IdP account (docs/design/auth.md
	// section 2.1). Both are optional. They map an IdP login to this user and
	// are not sent to clients.
	Email string `json:"-" yaml:"email"`
	Sub   string `json:"-" yaml:"sub"`

	// Groups are the IdP groups of the session this value was built for; nil
	// when the token had no groups claim, and always nil for a users.yaml entry.
	// GroupsMissing is true when a groups claim is configured but the token had
	// none (design section 2.2). Neither is part of users.yaml or the JSON.
	Groups        []string `json:"-" yaml:"-"`
	GroupsMissing bool     `json:"-" yaml:"-"`
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
	emails := map[string]string{} // lower-cased email -> user id
	subs := map[string]string{}
	for _, user := range f.Users {
		if user.ID == "" {
			return nil, fmt.Errorf("room: user with empty id")
		}
		// A colon or space in an id would let user "group:x" print like the
		// principal group:x (LayerOwner labels drop the user: prefix).
		if strings.ContainsFunc(user.ID, func(r rune) bool { return r == ':' || unicode.IsSpace(r) }) {
			return nil, fmt.Errorf("room: user id %q: a colon or whitespace is not allowed", user.ID)
		}
		if user.Email != "" {
			if !strings.Contains(user.Email, "@") || strings.ContainsAny(user.Email, " \t\r\n") {
				return nil, fmt.Errorf("room: user %q: bad email %q", user.ID, user.Email)
			}
			key := strings.ToLower(user.Email)
			if other, dup := emails[key]; dup {
				return nil, fmt.Errorf("room: users %q and %q have the same email %q", other, user.ID, user.Email)
			}
			emails[key] = user.ID
		}
		if user.Sub != "" {
			if other, dup := subs[user.Sub]; dup {
				return nil, fmt.Errorf("room: users %q and %q have the same sub %q", other, user.ID, user.Sub)
			}
			subs[user.Sub] = user.ID
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
// events.Layers, and the two files must agree in BOTH directions: every layer
// in the owners config must name a user who exists in the users file and
// actually lists that layer (owners → users), and every layer a user lists
// must be assigned to that same user in the owners config (users → owners).
// The users → owners direction matters because approval rights come from the
// users file: if users.yaml grants priya "warehouse" while owners.yaml says
// "warehouse: meera", priya could approve a warehouse step without the real
// owner. Any disagreement is returned as an error so the server refuses to
// start: an approver that can never approve, or a layer with no owner, would
// otherwise stall proposals forever with no visible cause. own may be nil (no
// owners config loaded), in which case only the users file itself is checked.
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
	// Group rule (docs/design/auth.md section 2.3): the server does not know
	// group membership at start-up, so a layer that lists any group: principal
	// is not checked against users.yaml. Log it once.
	var skipped []string
	for _, layer := range layers {
		if !own.Layers[layer].AllUsers() {
			skipped = append(skipped, layer)
		}
	}
	if len(skipped) > 0 {
		slog.Info("users: layers with group principals are not checked against users.yaml", "layers", skipped)
	}
	for _, layer := range layers {
		ps := own.Layers[layer]
		if !ps.AllUsers() {
			continue
		}
		for _, ownerID := range ps.Users() {
			user, ok := users.Get(ownerID)
			if !ok {
				return fmt.Errorf("owners file assigns layer %q to %q, who is not in the users file", layer, ownerID)
			}
			if !user.OwnsLayer(layer) {
				return fmt.Errorf("users file and owners file disagree: %q owns layer %q in the owners file but lists layers %v", ownerID, layer, user.Layers)
			}
		}
	}
	// users → owners: every layer a user lists must be owned by that same user.
	// Iterate in file order for a deterministic error.
	for _, user := range users.order {
		for _, layer := range user.Layers {
			ps, ok := own.Layers[layer]
			if !ok {
				return fmt.Errorf("users file grants %q layer %q, which the owners file does not assign to anyone", user.ID, layer)
			}
			if !ps.AllUsers() {
				continue // group rule: skipped above
			}
			if !ps.Contains(owners.Principal{Kind: owners.KindUser, Name: user.ID}) {
				return fmt.Errorf("users file and owners file disagree: users file grants %q layer %q but the owners file assigns it to %q", user.ID, layer, ps.Display())
			}
		}
	}
	return nil
}

// CheckSoloUser is the solo-mode start check (docs/design/auth.md section 5.3).
// soloUser is the value of PIPEROOM_SOLO_USER; "" means solo mode is off and
// the check passes. Otherwise it returns an error when
//   - the solo user is not in users.yaml, or
//   - owners.yaml names any principal other than user:<soloUser> (in layers:,
//     any pipeline owners: block, or auditors:). A group counts as another
//     principal.
//
// own may be nil (no owners file), which passes the second rule. The caller
// refuses to start on an error and logs a warning when it returns nil.
func CheckSoloUser(soloUser string, users *Users, own *owners.Owners) error {
	if soloUser == "" {
		return nil
	}
	if users == nil {
		return fmt.Errorf("solo mode: PIPEROOM_SOLO_USER=%q but there is no users file", soloUser)
	}
	if _, ok := users.Get(soloUser); !ok {
		return fmt.Errorf("solo mode: PIPEROOM_SOLO_USER=%q is not in the users file", soloUser)
	}
	me := owners.Principal{Kind: owners.KindUser, Name: soloUser}
	var others []string
	for _, p := range own.NamedPrincipals() {
		if p != me {
			others = append(others, p.String())
		}
	}
	if len(others) > 0 {
		return fmt.Errorf("solo mode: the owners file names other principals (%s); solo mode is for a single owner, so remove PIPEROOM_SOLO_USER or the other owners", strings.Join(others, ", "))
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

// BySub returns the user whose users.yaml sub: is sub. An empty sub matches
// nobody.
func (u *Users) BySub(sub string) (User, bool) {
	if sub == "" {
		return User{}, false
	}
	for _, user := range u.order {
		if user.Sub == sub {
			return user, true
		}
	}
	return User{}, false
}

// ByEmail returns the user whose users.yaml email: equals email, ignoring case.
// An empty email matches nobody.
func (u *Users) ByEmail(email string) (User, bool) {
	if email == "" {
		return User{}, false
	}
	for _, user := range u.order {
		if user.Email != "" && strings.EqualFold(user.Email, email) {
			return user, true
		}
	}
	return User{}, false
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
