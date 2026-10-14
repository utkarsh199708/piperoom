package room

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"

	"piperoom/internal/auth"
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

	// Identity is how this request logged in: the session's iss, sub, public
	// session id, method and auth time (design section 3.2). The HTTP layer
	// fills it from the session; the service adds the grant when it records an
	// approval. The zero value means no session (a dev-mode X-User request, a
	// test): approvals then record method "dev" and no session id.
	Identity events.Identity `json:"-" yaml:"-"`

	// Verified is true for a users.yaml entry and false for a person known only
	// to the IdP (docs/design/auth.md section 2.1). GET /api/me and WebSocket
	// presence show it; event actors take it from users.yaml directly.
	Verified bool `json:"verified" yaml:"-"`
}

// Users is the user directory parsed from dev/users.yaml.
type Users struct {
	byID  map[string]User
	order []User
	// skeletons maps the look-alike form (auth.Skeleton) of every id, name and
	// email to the first user it belongs to, so an IdP name that renders like
	// one of them is found (security audit A-ID-2).
	skeletons map[string]string
}

type usersFile struct {
	Users []User `yaml:"users"`
}

// ParseUsers parses the users.yaml format.
func ParseUsers(data []byte) (*Users, error) {
	var f usersFile
	// Unknown keys are refused: a typo such as `subject:` for `sub:` would
	// otherwise drop the pin silently and let an email match bind another
	// account (security audit 2026-10, A-USR-2).
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("room: parse users: %w", err)
	}
	u := &Users{byID: map[string]User{}}
	ids := map[string]string{}    // case-folded id -> id as written
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
		// u_ + 12 hex is the id of an IdP user with no users.yaml entry, so a
		// file id in that space would collide with such a person.
		if strings.HasPrefix(user.ID, DerivedIDPrefix) {
			return nil, fmt.Errorf("room: user id %q: the prefix %q is reserved for IdP users without an entry", user.ID, DerivedIDPrefix)
		}
		// A repeated id (or one that differs only in letter case) is refused:
		// Get would answer with the last entry while BySub and ByEmail answer
		// with the first, so a sub: pin on one entry could be bypassed through
		// the email of the other (security audit 2026-10, A-USR-1).
		if other, dup := ids[foldKey(user.ID)]; dup {
			return nil, fmt.Errorf("room: user id %q is listed twice (as %q and %q); ids must be unique, ignoring letter case", user.ID, other, user.ID)
		}
		ids[foldKey(user.ID)] = user.ID
		if user.Email != "" {
			if !strings.Contains(user.Email, "@") || strings.ContainsAny(user.Email, " \t\r\n") || !isASCII(user.Email) {
				return nil, fmt.Errorf("room: user %q: bad email %q (ASCII only, no spaces)", user.ID, user.Email)
			}
			key := asciiLower(user.Email)
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
		user.Verified = true
		u.byID[user.ID] = user
		u.order = append(u.order, user)
	}
	u.indexSkeletons()
	return u, nil
}

// indexSkeletons fills u.skeletons. A skeleton shared by two users (two people
// called Alex) keeps the first; a look-up only needs to know that one exists.
func (u *Users) indexSkeletons() {
	u.skeletons = map[string]string{}
	for _, user := range u.order {
		for _, s := range []string{user.ID, user.Name, user.Email} {
			k := auth.Skeleton(s)
			if _, seen := u.skeletons[k]; k != "" && !seen {
				u.skeletons[k] = user.ID
			}
		}
	}
}

// Lookalike returns the users.yaml user whose id, name or email renders like
// name (same auth.Skeleton), for the display-name rule of design section 2.1.
func (u *Users) Lookalike(name string) (User, bool) {
	k := auth.Skeleton(name)
	if k == "" {
		return User{}, false
	}
	id, ok := u.skeletons[k]
	if !ok {
		return User{}, false
	}
	return u.byID[id], true
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
	// Group rule (docs/design/auth.md section 2.3), owners -> users direction
	// only: the server does not know group membership at start-up, so a layer
	// that lists any group: principal cannot be checked against users.yaml for
	// who must list it. The users -> owners direction below is always checked.
	// Log the skipped layers once.
	var skipped []string
	for _, layer := range layers {
		if !own.Layers[layer].AllUsers() {
			skipped = append(skipped, layer)
		}
	}
	if len(skipped) > 0 {
		slog.Info("users: owners->users check skips layers with group principals (not checked against users.yaml); users->owners is always checked", "layers", skipped)
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
			// A layer that also lists groups is NOT skipped here: the server
			// cannot vouch for group membership at start-up, so a users.yaml
			// claim must not stand in for it (security audit A-AP-3).
			if !ps.Contains(owners.Principal{Kind: owners.KindUser, Name: user.ID}) {
				return fmt.Errorf("users file and owners file disagree: users file grants %q layer %q but the owners file assigns it to %q (a users.yaml layer must be a user: principal there, even when the layer also lists groups)", user.ID, layer, ps.Display())
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
		user.Verified = true
		u.byID[user.ID] = user
		u.order = append(u.order, user)
	}
	u.indexSkeletons()
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

// ByEmail returns the user whose users.yaml email: equals email, ignoring ASCII
// case. An empty or non-ASCII email matches nobody: Unicode folding would let a
// Kelvin sign or a long s pass for k or s.
func (u *Users) ByEmail(email string) (User, bool) {
	if email == "" || !isASCII(email) {
		return User{}, false
	}
	want := asciiLower(email)
	for _, user := range u.order {
		if user.Email != "" && asciiLower(user.Email) == want {
			return user, true
		}
	}
	return User{}, false
}

// DerivedIDPrefix starts the id of an IdP user with no users.yaml entry
// ("u_" + 12 hex of SHA-256(iss|sub)); users.yaml may not use it.
const DerivedIDPrefix = "u_"

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// asciiLower lower-cases A-Z only.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
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
