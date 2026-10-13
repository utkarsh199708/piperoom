package owners

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Principal kinds. A principal is written `user:<id>` or `group:<name>`.
const (
	KindUser  = "user"
	KindGroup = "group"
)

// Principal names one user (a users.yaml id) or one IdP group.
//
// A group name is compared as an exact string against the values of the
// token's groups claim. Use the IdP group id where the claim carries ids
// (docs/design/auth.md section 2.2).
type Principal struct {
	Kind string // KindUser or KindGroup
	Name string
}

// ParsePrincipal parses `user:<id>`, `group:<name>`, or a bare id.
//
// A bare id (no colon) is a user: this keeps every owners.yaml written before
// principals existed valid (`postgres: priya`). Anything with a colon must
// start with a known prefix, so a typo such as `grp:ops` is an error and does
// not become a user called "grp:ops".
func ParsePrincipal(s string) (Principal, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Principal{}, fmt.Errorf("empty principal")
	}
	kind, name := KindUser, s
	if i := strings.Index(s, ":"); i >= 0 {
		kind, name = s[:i], strings.TrimSpace(s[i+1:])
		if kind != KindUser && kind != KindGroup {
			return Principal{}, fmt.Errorf("principal %q: bad prefix %q (want %q or %q)", s, kind, KindUser+":", KindGroup+":")
		}
	}
	if name == "" {
		return Principal{}, fmt.Errorf("principal %q: empty %s name", s, kind)
	}
	if strings.Contains(name, ",") {
		return Principal{}, fmt.Errorf("principal %q: a comma is not allowed in a name", s)
	}
	// IdP group names may hold spaces ("Data Eng On-Call"); a user id may not.
	if kind == KindUser && strings.ContainsAny(name, " \t\r\n") {
		return Principal{}, fmt.Errorf("principal %q: a user id cannot contain whitespace", s)
	}
	return Principal{Kind: kind, Name: name}, nil
}

// String returns the canonical form: "user:priya" or "group:dataeng-oncall".
func (p Principal) String() string { return p.Kind + ":" + p.Name }

// Display is the label form: the `user:` prefix is dropped.
func (p Principal) Display() string {
	if p.Kind == KindUser {
		return p.Name
	}
	return p.String()
}

// Matches reports whether the principal names this user or one of the groups.
func (p Principal) Matches(userID string, groups []string) bool {
	switch p.Kind {
	case KindUser:
		return userID != "" && p.Name == userID
	case KindGroup:
		for _, g := range groups {
			if g == p.Name {
				return true
			}
		}
	}
	return false
}

// Principals is a list of principals. In YAML it is either one string
// (`kafka: arjun`, the form used before principals existed) or a list
// (`kafka: [user:arjun, group:dataeng-oncall]`).
type Principals []Principal

// UnmarshalYAML accepts a string or a list of strings.
func (ps *Principals) UnmarshalYAML(n *yaml.Node) error {
	var out Principals
	add := func(v *yaml.Node) error {
		if v.Kind != yaml.ScalarNode {
			return fmt.Errorf("line %d: a principal must be a string like user:<id> or group:<name>", v.Line)
		}
		p, err := ParsePrincipal(v.Value)
		if err != nil {
			return fmt.Errorf("line %d: %w", v.Line, err)
		}
		out = append(out, p)
		return nil
	}
	switch n.Kind {
	case yaml.ScalarNode:
		if err := add(n); err != nil {
			return err
		}
	case yaml.SequenceNode:
		for _, c := range n.Content {
			if err := add(c); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("line %d: want a principal or a list of principals", n.Line)
	}
	*ps = out
	return nil
}

// MarshalYAML writes the canonical `user:`/`group:` list.
func (ps Principals) MarshalYAML() (any, error) {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.String()
	}
	return out, nil
}

// Matches reports whether any principal names this user or one of the groups.
// It returns the first matching principal.
func (ps Principals) Matches(userID string, groups []string) (Principal, bool) {
	for _, p := range ps {
		if p.Matches(userID, groups) {
			return p, true
		}
	}
	return Principal{}, false
}

// AllUsers reports whether every principal is a `user:` (true for an empty list).
func (ps Principals) AllUsers() bool {
	for _, p := range ps {
		if p.Kind != KindUser {
			return false
		}
	}
	return true
}

// Display joins the principals for a label: "priya, group:dataeng-oncall".
// The `user:` prefix is dropped. It is a label only; no right comes from it.
func (ps Principals) Display() string {
	parts := make([]string, len(ps))
	for i, p := range ps {
		parts[i] = p.Display()
	}
	return strings.Join(parts, ", ")
}

// Users returns the ids of the `user:` principals, in order.
func (ps Principals) Users() []string {
	var out []string
	for _, p := range ps {
		if p.Kind == KindUser {
			out = append(out, p.Name)
		}
	}
	return out
}

// Contains reports whether the exact principal is in the list.
func (ps Principals) Contains(want Principal) bool {
	for _, p := range ps {
		if p == want {
			return true
		}
	}
	return false
}

// UserLayers builds a layer map from plain user ids, for tests and demos:
// UserLayers(map[string]string{"postgres": "priya"}).
func UserLayers(m map[string]string) map[string]Principals {
	out := make(map[string]Principals, len(m))
	for layer, id := range m {
		out[layer] = Principals{{Kind: KindUser, Name: id}}
	}
	return out
}
