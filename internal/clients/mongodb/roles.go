package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// Role is one role the authenticated user holds, as connectionStatus reports it.
type Role struct {
	Role string `json:"role"`
	DB   string `json:"db,omitempty"`
}

// privilege is one effective privilege from connectionStatus.authInfo.
type privilege struct {
	db         string // resource.db ("" = any resource when collection is also "")
	collection string // resource.collection ("" = the whole db)
	actions    []string
}

// authInfo is the connectionStatus.authInfo document.
type authInfo struct {
	user       string
	db         string
	roles      []Role
	privileges []privilege
}

// Roles returns the roles of the authenticated user
// (connectionStatus.authInfo.authenticatedUserRoles). It changes nothing.
func (c *Client) Roles(ctx context.Context) ([]Role, error) {
	ai, err := c.auth(ctx)
	if err != nil {
		return nil, err
	}
	return ai.roles, nil
}

// auth reads connectionStatus with showPrivileges (roles plus the user's
// effective privileges in one round trip) and parses the authInfo document.
func (c *Client) auth(ctx context.Context) (authInfo, error) {
	reply, err := c.cmd(ctx, "admin", bson.D{
		{Key: "connectionStatus", Value: 1},
		{Key: "showPrivileges", Value: true},
	})
	if err != nil {
		return authInfo{}, fmt.Errorf("mongodb: connectionStatus: %w", err)
	}
	doc, ok := asMap(reply["authInfo"])
	if !ok {
		return authInfo{}, fmt.Errorf("mongodb: connectionStatus: reply has no authInfo")
	}
	ai := authInfo{}
	if users, ok := asSlice(doc["authenticatedUsers"]); ok && len(users) > 0 {
		if u, ok := asMap(users[0]); ok {
			ai.user, ai.db = stringOf(u["user"]), stringOf(u["db"])
		}
	}
	if roles, ok := asSlice(doc["authenticatedUserRoles"]); ok {
		for _, r := range roles {
			m, ok := asMap(r)
			if !ok {
				continue
			}
			ai.roles = append(ai.roles, Role{Role: stringOf(m["role"]), DB: stringOf(m["db"])})
		}
	}
	if privs, ok := asSlice(doc["authenticatedUserPrivileges"]); ok {
		for _, p := range privs {
			m, ok := asMap(p)
			if !ok {
				continue
			}
			res, _ := asMap(m["resource"])
			ai.privileges = append(ai.privileges, privilege{
				db:         stringOf(res["db"]),
				collection: stringOf(res["collection"]),
				actions:    stringSlice(m["actions"]),
			})
		}
	}
	return ai, nil
}

// canReadOplog reports whether the user can read the oplog (local.oplog.rs),
// which the connector's cursor mode needs.
func (ai authInfo) canReadOplog() bool {
	return ai.can("local", "oplog.rs")
}

// can reports whether the user holds find, changeStream or anyAction on
// db.col (or on the whole db, or on any resource).
func (ai authInfo) can(db, coll string) bool {
	for _, p := range ai.privileges {
		if p.db == "" && p.collection == "" {
			// anyResource (e.g. the root role).
			if hasAction(p.actions, "anyAction") {
				return true
			}
			continue
		}
		if p.db != db {
			continue
		}
		if p.collection != "" && p.collection != coll {
			continue
		}
		if hasAction(p.actions, "find", "changeStream", "anyAction") {
			return true
		}
	}
	return false
}

func hasAction(actions []string, want ...string) bool {
	for _, a := range actions {
		for _, w := range want {
			if a == w {
				return true
			}
		}
	}
	return false
}

func stringSlice(v any) []string {
	raw, ok := asSlice(v)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
