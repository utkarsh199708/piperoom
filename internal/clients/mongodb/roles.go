package mongodb

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Role is one role the authenticated user holds, as connectionStatus reports it.
type Role struct {
	Role string `json:"role"`
	DB   string `json:"db,omitempty"`
}

// privilege is one effective privilege from connectionStatus.authInfo (or one
// inherited privilege from usersInfo). A privilege names a resource as either a
// specific db.collection, an empty db (any database) with an empty collection
// (any non-system collection), a cluster resource, or the special anyResource.
type privilege struct {
	db          string // resource.db ("" = any database when anyResource is unset)
	collection  string // resource.collection ("" = every non-system collection in db)
	anyResource bool   // resource.anyResource: any resource (cluster excepted)
	cluster     bool   // resource.cluster: a cluster-level resource
	actions     []string
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
	ai.privileges = parsePrivileges(doc["authenticatedUserPrivileges"])
	return ai, nil
}

// userInfo reads another user's effective privileges with the read-only
// usersInfo command (which requires the viewUser action). found reports whether
// the user exists. A bare user (no auth database) is looked up in the admin
// database, where MongoDB service accounts live by convention.
func (c *Client) userInfo(ctx context.Context, user, authDB string) (authInfo, bool, error) {
	spec := any(user)
	db := "admin"
	if authDB != "" {
		spec = bson.M{"user": user, "db": authDB}
		db = authDB
	}
	reply, err := c.cmd(ctx, db, bson.D{
		{Key: "usersInfo", Value: spec},
		{Key: "showPrivileges", Value: true},
	})
	if err != nil {
		return authInfo{}, false, fmt.Errorf("mongodb: usersInfo: %w", err)
	}
	users, ok := asSlice(reply["users"])
	if !ok || len(users) == 0 {
		return authInfo{}, false, nil
	}
	u, ok := asMap(users[0])
	if !ok {
		return authInfo{}, false, fmt.Errorf("mongodb: usersInfo: unexpected users element")
	}
	return authInfo{
		user:       stringOf(u["user"]),
		db:         stringOf(u["db"]),
		privileges: parsePrivileges(u["inheritedPrivileges"]),
	}, true, nil
}

// parsePrivileges converts a decoded privileges array (the
// authenticatedUserPrivileges or inheritedPrivileges field) into []privilege.
func parsePrivileges(v any) []privilege {
	raw, ok := asSlice(v)
	if !ok {
		return nil
	}
	out := make([]privilege, 0, len(raw))
	for _, p := range raw {
		m, ok := asMap(p)
		if !ok {
			continue
		}
		res, _ := asMap(m["resource"])
		out = append(out, privilege{
			db:          stringOf(res["db"]),
			collection:  stringOf(res["collection"]),
			anyResource: asBool(res["anyResource"]),
			cluster:     asBool(res["cluster"]),
			actions:     stringSlice(m["actions"]),
		})
	}
	return out
}

// canReadOplog reports whether the user can read the oplog (local.oplog.rs),
// which the connector's cursor mode needs. Reading the oplog requires find;
// changeStream never counts toward oplog read access.
func (ai authInfo) canReadOplog() bool {
	find, _ := ai.effectiveActions("local", "oplog.rs")
	return find
}

// can reports whether the user can open a change stream on db.coll: the
// effective actions across every matching privilege must include both find and
// changeStream. MongoDB requires both actions to open a change-stream cursor.
// A privilege matches when it names db.coll exactly, names the whole database
// (collection == "" matches any non-system collection), names any database
// (db == ""), or grants anyResource with anyAction. Cluster-level privileges
// never grant collection read access.
func (ai authInfo) can(db, coll string) bool {
	find, changeStream := ai.effectiveActions(db, coll)
	return find && changeStream
}

// effectiveActions returns whether the union of every privilege matching
// db.coll grants find and changeStream. anyAction grants both, and the two
// actions may come from different privileges that match the same resource (for
// example find on the whole database and changeStream on one collection).
func (ai authInfo) effectiveActions(db, coll string) (find, changeStream bool) {
	for _, p := range ai.privileges {
		if p.cluster {
			continue
		}
		if p.anyResource {
			if hasAction(p.actions, "anyAction") {
				return true, true
			}
			continue
		}
		if p.db != "" && p.db != db {
			continue
		}
		if p.collection != "" {
			if p.collection != coll {
				continue
			}
		} else if strings.HasPrefix(coll, "system.") {
			// collection == "" covers every non-system collection in the db.
			continue
		}
		for _, a := range p.actions {
			switch a {
			case "anyAction":
				return true, true
			case "find":
				find = true
			case "changeStream":
				changeStream = true
			}
		}
	}
	return find, changeStream
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
