package mongodb

import (
	"context"
	"strings"
	"time"

	"piperoom/internal/clients"
)

// The types below mirror the clients.SourceSettingsReader contract proposed in
// tasks/Q-board.md Q2a (Settings / TableReplication / RoleInfo / Blockers and
// the DBSetting, TableReplication, RoleInfo and DBSession DTOs). They are not
// yet in internal/clients/clients.go, so the client implements this local copy
// and the PR carries a "Contract change request" to land the interface in
// clients.go (after which these types become aliases). Field names, JSON tags
// and semantics follow the Q-board exactly.

// SourceSettingsReader is the read-only source-settings surface of this client.
type SourceSettingsReader interface {
	Settings(ctx context.Context) ([]DBSetting, error)
	TableReplication(ctx context.Context, tables []string) ([]TableReplication, error)
	RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error)
	Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error)
}

// DBSetting is one named source setting. PendingRestart is always false for
// MongoDB (there is no ALTER SYSTEM-style setting a restart would apply).
type DBSetting struct {
	Name           string `json:"name"`
	Value          string `json:"value"`
	Unit           string `json:"unit,omitempty"`
	PendingRestart bool   `json:"pending_restart,omitempty"`
}

// TableReplication mirrors the contract DTO; MongoDB has no per-table
// replica identity, so TableReplication returns clients.ErrNotSupported.
type TableReplication struct {
	Table           string `json:"table"`
	HasPrimaryKey   bool   `json:"has_primary_key"`
	ReplicaIdentity string `json:"replica_identity,omitempty"` // default | full | index | nothing; "" when the source has none
}

// RoleInfo mirrors the contract DTO. Replication means the user can read the
// oplog (local.oplog.rs); Selectable maps each requested "db.collection" to
// whether the user can find/changeStream it.
type RoleInfo struct {
	Role        string          `json:"role"`
	Exists      bool            `json:"exists"`
	CanLogin    bool            `json:"can_login"`
	Replication bool            `json:"replication"`
	Selectable  map[string]bool `json:"selectable"`
}

// DBSession mirrors the contract DTO; MongoDB has no SQL-style open
// transaction holding a WAL horizon, so Blockers returns clients.ErrNotSupported.
type DBSession struct {
	PID        int64     `json:"pid"`
	StartedAt  time.Time `json:"started_at"`
	XactStart  time.Time `json:"xact_start"`
	User       string    `json:"user"`
	App        string    `json:"application"`
	State      string    `json:"state"`
	Query      string    `json:"query"`
	BlocksSlot bool      `json:"blocks_slot"`
}

var _ SourceSettingsReader = (*Client)(nil)

// Settings returns the fixed list of source settings the setup driver checks:
// the server version, the server type (standalone, replica set member or
// mongos), the replica set name and state, and change-stream support.
func (c *Client) Settings(ctx context.Context) ([]DBSetting, error) {
	version, err := c.Version(ctx)
	if err != nil {
		return nil, err
	}
	rs, err := c.ReplicaSetStatus(ctx)
	if err != nil {
		return nil, err
	}
	cs, err := c.ChangeStreamSupport(ctx)
	if err != nil {
		return nil, err
	}
	changeStreams := "not supported"
	if cs.Supported {
		changeStreams = "supported"
	}
	serverType := "standalone"
	switch rs.State {
	case "MONGOS":
		serverType = "mongos"
	case "STANDALONE":
		serverType = "standalone"
	default:
		serverType = "replica_set_member"
	}
	return []DBSetting{
		{Name: "version", Value: version},
		{Name: "server_type", Value: serverType},
		{Name: "replica_set", Value: rs.SetName},
		{Name: "replica_set_state", Value: rs.State},
		{Name: "change_streams", Value: changeStreams},
	}, nil
}

// TableReplication has no MongoDB equivalent: change streams capture every
// collection in the connector's include list, and there is no per-table
// replica identity or primary key to enable. It answers ErrNotSupported.
func (c *Client) TableReplication(ctx context.Context, tables []string) ([]TableReplication, error) {
	return nil, clients.ErrNotSupported
}

// RoleInfo reports what the named user can read. role is a MongoDB user, as
// either the bare user name or "authDb.user". When it names the authenticated
// user the connectionStatus privileges are used; otherwise the user's
// inheritedPrivileges are read with usersInfo (requires viewUser). Exists is
// whether the user was found; Replication is whether they can read the oplog;
// Selectable is per-collection read ability for the requested tables (a
// MongoDB "table" is a "db.collection").
func (c *Client) RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error) {
	user, authDB := splitUser(role)
	ai, err := c.auth(ctx)
	if err != nil {
		return RoleInfo{}, err
	}
	ri := RoleInfo{Role: role}
	switch {
	case user != "" && user == ai.user && (authDB == "" || authDB == ai.db):
		// role names the authenticated user; connectionStatus has their
		// effective privileges.
		ri.Exists = true
	default:
		ui, found, err := c.userInfo(ctx, user, authDB)
		if err != nil {
			return RoleInfo{}, err
		}
		ai, ri.Exists = ui, found
	}
	ri.CanLogin = ri.Exists
	ri.Replication = ri.Exists && ai.canReadOplog()
	if len(tables) > 0 {
		ri.Selectable = make(map[string]bool, len(tables))
		for _, t := range tables {
			db, coll := splitCollection(t)
			ri.Selectable[t] = ai.can(db, coll)
		}
	}
	return ri, nil
}

// Blockers has no MongoDB equivalent: change streams keep a resume token and
// there is no replication slot or open transaction holding a WAL horizon.
// It answers ErrNotSupported.
func (c *Client) Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error) {
	return nil, clients.ErrNotSupported
}

// splitCollection splits "db.collection" into its two parts; a bare name is
// treated as a collection in the empty database (callers should pass
// "db.collection").
func splitCollection(name string) (db, coll string) {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "", name
}

// splitUser splits "authDb.user" into its two parts; a bare name is a user in
// an unspecified auth database.
// splitUser splits "authDb.user" at the first dot. A bare user name that
// contains a dot is therefore read as authDb.user; pass a dotted user with its
// auth database explicitly.
func splitUser(name string) (user, authDB string) {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[i+1:], name[:i]
	}
	return name, ""
}
