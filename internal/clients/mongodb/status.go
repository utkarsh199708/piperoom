package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// ReplicaSetStatus is the replica set state of the source as seen from the
// node the client connected to. State is the connecting node's role.
type ReplicaSetStatus struct {
	// IsReplicaSet is false for a standalone mongod and for a mongos (State is
	// "MONGOS" there).
	IsReplicaSet bool     `json:"is_replica_set"`
	SetName      string   `json:"set_name,omitempty"`
	State        string   `json:"state"` // PRIMARY | SECONDARY | ARBITER | OTHER | STANDALONE | MONGOS
	Members      []Member `json:"members,omitempty"`
}

// Member is one member of the replica set from replSetGetStatus.
type Member struct {
	Name   string `json:"name"`
	State  string `json:"state"`  // stateStr, e.g. PRIMARY, SECONDARY, ARBITER, RECOVERING
	Health int    `json:"health"` // 1 = healthy, 0 = down
	IsSelf bool   `json:"is_self"`
}

// ChangeStreamSupport reports whether the source can serve change streams,
// which the Debezium MongoDB connector needs.
type ChangeStreamSupport struct {
	Supported bool   `json:"supported"`
	Reason    string `json:"reason,omitempty"` // why not, when unsupported
}

// Version returns the server version string (buildInfo.version), e.g. "7.0.14".
func (c *Client) Version(ctx context.Context) (string, error) {
	reply, err := c.cmd(ctx, "admin", bson.D{{Key: "buildInfo", Value: 1}})
	if err != nil {
		return "", fmt.Errorf("mongodb: buildInfo: %w", err)
	}
	v := stringOf(reply["version"])
	if v == "" {
		return "", fmt.Errorf("mongodb: buildInfo: reply has no version")
	}
	return v, nil
}

// ReplicaSetStatus reads the replica set state. isMaster classifies the server
// (standalone, mongos or a replica set member); for a replica set member it
// also reads replSetGetStatus for the member list, falling back to the
// isMaster-derived state when that command fails (e.g. mid-election).
func (c *Client) ReplicaSetStatus(ctx context.Context) (ReplicaSetStatus, error) {
	h, err := c.hello(ctx)
	if err != nil {
		return ReplicaSetStatus{}, err
	}
	if msg := stringOf(h["msg"]); msg == "isdbgrid" {
		return ReplicaSetStatus{State: "MONGOS"}, nil
	}
	setName := stringOf(h["setName"])
	if setName == "" {
		return ReplicaSetStatus{State: "STANDALONE"}, nil
	}
	rs := ReplicaSetStatus{IsReplicaSet: true, SetName: setName, State: stateFromHello(h)}
	g, err := c.cmd(ctx, "admin", bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err != nil {
		// Still report what isMaster told us; the member list is empty.
		return rs, nil
	}
	rs.Members = parseMembers(g["members"])
	return rs, nil
}

// ChangeStreamSupport reports change-stream availability. A standalone mongod
// has no change streams; a replica set member or mongos does (every supported
// MongoDB version is 3.6+, where change streams appeared).
func (c *Client) ChangeStreamSupport(ctx context.Context) (ChangeStreamSupport, error) {
	h, err := c.hello(ctx)
	if err != nil {
		return ChangeStreamSupport{}, err
	}
	if msg := stringOf(h["msg"]); msg == "isdbgrid" {
		return ChangeStreamSupport{Supported: true}, nil
	}
	if setName := stringOf(h["setName"]); setName == "" {
		return ChangeStreamSupport{Supported: false, Reason: "standalone mongod has no change streams"}, nil
	}
	if _, ok := h["logicalSessionTimeoutMinutes"]; !ok {
		return ChangeStreamSupport{Supported: false, Reason: "server has no logical sessions (MongoDB < 3.6)"}, nil
	}
	return ChangeStreamSupport{Supported: true}, nil
}

// hello runs the isMaster command (which works on every supported MongoDB
// version, unlike hello which needs 5.0+) and returns its reply.
func (c *Client) hello(ctx context.Context) (bson.M, error) {
	reply, err := c.cmd(ctx, "admin", bson.D{{Key: "isMaster", Value: 1}})
	if err != nil {
		return nil, fmt.Errorf("mongodb: isMaster: %w", err)
	}
	return reply, nil
}

// stateFromHello returns the connecting node's role from the isMaster reply.
func stateFromHello(h bson.M) string {
	if isPrimary(h) {
		return "PRIMARY"
	}
	if asBool(h["secondary"]) {
		return "SECONDARY"
	}
	if asBool(h["arbiterOnly"]) {
		return "ARBITER"
	}
	return "OTHER"
}

// isPrimary reads both spellings: isWritablePrimary (5.0+) and ismaster (the
// deprecated alias still returned by 5.0+, and the only one before 5.0).
func isPrimary(h bson.M) bool {
	return asBool(h["isWritablePrimary"]) || asBool(h["ismaster"])
}

func parseMembers(v any) []Member {
	raw, ok := asSlice(v)
	if !ok {
		return nil
	}
	out := make([]Member, 0, len(raw))
	for _, r := range raw {
		m, ok := asMap(r)
		if !ok {
			continue
		}
		out = append(out, Member{
			Name:   stringOf(m["name"]),
			State:  stringOf(m["stateStr"]),
			Health: intOf(m["health"]),
			IsSelf: asBool(m["self"]),
		})
	}
	return out
}
