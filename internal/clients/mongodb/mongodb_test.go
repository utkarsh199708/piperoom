package mongodb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"piperoom/internal/clients"
)

// fakeRunner answers canned command replies and records what it was asked to
// run, so the unit tests need no real mongod.
type fakeRunner struct {
	replies     map[string]bson.M // "db.command" -> reply
	err         error             // when set, every command fails with it
	fail        map[string]error  // "db.command" -> error for that command only
	got         []bson.D
	deadlineSet bool
}

func (f *fakeRunner) runCommand(ctx context.Context, db string, cmd bson.D) (bson.M, error) {
	if _, ok := ctx.Deadline(); ok {
		f.deadlineSet = true
	}
	f.got = append(f.got, cmd)
	key := ""
	if len(cmd) > 0 {
		key = db + "." + cmd[0].Key
	}
	if f.err != nil {
		return nil, f.err
	}
	if e := f.fail[key]; e != nil {
		return nil, e
	}
	if r, ok := f.replies[key]; ok {
		return r, nil
	}
	return bson.M{"ok": 1}, nil
}

func (f *fakeRunner) disconnect(context.Context) error { return nil }

func newClient(f *fakeRunner) *Client { return &Client{run: f} }

// primaryRS is an isMaster reply for a healthy replica set primary.
func primaryRS() bson.M {
	return bson.M{
		"ismaster":                     true,
		"isWritablePrimary":            true,
		"secondary":                    false,
		"setName":                      "rs0",
		"logicalSessionTimeoutMinutes": int32(30),
	}
}

func replStatus() bson.M {
	return bson.M{
		"set":     "rs0",
		"myState": int32(1),
		"members": bson.A{
			bson.M{"_id": int32(0), "name": "a:27017", "stateStr": "PRIMARY", "health": int32(1), "self": true},
			bson.M{"_id": int32(1), "name": "b:27017", "stateStr": "SECONDARY", "health": int32(1)},
		},
	}
}

func TestVersion(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{"admin.buildInfo": {"version": "7.0.14"}}}
	got, err := newClient(f).Version(context.Background())
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if got != "7.0.14" {
		t.Fatalf("Version = %q, want 7.0.14", got)
	}
	if !f.deadlineSet {
		t.Fatal("expected a deadline on the command context")
	}
}

func TestVersionMissingField(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{"admin.buildInfo": {"ok": 1}}}
	if _, err := newClient(f).Version(context.Background()); err == nil {
		t.Fatal("expected an error when buildInfo has no version")
	}
}

func TestVersionErrorWrapped(t *testing.T) {
	boom := errors.New("boom")
	f := &fakeRunner{err: boom}
	_, err := newClient(f).Version(context.Background())
	if err == nil || !strings.Contains(err.Error(), "mongodb: buildInfo") {
		t.Fatalf("err = %v, want a wrapped buildInfo error", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("errors.Is(err, boom) = false, want true")
	}
}

func TestReplicaSetStatusPrimary(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{
		"admin.isMaster":         primaryRS(),
		"admin.replSetGetStatus": replStatus(),
	}}
	got, err := newClient(f).ReplicaSetStatus(context.Background())
	if err != nil {
		t.Fatalf("ReplicaSetStatus: %v", err)
	}
	if !got.IsReplicaSet || got.SetName != "rs0" || got.State != "PRIMARY" {
		t.Fatalf("got %+v, want primary of rs0", got)
	}
	if len(got.Members) != 2 || got.Members[0].Name != "a:27017" || got.Members[0].Health != 1 || !got.Members[0].IsSelf {
		t.Fatalf("members = %+v", got.Members)
	}
}

func TestReplicaSetStatusSecondary(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{
		"admin.isMaster": bson.M{"ismaster": false, "isWritablePrimary": false, "secondary": true, "setName": "rs0"},
		"admin.replSetGetStatus": bson.M{"set": "rs0", "myState": int32(2), "members": bson.A{
			bson.M{"_id": int32(0), "name": "a:27017", "stateStr": "SECONDARY", "health": 1.0, "self": true},
		}},
	}}
	got, err := newClient(f).ReplicaSetStatus(context.Background())
	if err != nil {
		t.Fatalf("ReplicaSetStatus: %v", err)
	}
	if got.State != "SECONDARY" || !got.IsReplicaSet {
		t.Fatalf("got %+v, want secondary", got)
	}
}

func TestReplicaSetStatusStandalone(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{"admin.isMaster": {"ismaster": true}}}
	got, err := newClient(f).ReplicaSetStatus(context.Background())
	if err != nil {
		t.Fatalf("ReplicaSetStatus: %v", err)
	}
	if got.IsReplicaSet || got.State != "STANDALONE" {
		t.Fatalf("got %+v, want standalone", got)
	}
}

func TestReplicaSetStatusMongos(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{"admin.isMaster": {"msg": "isdbgrid", "ismaster": true}}}
	got, err := newClient(f).ReplicaSetStatus(context.Background())
	if err != nil {
		t.Fatalf("ReplicaSetStatus: %v", err)
	}
	if got.IsReplicaSet || got.State != "MONGOS" {
		t.Fatalf("got %+v, want mongos", got)
	}
}

func TestReplicaSetStatusFallsBackWhenGetStatusFails(t *testing.T) {
	f := &fakeRunner{
		replies: map[string]bson.M{"admin.isMaster": primaryRS()},
		fail:    map[string]error{"admin.replSetGetStatus": errors.New("not running with --replSet")},
	}
	got, err := newClient(f).ReplicaSetStatus(context.Background())
	if err != nil {
		t.Fatalf("ReplicaSetStatus must not fail when replSetGetStatus fails: %v", err)
	}
	if !got.IsReplicaSet || got.State != "PRIMARY" || len(got.Members) != 0 {
		t.Fatalf("got %+v, want primary with no members", got)
	}
}

func TestChangeStreamSupport(t *testing.T) {
	tests := []struct {
		name      string
		hello     bson.M
		supported bool
		reason    string
	}{
		{"primary", primaryRS(), true, ""},
		{"mongos", bson.M{"msg": "isdbgrid", "ismaster": true}, true, ""},
		{"standalone", bson.M{"ismaster": true}, false, "standalone"},
		{"no logical sessions", bson.M{"ismaster": true, "setName": "rs0"}, false, "logical sessions"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeRunner{replies: map[string]bson.M{"admin.isMaster": tt.hello}}
			got, err := newClient(f).ChangeStreamSupport(context.Background())
			if err != nil {
				t.Fatalf("ChangeStreamSupport: %v", err)
			}
			if got.Supported != tt.supported {
				t.Fatalf("Supported = %v, want %v (reason %q)", got.Supported, tt.supported, got.Reason)
			}
			if !tt.supported && !strings.Contains(got.Reason, tt.reason) {
				t.Fatalf("Reason = %q, want it to mention %q", got.Reason, tt.reason)
			}
		})
	}
}

func TestRoles(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{"admin.connectionStatus": {
		"authInfo": bson.M{
			"authenticatedUsers": bson.A{bson.M{"user": "debezium", "db": "orders"}},
			"authenticatedUserRoles": bson.A{
				bson.M{"role": "read", "db": "orders"},
				bson.M{"role": "readWrite", "db": "app"},
			},
		},
	}}}
	got, err := newClient(f).Roles(context.Background())
	if err != nil {
		t.Fatalf("Roles: %v", err)
	}
	if len(got) != 2 || got[0].Role != "read" || got[0].DB != "orders" || got[1].Role != "readWrite" {
		t.Fatalf("Roles = %+v", got)
	}
}

func connStatusWithPrivileges() bson.M {
	return bson.M{
		"authInfo": bson.M{
			"authenticatedUsers":     bson.A{bson.M{"user": "debezium", "db": "orders"}},
			"authenticatedUserRoles": bson.A{bson.M{"role": "read", "db": "orders"}},
			"authenticatedUserPrivileges": bson.A{
				bson.M{"resource": bson.M{"db": "orders", "collection": "customers"}, "actions": bson.A{"find", "changeStream"}},
				bson.M{"resource": bson.M{"db": "local", "collection": "oplog.rs"}, "actions": bson.A{"find"}},
			},
		},
	}
}

func TestRoleInfo(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{"admin.connectionStatus": connStatusWithPrivileges()}}
	c := newClient(f)

	ri, err := c.RoleInfo(context.Background(), "read", []string{"orders.customers", "orders.missing"})
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if !ri.Exists {
		t.Fatalf("RoleInfo = %+v, want Exists", ri)
	}
	if !ri.CanLogin {
		t.Fatalf("RoleInfo = %+v, want CanLogin", ri)
	}
	if !ri.Replication {
		t.Fatalf("RoleInfo = %+v, want Replication (oplog read)", ri)
	}
	if !ri.Selectable["orders.customers"] || ri.Selectable["orders.missing"] {
		t.Fatalf("Selectable = %+v", ri.Selectable)
	}

	// A role the user does not hold.
	ri, err = c.RoleInfo(context.Background(), "dbAdmin", nil)
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if ri.Exists {
		t.Fatalf("RoleInfo = %+v, want not Exists", ri)
	}
}

func TestRoleInfoMatchesDBDotRole(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{"admin.connectionStatus": connStatusWithPrivileges()}}
	ri, err := newClient(f).RoleInfo(context.Background(), "orders.read", nil)
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if !ri.Exists {
		t.Fatalf("RoleInfo = %+v, want Exists for orders.read", ri)
	}
}

func TestSettings(t *testing.T) {
	f := &fakeRunner{replies: map[string]bson.M{
		"admin.buildInfo":        {"version": "7.0.14"},
		"admin.isMaster":         primaryRS(),
		"admin.replSetGetStatus": replStatus(),
	}}
	got, err := newClient(f).Settings(context.Background())
	if err != nil {
		t.Fatalf("Settings: %v", err)
	}
	want := map[string]string{
		"version":           "7.0.14",
		"server_type":       "replica_set_member",
		"replica_set":       "rs0",
		"replica_set_state": "PRIMARY",
		"change_streams":    "supported",
	}
	if len(got) != len(want) {
		t.Fatalf("Settings = %+v, want %d entries", got, len(want))
	}
	for _, s := range got {
		if want[s.Name] != s.Value {
			t.Fatalf("setting %q = %q, want %q", s.Name, s.Value, want[s.Name])
		}
	}
}

func TestTableReplicationNotSupported(t *testing.T) {
	_, err := newClient(&fakeRunner{}).TableReplication(context.Background(), []string{"orders.customers"})
	if !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err = %v, want clients.ErrNotSupported", err)
	}
}

func TestBlockersNotSupported(t *testing.T) {
	_, err := newClient(&fakeRunner{}).Blockers(context.Background(), time.Minute)
	if !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err = %v, want clients.ErrNotSupported", err)
	}
}

func TestRedactURI(t *testing.T) {
	tests := []struct{ in, want string }{
		{"mongodb://u:p@h:27017/db", "mongodb://u:***@h:27017/db"},
		{"mongodb+srv://u:p@cluster0.mongodb.net/db?retryWrites=true", "mongodb+srv://u:***@cluster0.mongodb.net/db?retryWrites=true"},
		{"mongodb://h:27017/db", "mongodb://h:27017/db"},
		{"mongodb://u@h:27017/db", "mongodb://u@h:27017/db"},
	}
	for _, tt := range tests {
		if got := redactURI(tt.in); got != tt.want {
			t.Fatalf("redactURI(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRedactErr(t *testing.T) {
	uri := "mongodb://debezium:sekret@h:27017/orders"
	inner := errors.New("connect failed: mongodb://debezium:sekret@h:27017/orders: auth failed")
	got := redactErr(inner, uri)
	if strings.Contains(got.Error(), "sekret") {
		t.Fatalf("redacted error still leaks the password: %v", got)
	}
	if !errors.Is(got, inner) {
		t.Fatal("redacted error lost the wrapped error")
	}
	if redactErr(nil, uri) != nil {
		t.Fatal("redactErr(nil) must be nil")
	}
}
