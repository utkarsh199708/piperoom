package mongodb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"piperoom/internal/clients"
)

// fakeRunner answers canned command replies and records what it was asked to
// run, so the unit tests need no real mongod.
type fakeRunner struct {
	replies       map[string]bson.M // "db.command" -> reply
	err           error             // when set, every command fails with it
	fail          map[string]error  // "db.command" -> error for that command only
	got           []bson.D
	deadlineSet   bool
	disconnectErr error
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

func (f *fakeRunner) disconnect(context.Context) error { return f.disconnectErr }

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

func TestReplicaSetStatusPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := &fakeRunner{
		replies: map[string]bson.M{"admin.isMaster": primaryRS()},
		fail:    map[string]error{"admin.replSetGetStatus": context.Canceled},
	}
	_, err := newClient(f).ReplicaSetStatus(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
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

	// The authenticated user, named bare.
	ri, err := c.RoleInfo(context.Background(), "debezium", []string{"orders.customers", "orders.missing"})
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

	// The authenticated user, named authDb.user.
	ri, err = c.RoleInfo(context.Background(), "orders.debezium", nil)
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if !ri.Exists {
		t.Fatalf("RoleInfo = %+v, want Exists for orders.debezium", ri)
	}

	// A user the connection is not authenticated as, found via usersInfo.
	f2 := &fakeRunner{replies: map[string]bson.M{
		"admin.connectionStatus": connStatusWithPrivileges(),
		"admin.usersInfo": {
			"users": bson.A{bson.M{
				"user": "alice",
				"db":   "admin",
				"inheritedPrivileges": bson.A{
					bson.M{"resource": bson.M{"db": "app", "collection": ""}, "actions": bson.A{"find", "changeStream"}},
					bson.M{"resource": bson.M{"db": "local", "collection": "oplog.rs"}, "actions": bson.A{"find"}},
				},
			}},
		},
	}}
	ri, err = newClient(f2).RoleInfo(context.Background(), "alice", []string{"app.things", "otherdb.things"})
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if !ri.Exists || !ri.CanLogin {
		t.Fatalf("RoleInfo = %+v, want Exists/CanLogin for alice", ri)
	}
	if !ri.Replication {
		t.Fatalf("RoleInfo = %+v, want Replication (oplog read)", ri)
	}
	if !ri.Selectable["app.things"] || ri.Selectable["otherdb.things"] {
		t.Fatalf("Selectable = %+v", ri.Selectable)
	}

	// A user named authDb.user, found via usersInfo against that auth db.
	f2b := &fakeRunner{replies: map[string]bson.M{
		"admin.connectionStatus": connStatusWithPrivileges(),
		"app.usersInfo": {
			"users": bson.A{bson.M{
				"user": "alice",
				"db":   "app",
				"inheritedPrivileges": bson.A{
					bson.M{"resource": bson.M{"db": "app", "collection": "things"}, "actions": bson.A{"find", "changeStream"}},
				},
			}},
		},
	}}
	ri, err = newClient(f2b).RoleInfo(context.Background(), "app.alice", []string{"app.things", "app.other"})
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if !ri.Exists || !ri.Selectable["app.things"] || ri.Selectable["app.other"] {
		t.Fatalf("RoleInfo = %+v, want Exists and only app.things selectable", ri)
	}

	// A user nobody holds: usersInfo finds nothing.
	f3 := &fakeRunner{replies: map[string]bson.M{
		"admin.connectionStatus": connStatusWithPrivileges(),
		"admin.usersInfo":        {"users": bson.A{}},
	}}
	ri, err = newClient(f3).RoleInfo(context.Background(), "ghost", nil)
	if err != nil {
		t.Fatalf("RoleInfo: %v", err)
	}
	if ri.Exists {
		t.Fatalf("RoleInfo = %+v, want not Exists", ri)
	}
}

func TestCan(t *testing.T) {
	tests := []struct {
		name  string
		privs []privilege
		db    string
		coll  string
		want  bool
	}{
		{"find and changeStream match any collection", []privilege{{actions: []string{"find", "changeStream"}}}, "orders", "customers", true},
		{"readAnyDatabase does not match the oplog", []privilege{{actions: []string{"find", "changeStream"}}}, "local", "oplog.rs", false},
		{"readAnyDatabase plus a local grant matches the oplog", []privilege{
			{actions: []string{"find", "changeStream"}},
			{db: "local", actions: []string{"find", "changeStream"}},
		}, "local", "oplog.rs", true},
		{"readAnyDatabase does not match config", []privilege{{actions: []string{"find", "changeStream"}}}, "config", "settings", false},
		{"only find is not selectable", []privilege{{actions: []string{"find"}}}, "orders", "customers", false},
		{"only changeStream is not selectable", []privilege{{actions: []string{"changeStream"}}}, "orders", "customers", false},
		{"find and changeStream across two matching privileges", []privilege{
			{actions: []string{"find"}},
			{actions: []string{"changeStream"}},
		}, "orders", "customers", true},
		{"anyAction alone grants both", []privilege{{actions: []string{"anyAction"}}}, "orders", "customers", true},
		{"readAnyDatabase excludes system collections", []privilege{{actions: []string{"find", "changeStream"}}}, "admin", "system.users", false},
		{"db-wide privilege matches its collections", []privilege{{db: "orders", actions: []string{"find", "changeStream"}}}, "orders", "customers", true},
		{"db-wide privilege does not match other dbs", []privilege{{db: "orders", actions: []string{"find", "changeStream"}}}, "local", "oplog.rs", false},
		{"db-wide privilege excludes system collections", []privilege{{db: "orders", actions: []string{"find", "changeStream"}}}, "orders", "system.profile", false},
		{"anyResource with anyAction matches anything", []privilege{{anyResource: true, actions: []string{"anyAction"}}}, "local", "oplog.rs", true},
		{"anyResource without anyAction does not match", []privilege{{anyResource: true, actions: []string{"find", "changeStream"}}}, "orders", "customers", false},
		{"cluster-only role matches no collection", []privilege{{cluster: true, actions: []string{"anyAction"}}}, "orders", "customers", false},
		{"specific collection matches exactly", []privilege{{db: "local", collection: "oplog.rs", actions: []string{"find", "changeStream"}}}, "local", "oplog.rs", true},
		{"specific collection does not match siblings", []privilege{{db: "local", collection: "oplog.rs", actions: []string{"find", "changeStream"}}}, "local", "startup_log", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ai := authInfo{privileges: tt.privs}
			if got := ai.can(tt.db, tt.coll); got != tt.want {
				t.Fatalf("can(%q, %q) = %v, want %v (privs %+v)", tt.db, tt.coll, got, tt.want, tt.privs)
			}
		})
	}
}

func TestCanReadOplog(t *testing.T) {
	tests := []struct {
		name  string
		privs []privilege
		want  bool
	}{
		{"find on the oplog", []privilege{{db: "local", collection: "oplog.rs", actions: []string{"find"}}}, true},
		{"changeStream alone does not read the oplog", []privilege{{db: "local", collection: "oplog.rs", actions: []string{"changeStream"}}}, false},
		{"find and changeStream on the oplog", []privilege{{db: "local", collection: "oplog.rs", actions: []string{"find", "changeStream"}}}, true},
		{"anyAction on the oplog", []privilege{{db: "local", collection: "oplog.rs", actions: []string{"anyAction"}}}, true},
		{"anyResource with anyAction reads the oplog", []privilege{{anyResource: true, actions: []string{"anyAction"}}}, true},
		{"find on another collection does not read the oplog", []privilege{{db: "local", collection: "startup_log", actions: []string{"find"}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ai := authInfo{privileges: tt.privs}
			if got := ai.canReadOplog(); got != tt.want {
				t.Fatalf("canReadOplog() = %v, want %v (privs %+v)", got, tt.want, tt.privs)
			}
		})
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

func TestCloseIsIdempotent(t *testing.T) {
	f := &fakeRunner{disconnectErr: mongo.ErrClientDisconnected}
	if err := newClient(f).Close(context.Background()); err != nil {
		t.Fatalf("Close after an earlier disconnect = %v, want nil", err)
	}
	f.disconnectErr = errors.New("boom")
	if err := newClient(f).Close(context.Background()); err == nil {
		t.Fatal("Close swallowed a real disconnect error")
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

func TestRedactErrDecodedPassword(t *testing.T) {
	// The URI carries the percent-encoded password; the driver error carries
	// the decoded form. Both must be redacted.
	uri := "mongodb://debezium:sek%40ret@h:27017/orders"
	inner := errors.New("connect failed: mongodb://debezium:sek@ret@h:27017/orders: auth failed")
	got := redactErr(inner, uri)
	for _, leak := range []string{"sek%40ret", "sek@ret"} {
		if strings.Contains(got.Error(), leak) {
			t.Fatalf("redacted error still leaks %q: %v", leak, got)
		}
	}
	if !errors.Is(got, inner) {
		t.Fatal("redacted error lost the wrapped error")
	}
}
