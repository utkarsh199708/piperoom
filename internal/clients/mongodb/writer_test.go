package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- writer fakes ----

// sentCmd is one command the writer sent: the database and the document.
type sentCmd struct {
	db  string
	cmd bson.D
}

// writeRunner is the setup writer's fake: canned replies and per-command
// errors keyed by "db.command", and a record of every command sent (with its
// database), so a test can assert which commands ran and what they carried.
type writeRunner struct {
	replies map[string]bson.M
	fail    map[string]error
	block   bool // every command waits for ctx to end
	sent    []sentCmd
}

func (f *writeRunner) runCommand(ctx context.Context, db string, cmd bson.D) (bson.M, error) {
	f.sent = append(f.sent, sentCmd{db: db, cmd: cmd})
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("fake: command sent without a deadline")
	}
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	key := db + "." + cmd[0].Key
	if e := f.fail[key]; e != nil {
		return nil, e
	}
	if r, ok := f.replies[key]; ok {
		return r, nil
	}
	return bson.M{"ok": 1}, nil
}

func (f *writeRunner) disconnect(context.Context) error { return nil }

// names lists the command names sent, as "db.command".
func (f *writeRunner) names() []string {
	out := make([]string, 0, len(f.sent))
	for _, s := range f.sent {
		out = append(out, s.db+"."+s.cmd[0].Key)
	}
	return out
}

// memStore records the one secret the writer stores.
type memStore struct {
	name, value string
	calls       int
	err         error
}

func (m *memStore) Store(_ context.Context, name, value string) error {
	m.calls++
	if m.err != nil {
		return m.err
	}
	m.name, m.value = name, value
	return nil
}

func testSetupWriter(f *writeRunner, s SecretStore) *SetupWriter {
	return &SetupWriter{run: f, timeout: defaultTimeout, secrets: s}
}

func unscoped() context.Context { return tools.WithUnscoped(context.Background()) }

func pipelineCtx(tables ...string) context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{ID: "p1", SourceTables: tables})
}

const testNonce = "0123456789abcdef0123456789abcdef"

// lookup returns the value of key in d, or nil.
func lookup(d bson.D, key string) any {
	for _, e := range d {
		if e.Key == key {
			return e.Value
		}
	}
	return nil
}

// ---- tests ----

func TestNewSetupWriterNeedsWriteMode(t *testing.T) {
	const uri = "mongodb://setup:Wr1tePassw0rd@db.corp:27017/?replicaSet=rs0"
	t.Run("write mode off", func(t *testing.T) {
		w, err := NewSetupWriter(context.Background(), false, uri)
		if w != nil {
			t.Fatal("got a writer with write mode off")
		}
		if !errors.Is(err, ErrWriteModeOff) {
			t.Fatalf("err = %v, want ErrWriteModeOff", err)
		}
		if strings.Contains(err.Error(), "Wr1tePassw0rd") || strings.Contains(err.Error(), uri) {
			t.Fatalf("error leaks the URI: %q", err)
		}
	})
	for _, u := range []string{"", "   "} {
		t.Run(fmt.Sprintf("empty uri %q", u), func(t *testing.T) {
			w, err := NewSetupWriter(context.Background(), true, u)
			if w != nil {
				t.Fatal("got a writer without a write URI")
			}
			if !errors.Is(err, ErrNoWriter) {
				t.Fatalf("err = %v, want ErrNoWriter", err)
			}
			if strings.Contains(err.Error(), "mongodb://") {
				t.Fatalf("error carries a URI: %q", err)
			}
		})
	}
	t.Run("zero writer", func(t *testing.T) {
		var w *SetupWriter
		if err := w.CreateCanaryTable(unscoped(), "orders.canary"); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("nil writer: err = %v, want ErrNoWriter", err)
		}
		if _, err := (&SetupWriter{}).CreateReadRole(unscoped(), "dbz", []string{"orders.customers"}); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("zero writer: err = %v, want ErrNoWriter", err)
		}
	})
}

func TestCreateReadRoleReturnsOnlySecretName(t *testing.T) {
	f := &writeRunner{replies: map[string]bson.M{
		"admin.usersInfo": {"users": bson.A{}, "ok": 1},
		"admin.rolesInfo": {"roles": bson.A{}, "ok": 1},
	}}
	store := &memStore{}
	w := testSetupWriter(f, store)
	ctx := pipelineCtx("orders.customers", "orders.items")

	name, err := w.CreateReadRole(ctx, "dbz_reader", []string{"orders.customers", "orders.items", "orders.customers"})
	if err != nil {
		t.Fatal(err)
	}
	if name != readPasswordSecretName {
		t.Fatalf("name = %q, want %q", name, readPasswordSecretName)
	}
	if store.name != readPasswordSecretName || len(store.value) != 48 {
		t.Fatalf("store got name %q and a %d-char value, want %q and 48 hex chars", store.name, len(store.value), readPasswordSecretName)
	}
	if strings.Contains(name, store.value) {
		t.Fatal("the returned name carries the password")
	}

	want := []string{"admin.usersInfo", "admin.rolesInfo", "admin.createRole", "admin.createUser"}
	if got := f.names(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("commands = %v, want %v", got, want)
	}

	// The password travels only in the createUser pwd field: no other command
	// carries it, and no other field of createUser does.
	for i, s := range f.sent {
		raw := fmt.Sprint(s.cmd)
		if s.cmd[0].Key != "createUser" {
			if strings.Contains(raw, store.value) {
				t.Fatalf("command %d (%s) carries the password", i, s.cmd[0].Key)
			}
			continue
		}
		if lookup(s.cmd, "pwd") != store.value {
			t.Fatal("createUser pwd is not the stored password")
		}
		if lookup(s.cmd, "createUser") != "dbz_reader" {
			t.Fatalf("createUser user = %v", lookup(s.cmd, "createUser"))
		}
		roles := fmt.Sprint(lookup(s.cmd, "roles"))
		if !strings.Contains(roles, "dbz_reader_cdc") || strings.Contains(roles, "readWrite") || strings.Contains(roles, "root") {
			t.Fatalf("createUser roles = %s, want only dbz_reader_cdc", roles)
		}
	}

	// The custom role: find+changeStream on each requested collection once,
	// find on local.oplog.rs, nothing else and no inherited role.
	role := f.sent[2].cmd
	if lookup(role, "createRole") != "dbz_reader_cdc" {
		t.Fatalf("createRole = %v", lookup(role, "createRole"))
	}
	if inh, _ := lookup(role, "roles").(bson.A); len(inh) != 0 {
		t.Fatalf("the read role inherits %v, want no role", inh)
	}
	privs, _ := lookup(role, "privileges").(bson.A)
	if len(privs) != 3 {
		t.Fatalf("privileges = %v, want 3 (two collections, the oplog)", privs)
	}
	type grant struct{ db, coll, actions string }
	var got []grant
	for _, p := range privs {
		pd := p.(bson.D)
		res := lookup(pd, "resource").(bson.D)
		got = append(got, grant{fmt.Sprint(lookup(res, "db")), fmt.Sprint(lookup(res, "collection")), fmt.Sprint(lookup(pd, "actions"))})
	}
	wantGrants := []grant{
		{"orders", "customers", "[find changeStream]"},
		{"orders", "items", "[find changeStream]"},
		{"local", "oplog.rs", "[find]"},
	}
	for i := range wantGrants {
		if got[i] != wantGrants[i] {
			t.Fatalf("privilege %d = %+v, want %+v", i, got[i], wantGrants[i])
		}
	}
}

func TestCreateReadRoleExistingUserIsError(t *testing.T) {
	t.Run("usersInfo finds the user", func(t *testing.T) {
		f := &writeRunner{replies: map[string]bson.M{
			"admin.usersInfo": {"users": bson.A{bson.M{"user": "dbz", "db": "admin"}}, "ok": 1},
		}}
		store := &memStore{}
		_, err := testSetupWriter(f, store).CreateReadRole(unscoped(), "dbz", []string{"orders.customers"})
		if !errors.Is(err, errUserExists) {
			t.Fatalf("err = %v, want errUserExists", err)
		}
		if got := f.names(); len(got) != 1 || got[0] != "admin.usersInfo" {
			t.Fatalf("commands = %v, want only usersInfo", got)
		}
		if store.calls != 0 {
			t.Fatal("a password was stored for an existing user")
		}
	})

	t.Run("the custom role name is taken", func(t *testing.T) {
		f := &writeRunner{replies: map[string]bson.M{
			"admin.usersInfo": {"users": bson.A{}, "ok": 1},
			"admin.rolesInfo": {"roles": bson.A{bson.M{"role": "dbz_cdc"}}, "ok": 1},
		}}
		_, err := testSetupWriter(f, &memStore{}).CreateReadRole(unscoped(), "dbz", []string{"orders.customers"})
		if !errors.Is(err, errUserExists) {
			t.Fatalf("err = %v, want errUserExists", err)
		}
		for _, n := range f.names() {
			if strings.HasSuffix(n, ".createRole") || strings.HasSuffix(n, ".createUser") {
				t.Fatalf("sent %s for a taken role name", n)
			}
		}
	})

	t.Run("createUser races with another creator", func(t *testing.T) {
		f := &writeRunner{
			replies: map[string]bson.M{
				"admin.usersInfo": {"users": bson.A{}, "ok": 1},
				"admin.rolesInfo": {"roles": bson.A{}, "ok": 1},
			},
			fail: map[string]error{"admin.createUser": mongo.CommandError{Code: codeUserExists, Name: "Location51003", Message: `User "dbz@admin" already exists`}},
		}
		store := &memStore{}
		_, err := testSetupWriter(f, store).CreateReadRole(unscoped(), "dbz", []string{"orders.customers"})
		if !errors.Is(err, errUserExists) {
			t.Fatalf("err = %v, want errUserExists", err)
		}
		if strings.Contains(err.Error(), sentPassword(t, f)) {
			t.Fatal("error carries the password")
		}
		// The other creator's user is never dropped, and the losing call
		// never overwrites the stored secret.
		want := "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.createUser,admin.dropRole"
		if got := strings.Join(f.names(), ","); got != want {
			t.Fatalf("commands = %s, want %s (the just-created role dropped, never updateUser)", got, want)
		}
		if store.calls != 0 {
			t.Fatal("a losing createUser stored its password")
		}
	})

	t.Run("createUser error never carries the password", func(t *testing.T) {
		store := &memStore{}
		// The fake fails createUser with the whole command in its text,
		// password included, the way a careless driver might.
		echo := &echoRunner{writeRunner: &writeRunner{replies: map[string]bson.M{
			"admin.usersInfo": {"users": bson.A{}, "ok": 1},
			"admin.rolesInfo": {"roles": bson.A{}, "ok": 1},
		}}}
		w := &SetupWriter{run: echo, timeout: defaultTimeout, secrets: store}
		_, err := w.CreateReadRole(unscoped(), "dbz", []string{"orders.customers"})
		if err == nil {
			t.Fatal("want an error")
		}
		if strings.Contains(err.Error(), sentPassword(t, echo.writeRunner)) {
			t.Fatalf("error carries the password: %q", err)
		}
		// The user is dropped too (the create may have applied), and nothing
		// is stored.
		want := "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.createUser,admin.dropUser,admin.dropRole"
		if got := strings.Join(echo.names(), ","); got != want {
			t.Fatalf("commands = %s, want %s", got, want)
		}
		if store.calls != 0 {
			t.Fatal("a failed createUser stored its password")
		}
	})

	t.Run("no secret store refuses before any command", func(t *testing.T) {
		f := &writeRunner{}
		if _, err := testSetupWriter(f, nil).CreateReadRole(unscoped(), "dbz", []string{"orders.customers"}); err == nil {
			t.Fatal("want an error without a secret store")
		}
		if len(f.sent) != 0 {
			t.Fatalf("sent %v without a secret store", f.names())
		}
	})

	t.Run("a store that refuses gets the user and role dropped", func(t *testing.T) {
		f := &writeRunner{replies: map[string]bson.M{
			"admin.usersInfo": {"users": bson.A{}, "ok": 1},
			"admin.rolesInfo": {"roles": bson.A{}, "ok": 1},
		}}
		storeErr := errors.New("disk full")
		_, err := testSetupWriter(f, &memStore{err: storeErr}).CreateReadRole(unscoped(), "dbz", []string{"orders.customers"})
		if !errors.Is(err, storeErr) {
			t.Fatalf("err = %v, want the store error", err)
		}
		if strings.Contains(err.Error(), sentPassword(t, f)) {
			t.Fatalf("error carries the password: %q", err)
		}
		want := "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.createUser,admin.dropUser,admin.dropRole"
		if got := strings.Join(f.names(), ","); got != want {
			t.Fatalf("commands = %s, want %s", got, want)
		}
	})
}

// sentPassword returns the pwd of the createUser command f recorded, failing
// the test when none was sent.
func sentPassword(t *testing.T, f *writeRunner) string {
	t.Helper()
	for _, s := range f.sent {
		if s.cmd[0].Key == "createUser" {
			if pwd, _ := lookup(s.cmd, "pwd").(string); pwd != "" {
				return pwd
			}
		}
	}
	t.Fatal("no createUser with a pwd was sent")
	return ""
}

// echoRunner fails createUser with an error whose text is the whole command
// document, the password included.
type echoRunner struct{ *writeRunner }

func (e *echoRunner) runCommand(ctx context.Context, db string, cmd bson.D) (bson.M, error) {
	r, err := e.writeRunner.runCommand(ctx, db, cmd)
	if cmd[0].Key == "createUser" {
		return nil, fmt.Errorf("command failed: %v", cmd)
	}
	return r, err
}

func TestCreateReadRoleRollback(t *testing.T) {
	empty := map[string]bson.M{
		"admin.usersInfo": {"users": bson.A{}, "ok": 1},
		"admin.rolesInfo": {"roles": bson.A{}, "ok": 1},
	}
	unauthorized := mongo.CommandError{Code: 13, Name: "Unauthorized", Message: "not authorized on admin"}
	cases := []struct {
		name      string
		fail      map[string]error
		wantCmds  string
		wantIs    error // the error must wrap this, when set
		wantCode  int   // or carry this server error code, when set
		wantCount int   // errors joined in the message ("mongodb:" prefixes)
	}{
		{
			name:     "createRole refused drops the role, in case it applied",
			fail:     map[string]error{"admin.createRole": unauthorized},
			wantCmds: "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.dropRole",
			wantCode: 13,
		},
		{
			name:     "createRole race leaves the other creator's role",
			fail:     map[string]error{"admin.createRole": mongo.CommandError{Code: codeRoleExists, Name: "Location51002"}},
			wantCmds: "admin.usersInfo,admin.rolesInfo,admin.createRole",
			wantIs:   errUserExists,
		},
		{
			name:     "createRole with an unknown outcome drops the role",
			fail:     map[string]error{"admin.createRole": context.DeadlineExceeded},
			wantCmds: "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.dropRole",
			wantIs:   context.DeadlineExceeded,
		},
		{
			name:     "createUser refused drops the user and the role, in case it applied",
			fail:     map[string]error{"admin.createUser": unauthorized},
			wantCmds: "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.createUser,admin.dropUser,admin.dropRole",
			wantCode: 13,
		},
		{
			name:     "createUser with an unknown outcome drops the user and the role",
			fail:     map[string]error{"admin.createUser": mongo.ErrClientDisconnected},
			wantCmds: "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.createUser,admin.dropUser,admin.dropRole",
			wantIs:   mongo.ErrClientDisconnected,
		},
		{
			name: "a failed drop is joined with the create error",
			fail: map[string]error{
				"admin.createUser": unauthorized,
				"admin.dropRole":   errors.New("connection reset"),
			},
			wantCmds:  "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.createUser,admin.dropUser,admin.dropRole",
			wantCode:  13,
			wantCount: 2,
		},
		{
			name: "already-gone user and role are not drop errors",
			fail: map[string]error{
				"admin.createUser": context.DeadlineExceeded,
				"admin.dropUser":   mongo.CommandError{Code: codeUserNotFound, Name: "UserNotFound"},
				"admin.dropRole":   mongo.CommandError{Code: codeRoleNotFound, Name: "RoleNotFound"},
			},
			wantCmds:  "admin.usersInfo,admin.rolesInfo,admin.createRole,admin.createUser,admin.dropUser,admin.dropRole",
			wantIs:    context.DeadlineExceeded,
			wantCount: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &writeRunner{replies: empty, fail: c.fail}
			store := &memStore{}
			_, err := testSetupWriter(f, store).CreateReadRole(unscoped(), "dbz", []string{"orders.customers"})
			if err == nil {
				t.Fatal("want an error")
			}
			if c.wantIs != nil && !errors.Is(err, c.wantIs) {
				t.Fatalf("err = %v, want it to wrap %v", err, c.wantIs)
			}
			// mongo.CommandError holds a slice, so errors.Is cannot match it by
			// value; the code is what callers branch on.
			if c.wantCode != 0 && !hasCode(err, c.wantCode) {
				t.Fatalf("err = %v, want server code %d", err, c.wantCode)
			}
			if got := strings.Join(f.names(), ","); got != c.wantCmds {
				t.Fatalf("commands = %s, want %s", got, c.wantCmds)
			}
			if store.calls != 0 {
				t.Fatal("a failed create stored a password")
			}
			for _, s := range f.sent {
				if s.cmd[0].Key == "createUser" {
					if pwd := sentPassword(t, f); strings.Contains(err.Error(), pwd) {
						t.Fatalf("error carries the password: %q", err)
					}
				}
			}
			if c.wantCount > 0 {
				if n := strings.Count(err.Error(), "mongodb:"); n != c.wantCount {
					t.Fatalf("error %q joins %d errors, want %d", err, n, c.wantCount)
				}
			}
			for _, s := range f.sent {
				if s.cmd[0].Key == "dropUser" || s.cmd[0].Key == "dropRole" {
					if s.db != "admin" {
						t.Fatalf("%s sent to %s, want admin", s.cmd[0].Key, s.db)
					}
				}
			}
		})
	}

	t.Run("the drop runs under a fresh deadline after the call's ran out", func(t *testing.T) {
		f := &writeRunner{replies: empty, fail: map[string]error{"admin.createUser": context.DeadlineExceeded}}
		var dropCtxErr error
		run := &ctxRunner{writeRunner: f, onDrop: func(ctx context.Context) { dropCtxErr = ctx.Err() }}
		w := &SetupWriter{run: run, timeout: defaultTimeout, secrets: &memStore{}}
		ctx, cancel := context.WithCancel(unscoped())
		run.beforeCreateUser = cancel // the caller gives up mid-call
		if _, err := w.CreateReadRole(ctx, "dbz", []string{"orders.customers"}); err == nil {
			t.Fatal("want an error")
		}
		if dropCtxErr != nil {
			t.Fatalf("drop ran under an ended context: %v", dropCtxErr)
		}
	})
}

// ctxRunner wraps writeRunner to cancel the call before createUser and to
// observe the context the drops run under.
type ctxRunner struct {
	*writeRunner
	beforeCreateUser func()
	onDrop           func(context.Context)
}

func (c *ctxRunner) runCommand(ctx context.Context, db string, cmd bson.D) (bson.M, error) {
	switch cmd[0].Key {
	case "createUser":
		if c.beforeCreateUser != nil {
			c.beforeCreateUser()
		}
	case "dropUser", "dropRole":
		if c.onDrop != nil {
			c.onDrop(ctx)
		}
	}
	return c.writeRunner.runCommand(ctx, db, cmd)
}

func TestCreateReadRoleReplyShapeFailsClosed(t *testing.T) {
	for _, c := range []struct {
		name    string
		replies map[string]bson.M
	}{
		{"usersInfo without users", map[string]bson.M{"admin.usersInfo": {"ok": 1}}},
		{"rolesInfo without roles", map[string]bson.M{
			"admin.usersInfo": {"users": bson.A{}, "ok": 1},
			"admin.rolesInfo": {"ok": 1},
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &writeRunner{replies: c.replies}
			store := &memStore{}
			if _, err := testSetupWriter(f, store).CreateReadRole(unscoped(), "dbz", []string{"orders.customers"}); err == nil {
				t.Fatal("want an error for a reply without the array")
			}
			for _, n := range f.names() {
				if strings.HasSuffix(n, ".createRole") || strings.HasSuffix(n, ".createUser") {
					t.Fatalf("sent %s after an unreadable existence check", n)
				}
			}
			if store.calls != 0 {
				t.Fatal("stored a password after an unreadable existence check")
			}
		})
	}
}

func TestCreateCanaryAndInsert(t *testing.T) {
	t.Run("create, then insert exactly the nonce", func(t *testing.T) {
		f := &writeRunner{replies: map[string]bson.M{"orders.insert": {"n": int32(1), "ok": 1}}}
		w := testSetupWriter(f, nil)
		ctx := pipelineCtx("orders.piperoom_canary")
		if err := w.CreateCanaryTable(ctx, "orders.piperoom_canary"); err != nil {
			t.Fatal(err)
		}
		if err := w.InsertCanary(ctx, "orders.piperoom_canary", testNonce); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(f.names(), ","); got != "orders.create,orders.insert" {
			t.Fatalf("commands = %s", got)
		}
		if lookup(f.sent[0].cmd, "create") != "piperoom_canary" {
			t.Fatalf("create = %v", f.sent[0].cmd)
		}
		ins := f.sent[1].cmd
		if lookup(ins, "insert") != "piperoom_canary" {
			t.Fatalf("insert = %v", lookup(ins, "insert"))
		}
		docs, _ := lookup(ins, "documents").(bson.A)
		if len(docs) != 1 {
			t.Fatalf("inserted %d documents, want 1", len(docs))
		}
		doc := docs[0].(bson.D)
		if lookup(doc, "_id") != testNonce || lookup(doc, "nonce") != testNonce {
			t.Fatalf("document = %v, want _id and nonce = %s", doc, testNonce)
		}
		if _, ok := lookup(doc, "ts").(bson.DateTime); !ok {
			t.Fatalf("ts = %T, want bson.DateTime", lookup(doc, "ts"))
		}
		if len(doc) != 3 {
			t.Fatalf("document has %d fields, want _id, nonce, ts", len(doc))
		}
	})

	t.Run("an existing collection is success", func(t *testing.T) {
		f := &writeRunner{fail: map[string]error{"orders.create": mongo.CommandError{Code: codeNamespaceExists, Name: "NamespaceExists", Message: "Collection orders.piperoom_canary already exists."}}}
		if err := testSetupWriter(f, nil).CreateCanaryTable(unscoped(), "orders.piperoom_canary"); err != nil {
			t.Fatalf("existing canary: %v, want nil", err)
		}
	})

	t.Run("another create error is wrapped", func(t *testing.T) {
		boom := mongo.CommandError{Code: 13, Name: "Unauthorized", Message: "not authorized on orders"}
		f := &writeRunner{fail: map[string]error{"orders.create": boom}}
		err := testSetupWriter(f, nil).CreateCanaryTable(unscoped(), "orders.piperoom_canary")
		if !hasCode(err, 13) || !strings.Contains(err.Error(), "mongodb: create canary collection") {
			t.Fatalf("err = %v, want the wrapped Unauthorized error", err)
		}
	})

	t.Run("a write error in an ok reply is an error", func(t *testing.T) {
		f := &writeRunner{replies: map[string]bson.M{"orders.insert": {
			"n": int32(0), "ok": 1,
			"writeErrors": bson.A{bson.M{"index": int32(0), "code": int32(11000), "errmsg": "E11000 duplicate key"}},
		}}}
		err := testSetupWriter(f, nil).InsertCanary(unscoped(), "orders.piperoom_canary", testNonce)
		if err == nil || !strings.Contains(err.Error(), "11000") {
			t.Fatalf("err = %v, want the duplicate key write error", err)
		}
	})

	t.Run("a write concern error is an error", func(t *testing.T) {
		f := &writeRunner{replies: map[string]bson.M{"orders.insert": {
			"n": int32(1), "ok": 1,
			"writeConcernError": bson.M{"code": int32(64), "errmsg": "waiting for replication timed out"},
		}}}
		if err := testSetupWriter(f, nil).InsertCanary(unscoped(), "orders.piperoom_canary", testNonce); err == nil {
			t.Fatal("want the write concern error")
		}
	})

	t.Run("a bad nonce is refused before any command", func(t *testing.T) {
		for _, n := range []string{"", "xyz", strings.ToUpper(testNonce), testNonce + "00", `{"$gt":""}`} {
			f := &writeRunner{}
			if err := testSetupWriter(f, nil).InsertCanary(unscoped(), "orders.piperoom_canary", n); err == nil {
				t.Fatalf("nonce %q: want an error", n)
			}
			if len(f.sent) != 0 {
				t.Fatalf("nonce %q: sent %v", n, f.names())
			}
		}
	})
}

func TestNotSupportedMethods(t *testing.T) {
	f := &writeRunner{}
	w := testSetupWriter(f, &memStore{})
	if err := w.CreatePublication(unscoped(), "dbz_pub", []string{"orders.customers"}); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("CreatePublication: err = %v, want clients.ErrNotSupported", err)
	}
	pending, err := w.AlterSystem(unscoped(), "wal_level", "logical")
	if !errors.Is(err, clients.ErrNotSupported) || pending {
		t.Fatalf("AlterSystem: (%v, %v), want (false, clients.ErrNotSupported)", pending, err)
	}
	if len(f.sent) != 0 {
		t.Fatalf("sent %v for a not-supported method", f.names())
	}
}

func TestSetupWriterRefusesBadNames(t *testing.T) {
	bad := []string{"a.b$c", "$where", "1abc", "", " ", "a\x00b", "orders.$cmd", "orders.a b", "orders", "admin.canary", "local.oplog", "config.x", "Admin.canary", "LOCAL.oplog", "Config.x", "orders.a.b", strings.Repeat("a", 65) + ".c"}
	type call struct {
		name string
		do   func(w *SetupWriter, n string) error
	}
	calls := []call{
		{"CreateCanaryTable", func(w *SetupWriter, n string) error { return w.CreateCanaryTable(unscoped(), n) }},
		{"InsertCanary", func(w *SetupWriter, n string) error { return w.InsertCanary(unscoped(), n, testNonce) }},
		{"CreateReadRole tables", func(w *SetupWriter, n string) error {
			_, err := w.CreateReadRole(unscoped(), "dbz", []string{"orders.customers", n})
			return err
		}},
		{"CreateReadRole role", func(w *SetupWriter, n string) error {
			_, err := w.CreateReadRole(unscoped(), n, []string{"orders.customers"})
			return err
		}},
		{"CreatePublication", func(w *SetupWriter, n string) error { return w.CreatePublication(unscoped(), n, []string{n}) }},
		{"AlterSystem", func(w *SetupWriter, n string) error { _, err := w.AlterSystem(unscoped(), n, n); return err }},
	}
	for _, c := range calls {
		for _, n := range bad {
			if c.name == "CreateReadRole role" && n == "orders" {
				continue // a valid user name; only a namespace needs the dot
			}
			t.Run(fmt.Sprintf("%s/%q", c.name, n), func(t *testing.T) {
				f := &writeRunner{}
				w := testSetupWriter(f, &memStore{})
				if err := c.do(w, n); err == nil {
					t.Fatal("want an error for a bad name")
				}
				if len(f.sent) != 0 {
					t.Fatalf("sent %v for a bad name", f.names())
				}
			})
		}
	}
	t.Run("empty collection list", func(t *testing.T) {
		f := &writeRunner{}
		if _, err := testSetupWriter(f, &memStore{}).CreateReadRole(unscoped(), "dbz", nil); err == nil {
			t.Fatal("want an error for no collections")
		}
		if len(f.sent) != 0 {
			t.Fatalf("sent %v", f.names())
		}
	})
}

func TestSetupWriterScopeFailsClosed(t *testing.T) {
	type call struct {
		name string
		do   func(ctx context.Context, w *SetupWriter) error
	}
	calls := []call{
		{"CreateCanaryTable", func(ctx context.Context, w *SetupWriter) error {
			return w.CreateCanaryTable(ctx, "orders.piperoom_canary")
		}},
		{"InsertCanary", func(ctx context.Context, w *SetupWriter) error {
			return w.InsertCanary(ctx, "orders.piperoom_canary", testNonce)
		}},
		{"CreateReadRole", func(ctx context.Context, w *SetupWriter) error {
			_, err := w.CreateReadRole(ctx, "dbz", []string{"orders.piperoom_canary"})
			return err
		}},
		{"CreatePublication", func(ctx context.Context, w *SetupWriter) error {
			return w.CreatePublication(ctx, "pub", []string{"orders.piperoom_canary"})
		}},
		{"AlterSystem", func(ctx context.Context, w *SetupWriter) error { _, err := w.AlterSystem(ctx, "x", "y"); return err }},
	}
	noScope := map[string]context.Context{
		"no scope":         context.Background(),
		"pipeline id only": tools.WithPipelineID(context.Background(), "p1"),
	}
	for _, c := range calls {
		for label, ctx := range noScope {
			t.Run(c.name+"/"+label, func(t *testing.T) {
				f := &writeRunner{}
				if err := c.do(ctx, testSetupWriter(f, &memStore{})); !errors.Is(err, tools.ErrNoScope) {
					t.Fatalf("err = %v, want tools.ErrNoScope", err)
				}
				if len(f.sent) != 0 {
					t.Fatalf("sent %v without a scope", f.names())
				}
			})
		}
	}
	outside := pipelineCtx("orders.customers")
	for _, c := range calls[:3] {
		t.Run(c.name+"/outside the pipeline", func(t *testing.T) {
			f := &writeRunner{}
			if err := c.do(outside, testSetupWriter(f, &memStore{})); !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("err = %v, want tools.ErrOutOfScope", err)
			}
			if len(f.sent) != 0 {
				t.Fatalf("sent %v for an out-of-scope collection", f.names())
			}
		})
	}
}

func TestSetupWriterTimeout(t *testing.T) {
	t.Run("blocking command hits the deadline", func(t *testing.T) {
		calls := map[string]func(w *SetupWriter) error{
			"CreateCanaryTable": func(w *SetupWriter) error { return w.CreateCanaryTable(unscoped(), "orders.piperoom_canary") },
			"InsertCanary":      func(w *SetupWriter) error { return w.InsertCanary(unscoped(), "orders.piperoom_canary", testNonce) },
			"CreateReadRole": func(w *SetupWriter) error {
				_, err := w.CreateReadRole(unscoped(), "dbz", []string{"orders.customers"})
				return err
			},
		}
		for name, do := range calls {
			t.Run(name, func(t *testing.T) {
				w := &SetupWriter{run: &writeRunner{block: true}, timeout: 20 * time.Millisecond, secrets: &memStore{}}
				start := time.Now()
				err := do(w)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("err = %v, want a wrapped context.DeadlineExceeded", err)
				}
				if !strings.HasPrefix(err.Error(), "mongodb: ") {
					t.Fatalf("err = %q, want it wrapped with the package prefix", err)
				}
				if time.Since(start) > 5*time.Second {
					t.Fatal("deadline test took too long")
				}
			})
		}
	})

	t.Run("target down: error without the URI", func(t *testing.T) {
		// Port 1 on loopback refuses at once; no network leaves the machine.
		uri := "mongodb://setup:Wr1tePassw0rd@127.0.0.1:1/?directConnection=true"
		start := time.Now()
		w, err := NewSetupWriter(context.Background(), true, uri, WithSetupTimeout(300*time.Millisecond))
		if w != nil || err == nil {
			t.Fatal("want an error for a target that is down")
		}
		if strings.Contains(err.Error(), "Wr1tePassw0rd") || strings.Contains(err.Error(), uri) {
			t.Fatalf("error leaks the URI: %q", err)
		}
		if !strings.HasPrefix(err.Error(), "mongodb: setup writer: ") {
			t.Fatalf("err = %q, want it wrapped", err)
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("the setup timeout did not bound the connect")
		}
	})
}
