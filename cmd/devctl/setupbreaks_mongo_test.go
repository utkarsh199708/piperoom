package main

// Unit tests for the MongoDB setup breaks (setupbreaks_mongo.go) and the
// compose profile "mongo": a fake mongod plays the profile (its replica set
// mode and the capture user's roles), a fake `docker exec` and a fake
// `docker compose` record exactly what the breaks run, so every pair runs
// without Docker. The real run is TestIntegrationMongoSetupBreaks.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/clients/mongodb"
)

// mongoWorld is the fake profile.
type mongoWorld struct {
	replSet   bool // started with --replSet rs0
	oplogRead bool // the capture user holds read on local
	down      bool // the client cannot connect
	// ignoreRoleEdits makes the fake mongosh succeed without changing the
	// roles: a break that does not take.
	ignoreRoleEdits bool
	mongoshExit     int

	execCalls    [][]string // container + argv of every docker exec
	composeCalls [][]string // the args of every docker compose call
	overrides    []string   // the override file's content per compose call ("" = none)
}

func healthyMongoWorld() *mongoWorld { return &mongoWorld{replSet: true, oplogRead: true} }

// fakeMongo is the client the checks read: it answers from the world the way
// internal/clients/mongodb does (the same reason strings).
type fakeMongo struct{ w *mongoWorld }

func (f fakeMongo) ChangeStreamSupport(context.Context) (mongodb.ChangeStreamSupport, error) {
	if !f.w.replSet {
		return mongodb.ChangeStreamSupport{Supported: false, Reason: "standalone mongod has no change streams"}, nil
	}
	return mongodb.ChangeStreamSupport{Supported: true}, nil
}

func (f fakeMongo) RoleInfo(_ context.Context, role string, tables []string) (mongodb.RoleInfo, error) {
	ri := mongodb.RoleInfo{Role: role, Exists: role == mongoCaptureRole, Selectable: map[string]bool{}}
	ri.CanLogin = ri.Exists
	ri.Replication = ri.Exists && f.w.oplogRead
	for _, t := range tables {
		ri.Selectable[t] = ri.Exists
	}
	return ri, nil
}

func (f fakeMongo) Close(context.Context) error { return nil }

func (w *mongoWorld) connect(context.Context) (mongoSource, error) {
	if w.down {
		return nil, errors.New("server selection error: connection refused")
	}
	return fakeMongo{w}, nil
}

func (w *mongoWorld) exec(_ context.Context, container string, argv ...string) (string, int, error) {
	w.execCalls = append(w.execCalls, append([]string{container}, argv...))
	if container != mongoContainer || len(argv) == 0 || argv[0] != "mongosh" {
		return "unexpected command", 127, nil
	}
	if w.mongoshExit != 0 {
		return "MongoServerError: not authorized", w.mongoshExit, nil
	}
	if w.ignoreRoleEdits {
		return "", 0, nil
	}
	switch argv[len(argv)-1] {
	case scriptRevokeOplogRead:
		w.oplogRead = false
	case scriptGrantOplogRead:
		w.oplogRead = true
	default:
		return "unexpected script", 1, nil
	}
	return "", 0, nil
}

// composeOverride plays `docker compose [-f override] ...`: the service's
// command decides whether mongod runs with --replSet.
func (w *mongoWorld) composeOverride(_ context.Context, override string, args ...string) error {
	w.composeCalls = append(w.composeCalls, args)
	content := ""
	if override != "" {
		b, err := os.ReadFile(override)
		if err != nil {
			return err
		}
		content = string(b)
	}
	w.overrides = append(w.overrides, content)
	w.replSet = true
	if content != "" {
		var doc struct {
			Services map[string]struct {
				Command []string `yaml:"command"`
			} `yaml:"services"`
		}
		if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
			return err
		}
		if cmd := doc.Services["mongo"].Command; cmd != nil {
			w.replSet = slices.Contains(cmd, "--replSet")
		}
	}
	return nil
}

func testMongoStack(t *testing.T, w *mongoWorld) *stack {
	t.Helper()
	return &stack{
		root:              t.TempDir(),
		log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		poll:              time.Millisecond,
		mongoFn:           w.connect,
		execInFn:          w.exec,
		composeOverrideFn: w.composeOverride,
	}
}

func TestMongoSetupScenariosAreRegistered(t *testing.T) {
	var names []string
	for n := range mongoSetupScenarios {
		names = append(names, n)
	}
	slices.Sort(names)
	if want := []string{"no-oplog-role", "standalone-mongod"}; !slices.Equal(names, want) {
		t.Fatalf("mongo scenarios = %v, want %v", names, want)
	}
	// Names and checks are unique across every set, so a name finds one
	// scenario and a check names one break.
	seenName, seenCheck := map[string]bool{}, map[string]string{}
	for _, set := range setupScenarioSets {
		for name, sc := range set {
			if seenName[name] {
				t.Errorf("scenario %s is in two sets", name)
			}
			seenName[name] = true
			if sc.name != name || sc.check == "" || sc.breakIt == nil || sc.fix == nil || sc.verify == nil {
				t.Errorf("%s: incomplete scenario", name)
			}
			if other, dup := seenCheck[sc.check]; dup {
				t.Errorf("%s and %s share check %q", other, name, sc.check)
			}
			seenCheck[sc.check] = name
		}
	}
	for _, name := range []string{"standalone-mongod", "no-oplog-role", "flat-plugin-dir"} {
		if _, ok := findSetupScenario(name); !ok {
			t.Errorf("findSetupScenario(%q) found nothing", name)
		}
		if !slices.Contains(allSetupScenarioNames(), name) {
			t.Errorf("allSetupScenarioNames lacks %q", name)
		}
	}
	if got := mongoSetupScenarios["standalone-mongod"].check; got != checkMongoChangeStreams {
		t.Errorf("standalone-mongod check = %q", got)
	}
	if got := mongoSetupScenarios["no-oplog-role"].check; got != checkMongoOplogRead {
		t.Errorf("no-oplog-role check = %q", got)
	}
	// The usage text names both, like the Postgres breaks.
	for _, name := range []string{"standalone-mongod", "no-oplog-role"} {
		if !strings.Contains(usage, name) {
			t.Errorf("usage text does not name %s", name)
		}
	}
}

// TestMongoSetupBreakFixPair: the named check passes, fails after the break
// with the client's own reason, passes after the fix; a second break and a
// second fix are no-ops.
func TestMongoSetupBreakFixPair(t *testing.T) {
	wantDetail := map[string]string{
		"standalone-mongod": "standalone mongod has no change streams",
		"no-oplog-role":     "cannot read local.oplog.rs",
	}
	for name, sc := range mongoSetupScenarios {
		t.Run(name, func(t *testing.T) {
			w := healthyMongoWorld()
			s := testMongoStack(t, w)
			ctx := context.Background()
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check %s on the healthy profile: ok=%v %s err=%v", sc.check, ok, detail, err)
			}
			for i := 0; i < 2; i++ {
				if err := setupBreakCmd(ctx, s, name); err != nil {
					t.Fatalf("setup-break %s (#%d): %v", name, i+1, err)
				}
			}
			ok, detail, err := sc.verify(ctx, s)
			if err != nil || ok {
				t.Fatalf("check %s after the break: ok=%v err=%v", sc.check, ok, err)
			}
			if !strings.Contains(detail, wantDetail[name]) {
				t.Errorf("detail %q does not name %q", detail, wantDetail[name])
			}
			calls := len(w.execCalls) + len(w.composeCalls)
			for i := 0; i < 2; i++ {
				if err := setupFixCmd(ctx, s, name); err != nil {
					t.Fatalf("setup-fix %s (#%d): %v", name, i+1, err)
				}
			}
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check %s after the fix: ok=%v %s err=%v", sc.check, ok, detail, err)
			}
			if got := len(w.execCalls) + len(w.composeCalls); got != calls+1 {
				t.Errorf("two fixes ran %d commands, want 1 (the second is a no-op)", got-calls)
			}
		})
	}
}

// TestMongoStandaloneCommands pins what the standalone break and fix run:
// the override recreates only the mongo service with the base command minus
// --replSet rs0; the fix recreates it from the base file and waits for the
// healthcheck.
func TestMongoStandaloneCommands(t *testing.T) {
	w := healthyMongoWorld()
	s := testMongoStack(t, w)
	ctx := context.Background()
	if err := setupBreakCmd(ctx, s, "standalone-mongod"); err != nil {
		t.Fatal(err)
	}
	if err := setupFixCmd(ctx, s, "standalone-mongod"); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"--profile", "mongo", "up", "-d", "--no-deps", "mongo"},
		{"--profile", "mongo", "up", "-d", "--no-deps", "--wait", "mongo"},
	}
	if len(w.composeCalls) != len(want) {
		t.Fatalf("compose calls = %v, want %v", w.composeCalls, want)
	}
	for i := range want {
		if !slices.Equal(w.composeCalls[i], want[i]) {
			t.Errorf("compose call %d = %v, want %v", i, w.composeCalls[i], want[i])
		}
	}
	if w.overrides[1] != "" {
		t.Errorf("the fix used an override: %q", w.overrides[1])
	}
	if len(w.execCalls) != 0 {
		t.Errorf("the standalone pair ran docker exec: %v", w.execCalls)
	}

	var doc struct {
		Services map[string]struct {
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(w.overrides[0]), &doc); err != nil {
		t.Fatalf("override does not parse: %v", err)
	}
	if len(doc.Services) != 1 {
		t.Fatalf("override touches %d services, want only mongo", len(doc.Services))
	}
	_, svcs := readQ3Compose(t)
	base := svcs["mongo"].Command
	i := slices.Index(base, "--replSet")
	if i < 0 || i+1 >= len(base) || base[i+1] != "rs0" {
		t.Fatalf("base mongo command %v has no --replSet rs0", base)
	}
	wantCmd := slices.Concat(base[:i], base[i+2:])
	if got := doc.Services["mongo"].Command; !slices.Equal(got, wantCmd) {
		t.Errorf("override command = %v, want the base command without --replSet rs0: %v", got, wantCmd)
	}
}

// TestMongoOplogRoleCommands pins the role edits: one fixed mongosh script
// each, in the mongo container, as the profile's root user.
func TestMongoOplogRoleCommands(t *testing.T) {
	w := healthyMongoWorld()
	s := testMongoStack(t, w)
	ctx := context.Background()
	if err := setupBreakCmd(ctx, s, "no-oplog-role"); err != nil {
		t.Fatal(err)
	}
	if err := setupFixCmd(ctx, s, "no-oplog-role"); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		append([]string{mongoContainer}, mongoAdminEval(scriptRevokeOplogRead)...),
		append([]string{mongoContainer}, mongoAdminEval(scriptGrantOplogRead)...),
	}
	if len(w.execCalls) != len(want) {
		t.Fatalf("exec calls = %v", w.execCalls)
	}
	for i := range want {
		if !slices.Equal(w.execCalls[i], want[i]) {
			t.Errorf("exec %d = %q, want %q", i, w.execCalls[i], want[i])
		}
	}
	if len(w.composeCalls) != 0 {
		t.Errorf("the role pair ran compose: %v", w.composeCalls)
	}
	for _, script := range []string{scriptRevokeOplogRead, scriptGrantOplogRead} {
		if !strings.Contains(script, "'piperoom_cdc'") || !strings.Contains(script, "{role: 'read', db: 'local'}") {
			t.Errorf("script %q does not edit read on local for piperoom_cdc", script)
		}
	}
}

func TestMongoSetupBreakErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("profile down", func(t *testing.T) {
		w := healthyMongoWorld()
		w.down = true
		s := testMongoStack(t, w)
		for name := range mongoSetupScenarios {
			err := setupBreakCmd(ctx, s, name)
			if err == nil || !strings.Contains(err.Error(), "--profile mongo up") {
				t.Errorf("%s with the profile down = %v, want the start hint", name, err)
			}
		}
		if len(w.execCalls)+len(w.composeCalls) != 0 {
			t.Errorf("a break ran commands although its check could not: %v %v", w.execCalls, w.composeCalls)
		}
	})

	t.Run("mongosh refused", func(t *testing.T) {
		w := healthyMongoWorld()
		w.mongoshExit = 1
		s := testMongoStack(t, w)
		err := setupBreakCmd(ctx, s, "no-oplog-role")
		if err == nil || !strings.Contains(err.Error(), "exit 1") || !strings.Contains(err.Error(), "not authorized") {
			t.Fatalf("break with mongosh failing = %v", err)
		}
	})

	t.Run("break does not take", func(t *testing.T) {
		w := healthyMongoWorld()
		w.ignoreRoleEdits = true
		s := testMongoStack(t, w)
		err := setupBreakCmd(ctx, s, "no-oplog-role")
		if err == nil || !strings.Contains(err.Error(), "did not make check "+checkMongoOplogRead+" fail") {
			t.Fatalf("break that changed nothing = %v", err)
		}
	})

	t.Run("fix does not take", func(t *testing.T) {
		w := healthyMongoWorld()
		w.oplogRead = false
		w.ignoreRoleEdits = true
		s := testMongoStack(t, w)
		err := setupFixCmd(ctx, s, "no-oplog-role")
		if err == nil || !strings.Contains(err.Error(), "did not make check "+checkMongoOplogRead+" pass") {
			t.Fatalf("fix that changed nothing = %v", err)
		}
	})

	t.Run("unknown capture user", func(t *testing.T) {
		ok, detail, err := verifyMongoOplogRead(ctx, &stack{
			log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
			mongoFn: func(context.Context) (mongoSource, error) { return noUser{}, nil },
		})
		if err != nil || ok || !strings.Contains(detail, "does not exist") {
			t.Fatalf("missing user: ok=%v %q err=%v", ok, detail, err)
		}
	})
}

// noUser is a profile whose capture user was never created.
type noUser struct{ fakeMongo }

func (noUser) RoleInfo(_ context.Context, role string, _ []string) (mongodb.RoleInfo, error) {
	return mongodb.RoleInfo{Role: role}, nil
}

// TestComposeMongoProfile: the profile is opt-in, pinned, loopback-only,
// healthy only as a writable primary, and its credentials are the ones the
// breaks use.
func TestComposeMongoProfile(t *testing.T) {
	root, svcs := readQ3Compose(t)
	m, ok := svcs["mongo"]
	if !ok {
		t.Fatal("dev/docker-compose.yml has no mongo service")
	}
	if !slices.Equal(m.Profiles, []string{"mongo"}) {
		t.Errorf("mongo profiles = %v, want [mongo]: `devctl up` must not start it", m.Profiles)
	}
	if !pinned(m.Image) || !strings.HasPrefix(m.Image, "mongo:7.0.") || !strings.Contains(m.Image, "@sha256:") {
		t.Errorf("mongo image %q: want the official mongo 7.0 image, exact tag and digest", m.Image)
	}
	if !slices.Equal(m.Ports, []string{"127.0.0.1:27017:27017"}) {
		t.Errorf("mongo ports = %v, want host loopback 27017 only", m.Ports)
	}
	if !slices.Equal(m.Command, []string{"bash", "/piperoom-mongo/start.sh", "--replSet", "rs0"}) {
		t.Errorf("mongo command = %v", m.Command)
	}
	hc := strings.Join(m.Healthcheck.Test, " ")
	for _, want := range []string{"mongosh", "rs.initiate", "_id: 'rs0'", "host: 'mongo:27017'", "isWritablePrimary"} {
		if !strings.Contains(hc, want) {
			t.Errorf("mongo healthcheck lacks %q: %s", want, hc)
		}
	}
	// The breaks log in as these (mongoAdminEval, mongoDevURI).
	argv := strings.Join(mongoAdminEval("x"), " ")
	if !strings.Contains(argv, "-u "+m.Environment["MONGO_INITDB_ROOT_USERNAME"]+" ") || !strings.Contains(argv, "-p "+m.Environment["MONGO_INITDB_ROOT_PASSWORD"]+" ") {
		t.Errorf("mongoAdminEval credentials %q differ from the service's root user", argv)
	}
	if !strings.Contains(hc, m.Environment["MONGO_INITDB_ROOT_PASSWORD"]) {
		t.Errorf("the healthcheck does not use the root user")
	}
	for _, v := range m.Volumes {
		src, _, _ := strings.Cut(v, ":")
		if _, err := os.Stat(filepath.Join(root, "dev", src)); err != nil {
			t.Errorf("mongo mounts %s: %v", src, err)
		}
	}
	// No service the default `up` starts may need the profile.
	for name, s := range svcs {
		if _, dep := s.DependsOn["mongo"]; dep && !slices.Contains(s.Profiles, "mongo") {
			t.Errorf("%s depends on mongo outside the profile", name)
		}
	}

	start, err := os.ReadFile(filepath.Join(root, "dev", "mongo", "start.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"chmod 400", "chown mongodb:mongodb", "--keyFile", `"$@"`, "exec docker-entrypoint.sh mongod"} {
		if !strings.Contains(string(start), want) {
			t.Errorf("dev/mongo/start.sh lacks %q", want)
		}
	}
	initJS, err := os.ReadFile(filepath.Join(root, "dev", "mongo", "init", "01-capture-user.js"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`user: "piperoom_cdc"`, `pwd: "piperoom-dev"`, `{ role: "read", db: "local" }`, `"inventory"`} {
		if !strings.Contains(string(initJS), want) {
			t.Errorf("01-capture-user.js lacks %s", want)
		}
	}
	if !strings.Contains(mongoDevURI, "piperoom_cdc:piperoom-dev@") || !strings.Contains(mongoDevURI, "directConnection=true") {
		t.Errorf("mongoDevURI %q is not the capture user's direct connection", mongoDevURI)
	}
}
