package main

// Unit tests for the setup breaks (setupbreaks.go): a fake Connect, fake psql
// and a fake `docker exec` play the dev stack, so every break/fix pair and
// every named check runs without Docker. The integration test
// (setupbreaks_integration_test.go) runs the same pairs on the real stack.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
)

// setupWorld is the fake dev stack: the Connect plugin directories, the
// source's settings and the connector's grants, config and task state.
type setupWorld struct {
	rootJars       []string
	dirJars        []string
	dirExists      bool
	walLevel       string
	connector      map[string]any
	connectorState string
	taskState      string
	trace          string
	slotExists     bool
	slotActive     bool
	plugins        []string
	blockedHost    bool
	composeCall    []string
	sqlCalls       []string
	otherSlots     int
	// stashed is the plugin directory parked in the break's stash.
	stashed bool
	// httpCalls are the Connect REST calls, "METHOD /path", in order.
	httpCalls []string
	// execUsers are the users each docker exec ran as, "user argv0".
	execUsers []string
}

// healthySetupWorld is the stack `devctl reset` leaves: one connector
// directory with its jars, wal_level=logical, a replicating superuser, the
// dev connector config, a RUNNING task and the plugin in the worker's list.
func healthySetupWorld() *setupWorld {
	return &setupWorld{
		dirExists: true,
		dirJars: []string{
			"debezium-connector-postgres-3.2.0.Final.jar",
			"debezium-core-3.2.0.Final.jar",
			"debezium-api-3.2.0.Final.jar",
			"postgresql-42.7.5.jar",
		},
		walLevel: "logical",
		connector: map[string]any{
			"connector.class":    postgresConnector,
			"database.hostname":  "postgres",
			"database.port":      "5432",
			"database.user":      "piperoom",
			"database.password":  "piperoom-dev",
			"database.dbname":    "orders",
			"topic.prefix":       "pg",
			"plugin.name":        "pgoutput",
			"slot.name":          slotName,
			"table.include.list": "public.orders",
			"tasks.max":          "1",
		},
		connectorState: "RUNNING",
		taskState:      "RUNNING",
		slotExists:     true,
		slotActive:     true,
		plugins:        []string{postgresConnector, "org.apache.kafka.connect.file.FileStreamSinkConnector"},
	}
}

// applyConfig is what Connect does with a PUT /config: store the values and
// restart the connector, whose task then fails or recovers.
func (w *setupWorld) applyConfig(cfg map[string]any) {
	w.connector = cfg
	w.connectorState = "RUNNING"
	w.trace = ""
	switch {
	case cfg["database.user"] == setupGrantRole:
		w.taskState = "FAILED"
		w.trace = "org.postgresql.util.PSQLException: ERROR: must be superuser or replication role to start walsender"
	default:
		w.taskState = "RUNNING"
	}
}

func (w *setupWorld) handler() http.Handler {
	mux := http.NewServeMux()
	writeJSON := func(rw http.ResponseWriter, v any) {
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(v)
	}
	mux.HandleFunc("/connectors", func(rw http.ResponseWriter, _ *http.Request) {
		writeJSON(rw, []string{connectorName})
	})
	mux.HandleFunc("/connectors/"+connectorName+"/config", func(rw http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(rw, w.connector)
		case http.MethodPut:
			var cfg map[string]any
			if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
				http.Error(rw, err.Error(), http.StatusBadRequest)
				return
			}
			w.applyConfig(cfg)
			writeJSON(rw, cfg)
		default:
			http.Error(rw, "method", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/connectors/"+connectorName+"/status", func(rw http.ResponseWriter, _ *http.Request) {
		tasks := []map[string]any{{"id": 0, "state": w.taskState, "trace": w.trace}}
		if w.connectorState == "STOPPED" {
			tasks = nil
		}
		writeJSON(rw, map[string]any{
			"connector": map[string]any{"state": w.connectorState},
			"tasks":     tasks,
		})
	})
	mux.HandleFunc("/connectors/"+connectorName+"/stop", func(rw http.ResponseWriter, _ *http.Request) {
		w.connectorState, w.taskState = "STOPPED", "STOPPED"
		w.slotActive = false
		rw.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/connectors/"+connectorName+"/resume", func(rw http.ResponseWriter, _ *http.Request) {
		// Debezium streams from the slot the fix created; a resume onto a
		// missing slot leaves nothing active.
		w.connectorState, w.taskState = "RUNNING", "RUNNING"
		w.slotActive = w.slotExists
		rw.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/connectors/"+connectorName+"/offsets", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/connectors/"+connectorName+"/tasks/0/restart", func(rw http.ResponseWriter, _ *http.Request) {
		if w.blockedHost {
			w.taskState, w.trace = "FAILED", "java.net.SocketTimeoutException: connect timed out"
		} else if w.connector["database.user"] == "piperoom" {
			w.taskState, w.trace = "RUNNING", ""
		}
		rw.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/connector-plugins", func(rw http.ResponseWriter, _ *http.Request) {
		out := make([]map[string]string, 0, len(w.plugins))
		for _, c := range w.plugins {
			out = append(out, map[string]string{"class": c})
		}
		writeJSON(rw, out)
	})
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.httpCalls = append(w.httpCalls, r.Method+" "+r.URL.Path)
		mux.ServeHTTP(rw, r)
	})
}

// sql plays the source Postgres: wal_level, the role's rights and the slot.
func (w *setupWorld) sql(_ context.Context, _ string, queries ...string) (string, error) {
	for _, q := range queries {
		w.sqlCalls = append(w.sqlCalls, q)
		switch {
		case q == "SHOW wal_level":
			if w.walLevel == "" {
				return "", fmt.Errorf("connection refused")
			}
			return w.walLevel, nil
		case strings.HasPrefix(q, "ALTER SYSTEM SET wal_level"):
			w.walLevel = "replica"
			return "ALTER SYSTEM", nil
		case strings.HasPrefix(q, "ALTER SYSTEM RESET wal_level"):
			w.walLevel = "logical"
			return "ALTER SYSTEM", nil
		case strings.HasPrefix(q, "SELECT rolcanlogin, rolreplication"):
			role := between(q, "rolname = '", "'")
			login, repl, ok := w.role(role)
			if !ok {
				return "", nil
			}
			return pgBool(login) + "|" + pgBool(repl), nil
		case strings.HasPrefix(q, "SELECT has_table_privilege"):
			role := between(q, "has_table_privilege('", "'")
			table := between(q, "', '", "', 'SELECT')")
			_, _, ok := w.role(role)
			if !ok {
				return "f", nil
			}
			return pgBool(w.roleCanSelect(role, table)), nil
		case strings.HasPrefix(q, "SELECT active::text || '|'"):
			// sqlSlotRow: active|retained, "" when the slot is gone.
			if !w.slotExists {
				return "", nil
			}
			return boolText(w.slotActive) + "|1024", nil
		case q == sqlOtherLogicalSlots:
			return fmt.Sprint(w.otherSlots), nil
		case q == sqlDropSlot:
			w.slotExists, w.slotActive = false, false
			return "", nil
		case q == sqlCreateSlot:
			if w.slotExists {
				return "", fmt.Errorf("slot already exists")
			}
			w.slotExists, w.slotActive = true, false
			return slotName, nil
		case strings.HasPrefix(q, "SELECT active FROM pg_replication_slots"):
			if !w.slotExists || !w.slotActive {
				return "f", nil
			}
			return "t", nil
		}
	}
	return "", nil
}

// role reports the fake grants of a role: the baseline user may do
// everything; the break's role may log in and nothing else.
func (w *setupWorld) role(name string) (login, replication, exists bool) {
	if user, _ := w.connector["database.user"].(string); user != "" && name != user {
		return false, false, false
	}
	if name == setupGrantRole {
		return true, false, true
	}
	return true, true, true
}

func (w *setupWorld) roleCanSelect(name, table string) bool {
	if name == setupGrantRole {
		return false
	}
	return table == "public.orders"
}

// exec plays `docker exec` inside the Connect container.
func (w *setupWorld) exec(_ context.Context, _ string, argv ...string) (string, int, error) {
	switch argv[0] {
	case "ls":
		path := argv[len(argv)-1]
		switch path {
		case setupPluginRoot:
			return strings.Join(w.rootJars, "\n"), 0, nil
		case setupConnectorDir:
			if !w.dirExists {
				return "ls: cannot access '" + setupConnectorDir + "': No such file or directory", 2, nil
			}
			return strings.Join(w.dirJars, "\n"), 0, nil
		}
		return "ls: cannot access " + path, 2, nil
	case "bash":
		// scriptProbeTCP: the worker's TCP path to the source.
		if w.blockedHost {
			return "", 124, nil
		}
		return "", 0, nil
	case "getent":
		host := argv[len(argv)-1]
		if w.blockedHost {
			return "192.0.2.1\t" + host, 0, nil
		}
		return "10.0.0.2\t" + host, 0, nil
	case "sh":
		// argv is ["sh", "-c", script, "_", ...args].
		return "", w.runScript(argv[2]), nil
	}
	return "", 1, fmt.Errorf("unexpected argv %v", argv)
}

// runScript applies the container steps of setupbreaks.go to the world. The
// scripts are exact constants of the package, so a test cannot drift from
// what the break runs.
func (w *setupWorld) runScript(script string) int {
	switch script {
	case scriptFlatPlugin:
		if !w.dirExists {
			return 1
		}
		w.rootJars = append(w.rootJars, w.dirJars...)
		w.dirJars, w.dirExists = nil, false
	case scriptUnflatPlugin:
		w.dirJars = append(w.dirJars, w.rootJars...)
		w.rootJars, w.dirExists = nil, true
	case scriptStashConverters:
		var kept []string
		for _, j := range w.dirJars {
			if strings.HasPrefix(j, "debezium-core-") || strings.HasPrefix(j, "debezium-api-") {
				continue
			}
			kept = append(kept, j)
		}
		if len(kept) == len(w.dirJars) {
			return 1 // nothing to move: mv fails
		}
		w.dirJars = kept
	case scriptRestoreConverters:
		w.dirJars = append(w.dirJars, "debezium-core-3.2.0.Final.jar", "debezium-api-3.2.0.Final.jar")
	case scriptDisablePluginDir:
		w.dirExists, w.stashed = false, true
	case scriptRestorePluginDir:
		if !w.stashed {
			return 1
		}
		w.dirExists, w.stashed = true, false
	case scriptRestoreIfStashed:
		if w.stashed {
			w.dirExists, w.stashed = true, false
		}
	case scriptBlockSource:
		w.blockedHost = true
	default:
		return 1
	}
	return 0
}

// compose records the restart the break/fix asked for. A restarted Connect
// rescans its plugin path: the connector class is in the list exactly when
// the connector directory is there at that moment.
func (w *setupWorld) compose(_ context.Context, _ map[string]string, args ...string) error {
	w.composeCall = append(w.composeCall, strings.Join(args, " "))
	if strings.Join(args, " ") == "restart connect" {
		w.rescanPlugins()
		// Docker regenerates /etc/hosts, and the JVM starts with an empty DNS
		// cache; Connect brings the connector back from its config topic.
		w.blockedHost = false
		if w.connectorState != "STOPPED" && w.connector["database.user"] == "piperoom" && w.walLevel == "logical" {
			w.taskState, w.trace = "RUNNING", ""
		}
	}
	// A restarted Postgres drops the connector's replication connection: the
	// task fails when the server cannot serve logical replication, and comes
	// back when it can.
	if strings.Join(args, " ") == "restart postgres" {
		if w.walLevel != "logical" {
			w.taskState = "FAILED"
			w.trace = "org.postgresql.util.PSQLException: ERROR: logical decoding requires wal_level >= logical"
		} else if w.connector["database.user"] == "piperoom" && w.connector["database.hostname"] == "postgres" {
			w.taskState = "RUNNING"
			w.trace = ""
		}
	}
	return nil
}

// rescanPlugins is one worker scan of the plugin path.
func (w *setupWorld) rescanPlugins() {
	out := []string{"org.apache.kafka.connect.file.FileStreamSinkConnector"}
	if w.dirExists {
		out = append([]string{postgresConnector}, out...)
	}
	w.plugins = out
}

// composeOverride is `docker compose -f dev/docker-compose.yml [-f override]
// up -d --no-deps postgres`: the override recreates Postgres with
// wal_level=replica, the base file recreates it with logical. The recreate
// drops the connector's replication connection.
func (w *setupWorld) composeOverride(_ context.Context, override string, args ...string) error {
	w.composeCall = append(w.composeCall, "override="+override+" "+strings.Join(args, " "))
	// The server starts with the wal_level its command line names: the
	// override's when it sets one, else the base file's logical.
	w.walLevel = "logical"
	if override != "" {
		body, err := os.ReadFile(override)
		if err != nil {
			return err
		}
		if m := walLevelArgRE.FindStringSubmatch(string(body)); m != nil {
			w.walLevel = m[1]
		}
	}
	switch {
	case w.walLevel != "logical":
		w.taskState, w.trace = "FAILED", "ERROR: logical decoding requires wal_level >= logical"
	case w.connector["database.user"] == "piperoom" && !w.blockedHost:
		w.taskState, w.trace = "RUNNING", ""
	}
	return nil
}

// walLevelArgRE reads the wal_level a Compose command line sets (the "-c"
// argument, not a comment that names it).
var walLevelArgRE = regexp.MustCompile(`"-c", "wal_level=([a-z]+)"`)

// testSetupStack wires a stack to the fake world.
func testSetupStack(t *testing.T, w *setupWorld) *stack {
	t.Helper()
	srv := httptest.NewServer(w.handler())
	t.Cleanup(srv.Close)
	root := t.TempDir()
	dir := filepath.Join(root, "dev", "connectors")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(w.connector)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, connectorName+".json"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	return &stack{
		root:              root,
		cfg:               config.Config{ConnectURL: srv.URL},
		http:              srv.Client(),
		log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		poll:              time.Millisecond,
		connectStart:      2 * time.Second,
		sqlFn:             w.sql,
		composeFn:         w.compose,
		composeOverrideFn: w.composeOverride,
		execAsFn: func(ctx context.Context, user, container string, argv ...string) (string, int, error) {
			w.execUsers = append(w.execUsers, user+" "+argv[0])
			return w.exec(ctx, container, argv...)
		},
	}
}

func TestSetupScenariosAreRegistered(t *testing.T) {
	names := setupScenarioNamesFor("")
	want := []string{
		"agent-stopped", "cdc-not-enabled-table",
		"db-unreachable-from-connect", "flat-plugin-dir", "missing-converter-jars",
		"missing-grant", "plugin-after-scan", "wal-level-replica",
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("scenario names = %v, want %v", names, want)
	}
	checks := map[string]string{}
	for name, sc := range setupScenarios {
		if sc.name != name {
			t.Errorf("%s: .name = %q", name, sc.name)
		}
		if sc.check == "" || sc.breakIt == nil || sc.fix == nil || sc.verify == nil {
			t.Errorf("%s: incomplete scenario: %+v", name, sc)
		}
		if other, dup := checks[sc.check]; dup {
			t.Errorf("%s and %s share check %q", other, name, sc.check)
		}
		checks[sc.check] = name
	}
}

// TestSetupBreakFixPair runs every break and its fix against the fake stack:
// the named check passes before, fails after the break, and passes after the
// fix.
func TestSetupBreakFixPair(t *testing.T) {
	for _, name := range setupScenarioNamesFor("") {
		if isMSSQLSetupScenario(name) {
			continue // the fake SQL Server: setupbreaks_mssql_test.go
		}
		t.Run(name, func(t *testing.T) {
			w := healthySetupWorld()
			s := testSetupStack(t, w)
			ctx := context.Background()
			sc := setupScenarios[name]

			ok, detail, err := sc.verify(ctx, s)
			if err != nil {
				t.Fatalf("check %s on the healthy stack: %v", sc.check, err)
			}
			if !ok {
				t.Fatalf("check %s fails on the healthy stack: %s", sc.check, detail)
			}

			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-break %s: %v", name, err)
			}
			ok, detail, err = sc.verify(ctx, s)
			if err != nil {
				t.Fatalf("check %s after the break: %v", sc.check, err)
			}
			if ok {
				t.Fatalf("check %s still passes after the break (%s)", sc.check, detail)
			}

			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-fix %s: %v", name, err)
			}
			ok, detail, err = sc.verify(ctx, s)
			if err != nil {
				t.Fatalf("check %s after the fix: %v", sc.check, err)
			}
			if !ok {
				t.Fatalf("check %s still fails after the fix: %s", sc.check, detail)
			}
		})
	}
}

// TestSetupBreakFixIdempotent: a second break and a second fix are no-ops;
// neither errors and the check keeps its state.
func TestSetupBreakFixIdempotent(t *testing.T) {
	for _, name := range setupScenarioNamesFor("") {
		if isMSSQLSetupScenario(name) {
			continue // the fake SQL Server: setupbreaks_mssql_test.go
		}
		t.Run(name, func(t *testing.T) {
			w := healthySetupWorld()
			s := testSetupStack(t, w)
			ctx := context.Background()
			sc := setupScenarios[name]

			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatal(err)
			}
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("second break: %v", err)
			}
			if ok, _, err := sc.verify(ctx, s); err != nil || ok {
				t.Fatalf("check after two breaks: ok=%v err=%v", ok, err)
			}
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatal(err)
			}
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("second fix: %v", err)
			}
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check after two fixes: ok=%v %s err=%v", ok, detail, err)
			}
		})
	}
}

// TestSetupBreakUnknownScenario names the known scenarios in the error.
func TestSetupBreakUnknownScenario(t *testing.T) {
	s := testSetupStack(t, healthySetupWorld())
	err := setupBreakCmd(context.Background(), s, "no-such-break")
	if err == nil || !strings.Contains(err.Error(), "flat-plugin-dir") {
		t.Fatalf("err = %v, want the scenario list", err)
	}
}

// TestSetupCheckErrorPaths: a stack that cannot be read fails the check with
// an error, never with a silent pass (a broken psql or Connect is not a
// healthy prerequisite).
func TestSetupCheckErrorPaths(t *testing.T) {
	t.Run("psql down", func(t *testing.T) {
		s := testSetupStack(t, healthySetupWorld())
		s.sqlFn = func(context.Context, string, ...string) (string, error) {
			return "", fmt.Errorf("connection refused")
		}
		if ok, _, err := verifyWALLevel(context.Background(), s); err == nil || ok {
			t.Fatalf("wal_level check with psql down: ok=%v err=%v", ok, err)
		}
	})
	t.Run("connect down", func(t *testing.T) {
		s := testSetupStack(t, healthySetupWorld())
		s.cfg.ConnectURL = "http://127.0.0.1:1"
		if ok, _, err := verifySourceRoleGrants(context.Background(), s); err == nil || ok {
			t.Fatalf("grants check with Connect down: ok=%v err=%v", ok, err)
		}
	})
	t.Run("no connector plugin jars", func(t *testing.T) {
		w := healthySetupWorld()
		w.dirJars = []string{"postgresql-42.7.5.jar"}
		s := testSetupStack(t, w)
		ok, detail, err := verifyConverterJars(context.Background(), s)
		if err != nil || ok || !strings.Contains(detail, "no debezium-connector-postgres jar") {
			t.Fatalf("converter check without the connector jar: ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
}

// TestSetupCheckDetails pins the words the checks report, so the scenario
// docs and the eval labels agree with the code.
func TestSetupCheckDetails(t *testing.T) {
	cases := []struct{ scenario, want string }{
		{"flat-plugin-dir", "jar(s) directly in /kafka/connect"},
		{"missing-converter-jars", "missing debezium-core-3.2.0.Final.jar"},
		{"wal-level-replica", "wal_level=replica"},
		{"missing-grant", "missing REPLICATION, SELECT on public.orders"},
		{"db-unreachable-from-connect", "cannot open a TCP connection to postgres:5432"},
		{"plugin-after-scan", "scanned the plugin path before the install"},
	}
	for _, tc := range cases {
		t.Run(tc.scenario, func(t *testing.T) {
			w := healthySetupWorld()
			s := testSetupStack(t, w)
			ctx := context.Background()
			sc := setupScenarios[tc.scenario]
			if err := sc.breakIt(ctx, s); err != nil {
				t.Fatalf("break: %v", err)
			}
			ok, detail, err := sc.verify(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("check passes after the break")
			}
			if !strings.Contains(detail, tc.want) {
				t.Errorf("detail %q does not contain %q", detail, tc.want)
			}
		})
	}
}

// pgBool is Postgres's unaligned boolean ("t"/"f"), the form psql -At prints.
func pgBool(v bool) string {
	if v {
		return "t"
	}
	return "f"
}

// boolText is `boolean::text` ("true"/"false"), the form sqlSlotRow reads.
func boolText(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

// between returns the text after the first from marker and before the next to
// marker; "" when a marker is missing.
func between(s, from, to string) string {
	i := strings.Index(s, from)
	if i < 0 {
		return ""
	}
	rest := s[i+len(from):]
	j := strings.Index(rest, to)
	if j < 0 {
		return ""
	}
	return rest[:j]
}
