package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/owners"
)

// The Q8a breaks run against mysqlWorld (scenarios_mysql_fake_test.go). What
// no fake shows is MySQL and Debezium themselves: the live checks in
// dev/scenarios.md §25-§28 are not run here (no Docker).

var mysqlScenarios = []string{"binlog-purged", "history-topic-deleted", "missing-replication-grant", "server-id-clash"}

func TestMySQLScenariosRegistered(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "dev", "scenarios.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	for _, name := range mysqlScenarios {
		sc, ok := scenarios[name]
		if !ok || sc.breakIt == nil || sc.fix == nil {
			t.Fatalf("scenario %q not registered with a break and a fix", name)
		}
		if sc.owners != mysqlOwnersFile {
			t.Errorf("%s: owners %q, want %s", name, sc.owners, mysqlOwnersFile)
		}
		wantAlert := name == "server-id-clash"
		if (sc.alert != "") != wantAlert {
			t.Errorf("%s: alert %q, want alert %t", name, sc.alert, wantAlert)
		}
		if !wantAlert && !slices.Equal(sc.signals, []string{owners.SignalConnectorDown}) {
			t.Errorf("%s: signals %v, want [connector_down]", name, sc.signals)
		}
		if sc.alert != "" {
			var a struct {
				Title, Severity, Source, Description string
				PipelineID                           string `json:"pipeline_id"`
				DedupKey                             string `json:"dedup_key"`
			}
			if err := json.Unmarshal([]byte(sc.alert), &a); err != nil {
				t.Fatalf("alert of %q is not JSON: %v", name, err)
			}
			if a.Title == "" || a.Severity == "" || a.Source == "" || a.Description == "" || a.PipelineID != mysqlPipelineID || a.DedupKey != "devctl-"+name {
				t.Errorf("alert of %q = %+v", name, a)
			}
			// The alert says what was observed, never the cause.
			if strings.Contains(strings.ToLower(a.Description), "server.id") || strings.Contains(strings.ToLower(a.Description), "server id") {
				t.Errorf("alert of %q names the cause: %q", name, a.Description)
			}
		}
		if !strings.Contains(doc, "`"+name+"` —") {
			t.Errorf("dev/scenarios.md does not document %s", name)
		}
	}
}

// readYAMLMap parses dev/<name> into a generic map.
func readYAMLMap(t *testing.T, name string) map[string]any {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "dev", name))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := yaml.Unmarshal(b, &m); err != nil {
		t.Fatalf("dev/%s: %v", name, err)
	}
	return m
}

func mysqlPipeline(t *testing.T, m map[string]any) (map[string]any, int) {
	t.Helper()
	for i, p := range m["pipelines"].([]any) {
		if pm := p.(map[string]any); pm["id"] == mysqlPipelineID {
			return pm, i
		}
	}
	t.Fatalf("%s has no pipeline %q", mysqlOwnersFile, mysqlPipelineID)
	return nil, 0
}

// dev/owners.mysql.yaml names what the profile runs: the connector, its
// topic.prefix and its tables, with source_kind mysql, the mysql layer owner
// and no slot signal (Q8d refuses one on a MySQL pipeline).
//
// owners.Load knows source_kind only once Q8d merges. Until then the test
// loads the file with the two Q8d keys taken out, so everything else is
// checked against today's parser; once Pipeline has SourceKind, the file
// itself must load.
func TestOwnersMySQLFile(t *testing.T) {
	m := readYAMLMap(t, filepath.Base(mysqlOwnersFile))
	p, _ := mysqlPipeline(t, m)
	cfg, err := (&stack{root: mustRoot(t)}).mysqlConnectorConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if p["source_kind"] != "mysql" {
		t.Errorf("source_kind = %v, want mysql", p["source_kind"])
	}
	if p["topic_prefix"] != cfg["topic.prefix"] {
		t.Errorf("topic_prefix %v != connector topic.prefix %v", p["topic_prefix"], cfg["topic.prefix"])
	}
	if got := fmt.Sprint(p["connectors"]); got != "["+mysqlConnector+"]" {
		t.Errorf("connectors = %s, want [%s]", got, mysqlConnector)
	}
	var tables []string
	for _, x := range p["source_tables"].([]any) {
		tables = append(tables, x.(string))
	}
	if strings.Join(tables, ",") != cfg["table.include.list"] {
		t.Errorf("source_tables %v != table.include.list %v", tables, cfg["table.include.list"])
	}
	sigs := p["watch"].(map[string]any)["signals"].(map[string]any)
	if _, ok := sigs["connector_down"]; !ok || len(sigs) != 1 {
		t.Errorf("watch.signals = %v, want connector_down only", sigs)
	}
	if _, ok := p["owners"].(map[string]any)["mysql"]; !ok {
		t.Errorf("owners = %v, want a mysql layer owner", p["owners"])
	}

	path := filepath.Join(mustRoot(t), mysqlOwnersFile)
	if _, ok := reflect.TypeFor[owners.Pipeline]().FieldByName("SourceKind"); ok {
		if _, err := owners.Load(path); err != nil {
			t.Fatalf("owners.Load(%s): %v", mysqlOwnersFile, err)
		}
		return
	}
	t.Logf("owners.Pipeline has no SourceKind yet (Q8d): loading %s without source_kind and owners.mysql", mysqlOwnersFile)
	delete(p, "source_kind")
	delete(p, "owners")
	b, err := yaml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(tmp)
	if err != nil {
		t.Fatalf("owners.Load(%s minus the Q8d keys): %v", mysqlOwnersFile, err)
	}
	lp, ok := own.Pipeline(mysqlPipelineID)
	if !ok {
		t.Fatalf("pipeline %s missing after Load", mysqlPipelineID)
	}
	if w := lp.Watch(); !w.ConnectorDown.Enabled || w.ConnectorDown.Action != owners.ActionRoom {
		t.Errorf("connector_down = %+v, want enabled with a room", w.ConnectorDown)
	}
}

// dev/owners.mysql.yaml is the baseline plus the orders-mysql pipeline.
func TestOwnersMySQLMatchesBaseline(t *testing.T) {
	base, my := readYAMLMap(t, "owners.yaml"), readYAMLMap(t, filepath.Base(mysqlOwnersFile))
	_, i := mysqlPipeline(t, my)
	ps := my["pipelines"].([]any)
	my["pipelines"] = append(ps[:i:i], ps[i+1:]...)
	if fmt.Sprint(base) != fmt.Sprint(my) {
		t.Errorf("%s drifted from dev/owners.yaml beyond the MySQL pipeline:\nbase  %v\nmysql %v", mysqlOwnersFile, base, my)
	}
}

func mustRoot(t *testing.T) string {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// The profile is what design §10.1 asks: MySQL 8.4 pinned by digest, GTIDs
// on, a ROW binlog with full images, a server id that is not the connector's,
// loopback only, opt-in; and the connector matches it.
func TestMySQLProfile(t *testing.T) {
	m := readYAMLMap(t, "docker-compose.yml")
	svc, ok := m["services"].(map[string]any)["mysql"].(map[string]any)
	if !ok {
		t.Fatal("dev/docker-compose.yml has no mysql service")
	}
	if fmt.Sprint(svc["profiles"]) != "[mysql]" {
		t.Errorf("profiles = %v, want [mysql]", svc["profiles"])
	}
	img, _ := svc["image"].(string)
	if !strings.HasPrefix(img, "mysql:8.4.") || !strings.Contains(img, "@sha256:") {
		t.Errorf("image = %q, want mysql:8.4.x pinned by digest", img)
	}
	var cmd []string
	for _, x := range svc["command"].([]any) {
		cmd = append(cmd, x.(string))
	}
	for _, flag := range []string{"--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--binlog-format=ROW", "--binlog-row-image=FULL", "--server-id=223344"} {
		if !slices.Contains(cmd, flag) {
			t.Errorf("command %v lacks %s", cmd, flag)
		}
	}
	for _, p := range svc["ports"].([]any) {
		if !strings.HasPrefix(p.(string), "127.0.0.1:") {
			t.Errorf("port %v is not bound to host loopback", p)
		}
	}
	if _, ok := svc["depends_on"]; ok {
		t.Error("mysql depends on nothing: it starts alone with its profile")
	}
	cfg, err := (&stack{root: mustRoot(t)}).mysqlConnectorConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"connector.class":                     "io.debezium.connector.mysql.MySqlConnector",
		"database.hostname":                   "mysql",
		"database.port":                       "3306",
		"database.server.id":                  "5401",
		"topic.prefix":                        "shop",
		"schema.history.internal.kafka.topic": mysqlHistory,
	}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("orders-mysql %s = %v, want %s", k, cfg[k], v)
		}
	}
	if _, ok := cfg["snapshot.mode"]; ok {
		t.Error("orders-mysql sets snapshot.mode: the baseline is Debezium's default (initial), which the binlog-purged break relies on")
	}
	if mysqlOrdersTopic != cfg["topic.prefix"].(string)+".shop.orders" {
		t.Errorf("mysqlOrdersTopic %s does not follow topic.prefix", mysqlOrdersTopic)
	}
}

func TestBinlogFileRE(t *testing.T) {
	for _, tc := range []struct {
		in string
		ok bool
	}{
		{"binlog.000042", true},
		{"mysql-bin.1000000", true},
		{"binlog.42", false},
		{"binlog.000042'; DROP TABLE x; --", false},
		{"", false},
		{"../binlog.000001", false},
	} {
		if got := binlogFileRE.MatchString(tc.in); got != tc.ok {
			t.Errorf("binlogFileRE(%q) = %t, want %t", tc.in, got, tc.ok)
		}
	}
}

// ------------------------------------------------------------ break and fix

// assertMySQLStreaming checks the end state every fix promises.
func assertMySQLStreaming(t *testing.T, w *mysqlWorld) {
	t.Helper()
	w.with(func() {
		c := w.conn[mysqlConnector]
		if c == nil || !c.running() {
			t.Fatalf("%s not RUNNING after fix: %+v", mysqlConnector, c)
		}
		if _, ok := w.conn[mysqlShadow]; ok {
			t.Errorf("%s still registered", mysqlShadow)
		}
		if c.str("snapshot.mode") != "" {
			t.Errorf("snapshot.mode %q left in the config", c.str("snapshot.mode"))
		}
		if !w.replGrant {
			t.Error("REPLICATION SLAVE not granted")
		}
	})
	if !w.topicExists(mysqlHistory) {
		t.Errorf("%s missing after fix", mysqlHistory)
	}
}

func TestBinlogPurgedBreakAndFix(t *testing.T) {
	w := newMySQLWorld(t).withRunningConnector()
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakBinlogPurged(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		c := w.conn[mysqlConnector]
		if c.task != "FAILED" || !strings.Contains(c.trace, "no longer available") {
			t.Fatalf("after break: task %s trace %q", c.task, c.trace)
		}
		if w.first != w.last || w.last != 5 {
			t.Errorf("binlogs %d..%d, want only the newest (5) after two flushes", w.first, w.last)
		}
	})
	if !strings.Contains(logs.String(), "gtid_purged=") {
		t.Errorf("break log lacks gtid_purged:\n%s", logs)
	}
	if err := breakBinlogPurged(ctx, s); err == nil || !strings.Contains(err.Error(), "devctl fix binlog-purged") {
		t.Errorf("second break = %v, want a refusal naming the fix", err)
	}
	if err := fixBinlogPurged(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	assertMySQLStreaming(t, w)
	w.with(func() {
		if got := w.conn[mysqlConnector].historyBuilt; got != 2 {
			t.Errorf("history written %d times, want 2 (the seed snapshot and the re-snapshot)", got)
		}
	})
	logs.Reset()
	if err := fixBinlogPurged(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	if !strings.Contains(logs.String(), "skipped") {
		t.Errorf("second fix did not skip:\n%s", logs)
	}
}

func TestHistoryTopicDeletedBreakAndFix(t *testing.T) {
	w := newMySQLWorld(t).withRunningConnector()
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakHistoryTopicDeleted(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		c := w.conn[mysqlConnector]
		if c.task != "FAILED" || !strings.Contains(c.trace, "history topic is missing") {
			t.Fatalf("after break: task %s trace %q", c.task, c.trace)
		}
	})
	if w.topicExists(mysqlHistory) {
		t.Fatal("history topic still there after the break")
	}
	if err := breakHistoryTopicDeleted(ctx, s); err == nil {
		t.Error("second break did not refuse")
	}
	var posBefore mysqlPos
	w.with(func() { posBefore = *w.conn[mysqlConnector].offset })
	if err := fixHistoryTopicDeleted(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	assertMySQLStreaming(t, w)
	w.with(func() {
		c := w.conn[mysqlConnector]
		// A recovery keeps the binlog position: no re-snapshot.
		if c.offset == nil || c.offset.gtid < posBefore.gtid {
			t.Errorf("offset after recovery = %+v, want at or past %+v", c.offset, posBefore)
		}
	})
	if !strings.Contains(logs.String(), "snapshot.mode=recovery") {
		t.Errorf("fix log does not name the recovery:\n%s", logs)
	}
	logs.Reset()
	if err := fixHistoryTopicDeleted(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	if !strings.Contains(logs.String(), "skipped") {
		t.Errorf("second fix did not skip:\n%s", logs)
	}
}

func TestMissingReplicationGrantBreakAndFix(t *testing.T) {
	w := newMySQLWorld(t).withRunningConnector()
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakMissingReplicationGrant(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		c := w.conn[mysqlConnector]
		if w.replGrant || c.task != "FAILED" || !strings.Contains(c.trace, "REPLICATION SLAVE") {
			t.Fatalf("after break: grant %t task %s trace %q", w.replGrant, c.task, c.trace)
		}
		if c.restarts != 1 {
			t.Errorf("task restarts = %d, want 1 (a global privilege reaches a session only on reconnect)", c.restarts)
		}
	})
	if err := breakMissingReplicationGrant(ctx, s); err == nil {
		t.Error("second break did not refuse")
	}
	if err := fixMissingReplicationGrant(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	assertMySQLStreaming(t, w)
	logs.Reset()
	if err := fixMissingReplicationGrant(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	if !strings.Contains(logs.String(), "skipped") {
		t.Errorf("second fix did not skip:\n%s", logs)
	}
}

// A fix that finds the grant back but the task still FAILED restarts it.
func TestMissingReplicationGrantFixAfterManualGrant(t *testing.T) {
	w := newMySQLWorld(t).withRunningConnector()
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakMissingReplicationGrant(ctx, s); err != nil {
		t.Fatal(err)
	}
	w.with(func() { w.replGrant = true })
	if err := fixMissingReplicationGrant(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	assertMySQLStreaming(t, w)
	w.with(func() {
		if slices.Contains(w.stmts, sqlMySQLGrantRepl) {
			t.Error("fix granted again although the grant was there")
		}
	})
}

func TestServerIDClashBreakAndFix(t *testing.T) {
	w := newMySQLWorld(t).withRunningConnector()
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakServerIDClash(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		sh := w.conn[mysqlShadow]
		if sh == nil || !sh.running() {
			t.Fatalf("shadow not running: %+v", sh)
		}
		main := w.conn[mysqlConnector]
		if sh.str("database.server.id") != main.str("database.server.id") {
			t.Errorf("server ids %s / %s, want the same", sh.str("database.server.id"), main.str("database.server.id"))
		}
		if sh.str("topic.prefix") == main.str("topic.prefix") || sh.str("schema.history.internal.kafka.topic") == main.str("schema.history.internal.kafka.topic") {
			t.Error("the shadow shares a topic with orders-mysql: only the server id may clash")
		}
		if w.zombies == 0 {
			t.Error("no zombie dump thread")
		}
	})
	if !strings.Contains(logs.String(), "zombie_warning_logged=true") {
		t.Errorf("break log lacks the zombie evidence:\n%s", logs)
	}
	if err := breakServerIDClash(ctx, s); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("second break = %v, want a refusal", err)
	}
	if err := fixServerIDClash(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	assertMySQLStreaming(t, w)
	if w.topicExists(mysqlShadowHistory) {
		t.Errorf("%s left behind", mysqlShadowHistory)
	}
	if err := fixServerIDClash(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
}

// The zombie warning is evidence, not a condition: without it the break
// still succeeds and says so.
func TestServerIDClashBreakWithoutZombieWarning(t *testing.T) {
	w := newMySQLWorld(t).withRunningConnector()
	s, logs := w.stack()
	// Keep the counter where it is.
	s.execInFn = func(ctx context.Context, container string, argv ...string) (string, int, error) {
		if len(argv) == 5 && argv[4] == sqlMySQLZombies {
			return "0", 0, nil
		}
		return w.execIn(ctx, container, argv...)
	}
	// The break waits up to 60 s for the warning; a short deadline ends that
	// wait (everything before it is instant on the fake).
	ctx, cancel := context.WithTimeout(w.ctx(), 3*time.Second)
	defer cancel()
	if err := breakServerIDClash(ctx, s); err != nil {
		t.Fatalf("break: %v", err)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "zombie_warning_logged=false") {
		t.Errorf("break did not warn:\n%s", logs)
	}
}

// Without the profile every break and fix stops at once with the command
// that starts it, and nothing reaches Connect.
func TestMySQLScenariosNeedProfile(t *testing.T) {
	for _, name := range mysqlScenarios {
		for _, which := range []string{"break", "fix"} {
			t.Run(name+"/"+which, func(t *testing.T) {
				w := newMySQLWorld(t)
				w.up = false
				s, _ := w.stack()
				f := scenarios[name].breakIt
				if which == "fix" {
					f = scenarios[name].fix
				}
				err := f(w.ctx(), s)
				if err == nil || !strings.Contains(err.Error(), mysqlUpHint) {
					t.Fatalf("%s %s = %v, want the profile hint", which, name, err)
				}
				if len(w.conn) != 0 {
					t.Error("a connector was registered without the profile")
				}
			})
		}
	}
}

// A break on a stack where orders-mysql is not registered yet registers it
// from dev/connectors and waits for its snapshot first.
func TestMySQLBreakRegistersConnector(t *testing.T) {
	w := newMySQLWorld(t)
	s, logs := w.stack()
	if err := breakMissingReplicationGrant(w.ctx(), s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	if !strings.Contains(logs.String(), "registered "+mysqlConnector) {
		t.Errorf("no registration logged:\n%s", logs)
	}
	w.with(func() {
		if c := w.conn[mysqlConnector]; c == nil || c.task != "FAILED" {
			t.Errorf("connector = %+v, want registered and FAILED", c)
		}
	})
}

// A break refuses a connector that is already unhealthy.
func TestMySQLBreakRefusesFailedConnector(t *testing.T) {
	for _, name := range mysqlScenarios {
		t.Run(name, func(t *testing.T) {
			w := newMySQLWorld(t).withRunningConnector()
			w.with(func() { w.conn[mysqlConnector].task = "FAILED" })
			s, _ := w.stack()
			err := scenarios[name].breakIt(w.ctx(), s)
			if err == nil || !strings.Contains(err.Error(), "devctl fix "+name) {
				t.Fatalf("break = %v, want a refusal naming the fix", err)
			}
		})
	}
}

// Each fix brings a connector left STOPPED by a break cut short back.
func TestMySQLFixesResumeStoppedConnector(t *testing.T) {
	for _, name := range mysqlScenarios {
		t.Run(name, func(t *testing.T) {
			w := newMySQLWorld(t).withRunningConnector()
			w.with(func() { w.conn[mysqlConnector].state, w.conn[mysqlConnector].task = "STOPPED", "" })
			s, logs := w.stack()
			if err := scenarios[name].fix(w.ctx(), s); err != nil {
				t.Fatalf("fix: %v\n%s", err, logs)
			}
			assertMySQLStreaming(t, w)
		})
	}
}
