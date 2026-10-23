package main

// Setup breaks (U1-15, docs/design/setup-agent.md section 8): the dev stack
// made to fail one named prerequisite check a setup run reports, so the setup
// agent's diagnosis and repair can be exercised on a real environment without
// a model. Each scenario has
//
//   - a named check, the check the setup run fails at (jar_list's layout and
//     missing-converter findings, the source settings, the source role's
//     grants, Connect's reach of the source, and Connect's plugin list);
//   - a break that makes exactly that check fail;
//   - a fix that restores the stack, so `devctl setup-break` and
//     `devctl setup-fix` are a reversible pair like every other scenario.
//
// The breaks work on the dev stack (dev/docker-compose.yml) because that is
// the environment the setup run is pointed at: the Connect plugin directory
// lives inside the Connect image, the source is the stack's Postgres, and the
// worker is the one the run talks to. They change only what a setup run reads
// before it renders anything: the plugin layout, the source's settings and
// grants, the worker's network path to the source, and the worker's plugin
// scan. No rows are deleted; the one break that has to drop the connector's
// replication slot (wal-level-replica) makes the fix re-snapshot, and
// `devctl reset` rebuilds the stack if a fix is ever interrupted.
//
// The integration test (setupbreaks_integration_test.go) applies each break
// and its fix on the dev stack and asserts the named check fails and passes.
// The recorded agent's repair is replayed as the scenario's fix — the model
// recording itself is U1-19/U1-20.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Where the dev Connect worker keeps its plugins. The image bakes the
// directory (dev/connect/Dockerfile); the setup driver's own stack mounts
// connect-plugins/ from the project there, which is why the checks are the
// same reads a setup run does (jar_list).
const (
	connectContainer  = "piperoom-dev-connect-1"
	setupPluginRoot   = "/kafka/connect"
	setupConnectorDir = setupPluginRoot + "/debezium-connector-postgres"
	setupBreakStash   = "/tmp/devctl-setup-break"
	postgresConnector = "io.debezium.connector.postgresql.PostgresConnector"
	setupGrantRole    = "devctl_setup_grant"
)

// The named checks. The strings are what the break and fix log lines and
// dev/scenarios.md use; a test asserts they are unique.
const (
	checkPluginLayout       = "plugin-layout"
	checkPluginConverters   = "plugin-converter-jars"
	checkWALLevel           = "postgres-wal-level"
	checkSourceRoleGrants   = "source-role-grants"
	checkConnectSourceReach = "connect-source-reachable"
	checkConnectPluginScan  = "connect-plugin-scan"
)

// setupScenario is one reversible setup break. verify reports whether the
// stack passes the scenario's named check; at most one of ok and detail is
// meaningful when err is nil.
type setupScenario struct {
	name    string
	check   string
	breakIt func(ctx context.Context, s *stack) error
	fix     func(ctx context.Context, s *stack) error
	verify  func(ctx context.Context, s *stack) (ok bool, detail string, err error)
}

// setupScenarios are the six breaks of U1-15. The names match the evals
// labels of U1-19 (setup/flat-plugin-dir, ...).
var setupScenarios = map[string]setupScenario{
	"flat-plugin-dir":             {name: "flat-plugin-dir", check: checkPluginLayout, breakIt: breakFlatPluginDir, fix: fixFlatPluginDir, verify: verifyPluginLayout},
	"missing-converter-jars":      {name: "missing-converter-jars", check: checkPluginConverters, breakIt: breakMissingConverterJars, fix: fixMissingConverterJars, verify: verifyConverterJars},
	"wal-level-replica":           {name: "wal-level-replica", check: checkWALLevel, breakIt: breakWALLevelReplica, fix: fixWALLevelReplica, verify: verifyWALLevel},
	"missing-grant":               {name: "missing-grant", check: checkSourceRoleGrants, breakIt: breakSetupMissingGrant, fix: fixSetupMissingGrant, verify: verifySourceRoleGrants},
	"db-unreachable-from-connect": {name: "db-unreachable-from-connect", check: checkConnectSourceReach, breakIt: breakDBUnreachable, fix: fixDBUnreachable, verify: verifyConnectSourceReachable},
	"plugin-after-scan":           {name: "plugin-after-scan", check: checkConnectPluginScan, breakIt: breakPluginAfterScan, fix: fixPluginAfterScan, verify: verifyConnectPluginScan},
}

// setupScenarioNames returns the names sorted, for usage text and errors.
func setupScenarioNames() []string {
	names := make([]string, 0, len(setupScenarios))
	for n := range setupScenarios {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// setupBreakCmd is `devctl setup-break <scenario>`: make the named check
// fail. A stack that already fails the check is left alone, so a repeated
// break is a no-op rather than an error.
func setupBreakCmd(ctx context.Context, s *stack, name string) error {
	sc, ok := setupScenarios[name]
	if !ok {
		return fmt.Errorf("unknown setup-break scenario %q (%s)", name, strings.Join(setupScenarioNames(), ", "))
	}
	okNow, detail, err := sc.verify(ctx, s)
	if err != nil {
		return fmt.Errorf("devctl: check %s: %w", sc.check, err)
	}
	if !okNow {
		s.log.Info("skipped: the stack already fails check "+sc.check, "check", sc.check, "detail", detail)
		return nil
	}
	if err := sc.breakIt(ctx, s); err != nil {
		return fmt.Errorf("devctl: break %s: %w", name, err)
	}
	okNow, detail, err = sc.verify(ctx, s)
	if err != nil {
		return fmt.Errorf("devctl: check %s after the break: %w", sc.check, err)
	}
	if okNow {
		return fmt.Errorf("devctl: break %s did not make check %s fail", name, sc.check)
	}
	s.log.Info("broken: check "+sc.check+" fails", "scenario", name, "check", sc.check, "detail", detail)
	return nil
}

// setupFixCmd is `devctl setup-fix <scenario>`: restore the stack and prove
// the named check passes again. Idempotent when the stack is already healthy.
func setupFixCmd(ctx context.Context, s *stack, name string) error {
	sc, ok := setupScenarios[name]
	if !ok {
		return fmt.Errorf("unknown setup-break scenario %q (%s)", name, strings.Join(setupScenarioNames(), ", "))
	}
	// A check that cannot run (the component it reads is down, often because
	// of the break) does not block the fix: the fix restarts that component,
	// and the check after it decides.
	okNow, _, err := sc.verify(ctx, s)
	if err != nil {
		s.log.Info("check "+sc.check+" cannot run; applying the fix", "check", sc.check, "err", err.Error())
	} else if okNow {
		s.log.Info("skipped: the stack already passes check "+sc.check, "check", sc.check)
		return nil
	}
	if err := sc.fix(ctx, s); err != nil {
		return fmt.Errorf("devctl: fix %s: %w", name, err)
	}
	okNow, detail, err := sc.verify(ctx, s)
	if err != nil {
		return fmt.Errorf("devctl: check %s after the fix: %w", sc.check, err)
	}
	if !okNow {
		return fmt.Errorf("devctl: fix %s did not make check %s pass: %s", name, sc.check, detail)
	}
	s.log.Info("fixed: check "+sc.check+" passes", "scenario", name, "check", sc.check)
	return nil
}

// ---------------------------------------------------------------- plugin layout

// The container-side steps are fixed scripts, never text from a request: mv
// of a known directory under /kafka/connect and a stash under /tmp.
const (
	// scriptFlatPlugin moves every connector jar into the plugins root, the
	// layout jar_list reports as flat ("each connector needs its own
	// directory"). The empty connector directory is removed.
	scriptFlatPlugin = `set -e; d=` + setupConnectorDir + `; mkdir -p ` + setupBreakStash + `; mv "$d"/*.jar ` + setupPluginRoot + `/; rmdir "$d" 2>/dev/null || true`
	// scriptUnflatPlugin puts the jars back into the connector directory.
	scriptUnflatPlugin = `set -e; d=` + setupConnectorDir + `; mkdir -p "$d"; mv ` + setupPluginRoot + `/*.jar "$d"/`
	// scriptStashConverters moves debezium-core and debezium-api out of the
	// connector directory: the connector jar is there, its converter jars are
	// not (jar_list's missing_converters).
	scriptStashConverters = `set -e; d=` + setupConnectorDir + `; b=` + setupBreakStash + `/converters; mkdir -p "$b"; mv "$d"/debezium-core-*.jar "$d"/debezium-api-*.jar "$b"/`
	// scriptRestoreConverters puts them back.
	scriptRestoreConverters = `set -e; b=` + setupBreakStash + `/converters; mv "$b"/*.jar ` + setupConnectorDir + `/`
	// scriptDisablePluginDir takes the connector directory out of the plugin
	// path, so a worker restart scans without it.
	scriptDisablePluginDir = `set -e; mkdir -p ` + setupBreakStash + `; rm -rf ` + setupBreakStash + `/plugin; mv ` + setupConnectorDir + ` ` + setupBreakStash + `/plugin`
	// scriptRestorePluginDir puts the directory back, without restarting the
	// worker: the plugin is on disk but was not in the scan (plugin-after-scan).
	scriptRestorePluginDir = `set -e; rm -rf ` + setupConnectorDir + `; mv ` + setupBreakStash + `/plugin ` + setupConnectorDir
	// scriptRestoreIfStashed puts the directory back only when the break left
	// it in the stash, so the fix is safe to run after a half-applied break.
	scriptRestoreIfStashed = `if [ -d ` + setupBreakStash + `/plugin ]; then rm -rf ` + setupConnectorDir + `; mv ` + setupBreakStash + `/plugin ` + setupConnectorDir + `; fi`
	// connectStartTimeout bounds a Connect worker start. The Debezium image
	// ships a dozen connector plugins and the worker loads each one, so a start
	// takes from seconds to minutes depending on the machine; the scan must
	// finish before the plugin list means anything.
	connectStartTimeout = 6 * time.Minute
)

// connectStartTimeout is the stack's bound on a Connect worker start: the
// connectStart field when set (tests), else the constant.
func (s *stack) connectStartTimeout() time.Duration {
	if s.connectStart > 0 {
		return s.connectStart
	}
	return connectStartTimeout
}

// pluginDirView is the plugin path as jar_list reads it: jars directly in the
// plugins root (flat), and the connector directory's jars.
type pluginDirView struct {
	RootJars  []string
	DirJars   []string
	DirExists bool
}

// pluginDirs lists the two directories inside the Connect container.
func (s *stack) pluginDirs(ctx context.Context) (pluginDirView, error) {
	var v pluginDirView
	out, code, err := s.execIn(ctx, connectContainer, "ls", "-1", setupPluginRoot)
	if err != nil {
		return v, fmt.Errorf("devctl: list %s in %s: %w", setupPluginRoot, connectContainer, err)
	}
	if code != 0 {
		return v, fmt.Errorf("devctl: list %s in %s: exit %d: %s", setupPluginRoot, connectContainer, code, strings.TrimSpace(out))
	}
	v.RootJars = jarNames(out)
	out, code, err = s.execIn(ctx, connectContainer, "ls", "-1", setupConnectorDir)
	if err != nil {
		return v, fmt.Errorf("devctl: list %s in %s: %w", setupConnectorDir, connectContainer, err)
	}
	if code == 0 {
		v.DirExists = true
		v.DirJars = jarNames(out)
	}
	return v, nil
}

// jarNames keeps the .jar entries of an ls listing.
func jarNames(listing string) []string {
	var out []string
	for _, line := range strings.Split(listing, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasSuffix(strings.ToLower(line), ".jar") {
			out = append(out, line)
		}
	}
	return out
}

// connectorJar is the directory's debezium-connector-postgres-<version>.jar.
func connectorJar(jars []string) (name, version string, ok bool) {
	const prefix = "debezium-connector-postgres-"
	for _, j := range jars {
		if strings.HasPrefix(j, prefix) && strings.HasSuffix(j, ".jar") {
			return j, strings.TrimSuffix(strings.TrimPrefix(j, prefix), ".jar"), true
		}
	}
	return "", "", false
}

// verifyPluginLayout is the plugin-layout check: no jars directly in the
// plugins root, one connector directory with the connector jar.
func verifyPluginLayout(ctx context.Context, s *stack) (bool, string, error) {
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return false, "", err
	}
	if len(v.RootJars) > 0 {
		return false, fmt.Sprintf("%d jar(s) directly in %s (%s); each connector needs its own directory", len(v.RootJars), setupPluginRoot, strings.Join(firstN(v.RootJars, 3), ", ")), nil
	}
	if !v.DirExists {
		return false, setupConnectorDir + " is missing from the plugin path", nil
	}
	if _, _, ok := connectorJar(v.DirJars); !ok {
		return false, setupConnectorDir + " holds no debezium-connector-postgres jar", nil
	}
	return true, "one connector directory under " + setupPluginRoot, nil
}

func breakFlatPluginDir(ctx context.Context, s *stack) error {
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return err
	}
	if !v.DirExists {
		return fmt.Errorf("%s is missing — run `devctl reset` first", setupConnectorDir)
	}
	if _, _, ok := connectorJar(v.DirJars); !ok {
		return fmt.Errorf("%s holds no connector jar — run `devctl reset` first", setupConnectorDir)
	}
	_, code, err := s.execIn(ctx, connectContainer, "sh", "-c", scriptFlatPlugin)
	if err != nil {
		return fmt.Errorf("flattening the plugin directory: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("flattening the plugin directory: exit %d", code)
	}
	s.log.Info("broken: every connector jar is directly in " + setupPluginRoot + " (the setup check " + checkPluginLayout + " fails)")
	return nil
}

func fixFlatPluginDir(ctx context.Context, s *stack) error {
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return err
	}
	if len(v.RootJars) == 0 {
		s.log.Info("skipped: no jars directly in " + setupPluginRoot)
		return nil
	}
	_, code, err := s.execIn(ctx, connectContainer, "sh", "-c", scriptUnflatPlugin)
	if err != nil {
		return fmt.Errorf("restoring the plugin directory: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("restoring the plugin directory: exit %d", code)
	}
	s.log.Info("fixed: connector jars moved back into " + setupConnectorDir)
	return nil
}

// ----------------------------------------------- missing converter jars

// verifyConverterJars is the plugin-converter-jars check: the connector
// directory carries debezium-core and debezium-api of the connector's own
// version (jar_list's missing_converters field).
func verifyConverterJars(ctx context.Context, s *stack) (bool, string, error) {
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return false, "", err
	}
	if !v.DirExists {
		return false, setupConnectorDir + " is missing from the plugin path", nil
	}
	_, version, ok := connectorJar(v.DirJars)
	if !ok {
		return false, setupConnectorDir + " holds no debezium-connector-postgres jar", nil
	}
	have := make(map[string]bool, len(v.DirJars))
	for _, j := range v.DirJars {
		have[j] = true
	}
	var missing []string
	for _, want := range []string{"debezium-core-" + version + ".jar", "debezium-api-" + version + ".jar"} {
		if !have[want] {
			missing = append(missing, want)
		}
	}
	if len(missing) > 0 {
		return false, "connector postgres " + version + " is missing " + strings.Join(missing, " and "), nil
	}
	return true, "debezium-core and debezium-api are next to the connector jar", nil
}

func breakMissingConverterJars(ctx context.Context, s *stack) error {
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return err
	}
	if !v.DirExists {
		return fmt.Errorf("%s is missing — run `devctl reset` first", setupConnectorDir)
	}
	_, code, err := s.execIn(ctx, connectContainer, "sh", "-c", scriptStashConverters)
	if err != nil {
		return fmt.Errorf("stashing the converter jars: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("stashing the converter jars: exit %d (are debezium-core and debezium-api in %s?)", code, setupConnectorDir)
	}
	s.log.Info("broken: debezium-core and debezium-api moved out of " + setupConnectorDir + " (the setup check " + checkPluginConverters + " fails)")
	return nil
}

func fixMissingConverterJars(ctx context.Context, s *stack) error {
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return err
	}
	if v.DirExists {
		_, version, ok := connectorJar(v.DirJars)
		if ok {
			have := make(map[string]bool, len(v.DirJars))
			for _, j := range v.DirJars {
				have[j] = true
			}
			if have["debezium-core-"+version+".jar"] && have["debezium-api-"+version+".jar"] {
				s.log.Info("skipped: the converter jars are already in " + setupConnectorDir)
				return nil
			}
		}
	}
	_, code, err := s.execIn(ctx, connectContainer, "sh", "-c", scriptRestoreConverters)
	if err != nil {
		return fmt.Errorf("restoring the converter jars: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("restoring the converter jars: exit %d", code)
	}
	s.log.Info("fixed: debezium-core and debezium-api back in " + setupConnectorDir)
	return nil
}

// ---------------------------------------------------------------- wal_level

// walLevelOverride is the Compose override that restarts the dev Postgres
// with wal_level=replica. ALTER SYSTEM cannot do it: the dev service passes
// wal_level=logical on the command line (`-c`), and a command-line setting
// wins over postgresql.auto.conf. Compose replaces the command, so the
// override is the only way to change it, and `up` recreates the container
// with its existing data volume.
const walLevelOverride = `# devctl setup-break wal-level-replica (dev/scenarios.md): the dev Postgres
# restarts with wal_level=replica, which a logical replication slot cannot use.
services:
  postgres:
    command: ["postgres", "-c", "wal_level=replica", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"]
`

// verifyWALLevel is the postgres-wal-level check: wal_level must be logical,
// the setting a logical replication slot needs.
func verifyWALLevel(ctx context.Context, s *stack) (bool, string, error) {
	level, err := s.psql(ctx, "orders", "SHOW wal_level")
	if err != nil {
		return false, "", err
	}
	level = strings.TrimSpace(level)
	if level != "logical" {
		return false, "wal_level=" + level + "; a logical replication slot needs wal_level=logical", nil
	}
	return true, "wal_level=logical", nil
}

// breakWALLevelReplica restarts the dev Postgres with wal_level=replica. The
// running server must have no logical slot: Postgres refuses to start when a
// logical slot exists at a lower wal_level (that is the failure a real setup
// meets, before any slot exists). So the connector stops and its slot is
// dropped first; the fix recreates the slot and Debezium re-snapshots.
func breakWALLevelReplica(ctx context.Context, s *stack) error {
	// Postgres refuses to start at wal_level=replica while any logical slot
	// exists, and only debezium_orders is dropped here: another slot (the
	// staging connector's, a q5 scenario's) would leave the server down.
	others, err := s.psql(ctx, "orders", sqlOtherLogicalSlots)
	if err != nil {
		return err
	}
	if others = strings.TrimSpace(others); others != "" && others != "0" {
		return fmt.Errorf("refusing: %s other logical slot(s) exist on the dev Postgres (staging or another scenario); Postgres would not start at wal_level=replica. Drop them or run without them first", others)
	}
	if err := s.stopConnectorUntilSlotInactive(ctx); err != nil {
		return err
	}
	if _, exists, err := s.slotRow(ctx); err != nil {
		return err
	} else if exists {
		if _, err := s.psql(ctx, "orders", sqlDropSlot); err != nil {
			return err
		}
	}
	dir, err := os.MkdirTemp("", "devctl-wal-level-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	override := filepath.Join(dir, "wal-level-replica.yaml")
	if err := os.WriteFile(override, []byte(walLevelOverride), 0o600); err != nil {
		return err
	}
	if err := s.composeWith(ctx, override, "up", "-d", "--no-deps", "postgres"); err != nil {
		return err
	}
	if err := s.waitSQLValue(ctx, 120*time.Second, "SHOW wal_level", "replica"); err != nil {
		return fmt.Errorf("the server did not restart with wal_level=replica: %w", err)
	}
	s.log.Info("broken: wal_level=replica after a server restart (the setup check " + checkWALLevel + " fails)")
	return nil
}

// fixWALLevelReplica restarts the Postgres on its own command line again and
// restores the dropped slot: the connector's saved offset points into the
// dropped slot (KAN-411), so the offset is cleared and snapshot.mode=when_needed
// makes Debezium take a fresh snapshot on the new slot, as after `dead-slot`.
func fixWALLevelReplica(ctx context.Context, s *stack) error {
	if err := s.composeWith(ctx, "", "up", "-d", "--no-deps", "postgres"); err != nil {
		return err
	}
	if err := s.waitSQLValue(ctx, 120*time.Second, "SHOW wal_level", "logical"); err != nil {
		return fmt.Errorf("the server did not restart with wal_level=logical: %w", err)
	}
	// Stop first: Connect deletes a connector's offsets only while it is
	// STOPPED, and the slot must have no WAL sender to be (re)created cleanly.
	if err := s.stopConnectorUntilSlotInactive(ctx); err != nil {
		return err
	}
	if _, exists, err := s.slotRow(ctx); err != nil {
		return err
	} else if !exists {
		if _, err := s.psql(ctx, "orders", sqlCreateSlot); err != nil {
			return err
		}
	}
	if err := s.clearConnectorOffsets(ctx); err != nil {
		return err
	}
	if err := s.setSnapshotMode(ctx, "when_needed"); err != nil {
		return err
	}
	if _, err := s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/resume", nil, http.StatusAccepted, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 120*time.Second); err != nil {
		return fmt.Errorf("the connector did not recover after wal_level returned to logical: %w", err)
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		active, err := s.slotActive(ctx)
		if err != nil {
			return false, nil
		}
		return active, nil
	}); err != nil {
		return fmt.Errorf("slot %s did not become active: %w", slotName, err)
	}
	s.log.Info("fixed: wal_level=logical, slot " + slotName + " recreated, connector RUNNING on a fresh snapshot")
	return nil
}

// waitSQLValue waits until query returns want (or the timeout).
func (s *stack) waitSQLValue(ctx context.Context, timeout time.Duration, query, want string) error {
	var last string
	err := s.waitFor(ctx, timeout, func() (bool, error) {
		out, err := s.psql(ctx, "orders", query)
		if err != nil {
			last = err.Error()
			return false, nil
		}
		last = strings.TrimSpace(out)
		return last == want, nil
	})
	if err != nil {
		return fmt.Errorf("%w (last value %q, want %q)", err, last, want)
	}
	return nil
}

// waitTaskFailed waits until connector task 0 reports FAILED.
func (s *stack) waitTaskFailed(ctx context.Context, timeout time.Duration) error {
	return s.waitFor(ctx, timeout, func() (bool, error) {
		st, err := s.connectorStatus(ctx)
		if err != nil {
			return false, nil
		}
		return len(st.Tasks) > 0 && st.Tasks[0].State == "FAILED", nil
	})
}

// ------------------------------------------------------------ missing grant

// roleNameRE is the only shape a database.user may take before it goes into a
// query: the repo's identifier rule (AGENTS.md section 4 rule 6). A value
// outside it fails the check instead of being quoted.
var roleNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// tableNameRE is the same for a table.include.list entry ("public.orders").
var tableNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`)

// connectorSetting reads one string setting of the registered connector.
func (s *stack) connectorSetting(ctx context.Context, key string) (string, error) {
	b, err := s.do(ctx, http.MethodGet, s.cfg.ConnectURL+"/connectors/"+connectorName+"/config", nil, http.StatusOK)
	if err != nil {
		return "", err
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return "", fmt.Errorf("devctl: decode connector config: %w", err)
	}
	v, _ := cfg[key].(string)
	return v, nil
}

// sourceTables is the connector's table.include.list, split and trimmed.
func sourceTables(ctx context.Context, s *stack) ([]string, error) {
	list, err := s.connectorSetting(ctx, "table.include.list")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, t := range strings.Split(list, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if !tableNameRE.MatchString(t) {
			return nil, fmt.Errorf("table.include.list entry %q is not schema.table", t)
		}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("the connector has no table.include.list")
	}
	return out, nil
}

// roleGrants is what the source role may do: log in, replicate, and SELECT
// each published table.
type roleGrants struct {
	Role        string
	Exists      bool
	CanLogin    bool
	Replication bool
	Selectable  map[string]bool
}

// readRoleGrants reads the connector's database.user and its rights.
func (s *stack) readRoleGrants(ctx context.Context) (roleGrants, error) {
	role, err := s.connectorSetting(ctx, "database.user")
	if err != nil {
		return roleGrants{}, err
	}
	g := roleGrants{Role: role, Selectable: map[string]bool{}}
	if !roleNameRE.MatchString(role) {
		return g, fmt.Errorf("database.user %q is not a plain role name", role)
	}
	out, err := s.psql(ctx, "orders", "SELECT rolcanlogin, rolreplication FROM pg_roles WHERE rolname = '"+role+"'")
	if err != nil {
		return g, err
	}
	out = strings.TrimSpace(out)
	if out != "" {
		parts := strings.Split(out, "|")
		if len(parts) == 2 {
			g.Exists = true
			g.CanLogin = parts[0] == "t"
			g.Replication = parts[1] == "t"
		}
	}
	tables, err := sourceTables(ctx, s)
	if err != nil {
		return g, err
	}
	for _, t := range tables {
		ok, err := s.psql(ctx, "orders", "SELECT has_table_privilege('"+role+"', '"+t+"', 'SELECT')")
		if err != nil {
			return g, err
		}
		g.Selectable[t] = strings.TrimSpace(ok) == "t"
	}
	return g, nil
}

// verifySourceRoleGrants is the source-role-grants check: the connector's
// database.user exists, may log in and replicate, and may SELECT every table
// in table.include.list.
func verifySourceRoleGrants(ctx context.Context, s *stack) (bool, string, error) {
	g, err := s.readRoleGrants(ctx)
	if err != nil {
		return false, "", err
	}
	if !g.Exists {
		return false, "database.user " + g.Role + " does not exist", nil
	}
	var missing []string
	if !g.CanLogin {
		missing = append(missing, "LOGIN")
	}
	if !g.Replication {
		missing = append(missing, "REPLICATION")
	}
	for t, ok := range g.Selectable {
		if !ok {
			missing = append(missing, "SELECT on "+t)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return false, "role " + g.Role + " is missing " + strings.Join(missing, ", "), nil
	}
	return true, "role " + g.Role + " may log in, replicate and select the published tables", nil
}

func breakSetupMissingGrant(ctx context.Context, s *stack) error {
	_, err := s.psql(ctx, "orders",
		// CREATE ROLE is a utility statement, so it runs through EXECUTE inside
		// the DO block; the password is the dev stack's, never a secret.
		"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '"+setupGrantRole+"') THEN EXECUTE 'CREATE ROLE "+setupGrantRole+" LOGIN PASSWORD ''piperoom-dev'''; END IF; END $$",
		// CONNECT and USAGE are granted; REPLICATION and SELECT are what the
		// break leaves out, the two a CDC role needs (the check names them).
		"GRANT CONNECT ON DATABASE orders TO "+setupGrantRole,
		"GRANT USAGE ON SCHEMA public TO "+setupGrantRole)
	if err != nil {
		return err
	}
	if err := s.registerConnectorUser(ctx, setupGrantRole, "piperoom-dev"); err != nil {
		return err
	}
	// PUT config restarts the connector; the task fails on the missing rights.
	if err := s.waitTaskFailed(ctx, 20*time.Second); err != nil {
		s.log.Info("note: task 0 did not report FAILED after the grant break; the grants check already fails")
	}
	s.log.Info("broken: connector user is " + setupGrantRole + " without REPLICATION or SELECT (the setup check " + checkSourceRoleGrants + " fails)")
	return nil
}

func fixSetupMissingGrant(ctx context.Context, s *stack) error {
	// Back to the baseline connector user first, so the role can be dropped.
	if err := s.registerConnector(ctx); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 120*time.Second); err != nil {
		return fmt.Errorf("the connector did not return to RUNNING: %w", err)
	}
	// End the dropped role's sessions before dropping it, then drop what it
	// was granted; DROP ROLE refuses a role that still holds privileges.
	if _, err := s.psql(ctx, "orders",
		"SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE usename = '"+setupGrantRole+"'",
		"DROP OWNED BY "+setupGrantRole,
		"DROP ROLE IF EXISTS "+setupGrantRole); err != nil {
		// A role that was never created is fine: DROP IF EXISTS covers it, and
		// DROP OWNED on a missing role errors, so tolerate that one.
		if !strings.Contains(err.Error(), "does not exist") {
			return err
		}
	}
	s.log.Info("fixed: connector back on its baseline user, " + setupGrantRole + " dropped")
	return nil
}

// --------------------------------------------------- db unreachable from Connect

// The network break of db-unreachable-from-connect: the worker's /etc/hosts is
// changed so the connector's database.hostname points at 192.0.2.1 (TEST-NET-1,
// a black hole), the way a wrong DNS entry or a missing route looks from
// inside Connect. The config stays valid, so Connect's own config validation
// (which tries a connection and refuses a bad host) does not reject it; only
// the network path from the worker is broken. /etc/hosts is a bind mount, so
// the change survives a task restart and is undone by rewriting the file.
// sqlOtherLogicalSlots counts the logical slots other than the dev
// connector's, which breakWALLevelReplica drops itself.
const sqlOtherLogicalSlots = "SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'logical' AND slot_name <> '" + slotName + "'"

const (
	// scriptBlockSource appends the black-hole line for the host in $1.
	scriptBlockSource = `set -e; printf "\n192.0.2.1 %s\n" "$1" >> /etc/hosts`
	// scriptProbeTCP opens a TCP connection to $1:$2 from inside the worker,
	// with a 3 s timeout. It is the reachability read the check makes. The
	// host and port reach the inner bash as positional arguments, never as
	// script text, and are validated in Go first (hostNameRE, portRE).
	scriptProbeTCP = `timeout 3 bash -c 'exec 3<>/dev/tcp/$1/$2' _ "$1" "$2"`
)

// hostNameRE is the only shape a database.hostname may take before it reaches
// the probe: a host name or IPv4 address. Anything else fails the check
// instead of being handed to a shell.
var hostNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$`)

// verifyConnectSourceReachable is the connect-source-reachable check: the
// worker can open a TCP connection to the connector's database.hostname:port.
// That is the network path a wrong DNS entry, a missing route or a firewall
// blocks, and it is independent of credentials (other checks cover those).
func verifyConnectSourceReachable(ctx context.Context, s *stack) (bool, string, error) {
	host, err := s.connectorSetting(ctx, "database.hostname")
	if err != nil {
		return false, "", err
	}
	if host == "" {
		return false, "database.hostname is not set", nil
	}
	if !hostNameRE.MatchString(host) {
		return false, fmt.Sprintf("database.hostname %q is not a host name", host), nil
	}
	port, err := s.connectorSetting(ctx, "database.port")
	if err != nil {
		return false, "", err
	}
	if port == "" {
		port = "5432"
	}
	if !portRE.MatchString(port) {
		return false, fmt.Sprintf("database.port %q is not a port", port), nil
	}
	_, code, err := s.execIn(ctx, connectContainer, "bash", "-c", scriptProbeTCP, "_", host, port)
	if err != nil {
		return false, "", fmt.Errorf("devctl: probing %s:%s in %s: %w", host, port, connectContainer, err)
	}
	switch code {
	case 0:
		return true, "the worker can reach " + host + ":" + port, nil
	case 126, 127:
		// No bash or timeout in the image: fall back to the name lookup and
		// the task's own error.
		lookup, lcode, lerr := s.execIn(ctx, connectContainer, "getent", "hosts", host)
		if lerr != nil {
			return false, "", lerr
		}
		if lcode != 0 || strings.TrimSpace(lookup) == "" {
			return false, "database.hostname " + host + " does not resolve from the Connect container", nil
		}
	default:
		return false, "the Connect container cannot open a TCP connection to " + host + ":" + port, nil
	}
	st, err := s.connectorStatus(ctx)
	if err != nil {
		return false, "", err
	}
	if len(st.Tasks) > 0 && st.Tasks[0].State == "FAILED" && strings.Contains(st.Tasks[0].Trace, "UnknownHostException") {
		return false, "task 0 FAILED: database.hostname " + host + " is unknown to the Connect container", nil
	}
	return true, "database.hostname " + host + " resolves from the Connect container", nil
}

// portRE is a TCP port as the connector configuration carries it (a string).
var portRE = regexp.MustCompile(`^[0-9]{1,5}$`)

func breakDBUnreachable(ctx context.Context, s *stack) error {
	host, err := s.connectorSetting(ctx, "database.hostname")
	if err != nil {
		return err
	}
	if !hostNameRE.MatchString(host) {
		return fmt.Errorf("database.hostname %q is not a host name", host)
	}
	// -u 0: /etc/hosts is root-owned; the worker process runs unprivileged.
	if _, code, err := s.execInUser(ctx, "0", connectContainer, "sh", "-c", scriptBlockSource, "_", host); err != nil {
		return fmt.Errorf("blocking %s in the Connect container: %w", host, err)
	} else if code != 0 {
		return fmt.Errorf("blocking %s in the Connect container: exit %d", host, code)
	}
	// Restart the task so it resolves the host again and sees the black hole.
	// The task stays RUNNING while Debezium retries, so the break is proven by
	// the reachability check, not by the task state.
	if err := s.restartTask(ctx, 0); err != nil {
		s.log.Info("note: the task restart was refused; the reachability check already fails", "err", err)
	}
	s.log.Info("broken: " + host + " resolves to 192.0.2.1 inside the Connect container (the setup check " + checkConnectSourceReach + " fails)")
	return nil
}

// fixDBUnreachable restarts the Connect container: Docker regenerates
// /etc/hosts from the service definition, and the worker's JVM starts with an
// empty DNS cache (a long-lived JVM may keep a cached bad address, which a
// task restart alone would not clear). The connector's config never changed,
// so Connect brings it back from its config topic.
func fixDBUnreachable(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, nil, "restart", "connect"); err != nil {
		return err
	}
	if err := s.waitHTTP(ctx, s.cfg.ConnectURL+"/connectors", s.connectStartTimeout()); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", s.connectStartTimeout()); err != nil {
		return fmt.Errorf("the connector did not return to RUNNING: %w", err)
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		active, err := s.slotActive(ctx)
		if err != nil {
			return false, nil
		}
		return active, nil
	}); err != nil {
		return fmt.Errorf("slot %s did not become active: %w", slotName, err)
	}
	s.log.Info("fixed: Connect restarted with a clean /etc/hosts, connector task 0 RUNNING, slot active")
	return nil
}

// -------------------------------------------------------- plugin after scan

// connectorPluginClasses lists the connector classes the worker found when it
// scanned its plugin path.
func (s *stack) connectorPluginClasses(ctx context.Context) ([]string, error) {
	b, err := s.do(ctx, http.MethodGet, s.cfg.ConnectURL+"/connector-plugins", nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var plugins []struct {
		Class string `json:"class"`
	}
	if err := json.Unmarshal(b, &plugins); err != nil {
		return nil, fmt.Errorf("devctl: decode connector plugins: %w", err)
	}
	classes := make([]string, 0, len(plugins))
	for _, p := range plugins {
		classes = append(classes, p.Class)
	}
	return classes, nil
}

// hasClass reports whether classes holds want.
func hasClass(classes []string, want string) bool {
	for _, c := range classes {
		if c == want {
			return true
		}
	}
	return false
}

// verifyConnectPluginScan is the connect-plugin-scan check: the plugin is on
// disk and the worker's plugin list knows it. A plugin directory that
// appeared after the worker's scan is on disk and not in the list — the
// worker must restart to see it.
func verifyConnectPluginScan(ctx context.Context, s *stack) (bool, string, error) {
	classes, err := s.connectorPluginClasses(ctx)
	if err != nil {
		return false, "", err
	}
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return false, "", err
	}
	_, _, onDisk := connectorJar(v.DirJars)
	switch {
	case hasClass(classes, postgresConnector):
		return true, postgresConnector + " is in the worker's plugin list", nil
	case onDisk:
		return false, setupConnectorDir + " holds the connector jar but " + postgresConnector + " is not in /connector-plugins: the worker scanned the plugin path before the install", nil
	default:
		return false, "the postgres connector plugin is not in " + setupPluginRoot, nil
	}
}

func breakPluginAfterScan(ctx context.Context, s *stack) error {
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return err
	}
	if !v.DirExists {
		return fmt.Errorf("%s is missing — run `devctl reset` first", setupConnectorDir)
	}
	// Take the plugin out, restart the worker so it scans without it, wait
	// until the scan is done (the REST API answers and the class is gone),
	// then put the plugin back without restarting: exactly what installing a
	// plugin after the scan leaves behind.
	if _, code, err := s.execIn(ctx, connectContainer, "sh", "-c", scriptDisablePluginDir); err != nil {
		return fmt.Errorf("moving the plugin directory out: %w", err)
	} else if code != 0 {
		return fmt.Errorf("moving the plugin directory out: exit %d", code)
	}
	if err := s.compose(ctx, nil, "restart", "connect"); err != nil {
		return err
	}
	if err := s.waitHTTP(ctx, s.cfg.ConnectURL+"/connector-plugins", s.connectStartTimeout()); err != nil {
		return err
	}
	if err := s.waitFor(ctx, s.connectStartTimeout(), func() (bool, error) {
		classes, err := s.connectorPluginClasses(ctx)
		if err != nil {
			return false, nil
		}
		return !hasClass(classes, postgresConnector), nil
	}); err != nil {
		return fmt.Errorf("the worker still lists %s after the plugin was removed: %w", postgresConnector, err)
	}
	if _, code, err := s.execIn(ctx, connectContainer, "sh", "-c", scriptRestorePluginDir); err != nil {
		return fmt.Errorf("putting the plugin directory back: %w", err)
	} else if code != 0 {
		return fmt.Errorf("putting the plugin directory back: exit %d", code)
	}
	s.log.Info("broken: the plugin is on disk but the worker scanned before it appeared (the setup check " + checkConnectPluginScan + " fails)")
	return nil
}

func fixPluginAfterScan(ctx context.Context, s *stack) error {
	// A break interrupted before it could put the directory back leaves it in
	// the stash; the fix restores it first (and does nothing when it is in
	// place).
	v, err := s.pluginDirs(ctx)
	if err != nil {
		return err
	}
	if !v.DirExists {
		if _, code, err := s.execIn(ctx, connectContainer, "sh", "-c", scriptRestoreIfStashed); err != nil {
			return fmt.Errorf("restoring the stashed plugin directory: %w", err)
		} else if code != 0 {
			return fmt.Errorf("restoring the stashed plugin directory: exit %d", code)
		}
	}
	// The fix a setup run records is "restart the worker": the scan is what
	// picks the plugin up.
	if err := s.compose(ctx, nil, "restart", "connect"); err != nil {
		return err
	}
	if err := s.waitHTTP(ctx, s.cfg.ConnectURL+"/connector-plugins", s.connectStartTimeout()); err != nil {
		return err
	}
	if err := s.waitFor(ctx, s.connectStartTimeout(), func() (bool, error) {
		classes, err := s.connectorPluginClasses(ctx)
		if err != nil {
			return false, nil
		}
		return hasClass(classes, postgresConnector), nil
	}); err != nil {
		return fmt.Errorf("the worker did not list %s after the restart: %w", postgresConnector, err)
	}
	if err := s.waitConnector(ctx, "RUNNING", s.connectStartTimeout()); err != nil {
		return fmt.Errorf("the connector did not return to RUNNING after the worker restart: %w", err)
	}
	s.log.Info("fixed: worker restarted, " + postgresConnector + " in the plugin list, connector RUNNING")
	return nil
}

// firstN returns at most n entries, for a summary line.
func firstN(in []string, n int) []string {
	if len(in) <= n {
		return in
	}
	return in[:n]
}
