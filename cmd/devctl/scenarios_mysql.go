package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// MySQL source breaks (tasks/Q-board.md row Q8a, docs/design/mysql-source.md
// §10.1, dev/scenarios.md §25-§28). They run on the `mysql` compose profile:
// MySQL 8.4 with GTIDs and the Debezium MySQL connector orders-mysql on the
// shared Connect worker. Each break needs the server on dev/owners.mysql.yaml
// (the orders-mysql pipeline), which loads once Q8d merges.
//
//   - binlog-purged: the binlogs the stopped connector still needs are purged.
//   - history-topic-deleted: the schema history topic is deleted.
//   - missing-replication-grant: the connector user loses REPLICATION SLAVE.
//   - server-id-clash: a second connector with the same database.server.id.
//
// Every SQL statement is a constant below, except the PURGE target, which is
// a binlog file name the server itself listed and which must match
// binlogFileRE. mysql runs inside the container as root with the container's
// own MYSQL_ROOT_PASSWORD, passed through an option file (never argv), so
// devctl never holds or prints it.

const (
	mysqlContainer   = "piperoom-dev-mysql-1"
	mysqlConnector   = "orders-mysql"
	mysqlShadow      = "orders-mysql-shadow"
	mysqlHistory     = "schema-history.shop"
	mysqlOrdersTopic = "shop.shop.orders"
	mysqlOwnersFile  = "dev/owners.mysql.yaml"
	mysqlPipelineID  = "orders-mysql"
	// The shadow connector writes its own topics, so only the server id
	// clashes, never a topic.
	mysqlShadowPrefix  = "shop_shadow"
	mysqlShadowHistory = "schema-history.shop-shadow"
	mysqlUpHint        = "docker compose -f dev/docker-compose.yml --profile mysql up -d --wait mysql"
)

// mysqlShadowTopics are every topic the shadow connector can create: the
// schema change topic (topic.prefix) and one per captured table.
var mysqlShadowTopics = []string{mysqlShadowHistory, mysqlShadowPrefix, mysqlShadowPrefix + ".shop.orders", mysqlShadowPrefix + ".shop.order_items"}

// scriptMySQLQuery runs one statement ($1) as root. The option file comes from
// a process substitution, as the image's entrypoint passes the password; -N
// -B print bare tab-separated rows.
const scriptMySQLQuery = `exec mysql --defaults-extra-file=<(printf '[client]\nuser=root\npassword=%s\n' "$MYSQL_ROOT_PASSWORD") -N -B -e "$1"`

const (
	sqlMySQLPing         = "SELECT 1"
	sqlMySQLInsertMarker = "INSERT INTO shop.orders (customer_id, amount) VALUES (4242, 1)"
	// sqlMySQLWriteBatch is the traffic the stopped connector misses.
	sqlMySQLWriteBatch  = "INSERT INTO shop.orders (customer_id, amount) VALUES (4300, 10), (4301, 20), (4302, 30)"
	sqlMySQLFlushLogs   = "FLUSH BINARY LOGS"
	sqlMySQLShowBinlogs = "SHOW BINARY LOGS"
	sqlMySQLGTIDPurged  = "SELECT @@GLOBAL.gtid_purged"
	// The connector user's global REPLICATION SLAVE, 'Y' or 'N'. A global
	// privilege change reaches a session only when it reconnects, so the break
	// and the fix restart the task.
	sqlMySQLReplGrant  = "SELECT Repl_slave_priv FROM mysql.user WHERE User = 'debezium' AND Host = '%'"
	sqlMySQLRevokeRepl = "REVOKE REPLICATION SLAVE ON *.* FROM 'debezium'@'%'"
	sqlMySQLGrantRepl  = "GRANT REPLICATION SLAVE ON *.* TO 'debezium'@'%'"
	// The source logs a warning each time a dump thread connects with the
	// server id of a running one ("found a zombie dump thread with the same
	// UUID/server_id"): the evidence of server-id-clash.
	sqlMySQLZombies = "SELECT COUNT(*) FROM performance_schema.error_log WHERE DATA LIKE '%zombie dump thread%'"
)

// binlogFileRE is a binlog file name as SHOW BINARY LOGS lists it
// (binlog.000042); anything else never reaches the PURGE statement.
var binlogFileRE = regexp.MustCompile(`^[A-Za-z0-9_-]+\.[0-9]{6,}$`)

const serverIDClashAlert = `{
  "title": "orders-mysql: new shop orders reach Kafka late and in bursts",
  "severity": "sev3",
  "pipeline_id": "orders-mysql",
  "dedup_key": "devctl-server-id-clash",
  "source": "freshness",
  "description": "Rows inserted into shop.orders reach topic shop.shop.orders late and in bursts. Connector orders-mysql reports RUNNING; its log shows the binlog connection dropping and reconnecting."
}`

// mysqlQuery runs one statement in the mysql container and returns its
// trimmed output. A non-zero exit (SQL error, no such container) is an error
// that names how to start the profile.
func (s *stack) mysqlQuery(ctx context.Context, query string) (string, error) {
	out, code, err := s.execIn(ctx, mysqlContainer, "bash", "-c", scriptMySQLQuery, "_", query)
	if err != nil {
		return "", fmt.Errorf("devctl: mysql in %s: %w", mysqlContainer, err)
	}
	if code != 0 {
		return "", fmt.Errorf("devctl: mysql in %s exited %d (is the profile up? %s): %s", mysqlContainer, code, mysqlUpHint, strings.TrimSpace(out))
	}
	return strings.TrimSpace(out), nil
}

func (s *stack) mysqlConnectorURL(name string) string {
	return s.cfg.ConnectURL + "/connectors/" + url.PathEscape(name)
}

// mysqlConnectorConfig is dev/connectors/orders-mysql.json with the given keys
// replaced (a value of "" removes the key).
func (s *stack) mysqlConnectorConfig(set map[string]string) (map[string]any, error) {
	body, err := os.ReadFile(filepath.Join(s.root, "dev", "connectors", mysqlConnector+".json"))
	if err != nil {
		return nil, fmt.Errorf("devctl: read mysql connector config: %w", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(body, &cfg); err != nil {
		return nil, fmt.Errorf("devctl: decode mysql connector config: %w", err)
	}
	for k, v := range set {
		if v == "" {
			delete(cfg, k)
			continue
		}
		cfg[k] = v
	}
	return cfg, nil
}

// putMySQLConnector creates or updates connector name from the dev config with
// set applied. PUT /config is safe to repeat.
func (s *stack) putMySQLConnector(ctx context.Context, name string, set map[string]string) error {
	cfg, err := s.mysqlConnectorConfig(set)
	if err != nil {
		return err
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("devctl: encode mysql connector config: %w", err)
	}
	_, err = s.do(ctx, http.MethodPut, s.mysqlConnectorURL(name)+"/config", b, http.StatusOK, http.StatusCreated)
	return err
}

func (s *stack) mysqlConnectorExists(ctx context.Context, name string) (bool, error) {
	b, err := s.do(ctx, http.MethodGet, s.cfg.ConnectURL+"/connectors", nil, http.StatusOK)
	if err != nil {
		return false, err
	}
	var names []string
	if err := json.Unmarshal(b, &names); err != nil {
		return false, fmt.Errorf("devctl: decode connector list: %w", err)
	}
	for _, n := range names {
		if n == name {
			return true, nil
		}
	}
	return false, nil
}

func (s *stack) mysqlStatus(ctx context.Context, name string) (connectorStatus, error) {
	var st connectorStatus
	b, err := s.do(ctx, http.MethodGet, s.mysqlConnectorURL(name)+"/status", nil, http.StatusOK)
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, fmt.Errorf("devctl: decode %s status: %w", name, err)
	}
	return st, nil
}

// mysqlRunning reports whether the connector and task 0 are RUNNING.
func (s *stack) mysqlRunning(ctx context.Context, name string) (bool, error) {
	st, err := s.mysqlStatus(ctx, name)
	if err != nil {
		return false, err
	}
	return taskState(st) == "RUNNING", nil
}

// taskState is task 0's state when the connector runs, else the connector's.
func taskState(st connectorStatus) string {
	if st.Connector.State != "RUNNING" || len(st.Tasks) == 0 {
		return st.Connector.State
	}
	return st.Tasks[0].State
}

// waitMySQLTask waits until the connector runs and task 0 is in state want.
func (s *stack) waitMySQLTask(ctx context.Context, name, want string, timeout time.Duration) error {
	if err := s.waitFor(ctx, timeout, func() (bool, error) {
		st, err := s.mysqlStatus(ctx, name)
		if err != nil {
			return false, nil
		}
		return st.Connector.State == "RUNNING" && len(st.Tasks) > 0 && st.Tasks[0].State == want, nil
	}); err != nil {
		return fmt.Errorf("devctl: %s task 0 not %s: %w", name, want, err)
	}
	return nil
}

// stopMySQLConnector stops the connector (a stopped connector closes its
// binlog connection; a paused one does not) and waits for STOPPED.
func (s *stack) stopMySQLConnector(ctx context.Context, name string) error {
	if _, err := s.do(ctx, http.MethodPut, s.mysqlConnectorURL(name)+"/stop", nil, http.StatusNoContent, http.StatusAccepted, http.StatusOK); err != nil {
		return err
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		st, err := s.mysqlStatus(ctx, name)
		if err != nil {
			return false, nil
		}
		return st.Connector.State == "STOPPED", nil
	}); err != nil {
		return fmt.Errorf("devctl: %s did not stop: %w", name, err)
	}
	return nil
}

func (s *stack) resumeMySQLConnector(ctx context.Context, name string) error {
	_, err := s.do(ctx, http.MethodPut, s.mysqlConnectorURL(name)+"/resume", nil, http.StatusAccepted, http.StatusOK, http.StatusNoContent)
	return err
}

func (s *stack) restartMySQLTask(ctx context.Context, name string) error {
	_, err := s.do(ctx, http.MethodPost, s.mysqlConnectorURL(name)+"/tasks/0/restart", nil, http.StatusOK, http.StatusNoContent, http.StatusAccepted)
	return err
}

// clearMySQLOffsets deletes the stopped connector's stored offsets (Kafka
// Connect 3.6+; 204 when there are none, so it is safe to repeat).
func (s *stack) clearMySQLOffsets(ctx context.Context, name string) error {
	if _, err := s.do(ctx, http.MethodDelete, s.mysqlConnectorURL(name)+"/offsets", nil, http.StatusNoContent, http.StatusOK); err != nil {
		return fmt.Errorf("devctl: clear %s offsets: %w", name, err)
	}
	return nil
}

// mysqlStoredPosition is the stored binlog position of the connector, for the
// log line: "file:pos gtids=<set>", "" when there is none.
func (s *stack) mysqlStoredPosition(ctx context.Context, name string) string {
	b, err := s.do(ctx, http.MethodGet, s.mysqlConnectorURL(name)+"/offsets", nil, http.StatusOK)
	if err != nil {
		return ""
	}
	var o storedOffsets
	if json.Unmarshal(b, &o) != nil {
		return ""
	}
	for _, e := range o.Offsets {
		if f, ok := e.Offset["file"]; ok {
			return fmt.Sprintf("%v:%v gtids=%v", f, e.Offset["pos"], e.Offset["gtids"])
		}
	}
	return ""
}

func (s *stack) mysqlTrace(ctx context.Context, name string) string {
	st, err := s.mysqlStatus(ctx, name)
	if err != nil || len(st.Tasks) == 0 {
		return ""
	}
	return firstLine(st.Tasks[0].Trace)
}

// waitTopicPresence waits until topic exists (want true) or is gone (false).
// Topic deletion is asynchronous on the broker.
func (s *stack) waitTopicPresence(ctx context.Context, topic string, want bool, timeout time.Duration) error {
	if err := s.waitFor(ctx, timeout, func() (bool, error) {
		_, ok, err := s.topicSpan(ctx, topic)
		if err != nil {
			return false, nil
		}
		return ok == want, nil
	}); err != nil {
		return fmt.Errorf("devctl: topic %s present=%t: %w", topic, !want, err)
	}
	return nil
}

func (s *stack) topicRecords(ctx context.Context, topic string) (int64, error) {
	span, _, err := s.topicSpan(ctx, topic)
	if err != nil {
		return 0, err
	}
	return span.records(), nil
}

// ensureMySQLPipeline checks the profile answers and registers orders-mysql
// when it is missing (it is not part of `devctl up`), waiting for its snapshot
// to reach Kafka.
func (s *stack) ensureMySQLPipeline(ctx context.Context) error {
	if _, err := s.mysqlQuery(ctx, sqlMySQLPing); err != nil {
		return err
	}
	ok, err := s.mysqlConnectorExists(ctx, mysqlConnector)
	if err != nil || ok {
		return err
	}
	if err := s.putMySQLConnector(ctx, mysqlConnector, nil); err != nil {
		return err
	}
	if err := s.waitMySQLTask(ctx, mysqlConnector, "RUNNING", 2*time.Minute); err != nil {
		return err
	}
	if err := s.waitFor(ctx, 2*time.Minute, func() (bool, error) {
		n, err := s.topicRecords(ctx, mysqlOrdersTopic)
		return err == nil && n > 0, nil
	}); err != nil {
		return fmt.Errorf("devctl: the %s snapshot did not reach %s: %w", mysqlConnector, mysqlOrdersTopic, err)
	}
	s.log.Info("registered " + mysqlConnector + " (dev/connectors/orders-mysql.json); snapshot in " + mysqlOrdersTopic)
	return nil
}

// requireMySQLRunning refuses a break on a connector that is not healthy: the
// break would not be the cause of what follows.
func (s *stack) requireMySQLRunning(ctx context.Context, scenario string) error {
	ok, err := s.mysqlRunning(ctx, mysqlConnector)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("devctl: %s is not RUNNING — run `devctl fix %s` first", mysqlConnector, scenario)
	}
	return nil
}

// waitMySQLStreaming inserts a marker order and waits until a new record
// reaches shop.shop.orders: the binlog stream works end to end.
func (s *stack) waitMySQLStreaming(ctx context.Context) error {
	before, err := s.topicRecords(ctx, mysqlOrdersTopic)
	if err != nil {
		return err
	}
	if _, err := s.mysqlQuery(ctx, sqlMySQLInsertMarker); err != nil {
		return err
	}
	if err := s.waitFor(ctx, 90*time.Second, func() (bool, error) {
		n, err := s.topicRecords(ctx, mysqlOrdersTopic)
		return err == nil && n > before, nil
	}); err != nil {
		return fmt.Errorf("devctl: a marker order did not reach %s: %w", mysqlOrdersTopic, err)
	}
	return nil
}

// ------------------------------------------------------------ binlog-purged

// breakBinlogPurged: the connector is stopped (a maintenance window), writes
// go on, two FLUSH BINARY LOGS rotate the binlog and PURGE BINARY LOGS TO the
// newest file drops every older one, so the position and the GTIDs the stored
// offset needs are gone. On resume Debezium (snapshot.mode initial) fails.
func breakBinlogPurged(ctx context.Context, s *stack) error {
	if err := s.ensureMySQLPipeline(ctx); err != nil {
		return err
	}
	if err := s.requireMySQLRunning(ctx, "binlog-purged"); err != nil {
		return err
	}
	if err := s.stopMySQLConnector(ctx, mysqlConnector); err != nil {
		return err
	}
	stored := s.mysqlStoredPosition(ctx, mysqlConnector)
	if _, err := s.mysqlQuery(ctx, sqlMySQLWriteBatch); err != nil {
		return err
	}
	for range 2 {
		if _, err := s.mysqlQuery(ctx, sqlMySQLFlushLogs); err != nil {
			return err
		}
	}
	newest, err := s.newestBinlog(ctx)
	if err != nil {
		return err
	}
	if _, err := s.mysqlQuery(ctx, "PURGE BINARY LOGS TO '"+newest+"'"); err != nil {
		return err
	}
	purged, err := s.mysqlQuery(ctx, sqlMySQLGTIDPurged)
	if err != nil {
		return err
	}
	if err := s.resumeMySQLConnector(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.waitMySQLTask(ctx, mysqlConnector, "FAILED", 90*time.Second); err != nil {
		return err
	}
	s.log.Info("broken: binlogs before "+newest+" purged while "+mysqlConnector+" was stopped — task 0 FAILED",
		"stored_position", stored, "gtid_purged", purged, "trace", s.mysqlTrace(ctx, mysqlConnector))
	return nil
}

// newestBinlog is the last file SHOW BINARY LOGS lists (name, size,
// encrypted, tab-separated).
func (s *stack) newestBinlog(ctx context.Context) (string, error) {
	out, err := s.mysqlQuery(ctx, sqlMySQLShowBinlogs)
	if err != nil {
		return "", err
	}
	lines := strings.Split(out, "\n")
	name := strings.Fields(lines[len(lines)-1])
	if len(name) == 0 || !binlogFileRE.MatchString(name[0]) {
		return "", fmt.Errorf("devctl: SHOW BINARY LOGS gave no binlog file name: %q", firstLine(out))
	}
	return name[0], nil
}

// The fix is what connect_stop_connector and connect_reset_source_offsets (to
// empty) do, then a fresh snapshot: stop, delete the stored offsets and the
// schema history (it describes the old position; the snapshot writes it
// again), resume. Idempotent: a RUNNING connector is left alone.
func fixBinlogPurged(ctx context.Context, s *stack) error {
	if err := s.ensureMySQLPipeline(ctx); err != nil {
		return err
	}
	if ok, err := s.mysqlRunning(ctx, mysqlConnector); err != nil {
		return err
	} else if ok {
		s.log.Info("skipped: " + mysqlConnector + " is RUNNING, nothing to re-snapshot")
		return nil
	}
	if err := s.resnapshotMySQL(ctx); err != nil {
		return err
	}
	if err := s.waitMySQLStreaming(ctx); err != nil {
		return err
	}
	s.log.Info("fixed: offsets and schema history cleared, " + mysqlConnector + " re-snapshotted and streaming")
	return nil
}

// resnapshotMySQL stops orders-mysql, deletes its offsets and history topic,
// and resumes it with no offsets, so Debezium snapshots from the current
// binlog position.
func (s *stack) resnapshotMySQL(ctx context.Context) error {
	if err := s.stopMySQLConnector(ctx, mysqlConnector); err != nil {
		return err
	}
	stale := s.mysqlStoredPosition(ctx, mysqlConnector)
	if err := s.clearMySQLOffsets(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.deleteTopics(ctx, mysqlHistory); err != nil {
		return err
	}
	if err := s.waitTopicPresence(ctx, mysqlHistory, false, 60*time.Second); err != nil {
		return err
	}
	s.log.Info("stopped "+mysqlConnector+", cleared its offsets and "+mysqlHistory, "stored_position", stale)
	if err := s.resumeMySQLConnector(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.waitMySQLTask(ctx, mysqlConnector, "RUNNING", 2*time.Minute); err != nil {
		return err
	}
	return s.waitTopicPresence(ctx, mysqlHistory, true, 60*time.Second)
}

// ------------------------------------------------------------ history-topic-deleted

// breakHistoryTopicDeleted: someone cleans up "unused" topics while the
// connector is stopped and deletes schema-history.shop. The stored offset
// says the snapshot is done, so on resume Debezium needs the history to know
// the table schemas at that position, finds none and fails.
func breakHistoryTopicDeleted(ctx context.Context, s *stack) error {
	if err := s.ensureMySQLPipeline(ctx); err != nil {
		return err
	}
	if err := s.requireMySQLRunning(ctx, "history-topic-deleted"); err != nil {
		return err
	}
	if _, ok, err := s.topicSpan(ctx, mysqlHistory); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("devctl: %s is already missing — run `devctl fix history-topic-deleted` first", mysqlHistory)
	}
	if err := s.stopMySQLConnector(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.deleteTopics(ctx, mysqlHistory); err != nil {
		return err
	}
	if err := s.waitTopicPresence(ctx, mysqlHistory, false, 60*time.Second); err != nil {
		return err
	}
	if err := s.resumeMySQLConnector(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.waitMySQLTask(ctx, mysqlConnector, "FAILED", 90*time.Second); err != nil {
		return err
	}
	s.log.Info("broken: "+mysqlHistory+" deleted, "+mysqlConnector+" task 0 FAILED",
		"stored_position", s.mysqlStoredPosition(ctx, mysqlConnector), "trace", s.mysqlTrace(ctx, mysqlConnector))
	return nil
}

// The fix is what connect_update_connector_config with snapshot.mode
// recovery does (design §8.3): the connector rebuilds the history from the
// current table schemas and keeps its binlog position. Safe here because no
// DDL ran since the stored position. Then the baseline config (no
// snapshot.mode) goes back, as Debezium's docs ask after a recovery.
// Idempotent: a RUNNING connector with its history topic is left alone.
func fixHistoryTopicDeleted(ctx context.Context, s *stack) error {
	if err := s.ensureMySQLPipeline(ctx); err != nil {
		return err
	}
	running, err := s.mysqlRunning(ctx, mysqlConnector)
	if err != nil {
		return err
	}
	_, present, err := s.topicSpan(ctx, mysqlHistory)
	if err != nil {
		return err
	}
	if running && present {
		s.log.Info("skipped: " + mysqlConnector + " is RUNNING and " + mysqlHistory + " exists")
		return nil
	}
	if err := s.stopMySQLConnector(ctx, mysqlConnector); err != nil {
		return err
	}
	if present {
		// A recovery rebuilds the history into the topic: a partial one left
		// in place would be read first.
		if err := s.deleteTopics(ctx, mysqlHistory); err != nil {
			return err
		}
		if err := s.waitTopicPresence(ctx, mysqlHistory, false, 60*time.Second); err != nil {
			return err
		}
	}
	if err := s.putMySQLConnector(ctx, mysqlConnector, map[string]string{"snapshot.mode": "recovery"}); err != nil {
		return err
	}
	if err := s.resumeMySQLConnector(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.waitMySQLTask(ctx, mysqlConnector, "RUNNING", 2*time.Minute); err != nil {
		return err
	}
	if err := s.waitTopicPresence(ctx, mysqlHistory, true, 60*time.Second); err != nil {
		return err
	}
	if err := s.putMySQLConnector(ctx, mysqlConnector, nil); err != nil {
		return err
	}
	if err := s.waitMySQLTask(ctx, mysqlConnector, "RUNNING", 2*time.Minute); err != nil {
		return err
	}
	if err := s.waitMySQLStreaming(ctx); err != nil {
		return err
	}
	s.log.Info("fixed: " + mysqlHistory + " rebuilt by snapshot.mode=recovery, config back to dev/connectors, " + mysqlConnector + " streaming")
	return nil
}

// ------------------------------------------------------------ missing-replication-grant

// breakMissingReplicationGrant: a privilege clean-up revokes REPLICATION
// SLAVE from the connector user. The running binlog session keeps it until it
// reconnects, so the break restarts task 0; the new binlog connection is
// refused and the task fails.
func breakMissingReplicationGrant(ctx context.Context, s *stack) error {
	if err := s.ensureMySQLPipeline(ctx); err != nil {
		return err
	}
	if err := s.requireMySQLRunning(ctx, "missing-replication-grant"); err != nil {
		return err
	}
	if g, err := s.mysqlQuery(ctx, sqlMySQLReplGrant); err != nil {
		return err
	} else if g != "Y" {
		return fmt.Errorf("devctl: 'debezium'@'%%' has REPLICATION SLAVE %q, not Y — run `devctl fix missing-replication-grant` first", g)
	}
	if _, err := s.mysqlQuery(ctx, sqlMySQLRevokeRepl); err != nil {
		return err
	}
	if err := s.restartMySQLTask(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.waitMySQLTask(ctx, mysqlConnector, "FAILED", 90*time.Second); err != nil {
		return err
	}
	s.log.Info("broken: REPLICATION SLAVE revoked from 'debezium'@'%', "+mysqlConnector+" task 0 FAILED",
		"trace", s.mysqlTrace(ctx, mysqlConnector))
	return nil
}

// The fix is the GRANT a DBA runs (Piperoom only drafts it, design §8.4),
// then a task restart. Idempotent: with the grant and a RUNNING task it does
// nothing.
func fixMissingReplicationGrant(ctx context.Context, s *stack) error {
	if err := s.ensureMySQLPipeline(ctx); err != nil {
		return err
	}
	g, err := s.mysqlQuery(ctx, sqlMySQLReplGrant)
	if err != nil {
		return err
	}
	running, err := s.mysqlRunning(ctx, mysqlConnector)
	if err != nil {
		return err
	}
	if g == "Y" && running {
		s.log.Info("skipped: 'debezium'@'%' has REPLICATION SLAVE and " + mysqlConnector + " is RUNNING")
		return nil
	}
	if g != "Y" {
		if _, err := s.mysqlQuery(ctx, sqlMySQLGrantRepl); err != nil {
			return err
		}
	}
	if err := s.startMySQLTask(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.waitMySQLStreaming(ctx); err != nil {
		return err
	}
	s.log.Info("fixed: REPLICATION SLAVE granted to 'debezium'@'%', " + mysqlConnector + " streaming")
	return nil
}

// startMySQLTask resumes a stopped connector or restarts task 0, then waits
// for RUNNING.
func (s *stack) startMySQLTask(ctx context.Context, name string) error {
	st, err := s.mysqlStatus(ctx, name)
	if err != nil {
		return err
	}
	switch {
	case st.Connector.State == "STOPPED" || st.Connector.State == "PAUSED":
		err = s.resumeMySQLConnector(ctx, name)
	case taskState(st) != "RUNNING":
		err = s.restartMySQLTask(ctx, name)
	}
	if err != nil {
		return err
	}
	return s.waitMySQLTask(ctx, name, "RUNNING", 90*time.Second)
}

// ------------------------------------------------------------ server-id-clash

// breakServerIDClash: a second connector (a copy someone registered to test
// a change) runs with the same database.server.id. The source allows one
// binlog dump thread per server id and kills the older one whenever the other
// connects, so the two connectors take turns: both report RUNNING while their
// streams drop and reconnect. The watcher sees no failed task; the room
// opens with `devctl alert server-id-clash`.
func breakServerIDClash(ctx context.Context, s *stack) error {
	if err := s.ensureMySQLPipeline(ctx); err != nil {
		return err
	}
	if err := s.requireMySQLRunning(ctx, "server-id-clash"); err != nil {
		return err
	}
	if ok, err := s.mysqlConnectorExists(ctx, mysqlShadow); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("devctl: %s already exists — run `devctl fix server-id-clash` first", mysqlShadow)
	}
	base, err := s.mysqlConnectorConfig(nil)
	if err != nil {
		return err
	}
	id, _ := base["database.server.id"].(string)
	if id == "" {
		return fmt.Errorf("devctl: dev/connectors/%s.json has no database.server.id", mysqlConnector)
	}
	zombies, err := s.mysqlQuery(ctx, sqlMySQLZombies)
	if err != nil {
		return err
	}
	// no_data: the shadow streams from the current position without copying
	// the tables; only its server id is the same.
	if err := s.putMySQLConnector(ctx, mysqlShadow, map[string]string{
		"database.server.id":                  id,
		"topic.prefix":                        mysqlShadowPrefix,
		"schema.history.internal.kafka.topic": mysqlShadowHistory,
		"snapshot.mode":                       "no_data",
	}); err != nil {
		return err
	}
	if err := s.waitMySQLTask(ctx, mysqlShadow, "RUNNING", 90*time.Second); err != nil {
		return err
	}
	// The evidence: the source logs a zombie dump thread once the two
	// streams collide. Not required, since how soon Debezium reconnects
	// varies, but said when it does not show.
	seen := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		if _, err := s.mysqlQuery(ctx, sqlMySQLInsertMarker); err != nil {
			return false, err
		}
		n, err := s.mysqlQuery(ctx, sqlMySQLZombies)
		return err == nil && n != zombies, nil
	}) == nil
	if !seen {
		s.log.Warn("no zombie dump thread logged within 60 s; the clash is in place (same database.server.id) but its effect was not observed",
			"server_id", id, "connectors", mysqlConnector+","+mysqlShadow)
	}
	s.log.Info("broken: "+mysqlShadow+" runs with database.server.id "+id+" like "+mysqlConnector+"; the source drops one binlog stream whenever the other connects",
		"zombie_warning_logged", seen)
	return nil
}

// The fix is connect_delete_connector on the shadow (or a new
// database.server.id for it): stop it, delete its offsets, the connector and
// its topics, then make sure orders-mysql streams. Idempotent: without the
// shadow it only checks orders-mysql.
func fixServerIDClash(ctx context.Context, s *stack) error {
	if err := s.ensureMySQLPipeline(ctx); err != nil {
		return err
	}
	ok, err := s.mysqlConnectorExists(ctx, mysqlShadow)
	if err != nil {
		return err
	}
	if ok {
		if err := s.stopMySQLConnector(ctx, mysqlShadow); err != nil {
			return err
		}
		if err := s.clearMySQLOffsets(ctx, mysqlShadow); err != nil {
			return err
		}
		if _, err := s.do(ctx, http.MethodDelete, s.mysqlConnectorURL(mysqlShadow), nil, http.StatusNoContent, http.StatusOK, http.StatusNotFound); err != nil {
			return err
		}
		if err := s.deleteTopics(ctx, mysqlShadowTopics...); err != nil {
			return err
		}
		s.log.Info("deleted " + mysqlShadow + ", its offsets and topics")
	}
	if err := s.startMySQLTask(ctx, mysqlConnector); err != nil {
		return err
	}
	if err := s.waitMySQLStreaming(ctx); err != nil {
		return err
	}
	s.log.Info("fixed: one connector per database.server.id, " + mysqlConnector + " streaming")
	return nil
}
