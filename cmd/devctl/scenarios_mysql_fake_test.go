package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/config"
)

// mysqlWorld plays the `mysql` profile for the Q8a breaks: MySQL (the exact
// statements the scenarios send through `docker exec`), Kafka Connect
// (httptest, two connector names, KIP-875 stop and offsets) and Kafka (kfake:
// the history and data topics).
//
// It is a model of Debezium's MySQL connector, not a recording: a task start
// with no offset snapshots (and creates its history topic); with an offset it
// fails when the history topic is missing (unless snapshot.mode is recovery),
// when the stored file or GTIDs were purged, and when the user lacks
// REPLICATION SLAVE. A second running connector with the same server id logs
// a zombie dump thread. A statement the model does not know fails the test.
type mysqlWorld struct {
	t     *testing.T
	addrs []string
	kcl   *kgo.Client

	mu   sync.Mutex
	up   bool // the mysql container runs
	conn map[string]*mysqlConn
	// binlog files binlog.<first>..binlog.<last>; GTIDs 1..gtidExec executed,
	// 1..gtidPurged purged.
	first, last          int
	gtidExec, gtidPurged int
	replGrant            bool
	zombies              int
	stmts                []string
}

type mysqlConn struct {
	cfg          map[string]any
	state, task  string
	trace        string
	offset       *mysqlPos
	restarts     int
	historyBuilt int // snapshots or recoveries that wrote the history
}

type mysqlPos struct{ file, gtid int }

func newMySQLWorld(t *testing.T) *mysqlWorld {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, mysqlOrdersTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	kcl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kcl.Close)
	w := &mysqlWorld{
		t: t, addrs: c.ListenAddrs(), kcl: kcl, up: true, conn: map[string]*mysqlConn{},
		first: 1, last: 3, gtidExec: 120, replGrant: true,
	}
	return w
}

// withRunningConnector registers orders-mysql from the dev file, snapshotted
// and streaming, as `devctl break` finds it after the first run.
func (w *mysqlWorld) withRunningConnector() *mysqlWorld {
	w.t.Helper()
	root, err := repoRoot()
	if err != nil {
		w.t.Fatal(err)
	}
	cfg, err := (&stack{root: root}).mysqlConnectorConfig(nil)
	if err != nil {
		w.t.Fatal(err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	c := &mysqlConn{cfg: cfg, state: "RUNNING"}
	w.conn[mysqlConnector] = c
	w.startTask(mysqlConnector)
	if c.task != "RUNNING" {
		w.t.Fatalf("seed: %s task %s: %s", mysqlConnector, c.task, c.trace)
	}
	return w
}

func (w *mysqlWorld) stack() (*stack, *bytes.Buffer) {
	w.t.Helper()
	srv := httptest.NewServer(w)
	w.t.Cleanup(srv.Close)
	root, err := repoRoot()
	if err != nil {
		w.t.Fatal(err)
	}
	var logs bytes.Buffer
	return &stack{
		root:     root,
		cfg:      config.Config{ConnectURL: srv.URL, KafkaBrokers: strings.Join(w.addrs, ",")},
		http:     &http.Client{Timeout: 5 * time.Second},
		log:      slog.New(slog.NewTextHandler(&logs, nil)),
		execInFn: w.execIn,
		poll:     5 * time.Millisecond,
	}, &logs
}

func (w *mysqlWorld) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	w.t.Cleanup(cancel)
	return ctx
}

func (w *mysqlWorld) with(f func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f()
}

func (c *mysqlConn) str(key string) string {
	v, _ := c.cfg[key].(string)
	return v
}

func (c *mysqlConn) running() bool { return c.state == "RUNNING" && c.task == "RUNNING" }

func (w *mysqlWorld) topicExists(topic string) bool {
	w.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A fresh client: kgo caches metadata answers, so a long-lived one would
	// report a topic created or deleted a moment ago wrongly.
	cl, err := kgo.NewClient(kgo.SeedBrokers(w.addrs...))
	if err != nil {
		w.t.Errorf("kafka client: %v", err)
		return false
	}
	defer cl.Close()
	td, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		w.t.Errorf("list %s: %v", topic, err)
		return false
	}
	return td.Has(topic)
}

func (w *mysqlWorld) createTopic(topic string) {
	w.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := kadm.NewClient(w.kcl).CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		w.t.Errorf("create %s: %v", topic, err)
	}
}

func (w *mysqlWorld) produce(topic string) {
	w.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.kcl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("change")}).FirstErr(); err != nil {
		w.t.Errorf("produce %s: %v", topic, err)
	}
}

// startTask is a task start: Debezium reads its offset, history and grants.
// Caller holds w.mu.
func (w *mysqlWorld) startTask(name string) {
	c := w.conn[name]
	c.trace = ""
	history := c.str("schema.history.internal.kafka.topic")
	mode := c.str("snapshot.mode")
	if c.offset == nil {
		if !w.replGrant && c.str("database.user") == "debezium" {
			w.fail(c, "io.debezium.DebeziumException: Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s) for this operation")
			return
		}
		if !w.topicExists(history) {
			w.createTopic(history)
		}
		c.historyBuilt++
		if mode != "no_data" && c.str("topic.prefix") == "shop" {
			w.produce(mysqlOrdersTopic) // the snapshot
		}
		c.offset = &mysqlPos{file: w.last, gtid: w.gtidExec}
	} else {
		if !w.topicExists(history) {
			if mode != "recovery" {
				w.fail(c, "io.debezium.DebeziumException: The db history topic is missing. You may attempt to recover it by reconfiguring the connector to recovery")
				return
			}
			w.createTopic(history)
			c.historyBuilt++
		}
		if c.offset.file < w.first || c.offset.gtid < w.gtidPurged {
			w.fail(c, "io.debezium.DebeziumException: The connector is trying to read binlog starting at binlog."+fmt.Sprintf("%06d", c.offset.file)+", but this is no longer available on the server.")
			return
		}
		if !w.replGrant && c.str("database.user") == "debezium" {
			w.fail(c, "io.debezium.DebeziumException: Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s) for this operation")
			return
		}
	}
	c.task = "RUNNING"
	for other, o := range w.conn {
		if other != name && o.running() && o.str("database.server.id") == c.str("database.server.id") {
			w.zombies++
		}
	}
}

func (w *mysqlWorld) fail(c *mysqlConn, trace string) { c.task, c.trace = "FAILED", trace }

// write is one committed transaction: a GTID, and a change event on the
// orders topic when orders-mysql streams.
func (w *mysqlWorld) write() {
	w.gtidExec++
	if c := w.conn[mysqlConnector]; c != nil && c.running() {
		w.produce(mysqlOrdersTopic)
		c.offset = &mysqlPos{file: w.last, gtid: w.gtidExec}
	}
}

func (w *mysqlWorld) execIn(_ context.Context, container string, argv ...string) (string, int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if container != mysqlContainer || len(argv) != 5 || argv[0] != "bash" || argv[1] != "-c" || argv[2] != scriptMySQLQuery || argv[3] != "_" {
		w.t.Errorf("unexpected docker exec %s %q", container, argv)
		return "", 1, nil
	}
	if !w.up {
		return "Error response from daemon: No such container: " + mysqlContainer, 1, nil
	}
	q := argv[4]
	w.stmts = append(w.stmts, q)
	switch {
	case q == sqlMySQLPing:
		return "1", 0, nil
	case q == sqlMySQLInsertMarker:
		w.write()
	case q == sqlMySQLWriteBatch:
		w.write()
	case q == sqlMySQLFlushLogs:
		w.last++
	case q == sqlMySQLShowBinlogs:
		var b strings.Builder
		for i := w.first; i <= w.last; i++ {
			fmt.Fprintf(&b, "binlog.%06d\t%d\tNo\n", i, 1000+i)
		}
		return b.String(), 0, nil
	case strings.HasPrefix(q, "PURGE BINARY LOGS TO '"):
		name := strings.TrimSuffix(strings.TrimPrefix(q, "PURGE BINARY LOGS TO '"), "'")
		n, err := strconv.Atoi(strings.TrimPrefix(name, "binlog."))
		if err != nil || n < w.first || n > w.last {
			return "ERROR 1373 (HY000): Target log not found in binlog index", 1, nil
		}
		w.first = n
		// Every transaction so far is in a purged file: the newest one was
		// just opened by FLUSH.
		w.gtidPurged = w.gtidExec
	case q == sqlMySQLGTIDPurged:
		if w.gtidPurged == 0 {
			return "", 0, nil
		}
		return fmt.Sprintf("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-%d", w.gtidPurged), 0, nil
	case q == sqlMySQLReplGrant:
		if w.replGrant {
			return "Y", 0, nil
		}
		return "N", 0, nil
	case q == sqlMySQLRevokeRepl:
		w.replGrant = false
	case q == sqlMySQLGrantRepl:
		w.replGrant = true
	case q == sqlMySQLZombies:
		return strconv.Itoa(w.zombies), 0, nil
	default:
		w.t.Errorf("fake mysql: unexpected SQL: %s", q)
		return "ERROR: unexpected", 1, nil
	}
	return "", 0, nil
}

// ServeHTTP plays Kafka Connect.
func (w *mysqlWorld) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Path == "/connectors" {
		names := []string{}
		for n := range w.conn {
			names = append(names, n)
		}
		b, _ := json.Marshal(names)
		writeJSON(rw, http.StatusOK, string(b))
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/connectors/")
	if !ok {
		writeJSON(rw, http.StatusNotFound, `{"error_code":404}`)
		return
	}
	name, sub, _ := strings.Cut(rest, "/")
	c := w.conn[name]
	if c == nil && !(r.Method == http.MethodPut && sub == "config") {
		writeJSON(rw, http.StatusNotFound, fmt.Sprintf(`{"error_code":404,"message":"Connector %s not found"}`, name))
		return
	}
	switch {
	case r.Method == http.MethodPut && sub == "config":
		var cfg map[string]any
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &cfg); err != nil {
			writeJSON(rw, http.StatusBadRequest, `{}`)
			return
		}
		if c == nil {
			c = &mysqlConn{cfg: cfg, state: "RUNNING"}
			w.conn[name] = c
			w.startTask(name)
			writeJSON(rw, http.StatusCreated, `{}`)
			return
		}
		c.cfg = cfg
		// Connect restarts the tasks of a running connector on a new config.
		if c.state == "RUNNING" {
			w.startTask(name)
		}
		writeJSON(rw, http.StatusOK, `{}`)
	case r.Method == http.MethodGet && sub == "status":
		tasks := `[]`
		if c.state == "RUNNING" {
			b, _ := json.Marshal([]map[string]any{{"id": 0, "state": c.task, "trace": c.trace}})
			tasks = string(b)
		}
		writeJSON(rw, http.StatusOK, fmt.Sprintf(`{"connector":{"state":%q},"tasks":%s}`, c.state, tasks))
	case r.Method == http.MethodPost && sub == "tasks/0/restart":
		c.restarts++
		if c.state == "RUNNING" {
			w.startTask(name)
		}
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && sub == "stop":
		c.state, c.task = "STOPPED", ""
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && sub == "resume":
		if c.state != "RUNNING" {
			c.state = "RUNNING"
			w.startTask(name)
		}
		rw.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodGet && sub == "offsets":
		if c.offset == nil {
			writeJSON(rw, http.StatusOK, `{"offsets":[]}`)
			return
		}
		writeJSON(rw, http.StatusOK, fmt.Sprintf(`{"offsets":[{"partition":{"server":%q},"offset":{"file":"binlog.%06d","pos":157,"gtids":"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-%d","ts_sec":1760000000}}]}`, c.str("topic.prefix"), c.offset.file, c.offset.gtid))
	case r.Method == http.MethodDelete && sub == "offsets":
		if c.state != "STOPPED" {
			writeJSON(rw, http.StatusBadRequest, `{"error_code":400,"message":"Connectors must be in the STOPPED state before their offsets can be modified"}`)
			return
		}
		c.offset = nil
		rw.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && sub == "":
		delete(w.conn, name)
		rw.WriteHeader(http.StatusNoContent)
	default:
		writeJSON(rw, http.StatusNotFound, `{"error_code":404}`)
	}
}
