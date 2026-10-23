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

// qWorld plays the dev stack for the Q2f and Q5e scenarios: Postgres (the
// exact SQL the scenarios send), Kafka Connect (httptest, with KIP-875 stop
// and offsets), the detached blocker session and the consumer group.
//
// It is a model, not a recording: a task start reads the connector config the
// way Debezium would. It creates a missing slot (and waits while a transaction
// with an xid is open), snapshots when there is no offset or snapshot.mode is
// always (and fails without SELECT), fails on a stored offset from before its
// slot existed, and with a captured heartbeat table confirms WAL while orders
// is idle. A scenario that sends a statement the model does not know fails
// with "unexpected SQL".
type qWorld struct {
	t     *testing.T
	addrs []string // kfake: the topics a fix deletes

	mu sync.Mutex
	// orders: ids in the source and the ones in analytics.orders
	nextID int64
	loaded map[int64]bool
	ident  string // relreplident of public.orders
	// roles
	roleExists, roleSelect bool
	// sessions
	blockerOpen, slotWaiting bool
	blockerRole              bool // devctl_blocker exists
	// the orders slot
	slotExists, slotActive bool
	retained               int64
	slotStart              uint64
	// idle-wal-growth
	batchDB, fillerTable, heartbeat, heartbeatInPub bool
	// stale-source-offsets
	freshDB, freshSlot bool
	freshRows          int
	// connect
	connState, taskState, trace string
	connCfg                     map[string]any
	offsetLSN                   uint64 // 0 = no stored offsets
	restarts                    int
	ops                         []string
	subjectsDel                 []string
	detached                    [][]string
}

func newQWorld(t *testing.T) *qWorld {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, ordersTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	w := &qWorld{
		t: t, addrs: c.ListenAddrs(), nextID: 1000, loaded: map[int64]bool{}, ident: "f",
		slotExists: true, slotActive: true, retained: 900_000, slotStart: 0x1000000,
		connState: "RUNNING", taskState: "RUNNING", offsetLSN: 0x1200000,
	}
	for id := int64(1); id <= w.nextID; id++ {
		w.loaded[id] = true
	}
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	base := &stack{root: root}
	b, err := base.connectorConfigWith(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &w.connCfg); err != nil {
		t.Fatal(err)
	}
	return w
}

func (w *qWorld) stack() (*stack, *bytes.Buffer) {
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
		cfg:      config.Config{ConnectURL: srv.URL, SchemaRegistryURL: srv.URL, KafkaBrokers: strings.Join(w.addrs, ",")},
		http:     &http.Client{Timeout: 5 * time.Second},
		log:      slog.New(slog.NewTextHandler(&logs, nil)),
		sqlFn:    w.psql,
		groupFn:  w.group,
		detachFn: w.detach,
		poll:     5 * time.Millisecond,
	}, &logs
}

func (w *qWorld) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	w.t.Cleanup(cancel)
	return ctx
}

// createTopics makes topics exist on the fake broker, as Debezium would.
func (w *qWorld) createTopics(topics ...string) {
	w.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(w.addrs...))
	if err != nil {
		w.t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopics(context.Background(), 1, 1, nil, topics...); err != nil {
		w.t.Fatalf("create %v: %v", topics, err)
	}
}

// with runs f under the world's lock.
func (w *qWorld) with(f func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f()
}

func (w *qWorld) cfg(key string) string {
	v, _ := w.connCfg[key].(string)
	return v
}

func (w *qWorld) streaming() bool {
	return w.connState == "RUNNING" && w.taskState == "RUNNING" && w.slotActive
}

func (w *qWorld) heartbeatOn() bool {
	return w.streaming() && w.heartbeat && w.heartbeatInPub &&
		strings.Contains(w.cfg("table.include.list"), heartbeatTable) && w.cfg("heartbeat.action.query") != ""
}

// startTask is a task start: Debezium reads the config. Caller holds w.mu.
func (w *qWorld) startTask() {
	w.trace = ""
	if w.cfg("database.dbname") == freshDB {
		if !w.freshDB {
			w.fail("FATAL: database \"devctl_orders_v2\" does not exist")
			return
		}
		w.freshSlot = true
		// The new slot starts at the current WAL position, past the offset
		// saved against the old database.
		if w.offsetLSN != 0 {
			w.fail("io.debezium.DebeziumException: Saved offset is before replication slot's confirmed lsn")
			return
		}
		w.taskState = "RUNNING"
		return
	}
	if !w.slotExists {
		if w.blockerOpen {
			// CREATE_REPLICATION_SLOT waits; Connect has not seen start() return.
			w.slotWaiting, w.taskState = true, "UNASSIGNED"
			return
		}
		w.createSlot()
	}
	if w.offsetLSN != 0 && w.offsetLSN < w.slotStart {
		w.fail("io.debezium.DebeziumException: Saved offset is before replication slot's confirmed lsn")
		return
	}
	snapshot := w.offsetLSN == 0 || w.cfg("snapshot.mode") == "always"
	if snapshot && w.cfg("database.user") == cdcRole && !w.roleSelect {
		w.fail("org.apache.kafka.connect.errors.ConnectException: ERROR: permission denied for table orders\n\tat io.debezium...")
		return
	}
	w.taskState, w.slotActive = "RUNNING", true
	if snapshot {
		for id := int64(1); id <= w.nextID; id++ {
			w.loaded[id] = true
		}
	}
	w.offsetLSN = w.slotStart + 0x100
}

func (w *qWorld) createSlot() {
	w.slotExists, w.slotActive, w.retained = true, true, 0
	w.slotStart += 0x1000000
}

func (w *qWorld) fail(trace string) {
	w.taskState, w.trace, w.slotActive = "FAILED", trace, false
}

func (w *qWorld) detach(_ context.Context, name string, args ...string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.detached = append(w.detached, append([]string{name}, args...))
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "PGAPPNAME="+blockerApp) && strings.Contains(joined, "pg_current_xact_id()") {
		if !strings.Contains(joined, "-U "+blockerRole+" ") || !w.blockerRole {
			return fmt.Errorf("fake docker: the blocker must log in as the existing role %s: %s", blockerRole, joined)
		}
		w.blockerOpen = true
	}
	return nil
}

func (w *qWorld) group(_ context.Context, _ string) (groupView, error) {
	return groupView{state: "Stable", members: 1}, nil
}

func (w *qWorld) psql(_ context.Context, db string, stmts ...string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out string
	for _, q := range stmts {
		res, err := w.exec(db, q)
		if err != nil {
			return "", err
		}
		out = res
	}
	return out, nil
}

func (w *qWorld) exec(db, q string) (string, error) {
	tf := func(b bool) string {
		if b {
			return "t"
		}
		return "f"
	}
	switch {
	// shared
	case q == sqlSlotRow && db == "orders":
		if !w.slotExists {
			return "", nil
		}
		if w.heartbeatOn() {
			w.retained = min(w.retained, 400_000)
		}
		return fmt.Sprintf("%t|%d", w.slotActive, w.retained), nil
	case strings.HasPrefix(q, "SELECT active FROM pg_replication_slots"):
		return tf(w.slotExists && w.slotActive), nil
	case strings.HasPrefix(q, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)"):
		return strconv.FormatInt(w.retained, 10), nil
	case q == sqlInsertMarker && db == "orders":
		w.nextID++
		if w.streaming() {
			w.loaded[w.nextID] = true
		}
		return strconv.FormatInt(w.nextID, 10), nil
	case strings.HasPrefix(q, sqlWarehouseHasOrder) && db == "warehouse":
		id, _ := strconv.ParseInt(strings.TrimPrefix(q, sqlWarehouseHasOrder), 10, 64)
		return boolCount(w.loaded[id]), nil
	case q == sqlDropSlot:
		if !w.slotExists || w.slotActive {
			return "", fmt.Errorf("psql: replication slot %q does not exist or is active", slotName)
		}
		w.ops = append(w.ops, "drop-slot")
		w.slotExists, w.retained = false, 0
	// no-replica-identity
	case q == sqlReplicaIdentity:
		return w.ident, nil
	case q == sqlReplicaNothing:
		w.ident = "n"
	case q == sqlReplicaFull:
		w.ident = "f"
	case q == sqlAppUpdate:
		if w.ident == "n" {
			return "", fmt.Errorf("devctl: psql: exit status 1: ERROR:  cannot update table \"orders\" because it does not have a replica identity and publishes updates")
		}
	// missing-grant
	case q == sqlCDCRoleExists:
		return boolCount(w.roleExists), nil
	case q == sqlCreateCDCRole:
		if w.roleExists {
			return "", fmt.Errorf("psql: role %q already exists", cdcRole)
		}
		w.roleExists = true
	case q == sqlCDCConnect, q == sqlCDCUsage:
		if !w.roleExists {
			return "", fmt.Errorf("psql: role %q does not exist", cdcRole)
		}
	case q == sqlCDCCanSelect:
		return tf(w.roleSelect), nil
	case q == sqlGrantCDCSelect:
		w.roleSelect = true
	case q == sqlDropCDCRole:
		if w.cfg("database.user") == cdcRole && w.connState == "RUNNING" {
			return "", fmt.Errorf("fake postgres: role %s dropped while the connector still uses it", cdcRole)
		}
		w.roleExists, w.roleSelect = false, false
	// blocked-slot
	case q == sqlBlockerOpen, q == sqlBlockerSessions:
		return boolCount(w.blockerOpen), nil
	case q == sqlCreateBlockerRole:
		w.blockerRole = true
	case q == sqlBlockerConnect:
		if !w.blockerRole {
			return "", fmt.Errorf("psql: role %q does not exist", blockerRole)
		}
	case q == sqlDropBlockerRole:
		if w.blockerOpen {
			return "", fmt.Errorf("fake postgres: role %s dropped while its session is open", blockerRole)
		}
		w.blockerRole = false
	case q == sqlSlotCreateWaits:
		return boolCount(w.slotWaiting), nil
	case q == sqlEndBlocker:
		n := boolCount(w.blockerOpen)
		w.blockerOpen = false
		if w.slotWaiting {
			w.slotWaiting = false
			w.createSlot()
			w.taskState = "RUNNING"
			for id := int64(1); id <= w.nextID; id++ {
				w.loaded[id] = true
			}
			w.offsetLSN = w.slotStart + 0x100
		}
		return n, nil
	// idle-wal-growth
	case q == sqlBatchDBExists:
		return boolCount(w.batchDB), nil
	case q == sqlCreateBatchDB:
		w.batchDB = true
	case q == sqlCreateFiller && db == batchDB:
		if !w.batchDB {
			return "", fmt.Errorf("psql: database %q does not exist", batchDB)
		}
		w.fillerTable = true
	case q == sqlFillWAL && db == batchDB:
		if !w.fillerTable {
			return "", fmt.Errorf("psql: relation devctl_wal_filler does not exist")
		}
		// Another database: decoded by nobody, confirmed by nobody.
		if w.slotExists {
			w.retained += 20 << 20
		}
	case q == sqlDropBatchDB:
		w.batchDB, w.fillerTable = false, false
	case q == sqlCreateHeartbeat:
		w.heartbeat = true
	case q == sqlHeartbeatExists:
		return tf(w.heartbeat), nil
	case q == sqlHeartbeatInPub:
		return boolCount(w.heartbeatInPub), nil
	case q == sqlPubAddHeartbeat:
		if !w.heartbeat || w.heartbeatInPub {
			return "", fmt.Errorf("psql: cannot add %s to the publication", heartbeatTable)
		}
		w.ops = append(w.ops, "pub-add-heartbeat")
		w.heartbeatInPub = true
	case q == sqlDropHeartbeat:
		if w.cfg("heartbeat.action.query") != "" && w.connState == "RUNNING" {
			return "", fmt.Errorf("fake postgres: heartbeat table dropped while the connector still writes it")
		}
		w.heartbeat, w.heartbeatInPub = false, false
	// stale-source-offsets
	case q == sqlFreshDBExists:
		return boolCount(w.freshDB), nil
	case q == sqlCreateFreshDB:
		if w.freshDB {
			return "", fmt.Errorf("psql: database %q already exists", freshDB)
		}
		w.freshDB = true
	case db == freshDB && (q == sqlFreshOrders || q == sqlFreshReplicaFull):
		if !w.freshDB {
			return "", fmt.Errorf("psql: database %q does not exist", freshDB)
		}
	case db == freshDB && q == sqlFreshRows:
		w.freshRows += 10
	case q == sqlDropFreshSlot:
		if w.freshSlot && w.connState == "RUNNING" && w.cfg("slot.name") == freshSlot {
			return "0", nil // active: not dropped
		}
		n := boolCount(w.freshSlot)
		w.freshSlot = false
		return n, nil
	case q == sqlDropFreshDB:
		if w.freshSlot {
			return "", fmt.Errorf("psql: database %q is used by an active logical replication slot", freshDB)
		}
		w.freshDB, w.freshRows = false, 0
	default:
		return "", fmt.Errorf("fake postgres: unexpected SQL on %s: %s", db, q)
	}
	return "", nil
}

// ServeHTTP plays Kafka Connect and Schema Registry.
func (w *qWorld) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	defer w.mu.Unlock()
	base := "/connectors/" + connectorName
	switch {
	case r.Method == http.MethodGet && r.URL.Path == base+"/status":
		tasks := `[]`
		if w.connState == "RUNNING" {
			b, _ := json.Marshal([]map[string]any{{"id": 0, "state": w.taskState, "trace": w.trace}})
			tasks = string(b)
		}
		writeJSON(rw, http.StatusOK, fmt.Sprintf(`{"connector":{"state":%q},"tasks":%s}`, w.connState, tasks))
	case r.Method == http.MethodPut && r.URL.Path == base+"/config":
		var cfg map[string]any
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &cfg); err != nil {
			writeJSON(rw, http.StatusBadRequest, `{}`)
			return
		}
		w.ops = append(w.ops, "config")
		w.connCfg = cfg
		writeJSON(rw, http.StatusOK, `{}`)
	case r.Method == http.MethodPost && r.URL.Path == base+"/tasks/0/restart":
		w.restarts++
		if w.connState == "RUNNING" {
			w.startTask()
		}
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && r.URL.Path == base+"/stop":
		w.ops = append(w.ops, "stop")
		w.connState, w.taskState, w.slotActive, w.slotWaiting = "STOPPED", "", false, false
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && r.URL.Path == base+"/resume":
		w.ops = append(w.ops, "resume")
		w.connState = "RUNNING"
		w.startTask()
		rw.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodGet && r.URL.Path == base+"/offsets":
		if w.offsetLSN == 0 {
			writeJSON(rw, http.StatusOK, `{"offsets":[]}`)
			return
		}
		writeJSON(rw, http.StatusOK, fmt.Sprintf(`{"offsets":[{"partition":{"server":"pg"},"offset":{"lsn":%d,"txId":771,"ts_usec":1760000000000000}}]}`, w.offsetLSN))
	case r.Method == http.MethodDelete && r.URL.Path == base+"/offsets":
		if w.connState != "STOPPED" {
			writeJSON(rw, http.StatusBadRequest, `{"error_code":400,"message":"Connectors must be in the STOPPED state before their offsets can be modified"}`)
			return
		}
		w.ops = append(w.ops, "delete-offsets")
		w.offsetLSN = 0
		rw.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/subjects/"):
		w.subjectsDel = append(w.subjectsDel, r.URL.Path)
		writeJSON(rw, http.StatusNotFound, `{"error_code":40401}`)
	default:
		writeJSON(rw, http.StatusNotFound, `{"error_code":404}`)
	}
}
