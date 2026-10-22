package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/config"
)

// p3World plays the dev stack for the P3 scenarios: Postgres (the exact SQL
// the scenarios send), docker compose (devconsumer builds), Kafka Connect and
// Schema Registry (httptest), and the consumer group. Kafka itself is kfake,
// franz-go's in-process broker, so topic creation, offsets, reads, produces
// and the offset reset run over the real wire protocol.
//
// The fake is a model of the pipeline, not a recording: Debezium streams a
// table's changes only while the table is in the publication and the
// connector runs; devconsumer loads, skips or rejects records as its build
// says. A scenario that sends the wrong statement fails with "unexpected SQL".
type p3World struct {
	t     *testing.T
	addrs []string
	raw   *kgo.Client

	mu sync.Mutex
	// postgres: orders
	nextID int64
	loaded map[int64]bool // ids in analytics.orders
	// missed-table
	refundsExists, signalExists, refundsInPub bool
	refundRows                                int
	signals                                   int
	// dead-slot
	slotExists, slotActive bool
	retained               int64
	touched                []string
	// Debezium's offset storage: the WAL position task 0 last committed, and
	// the current slot's consistent point. Recreating the slot moves its start
	// past the saved offset, and a resume with that offset fails the task the
	// way the live stack did (KAN-411). offsetLSN 0 means no stored offsets.
	offsetLSN, slotStartLSN uint64
	taskFailed              bool
	ops                     []string
	// dlq-backlog
	sourceGift int
	// connect
	connState   string // RUNNING, STOPPED
	connCfg     map[string]any
	restarts    int
	subjectsDel []string
	// offsetDeleteStatus is the DELETE /offsets answer when non-zero; the
	// error-path tests use it to make the Connect call fail.
	offsetDeleteStatus int
	// devconsumer
	consumerUp   bool
	skipOps      string
	rejectStatus string
	lag          int64
	resetSeen    bool // the group's committed offsets were at the topic start when a good build started
	// X1g silent scenarios
	skipWhere, dropColumns    string // the running build's knobs
	backfillSrc, backfillSink int
	nullTS                    int  // analytics.orders rows with updated_at NULL
	sinkEmpty                 bool // analytics.orders has no updated_at yet
	riFull                    bool // public.orders REPLICA IDENTITY FULL
	placeholders              int  // analytics.orders rows holding the TOAST placeholder
	sql                       []string
}

func newP3World(t *testing.T) *p3World {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, ordersTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	raw, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	t.Cleanup(raw.Close)
	return &p3World{
		t: t, addrs: c.ListenAddrs(), raw: raw,
		nextID: 1000, loaded: map[int64]bool{},
		slotExists: true, slotActive: true, retained: 26000,
		offsetLSN: 0x1000000, slotStartLSN: 0x1000000,
		connState: "RUNNING", consumerUp: true, riFull: true,
	}
}

// stack returns a stack wired to the world, and its log.
func (w *p3World) stack() (*stack, *bytes.Buffer) {
	w.t.Helper()
	srv := httptest.NewServer(w)
	w.t.Cleanup(srv.Close)
	root, err := repoRoot()
	if err != nil {
		w.t.Fatal(err)
	}
	var logs bytes.Buffer
	return &stack{
		root:      root,
		cfg:       config.Config{ConnectURL: srv.URL, SchemaRegistryURL: srv.URL, KafkaBrokers: strings.Join(w.addrs, ",")},
		http:      &http.Client{Timeout: 5 * time.Second},
		log:       slog.New(slog.NewTextHandler(&logs, nil)),
		sqlFn:     w.psql,
		composeFn: w.compose,
		groupFn:   w.group,
		poll:      10 * time.Millisecond,
	}, &logs
}

func (w *p3World) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	w.t.Cleanup(cancel)
	return ctx
}

// produce writes n records to topic (created first), as Debezium or
// devconsumer would.
func (w *p3World) produce(topic string, recs ...*kgo.Record) {
	w.t.Helper()
	if _, err := kadm.NewClient(w.raw).CreateTopic(context.Background(), 1, 1, nil, topic); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		w.t.Fatalf("create %s: %v", topic, err)
	}
	for _, r := range recs {
		r.Topic = topic
	}
	if err := w.raw.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		w.t.Fatalf("produce %s: %v", topic, err)
	}
}

func (w *p3World) streaming() bool {
	return w.connState == "RUNNING" && w.slotExists && !w.taskFailed
}

func (w *p3World) psql(_ context.Context, db string, stmts ...string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out string
	for _, q := range stmts {
		w.sql = append(w.sql, q)
		res, err := w.exec(db, q)
		if err != nil {
			return "", err
		}
		out = res
	}
	return out, nil
}

// exec answers one statement. Caller holds w.mu.
func (w *p3World) exec(db, q string) (string, error) {
	switch {
	// missed-table
	case q == sqlRefundsInPub:
		return boolCount(w.refundsInPub), nil
	case q == sqlCreateSignal:
		w.signalExists = true
	case q == sqlCreateRefunds:
		w.refundsExists = true
	case q == sqlPubWithoutRefunds:
		w.refundsInPub = false
	case q == sqlInsertRefunds:
		w.refundRows += refundRows
		if w.refundsInPub && w.streaming() {
			w.produceRefunds(refundRows)
		}
	case q == sqlInsertMarker:
		w.nextID++
		if w.streaming() && w.consumerUp {
			w.loaded[w.nextID] = true
		}
		return strconv.FormatInt(w.nextID, 10), nil
	case strings.HasPrefix(q, sqlWarehouseHasOrder) && db == "warehouse":
		id, _ := strconv.ParseInt(strings.TrimPrefix(q, sqlWarehouseHasOrder), 10, 64)
		return boolCount(w.loaded[id]), nil
	case q == sqlRefundsExists:
		if w.refundsExists {
			return "t", nil
		}
		return "f", nil
	case q == sqlPubAddRefunds:
		if w.refundsInPub {
			return "", fmt.Errorf("psql: relation \"devctl_refunds\" is already member of publication")
		}
		w.refundsInPub = true
	case q == sqlCountRefunds:
		return strconv.Itoa(w.refundRows), nil
	case q == sqlSnapshotSignal:
		w.signals++
		if w.refundsInPub && w.streaming() {
			w.produceRefunds(w.refundRows)
		}
	case q == sqlPubBaseline:
		w.refundsInPub = false
	case q == sqlDropScenarioTables:
		w.refundsExists, w.signalExists, w.refundRows = false, false, 0
	// dead-slot and slot-inactive
	case q == sqlSlotRow:
		if !w.slotExists {
			return "", nil
		}
		return fmt.Sprintf("%t|%d", w.slotActive, w.retained), nil
	case strings.HasPrefix(q, "SELECT active FROM pg_replication_slots"):
		if w.slotExists && w.slotActive {
			return "t", nil
		}
		return "f", nil
	case strings.HasPrefix(q, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)"):
		return strconv.FormatInt(w.retained, 10), nil
	case strings.HasPrefix(q, "INSERT INTO orders (customer_id, amount) SELECT g, 100 + g"):
		w.nextID += 100
		if !w.slotActive {
			w.retained += 40000
		}
	case q == sqlDropSlot:
		if !w.slotExists {
			return "", fmt.Errorf("psql: replication slot %q does not exist", slotName)
		}
		if w.slotActive {
			return "", fmt.Errorf("psql: replication slot %q is active", slotName)
		}
		w.ops = append(w.ops, "drop-slot")
		w.slotExists, w.retained = false, 0
	case q == sqlCreateSlot:
		if w.slotExists {
			return "", fmt.Errorf("psql: replication slot %q already exists", slotName)
		}
		w.ops = append(w.ops, "create-slot")
		w.slotExists, w.slotActive, w.retained = true, false, 0
		// A new slot starts at the current WAL position, after the offset
		// Debezium saved on the slot that was dropped.
		w.slotStartLSN += 0x1000000
		return slotName, nil
	case q == sqlSlotStart:
		return fmt.Sprintf("0/%X", w.slotStartLSN), nil
	case strings.HasPrefix(q, "SELECT coalesce(round(extract(epoch FROM max(updated_at))") && db == "warehouse":
		if w.sinkEmpty {
			return "0", nil
		}
		return "1760000000000000", nil
	case strings.HasPrefix(q, sqlTouchPending):
		w.touched = append(w.touched, strings.TrimPrefix(q, sqlTouchPending))
	// consumer-bad-deploy
	case q == sqlInsertBadDeploy:
		lo := w.nextID + 1
		w.nextID += badDeployOrders
		if w.consumerUp && !strings.Contains(w.skipOps, "c") {
			for id := lo; id <= w.nextID; id++ {
				w.loaded[id] = true
			}
		}
		return fmt.Sprintf("%d %d", lo, w.nextID), nil
	case strings.HasPrefix(q, sqlWarehouseIDs) && db == "warehouse":
		var lo, hi int64
		if _, err := fmt.Sscanf(strings.TrimPrefix(q, sqlWarehouseIDs), "%d AND %d", &lo, &hi); err != nil {
			return "", fmt.Errorf("fake: %q: %w", q, err)
		}
		n := 0
		for id := lo; id <= hi; id++ {
			if w.loaded[id] {
				n++
			}
		}
		return strconv.Itoa(n), nil
	case q == sqlCountSourceOrders:
		return strconv.FormatInt(w.nextID, 10), nil
	case q == sqlCountWarehouse && db == "warehouse":
		return strconv.Itoa(len(w.loaded)), nil
	// X1g: sink-filtered-rows
	case q == sqlInsertBackfill:
		w.nextID += backfillOrders
		w.backfillSrc += backfillOrders
		if w.consumerUp && w.skipWhere != "status="+backfillStatus {
			w.backfillSink += backfillOrders
		}
	case q == sqlSourceBackfill:
		return strconv.Itoa(w.backfillSrc), nil
	case q == sqlSinkBackfill && db == "warehouse":
		return strconv.Itoa(w.backfillSink), nil
	// X1g: sink-dropped-column
	case strings.HasPrefix(q, sqlPinnedStart):
		w.nextID += droppedOrders
		if !strings.Contains(q, "1760000000000000 * interval") {
			return "", fmt.Errorf("fake postgres: pinned insert is not at the sink's newest updated_at: %s", q)
		}
		if w.consumerUp && strings.Contains(w.dropColumns, droppedColumn) {
			w.nullTS += droppedOrders
		}
	case q == sqlSinkNullTS && db == "warehouse":
		return strconv.Itoa(w.nullTS), nil
	// X1g: consumer-duplicates
	case q == sqlInsertDupSource:
		recs := make([]*kgo.Record, dupSourceOrders)
		for i := range recs {
			w.nextID++
			recs[i] = &kgo.Record{Key: []byte(fmt.Sprintf("key-%d", w.nextID)), Value: []byte(fmt.Sprintf("event-%d", w.nextID))}
		}
		w.produce(ordersTopic, recs...)
	// X1g: toast-placeholder
	case q == sqlReplicaDefault:
		w.riFull = false
	case q == sqlReplicaFull:
		w.riFull = true
	case q == sqlInsertToast:
		lo := w.nextID + 1
		w.nextID += toastOrders
		if w.consumerUp {
			// The insert event carries the full value: the sink loads it.
			for id := lo; id <= w.nextID; id++ {
				w.loaded[id] = true
			}
		}
		return fmt.Sprintf("%d %d", lo, w.nextID), nil
	case strings.HasPrefix(q, "UPDATE orders SET amount = amount + 1, updated_at = now() WHERE id BETWEEN "):
		if !w.riFull {
			w.placeholders += toastOrders
		}
	case q == sqlSinkPlaceholders && db == "warehouse":
		return strconv.Itoa(w.placeholders), nil
	case q == sqlSinkPlaceholderIDs && db == "warehouse":
		ids := make([]string, w.placeholders)
		for i := range ids {
			ids[i] = strconv.Itoa(2000 + i)
		}
		return strings.Join(ids, ","), nil
	case strings.HasPrefix(q, "UPDATE orders SET status = status || '', updated_at = now() WHERE id IN ("):
		w.placeholders = 0
	// dlq-backlog
	case q == sqlInsertGift:
		w.sourceGift += giftOrders
		if w.consumerUp && w.rejectStatus == rejectStatus {
			recs := make([]*kgo.Record, giftOrders)
			for i := range recs {
				recs[i] = &kgo.Record{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf("gift-%d", i)), Headers: []kgo.RecordHeader{
					{Key: "trace-id", Value: []byte("t1")},
					{Key: "__connect.errors.exception.message", Value: []byte(`unknown status "gift"`)},
				}}
			}
			w.produce(dlqTopic, recs...)
		}
	case q == sqlSourceGift:
		return strconv.Itoa(w.sourceGift), nil
	case q == sqlWarehouseGift && db == "warehouse":
		// The good build loads what was replayed into the source topic.
		if !w.consumerUp || w.rejectStatus != "" {
			return "0", nil
		}
		return strconv.Itoa(w.replayedGift()), nil
	default:
		return "", fmt.Errorf("fake postgres: unexpected SQL on %s: %s", db, q)
	}
	return "", nil
}

func (w *p3World) produceRefunds(n int) {
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Key: []byte(strconv.Itoa(i)), Value: []byte("refund")}
	}
	w.produce(refundsTopic, recs...)
}

// replayedGift counts the gift records in the orders topic.
func (w *p3World) replayedGift() int {
	recs, err := w.records(ordersTopic)
	if err != nil {
		w.t.Errorf("read %s: %v", ordersTopic, err)
		return 0
	}
	n := 0
	for _, r := range recs {
		if strings.HasPrefix(string(r.Value), "gift-") {
			n++
		}
	}
	return n
}

// records reads a topic through the code under test's own reader.
func (w *p3World) records(topic string) ([]*kgo.Record, error) {
	s := &stack{cfg: config.Config{KafkaBrokers: strings.Join(w.addrs, ",")}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.readTopic(ctx, topic)
}

func boolCount(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func (w *p3World) compose(_ context.Context, env map[string]string, args ...string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch strings.Join(args, " ") {
	case "up -d --no-deps devconsumer":
		w.consumerUp = true
		w.skipOps = env["DEVCONSUMER_SKIP_OPS"]
		w.rejectStatus = env["DEVCONSUMER_REJECT_STATUS"]
		w.skipWhere = env["DEVCONSUMER_SKIP_WHERE"]
		w.dropColumns = env["DEVCONSUMER_DROP_COLUMNS"]
		if w.atStart() {
			// A good build re-reads the topic from the reset offsets: the
			// upsert loads what the filter skipped and fills the NULLs.
			if w.skipWhere == "" {
				w.backfillSink = w.backfillSrc
			}
			if w.dropColumns == "" {
				w.nullTS = 0
			}
		}
		if w.skipOps == "" && w.atStart() {
			// The good build re-reads the topic from the reset offsets and
			// loads every order the bad build skipped.
			w.resetSeen = true
			for id := int64(1001); id <= w.nextID; id++ {
				w.loaded[id] = true
			}
		}
	case "stop devconsumer":
		w.consumerUp = false
	default:
		return fmt.Errorf("fake compose: unexpected %q", strings.Join(args, " "))
	}
	return nil
}

// atStart reports whether order-sync's committed offsets on the orders topic
// are at the topic's start, i.e. a reset happened. Caller holds w.mu.
func (w *p3World) atStart() bool {
	adm := kadm.NewClient(w.raw)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fetched, err := adm.FetchOffsets(ctx, ordersGroup)
	if err != nil || fetched.Error() != nil {
		return false
	}
	starts, err := adm.ListStartOffsets(ctx, ordersTopic)
	if err != nil {
		return false
	}
	n := 0
	ok := true
	starts.Each(func(lo kadm.ListedOffset) {
		c, has := fetched.Lookup(ordersTopic, lo.Partition)
		n++
		ok = ok && has && c.At == lo.Offset
	})
	return ok && n > 0
}

func (w *p3World) group(_ context.Context, _ string) (groupView, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.consumerUp {
		return groupView{state: "Stable", members: 1, lag: w.lag}, nil
	}
	return groupView{state: "Empty", lag: w.lag}, nil
}

// ServeHTTP plays Kafka Connect and Schema Registry.
func (w *p3World) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	defer w.mu.Unlock()
	base := "/connectors/" + connectorName
	switch {
	case r.Method == http.MethodGet && r.URL.Path == base+"/status":
		tasks := `[]`
		if w.connState == "RUNNING" {
			task := "RUNNING"
			if w.taskFailed {
				task = "FAILED"
			}
			tasks = fmt.Sprintf(`[{"id":0,"state":%q}]`, task)
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
		if w.connState == "RUNNING" && !w.slotExists {
			w.slotExists, w.slotActive = true, true
			w.slotStartLSN += 0x1000000
		}
		w.taskFailed = w.offsetLSN != 0 && w.offsetLSN < w.slotStartLSN
		w.slotActive = !w.taskFailed
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && r.URL.Path == base+"/stop":
		w.ops = append(w.ops, "stop")
		w.connState, w.slotActive = "STOPPED", false
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && r.URL.Path == base+"/resume":
		w.ops = append(w.ops, "resume")
		w.connState = "RUNNING"
		if !w.slotExists {
			// Debezium creates a missing slot when the task starts, at the
			// current WAL position — after any offset saved before.
			w.slotExists, w.retained = true, 0
			w.slotStartLSN += 0x1000000
		}
		// A stored offset older than the slot's start cannot be replayed on
		// it: the task fails with "change stream … is no longer available".
		w.taskFailed = w.offsetLSN != 0 && w.offsetLSN < w.slotStartLSN
		w.slotActive = !w.taskFailed
		if !w.taskFailed && w.consumerUp {
			// No offset: Debezium snapshots everything the warehouse lacks.
			for id := int64(1001); id <= w.nextID; id++ {
				w.loaded[id] = true
			}
		}
		rw.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodDelete && r.URL.Path == base+"/offsets":
		if w.offsetDeleteStatus != 0 {
			writeJSON(rw, w.offsetDeleteStatus, `{"error_code":500}`)
			return
		}
		// Connect only deletes offsets while the connector is STOPPED.
		if w.connState != "STOPPED" {
			writeJSON(rw, http.StatusConflict, `{"error_code":400}`)
			return
		}
		w.ops = append(w.ops, "delete-offsets")
		w.offsetLSN = 0
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/subjects/"):
		w.subjectsDel = append(w.subjectsDel, r.URL.Path+"?"+r.URL.RawQuery)
		writeJSON(rw, http.StatusNotFound, `{"error_code":40401}`)
	default:
		writeJSON(rw, http.StatusNotFound, `{"error_code":404}`)
	}
}

// with runs f under the world's lock, for assertions and setup.
func (w *p3World) with(f func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f()
}

// snapshotOps returns a copy of the connector operations seen so far.
func (w *p3World) snapshotOps() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.ops...)
}
