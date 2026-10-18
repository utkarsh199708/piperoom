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
	// dlq-backlog
	sourceGift int
	// connect
	connState   string // RUNNING, STOPPED
	connCfg     map[string]any
	restarts    int
	subjectsDel []string
	// devconsumer
	consumerUp   bool
	skipOps      string
	rejectStatus string
	lag          int64
	resetSeen    bool // the group's committed offsets were at the topic start when a good build started
	sql          []string
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
		connState: "RUNNING", consumerUp: true,
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

func (w *p3World) streaming() bool { return w.connState == "RUNNING" && w.slotExists }

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
		w.slotExists, w.retained = false, 0
	case strings.HasPrefix(q, "SELECT coalesce(round(extract(epoch FROM max(updated_at))") && db == "warehouse":
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
			tasks = `[{"id":0,"state":"RUNNING"}]`
		}
		writeJSON(rw, http.StatusOK, fmt.Sprintf(`{"connector":{"state":%q},"tasks":%s}`, w.connState, tasks))
	case r.Method == http.MethodPut && r.URL.Path == base+"/config":
		var cfg map[string]any
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &cfg); err != nil {
			writeJSON(rw, http.StatusBadRequest, `{}`)
			return
		}
		w.connCfg = cfg
		writeJSON(rw, http.StatusOK, `{}`)
	case r.Method == http.MethodPost && r.URL.Path == base+"/tasks/0/restart":
		w.restarts++
		if w.connState == "RUNNING" && !w.slotExists {
			w.slotExists, w.slotActive = true, true
		}
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && r.URL.Path == base+"/stop":
		w.connState, w.slotActive = "STOPPED", false
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && r.URL.Path == base+"/resume":
		w.connState = "RUNNING"
		// Debezium creates a missing slot when the task starts.
		w.slotExists, w.slotActive = true, true
		rw.WriteHeader(http.StatusAccepted)
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
