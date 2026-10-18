package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The P3 scenarios (tasks/P3-fixes-that-end-incidents.md): each one is the
// incident one of the P3 write actions ends. Everything they create is named
// devctl_* / devctl.* so a leftover is easy to spot, and each fix removes it.

// p3OwnersFile is the owners file the P3 scenarios need: dev/owners.yaml
// plus public.devctl_refunds in source_tables (the actions refuse a table
// outside it), the DLQ in watch.dlq_topics and dlq_growth. It is a separate
// file because evals/gen derives fixtures from the baseline.
const p3OwnersFile = "dev/owners.p3.yaml"

const (
	publicationName = "dbz_publication" // Debezium's default publication.name
	ordersTopic     = "pg.public.orders"
	ordersGroup     = "order-sync"

	// missed-table
	refundsTable = "public.devctl_refunds"
	signalTable  = "public.devctl_signal" // the connector's signal.data.collection during the break
	refundsTopic = "pg.public.devctl_refunds"
	signalTopic  = "pg.public.devctl_signal"
	refundRows   = 25

	// dlq-backlog: the topic is declared in dev/owners.p3.yaml watch.dlq_topics
	// and set as DLQ_TOPIC of devconsumer in dev/docker-compose.yml.
	dlqTopic     = "devctl.order-sync.dlq"
	rejectStatus = "gift"
	giftOrders   = 20

	// consumer-bad-deploy
	badDeployOrders = 30
)

// SQL the P3 scenarios run, named so the unit tests' fake Postgres answers
// exactly these statements.
const (
	sqlCreateSignal = "CREATE TABLE IF NOT EXISTS public.devctl_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048))"
	// Debezium needs a primary key (or REPLICA IDENTITY FULL) to snapshot and
	// to emit updates; pg_add_publication_table checks the same.
	sqlCreateRefunds = "CREATE TABLE IF NOT EXISTS public.devctl_refunds (id bigserial PRIMARY KEY, order_id bigint NOT NULL, amount integer NOT NULL, reason text NOT NULL, created_at timestamptz NOT NULL DEFAULT now())"
	sqlRefundsExists = "SELECT to_regclass('public.devctl_refunds') IS NOT NULL"
	sqlRefundsInPub  = "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'dbz_publication' AND schemaname = 'public' AND tablename = 'devctl_refunds'"
	// The publication of the break: the signal table is in it (Debezium reads
	// signals and snapshot watermarks from the WAL), the new table is not.
	sqlPubWithoutRefunds = "ALTER PUBLICATION dbz_publication SET TABLE public.orders, public.devctl_signal"
	sqlPubAddRefunds     = "ALTER PUBLICATION dbz_publication ADD TABLE public.devctl_refunds"
	sqlPubBaseline       = "ALTER PUBLICATION dbz_publication SET TABLE public.orders"
	sqlInsertRefunds     = "INSERT INTO public.devctl_refunds (order_id, amount, reason) SELECT id, amount, 'damaged' FROM orders ORDER BY id DESC LIMIT 25"
	sqlCountRefunds      = "SELECT count(*) FROM public.devctl_refunds"
	// The same row P3b's SourceWriter.InsertSnapshotSignal writes.
	sqlSnapshotSignal     = `INSERT INTO public.devctl_signal (id, type, data) VALUES (gen_random_uuid()::text, 'execute-snapshot', '{"data-collections":["public.devctl_refunds"],"type":"incremental"}')`
	sqlDropScenarioTables = "DROP TABLE IF EXISTS public.devctl_refunds, public.devctl_signal"
	sqlInsertMarker       = "WITH i AS (INSERT INTO orders (customer_id, amount) VALUES (77, 4200) RETURNING id) SELECT id FROM i"
	sqlWarehouseHasOrder  = "SELECT count(*) FROM analytics.orders WHERE id = "

	// dead-slot
	sqlSlotRow  = "SELECT active::text || '|' || coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, 0) FROM pg_replication_slots WHERE slot_name = '" + slotName + "'"
	sqlDropSlot = "SELECT pg_drop_replication_slot('" + slotName + "')"
	// Re-emits the rows changed while no slot existed, with their own
	// updated_at, so the warehouse gets them through CDC (followed by
	// pendingWhere's formatted timestamp, nothing else).
	sqlTouchPending = "UPDATE orders SET updated_at = updated_at WHERE "

	// consumer-bad-deploy
	sqlInsertBadDeploy = "WITH i AS (INSERT INTO orders (customer_id, amount) SELECT g, 700 + g FROM generate_series(1, 30) g RETURNING id) SELECT min(id) || ' ' || max(id) FROM i"
	sqlWarehouseIDs    = "SELECT count(*) FROM analytics.orders WHERE id BETWEEN "

	// dlq-backlog
	sqlInsertGift        = "INSERT INTO orders (customer_id, amount, status) SELECT g, 50 + g, 'gift' FROM generate_series(1, 20) g"
	sqlSourceGift        = "SELECT count(*) FROM orders WHERE status = 'gift'"
	sqlWarehouseGift     = "SELECT count(*) FROM analytics.orders WHERE status = 'gift'"
	sqlCountSourceOrders = "SELECT count(*) FROM orders"
	sqlCountWarehouse    = "SELECT count(*) FROM analytics.orders"
)

// missedTableConnector is the connector config of the missed-table break: the
// app team added devctl_refunds to table.include.list, but the publication is
// owned by the DBA (autocreate disabled), and nobody added the table to it.
// Debezium only receives what the publication publishes, so the table's rows
// never reach Kafka and nothing fails. The signal table is configured so the
// incremental-snapshot fix works on the dev stack.
var missedTableConnector = map[string]string{
	"publication.autocreate.mode": "disabled",
	"table.include.list":          "public.orders,public.devctl_refunds,public.devctl_signal",
	"signal.data.collection":      "public.devctl_signal",
}

// ---------------------------------------------------------------- missed-table

func breakMissedTable(ctx context.Context, s *stack) error {
	if n, err := s.psql(ctx, "orders", sqlRefundsInPub); err != nil {
		return err
	} else if n != "0" {
		return fmt.Errorf("devctl: %s is already in %s — run `devctl fix missed-table` (or reset) first", refundsTable, publicationName)
	}
	if _, err := s.psql(ctx, "orders", sqlCreateSignal, sqlCreateRefunds); err != nil {
		return err
	}
	if err := s.registerConnectorWith(ctx, missedTableConnector); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return fmt.Errorf("devctl: connector not RUNNING after the include-list change: %w", err)
	}
	// After the task is up: with autocreate disabled Debezium no longer
	// touches the publication, so this is the state it streams from.
	if _, err := s.psql(ctx, "orders", sqlPubWithoutRefunds, sqlInsertRefunds); err != nil {
		return err
	}
	// Proof: an order written after the refunds reaches the warehouse, so the
	// connector streams; the refunds, written first, are not in Kafka.
	if err := s.waitMarkerOrder(ctx); err != nil {
		return err
	}
	span, ok, err := s.topicSpan(ctx, refundsTopic)
	if err != nil {
		return err
	}
	if ok && span.records() > 0 {
		return fmt.Errorf("devctl: %s has %d records: the break did not hold", refundsTopic, span.records())
	}
	s.log.Info("broken: "+refundsTable+" is in table.include.list but not in "+publicationName+" — its rows never reach Kafka, and nothing fails",
		"refund_rows", refundRows, "topic", refundsTopic)
	return nil
}

// waitMarkerOrder inserts one order and waits until the warehouse has it.
func (s *stack) waitMarkerOrder(ctx context.Context) error {
	id, err := s.psql(ctx, "orders", sqlInsertMarker)
	if err != nil {
		return err
	}
	if _, err := strconv.ParseInt(id, 10, 64); err != nil {
		return fmt.Errorf("devctl: marker order id %q: %w", id, err)
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		n, err := s.psql(ctx, "warehouse", sqlWarehouseHasOrder+id)
		if err != nil {
			return false, nil
		}
		return n == "1", nil
	}); err != nil {
		return fmt.Errorf("devctl: marker order %s did not reach the warehouse: %w", id, err)
	}
	return nil
}

// The fix is what pg_add_publication_table plus connect_snapshot_table do:
// add the table to the publication (new changes flow), then ask Debezium for
// an incremental snapshot (the rows written before). Once the rows are in
// Kafka it puts the stack back as `reset` leaves it: the connector config from
// dev/connectors, the publication on orders only, and no devctl_* table,
// topic or subject.
func fixMissedTable(ctx context.Context, s *stack) error {
	exists, err := s.psql(ctx, "orders", sqlRefundsExists)
	if err != nil {
		return err
	}
	if exists == "t" {
		if err := s.publishAndSnapshotRefunds(ctx); err != nil {
			return err
		}
	} else {
		s.log.Info("skipped: " + refundsTable + " does not exist, nothing to publish or snapshot")
	}
	if err := s.registerConnector(ctx); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlPubBaseline, sqlDropScenarioTables); err != nil {
		return err
	}
	if err := s.deleteTopics(ctx, refundsTopic, signalTopic); err != nil {
		return err
	}
	for _, t := range []string{refundsTopic, signalTopic} {
		for _, sub := range []string{t + "-key", t + "-value"} {
			if err := s.deleteSubject(ctx, sub); err != nil {
				return err
			}
		}
	}
	s.log.Info("fixed: connector config, " + publicationName + " and topics back to the dev baseline; devctl_* tables, topics and subjects removed")
	return nil
}

func (s *stack) publishAndSnapshotRefunds(ctx context.Context) error {
	n, err := s.psql(ctx, "orders", sqlRefundsInPub)
	if err != nil {
		return err
	}
	if n == "0" {
		if _, err := s.psql(ctx, "orders", sqlPubAddRefunds); err != nil {
			return err
		}
		s.log.Info("added " + refundsTable + " to " + publicationName)
	} else {
		s.log.Info("skipped: " + refundsTable + " already in " + publicationName)
	}
	raw, err := s.psql(ctx, "orders", sqlCountRefunds)
	if err != nil {
		return err
	}
	rows, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("devctl: parse refund count %q: %w", raw, err)
	}
	inKafka := func() (int64, error) {
		span, _, err := s.topicSpan(ctx, refundsTopic)
		return span.records(), err
	}
	have, err := inKafka()
	if err != nil {
		return err
	}
	if have >= rows {
		s.log.Info("skipped: snapshot signal, the topic already holds every row", "rows", rows, "records", have)
		return nil
	}
	if _, err := s.psql(ctx, "orders", sqlSnapshotSignal); err != nil {
		return err
	}
	s.log.Info("sent an incremental snapshot signal for "+refundsTable, "rows", rows, "records_before", have)
	if err := s.waitFor(ctx, 120*time.Second, func() (bool, error) {
		n, err := inKafka()
		if err != nil {
			return false, nil
		}
		have = n
		return n >= rows, nil
	}); err != nil {
		return fmt.Errorf("devctl: %s holds %d of %d rows after the snapshot signal: %w", refundsTopic, have, rows, err)
	}
	s.log.Info("refund rows reached Kafka", "topic", refundsTopic, "records", have)
	return nil
}

// registerConnectorWith re-registers the connector from dev/connectors with
// the given keys replaced.
func (s *stack) registerConnectorWith(ctx context.Context, set map[string]string) error {
	body, err := os.ReadFile(filepath.Join(s.root, "dev", "connectors", connectorName+".json"))
	if err != nil {
		return fmt.Errorf("devctl: read connector config: %w", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(body, &cfg); err != nil {
		return fmt.Errorf("devctl: decode connector config: %w", err)
	}
	for k, v := range set {
		cfg[k] = v
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("devctl: encode connector config: %w", err)
	}
	_, err = s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/config", b, http.StatusOK, http.StatusCreated)
	return err
}

// deleteSubject removes a Schema Registry subject for good (soft delete, then
// permanent). A subject that does not exist is not an error.
func (s *stack) deleteSubject(ctx context.Context, subject string) error {
	base := s.cfg.SchemaRegistryURL + "/subjects/" + url.PathEscape(subject)
	if _, err := s.do(ctx, http.MethodDelete, base, nil, http.StatusOK, http.StatusNotFound); err != nil {
		return err
	}
	_, err := s.do(ctx, http.MethodDelete, base+"?permanent=true", nil, http.StatusOK, http.StatusNotFound)
	return err
}

// ------------------------------------------------------------------- dead-slot

// dead-slot: the orders connector was retired (STOPPED, its config kept for
// the record) and nobody dropped its slot. The slot is inactive and keeps
// every WAL segment written since; nothing will read it again. The mechanics
// are those of slot-inactive; the difference is the decision: here the slot
// is dropped (pg_drop_replication_slot), not the connector resumed.
func breakDeadSlot(ctx context.Context, s *stack) error {
	if _, ok, err := s.slotRow(ctx); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("devctl: slot %s does not exist — run `devctl fix dead-slot` (or reset) first", slotName)
	}
	if err := s.stopConnectorUntilSlotInactive(ctx); err != nil {
		return err
	}
	before, after, err := s.proveWALGrows(ctx)
	if err != nil {
		return err
	}
	s.log.Info("broken: connector retired (STOPPED), slot "+slotName+" dead and retaining WAL — run `devctl load` to grow it further",
		"retained_before_bytes", before, "retained_after_bytes", after)
	return nil
}

// slotState is one row of pg_replication_slots for slotName.
type slotState struct {
	active   bool
	retained int64
}

// slotRow reads the slot; ok is false when it does not exist.
func (s *stack) slotRow(ctx context.Context) (slotState, bool, error) {
	out, err := s.psql(ctx, "orders", sqlSlotRow)
	if err != nil {
		return slotState{}, false, err
	}
	if out == "" {
		return slotState{}, false, nil
	}
	active, retained, ok := strings.Cut(out, "|")
	if !ok || (active != "true" && active != "false") {
		return slotState{}, false, fmt.Errorf("devctl: parse slot row %q", out)
	}
	n, err := strconv.ParseInt(retained, 10, 64)
	if err != nil {
		return slotState{}, false, fmt.Errorf("devctl: parse slot row %q: %w", out, err)
	}
	return slotState{active: active == "true", retained: n}, true, nil
}

// The fix drops the dead slot (what pg_drop_replication_slot does, and what
// frees the WAL), then brings the dev pipeline back: resume the connector,
// which creates a new slot at the current WAL position, and re-emit the rows
// changed while no slot existed so the warehouse is complete again. In
// production that last step is connect_snapshot_table. Idempotent: a slot the
// agent already dropped is not dropped again, a running connector is left
// alone.
func fixDeadSlot(ctx context.Context, s *stack) error {
	st, err := s.connectorStatus(ctx)
	if err != nil {
		return err
	}
	running := st.Connector.State == "RUNNING"
	slot, exists, err := s.slotRow(ctx)
	if err != nil {
		return err
	}
	if running && exists && slot.active {
		s.log.Info("skipped: connector RUNNING and slot " + slotName + " active, nothing is dead")
		return nil
	}
	// Read before the resume: rows newer than this were written while the slot
	// was dead (or gone) and must be re-emitted.
	newest, err := s.warehouseNewest(ctx)
	if err != nil {
		return err
	}
	switch {
	case exists && !slot.active && !running:
		if _, err := s.psql(ctx, "orders", sqlDropSlot); err != nil {
			return err
		}
		s.log.Info("dropped slot "+slotName, "retained_bytes_freed", slot.retained)
	case exists && !slot.active:
		return fmt.Errorf("devctl: slot %s is inactive but the connector is %s; not dropping a slot the connector may still use", slotName, st.Connector.State)
	case !exists:
		s.log.Info("skipped: slot " + slotName + " already dropped")
		if running {
			// Debezium creates a missing slot only when its task starts.
			if err := s.restartTask(ctx, 0); err != nil {
				return err
			}
		}
	}
	if !running {
		if _, err := s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/resume", nil, http.StatusAccepted, http.StatusOK, http.StatusNoContent); err != nil {
			return err
		}
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return err
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		row, ok, err := s.slotRow(ctx)
		if err != nil {
			return false, nil
		}
		return ok && row.active, nil
	}); err != nil {
		return fmt.Errorf("devctl: connector did not create an active slot %s: %w", slotName, err)
	}
	if _, err := s.psql(ctx, "orders", sqlTouchPending+pendingWhere(warehousePendingFrom(newest))); err != nil {
		return err
	}
	if err := s.waitLagZero(ctx, ordersGroup, 90*time.Second); err != nil {
		return err
	}
	s.log.Info("fixed: dead slot dropped, connector RUNNING on a new slot " + slotName + ", rows changed meanwhile re-emitted, order-sync lag 0")
	return nil
}

// stopConnectorUntilSlotInactive stops the connector (Connect REST stop: a
// stopped connector closes its replication connection, a paused one does not)
// and waits until the slot has no WAL sender.
func (s *stack) stopConnectorUntilSlotInactive(ctx context.Context) error {
	if _, err := s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/stop", nil, http.StatusNoContent, http.StatusAccepted, http.StatusOK); err != nil {
		return err
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		st, err := s.connectorStatus(ctx)
		if err != nil {
			return false, nil
		}
		return st.Connector.State == "STOPPED", nil
	}); err != nil {
		return fmt.Errorf("devctl: connector did not stop: %w", err)
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		active, err := s.slotActive(ctx)
		if err != nil {
			return false, nil
		}
		return !active, nil
	}); err != nil {
		return fmt.Errorf("devctl: slot %s did not go inactive: %w", slotName, err)
	}
	return nil
}

// proveWALGrows writes one batch of orders and waits until the slot's
// retained WAL grew: nobody confirms WAL any more.
func (s *stack) proveWALGrows(ctx context.Context) (before, after int64, err error) {
	before, err = s.retainedWALBytes(ctx)
	if err != nil {
		return 0, 0, err
	}
	if _, err := s.psql(ctx, "orders", "INSERT INTO orders (customer_id, amount) SELECT g, 100 + g FROM generate_series(1, 100) g"); err != nil {
		return 0, 0, err
	}
	if err := s.waitFor(ctx, 30*time.Second, func() (bool, error) {
		n, err := s.retainedWALBytes(ctx)
		if err != nil {
			return false, nil
		}
		after = n
		return n > before, nil
	}); err != nil {
		return 0, 0, fmt.Errorf("devctl: retained WAL did not grow (stayed at %d bytes): %w", before, err)
	}
	return before, after, nil
}

// --------------------------------------------------------- consumer-bad-deploy

// consumer-bad-deploy: a devconsumer build that skips create events
// (SKIP_OPS=c) ran for a while: it committed every offset but never loaded the
// new orders, and updates still flowed, so the warehouse looked fresh. On-call
// stopped it; the offsets now point past orders that never arrived. The fix is
// kafka_reset_consumer_offsets on the stopped group, then the good build.
func breakConsumerBadDeploy(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, map[string]string{"DEVCONSUMER_SKIP_OPS": "c"}, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: bad build did not join the group: %w", err)
	}
	span, err := s.psql(ctx, "orders", sqlInsertBadDeploy)
	if err != nil {
		return err
	}
	lo, hi, err := parseIDSpan(span)
	if err != nil {
		return err
	}
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: bad build did not commit past the new orders: %w", err)
	}
	loaded, err := s.psql(ctx, "warehouse", fmt.Sprintf("%s%d AND %d", sqlWarehouseIDs, lo, hi))
	if err != nil {
		return err
	}
	if loaded != "0" {
		return fmt.Errorf("devctl: %s of the new orders %d..%d reached the warehouse: the bad build did not skip them", loaded, lo, hi)
	}
	if err := s.compose(ctx, nil, "stop", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Empty", 0, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not leave the group: %w", err)
	}
	s.log.Info("broken: a devconsumer build skipped create events and committed past them; consumer stopped, offsets past orders the warehouse never got",
		"missing_ids", fmt.Sprintf("%d..%d", lo, hi), "count", badDeployOrders)
	return nil
}

func parseIDSpan(out string) (lo, hi int64, err error) {
	a, b, ok := strings.Cut(strings.TrimSpace(out), " ")
	if !ok {
		return 0, 0, fmt.Errorf("devctl: parse id span %q", out)
	}
	if lo, err = strconv.ParseInt(a, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("devctl: parse id span %q: %w", out, err)
	}
	if hi, err = strconv.ParseInt(b, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("devctl: parse id span %q: %w", out, err)
	}
	if hi < lo {
		return 0, 0, fmt.Errorf("devctl: id span %q is reversed", out)
	}
	return lo, hi, nil
}

// The fix: stop the consumer, reset order-sync to the start of the topic (the
// warehouse load is an upsert by id, so replaying everything is safe; the
// action would pick the deploy time), and start the good build. Idempotent:
// a second run replays once more and ends in the same state.
func fixConsumerBadDeploy(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, nil, "stop", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Empty", 0, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not leave the group: %w", err)
	}
	if err := s.resetGroupToStart(ctx, ordersGroup, ordersTopic); err != nil {
		return err
	}
	if err := s.compose(ctx, map[string]string{"DEVCONSUMER_SKIP_OPS": ""}, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not rejoin the group: %w", err)
	}
	if err := s.waitLagZero(ctx, ordersGroup, 120*time.Second); err != nil {
		return err
	}
	src, err := s.psql(ctx, "orders", sqlCountSourceOrders)
	if err != nil {
		return err
	}
	wh, err := s.psql(ctx, "warehouse", sqlCountWarehouse)
	if err != nil {
		return err
	}
	s.log.Info("fixed: order-sync reset to the start of "+ordersTopic+" and replayed by the good build, lag 0", "source_orders", src, "warehouse_orders", wh)
	return nil
}

// ----------------------------------------------------------------- dlq-backlog

// dlq-backlog: a devconsumer release validates status and rejects a value it
// does not know ('gift', new upstream), routing those records to its DLQ
// instead of loading them. The DLQ grows; lag stays 0. The fix is to deploy a
// build that accepts the value, then dlq_replay.
func breakDLQBacklog(ctx context.Context, s *stack) error {
	if err := s.ensureTopic(ctx, dlqTopic, 1); err != nil {
		return err
	}
	if err := s.compose(ctx, map[string]string{"DEVCONSUMER_REJECT_STATUS": rejectStatus}, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: strict build did not join the group: %w", err)
	}
	span, _, err := s.topicSpan(ctx, dlqTopic)
	if err != nil {
		return err
	}
	before := span.records()
	if _, err := s.psql(ctx, "orders", sqlInsertGift); err != nil {
		return err
	}
	var now int64
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		span, _, err := s.topicSpan(ctx, dlqTopic)
		if err != nil {
			return false, nil
		}
		now = span.records()
		return now >= before+giftOrders, nil
	}); err != nil {
		return fmt.Errorf("devctl: %s grew from %d to %d, want +%d: %w", dlqTopic, before, now, giftOrders, err)
	}
	s.log.Info("broken: devconsumer rejects status '"+rejectStatus+"' and routes those orders to "+dlqTopic, "dlq_records", now)
	return nil
}

// connectErrorHeader is the prefix of the context headers Kafka Connect (and
// devconsumer, which copies the convention) puts on a DLQ record. A replayed
// record must not carry them: they describe the failure, not the data.
const connectErrorHeader = "__connect.errors."

// replayRecords copies DLQ records back to the source topic in their order,
// with the error headers dropped and every other header kept.
func replayRecords(recs []*kgo.Record, to string) []*kgo.Record {
	out := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		var hs []kgo.RecordHeader
		for _, h := range r.Headers {
			if !strings.HasPrefix(h.Key, connectErrorHeader) {
				hs = append(hs, h)
			}
		}
		out = append(out, &kgo.Record{Topic: to, Key: r.Key, Value: r.Value, Headers: hs})
	}
	return out
}

// The fix: deploy the build that accepts the value (the cause), replay the
// DLQ into pg.public.orders (what dlq_replay does), and delete the DLQ topic so
// nothing is replayed twice and nothing is left behind. Idempotent: with the
// topic gone a second run replays nothing.
func fixDLQBacklog(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, map[string]string{"DEVCONSUMER_REJECT_STATUS": ""}, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not rejoin the group: %w", err)
	}
	recs, err := s.readTopic(ctx, dlqTopic)
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		s.log.Info("skipped: " + dlqTopic + " is empty or gone, nothing to replay")
	} else {
		if err := s.produce(ctx, replayRecords(recs, ordersTopic)); err != nil {
			return err
		}
		s.log.Info("replayed "+dlqTopic+" into "+ordersTopic, "records", len(recs))
	}
	if err := s.deleteTopics(ctx, dlqTopic); err != nil {
		return err
	}
	if err := s.waitLagZero(ctx, ordersGroup, 90*time.Second); err != nil {
		return err
	}
	var src, wh string
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		if src, err = s.psql(ctx, "orders", sqlSourceGift); err != nil {
			return false, nil
		}
		if wh, err = s.psql(ctx, "warehouse", sqlWarehouseGift); err != nil {
			return false, nil
		}
		return src == wh, nil
	}); err != nil {
		return fmt.Errorf("devctl: warehouse has %s of %s '%s' orders after the replay: %w", wh, src, rejectStatus, err)
	}
	s.log.Info("fixed: devconsumer accepts '"+rejectStatus+"', DLQ replayed and removed, every '"+rejectStatus+"' order in the warehouse", "orders", src)
	return nil
}

// -------------------------------------------------------------------- helpers

// groupView is what the P3 scenarios read about a consumer group.
type groupView struct {
	state   string
	members int
	lag     int64
}

// groupView reads a group, through the test seam when one is set.
func (s *stack) groupView(ctx context.Context, group string) (groupView, error) {
	if s.groupFn != nil {
		return s.groupFn(ctx, group)
	}
	l, err := s.describeGroup(ctx, group)
	if err != nil {
		return groupView{}, err
	}
	return groupView{state: l.State, members: len(l.Members), lag: l.Lag.Total()}, nil
}

// errNoGroupYet keeps waiting while a group cannot be described.
var errNoGroupYet = errors.New("devctl: group not readable yet")

func (s *stack) waitGroupState(ctx context.Context, group, state string, minMembers int, timeout time.Duration) error {
	return s.waitFor(ctx, timeout, func() (bool, error) {
		g, err := s.groupView(ctx, group)
		if err != nil {
			return false, nil
		}
		return g.state == state && g.members >= minMembers, nil
	})
}

// waitLagZero waits until the group is Stable with total lag 0.
func (s *stack) waitLagZero(ctx context.Context, group string, timeout time.Duration) error {
	var last groupView
	lastErr := errNoGroupYet
	if err := s.waitFor(ctx, timeout, func() (bool, error) {
		g, err := s.groupView(ctx, group)
		if err != nil {
			lastErr = err
			return false, nil
		}
		last, lastErr = g, nil
		return g.state == "Stable" && g.lag == 0, nil
	}); err != nil {
		if lastErr != nil {
			return fmt.Errorf("devctl: %s lag did not drain to 0 (%v): %w", group, lastErr, err)
		}
		return fmt.Errorf("devctl: %s lag did not drain to 0 (state %s, lag %d): %w", group, last.state, last.lag, err)
	}
	return nil
}
