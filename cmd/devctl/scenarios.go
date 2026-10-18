package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hamba/avro/v2"

	"piperoom/internal/owners"
)

type scenario struct {
	breakIt func(ctx context.Context, s *stack) error
	fix     func(ctx context.Context, s *stack) error
	// signals are the watcher signals (owners.Signal*) the break trips with
	// dev/owners.yaml. A break the watcher cannot see has an alert instead:
	// the JSON body `devctl alert <scenario>` posts to the alert webhook, the
	// alert a team would have (a reconciliation or freshness check).
	signals []string
	alert   string
}

var scenarios = map[string]scenario{
	"schema-change":    {breakIt: breakSchemaChange, fix: fixSchemaChange},
	"connector-down":   {breakIt: breakConnectorDown, fix: fixConnectorDown},
	"consumer-stuck":   {breakIt: breakConsumerStuck, fix: fixConsumerStuck},
	"long-transaction": {breakIt: breakLongTransaction, fix: fixLongTransaction},
	"slot-inactive":    {breakIt: breakSlotInactive, fix: fixSlotInactive},
	"consumer-lag":     {breakIt: breakConsumerLag, fix: fixConsumerLag},
	"connector-auth":   {breakIt: breakConnectorAuth, fix: fixConnectorAuth},

	// P3 (tasks/P3-fixes-that-end-incidents.md): one per write action.
	"missed-table": {breakIt: breakMissedTable, fix: fixMissedTable, alert: missedTableAlert},
	"dead-slot": {breakIt: breakDeadSlot, fix: fixDeadSlot,
		signals: []string{owners.SignalSlotInactive, owners.SignalConnectorDown}},
	"consumer-bad-deploy": {breakIt: breakConsumerBadDeploy, fix: fixConsumerBadDeploy, alert: badDeployAlert},
	"dlq-backlog": {breakIt: breakDLQBacklog, fix: fixDLQBacklog,
		signals: []string{owners.SignalDLQGrowth}},
}

// schema-change: a migration changes orders.amount from integer to
// numeric(12,2). Debezium's new Avro schema (bytes/decimal instead of int) is
// not BACKWARD compatible, Schema Registry rejects it, and the task fails.
func breakSchemaChange(ctx context.Context, s *stack) error {
	typ, err := s.psql(ctx, "orders", "SELECT data_type FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'amount'")
	if err != nil {
		return err
	}
	if typ != "integer" {
		return fmt.Errorf("devctl: orders.amount is %q, not integer — run `devctl reset` first", typ)
	}
	if _, err := s.psql(ctx, "orders",
		"ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)",
		"INSERT INTO orders (customer_id, amount) VALUES (42, 1249.50), (43, 99.99)"); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "FAILED", 60*time.Second); err != nil {
		return fmt.Errorf("devctl: task 0 did not fail: %w", err)
	}
	s.log.Info("broken: connector task 0 FAILED after the amount type change")
	return nil
}

// The fix: allow the new schema version, restart the task, then restore the
// compatibility rule so the next breaking change is caught again.
//
// The fix is idempotent. The break may still be active, half-fixed (compatibility
// NONE, schema not registered yet) or fully fixed by someone else. Every case
// ends with task 0 RUNNING and compatibility BACKWARD.
func fixSchemaChange(ctx context.Context, s *stack) error {
	// "New schema already registered" is decided from the latest value version,
	// not from a version count. A count taken before a restart cannot tell a new
	// version from an old one, and on an already fixed stack no new version comes,
	// so waiting for one would time out. The schema from before the migration has
	// amount as Avro int, so amount as bytes/decimal in the latest version means
	// the new schema is in the registry.
	registered, err := s.latestAmountIsDecimal(ctx, valueSubject)
	if err != nil {
		return err
	}
	var before []int
	if registered {
		s.log.Info("skipped: new schema version already registered, no wait for a new version")
	} else {
		if err := s.setCompatibility(ctx, valueSubject, "NONE"); err != nil {
			return err
		}
		// Count the registered versions before restarting, so we can tell when the
		// task has registered the new one.
		if before, err = s.schemaVersions(ctx, valueSubject); err != nil {
			return err
		}
	}
	// Restart unless the schema is registered and task 0 is already RUNNING. A
	// task that shows RUNNING can still fail again on the 409, so when the schema
	// is not registered the restart always happens.
	restart := true
	if registered {
		st, err := s.connectorStatus(ctx)
		if err != nil {
			return err
		}
		restart = len(st.Tasks) == 0 || st.Tasks[0].State != "RUNNING"
		if !restart {
			s.log.Info("skipped: task 0 already RUNNING, no restart")
		}
	}
	if restart {
		if err := s.restartTask(ctx, 0); err != nil {
			return err
		}
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return err
	}
	// Wait until the new schema version is registered before restoring BACKWARD;
	// otherwise the task fails again on the next record.
	if !registered {
		if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
			vs, err := s.schemaVersions(ctx, valueSubject)
			if err != nil {
				return false, nil
			}
			return len(vs) > len(before), nil
		}); err != nil {
			return fmt.Errorf("devctl: new schema version was not registered: %w", err)
		}
	}
	if err := s.setCompatibility(ctx, valueSubject, "BACKWARD"); err != nil {
		return err
	}
	s.log.Info("fixed: new schema registered, task 0 RUNNING, compatibility back to BACKWARD")
	return nil
}

// latestAmountIsDecimal reports whether the latest value schema of subject has
// orders.amount as Avro bytes with logicalType decimal, the shape Debezium
// registers after the numeric(12,2) migration.
func (s *stack) latestAmountIsDecimal(ctx context.Context, subject string) (bool, error) {
	b, err := s.do(ctx, http.MethodGet, s.cfg.SchemaRegistryURL+"/subjects/"+url.PathEscape(subject)+"/versions/latest", nil, http.StatusOK)
	if err != nil {
		return false, err
	}
	var v struct {
		Version int    `json:"version"`
		Schema  string `json:"schema"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return false, fmt.Errorf("devctl: decode latest schema of %s: %w", subject, err)
	}
	ok, err := amountIsDecimal(v.Schema)
	if err != nil {
		return false, fmt.Errorf("devctl: latest schema of %s (version %d): %w", subject, v.Version, err)
	}
	return ok, nil
}

// amountIsDecimal reports whether the value schema's after record has amount as
// Avro bytes with logicalType decimal. Debezium defines the row record once,
// inside before, and refers to it by name from after, so the schema is parsed
// with hamba/avro (which resolves those references) before the fields are read.
func amountIsDecimal(schema string) (bool, error) {
	// A fresh cache per call: hamba/avro's default cache is package-global, so a
	// name defined by an earlier schema would resolve here without being defined.
	root, err := avro.ParseWithCache(schema, "", &avro.SchemaCache{})
	if err != nil {
		return false, fmt.Errorf("parse value schema: %w", err)
	}
	env, ok := root.(*avro.RecordSchema)
	if !ok {
		return false, fmt.Errorf("value schema is %s, want a record", root.Type())
	}
	after, ok := fieldByName(env, "after")
	if !ok {
		return false, errors.New("value schema has no after field")
	}
	row, ok := rowRecord(after)
	if !ok {
		return false, errors.New("after field has no record branch")
	}
	amount, ok := fieldByName(row, "amount")
	if !ok {
		return false, errors.New("row record has no amount field")
	}
	return isDecimalBytes(amount), nil
}

// rowRecord returns the record a field type points to: the record itself, a
// named reference to it, or the record inside a union such as ["null", Value].
func rowRecord(t avro.Schema) (*avro.RecordSchema, bool) {
	switch v := t.(type) {
	case *avro.RecordSchema:
		return v, true
	case *avro.RefSchema:
		rec, ok := v.Schema().(*avro.RecordSchema)
		return rec, ok
	case *avro.UnionSchema:
		for _, m := range v.Types() {
			if rec, ok := rowRecord(m); ok {
				return rec, true
			}
		}
	}
	return nil, false
}

// fieldByName returns the declared type of a field of rec.
func fieldByName(rec *avro.RecordSchema, name string) (avro.Schema, bool) {
	for _, f := range rec.Fields() {
		if f.Name() == name {
			return f.Type(), true
		}
	}
	return nil, false
}

// isDecimalBytes reports whether an Avro type is bytes with logicalType decimal.
// Debezium writes numeric columns this way (precise decimal mode).
func isDecimalBytes(t avro.Schema) bool {
	p, ok := t.(*avro.PrimitiveSchema)
	return ok && p.Type() == avro.Bytes && p.Logical() != nil && p.Logical().Type() == avro.Decimal
}

// connector-down: the connector is paused, so the replication slot goes
// inactive and Postgres keeps WAL for it.
func breakConnectorDown(ctx context.Context, s *stack) error {
	_, err := s.do(ctx, "PUT", s.cfg.ConnectURL+"/connectors/"+connectorName+"/pause", nil, 202, 200, 204)
	if err != nil {
		return err
	}
	s.log.Info("broken: connector paused — run `devctl load` to grow retained WAL")
	return nil
}

func fixConnectorDown(ctx context.Context, s *stack) error {
	_, err := s.do(ctx, "PUT", s.cfg.ConnectURL+"/connectors/"+connectorName+"/resume", nil, 202, 200, 204)
	if err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return err
	}
	s.log.Info("fixed: connector resumed")
	return nil
}

// consumer-stuck: devconsumer restarts with a poison-pill order id, then that
// order is inserted. The consumer crashes on it again and again; lag grows.
func breakConsumerStuck(ctx context.Context, s *stack) error {
	next, err := s.psql(ctx, "orders", "SELECT last_value + 1 FROM orders_id_seq")
	if err != nil {
		return err
	}
	if _, err := strconv.ParseInt(next, 10, 64); err != nil {
		return fmt.Errorf("devctl: next order id %q: %w", next, err)
	}
	if err := s.compose(ctx, map[string]string{"DEVCONSUMER_FAIL_ON_ID": next}, "up", "-d", "devconsumer"); err != nil {
		return err
	}
	// Wait until the poison-pill consumer rejoined the group, so the inserted
	// row actually reaches it and it starts crash-looping on it.
	if err := s.waitGroup(ctx, "order-sync", "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: consumer did not rejoin the group: %w", err)
	}
	if _, err := s.psql(ctx, "orders", "INSERT INTO orders (customer_id, amount) VALUES (7, 500)"); err != nil {
		return err
	}
	s.log.Info("broken: devconsumer crashes on order " + next + " — run `devctl load` to grow lag")
	return nil
}

func fixConsumerStuck(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, map[string]string{"DEVCONSUMER_FAIL_ON_ID": "0"}, "up", "-d", "devconsumer"); err != nil {
		return err
	}
	s.log.Info("fixed: devconsumer restarted without the poison-pill flag")
	return nil
}

// long-transaction: a session opens a transaction, writes, and stays open.
// The slot cannot release WAL older than that transaction.
const longTxApp = "piperoom-long-tx"

func breakLongTransaction(ctx context.Context, s *stack) error {
	// docker exec -d keeps the session alive after devctl exits.
	err := runDetached(ctx, "docker", "exec", "-d", "-e", "PGAPPNAME="+longTxApp, pgContainer,
		"psql", "-U", "piperoom", "-d", "orders",
		"-c", "BEGIN",
		"-c", "UPDATE orders SET status = 'on_hold' WHERE id = (SELECT min(id) FROM orders)",
		"-c", "SELECT pg_sleep(600)",
		"-c", "COMMIT")
	if err != nil {
		return err
	}
	var lastErr error
	if err := s.waitFor(ctx, 15*time.Second, func() (bool, error) {
		n, err := s.psql(ctx, "orders", "SELECT count(*) FROM pg_stat_activity WHERE application_name = '"+longTxApp+"' AND xact_start IS NOT NULL")
		if err != nil {
			lastErr = err
			return false, nil
		}
		return n != "0", nil
	}); err != nil {
		if lastErr != nil {
			return fmt.Errorf("devctl: long transaction did not start: %w (last error: %v)", err, lastErr)
		}
		return fmt.Errorf("devctl: long transaction did not start: %w", err)
	}
	s.log.Info("broken: a transaction holds WAL for 10 min — run `devctl load` to grow retained WAL")
	return nil
}

func fixLongTransaction(ctx context.Context, s *stack) error {
	out, err := s.psql(ctx, "orders", "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = '"+longTxApp+"'")
	if err != nil {
		return err
	}
	s.log.Info("fixed: terminated long transactions", "count", strings.TrimSpace(out))
	return nil
}

// slot-inactive: the Debezium connector is stopped (Connect REST `stop`). A
// stopped connector closes its replication connection — unlike the paused
// connector of `connector-down`, which keeps the slot ACTIVE — so the slot
// debezium_orders has no WAL sender: nothing confirms WAL and Postgres keeps
// every segment written after restart_lsn.
func breakSlotInactive(ctx context.Context, s *stack) error {
	if err := s.stopConnectorUntilSlotInactive(ctx); err != nil {
		return err
	}
	// Prove that WAL is really retained: sample the slot's retained bytes,
	// write one batch of orders, and check that the number grew (nobody
	// confirms the new WAL anymore).
	before, after, err := s.proveWALGrows(ctx)
	if err != nil {
		return err
	}
	s.log.Info("broken: connector STOPPED, slot "+slotName+" inactive — run `devctl load` to grow retained WAL further", "retained_before_bytes", before, "retained_after_bytes", after)
	return nil
}

// The fix: resume the stopped connector. It keeps streaming from the slot
// (offsets are intact, no re-snapshot), the slot becomes active again, and
// after it has replayed the retained WAL the consumer lag drains back to 0.
func fixSlotInactive(ctx context.Context, s *stack) error {
	if _, err := s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/resume", nil, http.StatusAccepted, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return err
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		active, err := s.slotActive(ctx)
		if err != nil {
			return false, nil
		}
		return active, nil
	}); err != nil {
		return fmt.Errorf("devctl: slot %s did not become active: %w", slotName, err)
	}
	if err := s.waitLag(ctx, "order-sync", 90*time.Second); err != nil {
		return err
	}
	s.log.Info("fixed: slot " + slotName + " active, connector RUNNING, order-sync lag 0")
	return nil
}

// consumer-lag: devconsumer (group order-sync) is stopped while `devctl load`
// keeps writing orders. Nothing upstream is broken — the connector produces —
// but nothing consumes: group lag grows and the warehouse stays stale.
func breakConsumerLag(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, nil, "stop", "devconsumer"); err != nil {
		return err
	}
	// Wait until the member left the group, so no more offsets are committed
	// and every new record shows up as lag.
	if err := s.waitGroup(ctx, "order-sync", "Empty", 0, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not leave the group: %w", err)
	}
	// Prove that lag grows: write one batch of orders and wait for it to show
	// up in the group's total lag.
	if _, err := s.psql(ctx, "orders", "INSERT INTO orders (customer_id, amount) SELECT g, 400 + g FROM generate_series(1, 50) g"); err != nil {
		return err
	}
	var lag int64 = -1
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		l, err := s.describeGroup(ctx, "order-sync")
		if err != nil {
			return false, nil
		}
		lag = l.Lag.Total()
		return lag > 0, nil
	}); err != nil {
		return fmt.Errorf("devctl: order-sync lag did not grow after stopping devconsumer: %w", err)
	}
	s.log.Info("broken: devconsumer stopped, order-sync lag growing — run `devctl load` to grow it further", "total_lag", lag)
	return nil
}

// The fix: start devconsumer (order-sync) again; it rejoins the group, drains
// the backlog, and the warehouse catches up. `compose start` only starts the
// stopped container — plain `up -d` would recreate it (and any drifted
// dependency, e.g. Postgres) whenever it is not byte-identical to the compose
// file, which kills the running pipeline and any `devctl load`.
func fixConsumerLag(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, nil, "start", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroup(ctx, "order-sync", "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not rejoin the group: %w", err)
	}
	if err := s.waitLag(ctx, "order-sync", 90*time.Second); err != nil {
		return err
	}
	s.log.Info("fixed: devconsumer running, order-sync lag 0, warehouse catching up")
	return nil
}

// connector-auth: a DBA moved ownership of public.orders to orders_owner and
// the connector now authenticates as the least-privilege role `debezium`
// (non-superuser). With publication.autocreate.mode=filtered Debezium runs
// `ALTER PUBLICATION dbz_publication SET TABLE "public"."orders"` on every task
// start, which requires owning the table. debezium owns the publication but not
// the table, so task 0 fails with "must be owner of table orders"; streaming
// never starts, so the slot goes inactive and the warehouse goes stale.
func breakConnectorAuth(ctx context.Context, s *stack) error {
	if _, err := s.psql(ctx, "orders",
		"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') THEN EXECUTE 'CREATE ROLE debezium LOGIN REPLICATION PASSWORD ''piperoom-dev'''; END IF; END $$",
		"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'orders_owner') THEN EXECUTE 'CREATE ROLE orders_owner LOGIN'; END IF; END $$",
		"GRANT CONNECT ON DATABASE orders TO debezium",
		"GRANT USAGE ON SCHEMA public TO debezium",
		"GRANT SELECT ON public.orders TO debezium",
		"ALTER PUBLICATION dbz_publication OWNER TO debezium",
		"ALTER TABLE public.orders OWNER TO orders_owner"); err != nil {
		return err
	}
	if err := s.registerConnectorUser(ctx, "debezium", "piperoom-dev"); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	// The task reports RUNNING but crash-loops on the publication update; the
	// observable symptom is the slot going inactive (streaming never starts).
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		active, err := s.slotActive(ctx)
		if err != nil {
			return false, nil
		}
		return !active, nil
	}); err != nil {
		return fmt.Errorf("devctl: slot %s did not go inactive after the permission break: %w", slotName, err)
	}
	s.log.Info("broken: connector user debezium cannot update dbz_publication — task 0 fails with \"must be owner of table orders\", slot inactive")
	return nil
}

// The fix: give ownership back (orders and the publication return to piperoom),
// point the connector back at piperoom, and restart the task.
func fixConnectorAuth(ctx context.Context, s *stack) error {
	if _, err := s.psql(ctx, "orders",
		"ALTER TABLE public.orders OWNER TO piperoom",
		"ALTER PUBLICATION dbz_publication OWNER TO piperoom"); err != nil {
		return err
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
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		active, err := s.slotActive(ctx)
		if err != nil {
			return false, nil
		}
		return active, nil
	}); err != nil {
		return fmt.Errorf("devctl: slot %s did not become active after the fix: %w", slotName, err)
	}
	s.log.Info("fixed: orders and dbz_publication owner restored to piperoom, connector re-registered, task 0 RUNNING, slot active")
	return nil
}

// slotName is the Debezium replication slot on the source Postgres.
const slotName = "debezium_orders"

// slotActive reports whether the replication slot has an active WAL sender.
func (s *stack) slotActive(ctx context.Context) (bool, error) {
	out, err := s.psql(ctx, "orders", "SELECT active FROM pg_replication_slots WHERE slot_name = '"+slotName+"'")
	if err != nil {
		return false, err
	}
	return out == "t", nil
}

// retainedWALBytes is how much WAL the slot retains, from restart_lsn to the
// current write position.
func (s *stack) retainedWALBytes(ctx context.Context) (int64, error) {
	out, err := s.psql(ctx, "orders", "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name = '"+slotName+"'")
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("devctl: parse retained WAL %q: %w", out, err)
	}
	return n, nil
}

// waitLag waits until the consumer group is stable with total lag 0, i.e. the
// backlog is drained.
func (s *stack) waitLag(ctx context.Context, group string, timeout time.Duration) error {
	if err := s.waitFor(ctx, timeout, func() (bool, error) {
		l, err := s.describeGroup(ctx, group)
		if err != nil {
			return false, nil
		}
		return l.State == "Stable" && l.Lag.Total() == 0, nil
	}); err != nil {
		return fmt.Errorf("devctl: %s lag did not drain to 0: %w", group, err)
	}
	return nil
}
