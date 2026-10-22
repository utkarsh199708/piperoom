package main

import (
	"context"
	"fmt"
	"time"
)

// The X2 recovery scenarios (X2e, dev/scenarios.md §18-§19): rows a fault lost
// downstream that a recovery action re-emits. Both leave order-sync running at
// lag 0 with orders missing from analytics.orders, both open from a
// reconciliation alert, and both need the server on dev/owners.recovery.yaml
// (the pipeline's reprocess_topic, which kafka_replay_range writes to).
//
//   - rows-lost-after-retention: retention deleted the change events before
//     order-sync read them. Kafka cannot replay them; pg_resnapshot_rows
//     re-emits the rows from the source.
//   - consumer-skipped-range: order-sync's offsets were moved past events that
//     are still on the topic. kafka_replay_range copies them to the reprocess
//     topic.

const (
	// recoveryOwnersFile is dev/owners.yaml plus the pipeline's
	// reprocess_topic (TestOwnersRecoveryMatchesBaseline).
	recoveryOwnersFile = "dev/owners.recovery.yaml"
	reprocessTopic     = "pg.public.orders.reprocess"

	// rows-lost-after-retention: lostBatches batches of lostBatchSize orders,
	// each rolled into a segment of its own.
	lostBatches   = 5
	lostBatchSize = 10
	// consumer-skipped-range
	skippedOrders = 40
)

// batchGapDefault spaces the lost batches past segment.ms (1 s), so each
// batch's first record rolls a new segment and the older ones can be deleted.
const batchGapDefault = 1500 * time.Millisecond

// SQL of the recovery scenarios, named so the unit tests' fake Postgres
// answers exactly these statements. The orders of a break are marked by
// their customer_id range, so a fix finds them without any saved state.
const (
	// sqlLostBatchPrefix starts every lost-batch insert (sqlLostBatch).
	sqlLostBatchPrefix = "WITH i AS (INSERT INTO orders (customer_id, amount) SELECT 9000 + "
	sqlLostSpan        = "SELECT coalesce(min(id) || ' ' || max(id), '') FROM orders WHERE customer_id BETWEEN 9001 AND 9050"
	sqlInsertSkipped   = "WITH i AS (INSERT INTO orders (customer_id, amount) SELECT 9100 + g, 500 + g FROM generate_series(1, 40) g RETURNING id) SELECT min(id) || ' ' || max(id) FROM i"
	sqlSkippedSpan     = "SELECT coalesce(min(id) || ' ' || max(id), '') FROM orders WHERE customer_id BETWEEN 9101 AND 9140"
	// sqlSourceIDs is followed by "<lo> AND <hi>", as sqlWarehouseIDs is.
	sqlSourceIDs    = "SELECT count(*) FROM orders WHERE id BETWEEN "
	sqlSignalExists = "SELECT to_regclass('public.devctl_signal') IS NOT NULL"
	sqlDropSignal   = "DROP TABLE IF EXISTS public.devctl_signal"
	// sqlResnapshotPrefix starts the signal row sqlResnapshotSignal writes.
	sqlResnapshotPrefix = `INSERT INTO public.devctl_signal (id, type, data) VALUES (gen_random_uuid()::text, 'execute-snapshot', '{"data-collections":["public.orders"],"type":"INCREMENTAL","additional-conditions":`
)

// sqlLostBatch inserts batch b (0-based) of the lost orders: customer_id
// 9001+10b.. and amount 301+10b.., returning the id span.
func sqlLostBatch(b int) string {
	return fmt.Sprintf("%s%d + g, %d + g FROM generate_series(1, %d) g RETURNING id) SELECT min(id) || ' ' || max(id) FROM i",
		sqlLostBatchPrefix, lostBatchSize*b, 300+lostBatchSize*b, lostBatchSize)
}

// sqlResnapshotSignal is the signal row pg_resnapshot_rows writes for
// key_range id lo..hi (postgres.Writer.InsertSnapshotSignalWhere): an
// incremental snapshot of public.orders limited by Debezium's
// additional-conditions. The bounds are integers the fix read itself.
func sqlResnapshotSignal(lo, hi int64) string {
	return fmt.Sprintf(`%s[{"data-collection":"public.orders","filter":"\"id\" >= ''%d'' AND \"id\" <= ''%d''"}]}')`, sqlResnapshotPrefix, lo, hi)
}

// recoveryConnector is the connector config both breaks register: the signal
// table pg_resnapshot_rows writes to, in table.include.list so the filtered
// publication carries it (Debezium reads signals and its snapshot watermarks
// from the WAL).
var recoveryConnector = map[string]string{
	"table.include.list":     "public.orders,public.devctl_signal",
	"signal.data.collection": "public.devctl_signal",
}

// enableRecovery creates the signal table, registers the connector with it,
// and creates the reprocess topic, so either recovery action can run against
// the break. Each step is safe to repeat.
func (s *stack) enableRecovery(ctx context.Context) error {
	if _, err := s.psql(ctx, "orders", sqlCreateSignal); err != nil {
		return err
	}
	if err := s.registerConnectorWith(ctx, recoveryConnector); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return fmt.Errorf("devctl: connector not RUNNING after the signal table was added: %w", err)
	}
	// The publication filtered autocreate keeps; set here too, so the
	// signal row reaches Debezium even before its next restart.
	if _, err := s.psql(ctx, "orders", sqlPubWithoutRefunds); err != nil {
		return err
	}
	return s.ensureTopic(ctx, reprocessTopic, 3)
}

// disableRecovery puts the connector, the publication and the topics back as
// `devctl reset` leaves them. Safe to repeat.
func (s *stack) disableRecovery(ctx context.Context) error {
	if err := s.registerConnector(ctx); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlPubBaseline, sqlDropSignal); err != nil {
		return err
	}
	if err := s.deleteTopics(ctx, signalTopic, reprocessTopic); err != nil {
		return err
	}
	for _, sub := range []string{signalTopic + "-key", signalTopic + "-value"} {
		if err := s.deleteSubject(ctx, sub); err != nil {
			return err
		}
	}
	return nil
}

// missingIn counts the source orders with id in [lo, hi] that the warehouse
// lacks.
func (s *stack) missingIn(ctx context.Context, lo, hi int64) (int64, error) {
	span := fmt.Sprintf("%d AND %d", lo, hi)
	src, err := s.countOf(ctx, "orders", sqlSourceIDs+span)
	if err != nil {
		return 0, err
	}
	wh, err := s.countOf(ctx, "warehouse", sqlWarehouseIDs+span)
	if err != nil {
		return 0, err
	}
	return max(src-wh, 0), nil
}

// markedSpan reads the id span of a break's marked orders; ok is false when
// there are none (no break ran, or `devctl reset` removed them).
func (s *stack) markedSpan(ctx context.Context, query string) (lo, hi int64, ok bool, err error) {
	out, err := s.psql(ctx, "orders", query)
	if err != nil || out == "" {
		return 0, 0, false, err
	}
	lo, hi, err = parseIDSpan(out)
	return lo, hi, err == nil, err
}

// ------------------------------------------------- rows-lost-after-retention

func breakRowsLostAfterRetention(ctx context.Context, s *stack) error {
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: order-sync is not running (start the stack first): %w", err)
	}
	if err := s.enableRecovery(ctx); err != nil {
		return err
	}
	if err := s.compose(ctx, nil, "stop", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Empty", 0, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not leave the group: %w", err)
	}
	if err := s.setTopicConfig(ctx, ordersTopic, retentionBreakCut); err != nil {
		return err
	}
	lo, hi, beforeLast, err := s.insertLostBatches(ctx)
	if err != nil {
		return err
	}
	// Every segment older than the last batch's is deleted: the start of each
	// partition reached the last batch (a partition the last batch did not
	// write to keeps its active segment).
	if err := s.waitFor(ctx, 90*time.Second, func() (bool, error) {
		starts, err := s.logStartOffsets(ctx, ordersTopic)
		if err != nil {
			return false, nil
		}
		span, _, err := s.topicSpan(ctx, ordersTopic)
		if err != nil {
			return false, nil
		}
		for p, e := range beforeLast {
			if starts[p] < e && span[p][1] > e {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("devctl: retention did not delete the older segments of %s: %w", ordersTopic, err)
	}
	// Remove the overrides before order-sync comes back, so nothing more is
	// deleted: the loss is exactly the deleted batches.
	if err := s.fixRetention(ctx, ordersTopic); err != nil {
		return err
	}
	if err := s.compose(ctx, nil, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not rejoin the group: %w", err)
	}
	if err := s.waitMarkerOrder(ctx); err != nil {
		return err
	}
	missing, err := s.missingIn(ctx, lo, hi)
	if err != nil {
		return err
	}
	if missing == 0 {
		return fmt.Errorf("devctl: every order %d..%d reached the warehouse: retention deleted nothing order-sync had not read", lo, hi)
	}
	s.log.Info("broken: retention deleted change events of "+ordersTopic+" before order-sync read them; order-sync resumed at the log start with lag 0 — recover with pg_resnapshot_rows (needs OWNERS_FILE="+recoveryOwnersFile+")",
		"orders", fmt.Sprintf("%d..%d", lo, hi), "missing_from_warehouse", missing)
	return nil
}

// insertLostBatches writes the lost orders in batches spaced past
// segment.ms, each once the previous one is on the topic, and returns their
// id span and each partition's end offset before the last batch.
func (s *stack) insertLostBatches(ctx context.Context) (lo, hi int64, beforeLast map[int32]int64, err error) {
	gap := s.batchGap
	if gap <= 0 {
		gap = batchGapDefault
	}
	span, _, err := s.topicSpan(ctx, ordersTopic)
	if err != nil {
		return 0, 0, nil, err
	}
	written := endTotal(span)
	for b := 0; b < lostBatches; b++ {
		if b > 0 {
			select {
			case <-ctx.Done():
				return 0, 0, nil, ctx.Err()
			case <-time.After(gap):
			}
		}
		if b == lostBatches-1 {
			now, _, err := s.topicSpan(ctx, ordersTopic)
			if err != nil {
				return 0, 0, nil, err
			}
			beforeLast = map[int32]int64{}
			for p, se := range now {
				beforeLast[p] = se[1]
			}
		}
		out, err := s.psql(ctx, "orders", sqlLostBatch(b))
		if err != nil {
			return 0, 0, nil, err
		}
		blo, bhi, err := parseIDSpan(out)
		if err != nil {
			return 0, 0, nil, err
		}
		if b == 0 {
			lo = blo
		}
		hi = bhi
		written += lostBatchSize
		want := written
		if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
			span, _, err := s.topicSpan(ctx, ordersTopic)
			if err != nil {
				return false, nil
			}
			return endTotal(span) >= want, nil
		}); err != nil {
			return 0, 0, nil, fmt.Errorf("devctl: batch %d of the lost orders did not reach %s: %w", b+1, ordersTopic, err)
		}
	}
	return lo, hi, beforeLast, nil
}

// endTotal sums the end offsets of a span.
func endTotal(span topicSpan) int64 {
	var n int64
	for _, se := range span {
		n += se[1]
	}
	return n
}

// The fix: the overrides go (if still there), then what pg_resnapshot_rows
// does: one signal row asking Debezium to re-emit the lost id range, until the
// warehouse has every order of it; then the connector, publication and topics
// go back to the baseline. Idempotent: with nothing missing no signal is sent.
func fixRowsLostAfterRetention(ctx context.Context, s *stack) error {
	if err := s.fixRetention(ctx, ordersTopic); err != nil {
		return err
	}
	lo, hi, ok, err := s.markedSpan(ctx, sqlLostSpan)
	if err != nil {
		return err
	}
	if ok {
		if err := s.resnapshotMissing(ctx, lo, hi); err != nil {
			return err
		}
	} else {
		s.log.Info("skipped: no order of the break in the source, nothing to re-emit")
	}
	if err := s.disableRecovery(ctx); err != nil {
		return err
	}
	s.log.Info("fixed: retention restored, lost orders re-emitted, connector and topics back to the dev baseline")
	return nil
}

func (s *stack) resnapshotMissing(ctx context.Context, lo, hi int64) error {
	missing, err := s.missingIn(ctx, lo, hi)
	if err != nil {
		return err
	}
	if missing == 0 {
		s.log.Info("skipped: every order of the break is in the warehouse", "orders", fmt.Sprintf("%d..%d", lo, hi))
		return nil
	}
	exists, err := s.psql(ctx, "orders", sqlSignalExists)
	if err != nil {
		return err
	}
	if exists != "t" {
		if err := s.enableRecovery(ctx); err != nil {
			return err
		}
	}
	if err := s.compose(ctx, nil, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlResnapshotSignal(lo, hi)); err != nil {
		return err
	}
	s.log.Info("sent an incremental snapshot signal for the lost orders", "orders", fmt.Sprintf("%d..%d", lo, hi), "missing", missing)
	if err := s.waitFor(ctx, 120*time.Second, func() (bool, error) {
		n, err := s.missingIn(ctx, lo, hi)
		if err != nil {
			return false, nil
		}
		missing = n
		return n == 0, nil
	}); err != nil {
		return fmt.Errorf("devctl: %d orders of %d..%d still missing from the warehouse after the snapshot signal: %w", missing, lo, hi, err)
	}
	return nil
}

// ----------------------------------------------------- consumer-skipped-range

func breakConsumerSkippedRange(ctx context.Context, s *stack) error {
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: order-sync is not running (start the stack first): %w", err)
	}
	if err := s.enableRecovery(ctx); err != nil {
		return err
	}
	if err := s.compose(ctx, nil, "stop", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Empty", 0, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not leave the group: %w", err)
	}
	span, _, err := s.topicSpan(ctx, ordersTopic)
	if err != nil {
		return err
	}
	want := endTotal(span) + skippedOrders
	out, err := s.psql(ctx, "orders", sqlInsertSkipped)
	if err != nil {
		return err
	}
	lo, hi, err := parseIDSpan(out)
	if err != nil {
		return err
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		span, _, err := s.topicSpan(ctx, ordersTopic)
		if err != nil {
			return false, nil
		}
		return endTotal(span) >= want, nil
	}); err != nil {
		return fmt.Errorf("devctl: the %d orders did not reach %s: %w", skippedOrders, ordersTopic, err)
	}
	// The deploy job's leftover step: --reset-offsets --to-latest.
	if err := s.resetGroupToEnd(ctx, ordersGroup, ordersTopic); err != nil {
		return err
	}
	if err := s.compose(ctx, nil, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not rejoin the group: %w", err)
	}
	if err := s.waitMarkerOrder(ctx); err != nil {
		return err
	}
	missing, err := s.missingIn(ctx, lo, hi)
	if err != nil {
		return err
	}
	if missing != hi-lo+1 {
		return fmt.Errorf("devctl: %d of the orders %d..%d reached the warehouse: the reset did not skip them", hi-lo+1-missing, lo, hi)
	}
	s.log.Info("broken: order-sync's offsets were moved to the end of "+ordersTopic+" past orders still on the topic; lag 0 — recover with kafka_replay_range into "+reprocessTopic+" (needs OWNERS_FILE="+recoveryOwnersFile+")",
		"orders", fmt.Sprintf("%d..%d", lo, hi), "missing_from_warehouse", missing)
	return nil
}

// The fix replays order-sync from the start of the topic (the load is an
// upsert by id, so that is safe on the dev stack), then removes the signal
// table and the reprocess topic. It does not run kafka_replay_range: devconsumer
// reads one topic, so the copy would load only with a second devconsumer on
// the reprocess topic (dev/scenarios.md §19). Idempotent.
func fixConsumerSkippedRange(ctx context.Context, s *stack) error {
	lo, hi, ok, err := s.markedSpan(ctx, sqlSkippedSpan)
	if err != nil {
		return err
	}
	missing := int64(0)
	if ok {
		if missing, err = s.missingIn(ctx, lo, hi); err != nil {
			return err
		}
	}
	if missing > 0 {
		if err := s.compose(ctx, nil, "stop", "devconsumer"); err != nil {
			return err
		}
		if err := s.waitGroupState(ctx, ordersGroup, "Empty", 0, 60*time.Second); err != nil {
			return fmt.Errorf("devctl: devconsumer did not leave the group: %w", err)
		}
		if err := s.resetGroupToStart(ctx, ordersGroup, ordersTopic); err != nil {
			return err
		}
	} else {
		s.log.Info("skipped: no skipped order is missing from the warehouse")
	}
	if err := s.compose(ctx, nil, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitLagZero(ctx, ordersGroup, 120*time.Second); err != nil {
		return err
	}
	if missing > 0 {
		if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
			n, err := s.missingIn(ctx, lo, hi)
			if err != nil {
				return false, nil
			}
			missing = n
			return n == 0, nil
		}); err != nil {
			return fmt.Errorf("devctl: %d orders of %d..%d still missing after the replay: %w", missing, lo, hi, err)
		}
	}
	if err := s.disableRecovery(ctx); err != nil {
		return err
	}
	if ok {
		s.log.Info("fixed: order-sync replayed "+ordersTopic+" from the start, every skipped order loaded, connector and topics back to the dev baseline",
			"orders", fmt.Sprintf("%d..%d", lo, hi))
	} else {
		s.log.Info("fixed: connector and topics back to the dev baseline")
	}
	return nil
}
