package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The silent-corruption scenarios (X1g, tasks X1: "Silent corruption becomes a
// symptom"). Nothing fails in any of them: the connector runs, the slot is
// active, order-sync is Stable with lag 0 and `devctl health` is green, yet the
// warehouse holds wrong data. Only a source-versus-sink comparison (the
// watcher's count_drift, row_drift and dup_keys signals) or a read of the topic
// sees them. They all use public.orders and analytics.orders, so the baseline
// dev/owners.yaml is enough.
//
// Each break keeps `devctl health` green on purpose: health measures "pending"
// rows by updated_at against the newest row in the sink, so a break that leaves
// a row older than the sink's newest updated_at is invisible to it, while a
// break that leaves a row newer would turn health red after 60 s. How each
// break stays older is named at its section.

const (
	// sink-filtered-rows
	backfillStatus = "backfill"
	backfillOrders = 40

	// sink-dropped-column
	droppedColumn  = "updated_at"
	droppedOrders  = 20
	sqlSinkNullTS  = "SELECT count(*) FROM analytics.orders WHERE updated_at IS NULL"
	sqlPinnedStart = "INSERT INTO orders (customer_id, amount, status, updated_at) SELECT g, 800 + g, 'placed', 'epoch'::timestamptz + ("

	// consumer-duplicates
	dupSourceOrders = 10   // fresh orders written so the topic has a recent tail on an idle stack
	dupCandidates   = 100  // records per partition the break picks its copies from
	dupCopies       = 10   // records the break publishes a second time
	dupWindow       = 5000 // records per partition the duplicate count reads (devctl reads at most maxReplayRecords)

	// toast-placeholder
	toastOrders       = 3
	toastStatusPrefix = "toast-big-"
	// toastPlaceholder is Debezium's default unavailable.value.placeholder.
	toastPlaceholder = "__debezium_unavailable_value"
)

// SQL of the silent scenarios, named so the unit tests' fake Postgres answers
// exactly these statements. The counts and the placeholder are constants of
// this file; the only interpolated values are integers parsed from the stack's
// own answers.
var (
	sqlInsertBackfill = fmt.Sprintf("INSERT INTO orders (customer_id, amount, status) SELECT g, 900 + g, '%s' FROM generate_series(1, %d) g", backfillStatus, backfillOrders)
	sqlSourceBackfill = fmt.Sprintf("SELECT count(*) FROM orders WHERE status = '%s'", backfillStatus)
	sqlSinkBackfill   = fmt.Sprintf("SELECT count(*) FROM analytics.orders WHERE status = '%s'", backfillStatus)

	sqlInsertDupSource = fmt.Sprintf("INSERT INTO orders (customer_id, amount) SELECT g, 600 + g FROM generate_series(1, %d) g", dupSourceOrders)

	sqlReplicaDefault = "ALTER TABLE public.orders REPLICA IDENTITY DEFAULT"
	sqlReplicaFull    = "ALTER TABLE public.orders REPLICA IDENTITY FULL"
	// One 12.8 kB hex string (400 md5s) per statement: random hex does not
	// compress below the 2 kB threshold, so Postgres stores it out of line
	// (TOAST). The prefix marks the rows this scenario wrote.
	sqlInsertToast = fmt.Sprintf("WITH i AS (INSERT INTO orders (customer_id, amount, status) "+
		"SELECT g, 300 + g, '%s' || (SELECT string_agg(md5(random()::text || h::text), '') FROM generate_series(1, 400) h) "+
		"FROM generate_series(1, %d) g RETURNING id) SELECT min(id) || ' ' || max(id) FROM i", toastStatusPrefix, toastOrders)
	sqlSinkPlaceholders   = fmt.Sprintf("SELECT count(*) FROM analytics.orders WHERE status = '%s'", toastPlaceholder)
	sqlSinkToastRows      = fmt.Sprintf("SELECT count(*) FROM analytics.orders WHERE status LIKE '%s%%'", toastStatusPrefix)
	sqlSinkPlaceholderIDs = fmt.Sprintf("SELECT coalesce(string_agg(id::text, ','), '') FROM analytics.orders WHERE status = '%s'", toastPlaceholder)
)

// sqlPinnedInsert writes droppedOrders orders whose updated_at is micros
// (epoch microseconds) exactly, the way late-arriving rows carry an old
// timestamp. Only the integer is interpolated.
func sqlPinnedInsert(micros int64) string {
	return sqlPinnedStart + strconv.FormatInt(micros, 10) + " * interval '1 microsecond') FROM generate_series(1, " + strconv.Itoa(droppedOrders) + ") g"
}

// sqlToastRepair updates the source rows ids so the new value is written in
// full, not as an unchanged TOAST pointer: status || ” builds a new datum.
func sqlToastRepair(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	return "UPDATE orders SET status = status || '', updated_at = now() WHERE id IN (" + strings.Join(parts, ",") + ")"
}

// sqlToastTouch updates a non-TOAST column of the rows lo..hi.
func sqlToastTouch(lo, hi int64) string {
	return fmt.Sprintf("UPDATE orders SET amount = amount + 1, updated_at = now() WHERE id BETWEEN %d AND %d", lo, hi)
}

// countOf runs a single-value count query.
func (s *stack) countOf(ctx context.Context, db, query string) (int64, error) {
	out, err := s.psql(ctx, db, query)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("devctl: parse count %q: %w", out, err)
	}
	return n, nil
}

// replaySink stops devconsumer, resets order-sync to the start of the orders
// topic and starts devconsumer again with env (the knobs of a good build). The
// warehouse load is an upsert by id, so replaying everything is safe and ends
// in the same state when it runs twice.
func (s *stack) replaySink(ctx context.Context, env map[string]string) error {
	if err := s.compose(ctx, nil, "stop", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Empty", 0, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not leave the group: %w", err)
	}
	if err := s.resetGroupToStart(ctx, ordersGroup, ordersTopic); err != nil {
		return err
	}
	if err := s.compose(ctx, env, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not rejoin the group: %w", err)
	}
	return s.waitLagZero(ctx, ordersGroup, 120*time.Second)
}

// ------------------------------------------------------------ sink-filtered-rows

// sink-filtered-rows: a devconsumer release added a filter (SKIP_WHERE
// status=backfill) that drops every order in status "backfill" without loading
// it, and commits the offset. The connector produces the rows, the consumer
// reads them, lag is 0 and the warehouse is fresh, but the sink holds fewer
// rows than the source: the count drifts and nothing says so.
//
// health stays green because a normal order is written after the backfill
// rows: it reaches the warehouse, so the sink's newest updated_at is later than
// every skipped row's.
func breakSinkFilteredRows(ctx context.Context, s *stack) error {
	env := map[string]string{"DEVCONSUMER_SKIP_WHERE": "status=" + backfillStatus}
	if err := s.compose(ctx, env, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: filtering build did not join the group: %w", err)
	}
	before, err := s.backfillMissing(ctx)
	if err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlInsertBackfill); err != nil {
		return err
	}
	// The marker is read after the backfill rows (same topic partition), so
	// when it is in the warehouse the consumer has passed them.
	if err := s.waitMarkerOrder(ctx); err != nil {
		return err
	}
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: filtering build did not commit past the backfill orders: %w", err)
	}
	after, err := s.backfillMissing(ctx)
	if err != nil {
		return err
	}
	if after < before+backfillOrders {
		return fmt.Errorf("devctl: %d '%s' orders are missing from the warehouse, want at least %d: the filtering build did not skip them", after, backfillStatus, before+backfillOrders)
	}
	src, err := s.countOf(ctx, "orders", sqlCountSourceOrders)
	if err != nil {
		return err
	}
	wh, err := s.countOf(ctx, "warehouse", sqlCountWarehouse)
	if err != nil {
		return err
	}
	s.log.Info("broken: a devconsumer build drops orders in status '"+backfillStatus+"' and commits past them; lag 0, nothing fails",
		"backfill_missing", after, "source_orders", src, "warehouse_orders", wh)
	return nil
}

// backfillMissing is how many backfill orders the source has that the
// warehouse lacks.
func (s *stack) backfillMissing(ctx context.Context) (int64, error) {
	src, err := s.countOf(ctx, "orders", sqlSourceBackfill)
	if err != nil {
		return 0, err
	}
	wh, err := s.countOf(ctx, "warehouse", sqlSinkBackfill)
	if err != nil {
		return 0, err
	}
	return src - wh, nil
}

// The fix: deploy the build without the filter and replay the topic from its
// start (the upsert makes that safe), then check that no backfill order is
// missing. The backfill rows stay in the source: they are ordinary orders.
func fixSinkFilteredRows(ctx context.Context, s *stack) error {
	if err := s.replaySink(ctx, map[string]string{"DEVCONSUMER_SKIP_WHERE": ""}); err != nil {
		return err
	}
	var missing int64
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		m, err := s.backfillMissing(ctx)
		if err != nil {
			return false, nil
		}
		missing = m
		return m == 0, nil
	}); err != nil {
		return fmt.Errorf("devctl: %d '%s' orders still missing from the warehouse after the replay: %w", missing, backfillStatus, err)
	}
	s.log.Info("fixed: devconsumer runs without the filter, order-sync replayed from the start of " + ordersTopic + ", no '" + backfillStatus + "' order missing")
	return nil
}

// ----------------------------------------------------------- sink-dropped-column

// sink-dropped-column: a devconsumer release rewrote its upsert and no longer
// writes updated_at (DROP_COLUMNS=updated_at). Every order loaded after it has
// updated_at NULL in the warehouse while the source has the real timestamp.
// Counts match, lag is 0.
//
// health stays green because the new orders carry the sink's own newest
// updated_at (late-arriving rows): they are never "pending" by that measure.
func breakSinkDroppedColumn(ctx context.Context, s *stack) error {
	newest, err := s.warehouseNewest(ctx)
	if err != nil {
		return err
	}
	if newest.IsZero() {
		return errors.New("devctl: analytics.orders has no updated_at yet — run `devctl reset` or wait for the sink to load")
	}
	env := map[string]string{"DEVCONSUMER_DROP_COLUMNS": droppedColumn}
	if err := s.compose(ctx, env, "up", "-d", "--no-deps", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: build without %s did not join the group: %w", droppedColumn, err)
	}
	before, err := s.countOf(ctx, "warehouse", sqlSinkNullTS)
	if err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlPinnedInsert(newest.UnixMicro())); err != nil {
		return err
	}
	var nulls int64
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		n, err := s.countOf(ctx, "warehouse", sqlSinkNullTS)
		if err != nil {
			return false, nil
		}
		nulls = n
		return n >= before+droppedOrders, nil
	}); err != nil {
		return fmt.Errorf("devctl: warehouse has %d orders with %s NULL, want at least %d: %w", nulls, droppedColumn, before+droppedOrders, err)
	}
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return err
	}
	s.log.Info("broken: a devconsumer build no longer writes "+droppedColumn+"; new orders have it NULL in the warehouse, lag 0, nothing fails",
		"warehouse_null_"+droppedColumn, nulls)
	return nil
}

// The fix: deploy the build that writes the column and replay the topic from
// its start, so the upsert fills every NULL; then check that none is left.
func fixSinkDroppedColumn(ctx context.Context, s *stack) error {
	if err := s.replaySink(ctx, map[string]string{"DEVCONSUMER_DROP_COLUMNS": ""}); err != nil {
		return err
	}
	var nulls int64
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		n, err := s.countOf(ctx, "warehouse", sqlSinkNullTS)
		if err != nil {
			return false, nil
		}
		nulls = n
		return n == 0, nil
	}); err != nil {
		return fmt.Errorf("devctl: %d orders still have %s NULL after the replay: %w", nulls, droppedColumn, err)
	}
	s.log.Info("fixed: devconsumer writes " + droppedColumn + " again, order-sync replayed from the start of " + ordersTopic + ", no NULL left in the warehouse")
	return nil
}

// ------------------------------------------------------------ consumer-duplicates

// consumer-duplicates: the same change events are on pg.public.orders twice,
// as after a redelivery (an unclean Connect restart that replays from the last
// flushed offset, or a relay re-publishing a window): same key, same payload,
// so the same source LSN. devconsumer reprocesses them without harm (the load
// is an upsert), so the warehouse looks right and nothing lags, but the topic
// has duplicate (key, lsn) pairs, which is what dup_keys and kafka_dup_keys
// read.
//
// A consumer cannot create these (it only reads), so the break republishes a
// window of the topic's own tail, the newest event of each key in it, so the
// per-key LSN order is not broken (a duplicate, not a regression).
func breakConsumerDuplicates(ctx context.Context, s *stack) error {
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: order-sync must be running and caught up first (devctl fix consumer-lag): %w", err)
	}
	if _, err := s.psql(ctx, "orders", sqlInsertDupSource); err != nil {
		return err
	}
	if err := s.waitMarkerOrder(ctx); err != nil {
		return err
	}
	before, err := s.duplicateEvents(ctx)
	if err != nil {
		return err
	}
	tail, err := s.readTopicTail(ctx, ordersTopic, dupCandidates)
	if err != nil {
		return err
	}
	copies := duplicateCandidates(tail, dupCopies)
	if len(copies) == 0 {
		return fmt.Errorf("devctl: %s has no change event to publish twice", ordersTopic)
	}
	if err := s.produce(ctx, replayRecords(copies, ordersTopic)); err != nil {
		return err
	}
	// devconsumer reprocesses the copies: lag goes back to 0.
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return err
	}
	after, err := s.duplicateEvents(ctx)
	if err != nil {
		return err
	}
	if after < before+int64(len(copies)) {
		return fmt.Errorf("devctl: %s holds %d duplicate events, want at least %d: the break did not hold", ordersTopic, after, before+int64(len(copies)))
	}
	s.log.Info("broken: "+ordersTopic+" holds the same change events twice (same key, same source LSN); devconsumer reprocessed them, lag 0, nothing fails",
		"duplicate_events", after, "published_again", len(copies))
	return nil
}

// duplicateEvents counts the records in the topic's tail that repeat an
// earlier record's key and value byte for byte. Debezium puts the source LSN
// in the value, so identical bytes are the same event.
func (s *stack) duplicateEvents(ctx context.Context) (int64, error) {
	recs, err := s.readTopicTail(ctx, ordersTopic, dupWindow)
	if err != nil {
		return 0, err
	}
	return countDuplicateEvents(recs), nil
}

// countDuplicateEvents is how many records repeat the key and value of an
// earlier one. Tombstones (no value) are not events.
func countDuplicateEvents(recs []*kgo.Record) int64 {
	seen := make(map[string]bool, len(recs))
	var dups int64
	for _, r := range recs {
		if len(r.Value) == 0 {
			continue
		}
		// Length-prefixed: an Avro key starts with a zero byte, so a plain
		// separator would not be unambiguous.
		id := strconv.Itoa(len(r.Key)) + ":" + string(r.Key) + string(r.Value)
		if seen[id] {
			dups++
			continue
		}
		seen[id] = true
	}
	return dups
}

// duplicateCandidates picks up to limit records to publish again: the newest
// record of each key (a tombstone or an empty key is skipped), in their
// original order. Taking only the newest per key keeps each key's source LSN
// non-decreasing once the copies are appended.
func duplicateCandidates(recs []*kgo.Record, limit int) []*kgo.Record {
	seen := map[string]bool{}
	var picked []*kgo.Record
	for i := len(recs) - 1; i >= 0 && len(picked) < limit; i-- {
		r := recs[i]
		if len(r.Value) == 0 || len(r.Key) == 0 || seen[string(r.Key)] {
			continue
		}
		seen[string(r.Key)] = true
		picked = append(picked, r)
	}
	for i, j := 0, len(picked)-1; i < j; i, j = i+1, j-1 {
		picked[i], picked[j] = picked[j], picked[i]
	}
	return picked
}

// The fix: Kafka cannot delete single records, so the fix cuts the log start of
// each partition up to what order-sync has committed, minus the last record
// (kept so the topic is never empty: `devctl health` reads the newest record's
// timestamp). The duplicates all sit before the end of the log, and the group
// has consumed them, so nothing is lost for the sink; the topic's older history
// is gone, as with `kafka-delete-records`. A topic without duplicates is left
// alone, so a second run is a no-op.
func fixConsumerDuplicates(ctx context.Context, s *stack) error {
	n, err := s.duplicateEvents(ctx)
	if err != nil {
		return err
	}
	if n == 0 {
		s.log.Info("skipped: " + ordersTopic + " holds no duplicate event")
		return nil
	}
	// The cut is bounded by the committed offsets: wait until the group has
	// consumed everything, so no record it still needs is deleted.
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: order-sync must be running and caught up before the topic is cut (devctl fix consumer-lag): %w", err)
	}
	cut, err := s.cutTopicToCommitted(ctx, ordersGroup, ordersTopic)
	if err != nil {
		return err
	}
	left, err := s.duplicateEvents(ctx)
	if err != nil {
		return err
	}
	if left != 0 {
		return fmt.Errorf("devctl: %s still holds %d duplicate events after the cut", ordersTopic, left)
	}
	s.log.Info("fixed: "+ordersTopic+" cut to the committed offsets of "+ordersGroup+" (its last record kept), no duplicate event left",
		"duplicate_events_before", n, "partitions_cut", cut)
	return nil
}

// cutTopicToCommitted moves each partition's log start to the group's
// committed offset, but never past the last record. It returns how many
// partitions moved.
func (s *stack) cutTopicToCommitted(ctx context.Context, group, topic string) (int, error) {
	committed, err := s.groupCommittedOffsets(ctx, group, topic)
	if err != nil {
		return 0, err
	}
	span, ok, err := s.topicSpan(ctx, topic)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("devctl: topic %s does not exist", topic)
	}
	offsets := kadm.Offsets{}
	for p, se := range span {
		to, has := committed[p]
		if !has {
			continue
		}
		to = min(to, se[1]-1)
		if to > se[0] {
			offsets.AddOffset(topic, p, to, -1)
		}
	}
	if len(offsets) == 0 {
		return 0, nil
	}
	cl, err := s.kafkaClient()
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).DeleteRecords(ctx, offsets)
	if err != nil {
		return 0, fmt.Errorf("devctl: delete records of %s: %w", topic, err)
	}
	if err := resp.Error(); err != nil {
		return 0, fmt.Errorf("devctl: delete records of %s: %w", topic, err)
	}
	return len(offsets[topic]), nil
}

// ------------------------------------------------------------- toast-placeholder

// toast-placeholder: public.orders is switched from REPLICA IDENTITY FULL (the
// dev default) to DEFAULT, then orders with a large status (text, stored out of
// line by TOAST) are written and updated in a column that is not TOAST. The
// update's WAL record carries no value for the unchanged TOAST column, so
// Debezium emits `__debezium_unavailable_value` in its place, and devconsumer
// loads that string into analytics.orders.status. Updated_at moves, counts
// match, lag is 0: the sink holds a placeholder where the source has data.
func breakToastPlaceholder(ctx context.Context, s *stack) error {
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: order-sync must be running and caught up first (devctl fix consumer-lag): %w", err)
	}
	before, err := s.countOf(ctx, "warehouse", sqlSinkPlaceholders)
	if err != nil {
		return err
	}
	// Before the updates: the replica identity decides what each WAL record
	// carries, and the setting is read when the record is written.
	if _, err := s.psql(ctx, "orders", sqlReplicaDefault); err != nil {
		return err
	}
	span, err := s.psql(ctx, "orders", sqlInsertToast)
	if err != nil {
		return err
	}
	lo, hi, err := parseIDSpan(span)
	if err != nil {
		return err
	}
	// The insert event has the full value; wait for it so the update, not the
	// insert, is what the sink ends up with.
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		n, err := s.countOf(ctx, "warehouse", fmt.Sprintf("SELECT count(*) FROM analytics.orders WHERE id BETWEEN %d AND %d", lo, hi))
		if err != nil {
			return false, nil
		}
		return n == hi-lo+1, nil
	}); err != nil {
		return fmt.Errorf("devctl: the new large orders %d..%d did not reach the warehouse: %w", lo, hi, err)
	}
	if _, err := s.psql(ctx, "orders", sqlToastTouch(lo, hi)); err != nil {
		return err
	}
	var now int64
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		n, err := s.countOf(ctx, "warehouse", sqlSinkPlaceholders)
		if err != nil {
			return false, nil
		}
		now = n
		return n >= before+toastOrders, nil
	}); err != nil {
		return fmt.Errorf("devctl: warehouse has %d placeholder values, want at least %d (did Debezium emit the real value? check unavailable.value.placeholder): %w", now, before+toastOrders, err)
	}
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return err
	}
	s.log.Info("broken: public.orders has REPLICA IDENTITY DEFAULT and large text was updated in another column; Debezium emitted "+toastPlaceholder+" and the sink loaded it, lag 0, nothing fails",
		"warehouse_placeholder_rows", now, "orders", fmt.Sprintf("%d..%d", lo, hi))
	return nil
}

// The fix: REPLICA IDENTITY FULL again (the dev default, dev/postgres-init),
// then rewrite the status of every row the sink holds as a placeholder, so the
// change event carries the full value, and wait until none is left. Idempotent:
// with no placeholder row only the (no-op) ALTER runs.
func fixToastPlaceholder(ctx context.Context, s *stack) error {
	if _, err := s.psql(ctx, "orders", sqlReplicaFull); err != nil {
		return err
	}
	out, err := s.psql(ctx, "warehouse", sqlSinkPlaceholderIDs)
	if err != nil {
		return err
	}
	ids, err := parseIDList(out)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		s.log.Info("skipped: no row of analytics.orders holds " + toastPlaceholder)
	} else {
		if _, err := s.psql(ctx, "orders", sqlToastRepair(ids)); err != nil {
			return err
		}
		var left int64
		if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
			n, err := s.countOf(ctx, "warehouse", sqlSinkPlaceholders)
			if err != nil {
				return false, nil
			}
			left = n
			return n == 0, nil
		}); err != nil {
			return fmt.Errorf("devctl: %d rows of analytics.orders still hold %s after the rewrite: %w", left, toastPlaceholder, err)
		}
	}
	if err := s.waitLagZero(ctx, ordersGroup, 60*time.Second); err != nil {
		return err
	}
	s.log.Info("fixed: public.orders has REPLICA IDENTITY FULL again and no row of analytics.orders holds "+toastPlaceholder, "rows_rewritten", len(ids))
	return nil
}

// parseIDList decodes "1,2,3" (empty = none). The ids go back into SQL, so
// anything that is not an integer is refused.
func parseIDList(out string) ([]int64, error) {
	out = strings.TrimSpace(out)
	if out == "" {
		return nil, nil
	}
	var ids []int64
	for _, f := range strings.Split(out, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(f), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("devctl: parse id list %q: %w", out, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
