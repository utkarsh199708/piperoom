package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// retention-loss: someone lowered the CDC topic's retention so the broker
// deleted the records before order-sync read them. The group's committed
// offset now points before the log start, so the consumer cannot resume where
// it left off and the watcher fires offset_below_log_start (sev1, at once).
//
// The break re-creates that: stop the consumer (its offsets stay committed),
// shrink retention.ms and segment.ms so segments roll and are deleted quickly,
// insert rows (Debezium produces them, creating new segments), and wait until
// the log start has moved past the group's committed offset. The fix restores
// the topic config and restarts the consumer, which resets to the earliest
// offset still on the topic (auto.offset.reset=earliest) and catches up.

// retentionOffsets are the number of orders the break inserts to roll segments.
const retentionOrders = 50

// sqlInsertRetentionLoss inserts a batch of orders after the consumer stopped;
// Debezium streams them to pg.public.orders, so the active segment rolls and
// the old one (holding the committed offset) is deleted by the short retention.
var sqlInsertRetentionLoss = fmt.Sprintf("INSERT INTO orders (customer_id, amount) SELECT g, 200 + g FROM generate_series(1, %d) g", retentionOrders)

func breakRetentionLoss(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, nil, "stop", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Empty", 0, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not leave the group: %w", err)
	}
	// Short retention and segment roll: with the broker's 1 s retention check
	// (dev/docker-compose.yml KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS) a rolled
	// segment is deleted about a second after it ages past retention.ms.
	if err := s.setTopicConfig(ctx, ordersTopic, map[string]string{
		"retention.ms": "1000",
		"segment.ms":   "1000",
	}); err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlInsertRetentionLoss); err != nil {
		return err
	}
	var parts []int32
	var lastErr error
	if err := s.waitFor(ctx, 90*time.Second, func() (bool, error) {
		below, ps, err := s.committedBelowLogStart(ctx, ordersGroup, ordersTopic)
		if err != nil {
			lastErr = err
			return false, nil
		}
		parts = ps
		return below, nil
	}); err != nil {
		if lastErr != nil {
			return fmt.Errorf("devctl: %s's committed offsets did not fall below the log start of %s: %w (last error: %v)", ordersGroup, ordersTopic, err, lastErr)
		}
		return fmt.Errorf("devctl: %s's committed offsets did not fall below the log start of %s: %w", ordersGroup, ordersTopic, err)
	}
	s.log.Info("broken: "+ordersTopic+" retention cut so its log start moved past "+ordersGroup+"'s committed offsets — the watcher fires offset_below_log_start",
		"partitions_below", fmt.Sprint(parts))
	return nil
}

func fixRetentionLoss(ctx context.Context, s *stack) error {
	// Restore the broker defaults by deleting the overrides; safe to repeat.
	if err := s.deleteTopicConfig(ctx, ordersTopic, "retention.ms", "segment.ms"); err != nil {
		return err
	}
	if err := s.compose(ctx, nil, "start", "devconsumer"); err != nil {
		return err
	}
	if err := s.waitGroupState(ctx, ordersGroup, "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: devconsumer did not rejoin the group: %w", err)
	}
	s.log.Info("fixed: " + ordersTopic + " retention and segment roll restored, devconsumer running again")
	return nil
}

// setTopicConfig sets topic config keys to the given values.
func (s *stack) setTopicConfig(ctx context.Context, topic string, kvs map[string]string) error {
	cl, err := s.kafkaClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	var cfgs []kadm.AlterConfig
	for _, k := range slices.Sorted(maps.Keys(kvs)) {
		cfgs = append(cfgs, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: kadm.StringPtr(kvs[k])})
	}
	resp, err := kadm.NewClient(cl).AlterTopicConfigs(ctx, cfgs, topic)
	if err != nil {
		return fmt.Errorf("devctl: alter topic configs of %s: %w", topic, err)
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("devctl: alter topic config of %s: %w", r.Name, r.Err)
		}
	}
	return nil
}

// deleteTopicConfig deletes the topic config overrides of keys, restoring the
// broker defaults. Deleting a key that is not overridden is not an error.
func (s *stack) deleteTopicConfig(ctx context.Context, topic string, keys ...string) error {
	cl, err := s.kafkaClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	cfgs := make([]kadm.AlterConfig, 0, len(keys))
	for _, k := range keys {
		cfgs = append(cfgs, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
	}
	resp, err := kadm.NewClient(cl).AlterTopicConfigs(ctx, cfgs, topic)
	if err != nil {
		return fmt.Errorf("devctl: delete topic configs of %s: %w", topic, err)
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("devctl: delete topic config of %s: %w", r.Name, r.Err)
		}
	}
	return nil
}

// committedBelowLogStart reports whether the group's committed offset on any
// partition of topic is below that partition's log start offset, i.e. the
// records at the committed position were deleted by retention. It also returns
// the affected partitions, sorted.
func (s *stack) committedBelowLogStart(ctx context.Context, group, topic string) (bool, []int32, error) {
	if s.belowLogStartFn != nil {
		return s.belowLogStartFn(ctx, group, topic)
	}
	committed, err := s.groupCommittedOffsets(ctx, group, topic)
	if err != nil {
		return false, nil, err
	}
	start, err := s.logStartOffsets(ctx, topic)
	if err != nil {
		return false, nil, err
	}
	below, parts := belowLogStart(committed, start)
	return below, parts, nil
}

// groupCommittedOffsets returns the group's committed offset per partition of
// topic. A partition with no committed offset is absent.
func (s *stack) groupCommittedOffsets(ctx context.Context, group, topic string) (map[int32]int64, error) {
	cl, err := s.kafkaClient()
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	fetched, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("devctl: fetch offsets of %s: %w", group, err)
	}
	if err := fetched.Error(); err != nil {
		return nil, fmt.Errorf("devctl: fetch offsets of %s: %w", group, err)
	}
	out := map[int32]int64{}
	for p, o := range fetched[topic] {
		if o.Err != nil || o.At < 0 {
			continue
		}
		out[p] = o.At
	}
	return out, nil
}

// logStartOffsets returns the log start offset per partition of topic.
func (s *stack) logStartOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	cl, err := s.kafkaClient()
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	starts, err := kadm.NewClient(cl).ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("devctl: start offsets of %s: %w", topic, err)
	}
	out := map[int32]int64{}
	starts.Each(func(o kadm.ListedOffset) {
		if o.Err == nil {
			out[o.Partition] = o.Offset
		}
	})
	return out, nil
}

// belowLogStart reports whether any partition's committed offset is below the
// log start offset (the records at that offset were deleted) and which
// partitions, sorted. A partition with no committed offset is skipped.
func belowLogStart(committed, start map[int32]int64) (bool, []int32) {
	var parts []int32
	for p, c := range committed {
		if c < 0 {
			continue // no committed offset for this partition
		}
		if lo, ok := start[p]; ok && c < lo {
			parts = append(parts, p)
		}
	}
	slices.Sort(parts)
	return len(parts) > 0, parts
}
