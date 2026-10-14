package topology

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

type connectSnapshot struct {
	config map[string]string
	status clients.ConnectorStatus
	err    error
}

type groupSnapshot struct {
	lag clients.GroupLag
	err error
}

type freshSnapshot struct {
	ts  time.Time
	err error
}

// collected holds everything the health phase gathers. One goroutine writes
// each field; the mutex protects the maps.
type collected struct {
	slots    []clients.ReplicationSlot
	slotsErr error

	connect   map[string]connectSnapshot
	groups    map[string]groupSnapshot
	freshness map[string]freshSnapshot

	topicNewest  map[string]time.Time
	topicRecords map[string]int64 // sum of end-start over partitions
	topicErr     map[string]error

	now time.Time
}

// collect gathers health for every system in parallel under ctx's budget.
func (b *builder) collect(ctx context.Context, p owners.Pipeline) *collected {
	c := &collected{
		connect:      map[string]connectSnapshot{},
		groups:       map[string]groupSnapshot{},
		freshness:    map[string]freshSnapshot{},
		topicNewest:  map[string]time.Time{},
		topicRecords: map[string]int64{},
		topicErr:     map[string]error{},
		now:          b.now(),
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	run := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}

	run(func() {
		slots, err := b.src.ReplicationSlots(ctx)
		mu.Lock()
		c.slots, c.slotsErr = slots, err
		mu.Unlock()
	})
	for _, cn := range p.Connectors {
		cn := cn
		run(func() {
			cfg, err := b.connect.Config(ctx, cn)
			if err != nil {
				mu.Lock()
				c.connect[cn] = connectSnapshot{err: err}
				mu.Unlock()
				return
			}
			st, err := b.connect.Status(ctx, cn)
			mu.Lock()
			c.connect[cn] = connectSnapshot{config: cfg, status: st, err: err}
			mu.Unlock()
		})
	}
	for _, gr := range p.ConsumerGroups {
		gr := gr
		run(func() {
			lag, err := b.kafka.GroupLag(ctx, gr)
			mu.Lock()
			c.groups[gr] = groupSnapshot{lag: lag, err: err}
			mu.Unlock()
		})
	}
	for _, s := range p.SinkTables {
		s := s
		run(func() {
			ts, err := b.wh.Freshness(ctx, s, p.SinkTSColumn)
			mu.Lock()
			c.freshness[s] = freshSnapshot{ts: ts, err: err}
			mu.Unlock()
		})
	}
	wg.Wait()
	return c
}

// collectTopics fetches the newest record timestamp per topic in parallel.
// Topics depend on connector config, so this runs after collect.
func (b *builder) collectTopics(ctx context.Context, topics []string, c *collected) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, t := range topics {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			offs, err := b.kafka.TopicOffsets(ctx, t)
			var newest time.Time
			var records int64
			if err == nil {
				for _, p := range offs {
					if p.LastTimestamp.After(newest) {
						newest = p.LastTimestamp
					}
					if p.End > p.Start {
						records += p.End - p.Start
					}
				}
			}
			mu.Lock()
			c.topicNewest[t] = newest
			c.topicRecords[t] = records
			c.topicErr[t] = err
			mu.Unlock()
		}()
	}
	wg.Wait()
}

// sourceHealth decides a source table's health from its replication slot.
func sourceHealth(slot clients.ReplicationSlot, t Thresholds) (Health, map[string]string) {
	facts := map[string]string{
		"slot_retained": HumanBytes(slot.RetainedWALBytes),
		"slot_active":   strconv.FormatBool(slot.Active),
	}
	// >= matches the watcher's slot_wal rule ("retained >= fire"). t comes from
	// ThresholdsFor, so the per-pipeline watch.slot_wal.fire overrides the base
	// SlotRetainedWarnBytes default.
	if !slot.Active || slot.RetainedWALBytes >= t.SlotRetainedWarnBytes {
		return HealthWarn, facts
	}
	return HealthOK, facts
}

// connectorHealth decides a connector's health from its status.
func connectorHealth(s clients.ConnectorStatus) (Health, map[string]string) {
	facts := map[string]string{}
	failed := s.State == "FAILED"
	failedTasks := 0
	pausedTasks := 0
	for _, task := range s.Tasks {
		switch task.State {
		case "FAILED":
			failed = true
			failedTasks++
		case "PAUSED", "UNASSIGNED", "STOPPED":
			pausedTasks++
		}
	}
	if failed {
		facts["tasks_failed"] = strconv.Itoa(failedTasks)
		if failedTasks == 0 {
			facts["state"] = s.State
		}
		return HealthFail, facts
	}
	switch s.State {
	case "PAUSED", "UNASSIGNED", "STOPPED":
		facts["state"] = s.State
		return HealthWarn, facts
	}
	if pausedTasks > 0 {
		facts["tasks_paused"] = strconv.Itoa(pausedTasks)
		return HealthWarn, facts
	}
	return HealthOK, facts
}

// groupHealth decides a consumer group's health from its lag.
func groupHealth(lag clients.GroupLag, t Thresholds) (Health, map[string]string) {
	facts := map[string]string{
		"lag":   strconv.FormatInt(lag.TotalLag, 10),
		"state": lag.State,
	}
	if lag.State == "Empty" || lag.TotalLag > t.GroupLagWarn {
		return HealthWarn, facts
	}
	return HealthOK, facts
}

// topicHealth is ok as long as Kafka answers; a quiet topic is not an error.
func topicHealth(err error, newest, now time.Time) (Health, map[string]string) {
	if err != nil {
		return HealthUnknown, nil
	}
	if newest.IsZero() {
		return HealthOK, map[string]string{"newest_record": "never"}
	}
	return HealthOK, map[string]string{"newest_record": HumanAge(now.Sub(newest)) + " ago"}
}

// sinkHealth decides a sink table's health from its newest row timestamp.
func sinkHealth(ts, now time.Time, t Thresholds) (Health, map[string]string) {
	if ts.IsZero() {
		return HealthWarn, map[string]string{"stale_for": "never"}
	}
	stale := now.Sub(ts)
	facts := map[string]string{"stale_for": HumanAge(stale)}
	switch {
	case stale > t.SinkStaleFail:
		return HealthFail, facts
	case stale > t.SinkStaleWarn:
		return HealthWarn, facts
	default:
		return HealthOK, facts
	}
}

// HumanBytes renders a byte count in binary units.
func HumanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	const unit = 1024
	div := int64(unit)
	exp := 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), suffixes[exp])
}

// HumanAge renders a duration compactly, clamped to zero.
func HumanAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%.1fh", d.Hours())
	}
}
