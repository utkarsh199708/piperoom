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

	// engineLoads holds one load-error reading per ClickHouse Kafka engine
	// table (clickhouse.go). It stays empty for a non-clickhouse warehouse.
	engineLoads map[string]engineLoadSnapshot

	topicNewest  map[string]time.Time
	topicRecords map[string]int64 // sum of end-start over partitions
	topicErr     map[string]error

	// srListed is the registry's subject listing and srErr its error; both
	// stay empty when the builder has no Schema Registry client.
	srListed map[string]bool
	srErr    error
	subjects map[string]subjectSnapshot

	// matchers are the include/exclude lists of each readable source
	// connector, compiled once per Build (Build fills them after collect).
	matchers map[string]streamMatcher

	now time.Time
}

// collect gathers health for every system in parallel under ctx's budget.
// engines are the ClickHouse Kafka engine tables to read load errors for (the
// sinks: writers, or every sink table of a legacy ClickHouse pipeline).
func (b *builder) collect(ctx context.Context, p owners.Pipeline, engines []string) *collected {
	c := &collected{
		connect:      map[string]connectSnapshot{},
		groups:       map[string]groupSnapshot{},
		freshness:    map[string]freshSnapshot{},
		engineLoads:  map[string]engineLoadSnapshot{},
		topicNewest:  map[string]time.Time{},
		topicRecords: map[string]int64{},
		topicErr:     map[string]error{},
		srListed:     map[string]bool{},
		subjects:     map[string]subjectSnapshot{},
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

	if b.sr != nil {
		run(func() {
			subs, err := b.sr.Subjects(ctx)
			mu.Lock()
			c.srErr = err
			for _, s := range subs {
				c.srListed[s] = true
			}
			mu.Unlock()
		})
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
	for _, gr := range p.ConsumerGroupNames() {
		gr := gr
		run(func() {
			lag, err := b.kafka.GroupLag(ctx, gr)
			mu.Lock()
			c.groups[gr] = groupSnapshot{lag: lag, err: err}
			mu.Unlock()
		})
	}
	for _, s := range p.SinkTableNames() {
		s := s
		run(func() {
			ts, err := b.wh.Freshness(ctx, s, p.SinkTSColumn)
			mu.Lock()
			c.freshness[s] = freshSnapshot{ts: ts, err: err}
			mu.Unlock()
		})
	}
	if lr, ok := b.wh.(clients.LoadErrorReader); ok {
		for _, engine := range engines {
			engine := engine
			run(func() {
				since := c.now.Add(-b.t.EngineErrorWindow)
				loads, err := lr.LoadErrors(ctx, engine, since, clickhouseLoadLimit)
				mu.Lock()
				c.engineLoads[engine] = engineLoadSnapshot{loads: loads, err: err}
				mu.Unlock()
			})
		}
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
	// A NULL restart_lsn (a lost slot) reaches us as 0 retained bytes: that is
	// not "holding nothing" (#402).
	if slot.RestartLSN == "" && slot.RetainedWALBytes == 0 {
		facts["slot_retained"] = "unknown"
	}
	if slot.WALStatus != "" {
		facts["wal_status"] = slot.WALStatus
	}
	// lost: the WAL the connector needs is gone, it cannot resume without a
	// re-snapshot. unreserved: one checkpoint from that. The watcher treats
	// both as a hard breach.
	if slot.WALStatus == "lost" || slot.WALStatus == "unreserved" {
		return HealthFail, facts
	}
	// >= matches the watcher's slot_wal rule ("retained >= fire"). t comes from
	// ThresholdsFor, so the per-pipeline watch.slot_wal.fire overrides the base
	// SlotRetainedWarnBytes default.
	if !slot.Active || slot.RetainedWALBytes >= t.SlotRetainedWarnBytes {
		return HealthWarn, facts
	}
	return HealthOK, facts
}

// connectorHealth decides a connector's health from its status, by the
// watcher's connector_down rule (classifyOne, devctl): healthy is RUNNING with
// at least one task and every task RUNNING. Anything else is not OK:
//   - FAILED connector or task: Fail;
//   - RUNNING with no tasks: Fail with tasks "none" (the connector moves no
//     data; the watcher opens a sev2 room for it);
//   - any other connector or task state (PAUSED, UNASSIGNED, STOPPED,
//     RESTARTING, one this code does not know): Warn, with the state or a
//     count of the tasks that are not running.
func connectorHealth(s clients.ConnectorStatus) (Health, map[string]string) {
	facts := map[string]string{}
	failed := s.State == "FAILED"
	failedTasks := 0
	pausedTasks := 0
	otherTasks := 0
	for _, task := range s.Tasks {
		switch task.State {
		case "RUNNING":
		case "FAILED":
			failed = true
			failedTasks++
		case "PAUSED", "UNASSIGNED", "STOPPED":
			pausedTasks++
		default:
			otherTasks++
		}
	}
	if failed {
		facts["tasks_failed"] = strconv.Itoa(failedTasks)
		if failedTasks == 0 {
			facts["state"] = s.State
		}
		return HealthFail, facts
	}
	if s.State != "RUNNING" {
		facts["state"] = truncFact(s.State)
		return HealthWarn, facts
	}
	if len(s.Tasks) == 0 {
		facts["tasks"] = "none"
		return HealthFail, facts
	}
	if pausedTasks > 0 {
		facts["tasks_paused"] = strconv.Itoa(pausedTasks)
	}
	if otherTasks > 0 {
		facts["tasks_not_running"] = strconv.Itoa(otherTasks)
	}
	if pausedTasks > 0 || otherTasks > 0 {
		return HealthWarn, facts
	}
	return HealthOK, facts
}

// truncFact caps a state string from Connect before it becomes a fact: the
// state is untrusted text, and a fact is shown to the model and the room.
func truncFact(s string) string {
	const max = 24
	if r := []rune(s); len(r) > max {
		return string(r[:max])
	}
	return s
}

// groupHealth decides a consumer group's health from its lag.
func groupHealth(lag clients.GroupLag, t Thresholds) (Health, map[string]string) {
	facts := map[string]string{
		"lag":   strconv.FormatInt(lag.TotalLag, 10),
		"state": lag.State,
	}
	// A Dead or Empty group without partitions has no committed offset to
	// measure from: its lag is unknown, not 0 (#402).
	if len(lag.Partitions) == 0 && (lag.State == "Dead" || lag.State == "Empty") {
		facts["lag"] = "unknown (no committed offsets)"
	}
	// Dead: the group is gone (deleted, renamed, offsets expired); nothing
	// consumes the pipeline's topics.
	if lag.State == "Dead" {
		return HealthFail, facts
	}
	// Unreadable partitions (a broker down) are not in TotalLag, which is
	// then only a lower bound: the lag is "at least" it, and a group is never
	// OK on that alone (#508, #495).
	if n := len(lag.Unknown); n > 0 {
		facts["unknown_partitions"] = strconv.Itoa(n)
		if len(lag.Partitions) == 0 {
			facts["lag"] = fmt.Sprintf("unknown (%d partition(s) unreadable)", n)
		} else {
			facts["lag"] = fmt.Sprintf("at least %d (%d partition(s) unreadable)", lag.TotalLag, n)
		}
	}
	if lag.State == "Empty" || lag.TotalLag > t.GroupLagWarn {
		return HealthWarn, facts
	}
	if len(lag.Unknown) > 0 {
		return HealthUnknown, facts
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

// FutureSkew is how far ahead of this server's clock a newest timestamp may
// be and still read as "now". Beyond it the clocks disagree (skew, a
// timestamp without time zone written in another zone) or the row is bad:
// the age is then unknown, and clamping it to 0s would show a stale table as
// fresh.
const FutureSkew = time.Minute

// FarFuture is how far ahead a newest timestamp may be and still be shown
// with its gap: no clock skew or time zone mistake is a year, so beyond it the
// value is a sentinel or bad data and its "age" means nothing.
const FarFuture = 365 * 24 * time.Hour

// sinkHealth decides a sink table's health from its newest row timestamp.
func sinkHealth(ts, now time.Time, t Thresholds) (Health, map[string]string) {
	if ts.IsZero() {
		return HealthWarn, map[string]string{"stale_for": "never"}
	}
	// The warehouse client's year-9999 stand-in for a Postgres 'infinity'
	// (#468): no row holds that time, and its "age" is time.Duration
	// saturating, so neither is shown (#476), as in warehouse_freshness.
	if ts.Year() >= 9999 {
		return HealthWarn, map[string]string{"stale_for": "unknown", "future_timestamp": "infinity (a sentinel or bad value)"}
	}
	// ts.Sub(now), not -now.Sub(ts): beyond ~292 years the Duration saturates
	// at its minimum and negating that overflows back to a negative, which
	// read as "ok, stale_for 0s".
	if ahead := ts.Sub(now); ahead > FarFuture {
		return HealthWarn, map[string]string{"stale_for": "unknown", "future_timestamp": "far in the future (a sentinel or bad value)"}
	} else if ahead > FutureSkew {
		return HealthWarn, map[string]string{"stale_for": "unknown", "future_timestamp": HumanAge(ahead) + " ahead"}
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
