package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/owners"
)

// Health thresholds. A layer is unhealthy when it breaks one of these; the
// numbers are documented in dev/scenarios.md, keep the two in sync.
const (
	// healthTimeout bounds each layer check. A check that does not answer in
	// time is reported as unhealthy and the other checks still run.
	healthTimeout = 15 * time.Second
	// maxRetainedWAL: a slot that retains more WAL than this is unhealthy.
	// Healthy demo state retains ~26 kB; broken states reach ~1-2 MB in a minute.
	maxRetainedWAL = 100 << 20
	// maxSyncLag: the oldest source change that has not reached the topic, or
	// the warehouse, may have waited at most this long. Waiting is measured from
	// the change's own updated_at, so an idle stack (nothing pending) is never
	// reported as broken, and a burst that never gets through is.
	maxSyncLag = 60 * time.Second
	// maxConsumerLag: total records the consumer group may trail by.
	maxConsumerLag = 100

	// pipelineID is the pipeline in dev/owners.yaml that health checks.
	pipelineID = "orders"
)

// errUnhealthy means health ran fully and at least one layer failed. The
// report is already printed; main turns it into exit code 1 without a log line.
var errUnhealthy = errors.New("pipeline unhealthy")

// slotHealth is the state of the replication slot on the source database.
type slotHealth struct {
	Exists        bool
	Active        bool
	RetainedBytes int64 // -1 when Postgres cannot tell (restart_lsn is NULL)
}

// groupHealth is the state of one consumer group.
type groupHealth struct {
	State   string
	Members int
	Lag     int64
}

// healthSource reads the facts the health layers need. *stack implements it
// against the real stack; tests pass fakes.
type healthSource interface {
	slot(ctx context.Context) (slotHealth, error)
	connector(ctx context.Context) (connectorStatus, error)
	// topicNewest is the timestamp of the newest record in topic; zero when the topic is empty.
	topicNewest(ctx context.Context, topic string) (time.Time, error)
	// warehouseNewest is the newest updated_at in analytics.orders; zero when empty.
	warehouseNewest(ctx context.Context) (time.Time, error)
	// unsynced is how long the oldest source row with updated_at >= from has been
	// waiting, measured on the source clock. Zero when there is no such row; a zero
	// from counts every row. Callers pass from from the pendingFrom helpers.
	unsynced(ctx context.Context, from time.Time) (time.Duration, error)
	group(ctx context.Context, name string) (groupHealth, error)
}

// healthPlan says which topic and group to check, and who owns each layer.
type healthPlan struct {
	Topic   string            // e.g. pg.public.orders
	Group   string            // e.g. order-sync
	Owners  map[string]string // layer -> owner, for the report only
	Timeout time.Duration     // per layer; zero means healthTimeout
}

// healthCheck is one layer. run must honor ctx; it returns whether the layer is
// healthy and a one-line description for the report.
type healthCheck struct {
	layer string
	run   func(ctx context.Context) (ok bool, line string, err error)
}

// healthChecks builds the five layer checks against src.
func healthChecks(src healthSource, plan healthPlan) []healthCheck {
	return []healthCheck{
		{"postgres", func(ctx context.Context) (bool, string, error) {
			s, err := src.slot(ctx)
			if err != nil {
				return false, "", err
			}
			if !s.Exists {
				return false, "slot " + slotName + ": missing", nil
			}
			state := "active=" + strconv.FormatBool(s.Active)
			retained := "retained " + formatBytes(s.RetainedBytes)
			line := fmt.Sprintf("slot %s: %s, %s (limit %s)", slotName, state, retained, formatBytes(maxRetainedWAL))
			return s.Active && s.RetainedBytes <= maxRetainedWAL, line, nil
		}},
		{"kafka-connect", func(ctx context.Context) (bool, string, error) {
			st, err := src.connector(ctx)
			if err != nil {
				return false, "", err
			}
			var tasks []string
			allRunning := len(st.Tasks) > 0
			for _, t := range st.Tasks {
				tasks = append(tasks, fmt.Sprintf("%d=%s", t.ID, t.State))
				allRunning = allRunning && t.State == "RUNNING"
			}
			taskList := strings.Join(tasks, " ")
			if taskList == "" {
				taskList = "none"
			}
			line := fmt.Sprintf("%s: %s, tasks %s", connectorName, st.Connector.State, taskList)
			return st.Connector.State == "RUNNING" && allRunning, line, nil
		}},
		{"kafka", func(ctx context.Context) (bool, string, error) {
			newest, err := src.topicNewest(ctx, plan.Topic)
			if err != nil {
				return false, "", err
			}
			pending, err := src.unsynced(ctx, topicPendingFrom(newest))
			if err != nil {
				return false, "", err
			}
			last := "no records"
			if !newest.IsZero() {
				last = "newest record " + formatDuration(time.Since(newest)) + " old"
			}
			line := fmt.Sprintf("topic %s: %s, oldest unsent change %s (limit %s)",
				plan.Topic, last, formatDuration(pending), formatDuration(maxSyncLag))
			return pending <= maxSyncLag, line, nil
		}},
		{"consumer", func(ctx context.Context) (bool, string, error) {
			g, err := src.group(ctx, plan.Group)
			if err != nil {
				return false, "", err
			}
			line := fmt.Sprintf("%s: state %s, members %d, total lag %d (limit %d)", plan.Group, g.State, g.Members, g.Lag, maxConsumerLag)
			return g.State == "Stable" && g.Members >= 1 && g.Lag <= maxConsumerLag, line, nil
		}},
		{"warehouse", func(ctx context.Context) (bool, string, error) {
			newest, err := src.warehouseNewest(ctx)
			if err != nil {
				return false, "", err
			}
			pending, err := src.unsynced(ctx, warehousePendingFrom(newest))
			if err != nil {
				return false, "", err
			}
			last := "empty"
			if !newest.IsZero() {
				last = "newest updated_at " + newest.UTC().Format("15:04:05") + " UTC"
			}
			line := fmt.Sprintf("analytics.orders: %s, oldest unloaded change %s (limit %s)",
				last, formatDuration(pending), formatDuration(maxSyncLag))
			return pending <= maxSyncLag, line, nil
		}},
	}
}

// layerHealth is the outcome of one check.
type layerHealth struct {
	Layer string
	OK    bool
	Line  string
}

// runHealth runs every check concurrently, each under its own timeout, prints
// one line per layer to w and returns errUnhealthy if any layer failed. A
// failing or hanging check never stops the others.
func runHealth(ctx context.Context, checks []healthCheck, plan healthPlan, w io.Writer) error {
	timeout := plan.Timeout
	if timeout <= 0 {
		timeout = healthTimeout
	}
	results := make([]layerHealth, len(checks))
	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = runCheck(ctx, c, timeout)
		}()
	}
	wg.Wait()

	var bad []string
	for _, r := range results {
		status := "OK  "
		if !r.OK {
			status = "FAIL"
			bad = append(bad, r.Layer)
		}
		fmt.Fprintf(w, "%s %-13s %s\n", status, r.Layer, r.Line)
	}
	if len(bad) == 0 {
		fmt.Fprintf(w, "pipeline healthy: %d/%d layers OK\n", len(results), len(results))
		return nil
	}
	named := make([]string, 0, len(bad))
	for _, l := range bad {
		if owner := plan.Owners[l]; owner != "" {
			named = append(named, fmt.Sprintf("%s (%s)", l, owner))
		} else {
			named = append(named, l)
		}
	}
	fmt.Fprintf(w, "pipeline UNHEALTHY: %s\n", strings.Join(named, ", "))
	return errUnhealthy
}

// runCheck runs one check with a deadline. The check runs in its own goroutine,
// so a check that ignores its context cannot hold up the report.
func runCheck(ctx context.Context, c healthCheck, timeout time.Duration) layerHealth {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	type outcome struct {
		ok   bool
		line string
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		ok, line, err := c.run(cctx)
		done <- outcome{ok, line, err}
	}()
	select {
	case o := <-done:
		// A check that returns ctx.Err() races with cctx.Done(); when the deadline
		// has passed, report it the same way as the Done branch below.
		if o.err != nil && cctx.Err() != nil {
			return layerHealth{Layer: c.layer, OK: false, Line: ctxLine(ctx, timeout)}
		}
		if o.err != nil {
			return layerHealth{Layer: c.layer, OK: false, Line: "error: " + o.err.Error()}
		}
		return layerHealth{Layer: c.layer, OK: o.ok, Line: o.line}
	case <-cctx.Done():
		return layerHealth{Layer: c.layer, OK: false, Line: ctxLine(ctx, timeout)}
	}
}

// ctxLine says why a check did not finish. The parent context ending (Ctrl+C) is
// a cancel, not a slow layer.
func ctxLine(parent context.Context, timeout time.Duration) string {
	if parent.Err() != nil {
		return "cancelled"
	}
	return fmt.Sprintf("no answer within %s", timeout)
}

// warehousePendingFrom is the first updated_at not yet in the warehouse. The
// warehouse holds exact microseconds, so its newest row is delivered and only
// rows strictly after it are pending. A zero newest means nothing is loaded.
func warehousePendingFrom(newest time.Time) time.Time {
	if newest.IsZero() {
		return time.Time{}
	}
	return newest.Add(time.Microsecond)
}

// topicPendingFrom is the first updated_at not yet in the topic. Kafka record
// timestamps have millisecond precision, so rows in the same millisecond as the
// newest record count as delivered. A zero newest means the topic is empty.
func topicPendingFrom(newest time.Time) time.Time {
	if newest.IsZero() {
		return time.Time{}
	}
	return newest.Truncate(time.Millisecond).Add(time.Millisecond)
}

// pendingWhere is the SQL filter for source rows with updated_at >= from. The
// timestamp is written with microseconds so nothing is rounded. Only this
// formatted time is interpolated; no user input reaches the SQL.
func pendingWhere(from time.Time) string {
	if from.IsZero() {
		return "TRUE"
	}
	return "updated_at >= '" + from.UTC().Format("2006-01-02 15:04:05.000000") + "+00'::timestamptz"
}

// parseEpochMicros decodes epoch microseconds; 0 means "no rows" and yields the zero time.
func parseEpochMicros(out string) (time.Time, error) {
	us, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("devctl: health: parse timestamp %q: %w", out, err)
	}
	if us == 0 {
		return time.Time{}, nil
	}
	return time.UnixMicro(us), nil
}

// lastRecordOffsets maps each partition that has records to its last offset
// (end offset - 1). Empty partitions are left out; a partition that could not be
// listed is an error.
func lastRecordOffsets(ends kadm.ListedOffsets) (map[int32]kgo.Offset, error) {
	last := map[int32]kgo.Offset{}
	for _, parts := range ends {
		for p, o := range parts {
			if o.Err != nil {
				return nil, fmt.Errorf("devctl: end offset of partition %d: %w", p, o.Err)
			}
			if o.Offset > 0 {
				last[p] = kgo.NewOffset().At(o.Offset - 1)
			}
		}
	}
	return last, nil
}

// settleHealth runs the health check until it reports healthy or limit has passed,
// waiting every between runs. Reports from the earlier runs are discarded; the last
// one is written to w. A run that fails with anything other than errUnhealthy stops
// the loop at once.
func settleHealth(ctx context.Context, run func(io.Writer) error, every, limit time.Duration, w io.Writer) error {
	deadline := time.Now().Add(limit)
	for {
		var buf bytes.Buffer
		err := run(&buf)
		if err == nil || !errors.Is(err, errUnhealthy) || !time.Now().Before(deadline) {
			_, _ = io.Copy(w, &buf)
			return err
		}
		select {
		case <-ctx.Done():
			_, _ = io.Copy(w, &buf)
			return ctx.Err()
		case <-time.After(every):
		}
	}
}

// printHealthAfterFix is used by demo after --fix. The fix returns before the
// backlog has drained, so the check is re-run for up to 90 s and the settled
// report is printed. A still-unhealthy report is printed too.
func printHealthAfterFix(ctx context.Context, s *stack) {
	fmt.Fprintln(os.Stdout, "health after the fix:")
	err := settleHealth(ctx, func(w io.Writer) error { return s.health(ctx, w) }, 5*time.Second, 90*time.Second, os.Stdout)
	if err != nil && !errors.Is(err, errUnhealthy) && !errors.Is(err, context.Canceled) {
		s.log.Warn("health could not run", "err", err)
	}
	fmt.Fprintln(os.Stdout)
}

// health implements `devctl health`. It is read-only, so it takes no stack lock.
// A run cut short by Ctrl+C is not an unhealthy pipeline: it returns the context
// error (exit 2), which also stops settleHealth at once.
func (s *stack) health(ctx context.Context, w io.Writer) error {
	plan, err := s.loadHealthPlan()
	if err != nil {
		return err
	}
	err = runHealth(ctx, healthChecks(s, plan), plan, w)
	if errors.Is(err, errUnhealthy) && ctx.Err() != nil {
		return fmt.Errorf("devctl: health: %w", ctx.Err())
	}
	return err
}

// loadHealthPlan reads the topic, consumer group and layer owners from dev/owners.yaml.
func (s *stack) loadHealthPlan() (healthPlan, error) {
	path := s.cfg.OwnersFile
	if !filepath.IsAbs(path) {
		path = filepath.Join(s.root, path)
	}
	o, err := owners.Load(path)
	if err != nil {
		return healthPlan{}, fmt.Errorf("devctl: health: %w", err)
	}
	p, ok := o.Pipeline(pipelineID)
	if !ok {
		return healthPlan{}, fmt.Errorf("devctl: health: pipeline %q not in %s", pipelineID, path)
	}
	if len(p.SourceTables) == 0 || len(p.ConsumerGroups) == 0 {
		return healthPlan{}, fmt.Errorf("devctl: health: pipeline %q needs a source table and a consumer group", pipelineID)
	}
	return healthPlan{
		Topic:  p.TopicPrefix + "." + p.SourceTables[0],
		Group:  p.ConsumerGroups[0],
		Owners: o.Layers,
	}, nil
}

// slot reads the replication slot. Only the slotName constant is concatenated into the SQL.
func (s *stack) slot(ctx context.Context) (slotHealth, error) {
	out, err := s.psql(ctx, "orders", "SELECT CASE WHEN active THEN 'active' ELSE 'inactive' END || ' ' || coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint::text, '-1') FROM pg_replication_slots WHERE slot_name = '"+slotName+"'")
	if err != nil {
		return slotHealth{}, err
	}
	return parseSlotRow(out)
}

// parseSlotRow decodes "<active|inactive> <retained bytes>". An empty row means the slot does not exist.
func parseSlotRow(out string) (slotHealth, error) {
	if out == "" {
		return slotHealth{}, nil
	}
	fields := strings.Fields(out)
	if len(fields) != 2 {
		return slotHealth{}, fmt.Errorf("devctl: health: unexpected slot row %q", out)
	}
	retained, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return slotHealth{}, fmt.Errorf("devctl: health: retained WAL %q: %w", fields[1], err)
	}
	return slotHealth{Exists: true, Active: fields[0] == "active", RetainedBytes: retained}, nil
}

func (s *stack) connector(ctx context.Context) (connectorStatus, error) {
	return s.connectorStatus(ctx)
}

// warehouseNewest is read in microseconds. round() (not floor or a millisecond
// cast) keeps the exact value: a rounded or truncated timestamp makes already
// loaded rows look pending.
func (s *stack) warehouseNewest(ctx context.Context) (time.Time, error) {
	out, err := s.psql(ctx, "warehouse", "SELECT coalesce(round(extract(epoch FROM max(updated_at)) * 1000000)::bigint, 0) FROM analytics.orders")
	if err != nil {
		return time.Time{}, err
	}
	return parseEpochMicros(out)
}

// unsynced is computed inside the source database so both ends of the subtraction
// use the same clock. The age is capped at zero for rows timestamped in the future.
// The only SQL interpolated here is pendingWhere's formatted timestamp.
func (s *stack) unsynced(ctx context.Context, from time.Time) (time.Duration, error) {
	out, err := s.psql(ctx, "orders", "SELECT coalesce(round(extract(epoch FROM now() - min(updated_at)) * 1000000)::bigint, 0) FROM orders WHERE "+pendingWhere(from))
	if err != nil {
		return 0, err
	}
	us, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("devctl: health: parse pending age %q: %w", out, err)
	}
	return max(time.Duration(us)*time.Microsecond, 0), nil
}

func (s *stack) group(ctx context.Context, name string) (groupHealth, error) {
	l, err := s.describeGroup(ctx, name)
	if err != nil {
		return groupHealth{}, err
	}
	return groupHealth{State: l.State, Members: len(l.Members), Lag: l.Lag.Total()}, nil
}

// topicNewest returns the timestamp of the newest record in topic: for each
// partition it reads the record at the last offset. Zero time means no records.
// A topic whose last offset is a transaction marker would never answer; the
// layer timeout reports that instead of hanging.
func (s *stack) topicNewest(ctx context.Context, topic string) (time.Time, error) {
	brokers := strings.Split(s.cfg.KafkaBrokers, ",")
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return time.Time{}, fmt.Errorf("devctl: kafka client: %w", err)
	}
	ends, err := kadm.NewClient(admin).ListEndOffsets(ctx, topic)
	admin.Close()
	if err != nil {
		return time.Time{}, fmt.Errorf("devctl: end offsets of %s: %w", topic, err)
	}
	last, err := lastRecordOffsets(ends)
	if err != nil {
		return time.Time{}, fmt.Errorf("devctl: %s: %w", topic, err)
	}
	if len(last) == 0 {
		return time.Time{}, nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: last}))
	if err != nil {
		return time.Time{}, fmt.Errorf("devctl: kafka consumer: %w", err)
	}
	defer cl.Close()

	var newest time.Time
	seen := map[int32]bool{}
	for len(seen) < len(last) {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				return time.Time{}, fmt.Errorf("devctl: newest record of %s: %w", topic, ctx.Err())
			}
			return time.Time{}, fmt.Errorf("devctl: fetch %s: %w", topic, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			seen[r.Partition] = true
			if r.Timestamp.After(newest) {
				newest = r.Timestamp
			}
		})
	}
	return newest, nil
}

// formatBytes prints a byte count for humans, e.g. "26 kB", "1.3 MB".
func formatBytes(n int64) string {
	switch {
	case n < 0:
		return "unknown"
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%d kB", n>>10)
	default:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	}
}

// formatDuration prints whole seconds, which is the precision the report needs.
func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%d s", int64(d.Round(time.Second)/time.Second))
}

// printHealth is used by demo: a failing report is expected there, so only a
// check that could not run is logged.
func printHealth(ctx context.Context, s *stack, label string) {
	fmt.Fprintln(os.Stdout, label)
	if err := s.health(ctx, os.Stdout); err != nil && !errors.Is(err, errUnhealthy) {
		s.log.Warn("health could not run", "err", err)
	}
	fmt.Fprintln(os.Stdout)
}
