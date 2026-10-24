package watch

// Sample history and forecasts (X3a; docs/api.md "Sample history and
// forecasts"). Every pipeline gets at most one sample per metric every
// config.WatchSampleInterval in Piperoom's own watch_samples table
// (Deps.Samples). A metric with a limit is then fitted with a least-squares
// line over the last config.WatchForecastFitWindow of its stored samples, and
// the forecast signal fires when the line reaches the limit within the
// horizon with R² >= config.WatchForecastMinR2. It generalises slot_wal's ETA
// to any sampled metric. A forecast room is quiet (sev4) and opens under its
// own dedup key per metric, so its cooldown is per (pipeline, metric) and a
// closed forecast room never mutes the pipeline's real signals.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// SampleStore is where the watcher keeps its samples and reads them back for
// the fit: the room store (store.SampleStore). Piperoom's own table, never a
// customer system.
type SampleStore interface {
	clients.SampleWriter
	clients.SampleReader
}

// forecastMetrics are the metrics with a limit, in clients.WatchMetrics order.
// db_size_bytes has no limit until owners.yaml carries watch.db_size_limit
// (A3), so it is sampled but never fires.
var forecastMetrics = []string{clients.WatchMetricSlotWALBytes, clients.WatchMetricDBSizeBytes, clients.WatchMetricTopicOldestUnreadAge}

// forecastID is the state-machine id of a metric's forecast ("forecast:slot_wal_bytes").
func forecastID(metric string) string { return events.SignalForecast + ":" + metric }

// forecastMetric is the metric of a forecast id, "" for any other signal.
func forecastMetric(id string) string {
	m, ok := strings.CutPrefix(id, events.SignalForecast+":")
	if !ok {
		return ""
	}
	return m
}

func forecastIDs() []string {
	out := make([]string, len(forecastMetrics))
	for i, m := range forecastMetrics {
		out[i] = forecastID(m)
	}
	return out
}

// forecastSignal is the settings of every forecast until owners.yaml carries
// them (A3 adds "forecast" to watch.signals): on, room, no `for` (the fit
// already needs WatchForecastMinSamples points, 2 h).
var forecastSignal = owners.WatchSignal{Enabled: true, Action: owners.ActionRoom}

// ForecastDedupKey is the alert dedup key of a forecast room: one per
// pipeline and metric, so the room service's last close of it is that
// metric's cooldown, apart from DedupKey's.
func ForecastDedupKey(pipelineID, metric string) string {
	return DedupKey(pipelineID) + ":" + events.SignalForecast + ":" + metric
}

// sampleJob is one sample to write in this tick.
type sampleJob struct {
	t      target
	metric string
	// read is set when the value needs a call (an optional reader); else value
	// and the rest come from this tick's observation.
	read   bool
	value  float64
	object string
	limit  float64 // 0 = no limit, no forecast
}

// planSamples picks the samples due in this tick from the observations and
// marks them taken, so a failed write is not retried before the next interval
// (no write storm on a fast tick). Caller holds w.mu.
func (w *Watcher) planSamples(now time.Time, ts []target, bySet map[string]*observation) []sampleJob {
	if w.samples == nil {
		return nil
	}
	var jobs []sampleJob
	for _, t := range ts {
		obs := bySet[t.set]
		if obs == nil {
			continue
		}
		ps := w.pipe(t.p.ID)
		due := func(m string) bool {
			if now.Sub(ps.lastSample[m]) < config.WatchSampleInterval {
				return false
			}
			if ps.lastSample == nil {
				ps.lastSample = map[string]time.Time{}
			}
			ps.lastSample[m] = now
			return true
		}
		// A non-Postgres pipeline has no slot: no slot sample, so no slot forecast.
		if row, ok := slotSample(ps, obs); ok && !hasNoSlot(t.p) && due(clients.WatchMetricSlotWALBytes) {
			jobs = append(jobs, sampleJob{t: t, metric: clients.WatchMetricSlotWALBytes, value: float64(row.RetainedWALBytes),
				object: ps.slot, limit: slotLimit(row, t.cfg.WALHeadroomBytes)})
		}
		if n, ok := dlqRecords(t, obs); ok && due(clients.WatchMetricDLQRecords) {
			jobs = append(jobs, sampleJob{t: t, metric: clients.WatchMetricDLQRecords, value: float64(n)})
		}
		if n, ok := groupLagTotal(t, obs); ok && due(clients.WatchMetricGroupLag) {
			jobs = append(jobs, sampleJob{t: t, metric: clients.WatchMetricGroupLag, value: float64(n)})
		}
		if _, ok := t.src.(clients.DBSizeReader); ok && due(clients.WatchMetricDBSizeBytes) {
			jobs = append(jobs, sampleJob{t: t, metric: clients.WatchMetricDBSizeBytes, read: true})
		}
		if _, ok := t.lag.(clients.UnreadAgeReader); ok && len(t.groups) > 0 && due(clients.WatchMetricTopicOldestUnreadAge) {
			jobs = append(jobs, sampleJob{t: t, metric: clients.WatchMetricTopicOldestUnreadAge, read: true})
		}
	}
	return jobs
}

// slotSample is the pipeline's slot row of this tick, when the slot was read
// and found (evaluate has set ps.slot).
func slotSample(ps *pipeState, obs *observation) (clients.ReplicationSlot, bool) {
	if obs.slotsErr != nil || ps.slot == "" {
		return clients.ReplicationSlot{}, false
	}
	for _, r := range obs.slots {
		if r.Name == ps.slot {
			return r, true
		}
	}
	return clients.ReplicationSlot{}, false
}

// slotLimit is the retained WAL at which the first limit is hit: the slot is
// invalidated at retained + safe_wal_size (exact), the disk is full at the
// configured watch.wal_headroom_bytes; the smaller wins. 0 when neither is known.
func slotLimit(row clients.ReplicationSlot, headroom int64) float64 {
	var lim int64
	if row.SafeWALBytes != nil && *row.SafeWALBytes > 0 {
		lim = row.RetainedWALBytes + *row.SafeWALBytes
	}
	if headroom > 0 && (lim == 0 || headroom < lim) {
		lim = headroom
	}
	return float64(lim)
}

// dlqRecords sums the records held in the pipeline's DLQ topics. ok is false
// when the offsets were not read, a connector config was not (a topic may be
// missing), or the pipeline has no DLQ topic.
func dlqRecords(t target, obs *observation) (int64, bool) {
	if obs.plan == nil || obs.dlqErr != nil || obs.dlqNoReader || obs.plan.cfgFail[t.p.ID] {
		return 0, false
	}
	topics := obs.plan.topics[t.p.ID]
	if len(topics) == 0 {
		return 0, false
	}
	var n int64
	for _, d := range topics {
		for _, p := range obs.dlqOffsets[d.name] {
			n += max(p.End-p.Start, 0)
		}
	}
	return n, true
}

// groupLagTotal sums the pipeline's consumer-group lag. A group with
// unreadable partitions makes the total a lower bound, which would draw a
// false dip, so no sample is taken then.
func groupLagTotal(t target, obs *observation) (int64, bool) {
	if len(t.groups) == 0 {
		return 0, false
	}
	lags, err := groupReads(t, obs)
	if err != nil {
		return 0, false
	}
	var n int64
	for _, gl := range lags {
		if len(gl.Unknown) > 0 {
			return 0, false
		}
		n += gl.TotalLag
	}
	return n, true
}

// writeSamples makes the calls of the planned jobs, without the lock: the
// optional reads, the append, and for a metric with a limit the history read
// and the forecast step. Every call has the check deadline.
func (w *Watcher) writeSamples(ctx context.Context, now time.Time, jobs []sampleJob) {
	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}
		pid := j.t.p.ID
		if j.read {
			var err error
			if j, err = w.readSample(ctx, now, j); err != nil {
				w.sampleError(pid, j.metric, err)
				continue
			}
		}
		_, err := call(ctx, w.cfg.CheckTimeout, "append sample", func(c context.Context) (struct{}, error) {
			return struct{}{}, w.samples.AppendSample(c, pid, j.metric, now, j.value)
		})
		if err != nil {
			w.sampleError(pid, j.metric, err)
			continue
		}
		if !slices.Contains(forecastMetrics, j.metric) {
			continue
		}
		var r reading
		if j.limit <= 0 {
			r = reading{v: vClear, value: "no limit", threshold: "no limit"}
		} else {
			pts, err := call(ctx, w.cfg.CheckTimeout, "read samples", func(c context.Context) ([]clients.SamplePoint, error) {
				return w.samples.Samples(c, pid, j.metric, now.Add(-config.WatchForecastFitWindow))
			})
			if err != nil {
				r = reading{v: vUnknown, err: err}
			} else {
				r = forecastReading(now, j.metric, j.object, pts, j.limit, w.horizon)
			}
		}
		w.mu.Lock()
		w.apply(now, j.t, w.pipe(pid), forecastID(j.metric), forecastSignal, r, map[string][]string{})
		w.mu.Unlock()
	}
}

// sampleError logs a failed sample read or write once per error text (the
// forecast state is left alone; the next interval tries again).
func (w *Watcher) sampleError(pid, metric string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.noteError(pid, "sample:"+metric, w.pipe(pid).sig("sample:"+metric), err)
}

// readSample fills a job whose value needs one of the optional readers.
func (w *Watcher) readSample(ctx context.Context, now time.Time, j sampleJob) (sampleJob, error) {
	switch j.metric {
	case clients.WatchMetricDBSizeBytes:
		n, err := call(ctx, w.cfg.CheckTimeout, "database size", j.t.src.(clients.DBSizeReader).DatabaseSize)
		if err != nil {
			return j, err
		}
		j.value = float64(n) // no limit until watch.db_size_limit exists (A3)
		return j, nil
	case clients.WatchMetricTopicOldestUnreadAge:
		ur := j.t.lag.(clients.UnreadAgeReader)
		var oldest time.Time
		for _, g := range j.t.groups {
			byTopic, err := call(ctx, w.cfg.CheckTimeout, "oldest unread record", func(c context.Context) (map[string]time.Time, error) {
				return ur.OldestUnread(c, g)
			})
			if err != nil {
				return j, err
			}
			// Only the pipeline's own topics count: names come from owners.
			for _, topic := range j.t.cdcTopics {
				if at, ok := byTopic[topic]; ok && !at.IsZero() && (oldest.IsZero() || at.Before(oldest)) {
					oldest, j.object = at, topic
				}
			}
		}
		if j.object == "" {
			return j, nil // nothing unread: age 0, no limit needed
		}
		j.value = max(now.Sub(oldest).Seconds(), 0)
		tc, ok := j.t.lag.(clients.TopicConfigReader)
		if !ok {
			return j, nil // no retention known: trend only
		}
		cfg, err := call(ctx, w.cfg.CheckTimeout, "topic config", func(c context.Context) (clients.TopicConfig, error) {
			return tc.DescribeTopicConfigs(c, j.object)
		})
		if err != nil {
			return j, err
		}
		if ms, err := strconv.ParseInt(strings.TrimSpace(cfg.RetentionMS), 10, 64); err == nil && ms > 0 {
			j.limit = float64(ms) / 1000
		}
		return j, nil
	}
	return j, fmt.Errorf("watch: no reader for metric %s", j.metric)
}

// lineFit is a least-squares line through samples: value = a + slope·hours.
type lineFit struct {
	slope  float64 // per hour
	r2     float64
	atLast float64 // the line's value at the last sample
}

// fitLine fits pts (oldest first). ok is false with fewer than two points or
// when they share one time.
func fitLine(pts []clients.SamplePoint) (lineFit, bool) {
	n := float64(len(pts))
	if len(pts) < 2 {
		return lineFit{}, false
	}
	t0 := pts[0].At
	var sx, sy, sxx, sxy float64
	for _, p := range pts {
		x := p.At.Sub(t0).Hours()
		sx += x
		sy += p.Value
		sxx += x * x
		sxy += x * p.Value
	}
	den := n*sxx - sx*sx
	if den == 0 {
		return lineFit{}, false
	}
	slope := (n*sxy - sx*sy) / den
	a := (sy - slope*sx) / n
	mean := sy / n
	var ssRes, ssTot float64
	for _, p := range pts {
		x := p.At.Sub(t0).Hours()
		ssRes += (p.Value - (a + slope*x)) * (p.Value - (a + slope*x))
		ssTot += (p.Value - mean) * (p.Value - mean)
	}
	r2 := 0.0
	if ssTot > 0 {
		r2 = math.Max(0, math.Min(1, 1-ssRes/ssTot))
	}
	return lineFit{slope: slope, r2: r2, atLast: a + slope*pts[len(pts)-1].At.Sub(t0).Hours()}, true
}

// forecastReading judges one metric's history against its limit. It fires
// (breach) when the line rises, fits with R² >= WatchForecastMinR2 over at
// least WatchForecastMinSamples points, and reaches limit within horizon. It
// clears when the series is flat or shrinking, the fit falls well below the
// bar (R² < bar - 0.1), or the ETA moves past twice the horizon; in between
// it holds, so a fit that hovers at the bar does not flap.
func forecastReading(now time.Time, metric, object string, pts []clients.SamplePoint, limit float64, horizon time.Duration) reading {
	if len(pts) < config.WatchForecastMinSamples {
		return reading{v: vHold, value: fmt.Sprintf("%d samples", len(pts)), threshold: "8 samples"}
	}
	f, ok := fitLine(pts)
	if !ok {
		return reading{v: vHold}
	}
	current := pts[len(pts)-1].Value
	r := reading{sev: events.SignalSeverity(events.SignalForecast),
		value: fmtMetric(metric, current), threshold: fmtMetric(metric, limit) + " within " + fmtDur(horizon)}
	if f.slope <= 0 {
		r.v = vClear
		return r
	}
	last := pts[len(pts)-1].At
	hours := (limit - f.atLast) / f.slope
	const maxHours = 10 * 365 * 24
	hours = math.Max(0, math.Min(hours, maxHours))
	eta := last.Add(time.Duration(hours * float64(time.Hour)))
	if eta.Before(now) {
		eta = now
	}
	left := eta.Sub(now)
	switch {
	case f.r2 >= config.WatchForecastMinR2 && left <= horizon:
		r.v = vBreach
	case f.r2 < config.WatchForecastMinR2-0.1 || left > 2*horizon:
		r.v = vClear
		return r
	default:
		r.v = vHold
		return r
	}
	r.label = forecastLabel(metric, object, limit, left)
	sig := events.ForecastSignal{
		Signal: events.SignalForecast, Severity: r.sev, Metric: metric, Object: object,
		Current: current, Limit: limit, ETA: eta.UTC().Truncate(time.Second),
		Slope: f.slope, R2: math.Round(f.r2*100) / 100, Samples: len(pts),
	}
	raw, _ := json.Marshal(sig) // a struct of numbers and names: cannot fail
	_ = json.Unmarshal(raw, &r.ev)
	return r
}

// forecastLabel is the title fragment of a forecast.
func forecastLabel(metric, object string, limit float64, left time.Duration) string {
	in := "in ~" + fmtDur(left.Round(time.Minute))
	switch metric {
	case clients.WatchMetricSlotWALBytes:
		return fmt.Sprintf("forecast: slot %s WAL reaches %s %s", object, fmtMetric(metric, limit), in)
	case clients.WatchMetricTopicOldestUnreadAge:
		return fmt.Sprintf("forecast: unread records of topic %s pass retention (data loss) %s", object, in)
	case clients.WatchMetricDBSizeBytes:
		return fmt.Sprintf("forecast: database size reaches %s %s", fmtMetric(metric, limit), in)
	}
	return fmt.Sprintf("forecast: %s reaches %s %s", metric, fmtMetric(metric, limit), in)
}

func fmtMetric(metric string, v float64) string {
	switch clients.WatchMetricUnit(metric) {
	case "bytes":
		return fmtBytes(int64(v))
	case "seconds":
		return fmtDur(time.Duration(v * float64(time.Second)))
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// forecastOnly: every active signal of the view is a forecast.
func (v view) forecastOnly() bool {
	for _, s := range v.active {
		if forecastMetric(s.id) == "" {
			return false
		}
	}
	return len(v.active) > 0
}

// forecastCooling: a forecast room of the metric closed less than the
// cooldown ago (the room service's close of its key, or one the watcher saw).
// A forecast room still open under the key is not cooling: it is joined.
func (w *Watcher) forecastCooling(now time.Time, id, metric string) bool {
	_, closedAt, found := w.rooms.LastRoomForKey(id, ForecastDedupKey(id, metric))
	var at time.Time
	if found && closedAt != nil {
		at = *closedAt
	}
	w.mu.Lock()
	if ps := w.pipes[id]; ps != nil && ps.fcClosed[metric].After(at) {
		at = ps.fcClosed[metric]
	}
	w.mu.Unlock()
	return !at.IsZero() && now.Sub(at) < w.cfg.Cooldown
}

// forecastOpen prepares a room open for a view that holds only forecasts: the
// forecasts in their own cooldown are left out, and the key is the first
// remaining metric's ForecastDedupKey. ok is false when nothing is left.
func (w *Watcher) forecastOpen(now time.Time, id string, v view) (view, string, bool) {
	kept := v.active[:0:0]
	for _, s := range v.active {
		if s.pending && w.forecastCooling(now, id, forecastMetric(s.id)) {
			continue
		}
		kept = append(kept, s)
	}
	v.active = kept
	p := v.pending()
	if len(p) == 0 {
		return v, "", false
	}
	return v, ForecastDedupKey(id, forecastMetric(p[0].id)), true
}
