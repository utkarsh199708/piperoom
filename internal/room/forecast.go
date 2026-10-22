package room

// Forecast rooms and escalation (X3c; docs/api.md "X3 events"). A forecast
// signal of the built-in watcher opens a quiet room: origin "forecast", the
// trend summary as its first system message, and no agent run (no model
// call, no usage) until a person asks (the first steer.sent or @agent
// comment, ensureAgentLocked) or the metric's real signal joins the room at a
// higher severity (room.escalated). The service recognises the room by the
// watcher's forecast dedup key, so the watcher passes nothing new.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Forecast is the snapshot's forecast member: the newest forecast signal of
// the room's watcher evidence (events.ForecastSignal without its severity and
// sample count), At being the time of the event that carried it.
type Forecast struct {
	Metric  string    `json:"metric"`
	Object  string    `json:"object,omitempty"`
	Current float64   `json:"current"`
	Limit   float64   `json:"limit"`
	ETA     time.Time `json:"eta"`
	Slope   float64   `json:"slope_per_hour"`
	R2      float64   `json:"r2"`
	At      time.Time `json:"at"`
}

// isForecastKey reports whether dedupKey is the watcher's forecast key for
// pipelineID, "watcher:<pid>:forecast:<metric>" (watch.ForecastDedupKey; the
// room package does not import the watcher).
func isForecastKey(pipelineID, dedupKey string) bool {
	metric, ok := strings.CutPrefix(dedupKey, "watcher:"+pipelineID+":"+events.SignalForecast+":")
	return ok && slices.Contains(clients.WatchMetrics(), metric)
}

// quietRoom reports whether a room is a forecast room whose agent never ran:
// no agent event in its log. Such a room spends nothing until it is asked.
func quietRoom(snap RoomSnapshot) bool {
	return snap.Origin == events.RoomOriginForecast && !snap.agentRan
}

// quietLog is quietRoom for a room's whole log.
func quietLog(evs []events.Event) bool {
	forecast := false
	for _, e := range evs {
		if e.Actor.Kind == events.ActorAgent {
			return false
		}
		if e.Type == events.RoomCreated {
			p, ok := decode[events.RoomCreatedPayload](e)
			forecast = ok && p.Origin == events.RoomOriginForecast
		}
	}
	return forecast
}

// watcherEvidence is the part of the watcher's evidence JSON (watch.compose)
// the room reads: who wrote it, for which pipeline, at which severity, and
// the flat signal entries.
type watcherEvidence struct {
	Source   string            `json:"source"`
	Pipeline string            `json:"pipeline"`
	Severity string            `json:"severity"`
	Signals  []json.RawMessage `json:"signals"`
}

// signalHead is the two keys every evidence entry starts with.
type signalHead struct {
	Signal   string `json:"signal"`
	Severity string `json:"severity"`
}

// decodeWatcherEvidence decodes raw as watcher evidence; ok is false for
// anything else (another source's alert, a non-object, broken JSON).
func decodeWatcherEvidence(raw json.RawMessage) (watcherEvidence, bool) {
	var ev watcherEvidence
	if len(raw) == 0 || json.Unmarshal(raw, &ev) != nil || ev.Source != string(alertin.Watcher) {
		return watcherEvidence{}, false
	}
	return ev, true
}

// maxForecastEntries caps the forecast entries one evidence object is read
// for (the watcher has three metrics with a limit).
const maxForecastEntries = 8

// forecastsOf returns the valid forecast entries of watcher evidence, at most
// maxForecastEntries. An entry with an unknown metric or a zero ETA is left
// out: it is not one the watcher writes.
func forecastsOf(ev watcherEvidence) []events.ForecastSignal {
	var out []events.ForecastSignal
	for _, s := range ev.Signals {
		var h signalHead
		if json.Unmarshal(s, &h) != nil || h.Signal != events.SignalForecast {
			continue
		}
		var f events.ForecastSignal
		if json.Unmarshal(s, &f) != nil || !slices.Contains(clients.WatchMetrics(), f.Metric) || f.ETA.IsZero() {
			continue
		}
		f.Object = clipRunes(f.Object, maxObjectRunes)
		out = append(out, f)
		if len(out) == maxForecastEntries {
			break
		}
	}
	return out
}

// maxObjectRunes caps a forecast's object name (a slot, topic, group or
// database name) where the room shows it.
const maxObjectRunes = 200

func clipRunes(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n])
	}
	return s
}

// forecastFrom is the snapshot forecast of watcher evidence raw seen at at:
// the entry with the soonest ETA, nil when raw holds none.
func forecastFrom(raw json.RawMessage, at time.Time) *Forecast {
	ev, ok := decodeWatcherEvidence(raw)
	if !ok {
		return nil
	}
	fs := forecastsOf(ev)
	if len(fs) == 0 {
		return nil
	}
	f := slices.MinFunc(fs, func(a, b events.ForecastSignal) int { return a.ETA.Compare(b.ETA) })
	return &Forecast{Metric: f.Metric, Object: f.Object, Current: f.Current, Limit: f.Limit,
		ETA: f.ETA.UTC(), Slope: f.Slope, R2: f.R2, At: at}
}

// evidenceMarker separates the watcher's note text from its evidence JSON in
// the system message of a join (watch.updateRoom, PostRoomEvidence). JSON
// text holds no raw newline, so the first marker is the separator.
const evidenceMarker = "\nevidence: "

// noteEvidence returns the evidence JSON of a watcher join note, nil for any
// other text.
func noteEvidence(text string) json.RawMessage {
	if !strings.HasPrefix(text, "watcher: ") {
		return nil
	}
	_, ev, ok := strings.Cut(text, evidenceMarker)
	if !ok {
		return nil
	}
	return json.RawMessage(ev)
}

// applyForecastEvidence folds the forecast of a watcher room.created alert
// or a watcher join note into the snapshot. Only the watcher's own evidence
// counts: another source's alert body is the sender's text, and only the
// system actor writes join notes.
func applyForecastEvidence(snap *RoomSnapshot, e events.Event) {
	var raw json.RawMessage
	switch e.Type {
	case events.RoomCreated:
		p, ok := decode[events.RoomCreatedPayload](e)
		if !ok || p.AlertSource != string(alertin.Watcher) {
			return
		}
		raw = p.Alert
	case events.AgentMessage:
		if e.Actor.Kind != events.ActorSystem {
			return
		}
		p, ok := decode[events.AgentMessagePayload](e)
		if !ok {
			return
		}
		raw = noteEvidence(p.Text)
	default:
		return
	}
	if f := forecastFrom(raw, e.At); f != nil {
		snap.Forecast = f
	}
}

// forecastNote is the first system message of a forecast room: one line per
// forecast of the opening evidence, then how to start the agent. Watcher
// numbers and names only.
func forecastNote(raw json.RawMessage) string {
	var b strings.Builder
	ev, _ := decodeWatcherEvidence(raw)
	for _, f := range forecastsOf(ev) {
		unit := clients.WatchMetricUnit(f.Metric)
		name := f.Metric
		if f.Object != "" {
			name += " of " + f.Object
		}
		fmt.Fprintf(&b, "forecast: %s is %s of its %s limit and grows %s per hour; at this rate it reaches the limit around %s (R² %.2f over %d samples).\n",
			name, fmtMetric(f.Current, unit), fmtMetric(f.Limit, unit), fmtMetric(f.Slope, unit),
			f.ETA.UTC().Format("2006-01-02 15:04 UTC"), f.R2, f.Samples)
	}
	if b.Len() == 0 {
		b.WriteString("forecast: a pipeline metric is trending toward its limit.\n")
	}
	b.WriteString("This room opened quietly: the agent has not run. The driver can steer, or anyone can mention @agent, to start it; it also starts if the watcher's real signal fires.")
	return stripBidi(sanitizeAgentText(b.String()))
}

// fmtMetric renders a value in a watch metric's unit.
func fmtMetric(v float64, unit string) string {
	switch unit {
	case "bytes":
		switch {
		case v >= 1<<40:
			return fmt.Sprintf("%.1f TiB", v/(1<<40))
		case v >= 1<<30:
			return fmt.Sprintf("%.1f GiB", v/(1<<30))
		case v >= 1<<20:
			return fmt.Sprintf("%.1f MiB", v/(1<<20))
		}
		return fmt.Sprintf("%.0f bytes", v)
	case "seconds":
		return (time.Duration(v * float64(time.Second))).Round(time.Second).String()
	case "records":
		return fmt.Sprintf("%.0f records", v)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// sevRank orders severities: sev1 is the most severe (1); anything else
// ranks below sev4.
func sevRank(sev string) int {
	if !severities[sev] {
		return 5
	}
	return int(sev[3] - '0')
}

// signalNameRe is the shape of a watcher signal name.
var signalNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// escalateLocked writes room.escalated when watcher evidence for the room's
// own pipeline carries a severity above the room's: a room is raised, never
// lowered. The signal named is the first entry at the new severity that is
// not a forecast (the real signal that joined), else the first at that
// severity. It reports whether the event was written. A failed append is
// logged and not returned: the note is already in the room, and the
// watcher's next post tries the raise again. Caller holds st's lock.
func (s *Service) escalateLocked(ctx context.Context, roomID string, st *roomState, evidence json.RawMessage) bool {
	ev, ok := decodeWatcherEvidence(evidence)
	if !ok || ev.Pipeline != st.snap.PipelineID || !severities[ev.Severity] || sevRank(ev.Severity) >= sevRank(st.snap.Severity) {
		return false
	}
	signal := ""
	for _, raw := range ev.Signals {
		var h signalHead
		if json.Unmarshal(raw, &h) != nil || h.Severity != ev.Severity || !signalNameRe.MatchString(h.Signal) {
			continue
		}
		if signal == "" {
			signal = h.Signal
		}
		if h.Signal != events.SignalForecast {
			signal = h.Signal
			break
		}
	}
	from := st.snap.Severity
	if _, err := s.emitLocked(ctx, st, roomID, events.RoomEscalated, systemActor, events.RoomEscalatedPayload{
		Severity: ev.Severity, From: from, Signal: signal,
	}); err != nil {
		if errors.Is(err, store.ErrLogFull) {
			s.log.Warn("room: escalation not stored, room log is full", "room", roomID, "severity", ev.Severity)
		} else {
			s.log.Warn("room: escalation not stored; the next watcher post retries it", "room", roomID, "severity", ev.Severity, "err", err)
		}
		return false
	}
	s.log.Info("room: escalated", "room", roomID, "from", from, "severity", ev.Severity, "signal", signal)
	return true
}
