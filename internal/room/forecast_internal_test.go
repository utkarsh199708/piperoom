package room

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

func TestIsForecastKey(t *testing.T) {
	for _, tc := range []struct {
		pid, key string
		want     bool
	}{
		{"orders", "watcher:orders:forecast:slot_wal_bytes", true},
		{"orders", "watcher:orders:forecast:db_size_bytes", true},
		{"orders", "watcher:orders:forecast:topic_oldest_unread_age", true},
		{"orders", "watcher:orders", false},
		{"orders", "watcher:orders:forecast:", false},
		{"orders", "watcher:orders:forecast:cpu", false},
		{"orders", "watcher:payments:forecast:slot_wal_bytes", false},
		{"orders", "generic:watcher:orders:forecast:slot_wal_bytes", false},
		{"orders", "", false},
	} {
		if got := isForecastKey(tc.pid, tc.key); got != tc.want {
			t.Errorf("isForecastKey(%q, %q) = %v, want %v", tc.pid, tc.key, got, tc.want)
		}
	}
}

func TestFmtMetric(t *testing.T) {
	for _, tc := range []struct {
		v          float64
		unit, want string
	}{
		{107374182400, "bytes", "100.0 GiB"},
		{3 << 40, "bytes", "3.0 TiB"},
		{5 << 20, "bytes", "5.0 MiB"},
		{512, "bytes", "512 bytes"},
		{5400.4, "seconds", "1h30m0s"},
		{1234, "records", "1234 records"},
		{1.5, "", "1.5"},
	} {
		if got := fmtMetric(tc.v, tc.unit); got != tc.want {
			t.Errorf("fmtMetric(%v, %q) = %q, want %q", tc.v, tc.unit, got, tc.want)
		}
	}
}

func fcEntry(metric, eta string, current float64) string {
	cur, _ := json.Marshal(current)
	return `{"signal":"forecast","severity":"sev4","metric":"` + metric + `","current":` + string(cur) +
		`,"limit":100,"eta":"` + eta + `","slope_per_hour":1,"r2":0.9,"samples":10}`
}

func evidenceWith(source string, entries ...string) json.RawMessage {
	return json.RawMessage(`{"source":"` + source + `","pipeline":"orders","severity":"sev4","signals":[` + strings.Join(entries, ",") + `]}`)
}

// The snapshot's forecast comes only from the watcher's own evidence: a
// watcher room.created alert or a system join note; the soonest ETA wins
// inside one evidence object, the newest evidence wins over older ones.
func TestApplyForecastEvidence(t *testing.T) {
	at := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	created := func(source, origin string, raw json.RawMessage) events.Event {
		p, _ := json.Marshal(events.RoomCreatedPayload{Title: "t", Severity: "sev4", PipelineID: "orders", Alert: raw, AlertSource: source, Origin: origin})
		return events.Event{RoomID: "r_1", Seq: 1, Type: events.RoomCreated, Actor: systemActor, At: at, Payload: p}
	}
	note := func(seq int64, actor events.Actor, text string) events.Event {
		p, _ := json.Marshal(events.AgentMessagePayload{Text: text})
		return events.Event{RoomID: "r_1", Seq: seq, Type: events.AgentMessage, Actor: actor, At: at.Add(time.Duration(seq) * time.Minute), Payload: p}
	}
	two := evidenceWith("watcher", fcEntry("slot_wal_bytes", "2026-10-15T00:00:00Z", 1), fcEntry("db_size_bytes", "2026-10-14T00:00:00Z", 2),
		`{"signal":"slot_wal","severity":"sev2","current":"not a number"}`)

	s := Apply(NewSnapshot(), created("watcher", events.RoomOriginForecast, two))
	if s.Origin != events.RoomOriginForecast || s.Forecast == nil || s.Forecast.Metric != "db_size_bytes" || !s.Forecast.At.Equal(at) {
		t.Fatalf("created: origin=%q forecast=%+v", s.Origin, s.Forecast)
	}
	newer := evidenceWith("watcher", fcEntry("slot_wal_bytes", "2026-10-13T12:00:00Z", 7))
	s = Apply(s, note(2, systemActor, "watcher: x (new signal)"+evidenceMarker+string(newer)))
	if s.Forecast.Metric != "slot_wal_bytes" || s.Forecast.Current != 7 {
		t.Fatalf("join note: forecast=%+v", s.Forecast)
	}
	// Ignored: an agent's message, a note that is not the watcher's, a cut
	// note, evidence without a forecast.
	agentText := "watcher: x" + evidenceMarker + string(evidenceWith("watcher", fcEntry("group_lag", "2026-10-13T10:00:00Z", 9)))
	for i, e := range []events.Event{
		note(3, events.AgentActor, agentText),
		note(4, systemActor, "pipeline \"x\" is unknown"+evidenceMarker+string(newer)),
		note(5, systemActor, "watcher: x"+evidenceMarker+`{"source":"watcher","signals":[{"sig…`),
		note(6, systemActor, "watcher: x"+evidenceMarker+string(evidenceWith("watcher", `{"signal":"slot_wal","severity":"sev2"}`))),
	} {
		if s2 := Apply(s, e); s2.Forecast == nil || *s2.Forecast != *s.Forecast {
			t.Errorf("case %d changed the forecast to %+v", i, s2.Forecast)
		}
	}
	// A metric outside clients.WatchMetrics is not one the watcher writes.
	if s2 := Apply(NewSnapshot(), created("watcher", "", evidenceWith("watcher", fcEntry("cpu", "2026-10-13T10:00:00Z", 1)))); s2.Forecast != nil {
		t.Errorf("unknown metric folded: %+v", s2.Forecast)
	}
	// Another source's alert body is the sender's text.
	if s2 := Apply(NewSnapshot(), created("generic", "", evidenceWith("watcher", fcEntry("slot_wal_bytes", "2026-10-13T10:00:00Z", 1)))); s2.Forecast != nil || s2.Origin != "" {
		t.Errorf("generic alert folded: origin=%q forecast=%+v", s2.Origin, s2.Forecast)
	}
	if !s.Clone().Forecast.ETA.Equal(s.Forecast.ETA) || s.Clone().Forecast == s.Forecast {
		t.Error("Clone must deep-copy the forecast")
	}
}

func TestApplyEscalatedAndQuiet(t *testing.T) {
	ev := func(seq int64, typ events.Type, actor events.Actor, payload any) events.Event {
		p, _ := json.Marshal(payload)
		return events.Event{RoomID: "r_1", Seq: seq, Type: typ, Actor: actor, Payload: p}
	}
	evs := []events.Event{
		ev(1, events.RoomCreated, systemActor, events.RoomCreatedPayload{Severity: "sev4", PipelineID: "orders", Origin: events.RoomOriginForecast}),
		ev(2, events.AgentMessage, systemActor, events.AgentMessagePayload{Text: "forecast: ..."}),
	}
	s := NewSnapshot()
	for _, e := range evs {
		s = Apply(s, e)
	}
	if !quietRoom(s) || !quietLog(evs) {
		t.Fatal("a forecast room with only system events must be quiet")
	}
	s = Apply(s, ev(3, events.RoomEscalated, systemActor, events.RoomEscalatedPayload{Severity: "sev2", From: "sev4", Signal: "slot_wal"}))
	if s.Severity != "sev2" {
		t.Fatalf("severity after escalation = %q", s.Severity)
	}
	if s2 := Apply(s, ev(4, events.RoomEscalated, systemActor, events.RoomEscalatedPayload{Severity: "sev9"})); s2.Severity != "sev2" {
		t.Fatalf("an invalid severity was folded: %q", s2.Severity)
	}
	if !quietRoom(s) {
		t.Fatal("escalation alone does not end quiet: the service starts the agent")
	}
	status := ev(5, events.AgentStatus, events.AgentActor, events.AgentStatusPayload{State: "working"})
	if quietRoom(Apply(s, status)) || quietLog(append(evs, status)) {
		t.Fatal("an agent event ends quiet")
	}
	normal := []events.Event{ev(1, events.RoomCreated, systemActor, events.RoomCreatedPayload{Severity: "sev2", PipelineID: "orders"})}
	ns := Apply(NewSnapshot(), normal[0])
	if quietRoom(ns) || quietLog(normal) {
		t.Fatal("a normal room is never quiet")
	}
	// Additive JSON: a room that is not a forecast room has neither key.
	b, _ := json.Marshal(ns)
	if strings.Contains(string(b), `"origin"`) || strings.Contains(string(b), `"forecast"`) {
		t.Fatalf("normal room JSON carries X3 keys: %s", b)
	}
}

func TestForecastNote(t *testing.T) {
	got := forecastNote(evidenceWith("watcher", fcEntry("slot_wal_bytes", "2026-10-14T03:00:00Z", 64424509440)))
	if !strings.HasPrefix(got, "forecast: slot_wal_bytes is 60.0 GiB of its 100 bytes limit and grows 1 bytes per hour; at this rate it reaches the limit around 2026-10-14 03:00 UTC (R² 0.90 over 10 samples).") {
		t.Fatalf("note = %q", got)
	}
	if fallback := forecastNote(json.RawMessage(`{"source":"watcher"}`)); !strings.HasPrefix(fallback, "forecast: a pipeline metric") || !strings.Contains(fallback, "@agent") {
		t.Fatalf("fallback note = %q", fallback)
	}
}
