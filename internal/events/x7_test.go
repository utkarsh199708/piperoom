package events_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// E1: the retention risk entry round-trips strictly, zero values included,
// with the "sev" key the connector risks of config_risk use.
func TestX7RetentionRiskRoundTrip(t *testing.T) {
	for _, v := range []events.RetentionRisk{
		{Code: events.RiskRetentionShorter, Sev: events.RetentionRiskSeverity, Topic: "pg.public.orders",
			Configured: "1d", Declared: "7d", Text: "pg.public.orders retains 1d; declared 7d"},
		{Code: events.RiskRetentionLonger, Sev: events.RetentionRiskSeverity, Topic: "pg.public.orders",
			Configured: "infinite", Declared: "30d", RetentionBytes: 1 << 30, Text: "longer than declared"},
		{Code: events.RiskRetentionShorter, Sev: events.RetentionRiskSeverity, Table: "analytics.orders",
			Configured: "30d", Declared: "400d", Text: "t"},
		{Code: events.RiskCompactCDCTopic, Sev: events.RetentionRiskSeverity, Topic: "pg.public.orders",
			CleanupPolicy: "compact", Text: "compact"},
		{},
	} {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if got := strictDecode(t, raw, v); !reflect.DeepEqual(got, v) {
			t.Errorf("round trip\n got %#v\nwant %#v", got, v)
		}
		if v.Sev != "" && !strings.Contains(string(raw), `"sev":"sev4"`) {
			t.Errorf("no sev key: %s", raw)
		}
	}
	raw, _ := json.Marshal(events.RetentionRisk{})
	if string(raw) != `{"code":"","sev":"","text":""}` {
		t.Fatalf("zero entry = %s", raw)
	}
}

// The codes are distinct words and the severity is the documented sev4, which
// is not one of the room-opening X-wave defaults.
func TestX7RetentionRiskCodes(t *testing.T) {
	codes := []string{events.RiskRetentionShorter, events.RiskRetentionLonger, events.RiskCompactCDCTopic}
	seen := map[string]bool{}
	for _, c := range codes {
		if c == "" || seen[c] || strings.ContainsAny(c, " .") {
			t.Fatalf("bad or duplicate code %q", c)
		}
		seen[c] = true
	}
	if events.RetentionRiskSeverity != "sev4" {
		t.Fatalf("RetentionRiskSeverity = %q", events.RetentionRiskSeverity)
	}
}
