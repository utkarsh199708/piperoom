package alertin

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

// genericBodyOfSize returns a generic-form body of exactly n bytes.
func genericBodyOfSize(t *testing.T, n int) []byte {
	t.Helper()
	const prefix = `{"title":"x","pad":"`
	const suffix = `"}`
	if n < len(prefix)+len(suffix) {
		t.Fatalf("size %d too small", n)
	}
	return []byte(prefix + strings.Repeat("a", n-len(prefix)-len(suffix)) + suffix)
}

// amBody returns a firing Alertmanager (version 4) body with one alert.
func amBody(t *testing.T, labels, annotations map[string]string) []byte {
	t.Helper()
	if annotations == nil {
		annotations = map[string]string{"summary": "t"}
	}
	b, err := json.Marshal(map[string]any{
		"version": "4", "groupKey": "{}:{}", "status": "firing",
		"alerts": []map[string]any{{
			"status": "firing", "labels": labels, "annotations": annotations,
			"startsAt": "2026-10-11T10:00:00Z", "fingerprint": "f1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBodyCapBoundary(t *testing.T) {
	exact := genericBodyOfSize(t, MaxBodyBytes)
	over := genericBodyOfSize(t, MaxBodyBytes+1)
	if len(exact) != MaxBodyBytes || len(over) != MaxBodyBytes+1 {
		t.Fatalf("fixture sizes %d, %d", len(exact), len(over))
	}

	t.Run("parse at cap", func(t *testing.T) {
		if _, err := testConfig().Parse(Generic, exact); err != nil {
			t.Fatalf("exact cap rejected: %v", err)
		}
	})
	t.Run("parse one over", func(t *testing.T) {
		if _, err := testConfig().Parse(Generic, over); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("got %v, want ErrTooLarge", err)
		}
	})
	t.Run("ingest at cap, signed", func(t *testing.T) {
		cfg := testConfig()
		a, err := cfg.Ingest(Generic, signedHeaders(t, Generic, exact, testNow), exact)
		if err != nil || a.Title != "x" {
			t.Fatalf("exact cap: %+v, %v", a, err)
		}
	})
	t.Run("ingest one over, unsigned", func(t *testing.T) {
		// The size check runs before authentication, so the reason is ErrTooLarge
		// and not ErrUnauthenticated.
		_, err := testConfig().Ingest(Generic, nil, over)
		if !errors.Is(err, ErrTooLarge) || errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("got %v, want ErrTooLarge only", err)
		}
	})
	t.Run("parse one over is not a decode error", func(t *testing.T) {
		// Invalid JSON over the cap must still report the size, not the decode error.
		bad := []byte(strings.Repeat("{", MaxBodyBytes+1))
		if _, err := testConfig().Parse(Alertmanager, bad); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("got %v, want ErrTooLarge", err)
		}
	})
}

func TestHugeBodyRejected(t *testing.T) {
	huge := genericBodyOfSize(t, 10*MaxBodyBytes)
	_, err := testConfig().Ingest(Generic, signedHeaders(t, Generic, huge, testNow), huge)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
}

// The error text is logged and may reach a client, so it must not carry the
// body. The size message carries only the two lengths.
func TestErrorsDoNotEchoBody(t *testing.T) {
	const marker = "SECRET-MARKER-7f3a9c2e1b"
	cases := []struct {
		name string
		err  func() error
	}{
		{"too large", func() error {
			body := []byte(`{"title":"` + marker + strings.Repeat("a", MaxBodyBytes) + `"}`)
			_, err := testConfig().Ingest(Generic, nil, body)
			return err
		}},
		{"too large, parse", func() error {
			body := []byte(`{"title":"` + marker + strings.Repeat("a", MaxBodyBytes) + `"}`)
			_, err := testConfig().Parse(Generic, body)
			return err
		}},
		{"not json, generic", func() error {
			_, err := testConfig().Parse(Generic, []byte(marker+"{"))
			return err
		}},
		// The JSON decoder names one offending byte at most, never the value.
		{"bad json, alertmanager", func() error {
			_, err := testConfig().Parse(Alertmanager, []byte(marker+"{"))
			return err
		}},
		{"bad json, pagerduty", func() error {
			_, err := testConfig().Parse(PagerDuty, []byte(`{"event":{"event_type":"`+marker+`"`))
			return err
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.err()
			if err == nil {
				t.Fatal("expected an error")
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("error echoes the body: %q", err)
			}
		})
	}
}

func TestTruncateRunes(t *testing.T) {
	tests := []struct {
		in   string
		n    int
		want string
	}{
		{"", 3, ""},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
		{"abc", 0, ""},
		{"€€€", 2, "€€"},
		{"a€b", 2, "a€"},
		{"a😀b", 2, "a😀"},
		{"😀😀", 1, "😀"},
	}
	for _, tt := range tests {
		got := truncateRunes(tt.in, tt.n)
		if got != tt.want || !utf8.ValidString(got) {
			t.Errorf("truncateRunes(%q, %d) = %q, want %q", tt.in, tt.n, got, tt.want)
		}
	}
}

// A cut that lands right before a multibyte rune keeps that rune whole.
func TestTitleCutAtMultibyteRune(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{"exactly at cap", strings.Repeat("a", MaxTitleRunes), strings.Repeat("a", MaxTitleRunes)},
		{"two-byte rune at the cut", strings.Repeat("a", MaxTitleRunes-1) + "€€", strings.Repeat("a", MaxTitleRunes-1) + "€"},
		{"four-byte rune at the cut", strings.Repeat("a", MaxTitleRunes-1) + "😀x", strings.Repeat("a", MaxTitleRunes-1) + "😀"},
		{"all multibyte", strings.Repeat("é", MaxTitleRunes+50), strings.Repeat("é", MaxTitleRunes)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cleanTitle(tt.in, "fallback")
			if got != tt.want || !utf8.ValidString(got) {
				t.Fatalf("got %q (%d runes), want %q", got, utf8.RuneCountInString(got), tt.want)
			}
		})
	}
}

// A description is only scanned for names, so a name past the cap is not
// matched. A name inside the cap still is.
func TestMatchTextCap(t *testing.T) {
	cfg := testConfig()
	cfg.Owners = testOwners()
	late := strings.Repeat("é", MaxMatchTextRunes+100) + " pg-orders-connector"
	early := "pg-orders-connector " + strings.Repeat("é", MaxMatchTextRunes+100)

	t.Run("name past the cap is not matched", func(t *testing.T) {
		a, err := cfg.Parse(Alertmanager, amBody(t, map[string]string{"alertname": "X"}, map[string]string{"summary": "t", "description": late}))
		if err != nil || a.PipelineID != "" {
			t.Fatalf("got pipeline %q, %v; want none", a.PipelineID, err)
		}
	})
	t.Run("name inside the cap is matched", func(t *testing.T) {
		a, err := cfg.Parse(Alertmanager, amBody(t, map[string]string{"alertname": "X"}, map[string]string{"summary": "t", "description": early}))
		if err != nil || a.PipelineID != "orders" {
			t.Fatalf("got pipeline %q, %v; want orders", a.PipelineID, err)
		}
	})
	t.Run("cut text stays valid UTF-8", func(t *testing.T) {
		p, err := parseAlertmanager(Alertmanager, amBody(t, nil, map[string]string{"summary": "t", "description": late}))
		if err != nil {
			t.Fatal(err)
		}
		desc := p.text[2]
		if utf8.RuneCountInString(desc) != MaxMatchTextRunes || !utf8.ValidString(desc) {
			t.Fatalf("description: %d runes, valid=%v", utf8.RuneCountInString(desc), utf8.ValidString(desc))
		}
	})
}

// Many labels stay inside the body cap but must not make matching unbounded.
func TestManyLabels(t *testing.T) {
	labels := map[string]string{"alertname": "X", "pipeline": "orders"}
	for i := range 1000 {
		labels[fmt.Sprintf("extra_%04d", i)] = fmt.Sprintf("value-%04d", i)
	}
	body := amBody(t, labels, nil)

	p, err := parseAlertmanager(Alertmanager, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.values) != MaxLabelValues {
		t.Fatalf("values: %d, want %d", len(p.values), MaxLabelValues)
	}
	// The same body must keep the same values on every parse, whatever order
	// the map is iterated in.
	for range 5 {
		again, err := parseAlertmanager(Alertmanager, body)
		if err != nil || !reflect.DeepEqual(p.values, again.values) {
			t.Fatalf("values changed between parses")
		}
	}
	// An explicit pipeline label is read from the map, not from the capped
	// values, so it still counts.
	cfg := testConfig()
	cfg.Owners = testOwners()
	a, err := cfg.Parse(Alertmanager, body)
	if err != nil || a.PipelineID != "orders" || a.PipelineVia != ViaLabel {
		t.Fatalf("got %q via %q, %v; want orders via label", a.PipelineID, a.PipelineVia, err)
	}
}

func TestLabelValueCap(t *testing.T) {
	long := strings.Repeat("é", MaxLabelRunes+500)
	got := labelValues(map[string]string{"k": long})
	if len(got) != 1 || utf8.RuneCountInString(got[0]) != MaxLabelRunes || !utf8.ValidString(got[0]) {
		t.Fatalf("label value not capped: %d values", len(got))
	}
}

func TestManyTagsAndKeysCapped(t *testing.T) {
	var tags []string
	tags = append(tags, "pipeline:orders")
	for i := range 10000 {
		tags = append(tags, fmt.Sprintf("k%d:v%d", i, i))
	}
	body, err := json.Marshal(map[string]string{
		"id": "1", "title": "t", "transition": "Triggered", "tags": strings.Join(tags, ","),
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := parseDatadog(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.values) != MaxLabelValues {
		t.Fatalf("datadog values: %d, want %d", len(p.values), MaxLabelValues)
	}

	fields := map[string]string{"title": "t"}
	for i := range 1000 {
		fields[fmt.Sprintf("f%04d", i)] = "v"
	}
	gbody, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	g, err := parseGeneric(gbody)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.values) != MaxLabelValues {
		t.Fatalf("generic values: %d, want %d", len(g.values), MaxLabelValues)
	}
}
