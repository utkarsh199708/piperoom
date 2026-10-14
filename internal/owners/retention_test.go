package owners

import (
	"strings"
	"testing"
)

func withRetention(line string) string {
	return strings.Replace(validYAML, "    sink_ts_column: updated_at\n", "    sink_ts_column: updated_at\n"+line, 1)
}

func TestRetentionDaysParsed(t *testing.T) {
	o, err := Load(writeTemp(t, withRetention("    retention_days: 30\n")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	if d, ok := p.Retention(); !ok || d != 30 {
		t.Fatalf("retention_days = %v, want 30", p.RetentionDays)
	}

	o, err = Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if p, _ := o.Pipeline("orders"); p.RetentionDays != nil {
		t.Fatalf("retention_days left out = %v, want nil", *p.RetentionDays)
	}
}

// TestRetentionDaysRefused: every bad value stops Load (and so the server's
// start), naming the key.
func TestRetentionDaysRefused(t *testing.T) {
	for _, v := range []string{"0", "-1", "36501", "1.5", "thirty", "30d", "[30]"} {
		t.Run(v, func(t *testing.T) {
			_, err := Load(writeTemp(t, withRetention("    retention_days: "+v+"\n")))
			if err == nil {
				t.Fatalf("retention_days %s loaded", v)
			}
			if !strings.Contains(err.Error(), "retention_days") {
				t.Fatalf("error %q does not point at retention_days", err)
			}
		})
	}
	// A misspelt key is refused too (unknown keys stop the start).
	if _, err := Load(writeTemp(t, withRetention("    retention_day: 30\n"))); err == nil {
		t.Fatal("retention_day (typo) loaded")
	}
	// The bounds themselves load.
	for _, v := range []string{"1", "36500"} {
		if _, err := Load(writeTemp(t, withRetention("    retention_days: "+v+"\n"))); err != nil {
			t.Fatalf("retention_days %s: %v", v, err)
		}
	}
}
