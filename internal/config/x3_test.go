package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseWatchForecast(t *testing.T) {
	t.Setenv("PIPEROOM_WATCH_FORECAST_HORIZON", "")
	if f, err := ParseWatchForecast(Load()); err != nil || f.Horizon != 48*time.Hour {
		t.Fatalf("default: ParseWatchForecast = %+v, %v; want 48h", f, err)
	}
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"", 48 * time.Hour, false},
		{" 24h ", 24 * time.Hour, false},
		{"1h", time.Hour, false},         // lower bound
		{"168h", 168 * time.Hour, false}, // upper bound: the trends window
		{"59m", 0, true},
		{"169h", 0, true},
		{"0", 0, true},
		{"-1h", 0, true},
		{"two days", 0, true},
	}
	for _, tt := range tests {
		f, err := ParseWatchForecast(Config{WatchForecastHorizon: tt.in})
		if tt.wantErr {
			if err == nil || !strings.Contains(err.Error(), "PIPEROOM_WATCH_FORECAST_HORIZON") {
				t.Errorf("%q: err = %v; want one naming PIPEROOM_WATCH_FORECAST_HORIZON", tt.in, err)
			}
			continue
		}
		if err != nil || f.Horizon != tt.want {
			t.Errorf("%q: ParseWatchForecast = %+v, %v; want %s", tt.in, f, err, tt.want)
		}
	}
}

// The sample constants are one design: the fit window fits in the history,
// the minimum fit is reachable inside it, and the history is whole periods.
func TestWatchSampleConstantsAgree(t *testing.T) {
	if WatchForecastFitWindow > WatchSampleRetention {
		t.Errorf("fit window %s longer than retention %s", WatchForecastFitWindow, WatchSampleRetention)
	}
	if n := int(WatchForecastFitWindow / WatchSampleInterval); n < WatchForecastMinSamples {
		t.Errorf("fit window holds %d samples, fewer than the minimum %d", n, WatchForecastMinSamples)
	}
	if WatchSampleRetention%WatchSampleInterval != 0 {
		t.Errorf("retention %s is not whole sample periods of %s", WatchSampleRetention, WatchSampleInterval)
	}
	if MaxWatchForecastHorizon > WatchSampleRetention {
		t.Errorf("max horizon %s past the trends window %s", MaxWatchForecastHorizon, WatchSampleRetention)
	}
	if WatchForecastMinR2 <= 0 || WatchForecastMinR2 >= 1 {
		t.Errorf("WatchForecastMinR2 = %v; want in (0, 1)", WatchForecastMinR2)
	}
}
