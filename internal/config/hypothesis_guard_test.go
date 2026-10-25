package config

import "testing"

func TestParseHypothesisGuard(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", "log", false},
		{"  ", "log", false},
		{"off", "off", false},
		{"log", "log", false},
		{"enforce", "enforce", false},
		{" enforce ", "enforce", false},
		{"on", "", true},
		{"true", "", true},
		{"ENFORCE", "", true},
	} {
		got, err := ParseHypothesisGuard(Config{HypothesisGuard: tc.in})
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("ParseHypothesisGuard(%q) = %q, %v; want %q, err=%v", tc.in, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestHypothesisGuardDefaultFromEnv(t *testing.T) {
	t.Setenv("PIPEROOM_HYPOTHESIS_GUARD", "")
	if got := Load().HypothesisGuard; got != DefaultHypothesisGuard {
		t.Fatalf("unset env: got %q, want %q", got, DefaultHypothesisGuard)
	}
	t.Setenv("PIPEROOM_HYPOTHESIS_GUARD", "enforce")
	if got := Load().HypothesisGuard; got != "enforce" {
		t.Fatalf("env enforce: got %q", got)
	}
}
