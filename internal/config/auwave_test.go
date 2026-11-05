package config

import (
	"strings"
	"testing"
)

func TestParseAutonomy(t *testing.T) {
	tests := []struct {
		name, autonomy, write string
		want                  bool
		wantErr               string // a fragment of the error; "" = no error
	}{
		{"unset, write on: on", "", "on", true, ""},
		{"on, write on", "on", "on", true, ""},
		{"any case", " ON ", "on", true, ""},
		{"off, write on", "off", "on", false, ""},
		{"unset, write off: off", "", "off", false, ""},
		{"on, write off: still off", "on", "off", false, ""},
		{"on, write unset: off", "on", "", false, ""},
		{"bad value, write on", "true", "on", false, "PIPEROOM_AUTONOMY"},
		{"bad value, write off: still refused", "yes", "off", false, "PIPEROOM_AUTONOMY"},
		{"bad write mode", "on", "1", false, "PIPEROOM_WRITE_MODE"},
	}
	for _, tt := range tests {
		got, err := ParseAutonomy(Config{Autonomy: tt.autonomy, WriteMode: tt.write})
		if tt.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("%s: err = %v, want one naming %s", tt.name, err, tt.wantErr)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("%s: ParseAutonomy = %v, %v; want %v", tt.name, got, err, tt.want)
		}
	}
}

func TestAutonomyDefault(t *testing.T) {
	t.Setenv("PIPEROOM_AUTONOMY", "")
	t.Setenv("PIPEROOM_WRITE_MODE", "on")
	c := Load()
	if c.Autonomy != "on" {
		t.Errorf("Load().Autonomy = %q, want on", c.Autonomy)
	}
	if on, err := ParseAutonomy(c); err != nil || !on {
		t.Errorf("ParseAutonomy(default, write on) = %v, %v; want on", on, err)
	}
}
