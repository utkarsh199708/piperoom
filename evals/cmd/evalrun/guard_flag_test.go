package main

import (
	"bytes"
	"strings"
	"testing"
)

// The guard mode of an eval run: enforce by default (the server's own default
// is log), EVAL_GUARD and -guard override, a typo is a usage error before any
// work.
func TestGuardFlag(t *testing.T) {
	env := func(v string) func(string) string {
		return func(k string) string {
			if k == "EVAL_GUARD" {
				return v
			}
			return ""
		}
	}
	for _, tc := range []struct{ env, want string }{{"", "enforce"}, {"  ", "enforce"}, {"log", "log"}, {"off", "off"}, {"enforce", "enforce"}} {
		if got := defaultGuard(env(tc.env)); got != strings.TrimSpace(tc.want) {
			t.Errorf("EVAL_GUARD=%q: default %q, want %q", tc.env, got, tc.want)
		}
	}

	t.Setenv("EVAL_GUARD", "")
	f, err := parseFlags(nil, &bytes.Buffer{})
	if err != nil || f.guard != "enforce" {
		t.Fatalf("no flag: %q, %v", f.guard, err)
	}
	for _, mode := range []string{"off", "log", "enforce"} {
		f, err := parseFlags([]string{"-guard", mode}, &bytes.Buffer{})
		if err != nil || f.guard != mode {
			t.Errorf("-guard %s: %q, %v", mode, f.guard, err)
		}
	}

	t.Setenv("EVAL_GUARD", "log")
	if f, err := parseFlags(nil, &bytes.Buffer{}); err != nil || f.guard != "log" {
		t.Errorf("EVAL_GUARD=log: %q, %v", f.guard, err)
	}
	if f, err := parseFlags([]string{"-guard", "off"}, &bytes.Buffer{}); err != nil || f.guard != "off" {
		t.Errorf("the flag must beat the environment: %q, %v", f.guard, err)
	}

	for _, bad := range []string{"Enforce!", "on", "strict"} {
		var stderr bytes.Buffer
		if _, err := parseFlags([]string{"-guard", bad}, &stderr); err == nil || !strings.Contains(stderr.String(), "want off, log or enforce") {
			t.Errorf("-guard %s: err %v, stderr %q", bad, err, stderr.String())
		}
	}
	t.Setenv("EVAL_GUARD", "strict")
	var stderr bytes.Buffer
	if _, err := parseFlags(nil, &stderr); err == nil || !strings.Contains(stderr.String(), "EVAL_GUARD") {
		t.Errorf("EVAL_GUARD=strict: err %v, stderr %q", err, stderr.String())
	}
}
