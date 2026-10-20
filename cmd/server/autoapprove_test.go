package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

type namedAction struct{ name string }

func (a namedAction) Name() string                                           { return a.name }
func (namedAction) Layer() string                                            { return "kafka-connect" }
func (namedAction) Describe() string                                         { return "" }
func (namedAction) ParamsSchema() map[string]any                             { return map[string]any{"type": "object"} }
func (namedAction) Validate(context.Context, json.RawMessage) error          { return nil }
func (namedAction) Execute(context.Context, json.RawMessage) (string, error) { return "", nil }

func registryOf(t *testing.T, names ...string) *actions.Registry {
	t.Helper()
	r := actions.NewRegistry()
	for _, n := range names {
		if err := r.Register(namedAction{name: n}); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func TestCheckAutoApprove(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	own := func(list ...string) *owners.Owners {
		return &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", AutoApprove: list}}}
	}
	// Two client sets: the names of both count (the proposal registry of
	// each pipeline), and a nil registry (--demo off, no acts) is skipped.
	regs := []*actions.Registry{nil, registryOf(t, "connect_restart_task"), registryOf(t, "schema_set_compatibility", "consumer_restart")}
	for _, tc := range []struct {
		name string
		own  *owners.Owners
		want string
	}{
		{name: "known actions", own: own("connect_restart_task", "consumer_restart")},
		{name: "no list", own: own()},
		{name: "typo", own: own("connect_restart_taks"), want: `unknown action "connect_restart_taks"`},
		{name: "schema action", own: own("schema_set_compatibility"), want: "schema change"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, delay := range []time.Duration{0, time.Minute} {
				err := checkAutoApprove(tc.own, regs, delay, log)
				if tc.want == "" && err != nil {
					t.Fatalf("delay %v: %v", delay, err)
				}
				if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
					t.Fatalf("delay %v: err = %v, want %q", delay, err, tc.want)
				}
			}
		})
	}
	// An empty registry refuses every entry (never "skip the check").
	if err := checkAutoApprove(own("connect_restart_task"), nil, time.Minute, log); err == nil {
		t.Fatal("no registry accepted an auto_approve entry")
	}
}

func TestAutoApproveDelay(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want time.Duration
		bad  bool
	}{
		{in: "", want: 60 * time.Second},
		{in: "0", want: 0},
		{in: "2s", want: 2 * time.Second},
		{in: "0s", bad: true},
		{in: "2h", bad: true},
		{in: "soon", bad: true},
	} {
		got, err := autoApproveDelay(config.Config{AutoApproveDelay: tc.in})
		if tc.bad {
			if err == nil || !strings.Contains(err.Error(), "PIPEROOM_AUTO_APPROVE_DELAY") {
				t.Errorf("%q: err = %v, want a refusal naming the variable", tc.in, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%q = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
}
