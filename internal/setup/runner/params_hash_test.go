package runner_test

import (
	"encoding/json"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/runner"
)

// TestParamsHashMatchesRoom: the runner's independent recompute equals
// internal/room.ComputeParamsHash, including the RFC 8785 unicode and number
// edges, so the runner and the room can never drift apart on what was approved.
func TestParamsHashMatchesRoom(t *testing.T) {
	cases := []struct {
		name string
		p    events.ProposalCreatedPayload
	}{
		{
			name: "unicode strings",
			p: events.ProposalCreatedPayload{
				ProposalID:     "p1",
				Title:          "fix Müller 日本語 🚀",
				Risk:           "low",
				Rollback:       "révert",
				RequiredLayers: []string{"environment", "postgres"},
				Steps: []events.ProposalStep{
					{Action: "compose_up", Layer: "environment", Params: json.RawMessage(`{"service":"connect","note":"héllo 世界"}`), Description: "start"},
				},
			},
		},
		{
			name: "number edges",
			p: events.ProposalCreatedPayload{
				ProposalID:     "p2",
				Title:          "tune",
				Risk:           "medium",
				Rollback:       "revert",
				RequiredLayers: []string{"postgres"},
				Steps: []events.ProposalStep{
					{Action: "pg_set", Layer: "postgres", Params: json.RawMessage(`{"a":1.0,"b":1e3,"c":-0,"d":2.5e2,"e":0.5,"f":1e-2}`), Description: "numbers"},
				},
			},
		},
		{
			name: "nested and empty",
			p: events.ProposalCreatedPayload{
				ProposalID: "p3",
				Title:      "nested",
				Risk:       "low",
				Rollback:   "revert",
				Steps: []events.ProposalStep{
					{Action: "compose_write", Layer: "environment", Params: json.RawMessage(`{"list":[1,2,3],"obj":{"k":"v"},"empty":{},"arr":[],"null":null}`), Description: "nested"},
				},
			},
		},
		{
			name: "unicode keys and no required layers",
			p: events.ProposalCreatedPayload{
				ProposalID: "p4",
				Title:      "keys",
				Risk:       "low",
				Rollback:   "revert",
				Steps: []events.ProposalStep{
					{Action: "file_write", Layer: "environment", Params: json.RawMessage(`{"naïve":1,"\/\/":true,"é":["x"]}`), Description: "keys"},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, err := room.ComputeParamsHash("r1", c.p.ProposalID, c.p.Title, c.p.Risk, c.p.Rollback, c.p.RequiredLayers, c.p.Steps)
			if err != nil {
				t.Fatalf("room.ComputeParamsHash: %v", err)
			}
			got, err := runner.ComputeParamsHash("r1", c.p.ProposalID, c.p.Title, c.p.Risk, c.p.Rollback, c.p.RequiredLayers, c.p.Steps)
			if err != nil {
				t.Fatalf("runner.ComputeParamsHash: %v", err)
			}
			if got != want {
				t.Fatalf("runner hash %q != room hash %q", got, want)
			}
		})
	}
}
