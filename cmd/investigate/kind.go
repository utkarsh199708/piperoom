package main

import (
	"fmt"
	"io"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/agent/fake"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/wiring"
	"piperoom/internal/wiring/netwire"
)

// roomKind returns the run's Alert.PipelineKind, as the room service derives
// it: the pipeline's owners kind. -kind (flagKind) may name it too, and must
// then match: the tools a room offers follow its pipeline, so a run that says
// batch for a CDC pipeline would show a room that cannot exist. Without the
// pipeline in owners (fake tools), -kind is the kind; with neither, "" (cdc).
func roomKind(flagKind string, pipe owners.Pipeline, havePipe bool) (string, error) {
	if flagKind == "" {
		if havePipe {
			return pipe.KindOrDefault(), nil
		}
		return "", nil
	}
	k, err := agent.RoomKind(flagKind)
	if err != nil {
		return "", fmt.Errorf("-kind: %w", err)
	}
	if havePipe && pipe.KindOrDefault() != k {
		return "", fmt.Errorf("-kind %s: pipeline %q is kind %s in the owners file", k, pipe.ID, pipe.KindOrDefault())
	}
	return k, nil
}

// listKind is the kind -list-tools lists: -kind when set (whatever the
// pipeline: listing asks "what does a room of this kind offer"), else the
// pipeline's.
func listKind(flagKind string, pipe owners.Pipeline, havePipe bool) (string, error) {
	if flagKind != "" {
		k, err := agent.RoomKind(flagKind)
		if err != nil {
			return "", fmt.Errorf("-kind: %w", err)
		}
		return k, nil
	}
	return roomKind("", pipe, havePipe)
}

// listToolsRegistry registers the read tools as the server does for the
// owners file's pipelines (wiring.RegisterTools: the packages of the kinds
// present), over no client: registration does no I/O, so listing needs no
// stack. similar_incidents needs the room store, which a CLI run has not, so
// it is not listed (nor offered in a CLI run). With fakeTools it is the
// replay tools.
func listToolsRegistry(own *owners.Owners, fakeTools bool) (*tools.Registry, error) {
	reg := tools.NewReadOnlyRegistry()
	if fakeTools {
		if err := fake.Register(reg); err != nil {
			return nil, fmt.Errorf("register fake tools: %w", err)
		}
		return reg, nil
	}
	if err := wiring.RegisterTools(reg, actions.NewRegistry(), &wiring.Wired{Owners: own, ProbeTools: netwire.Register}); err != nil {
		return nil, err
	}
	return reg, nil
}

// listTools writes the read tools a room of kind offers, one name per line in
// the order the model sees them (agent.ToolsForKind, the runner's filter).
func listTools(w io.Writer, reg *tools.Registry, kind string) error {
	room, err := agent.ToolsForKind(reg, kind)
	if err != nil {
		return err
	}
	for _, t := range room.All() {
		if _, err := fmt.Fprintln(w, t.Name()); err != nil {
			return fmt.Errorf("write tool list: %w", err)
		}
	}
	return nil
}
