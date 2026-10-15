package agent

import (
	"errors"
	"testing"

	"piperoom/internal/tools"
)

// roomKindTool is a tool that claims KindRoom: it may change room state, so it
// must never sit in Deps.Tools, where the model runs tools unapproved.
type roomKindTool struct{ *recordingTool }

func (roomKindTool) Kind() tools.Kind { return tools.KindRoom }

func TestRunRefusesToolsThatAreNotReadOnly(t *testing.T) {
	reg := tools.NewRegistry() // a plain registry takes KindRoom
	if err := reg.Register(roomKindTool{&recordingTool{name: "sneaky_write", schema: testSchema("x")}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err := newRunner(Alert{Title: "t"}, Deps{Tools: reg, Inbox: &fakeInbox{}, Sink: &fakeSink{}, Logger: discardLogger()}, Options{})
	if !errors.Is(err, tools.ErrNotReadOnly) {
		t.Fatalf("newRunner = %v, want ErrNotReadOnly", err)
	}
}

func TestReadOnlyRegistryRefusesTheAgentsRoomTools(t *testing.T) {
	// The agent's own room tools are KindRoom; they are added by the agent,
	// never through the read-only registry, which must refuse them.
	reg := tools.NewReadOnlyRegistry()
	for _, rt := range newRoomTools(Deps{}, newProposalTracker()) {
		if err := reg.Register(rt); !errors.Is(err, tools.ErrNotReadOnly) {
			t.Errorf("Register(%s) = %v, want ErrNotReadOnly", rt.Name(), err)
		}
	}
	if len(reg.All()) != 0 {
		t.Fatalf("read-only registry took room tools: %d", len(reg.All()))
	}
}

func TestRunAcceptsReadOnlyRegistry(t *testing.T) {
	reg := tools.NewReadOnlyRegistry()
	if err := reg.Register(&recordingTool{name: "connect_status", schema: testSchema("connector")}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := newRunner(Alert{Title: "t"}, Deps{Tools: reg, Inbox: &fakeInbox{}, Sink: &fakeSink{}, Logger: discardLogger()}, Options{}); err != nil {
		t.Fatalf("newRunner: %v", err)
	}
}
