package wiring

import (
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/tools/similarincidents"
)

func TestRegisterToolsAddsSimilarIncidentsOnlyWithARoomStore(t *testing.T) {
	for _, withRooms := range []bool{false, true} {
		w := &Wired{Owners: &owners.Owners{}}
		if withRooms {
			w.Rooms = similarincidents.FromStore(memstore.New())
		}
		reg := tools.NewRegistry()
		if err := RegisterTools(reg, actions.NewRegistry(), w); err != nil {
			t.Fatalf("RegisterTools(rooms=%v): %v", withRooms, err)
		}
		tl, ok := reg.Get(similarincidents.ToolName)
		if ok != withRooms {
			t.Errorf("rooms=%v: similar_incidents registered = %v", withRooms, ok)
		}
		if ok && tl.Kind() != tools.KindRead {
			t.Errorf("similar_incidents kind = %v, want read", tl.Kind())
		}
	}
}

func TestRegisterToolsAlwaysAddsWhatChanged(t *testing.T) {
	for _, withRooms := range []bool{false, true} {
		w := &Wired{Owners: &owners.Owners{}}
		if withRooms {
			w.Rooms = similarincidents.FromStore(memstore.New())
		}
		reg := tools.NewRegistry()
		if err := RegisterTools(reg, actions.NewRegistry(), w); err != nil {
			t.Fatalf("RegisterTools(rooms=%v): %v", withRooms, err)
		}
		tl, ok := reg.Get("what_changed")
		if !ok || tl.Kind() != tools.KindRead {
			t.Errorf("rooms=%v: what_changed registered = %v", withRooms, ok)
		}
	}
}
