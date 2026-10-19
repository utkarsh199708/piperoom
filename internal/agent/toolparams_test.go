package agent

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/batchtools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/dbttools"
	"piperoom/internal/tools/kafkatools"
)

// fullRegistry registers every real read tool of the packages agent can
// import without a cycle (db, Kafka, batch and dbt tools). Registration needs
// no live clients, only a non-nil Owners.
func fullRegistry(t *testing.T) *tools.Registry {
	t.Helper()
	reg := tools.NewRegistry()
	if err := dbtools.Register(reg, dbtools.Deps{Owners: &owners.Owners{}}); err != nil {
		t.Fatal(err)
	}
	if err := kafkatools.Register(reg, kafkatools.Deps{}); err != nil {
		t.Fatal(err)
	}
	// The batch packages (P4c-2, P4d-2): a set with both kinds registers
	// them next to the CDC ones, so the cap must hold with them too.
	if err := batchtools.Register(reg, batchtools.Deps{Owners: &owners.Owners{}}); err != nil {
		t.Fatal(err)
	}
	if err := dbttools.Register(reg, dbttools.Deps{Owners: &owners.Owners{}}); err != nil {
		t.Fatal(err)
	}
	return reg
}

// strictNames reads the tool definitions back from the JSON the request sends
// and returns the names in send order plus the set carrying strict: true.
func strictNames(t *testing.T, tl toolList) (order []string, strict map[string]bool) {
	t.Helper()
	strict = map[string]bool{}
	for _, p := range tl.params {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("marshal tool param: %v", err)
		}
		var def struct {
			Name   string `json:"name"`
			Strict *bool  `json:"strict"`
		}
		if err := json.Unmarshal(b, &def); err != nil {
			t.Fatalf("unmarshal tool param: %v", err)
		}
		order = append(order, def.Name)
		if def.Strict != nil && *def.Strict {
			strict[def.Name] = true
		}
	}
	return order, strict
}

// The live run sends every registered read tool plus the room tools. The API
// rejects a request with more than maxStrictTools strict tools, and one whose
// strict schemas compile to a grammar that is too large, so the full registry
// must stay within the cap and spend the slots on the room tools and the
// most-used read tools.
func TestFullRegistryStaysWithinStrictToolCap(t *testing.T) {
	reg := fullRegistry(t)
	room := newRoomTools(Deps{Actions: actions.NewRegistry()}, nil)
	order, strict := strictNames(t, buildToolList(reg, room))

	if len(order) <= maxStrictTools {
		t.Fatalf("full registry sends %d tools; the cap only bites above %d, so this test no longer proves anything", len(order), maxStrictTools)
	}
	if len(strict) > maxStrictTools {
		t.Fatalf("%d strict tools in the request, the API allows %d", len(strict), maxStrictTools)
	}
	if len(strict) != maxStrictTools {
		t.Errorf("%d strict tools, want the whole budget of %d used", len(strict), maxStrictTools)
	}
	for _, rt := range room {
		if !strict[rt.Name()] {
			t.Errorf("room tool %s must stay strict", rt.Name())
		}
	}
	slots := maxStrictTools - len(room)
	for _, name := range strictReadTools()[:min(slots, len(strictReadTools()))] {
		if !strict[name] {
			t.Errorf("most-used read tool %s must be strict", name)
		}
	}
}

// strictReadTools must name real registered tools, or a typo would silently
// hand its slot to whatever read tool was registered first.
func TestStrictReadToolsAreRegistered(t *testing.T) {
	reg := fullRegistry(t)
	seen := map[string]bool{}
	for _, name := range strictReadTools() {
		if _, ok := reg.Get(name); !ok {
			t.Errorf("strictReadTools names %q, which no read tool registers", name)
		}
		if seen[name] {
			t.Errorf("strictReadTools lists %q twice", name)
		}
		seen[name] = true
		if slices.Contains(roomToolNames(), name) {
			t.Errorf("%q is a room tool; room tools are strict already", name)
		}
	}
}

// Far more read tools than slots, none of them in strictReadTools: the cap
// still holds and the leftover slots fall back to registration order, so the
// tool list is byte-identical across requests and no tool is dropped.
func TestStrictToolCapWithManyReadTools(t *testing.T) {
	const n = 30
	reg := tools.NewRegistry()
	for i := 0; i < n; i++ {
		err := reg.Register(&recordingTool{name: fmt.Sprintf("read_%02d", i), schema: map[string]any{
			"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	room := newRoomTools(Deps{Actions: actions.NewRegistry()}, nil)
	order, strict := strictNames(t, buildToolList(reg, room))
	if len(order) != n+len(room) {
		t.Fatalf("sent %d tools, want %d: no tool may be dropped to meet the cap", len(order), n+len(room))
	}
	if len(strict) != maxStrictTools {
		t.Fatalf("%d strict tools, want %d", len(strict), maxStrictTools)
	}
	for _, rt := range room {
		if !strict[rt.Name()] {
			t.Errorf("room tool %s must stay strict", rt.Name())
		}
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("read_%02d", i)
		if want := i < maxStrictTools-len(room); strict[name] != want {
			t.Errorf("%s strict = %v, want %v", name, strict[name], want)
		}
	}
}

// A most-used tool registered last still beats the registration order.
func TestStrictReadToolsBeatRegistrationOrder(t *testing.T) {
	reg := tools.NewRegistry()
	for i := 0; i < 10; i++ {
		if err := reg.Register(&recordingTool{name: fmt.Sprintf("read_%02d", i), schema: map[string]any{"type": "object"}}); err != nil {
			t.Fatal(err)
		}
	}
	last := strictReadTools()[len(strictReadTools())-1]
	if err := reg.Register(&recordingTool{name: last, schema: map[string]any{"type": "object"}}); err != nil {
		t.Fatal(err)
	}
	_, strict := strictNames(t, buildToolList(reg, newRoomTools(Deps{Actions: actions.NewRegistry()}, nil)))
	if !strict[last] {
		t.Errorf("%s is in strictReadTools but lost its slot to registration order", last)
	}
}

// Room tools alone (no registry) are all strict: the cap never removes strict
// where there is room for it.
func TestStrictToolCapRoomOnly(t *testing.T) {
	room := newRoomTools(Deps{Actions: actions.NewRegistry()}, nil)
	order, strict := strictNames(t, buildToolList(nil, room))
	if len(order) != len(room) || len(strict) != len(room) {
		t.Fatalf("sent %d tools, %d strict; want %d and %d", len(order), len(strict), len(room), len(room))
	}
}
