package agent

import (
	"encoding/json"
	"fmt"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
)

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

// The live run sends every registered read tool plus the room tools, and the
// API rejects a request with more than maxStrictTools strict tools. Build the
// real full registry (registration needs no live clients) and check the cap.
func TestFullRegistryStaysWithinStrictToolCap(t *testing.T) {
	reg := tools.NewRegistry()
	if err := dbtools.Register(reg, dbtools.Deps{Owners: &owners.Owners{}}); err != nil {
		t.Fatal(err)
	}
	if err := kafkatools.Register(reg, kafkatools.Deps{}); err != nil {
		t.Fatal(err)
	}
	room := newRoomTools(Deps{Actions: actions.NewRegistry()})
	tl := buildToolList(reg, room)

	order, strict := strictNames(t, tl)
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
	// Strict read tools are a prefix of the registration order.
	for i, rt := range reg.All() {
		if want := i < maxStrictTools-len(room); strict[rt.Name()] != want {
			t.Errorf("read tool #%d %s strict = %v, want %v", i, rt.Name(), strict[rt.Name()], want)
		}
	}
}

// Far more read tools than slots: the cap still holds and the rule stays
// deterministic (room tools first, then read tools in registration order), so
// the tool list is byte-identical across requests.
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
	room := newRoomTools(Deps{Actions: actions.NewRegistry()})
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

// The cap is set by the compiled-grammar size limit, not by the count limit:
// a live run with 20 strict tools was rejected with "The compiled grammar is
// too large", so the cap must stay small until a live run proves a higher one.
// This pins the number so a later edit cannot silently raise it back.
func TestStrictToolCapStaysBelowGrammarLimit(t *testing.T) {
	const liveCap = 8
	if maxStrictTools > liveCap {
		t.Fatalf("maxStrictTools = %d; the cap chosen from the live-test diagnosis is %d (compiled grammar size, not the 20-tool count limit); raise it only after a live run passes", maxStrictTools, liveCap)
	}
	room := newRoomTools(Deps{Actions: actions.NewRegistry()})
	if maxStrictTools < len(room) {
		t.Fatalf("maxStrictTools = %d is smaller than the %d room tools; propose_fix must always be strict", maxStrictTools, len(room))
	}
}

// Room tools alone (no registry) are all strict: the cap never removes strict
// where there is room for it.
func TestStrictToolCapRoomOnly(t *testing.T) {
	room := newRoomTools(Deps{Actions: actions.NewRegistry()})
	order, strict := strictNames(t, buildToolList(nil, room))
	if len(order) != len(room) || len(strict) != len(room) {
		t.Fatalf("sent %d tools, %d strict; want %d and %d", len(order), len(strict), len(room), len(room))
	}
}
