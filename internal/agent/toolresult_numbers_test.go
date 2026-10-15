package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/jsonnum"
	"piperoom/internal/tools"
)

// jsonbSink behaves like the store under the room: it refuses an event whose
// payload holds a number outside the double range, the way Postgres jsonb
// refuses 1e131072 (SQLSTATE 22003), and keeps the rest.
type jsonbSink struct{ fakeSink }

func (s *jsonbSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	b, _ := json.Marshal(payload)
	if _, n := jsonnum.Quote(b); n > 0 {
		return fmt.Errorf("store: insert event: value overflows numeric format (22003)")
	}
	return s.fakeSink.Emit(ctx, typ, payload)
}

// A tool whose data holds numbers jsonb cannot take must not lose its
// tool_result event: the numbers are kept as strings with their exact text.
func TestToolResultDataKeepsEventWithOutOfRangeNumbers(t *testing.T) {
	data := json.RawMessage(`{"n":1e131072,"rows":[1,-2.5,1e-16384,3e400],"label":"1e400 stays text","ok":2.5e10}`)
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "values read", Data: data}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"read it","connector":"pg"}}
		]}`),
	}}
	sink := &jsonbSink{}
	r := newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{MaxTurns: 3})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	results := sink.ofType(events.AgentToolResult)
	if len(results) != 1 {
		t.Fatalf("tool_result events = %d, want 1 (the event must not be lost)", len(results))
	}
	var p events.AgentToolResultPayload
	if err := json.Unmarshal(results[0].payload, &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := `{"n":"1e131072","rows":[1,-2.5,"1e-16384","3e400"],"label":"1e400 stays text","ok":2.5e10}`
	if string(p.Data) != want {
		t.Fatalf("data = %s\nwant   %s", p.Data, want)
	}
	if p.Summary != "values read" || p.IsError {
		t.Fatalf("payload = %+v", p)
	}
}

// Data with only ordinary numbers is emitted untouched, and nil data stays nil.
func TestToolDataJSONLeavesOrdinaryDataAlone(t *testing.T) {
	r := newTestRunner(t, &scriptedModel{}, &fakeInbox{}, &fakeSink{}, &recordingTool{name: "t", schema: testSchema()}, Options{})
	in := map[string]any{"a": 1, "b": []float64{0, -1.5, 1e300, 5e-324}, "c": "x"}
	want, _ := json.Marshal(in)
	if got, cut := r.toolDataJSON("t", in); string(got) != string(want) || cut {
		t.Fatalf("data = %s cut=%v, want %s uncut", got, cut, want)
	}
	if got, cut := r.toolDataJSON("t", nil); got != nil || cut {
		t.Fatalf("nil data = %s cut=%v, want nil", got, cut)
	}
	if got, cut := r.toolDataJSON("t", make(chan int)); got != nil || cut {
		t.Fatalf("unmarshalable data = %s cut=%v, want nil", got, cut)
	}
}
