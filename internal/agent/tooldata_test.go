package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// quotedOfLen returns a JSON string literal whose encoded size is exactly n.
func quotedOfLen(n int) json.RawMessage {
	return json.RawMessage(`"` + strings.Repeat("a", n-2) + `"`)
}

// newCapRunner builds a runner whose only live parts are the logger and cap.
func newCapRunner(maxBytes int64) *runner {
	return &runner{
		log:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		opts: Options{MaxToolDataBytes: maxBytes},
	}
}

func TestToolDataCap(t *testing.T) {
	const def = 64 << 10
	tests := []struct {
		name     string
		maxBytes int64
		size     int
		wantCut  bool
		limit    int
	}{
		{name: "default under cap", maxBytes: 0, size: 100, limit: def},
		{name: "default exactly at cap", maxBytes: 0, size: def, limit: def},
		{name: "default one over", maxBytes: 0, size: def + 1, wantCut: true, limit: def},
		{name: "negative means default", maxBytes: -5, size: def + 1, wantCut: true, limit: def},
		{name: "smaller cap honoured", maxBytes: 1024, size: 2000, wantCut: true, limit: 1024},
		{name: "smaller cap not hit", maxBytes: 1024, size: 500, limit: 1024},
		{name: "larger cap honoured", maxBytes: 200 << 10, size: def * 2, limit: 200 << 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newCapRunner(tt.maxBytes)
			// A Go string of n-2 bytes encodes to exactly n JSON bytes.
			out, cut := r.toolDataJSON("t", strings.Repeat("a", tt.size-2))
			if cut != tt.wantCut {
				t.Fatalf("cut = %v, want %v", cut, tt.wantCut)
			}
			if len(out) > tt.limit || !json.Valid(out) {
				t.Fatalf("len=%d valid=%v, limit %d", len(out), json.Valid(out), tt.limit)
			}
			if !tt.wantCut {
				return
			}
			var td events.TruncatedData
			if err := json.Unmarshal(out, &td); err != nil {
				t.Fatalf("not a TruncatedData object: %v", err)
			}
			if !td.Truncated || td.OriginalBytes != int64(tt.size) || td.Preview == "" {
				t.Fatalf("marker = truncated %v original %d preview %d bytes, want original %d",
					td.Truncated, td.OriginalBytes, len(td.Preview), tt.size)
			}
		})
	}
}

// Zero must mean the 64 KiB default, never "no cap" (events.TruncateData's
// meaning of max <= 0).
func TestMaxToolDataZeroIsDefault(t *testing.T) {
	if got := newCapRunner(0).maxToolData(); got != 64<<10 {
		t.Fatalf("maxToolData() = %d, want %d", got, 64<<10)
	}
	if got := newCapRunner(4096).maxToolData(); got != 4096 {
		t.Fatalf("maxToolData() = %d, want 4096", got)
	}
}

func TestToolDataMultiByteCut(t *testing.T) {
	// "é" is two bytes, so a byte cut can land inside a rune.
	r := newCapRunner(1001)
	out, cut := r.toolDataJSON("t", strings.Repeat("é", 5000))
	if !cut {
		t.Fatal("want a cut")
	}
	var td events.TruncatedData
	if err := json.Unmarshal(out, &td); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !utf8.ValidString(td.Preview) || strings.ContainsRune(td.Preview, utf8.RuneError) {
		t.Fatalf("preview splits a rune")
	}
}

// A tool whose Data has a panicking MarshalJSON must not take the run down: the
// panic is masked and becomes a tool error, and the run goes on.
func TestToolDataPanicBecomesToolError(t *testing.T) {
	for _, path := range []string{"run", "verify"} {
		t.Run(path, func(t *testing.T) {
			rec := &recordingTool{name: "connect_status", schema: testSchema("connector"),
				result: tools.Result{Summary: "ok", Data: panicMarshaler{}}}
			sink := &fakeSink{}
			var model *scriptedModel
			if path == "run" {
				model = &scriptedModel{turns: []anthropic.BetaMessage{
					mustMessage(t, `{"stop_reason":"tool_use","content":[
						{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"read","connector":"pg"}}
					]}`),
				}}
			} else {
				model = &scriptedModel{}
			}
			r := newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{MaxTurns: 3})
			if path == "run" {
				if err := r.run(context.Background()); err != nil {
					t.Fatalf("run: %v (a panicking MarshalJSON must not end the run)", err)
				}
			} else {
				r.toolList = buildToolList(r.deps.Tools, nil)
				_, err := r.runStep(context.Background(), "verify", "connect_status", json.RawMessage(`{"connector":"pg"}`))
				if err == nil || !strings.Contains(err.Error(), "could not be encoded") {
					t.Fatalf("runStep err = %v, want an encode error", err)
				}
			}
			results := sink.ofType(events.AgentToolResult)
			if len(results) != 1 {
				t.Fatalf("tool_result events = %d, want 1", len(results))
			}
			var p events.AgentToolResultPayload
			if err := json.Unmarshal(results[0].payload, &p); err != nil {
				t.Fatal(err)
			}
			if !p.IsError || p.Data != nil || !strings.Contains(p.Summary, "could not be encoded") {
				t.Fatalf("payload = %+v, want an error result without data", p)
			}
		})
	}
}

// The emitted tool_result carries the cap: Truncated and the TruncatedData
// shape when Data is over it, untouched when it is not. Both read-tool paths
// (run and verify) are covered, since each sets Truncated from the cut.
func TestEmittedToolResultHonoursCap(t *testing.T) {
	tests := []struct {
		name     string
		maxBytes int64
		size     int // encoded JSON bytes of Data
		wantCut  bool
	}{
		{name: "configured 1024 at cap", maxBytes: 1024, size: 1024},
		{name: "configured 1024 over cap", maxBytes: 1024, size: 1025, wantCut: true},
		{name: "default at 65536", maxBytes: 0, size: 65536},
		{name: "default over 65536", maxBytes: 0, size: 65537, wantCut: true},
	}
	for _, tt := range tests {
		for _, path := range []string{"run", "verify"} {
			t.Run(tt.name+"/"+path, func(t *testing.T) {
				data := json.RawMessage(`"` + strings.Repeat("a", tt.size-2) + `"`)
				rec := &recordingTool{name: "connect_status", schema: testSchema("connector"),
					result: tools.Result{Summary: "ok", Data: data}}
				sink := &fakeSink{}
				model := &scriptedModel{}
				if path == "run" {
					model = &scriptedModel{turns: []anthropic.BetaMessage{
						mustMessage(t, `{"stop_reason":"tool_use","content":[
							{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"read","connector":"pg"}}
						]}`),
					}}
				}
				r := newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, rec,
					Options{MaxTurns: 3, MaxToolDataBytes: tt.maxBytes})
				if path == "run" {
					if err := r.run(context.Background()); err != nil {
						t.Fatalf("run: %v", err)
					}
				} else {
					r.toolList = buildToolList(r.deps.Tools, nil)
					if _, err := r.runStep(context.Background(), "verify", "connect_status", json.RawMessage(`{"connector":"pg"}`)); err != nil {
						t.Fatalf("runStep: %v", err)
					}
				}
				results := sink.ofType(events.AgentToolResult)
				if len(results) != 1 {
					t.Fatalf("tool_result events = %d, want 1", len(results))
				}
				var p events.AgentToolResultPayload
				if err := json.Unmarshal(results[0].payload, &p); err != nil {
					t.Fatal(err)
				}
				if p.Truncated != tt.wantCut {
					t.Fatalf("Truncated = %v, want %v", p.Truncated, tt.wantCut)
				}
				limit := int(tt.maxBytes)
				if limit == 0 {
					limit = 64 << 10
				}
				if len(p.Data) > limit {
					t.Fatalf("emitted Data is %d bytes, cap %d", len(p.Data), limit)
				}
				if !tt.wantCut {
					if !bytes.Equal(p.Data, data) {
						t.Fatalf("data under the cap changed (%d bytes)", len(p.Data))
					}
					return
				}
				var td events.TruncatedData
				if err := json.Unmarshal(p.Data, &td); err != nil || !td.Truncated || td.OriginalBytes != int64(tt.size) || td.Preview == "" {
					t.Fatalf("data = %.80s..., want TruncatedData with original_bytes %d (err %v)", p.Data, tt.size, err)
				}
			})
		}
	}
}

func TestEncodeToolDataErrors(t *testing.T) {
	tests := []struct {
		name  string
		v     any
		check func(error) bool // reports whether err wraps the expected cause
	}{
		{name: "channel", v: make(chan int), check: func(err error) bool {
			var e *json.UnsupportedTypeError
			return errors.As(err, &e)
		}},
		{name: "NaN", v: math.NaN(), check: func(err error) bool {
			var e *json.UnsupportedValueError
			return errors.As(err, &e)
		}},
		{name: "panicking marshaler", v: panicMarshaler{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := encodeToolData(tt.v)
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.HasPrefix(err.Error(), "agent: encode tool data") {
				t.Fatalf("error not wrapped with the package prefix: %v", err)
			}
			if tt.check != nil && !tt.check(err) {
				t.Fatalf("error %v does not wrap the encoder's error", err)
			}
		})
	}
}

// panicMarshaler panics while encoding, the way a buggy tool type could.
type panicMarshaler struct{}

func (panicMarshaler) MarshalJSON() ([]byte, error) { panic("boom") }

func TestToolDataJSON(t *testing.T) {
	var logs bytes.Buffer
	r := &runner{log: slog.New(slog.NewJSONHandler(&logs, nil))}

	if out, cut := r.toolDataJSON("t", nil); out != nil || cut {
		t.Fatalf("nil data = %s cut=%v, want nil and no cut", out, cut)
	}
	if out, cut := r.toolDataJSON("t", map[string]int{"a": 1}); string(out) != `{"a":1}` || cut {
		t.Fatalf("small data = %s cut=%v", out, cut)
	}
	if out, cut := r.toolDataJSON("t", make(chan int)); out != nil || cut {
		t.Fatalf("unencodable data = %s cut=%v, want dropped", out, cut)
	}
	if !strings.Contains(logs.String(), "tool data marshal failed") {
		t.Fatalf("unencodable data was not logged: %s", logs.String())
	}
}
