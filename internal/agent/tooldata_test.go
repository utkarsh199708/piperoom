package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"piperoom/internal/events"
)

// quotedOfLen returns a JSON string literal whose encoded size is exactly n.
func quotedOfLen(n int) json.RawMessage {
	return json.RawMessage(`"` + strings.Repeat("a", n-2) + `"`)
}

func TestCapToolDataJSON(t *testing.T) {
	tests := []struct {
		name      string
		raw       json.RawMessage
		wantCut   bool
		wantSame  bool
		wantValid bool
	}{
		{name: "under cap", raw: json.RawMessage(`{"a":1}`), wantSame: true},
		{name: "exactly at cap", raw: quotedOfLen(maxToolDataBytes), wantSame: true},
		{name: "one byte over cap", raw: quotedOfLen(maxToolDataBytes + 1), wantCut: true, wantValid: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, cut, err := capToolDataJSON(tt.raw)
			if err != nil {
				t.Fatalf("capToolDataJSON: %v", err)
			}
			if cut != tt.wantCut {
				t.Fatalf("cut = %v, want %v", cut, tt.wantCut)
			}
			if tt.wantSame && !bytes.Equal(out, tt.raw) {
				t.Fatalf("data changed under or at cap:\n got %s\nwant %s", out, tt.raw)
			}
			if len(out) > maxToolDataBytes {
				t.Fatalf("result is %d bytes, cap is %d", len(out), maxToolDataBytes)
			}
			if tt.wantValid && !json.Valid(out) {
				t.Fatalf("truncated result is not valid JSON: %.80s", out)
			}
		})
	}
}

// A cut result is the events.TruncatedData shape the room and the UI read.
func TestCapToolDataJSONShape(t *testing.T) {
	raw := quotedOfLen(maxToolDataBytes * 3)
	out, cut, err := capToolDataJSON(raw)
	if err != nil || !cut {
		t.Fatalf("cut=%v err=%v, want a cut and no error", cut, err)
	}
	var td events.TruncatedData
	if err := json.Unmarshal(out, &td); err != nil {
		t.Fatalf("result is not a TruncatedData object: %v", err)
	}
	if !td.Truncated || td.OriginalBytes != int64(len(raw)) {
		t.Fatalf("marker = %+v, want truncated with original_bytes %d", td, len(raw))
	}
	if !strings.HasPrefix(string(raw), td.Preview) || td.Preview == "" {
		t.Fatalf("preview is not a non-empty prefix of the original JSON")
	}
}

func TestCapToolDataJSONHugeArray(t *testing.T) {
	big := make([]int, 1_000_000)
	for i := range big {
		big[i] = i
	}
	raw, n, err := encodeToolData(big)
	if err != nil || n != 0 {
		t.Fatalf("encodeToolData: n=%d err=%v", n, err)
	}
	out, cut, err := capToolDataJSON(raw)
	if err != nil || !cut {
		t.Fatalf("cut=%v err=%v, want a cut", cut, err)
	}
	if len(out) > maxToolDataBytes {
		t.Fatalf("result is %d bytes, cap is %d", len(out), maxToolDataBytes)
	}
	var td events.TruncatedData
	if err := json.Unmarshal(out, &td); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if td.OriginalBytes != int64(len(raw)) {
		t.Fatalf("original_bytes = %d, want %d", td.OriginalBytes, len(raw))
	}
}

func TestCapToolDataJSONDeepNesting(t *testing.T) {
	// Nested arrays with a padded leaf, deeper than a typical payload, so the
	// encoded form is over the cap.
	var v any = strings.Repeat("x", 100)
	for i := 0; i < 5000; i++ {
		v = []any{v, strings.Repeat("y", 20)}
	}
	raw, _, err := encodeToolData(v)
	if err != nil {
		t.Fatalf("encodeToolData: %v", err)
	}
	out, cut, err := capToolDataJSON(raw)
	if err != nil || !cut {
		t.Fatalf("cut=%v err=%v, want a cut", cut, err)
	}
	if len(out) > maxToolDataBytes || !json.Valid(out) {
		t.Fatalf("result over cap or invalid: len=%d", len(out))
	}
}

func TestCapToolDataJSONMultiByteCut(t *testing.T) {
	// "é" is two bytes, so a byte cut can land inside a rune.
	raw := quotedOfLen(maxToolDataBytes*2 + 1)
	raw = json.RawMessage(strings.ReplaceAll(string(raw), "a", "é"))
	out, cut, err := capToolDataJSON(raw)
	if err != nil || !cut {
		t.Fatalf("cut=%v err=%v", cut, err)
	}
	var td events.TruncatedData
	if err := json.Unmarshal(out, &td); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !utf8.ValidString(td.Preview) {
		t.Fatalf("preview splits a rune")
	}
}

func TestEncodeToolDataErrors(t *testing.T) {
	tests := []struct {
		name    string
		v       any
		wantErr any
	}{
		{name: "channel", v: make(chan int), wantErr: &json.UnsupportedTypeError{}},
		{name: "NaN", v: math.NaN(), wantErr: &json.UnsupportedValueError{}},
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
			if tt.wantErr != nil {
				if !errors.As(err, &tt.wantErr) {
					t.Fatalf("error %v does not wrap %T", err, tt.wantErr)
				}
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
	big := strings.Repeat("z", maxToolDataBytes*2)
	out, cut := r.toolDataJSON("t", big)
	if !cut || len(out) > maxToolDataBytes || !json.Valid(out) {
		t.Fatalf("big data cut=%v len=%d valid=%v", cut, len(out), json.Valid(out))
	}
}
