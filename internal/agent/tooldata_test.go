package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
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

// A tool whose MarshalJSON panics must not take the run down: the data is
// dropped and the panic is logged.
func TestToolDataPanicRecovered(t *testing.T) {
	var logs bytes.Buffer
	r := &runner{log: slog.New(slog.NewJSONHandler(&logs, nil))}
	out, cut := r.toolDataJSON("t", panicMarshaler{})
	if out != nil || cut {
		t.Fatalf("out=%s cut=%v, want dropped", out, cut)
	}
	if !strings.Contains(logs.String(), "tool data marshal failed") {
		t.Fatalf("panic was not logged: %s", logs.String())
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
}
