package tools

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

func TestRecordWithoutRecorderIsNoop(t *testing.T) {
	Record(context.Background(), Call{Kind: CallSQL, Text: "SELECT 1"})
	if _, ok := RecorderFrom(context.Background()); ok {
		t.Fatal("RecorderFrom found a recorder in a bare context")
	}
	if _, ok := RecorderFrom(WithRecorder(context.Background(), nil)); ok {
		t.Fatal("RecorderFrom reported a nil recorder as present")
	}
}

func TestRecorderKeepsOrderAndCopies(t *testing.T) {
	r := NewRecorder()
	ctx := WithRecorder(context.Background(), r)
	params := []string{"$1=orders"}
	Record(ctx, Call{Kind: CallSQL, Text: "SELECT $1", ParamsRedacted: params})
	Record(ctx, Call{Kind: CallHTTP, Text: "GET /connectors/x/status"})
	params[0] = "changed"

	got := r.Calls()
	if len(got) != 2 || got[0].Kind != CallSQL || got[1].Kind != CallHTTP {
		t.Fatalf("calls = %+v, want sql then http", got)
	}
	if got[0].ParamsRedacted[0] != "$1=orders" {
		t.Fatalf("recorder aliased the caller's params slice: %q", got[0].ParamsRedacted[0])
	}
	got[0].Text = "mutated"
	if r.Calls()[0].Text != "SELECT $1" {
		t.Fatal("Calls did not return a copy")
	}
}

func TestRecorderConcurrent(t *testing.T) {
	r := NewRecorder()
	ctx := WithRecorder(context.Background(), r)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Record(ctx, Call{Kind: CallKafka, Text: "kadm.Lag(g)"})
		}()
	}
	wg.Wait()
	if n := len(r.Calls()); n != 50 {
		t.Fatalf("recorded %d calls, want 50", n)
	}
}

func TestCapProvenance(t *testing.T) {
	long := strings.Repeat("é", MaxCallTextBytes) // 2 bytes per rune
	tests := []struct {
		name     string
		in       []Call
		wantLen  int
		wantCut  bool
		checkLen bool
	}{
		{name: "nil", in: nil, wantLen: 0},
		{name: "within caps", in: []Call{{Text: "SELECT 1", ParamsRedacted: []string{"$1=a"}}}, wantLen: 1},
		{name: "too many calls", in: make([]Call, MaxProvenanceCalls+3), wantLen: MaxProvenanceCalls, wantCut: true},
		{name: "long text", in: []Call{{Text: long}}, wantLen: 1, wantCut: true, checkLen: true},
		{name: "long param", in: []Call{{Text: "x", ParamsRedacted: []string{strings.Repeat("p", MaxCallParamBytes+1)}}}, wantLen: 1, wantCut: true, checkLen: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, cut := CapProvenance(tt.in)
			if len(got) != tt.wantLen || cut != tt.wantCut {
				t.Fatalf("len=%d cut=%v, want len=%d cut=%v", len(got), cut, tt.wantLen, tt.wantCut)
			}
			if !tt.checkLen {
				return
			}
			c := got[0]
			if len(c.Text) > MaxCallTextBytes || !utf8.ValidString(c.Text) {
				t.Fatalf("text len %d valid=%v", len(c.Text), utf8.ValidString(c.Text))
			}
			for _, p := range c.ParamsRedacted {
				if len(p) > MaxCallParamBytes {
					t.Fatalf("param len %d > %d", len(p), MaxCallParamBytes)
				}
			}
		})
	}
	// The input is never modified.
	in := []Call{{Text: long}}
	CapProvenance(in)
	if in[0].Text != long {
		t.Fatal("CapProvenance modified its input")
	}
}

func TestResultProvenanceJSON(t *testing.T) {
	b, err := json.Marshal(Result{Summary: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "provenance") {
		t.Fatalf("empty provenance must be omitted: %s", b)
	}
	b, err = json.Marshal(Call{Kind: CallRegistry, Target: "sr:8081", Text: "GET /subjects"})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{`"kind"`, `"target"`, `"text"`, `"at"`, `"took_ms"`} {
		if !strings.Contains(string(b), k) {
			t.Fatalf("Call JSON %s lacks %s", b, k)
		}
	}
}
