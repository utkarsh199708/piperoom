package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestSanitizeTerminal(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "esc clear screen",
			in:   "\x1b[2J",
			want: "[2J",
		},
		{
			name: "c1 csi",
			in:   "\u009b31m",
			want: "31m",
		},
		{
			name: "del",
			in:   "\x7f",
			want: "",
		},
		{
			name: "osc title",
			in:   "\x1b]0;title\x07",
			want: "]0;title",
		},
		{
			name: "newlines and tabs kept",
			in:   "line1\n\tline2\n",
			want: "line1\n\tline2\n",
		},
		{
			name: "normal unicode kept",
			in:   "héllo — 東京 ✓",
			want: "héllo — 東京 ✓",
		},
		{
			name: "other c0 controls stripped",
			in:   "a\r\nb\x00c\x08d",
			want: "a\nbcd",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeTerminal(tt.in); got != tt.want {
				t.Errorf("sanitizeTerminal(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestTruncatePrinted(t *testing.T) {
	short := strings.Repeat("a", 100)
	atLimit := strings.Repeat("a", maxPrintedBytes)
	long := strings.Repeat("x", maxPrintedBytes+100)
	marker := printedTruncateMarker

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"short unchanged", short, short},
		{"at limit unchanged", atLimit, atLimit},
		{"long truncated with marker", long, strings.Repeat("x", maxPrintedBytes-len(marker)) + marker},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncatePrinted(tt.in)
			if got != tt.want {
				t.Errorf("truncatePrinted() len=%d, want len=%d", len(got), len(tt.want))
			}
			if len(got) > maxPrintedBytes {
				t.Errorf("truncatePrinted() returned %d bytes, want <= %d", len(got), maxPrintedBytes)
			}
		})
	}
}

func TestEmitSanitizesPayload(t *testing.T) {
	var buf bytes.Buffer
	sink := newTerminalSink(&buf)

	err := sink.Emit(context.Background(), events.AgentMessage, events.AgentMessagePayload{
		Text: "safe\x1b[2J\x7f\u009b31m tail",
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	out := buf.String()

	// The trusted color codes keep their raw ESC, but the untrusted payload must
	// have DEL and C1 stripped and ESC rendered as the JSON escape \u001b (six
	// printable characters, not a terminal command).
	want := "\x1b[35m" + "agent.message       " + "\x1b[0m" + " " +
		`{"text":"safe\u001b[2J31m tail"}` + "\n"
	if out != want {
		t.Errorf("Emit output = %q, want %q", out, want)
	}
	if strings.ContainsRune(out, 0x7f) {
		t.Errorf("output still contains DEL: %q", out)
	}
	if strings.ContainsRune(out, 0x9b) {
		t.Errorf("output still contains C1 U+009B: %q", out)
	}
}

// TestProposalOutputSanitized proves the auto-rejected proposal path strips C1
// controls (F8 round 2: a proposal title must not reach the terminal raw).
func TestProposalOutputSanitized(t *testing.T) {
	r := &rejectProposals{}
	p := events.ProposalCreatedPayload{Title: "fix \u009b31m style", RequiredLayers: []string{"schema-registry"}}

	old := os.Stdout
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = pw
	_, _ = r.Create(context.Background(), p)
	pw.Close()
	os.Stdout = old

	out, _ := io.ReadAll(pr)
	if strings.ContainsRune(string(out), 0x9b) {
		t.Fatalf("proposal output not sanitized (C1 U+009B present): %q", out)
	}
	if !strings.Contains(string(out), "fix ") {
		t.Fatalf("proposal output lost the title: %q", out)
	}
}
