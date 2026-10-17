package kafkatools

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"
)

// mainCondenseTrace is condenseTrace as main had it before #560, the
// reference of TestCondenseTraceNeverUnmasksMain.
func mainCondenseTrace(trace string) string {
	var out []string
	for _, line := range strings.Split(trace, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "at ") {
			continue
		}
		if strings.HasPrefix(trimmed, "Caused by:") || len(out) == 0 {
			out = append(out, sanitizeTrace(trimmed))
		}
	}
	return strings.Join(out, "\n")
}

// TestCondenseTraceNeverUnmasksMain (#560, org.md §12 differential): over
// random traces built from openers, markers, newlines and planted Leak<N>
// words, a word main's condenseTrace removes is removed by condenseTrace too.
func TestCondenseTraceNeverUnmasksMain(t *testing.T) {
	frags := []string{
		"Text '", "Text '", "text '", "Plain text '", "' could not be parsed", "' could not be parsed at index 0",
		"\n", "\n", "\nCaused by: ", "\nat ", "\n\n", "Caused by: ", "x: ", " ", "'", "\"",
		"Numeric value '", "malformed array literal: \"", "Invalid date: '", "Duplicate entry '", "DataException: ",
	}
	leak := regexp.MustCompile(`Leak\d+`)
	rng := rand.New(rand.NewSource(560))
	for n := 0; n < 20000; n++ {
		var b strings.Builder
		for k, c := 0, 2+rng.Intn(14); k < c; k++ {
			if rng.Intn(3) == 0 {
				fmt.Fprintf(&b, "Leak%d", rng.Intn(1000))
			} else {
				b.WriteString(frags[rng.Intn(len(frags))])
			}
		}
		in := b.String()
		oldOut, newOut := mainCondenseTrace(in), condenseTrace(in)
		for _, w := range leak.FindAllString(in, -1) {
			if !strings.Contains(oldOut, w) && strings.Contains(newOut, w) {
				t.Fatalf("main masks %s, condenseTrace shows it:\nin  %q\nold %q\nnew %q", w, in, oldOut, newOut)
			}
		}
	}
}
