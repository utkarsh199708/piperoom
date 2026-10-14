package textnorm

import (
	"strings"
	"testing"
	"time"
)

// worstInputs are 64 KiB inputs built to make each masking step do as much
// work as it can: long lines of one repeated unit, one unit per pattern.
func worstInputs() map[string]string {
	rep := func(prefix, unit string, n int) string {
		return prefix + strings.Repeat(unit, n)
	}
	fill := func(line string) string {
		var b strings.Builder
		for b.Len() < MaxInputBytes {
			b.WriteString(line)
			b.WriteByte('\n')
		}
		return b.String()[:MaxInputBytes]
	}
	m := map[string]string{
		"logfmt-escaped-quotes": func() string {
			var b strings.Builder
			for i := 0; i < 32; i++ {
				b.WriteString(rep(`msg="`, `\"`, 1020))
				b.WriteByte('\n')
			}
			return b.String()
		}(),
		"quote-mix":       fill(rep("", `" '`, 1023)),
		"dotted":          fill(rep("", "a.", 1023)),
		"slashes":         fill(rep("", "ab/", 682)),
		"at-no-paren":     fill("at " + strings.Repeat("a.", 1000)),
		"ip-port":         fill(rep("", "1.1.1.1:", 255)),
		"consumer":        fill(rep("", "consumer-", 227)),
		"digit-dots":      fill(rep("", "1.", 1023)),
		"letters":         fill(rep("", "a", 2047)),
		"iso-dates":       fill(rep("", "2026-10-11T09:37:00Z ", 186)),
		"empty-lines":     strings.Repeat("\n", MaxInputBytes),
		"many-causes":     strings.Repeat("Caused by: a.b.CException: x 1\n\tat a.B.c(B.java:1)\n", 50000),
		"many-frames":     "a.b.CException: x\n" + strings.Repeat("\tat a.B.c(B.java:1)\n", 50000),
		"emails-and-keys": fill(strings.Repeat("Key (email)=(bob@example.com) ", 60)),
	}
	for k, v := range m {
		if len(v) > MaxInputBytes*2 {
			m[k] = v[:MaxInputBytes*2]
		}
	}
	return m
}

func BenchmarkNormalizeWorst(b *testing.B) {
	for name, in := range worstInputs() {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(in)))
			for i := 0; i < b.N; i++ {
				_ = Fingerprint(in)
			}
		})
	}
}

// TestWorstCaseTime keeps every worst input well under the budget. The budget
// is generous against the up to ~4 ms measured so a loaded CI machine
// does not flake, and still far below the 50-375 ms of the unbounded version.
func TestWorstCaseTime(t *testing.T) {
	if raceEnabled {
		t.Skip("timing is not meaningful under the race detector")
	}
	const budget = 25 * time.Millisecond
	for name, in := range worstInputs() {
		best := time.Hour
		for i := 0; i < 5; i++ {
			start := time.Now()
			_ = Fingerprint(in)
			if d := time.Since(start); d < best {
				best = d
			}
		}
		if best > budget {
			t.Errorf("%s: %v > %v", name, best, budget)
		}
	}
}
