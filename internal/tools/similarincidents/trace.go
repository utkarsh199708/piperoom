package similarincidents

import (
	"regexp"
	"strings"

	"piperoom/internal/textnorm"
)

var (
	// reClassLine is a Java exception line: an optional "Caused by:" and a
	// dotted class whose last part names an error.
	reClassLine = regexp.MustCompile(`^(?:Caused by:\s*)?(?:[A-Za-z_$][\w$]*\.)+[A-Za-z_$][\w$]*(?:Exception|Error|Failure|Throwable)\b`)
	reFrame     = regexp.MustCompile(`^at\s+[\w$.<>]+\(`)
	reMore      = regexp.MustCompile(`^\.\.\.\s*\d+\s+(?:more|common)\b`)
	// rePGLine is a Postgres server error line ("ERROR:  relation ... does not exist").
	rePGLine = regexp.MustCompile(`^(?:ERROR|FATAL|PANIC):\s+\S`)
)

const (
	// maxTraceInput bounds the text the line scan reads.
	maxTraceInput = 16 << 10
	// maxTraceBytes bounds one extracted trace block.
	maxTraceBytes = 4 << 10
)

// extractTrace returns the error-trace block of text: from its first
// exception, frame or server-error line to its last, or "" when there is none.
// A tool summary mixes a status line, the trace and notes ("Task 0: FAILED:",
// "Rejected schema vs ..."), and textnorm would count each of those as a cause
// and so give the same failure different fingerprints. Cutting to the block
// leaves what textnorm was built for.
func extractTrace(text string) string {
	if len(text) > maxTraceInput {
		text = text[:maxTraceInput]
	}
	lines := strings.Split(text, "\n")
	first, last := -1, -1
	for i, l := range lines {
		l = strings.TrimSpace(l)
		if reClassLine.MatchString(l) || reFrame.MatchString(l) || reMore.MatchString(l) || rePGLine.MatchString(l) {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		return ""
	}
	block := strings.Join(lines[first:last+1], "\n")
	if len(block) > maxTraceBytes {
		block = block[:maxTraceBytes]
	}
	return block
}

// fingerprints holds the two ids of one trace. Full covers the whole cause
// chain. Root covers only the root cause (the last cause that names a class,
// else the last non-log cause), so a query that quotes just the final "Caused
// by" line still meets the room that had the whole chain.
type fingerprints struct {
	Full, Root string
}

// analyze returns the ids of the trace block in text and the masked template
// text (record values, hosts and ids replaced by textnorm), which is what gets
// indexed instead of the raw trace. ok is false when text has no trace.
func analyze(text string) (fp fingerprints, masked string, ok bool) {
	block := extractTrace(text)
	if block == "" {
		return fingerprints{}, "", false
	}
	t := textnorm.Normalize(block)
	if len(t.Causes) == 0 {
		return fingerprints{}, "", false
	}
	root := len(t.Causes) - 1
	found := false
	for i := root; i >= 0; i-- {
		if t.Causes[i].Class != "" {
			root, found = i, true
			break
		}
	}
	if !found {
		for i := len(t.Causes) - 1; i >= 0; i-- {
			if !t.Causes[i].Log {
				root = i
				break
			}
		}
	}
	rt := textnorm.Template{Causes: t.Causes[root : root+1]}
	return fingerprints{Full: t.Fingerprint(), Root: rt.Fingerprint()}, t.String(), true
}

// fingerprint is analyze without the masked text.
func fingerprint(text string) (fingerprints, bool) {
	fp, _, ok := analyze(text)
	return fp, ok
}
