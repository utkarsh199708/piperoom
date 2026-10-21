package kafkatools

import (
	"go/ast"
	"go/parser"
	"go/token"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/dlq"
	"piperoom/internal/redact"
)

// traceMatchTokens build random lines around the regexps' anchors, with the
// two runes Go's (?i) folds to ASCII letters (U+212A, U+017F), bytes \b and
// \w treat differently, and newlines that end a .* span.
var traceMatchTokens = []string{
	"key", "Key", "KEY", "Key", "value", "Value", "values", "VALUES", "\"", "\\\"", "(", ")", "=", " ", "  ", "\t", "\n", "\r", "\f",
	"is out of range", "IS OUT OF RANGE", "set", "SET", "ſet", "Set", "incorrect ", "Incorrect ", "integer", "a", "_", "0", "9",
	"batch entry ", "Batch Entry ", "detail:", "Detail:", "where:", "record", "Record", "{", "}", "struct{", "Struct{", "SinkRecord{",
	"ConnectRecord{", "SourceRecord{", "[Source:", "[ſource:", "unrecognized token", "invalid input syntax", "For input string",
	"failing row contains", "cannot parse", "Duplicate entry", "invalid input value", "Preview of field", "duplicate key value is",
	"x", "é", "\xff", "password", "PWD", "ſecret", "jdbc:oracle:", "JDBC:ORACLE:", "'", "-", ":", "[",
}

func randomTraceLine(r *rand.Rand) string {
	var b strings.Builder
	for n := r.IntN(20); n >= 0; n-- {
		b.WriteString(traceMatchTokens[r.IntN(len(traceMatchTokens))])
	}
	return b.String()
}

// TestMatchesRecordSnippetIsTheRegexp: #533. The literal matcher gives the
// regexp's answer on every line: hand-picked rows, then random lines.
func TestMatchesRecordSnippetIsTheRegexp(t *testing.T) {
	rows := []string{
		"Key (id)=(1) already exists", "key (a\n)=(1)", "key (a)\n=\n(1)", "key\n(a)=(", "akey (a)=(", "aKey (a)=(", "Key (a)=(",
		"key (a) = (", "key ( )=(", "key ()=(", "key (=(", "key (a)) = ((", "value \"x\" is out of range", "value\"x\" is out of range",
		"value \"x\"is out of range", "value \"x\n\" is out of range", "value \"\" is out of range", "value \"x\"\nis out of range",
		"VALUES(1)", "values (", "values", "xvalues (", "set \"col\" = 1", "set col=1", "set \"\"=1", "set =", "reset col = 1",
		"ſet col = 1", "aſet col = 1", "set cſol = 1", "incorrect integer value", "incorrect  value", "incorrect ſ value",
		"Batch Entry 0", "batch entry x", "struct{", "xstruct{", "record {", "record\n{", "detail:", "xdetail:", "where:",
		"[ſource:", "Duplicate entry 'a'",
	}
	check := func(s string) {
		t.Helper()
		if got, want := matchesRecordSnippet(s), recordSnippet.MatchString(s); got != want {
			t.Fatalf("matchesRecordSnippet(%q) = %v, regexp says %v", s, got, want)
		}
		if got, want := maskBracedPasswords(s), bracedPassword.ReplaceAllString(s, "${1}"+redactedPassword); got != want {
			t.Fatalf("maskBracedPasswords(%q) = %q, regexp gives %q", s, got, want)
		}
		if got, want := maskOracleThinCreds(s), oracleThinCreds.ReplaceAllStringFunc(s, maskOracleThin); got != want {
			t.Fatalf("maskOracleThinCreds(%q) = %q, regexp gives %q", s, got, want)
		}
	}
	for _, s := range rows {
		check(s)
	}
	r := rand.New(rand.NewPCG(533, 1))
	n := 100000
	if raceEnabled {
		n = 5000 // the race detector makes the regexp side ~10x slower
	}
	for range n {
		check(randomTraceLine(r))
	}
}

// TestSanitizeTraceHostileSpeed: #533. On a long line of any repeated unit,
// sanitizeTrace and condenseTrace stay linear: main's regexps took 28-44 ms
// on 64 KiB, growing with the square of the length. An absolute budget on
// shared CI runners flaked (#698: 12.9 ms against 10 ms), so the test checks
// how time scales instead: the same unit on 4x the bytes must take under 8x
// the time. Linear work gives about 4x; quadratic work gives about 16x, so 8x
// leaves room for noise on either side without letting a blowup through.
// Each time is the best of several runs of the thread's work (workTime),
// since CI runs packages in parallel. A generous absolute bound on top, over
// what redact.Text and dlq.RedactMessage (main's passes from other packages)
// take on the same line, still catches a linear pass that got much slower.
func TestSanitizeTraceHostileSpeed(t *testing.T) {
	const (
		small    = 64 << 10 // the line of #533
		large    = 4 * small
		maxRatio = 8.0
		// Below this the large run is too short for its ratio to mean
		// anything (timer and scheduler noise), and too short to matter.
		noiseFloor = time.Millisecond
	)
	budget := 100 * time.Millisecond
	if raceEnabled {
		budget = 1500 * time.Millisecond
	}
	for _, unit := range []string{
		"x", "a b c d ", "key (", "Key (a)=(", "value \"", "value \"x\" ", "set ", "set x ", "incorrect x ", "values ",
		"detail: ", "record ", "\u212aey (", "ſet x ", "password={", "Password={x", "jdbc:oracle:thin:a/", "jdbc:oracle:thin:a/b@",
		"oldSchema: '", "Error registering Avro schema{", "{\"a\":", "Text '", "Text 'x' could not be parsed ", "Text '\n",
		"malformed array literal: \"", "failed to parse date field [", "postgresql://u:p@h/", "Failing row contains (",
		"Not in union [", "Not in union [\"a\"]: ", "in document with id '", "\"reason\":\"in document with id '",
		"invalid byte sequence for encoding \"", "Got VALUE_", "invalid value \"",
	} {
		hostile := func(size int) string { return "Caused by: x: " + strings.Repeat(unit, size/len(unit)) + "Leak" }
		short, line := hostile(small), hostile(large)
		other := bestWorkTime(func() { redact.Text(line) }) + bestWorkTime(func() { dlq.RedactMessage(line) })
		for name, f := range map[string]func(string) string{"sanitizeTrace": sanitizeTrace, "condenseTrace": condenseTrace} {
			tSmall := bestWorkTime(func() { f(short) })
			tLarge := bestWorkTime(func() { f(line) })
			if tLarge > budget+other {
				t.Errorf("%s: %q x %d KiB took %v, want < %v (+ %v in redact.Text and dlq.RedactMessage)",
					name, unit, large>>10, tLarge, budget, other)
			}
			if tLarge < noiseFloor {
				continue
			}
			if ratio := float64(tLarge) / float64(max(tSmall, time.Microsecond)); ratio > maxRatio {
				t.Errorf("%s: %q x %d KiB took %v, x %d KiB took %v: %.1fx for 4x the bytes, want < %.0fx (linear)",
					name, unit, large>>10, tLarge, small>>10, tSmall, ratio, maxRatio)
			}
		}
	}
}

// bestWorkTime is the best of five workTime runs, so one GC pause is not a
// failure.
func bestWorkTime(f func()) time.Duration {
	best := time.Hour
	for range 5 {
		runtime.GC()
		best = min(best, workTime(f))
	}
	return best
}

// regexpSanitizeTrace is sanitizeTrace with main's regexps in place of the
// literal matchers (#533): the reference the fast path must equal.
func regexpSanitizeTrace(s string) string {
	s = stripSchemaAfter(s, "Error registering Avro schema")
	s = stripOldSchema(s)
	s = regexpMaskCreds(s)
	if recordSnippet.MatchString(s) {
		s = dlq.RedactMessage(failingRow.ReplaceAllString(s, "${1} "+dlq.Redacted))
	}
	return maskSinkValues(s)
}

func regexpMaskCreds(s string) string {
	s = bracedPassword.ReplaceAllString(s, "${1}"+redactedPassword)
	s, _ = redact.Text(s)
	return oracleThinCreds.ReplaceAllStringFunc(s, maskOracleThin)
}

// corpusStrings returns every string literal in this package's tests: the
// trace corpus rows and the rest.
func corpusStrings(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	fset := token.NewFileSet()
	for _, name := range files {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					out = append(out, s)
				}
			}
			return true
		})
	}
	return out
}

// TestSanitizeTraceSameAsRegexps: #533 changes speed only. On every corpus
// string, as a line, behind "Caused by: ", and as a whole trace, and on
// random lines, the output is byte for byte what main's regexps give.
func TestSanitizeTraceSameAsRegexps(t *testing.T) {
	lines := corpusStrings(t)
	if len(lines) < 500 {
		t.Fatalf("found %d corpus strings, want the package's test literals", len(lines))
	}
	r := rand.New(rand.NewPCG(533, 2))
	n := 3000
	if raceEnabled {
		n = 300
	}
	for range n {
		lines = append(lines, randomTraceLine(r))
	}
	for _, s := range lines {
		for _, in := range []string{s, "Caused by: " + s} {
			if got, want := sanitizeTrace(in), regexpSanitizeTrace(in); got != want {
				t.Fatalf("sanitizeTrace(%q)\n got %q\nwant %q", in, got, want)
			}
		}
		if got, want := maskCreds(s), regexpMaskCreds(s); got != want {
			t.Fatalf("maskCreds(%q)\n got %q\nwant %q", s, got, want)
		}
	}
}
