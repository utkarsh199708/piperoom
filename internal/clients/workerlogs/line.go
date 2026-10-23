package workerlogs

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// levelRE reads the level of a line as Connect and Debezium write it:
// "2026-10-23 10:00:00,123 ERROR ..." or "[2026-10-23 10:00:00,123] ERROR ...".
// A Promtail pipeline keeps whole stack traces joined to that first line, so
// this prefix is enough to label the entry.
var levelRE = regexp.MustCompile(`^\[?\d{4}-\d{2}-\d{2}[ T][0-9:.,]+\]?\s+(TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\b`)

// workerRE finds Kafka Connect's worker thread name ("[Worker-1]").
var workerRE = regexp.MustCompile(`\[(Worker-[^\]\s]{1,64})\]`)

// loggerRE finds a Java class name (at least three dot-separated parts), which
// is where Connect and Debezium put the logger.
var loggerRE = regexp.MustCompile(`\b([a-z][a-zA-Z0-9_]*(?:\.[a-zA-Z][a-zA-Z0-9_]*){2,})\b`)

func detectLevel(text string) string {
	if m := levelRE.FindStringSubmatch(text); m != nil {
		return m[1]
	}
	return ""
}

// contextTokens returns the worker thread and the logger as far as the line
// names them; both are optional and untrusted, so each is cut short.
func contextTokens(text string) (worker, logger string) {
	if m := workerRE.FindStringSubmatch(text); m != nil {
		worker = oneLine(m[1], 64)
	}
	if m := loggerRE.FindStringSubmatch(text); m != nil {
		logger = oneLine(m[1], 96)
	}
	return worker, logger
}

// cutText bounds one entry's text. The marker says the line is longer, so the
// model does not read a cut as the end of the message.
func cutText(s string) string {
	if len(s) <= MaxLineBytes {
		return s
	}
	n := MaxLineBytes
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…[line cut]"
}

// oneLine folds whitespace and control characters to single spaces and cuts s
// to n runes, so a value from Loki or the model cannot start a new line.
func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}
