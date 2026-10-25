package workerlogs

import (
	"regexp"
	"strconv"
	"strings"

	"piperoom/internal/clients/workerlogs/logql"
)

// CheckSelector reports whether sel is a LogQL stream selector and nothing
// else (logql.CheckSelector): `{` one or more `name op "value"` matchers
// `}`. A pipeline stage (`|= "x"`, `| json`) or a metric query is refused:
// the operator chooses which streams are read, the tool adds the filters.
// The check lives in the leaf package logql so internal/owners can apply it
// to a pipeline's consumer_logs.selector without an import cycle.
func CheckSelector(sel string) error { return logql.CheckSelector(sel) }

// buildQuery appends the line filters for q to the operator's selector.
// Every value from the model goes through strconv.Quote, which is the
// inverse of the strconv.Unquote Loki's LogQL lexer applies to string
// literals, so a value can never close its literal and add a stage; the
// regex filters additionally pass the connector name through
// regexp.QuoteMeta so it matches literally.
func buildQuery(selector string, q LogQuery) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(selector))
	if q.Connector != "" {
		if q.Task >= 0 {
			// Kafka Connect names a task "<connector>-<n>" (WorkerSourceTask{id=x-0})
			// and its log context "[<connector>|task-<n>]". \b keeps task 1 from
			// matching task 10.
			n := strconv.Itoa(q.Task)
			re := regexp.QuoteMeta(q.Connector) + `(-` + n + `\b|\|task-` + n + `\])`
			b.WriteString(" |~ ")
			b.WriteString(strconv.Quote(re))
		} else {
			b.WriteString(" |= ")
			b.WriteString(strconv.Quote(q.Connector))
		}
	}
	writeLevelFilter(&b, q.MinLevel)
	return b.String()
}

// buildStreamQuery appends the line filters of a StreamQuery to the
// selector: Contains as one literal |= filter (strconv.Quote, as in
// buildQuery, so the model's text can never close its literal and add a
// stage), then the level filter.
func buildStreamQuery(selector string, q StreamQuery) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(selector))
	if q.Contains != "" {
		b.WriteString(" |= ")
		b.WriteString(strconv.Quote(q.Contains))
	}
	writeLevelFilter(&b, q.MinLevel)
	return b.String()
}

func writeLevelFilter(b *strings.Builder, min string) {
	if lv := levelFilter(min); lv != "" {
		b.WriteString(" |~ ")
		b.WriteString(strconv.Quote(lv))
	}
}

// levelFilter is a coarse server-side regex for lines at min or above, so
// the line limit is not spent on INFO lines; WorkerLogs then drops lines
// whose own level is lower (a WARN word inside an INFO message). Below WARN
// no server-side filter is worth its cost.
func levelFilter(min string) string {
	switch min {
	case "WARN":
		return `\b(WARN|WARNING|ERROR|SEVERE|FATAL)\b`
	case "ERROR":
		return `\b(ERROR|SEVERE|FATAL)\b`
	case "FATAL":
		return `\bFATAL\b`
	}
	return ""
}
