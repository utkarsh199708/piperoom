package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"piperoom/internal/discover"
)

const (
	// maxCellItems keeps one pipeline with forty tables on one table row.
	maxCellItems = 3
	// sinkMissing is shown instead of a guessed name.
	sinkMissing = "MISSING"
)

// printTable writes the result of a -write-owners run: one row per pipeline
// with its connectors, topics, source tables, sink tables and the files
// written. A sink table is "found" only when a sink connector names it
// literally; otherwise the cell says MISSING. Text from the cluster goes
// through the redactor, which also removes the tabs and newlines that would
// break the columns. notes are extra lines printed under the table.
func printTable(w io.Writer, res discover.Result, files []string, red redactor, notes ...string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PIPELINE\tCONNECTORS\tTOPICS\tSOURCE TABLES\tSINK TABLES\tFILES")
	for _, p := range res.Pipelines {
		sinks := sinkMissing
		if len(p.SinkTables) > 0 {
			// Found, but not written into sink_tables: the timestamp column
			// is not discoverable, so the file asks for both (see RenderOwned).
			sinks = cell(red, p.SinkTables) + " (TODO)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			cell(red, []string{p.ID}), cell(red, []string{p.Connector}), cell(red, p.Topics),
			cell(red, p.SourceTables), sinks, red.line(strings.Join(files, ", ")))
	}
	_ = tw.Flush()
	if len(res.Pipelines) > 0 {
		// Every pipeline in the file has "sink_tables: []", found or not.
		fmt.Fprintln(w, "TODO: fill in sink_tables (schema.table) and sink_ts_column in the owners file to turn on warehouse checks; MISSING means no sink connector config names the table.")
	}
	for _, n := range notes {
		fmt.Fprintln(w, "NOTE: "+n)
	}
}

// cell joins up to maxCellItems names, then counts the rest. An empty list is
// "-".
func cell(red redactor, items []string) string {
	if len(items) == 0 {
		return "-"
	}
	shown := items
	if len(shown) > maxCellItems {
		shown = shown[:maxCellItems]
	}
	out := red.line(strings.Join(shown, ", "))
	if n := len(items) - len(shown); n > 0 {
		out += fmt.Sprintf(" +%d more", n)
	}
	return out
}
