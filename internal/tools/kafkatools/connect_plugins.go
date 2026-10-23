package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/tools"
)

// Caps on what connect_plugins shows. A worker usually has a dozen plugins; a
// hostile or synthetic answer can hold thousands, and each string is untrusted
// Connect text.
const (
	maxPluginSummaryLines = 50
	maxPluginTextBytes    = 256
	maxPluginRows         = 1000
)

// pluginTypeOrder is the order the summary groups plugins in: the four kinds
// the Q1c row names, then header converters. A type outside this list sorts
// after them, alphabetically.
var pluginTypeOrder = []string{"source", "sink", "converter", "transformation", "predicate", "header_converter"}

// ---- connect_plugins ----

type connectPlugins struct{ d Deps }

func (t *connectPlugins) Name() string     { return "connect_plugins" }
func (t *connectPlugins) Kind() tools.Kind { return tools.KindRead }
func (t *connectPlugins) InputSchema() map[string]any {
	return objectSchema(map[string]any{})
}
func (t *connectPlugins) Description() string {
	return "Lists the Kafka Connect worker's plugins with their versions: source and sink connectors, key/value converters, transforms and predicates, plus the Kafka version the worker reports (GET /). " +
		"Use it when an installed plugin version, or a missing converter or transform, may explain a failure. " +
		"On a Connect older than 3.2 it lists connector plugins only, reports no versions and says so."
}

func (t *connectPlugins) Run(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
	// The plugin inventory is worker-wide and holds no customer row, but the
	// tool stays scoped like every other read tool: a room call always has a
	// pipeline, and CLI/dev callers opt out explicitly.
	if _, err := t.d.scope(ctx); err != nil {
		return tools.Result{}, err
	}
	reader, ok := t.d.Connect.(connect.PluginReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("%w: this Connect client cannot list plugins", clients.ErrNotSupported)
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	plugins, err := reader.Plugins(cctx)
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: connect_plugins: %w", err)
	}
	// The Kafka version is a second, independent read: a worker that lists its
	// plugins but fails GET / still answers the plugin question, and the tool
	// says the version is unknown instead of failing whole.
	info, haveInfo := t.serverInfo(cctx)

	rows := pluginRows(plugins)
	counts := map[string]int{}
	for _, r := range rows {
		counts[r.Type]++
	}
	legacy := connect.LegacyPlugins(plugins)

	summary, shown := buildPluginsSummary(info, haveInfo, legacy, rows, counts)
	dataRows, omitted := rows, 0
	if len(rows) > maxPluginRows {
		dataRows, omitted = rows[:maxPluginRows], len(rows)-maxPluginRows
	}
	data := connectPluginsData{
		KafkaVersion:  info.Version,
		ConnectCommit: info.Commit,
		Legacy:        legacy,
		PluginCount:   len(rows),
		Omitted:       omitted,
		ByType:        counts,
		Plugins:       dataRows,
	}
	r := res(summary, data)
	if shown < len(rows) || omitted > 0 {
		r.Truncated = true
	}
	return r, nil
}

// serverInfo reads the worker's Kafka version, commit and cluster id when the
// client has the optional ConnectInfoReader. Every string is untrusted Connect
// output: safeVersion rejects a control character and caps the length before
// it can reach a summary or Data.
func (t *connectPlugins) serverInfo(ctx context.Context) (connect.ServerInfo, bool) {
	reader, ok := t.d.Connect.(connect.ConnectInfoReader)
	if !ok {
		return connect.ServerInfo{}, false
	}
	info, err := reader.ServerInfo(ctx)
	if err != nil {
		return connect.ServerInfo{}, false
	}
	info.Version = safeVersion(info.Version)
	info.Commit = safeVersion(info.Commit)
	info.KafkaClusterID = safeVersion(info.KafkaClusterID)
	return info, true
}

// connectPluginsData is the structured Data of connect_plugins.
type connectPluginsData struct {
	// KafkaVersion is the Kafka release the worker reports on GET /; "" when
	// the read failed or the client has no ConnectInfoReader.
	KafkaVersion  string `json:"kafka_version,omitempty"`
	ConnectCommit string `json:"connect_commit,omitempty"`
	Legacy        bool   `json:"legacy,omitempty"`
	PluginCount   int    `json:"plugin_count"`
	// Omitted counts rows the data cap (maxPluginRows) left out of Plugins;
	// PluginCount and ByType still count every plugin.
	Omitted int                `json:"omitted,omitempty"`
	ByType  map[string]int     `json:"by_type"`
	Plugins []connectPluginRow `json:"plugins"`
}

// connectPluginRow is one plugin in Data. Class, Type and Version are
// sanitized (printable, capped) because they are untrusted Connect text.
type connectPluginRow struct {
	Class   string `json:"class"`
	Type    string `json:"type,omitempty"`
	Version string `json:"version,omitempty"`
}

// pluginRows sanitizes and orders the plugins: by type in pluginTypeOrder,
// then by class, so two calls over the same inventory produce the same
// summary.
func pluginRows(plugins []connect.Plugin) []connectPluginRow {
	rows := make([]connectPluginRow, 0, len(plugins))
	for _, p := range plugins {
		rows = append(rows, connectPluginRow{
			Class:   pluginText(p.Class),
			Type:    pluginText(strings.ToLower(strings.TrimSpace(p.Type))),
			Version: pluginText(p.Version),
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return pluginLess(rows[i], rows[j]) })
	return rows
}

// pluginLess orders two rows by type rank, then class; two unknown types sort
// by their own name before the class, so the order is total and stable.
func pluginLess(a, b connectPluginRow) bool {
	ra, rb := pluginTypeRank(a.Type), pluginTypeRank(b.Type)
	if ra != rb {
		return ra < rb
	}
	if ra == len(pluginTypeOrder) && a.Type != b.Type {
		return a.Type < b.Type
	}
	return a.Class < b.Class
}

// pluginTypeRank returns the pluginTypeOrder index of a type, or
// len(pluginTypeOrder) for an unknown one.
func pluginTypeRank(typ string) int {
	for i, known := range pluginTypeOrder {
		if typ == known {
			return i
		}
	}
	return len(pluginTypeOrder)
}

// buildPluginsSummary renders the version, the legacy marker, the per-type
// counts and the plugin lines, until maxPluginSummaryLines lines. It returns
// how many plugin lines it wrote so the caller can mark a cut result.
func buildPluginsSummary(info connect.ServerInfo, haveInfo, legacy bool, rows []connectPluginRow, counts map[string]int) (string, int) {
	var sb strings.Builder
	switch {
	case haveInfo && info.Version != "":
		fmt.Fprintf(&sb, "Kafka %s: %d plugins", info.Version, len(rows))
	case haveInfo:
		fmt.Fprintf(&sb, "Kafka version not reported by the worker: %d plugins", len(rows))
	default:
		fmt.Fprintf(&sb, "Kafka version unknown (the worker's GET / did not answer): %d plugins", len(rows))
	}
	if legacy {
		appendLine(&sb, "pre-3.2 Connect: connector plugins only; this worker reports no converter, transform or predicate plugins and no plugin versions")
	}
	if len(rows) > 0 {
		appendLine(&sb, pluginCountLine(counts))
	}
	shown := 0
	for _, r := range rows {
		if shown >= maxPluginSummaryLines {
			break
		}
		line := r.Type + " " + r.Class
		if r.Version != "" {
			line += " " + r.Version
		}
		appendLine(&sb, strings.TrimSpace(line))
		shown++
	}
	if rest := len(rows) - shown; rest > 0 {
		appendLine(&sb, fmt.Sprintf("%d more plugins not shown (cap)", rest))
	}
	return sb.String(), shown
}

// pluginCountLine renders "1 source, 2 converter, ..." in pluginTypeOrder,
// then any unknown types alphabetically.
func pluginCountLine(counts map[string]int) string {
	var parts []string
	for _, typ := range pluginTypeOrder {
		if n := counts[typ]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, typ))
		}
	}
	var unknown []string
	for typ := range counts {
		if pluginTypeRank(typ) == len(pluginTypeOrder) && typ != "" {
			unknown = append(unknown, typ)
		}
	}
	sort.Strings(unknown)
	for _, typ := range unknown {
		parts = append(parts, fmt.Sprintf("%d %s", counts[typ], typ))
	}
	return strings.Join(parts, ", ")
}

// pluginText folds control and format runes (an ANSI escape, a line break, a
// bidi override from a hostile worker) to spaces so a value cannot forge a
// summary line, and caps the result on a rune boundary.
func pluginText(s string) string {
	s = strings.Map(func(r rune) rune {
		if (r != ' ' && !unicode.IsPrint(r)) || r == '\u2028' || r == '\u2029' {
			return ' '
		}
		return r
	}, s)
	if len(s) > maxPluginTextBytes {
		s = truncateRunes(s, maxPluginTextBytes) + "…"
	}
	return strings.TrimSpace(s)
}
